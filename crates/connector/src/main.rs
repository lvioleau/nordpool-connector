use anyhow::{Context, Result, anyhow};
use uuid::Uuid;

use metrics_exporter_dogstatsd::DogStatsDBuilder;
use nordpool_connector::{
    ArchiveSink, Config, ContractSink, FrameSink, LocalViewSink, QueuedSink, ShutdownRequested,
    StatisticSink, TeeSink, Token, Uploader, WsTransport, config, feed_status, fetch_token,
    request_shutdown, run_with_reconnect, shutdown_watch, transport,
};
use std::sync::mpsc;

use nordpool_stream_server::{
    ContractChannel, DepthChannels, ServeChannels, StatisticChannels, serve,
};

/// The build's short git SHA, stamped by `build.rs`; tags metrics and the startup log
/// so deploys draw a `version:` boundary through the dashboards.
const VERSION: &str = env!("GIT_SHA");

/// Receives the gRPC serve thread's outcome (a oneshot: one thread, one send). `serve` blocks
/// until a requested shutdown's graceful drain completes — the `Ok` path — so returning any
/// other way is a failure (bind error, runtime death): the thread sends the error here before
/// requesting shutdown, and `main` waits on this after the driver loop ends so the process
/// exits with that error instead of reporting a clean shutdown while the feeds are dead — and,
/// on the clean path, doesn't exit under a drain still ending client streams.
type ServeDone = mpsc::Receiver<anyhow::Result<()>>;

fn main() -> Result<()> {
    // This guard flushes the non-blocking stdout writer's background thread; keep it alive
    // for the whole process or buffered lines are lost on exit.
    let _log_guard = config::logging::init();
    tracing::info!(version = VERSION, "connector starting");
    config::init(Config::load().context("loading configuration")?);
    init_metrics()?;
    init_signal_handler()?;

    let cfg = config::get();
    // The sink comes first: building it runs the archive's startup recovery, which compresses
    // any `.partial` a previous hard kill left behind. The uploader's first scan then sees an
    // archive dir the writer has already reconciled, rather than one mid-recovery.
    let (sink, serve_done) = build_sink().context("building frame sink")?;
    let uploader = build_uploader().context("starting archive uploader")?;
    // run_with_reconnect takes ownership of the sink. it drops inside it on return.
    let result = run_with_reconnect(connect_session, &cfg.connector.credentials, sink);

    // The loop can also end on a fatal error with no shutdown requested; the gRPC server's
    // graceful drain waits on the shutdown watch, so flip it (idempotent) on every exit path.
    request_shutdown();

    // shutdown. no more sink here
    if let Some(uploader) = uploader {
        uploader.shutdown();
    }

    // serve_done is Some only when the server was spawned (listen_addr set), so the drain runs
    // exactly when the feed is enabled.
    if let Some(done) = serve_done {
        let server = &cfg.server;
        match done.recv_timeout(server.shutdown_grace) {
            // The graceful drain completed: every subscriber stream ended with its terminal
            // status and in-flight requests finished.
            Ok(Ok(())) => {}
            // A dead gRPC server thread ends the loop through the shutdown flag; its sent
            // error must fail the process, not pass as a clean shutdown.
            Ok(Err(err)) => return Err(err.context("gRPC server failed")),
            Err(mpsc::RecvTimeoutError::Timeout) => tracing::warn!(
                grace_secs = server.shutdown_grace.as_secs(),
                "gRPC drain grace expired; exiting with clients still connected"
            ),
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                return Err(anyhow!("gRPC server thread died without reporting"));
            }
        }
    }

    match result {
        Err(e) if e.is::<ShutdownRequested>() => {
            tracing::info!("shutdown complete");
            Ok(())
        }
        other => other.context("running client loop"),
    }
}

/// Install the global DogStatsD metrics recorder. Always installed: DogStatsD is UDP, so with no
/// agent at `dogstatsd_addr` (defaulting to localhost) the aggregated packets drop silently —
/// local runs need no agent, and the `metrics::` macros stay live rather than no-ops. The exporter
/// aggregates client-side and flushes from a background thread, so emission sites never do socket IO.
fn init_metrics() -> Result<()> {
    let metrics = &config::get().metrics;
    // Every series carries the build's version tag — deploys then show up as one tag value's
    // series ending where the next begins, on every chart — plus any configured identifying tags.
    let mut labels = vec![metrics::Label::new("version", VERSION)];
    labels.extend(
        metrics
            .tags
            .iter()
            .map(|(k, v)| metrics::Label::new(k.clone(), v.clone())),
    );
    DogStatsDBuilder::default()
        .with_remote_address(&metrics.dogstatsd_addr)
        .with_context(|| format!("invalid dogstatsd_addr {:?}", metrics.dogstatsd_addr))?
        .with_global_labels(labels)
        .install()
        .context("installing DogStatsD metrics exporter")?;
    tracing::info!(addr = %metrics.dogstatsd_addr, version = VERSION, tags = ?metrics.tags, "DogStatsD metrics export enabled");
    Ok(())
}

/// Install the SIGINT/SIGTERM/SIGHUP handler. It sets the shutdown flag; the loops poll
/// it and return Ok, unwinding the stack so transport.close() and the sinks' flush-on-Drop
/// run via Drop.
fn init_signal_handler() -> Result<()> {
    ctrlc::set_handler(|| {
        tracing::info!("shutdown signal received");
        request_shutdown();
    })
    .context("registering shutdown signal handler")
}

/// Fetch a fresh token and open a fresh WS session. The reconnect wrapper calls this
/// each attempt — every reconnect uses a new token and a new session UUID.
fn connect_session() -> Result<(WsTransport, Token)> {
    let cfg = &config::get().connector;
    let token = fetch_token(&cfg.credentials).context("fetching token")?;
    let session_id = Uuid::new_v4();
    let url = url::Url::parse(&format!(
        "wss://{}:{}/api/v2/{session_id}",
        cfg.ws_host, cfg.ws_port
    ))
    .context("parsing WebSocket URL")?;
    let transport =
        transport::connect(&url, &token).with_context(|| format!("connecting to {url}"))?;
    tracing::info!(url = %url, "connected");
    Ok((transport, token))
}

fn build_sink() -> Result<(TeeSink, Option<ServeDone>)> {
    let cfg = config::get();
    let mut sinks: Vec<Box<dyn FrameSink>> = Vec::new();
    // The archive is always on. It does blocking disk I/O, so it runs on its own thread behind a
    // bounded queue (QueuedSink) rather than on the driver's receive loop.
    let archive = &cfg.archive;
    let sink = ArchiveSink::new(archive).context("building archive sink")?;
    let queued = QueuedSink::new(
        "archive",
        Box::new(sink),
        archive.queue_capacity,
        archive.enqueue_timeout,
        archive.poll_interval,
    )
    .context("starting archive queue")?;
    sinks.push(Box::new(queued));
    let mut serve_done = None;
    if let Some(server) = spawn_stream_server()? {
        sinks.push(Box::new(server.local_view));
        sinks.push(Box::new(server.contracts));
        sinks.push(Box::new(server.statistics));
        serve_done = Some(server.done);
    }
    Ok((TeeSink::new(sinks), serve_done))
}

/// Archive uploader: the archive always runs, so this is gated only on
/// `[connector.archive.upload] uri` being set. Spawns the uploader thread and returns its handle;
/// `None` when upload is off. The handle is dropped after the frame loop so its final drain ships
/// everything `ArchiveSink::drop` just finalized.
fn build_uploader() -> Result<Option<Uploader>> {
    let cfg = config::get();
    let archive = &cfg.archive;
    let Some(upload) = &archive.upload else {
        tracing::info!("[connector.archive.upload] uri empty — archive upload disabled");
        return Ok(None);
    };
    // The archive is one multiplexed stream across all subscribed areas, so its S3 key carries no
    // per-area partition (`<base>/<type>/<date>/<name>`).
    Ok(Some(
        Uploader::start(archive.dir.clone(), upload).context("starting archive uploader")?,
    ))
}

/// A running gRPC server's producer side: the three sinks that feed it — over the tokio broadcast
/// channels the server subscribes each client onto — plus the slot its thread reports through.
struct StreamServer {
    local_view: LocalViewSink,
    contracts: ContractSink,
    statistics: StatisticSink,
    done: ServeDone,
}

/// Depth + contract + statistic gRPC server: enabled only when the `[server]` section is present.
/// When enabled, spawns the gRPC server on its own thread/runtime and returns its [`StreamServer`]
/// producer side, whose [`ServeDone`] the serve thread fills when it ends (a drained `Ok` or the
/// failure that killed it); returns `None` when disabled.
fn spawn_stream_server() -> Result<Option<StreamServer>> {
    let cfg = config::get();
    let server = &cfg.server;
    let Some(listen_addr) = server.listen_addr else {
        tracing::warn!("[server] not configured — stream server disabled");
        return Ok(None);
    };
    let delivery_areas = &cfg.connector.delivery_area_eics;

    // Each stream's producer↔server channel — the broadcast sender plus its seed (the depth seed
    // maps, one per served area / the global contract store). Created once, then cloned: the
    // connector's sink writes one clone, the gRPC service reads the other.
    let depth = DepthChannels::new(server.broadcast_capacity, delivery_areas);
    let contracts = ContractChannel::new(server.broadcast_capacity);
    // Statistics are area-keyed like depth — serve the full configured set, not just the primary.
    let statistics = StatisticChannels::new(server.broadcast_capacity, delivery_areas);

    let channels = ServeChannels {
        // The driver flips this with each session; the server mirrors it onto the three feed
        // services' health statuses.
        feed_up: feed_status(),
        // Fires the server's graceful drain: stop accepting, end every subscriber stream with
        // a terminal status. The watch's static sender never drops, so the server can't
        // mistake a closed channel for a shutdown.
        shutdown: shutdown_watch(),
        depth: depth.clone(),
        contracts: contracts.clone(),
        statistics: statistics.clone(),
    };
    let (done_tx, serve_done) = mpsc::channel();
    std::thread::spawn(move || {
        // `serve` returns Ok once a requested shutdown's drain completes — the expected end.
        // Returning any other way — bind failure, runtime death — means the configured feeds
        // cannot be served: send the failure and end the driver loop, so the process exits
        // loudly rather than running on feedless. Send before requesting shutdown so main's
        // post-loop receive is guaranteed to see it.
        let result = match serve(
            listen_addr,
            server.max_subscribers,
            server.keepalive_interval,
            server.keepalive_timeout,
            channels,
        ) {
            Ok(()) if *shutdown_watch().borrow() => Ok(()),
            Ok(()) => Err(anyhow!("gRPC server exited unexpectedly")),
            Err(err) => Err(err),
        };
        match result {
            Ok(()) => {
                let _ = done_tx.send(Ok(()));
            }
            Err(err) => {
                tracing::error!(
                    error = format!("{err:#}"),
                    "gRPC server stopped; shutting down"
                );
                let _ = done_tx.send(Err(err));
                request_shutdown();
            }
        }
    });

    Ok(Some(StreamServer {
        local_view: LocalViewSink::new(depth, cfg.metrics.book_state_period, server.depth),
        contracts: ContractSink::new(contracts, delivery_areas),
        statistics: StatisticSink::new(statistics),
        done: serve_done,
    }))
}
