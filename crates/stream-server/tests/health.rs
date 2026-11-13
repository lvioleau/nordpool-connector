//! Black-box checks that [`serve`] hosts the standard gRPC health service
//! (`grpc.health.v1.Health`): the server-wide "" service is statically serving (reachable means
//! the process is alive), while the three feed services mirror the upstream-liveness watch the
//! connector's driver flips with each session — and that firing the shutdown watch drains the
//! server and returns `serve` instead of blocking forever.

use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

use nordpool_marketdata::EicCode;
use nordpool_stream_server::{
    ContractChannel, DepthChannels, ServeChannels, StatisticChannels, serve,
};
use tokio::sync::watch;
use tonic_health::pb::HealthCheckRequest;
use tonic_health::pb::health_check_response::ServingStatus;
use tonic_health::pb::health_client::HealthClient;

/// The three feed services' health entries, by wire service name — the names a real
/// health-checking client would use, spelled out because this test is black-box.
const FEED_SERVICES: [&str; 3] = [
    "nordpool.marketdata.v1.DepthBookService",
    "nordpool.marketdata.v1.ContractService",
    "nordpool.marketdata.v1.PublicStatisticService",
];

/// A shutdown receiver that never fires: the sender is leaked to keep the watch open — a
/// closed watch reads as shutdown and would drain the server out from under the test.
fn no_shutdown() -> watch::Receiver<bool> {
    let (tx, rx) = watch::channel(false);
    std::mem::forget(tx);
    rx
}

/// Spawn the full server on a free local port with empty feed channels and the given
/// upstream-liveness and shutdown watches; returns the address it listens on and the serve
/// thread's handle.
fn spawn_server(
    feed_up: watch::Receiver<bool>,
    shutdown: watch::Receiver<bool>,
) -> (SocketAddr, std::thread::JoinHandle<anyhow::Result<()>>) {
    let addr = TcpListener::bind("127.0.0.1:0")
        .and_then(|l| l.local_addr())
        .unwrap();
    let server = std::thread::spawn(move || {
        serve(
            addr,
            1,
            Duration::from_secs(30),
            Duration::from_secs(20),
            ServeChannels {
                feed_up,
                shutdown,
                depth: DepthChannels::new(8, &[EicCode::AREA_FR]),
                contracts: ContractChannel::new(8),
                statistics: StatisticChannels::new(8, &[EicCode::AREA_FR]),
            },
        )
    });
    (addr, server)
}

async fn connect_with_retry(addr: SocketAddr) -> HealthClient<tonic::transport::Channel> {
    let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}")).unwrap();
    for _ in 0..50 {
        if let Ok(channel) = endpoint.connect().await {
            return HealthClient::new(channel);
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("gRPC server did not come up on {addr}");
}

async fn check(
    client: &mut HealthClient<tonic::transport::Channel>,
    service: &str,
) -> Result<i32, tonic::Status> {
    Ok(client
        .check(HealthCheckRequest {
            service: service.to_string(),
        })
        .await?
        .into_inner()
        .status)
}

/// Poll until `service` reports `want`. The health task mirrors the watch asynchronously, so a
/// flip is visible only eventually; a not-yet-registered service (NOT_FOUND) counts as not yet.
async fn await_status(
    client: &mut HealthClient<tonic::transport::Channel>,
    service: &str,
    want: ServingStatus,
) {
    for _ in 0..50 {
        if check(client, service).await.ok() == Some(want as i32) {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("{service} never reached {want:?}");
}

#[test]
fn health_service_reports_the_server_as_serving() {
    // Given the full server running with the upstream watch down — the "" entry is process
    // liveness, deliberately independent of the feeds' state.
    let (addr, _server) = spawn_server(watch::channel(false).1, no_shutdown());

    // When the standard health Check is called for the server-wide "" service.
    let runtime = tokio::runtime::Runtime::new().unwrap();
    let status = runtime.block_on(async move {
        let mut client = connect_with_retry(addr).await;
        check(&mut client, "").await.unwrap()
    });

    // Then it reports SERVING.
    assert_eq!(status, ServingStatus::Serving as i32);
}

#[test]
fn shutdown_drains_the_server_and_returns_serve() {
    // Given the full server running (proven reachable by a health check) and its shutdown
    // watch still unfired.
    let (shutdown, rx) = watch::channel(false);
    let (addr, server) = spawn_server(watch::channel(false).1, rx);
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        let mut client = connect_with_retry(addr).await;
        check(&mut client, "").await.unwrap();
    });

    // When a shutdown is requested.
    shutdown.send(true).unwrap();

    // Then the graceful drain completes and serve returns Ok — the thread ends instead of
    // blocking for the life of the process, so main can wait on it before exiting.
    server
        .join()
        .expect("serve thread panicked")
        .expect("drained cleanly");
}

#[test]
fn feed_services_health_mirrors_the_upstream_session() {
    // Given a server whose upstream-liveness watch starts down — the state at startup, before
    // the driver's first successful handshake.
    let (feed_up, rx) = watch::channel(false);
    let (addr, _server) = spawn_server(rx, no_shutdown());

    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async move {
        let mut client = connect_with_retry(addr).await;

        // Then every feed service starts NOT_SERVING, while the server-wide "" entry is
        // SERVING throughout — reachable process, dead upstream.
        for service in FEED_SERVICES {
            await_status(&mut client, service, ServingStatus::NotServing).await;
        }
        assert_eq!(
            check(&mut client, "").await.unwrap(),
            ServingStatus::Serving as i32
        );

        // When the driver reports a connected session, every feed flips to SERVING.
        feed_up.send_replace(true);
        for service in FEED_SERVICES {
            await_status(&mut client, service, ServingStatus::Serving).await;
        }

        // And when the session ends (silence recycle, reconnect), every feed flips back to
        // NOT_SERVING — a health-checking client can now tell stale-feed from healthy.
        feed_up.send_replace(false);
        for service in FEED_SERVICES {
            await_status(&mut client, service, ServingStatus::NotServing).await;
        }
    });
}
