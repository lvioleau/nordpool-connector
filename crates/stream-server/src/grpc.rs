//! The gRPC server: composes the streaming services onto one runtime.
//!
//! [`serve`] wires the [`DepthBookService`](crate::depth), the
//! [`ContractService`](crate::contracts), and the
//! [`PublicStatisticService`](crate::statistics) (plus reflection and the standard gRPC health
//! service) and blocks the calling thread on its own tokio runtime. Each service is built by its
//! own module; this module only assembles them.

use std::net::SocketAddr;
use std::time::Duration;

use anyhow::Context;
use tokio::sync::watch;
use tonic::server::NamedService;
use tonic_health::ServingStatus;
use tonic_health::server::HealthReporter;

use crate::contracts::{ContractFeed, contract_service};
use crate::depth::{DepthFeed, depth_book_service};
use crate::feed::{ContractChannel, DepthChannels, StatisticChannels};
use crate::proto;
use crate::proto::contract_service_server::ContractServiceServer;
use crate::proto::depth_book_service_server::DepthBookServiceServer;
use crate::proto::public_statistic_service_server::PublicStatisticServiceServer;
use crate::statistics::{StatisticFeed, statistic_service};

/// The runtime side of [`serve`]: the producer↔server wiring — one channel per stream plus the
/// driver's liveness watch. Each stream carries its own served-area truth inside its channel —
/// the depth and statistic sets in their per-area maps; the contract stream is global and filters
/// per subscription. Adding a stream is a new field here plus its service in [`serve`] — the
/// function signature doesn't grow per stream.
pub struct ServeChannels {
    /// Upstream liveness: `true` while the connector's market-data session is connected. The
    /// three feed services' health statuses mirror it (see [`publish_feed_health`]) so a
    /// health-checking client can tell "reachable but streaming from a dead upstream (stale
    /// seeds)" from healthy.
    pub feed_up: watch::Receiver<bool>,
    /// Process shutdown: `false` until requested, `true` from then on, never back. Fires the
    /// server's graceful drain — stop accepting, end every subscriber stream with a terminal
    /// `UNAVAILABLE`, flip the feed health entries to NotServing — after which [`serve`]
    /// returns `Ok`. The sender must outlive the server: a closed channel reads as shutdown.
    pub shutdown: watch::Receiver<bool>,
    pub depth: DepthChannels,
    pub contracts: ContractChannel,
    pub statistics: StatisticChannels,
}

/// Run the server on `addr` on a fresh tokio runtime until the `shutdown` watch in `channels`
/// fires, then drain gracefully: stop accepting connections, end every subscriber stream with a
/// terminal `UNAVAILABLE` (so clients uniformly treat any terminal status as "reconnect and
/// re-seed"), and return `Ok` once in-flight requests have completed. Blocking — call it from a
/// dedicated thread. Each stream is fed by its channel in [`ServeChannels`] (a clone of the one
/// the connector's sink writes) and guards subscribe requests against the areas its channel
/// serves; each stream is capped at `max_subscribers` concurrent clients. Idle client
/// connections are probed every `keepalive_interval` (an HTTP/2 keepalive PING, plus TCP
/// keepalive at the same interval for gRPC-Web/HTTP1 connections) and closed after an unanswered
/// `keepalive_timeout` — a client that vanished without closing (network partition, hard
/// power-off) otherwise holds its subscriber permit until a write to it fails, which on a quiet
/// feed may be never.
pub fn serve(
    addr: SocketAddr,
    max_subscribers: usize,
    keepalive_interval: Duration,
    keepalive_timeout: Duration,
    channels: ServeChannels,
) -> anyhow::Result<()> {
    let ServeChannels {
        feed_up,
        shutdown,
        depth,
        contracts,
        statistics,
    } = channels;

    // The health descriptor set makes grpc.health.v1.Health resolvable through reflection
    // (grpcurl/grpcui); without it the service answers but reflection-driven clients can't
    // find it.
    let reflection = tonic_reflection::server::Builder::configure()
        .register_encoded_file_descriptor_set(proto::FILE_DESCRIPTOR_SET)
        .register_encoded_file_descriptor_set(tonic_health::pb::FILE_DESCRIPTOR_SET)
        .build_v1()
        .context("building gRPC reflection service")?;

    let (mut health_reporter, health_service) = tonic_health::server::health_reporter();

    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async move {
        // The server-wide "" entry is process liveness and stays static: main aborts if this
        // server thread ever returns, so reachable means the process is alive. The three feed
        // services' entries track the upstream session instead (see publish_feed_health).
        health_reporter
            .set_service_status("", ServingStatus::Serving)
            .await;
        tokio::spawn(publish_feed_health(
            health_reporter,
            feed_up,
            shutdown.clone(),
        ));
        let mut drain = shutdown.clone();
        tracing::info!(%addr, max_subscribers, "depth-book gRPC server listening");
        // accept_http1 + GrpcWebLayer let browsers reach the same feeds over gRPC-Web (the web
        // viewer's fetch transport speaks HTTP/1.1); the layer is content-type aware, so native
        // gRPC clients (application/grpc over h2) pass through untouched.
        tonic::transport::Server::builder()
            .accept_http1(true)
            // Probe idle connections so a silently dead client is detected and closed, releasing
            // its subscriber permits — a quiet feed may otherwise never write, leaving a vanished
            // client's permit held forever. TCP keepalive covers the gRPC-Web/HTTP1 connections
            // that HTTP/2 PINGs can't reach.
            .http2_keepalive_interval(Some(keepalive_interval))
            .http2_keepalive_timeout(Some(keepalive_timeout))
            .tcp_keepalive(Some(keepalive_interval))
            .layer(tonic_web::GrpcWebLayer::new())
            .add_service(depth_book_service(depth, max_subscribers, shutdown.clone()))
            .add_service(contract_service(
                contracts,
                max_subscribers,
                shutdown.clone(),
            ))
            .add_service(statistic_service(statistics, max_subscribers, shutdown))
            .add_service(health_service)
            .add_service(reflection)
            // wait_for, not changed(): resolves even when shutdown was requested before the
            // server got here. The drain this triggers only completes because every
            // subscriber stream also cuts itself on the same watch — the streams are
            // infinite, so tonic would otherwise wait on them forever.
            .serve_with_shutdown(addr, async move {
                let _ = drain.wait_for(|&requested| requested).await;
            })
            .await
    })?;
    tracing::info!("gRPC server drained");
    Ok(())
}

/// The per-feed health entries: exactly the three streaming services, by gRPC service name. The
/// server-wide "" entry is deliberately not here — it stays Serving for the life of the process.
const FEED_SERVICES: [&str; 3] = [
    DepthBookServiceServer::<DepthFeed>::NAME,
    ContractServiceServer::<ContractFeed>::NAME,
    PublicStatisticServiceServer::<StatisticFeed>::NAME,
];

/// Mirror the upstream session's liveness onto the three feed services' health statuses: Serving
/// while the connector's market-data session is connected, NotServing while it is down or
/// reconnecting — the seeds and stores may then be stale, and nothing new will stream — and
/// NotServing from the moment a shutdown drain starts, so health checks stop routing to a server
/// that no longer accepts connections. Runs until a watch's sender side is dropped, leaving the
/// last published status standing.
async fn publish_feed_health(
    mut reporter: HealthReporter,
    mut feed_up: watch::Receiver<bool>,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        let up = *feed_up.borrow_and_update() && !*shutdown.borrow_and_update();
        let status = if up {
            ServingStatus::Serving
        } else {
            ServingStatus::NotServing
        };
        for service in FEED_SERVICES {
            reporter.set_service_status(service, status).await;
        }
        tokio::select! {
            changed = feed_up.changed() => if changed.is_err() { return },
            changed = shutdown.changed() => if changed.is_err() { return },
        }
    }
}
