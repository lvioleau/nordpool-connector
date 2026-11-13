use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::{DateTime, Utc};
use prost::Message;

use crate::auth::{Token, TokenSource};
use crate::config;
use crate::proto::pmd::client_frame::Payload as ClientPayload;
use crate::proto::pmd::server_frame::Payload as ServerPayload;
use crate::proto::pmd::{ClientFrame, ServerFrame, TokenRefresh};
use crate::sink::FrameSink;
use crate::transport::Transport;

mod errors;
mod feed_status;
mod reconnect;
mod retry;
mod shutdown;
mod subscriptions;

pub(crate) use errors::describe;
pub use errors::{
    ConnectedTimeout, MissingHeartbeatInterval, ServerError, ServerSilent, UnexpectedHandshakeFrame,
};
use errors::{SessionExpired, is_recoverable, reason_label};
pub use feed_status::feed_status;
use reconnect::{reconnect_requested, session_deadline};
use retry::{Backoff, retry_with_backoff};
// Crate-visible: the archive's worker-thread retry has to give up on a stopping process too.
pub(crate) use shutdown::shutdown_requested;
pub use shutdown::{ShutdownRequested, request_shutdown, shutdown_watch};
pub use subscriptions::{
    subscribe_all, subscribe_to_atc_capacity, subscribe_to_contracts, subscribe_to_delivery_areas,
    subscribe_to_h2h_capacity, subscribe_to_localview, subscribe_to_public_statistics,
    subscribe_to_ticker,
};

pub fn send<T: Transport>(transport: &mut T, payload: ClientPayload) -> Result<()> {
    let frame = ClientFrame {
        payload: Some(payload),
    };
    transport.send(frame.encode_to_vec()).map_err(Into::into)
}

pub fn run_with_reconnect<T, F, S, K>(mut connect: F, token_source: &S, mut sink: K) -> Result<()>
where
    T: Transport,
    F: FnMut() -> Result<(T, Token)>,
    S: TokenSource,
    K: FrameSink,
{
    let cfg = &config::get().connector;
    let backoff = Backoff {
        initial: cfg.initial_backoff,
        max: cfg.max_backoff,
        reset_after: cfg.backoff_reset_after,
    };
    mark_connected(false);
    retry_with_backoff(&backoff, std::thread::sleep, || {
        match connect_and_run(&mut connect, token_source, &mut sink) {
            Err(_) if shutdown_requested() => Err(ShutdownRequested.into()),
            // Deliberate, scheduled recycle — reconnect immediately (the long session already
            // reset the backoff), not an error to warn about.
            Err(e) if e.is::<SessionExpired>() => {
                metrics::counter!("nordpool.reconnects", "reason" => "scheduled").increment(1);
                tracing::info!("scheduled session reconnect; resubscribing for fresh snapshots");
                Ok(())
            }
            Err(e) if is_recoverable(&e) => {
                metrics::counter!("nordpool.reconnects", "reason" => reason_label(&e)).increment(1);
                tracing::warn!(error = format!("{e:#}"), "recoverable error, reconnecting");
                Ok(())
            }
            other => other,
        }
    })
}

/// One truth for session liveness: the `nordpool.connected` gauge and the [`feed_status`]
/// watch flip together, so the dashboards and the gRPC health service can never disagree.
fn mark_connected(connected: bool) {
    metrics::gauge!("nordpool.connected").set(if connected { 1.0 } else { 0.0 });
    feed_status::set_feed_up(connected);
}

fn connect_and_run<T, F, S, K>(connect: &mut F, token_source: &S, sink: &mut K) -> Result<()>
where
    T: Transport,
    F: FnMut() -> Result<(T, Token)>,
    S: TokenSource,
    K: FrameSink,
{
    let (mut transport, mut token) = connect()?;
    let result = run(&mut transport, token_source, &mut token, sink);
    // `run` only returns when the session is over, however it ended.
    mark_connected(false);
    result
}

pub fn run<T: Transport, S: TokenSource, K: FrameSink>(
    transport: &mut T,
    token_source: &S,
    token: &mut Token,
    sink: &mut K,
) -> Result<()> {
    let cfg = &config::get().connector;
    let heartbeat_interval = wait_for_connected(transport, cfg.connected_timeout)?;
    mark_connected(true);

    subscribe_all(transport, &cfg.delivery_area_eics)?;

    let reconnect_deadline = cfg.session_reconnect.map(session_deadline);
    let silence_limit = heartbeat_interval * cfg.heartbeat_timeout_factor;
    let mut last_frame = Instant::now();

    loop {
        if shutdown_requested() {
            tracing::info!("shutdown requested; ending session");
            return Err(ShutdownRequested.into());
        }
        if reconnect_requested(reconnect_deadline) {
            tracing::info!(
                "session reached its scheduled reconnect; recycling for fresh snapshots"
            );
            return Err(SessionExpired.into());
        }
        if last_frame.elapsed() > silence_limit {
            return Err(ServerSilent {
                silent_for: last_frame.elapsed(),
                heartbeat_interval,
            }
            .into());
        }

        if let Some(bytes) = transport.recv()? {
            let received_at = Utc::now();
            last_frame = Instant::now();
            let frame = ServerFrame::decode(bytes.as_slice())?;
            match &frame.payload {
                Some(ServerPayload::Heartbeat(_)) => record_heartbeat(),
                Some(
                    payload @ (ServerPayload::LocalView(_)
                    | ServerPayload::DeliveryArea(_)
                    | ServerPayload::Contract(_)
                    | ServerPayload::Ticker(_)
                    | ServerPayload::AtcCapacity(_)
                    | ServerPayload::H2hCapacity(_)
                    | ServerPayload::PublicStatistic(_)),
                ) => dispatch_frame(sink, payload, &bytes, received_at)?,
                Some(ServerPayload::Error(e)) => {
                    return Err(ServerError {
                        message: e.message.clone(),
                    }
                    .into());
                }
                Some(ServerPayload::Connected(_)) => {
                    tracing::warn!("unexpected Connected frame after handshake");
                }
                // A frame whose oneof variant our compiled proto doesn't know decodes to
                // None — likely the server speaks a newer schema. No sink can route it, so
                // flag the drop instead of losing it silently.
                None => tracing::warn!(
                    bytes = bytes.len(),
                    "frame with unknown payload variant; dropped"
                ),
            }
        }

        if token.needs_refresh() {
            refresh_token(transport, token_source, token)?;
        }
    }
}

/// Count a server heartbeat (`nordpool.heartbeats`). Observability only — the silence watchdog
/// keys off `last_frame`, which every frame resets, not off heartbeats specifically.
fn record_heartbeat() {
    metrics::counter!("nordpool.heartbeats").increment(1);
    tracing::debug!("server heartbeat");
}

/// Count a market-data frame for its stream (`nordpool.frames` / `nordpool.frame_bytes`) and
/// hand it to the sink with its original wire bytes and receive time.
fn dispatch_frame<K: FrameSink>(
    sink: &mut K,
    payload: &ServerPayload,
    bytes: &[u8],
    received_at: DateTime<Utc>,
) -> Result<()> {
    let stream = describe(payload);
    metrics::counter!("nordpool.frames", "stream" => stream).increment(1);
    metrics::counter!("nordpool.frame_bytes", "stream" => stream).increment(bytes.len() as u64);
    sink.handle(payload, bytes, received_at)
}

fn refresh_token<T: Transport, S: TokenSource>(
    transport: &mut T,
    token_source: &S,
    token: &mut Token,
) -> Result<()> {
    *token = token_source.fetch()?;
    metrics::counter!("nordpool.token_refreshes").increment(1);
    tracing::info!("sending TokenRefresh");
    send(
        transport,
        ClientPayload::TokenRefresh(TokenRefresh {
            token: token.access_token.clone(),
        }),
    )
}

/// Wait for the server's `Connected` handshake frame and return its announced heartbeat
/// interval. An unusable interval (zero or negative) is a recoverable
/// [`MissingHeartbeatInterval`] — the silence watchdog could not be armed, so the session is
/// refused rather than run unguarded.
fn wait_for_connected<T: Transport>(transport: &mut T, timeout: Duration) -> Result<Duration> {
    let deadline = Instant::now() + timeout;
    while !shutdown_requested() {
        if Instant::now() >= deadline {
            return Err(ConnectedTimeout(timeout).into());
        }
        let Some(bytes) = transport.recv()? else {
            continue;
        };
        let frame = ServerFrame::decode(bytes.as_slice())?;
        match &frame.payload {
            Some(ServerPayload::Connected(c)) => {
                tracing::info!(interval_ms = c.heartbeat_interval_ms, "server connected");
                if c.heartbeat_interval_ms <= 0 {
                    return Err(MissingHeartbeatInterval(c.heartbeat_interval_ms).into());
                }
                return Ok(Duration::from_millis(c.heartbeat_interval_ms as u64));
            }
            Some(ServerPayload::Error(e)) => {
                return Err(ServerError {
                    message: e.message.clone(),
                }
                .into());
            }
            Some(other) => {
                return Err(UnexpectedHandshakeFrame {
                    kind: describe(other),
                }
                .into());
            }
            None => {}
        }
    }
    Err(ShutdownRequested.into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::AuthError;
    use crate::proto::pmd::subscribe::Target as SubscribeTarget;
    use crate::proto::pmd::{
        CapacityFlow, ConnectedPayload, ErrorPayload, Heartbeat, LocalView, LocalViewPayload,
        ServerFrame,
    };
    use crate::transport::TransportError;
    use std::collections::VecDeque;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{Duration, Instant};

    /// Clears the thread-local shutdown flag when a shutdown test ends (even on panic), so a
    /// test thread reused by libtest doesn't inherit a set flag.
    struct ShutdownOnDrop;
    impl Drop for ShutdownOnDrop {
        fn drop(&mut self) {
            super::shutdown::reset();
        }
    }

    /// Queue of frames (or transport errors) the fake transport hands back from `recv`.
    type IncomingQueue = Arc<Mutex<VecDeque<Result<Vec<u8>, TransportError>>>>;

    #[derive(Clone, Default)]
    struct FakeTransport {
        incoming: IncomingQueue,
        sent: Arc<Mutex<Vec<Vec<u8>>>>,
        closed: Arc<Mutex<bool>>,
        shutdown_on_drain: Arc<AtomicBool>,
    }

    impl FakeTransport {
        fn push_incoming(&self, payload: ServerPayload) {
            let bytes = ServerFrame {
                payload: Some(payload),
            }
            .encode_to_vec();
            self.incoming.lock().unwrap().push_back(Ok(bytes));
        }
        fn push_recv_error(&self, e: TransportError) {
            self.incoming.lock().unwrap().push_back(Err(e));
        }
        /// Request shutdown the first time `recv` finds the queue drained, to simulate a
        /// shutdown arriving mid-session.
        fn shutdown_when_drained(&self) {
            self.shutdown_on_drain.store(true, Ordering::SeqCst);
        }
        fn sent_frames(&self) -> Vec<ClientFrame> {
            self.sent
                .lock()
                .unwrap()
                .iter()
                .map(|b| ClientFrame::decode(b.as_slice()).expect("decode ClientFrame"))
                .collect()
        }
    }

    impl Transport for FakeTransport {
        fn recv(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
            match self.incoming.lock().unwrap().pop_front() {
                Some(Ok(bytes)) => Ok(Some(bytes)),
                Some(Err(e)) => Err(e),
                None => {
                    if self.shutdown_on_drain.load(Ordering::SeqCst) {
                        request_shutdown();
                    }
                    Ok(None)
                }
            }
        }
        fn send(&mut self, bytes: Vec<u8>) -> Result<(), TransportError> {
            self.sent.lock().unwrap().push(bytes);
            Ok(())
        }
    }

    impl Drop for FakeTransport {
        fn drop(&mut self) {
            *self.closed.lock().unwrap() = true;
        }
    }

    struct FakeTokenSource {
        next: String,
    }
    impl TokenSource for FakeTokenSource {
        fn fetch(&self) -> Result<Token, AuthError> {
            Ok(Token {
                access_token: self.next.clone(),
                expires_at: Instant::now() + Duration::from_secs(3600),
            })
        }
    }

    #[derive(Clone, Default)]
    struct FakeSink {
        received: Arc<Mutex<Vec<Vec<u8>>>>,
    }
    impl FrameSink for FakeSink {
        fn handle(
            &mut self,
            _payload: &ServerPayload,
            bytes: &[u8],
            _recv_time: DateTime<Utc>,
        ) -> anyhow::Result<()> {
            self.received.lock().unwrap().push(bytes.to_vec());
            Ok(())
        }
    }

    fn fresh_token() -> Token {
        Token {
            access_token: "tok".into(),
            expires_at: Instant::now() + Duration::from_secs(3600),
        }
    }

    /// Sending outbound client frames: the `send` wrapper, the `subscribe_to_*` primitives, and
    /// `refresh_token`.
    mod sending {
        use super::*;

        #[test]
        fn send_wraps_payload_in_client_frame() {
            // Given a fresh transport that records everything written to it, with nothing sent yet.
            let mut transport = FakeTransport::default();

            // When we send a bare Heartbeat payload through the `send` wrapper, whose job is to wrap
            // any client payload in a `ClientFrame` envelope and encode it onto the transport.
            send(&mut transport, ClientPayload::Heartbeat(Heartbeat {})).unwrap();

            // Then exactly one frame went out and it carries the Heartbeat in its payload oneof,
            // confirming the wrapper preserves the payload rather than dropping or altering it.
            let frames = transport.sent_frames();
            assert_eq!(frames.len(), 1);
            assert!(matches!(
                frames[0].payload,
                Some(ClientPayload::Heartbeat(_))
            ));
        }

        #[test]
        fn subscribe_to_delivery_areas_sends_empty_target() {
            // Given a fresh recording transport, nothing sent yet.
            let mut transport = FakeTransport::default();

            // When we issue the delivery-areas subscription, which asks the server for the catalog
            // of delivery areas and so needs no parameters (an empty target selector).
            subscribe_to_delivery_areas(&mut transport).unwrap();

            // Then the sent frame is a Subscribe carrying the DeliveryArea target variant, proving
            // the subscription was encoded against the right stream.
            let frames = transport.sent_frames();
            let Some(ClientPayload::Subscribe(sub)) = frames[0].payload.clone() else {
                panic!("expected Subscribe payload")
            };
            assert!(
                matches!(sub.target, Some(SubscribeTarget::DeliveryArea(_))),
                "expected DeliveryArea target, got {:?}",
                sub.target
            );
        }

        #[test]
        fn subscribe_to_contracts_sends_empty_target() {
            // Given a fresh recording transport, nothing sent yet.
            let mut transport = FakeTransport::default();

            // When we issue the contracts subscription, which requests the contract reference feed
            // and so carries no selector (an empty target).
            subscribe_to_contracts(&mut transport).unwrap();

            // Then the sent frame is a Subscribe carrying the Contract target variant, confirming
            // it targets the contract stream.
            let frames = transport.sent_frames();
            let Some(ClientPayload::Subscribe(sub)) = frames[0].payload.clone() else {
                panic!("expected Subscribe payload")
            };
            assert!(
                matches!(sub.target, Some(SubscribeTarget::Contract(_))),
                "expected Contract target, got {:?}",
                sub.target
            );
        }

        #[test]
        fn subscribe_to_ticker_sends_empty_target() {
            // Given a fresh recording transport, nothing sent yet.
            let mut transport = FakeTransport::default();

            // When we issue the ticker subscription, which requests the trade-ticker feed and so
            // carries no selector (an empty target).
            subscribe_to_ticker(&mut transport).unwrap();

            // Then the sent frame is a Subscribe carrying the Ticker target variant, confirming it
            // targets the ticker stream.
            let frames = transport.sent_frames();
            let Some(ClientPayload::Subscribe(sub)) = frames[0].payload.clone() else {
                panic!("expected Subscribe payload")
            };
            assert!(
                matches!(sub.target, Some(SubscribeTarget::Ticker(_))),
                "expected Ticker target, got {:?}",
                sub.target
            );
        }

        #[test]
        fn subscribe_to_atc_capacity_includes_requested_flows() {
            // Given a fresh recording transport and two capacity flows to request: one with two
            // connected delivery areas and one with none, so we can check the list is carried as-is.
            let mut transport = FakeTransport::default();
            let flows = vec![
                CapacityFlow {
                    delivery_area: 40,
                    connected_delivery_areas: vec![38, 39],
                },
                CapacityFlow {
                    delivery_area: 41,
                    connected_delivery_areas: vec![],
                },
            ];

            // When we subscribe to ATC (available transfer capacity) for those specific flows —
            // unlike the catalog subscriptions, this one is parameterized by the caller's flows.
            subscribe_to_atc_capacity(&mut transport, flows.clone()).unwrap();

            // Then the Subscribe frame carries the AtcCapacity target with exactly the flows we
            // asked for, verifying the parameters round-trip into the wire message unchanged.
            let frames = transport.sent_frames();
            let Some(ClientPayload::Subscribe(sub)) = frames[0].payload.clone() else {
                panic!("expected Subscribe payload")
            };
            let Some(SubscribeTarget::AtcCapacity(atc)) = sub.target else {
                panic!("expected AtcCapacity target")
            };
            assert_eq!(atc.flows, flows);
        }

        #[test]
        fn subscribe_to_h2h_capacity_includes_requested_flows() {
            // Given a fresh recording transport and a single hub-to-hub capacity flow to request.
            let mut transport = FakeTransport::default();
            let flows = vec![CapacityFlow {
                delivery_area: 40,
                connected_delivery_areas: vec![],
            }];

            // When we subscribe to H2H (hub-to-hub) capacity for that flow — parameterized like the
            // ATC subscription.
            subscribe_to_h2h_capacity(&mut transport, flows.clone()).unwrap();

            // Then the Subscribe frame carries the H2hCapacity target with exactly the flow we
            // asked for, confirming the parameters reach the wire intact.
            let frames = transport.sent_frames();
            let Some(ClientPayload::Subscribe(sub)) = frames[0].payload.clone() else {
                panic!("expected Subscribe payload")
            };
            let Some(SubscribeTarget::H2hCapacity(h2h)) = sub.target else {
                panic!("expected H2hCapacity target")
            };
            assert_eq!(h2h.flows, flows);
        }

        #[test]
        fn subscribe_to_public_statistics_includes_requested_areas() {
            // Given a fresh recording transport, nothing sent yet.
            let mut transport = FakeTransport::default();

            // When we subscribe to public statistics for delivery areas 10 and 11 — this
            // subscription is parameterized by the list of areas the caller wants stats for.
            subscribe_to_public_statistics(&mut transport, vec![10, 11]).unwrap();

            // Then the Subscribe frame carries the PublicStatistic target listing exactly those two
            // areas, confirming the requested areas are encoded into the message.
            let frames = transport.sent_frames();
            let Some(ClientPayload::Subscribe(sub)) = frames[0].payload.clone() else {
                panic!("expected Subscribe payload")
            };
            let Some(SubscribeTarget::PublicStatistic(ps)) = sub.target else {
                panic!("expected PublicStatistic target")
            };
            assert_eq!(ps.delivery_areas, vec![10, 11]);
        }

        #[test]
        fn subscribe_to_localview_includes_requested_areas() {
            // Given a fresh recording transport, nothing sent yet.
            let mut transport = FakeTransport::default();

            // When we subscribe to the LocalView order-book feed for delivery areas 10 and 11 — the
            // areas whose books we want reconstructed are passed as parameters.
            subscribe_to_localview(&mut transport, vec![10, 11]).unwrap();

            // Then the Subscribe frame carries the LocalView target listing exactly those two
            // areas, confirming the requested areas drive the wire message.
            let frames = transport.sent_frames();
            let Some(ClientPayload::Subscribe(sub)) = frames[0].payload.clone() else {
                panic!("expected Subscribe payload")
            };
            let Some(SubscribeTarget::LocalView(lv)) = sub.target else {
                panic!("expected LocalView target")
            };
            assert_eq!(lv.delivery_areas, vec![10, 11]);
        }

        #[test]
        fn refresh_token_fetches_and_sends() {
            // Given a recording transport, a token source primed to hand back "new-token", and a
            // currently-held token "old" that we expect to be replaced.
            let mut transport = FakeTransport::default();
            let source = FakeTokenSource {
                next: "new-token".into(),
            };
            let mut token = Token {
                access_token: "old".into(),
                expires_at: Instant::now() + Duration::from_secs(10),
            };

            // When we run refresh_token, which fetches a new token from the source and pushes it to
            // the server as a TokenRefresh so the session keeps its credentials current.
            refresh_token(&mut transport, &source, &mut token).unwrap();

            // Then the in-place token is swapped to the freshly fetched one, and...
            assert_eq!(token.access_token, "new-token");
            let frames = transport.sent_frames();
            let Some(ClientPayload::TokenRefresh(tr)) = frames[0].payload.clone() else {
                panic!("expected TokenRefresh payload")
            };
            assert_eq!(tr.token, "new-token");
        }
    }

    /// The `wait_for_connected` handshake: its success, error, unexpected-frame, and timeout paths.
    mod handshake {
        use super::*;

        #[test]
        fn wait_for_connected_returns_the_announced_heartbeat_interval() {
            let mut transport = FakeTransport::default();
            transport.push_incoming(ServerPayload::Connected(ConnectedPayload {
                heartbeat_interval_ms: 5000,
            }));
            let interval = wait_for_connected(&mut transport, Duration::from_secs(1))
                .expect("should return Ok");
            assert_eq!(interval, Duration::from_millis(5000));
        }

        #[test]
        fn wait_for_connected_rejects_a_connected_frame_without_heartbeat_interval() {
            // Given a handshake whose Connected frame announces no heartbeat interval — the
            // silence watchdog would have nothing to scale from, so the session must be refused
            // rather than run unguarded.
            let mut transport = FakeTransport::default();
            transport.push_incoming(ServerPayload::Connected(ConnectedPayload {
                heartbeat_interval_ms: 0,
            }));

            // When we wait for the handshake to complete.
            let err = wait_for_connected(&mut transport, Duration::from_secs(1)).unwrap_err();

            // Then it fails with MissingHeartbeatInterval, classified recoverable so the
            // connector retries with backoff instead of giving up.
            err.downcast_ref::<MissingHeartbeatInterval>()
                .expect("expected MissingHeartbeatInterval");
            assert!(is_recoverable(&err), "should reconnect, not give up");
        }

        #[test]
        fn wait_for_connected_bubbles_server_error() {
            // Given a transport whose first handshake frame is a server Error ("nope") rather than
            // the expected Connected.
            let mut transport = FakeTransport::default();
            transport.push_incoming(ServerPayload::Error(ErrorPayload {
                message: "nope".into(),
            }));

            // When we wait for the handshake to complete — it reads that Error during the connect
            // phase instead of a Connected frame.
            let err = wait_for_connected(&mut transport, Duration::from_secs(1)).unwrap_err();

            // Then the server's error is surfaced as a typed ServerError carrying its message,
            // so the caller sees the rejection reason rather than a generic failure.
            let server = err
                .downcast_ref::<ServerError>()
                .expect("expected ServerError");
            assert_eq!(server.message, "nope");
        }

        #[test]
        fn wait_for_connected_errors_on_unexpected_frame() {
            // Given a transport whose first handshake frame is a Heartbeat — a valid frame, but not
            // the Connected the handshake protocol requires first.
            let mut transport = FakeTransport::default();
            transport.push_incoming(ServerPayload::Heartbeat(Heartbeat {}));

            // When we wait for the handshake — it receives the out-of-order Heartbeat before any
            // Connected frame.
            let err = wait_for_connected(&mut transport, Duration::from_secs(1)).unwrap_err();

            // Then it fails with UnexpectedHandshakeFrame naming the offending frame kind, and that
            // error is classified recoverable so the connector reconnects rather than giving up.
            let unexpected = err
                .downcast_ref::<UnexpectedHandshakeFrame>()
                .expect("expected UnexpectedHandshakeFrame");
            assert_eq!(unexpected.kind, "Heartbeat");
            assert!(
                is_recoverable(&err),
                "handshake frame error should reconnect"
            );
        }

        #[test]
        fn wait_for_connected_times_out_when_no_connected_arrives() {
            // Given an empty transport: no Connected ever arrives, so the deadline trips.
            let mut transport = FakeTransport::default();
            let timeout = Duration::from_millis(50);
            let started = Instant::now();

            // When we wait for the handshake against that silent transport — recv keeps returning
            // nothing until the connected-timeout deadline is reached.
            let err = wait_for_connected(&mut transport, timeout).unwrap_err();

            // Then it fails with ConnectedTimeout reporting the configured timeout, and only after
            // at least that much wall-clock elapsed — it actually waited out the deadline rather
            // than bailing early.
            let elapsed = started.elapsed();
            let ct = err
                .downcast_ref::<ConnectedTimeout>()
                .expect("expected ConnectedTimeout");
            assert_eq!(ct.0, timeout);
            assert!(elapsed >= timeout, "exited before deadline: {elapsed:?}");
        }
    }

    /// The `run` read loop for a single connected session: metrics, sink forwarding, in-session
    /// token refresh, and the post-connect subscribe.
    mod session {
        use super::*;

        #[test]
        fn run_counts_heartbeats_and_frames_per_stream() {
            use metrics_util::debugging::{DebugValue, DebuggingRecorder};

            // Given a session that delivers one heartbeat and one LocalView before erroring out.
            let transport = FakeTransport::default();
            transport.push_incoming(ServerPayload::Connected(ConnectedPayload {
                heartbeat_interval_ms: 5000,
            }));
            transport.push_incoming(ServerPayload::Heartbeat(Heartbeat {}));
            let local_view_payload = ServerPayload::LocalView(LocalViewPayload {
                is_snapshot: true,
                delivery_area_id: 40,
                local_views: vec![],
            });
            let frame_len = ServerFrame {
                payload: Some(local_view_payload.clone()),
            }
            .encode_to_vec()
            .len() as u64;
            transport.push_incoming(local_view_payload);
            transport.push_incoming(ServerPayload::Error(ErrorPayload {
                message: "stop".into(),
            }));
            let recorder = DebuggingRecorder::new();
            let snapshotter = recorder.snapshotter();

            // When the loop runs to completion with the inspectable recorder installed.
            metrics::with_local_recorder(&recorder, || {
                let mut transport = transport.clone();
                let mut sink = FakeSink::default();
                let mut token = fresh_token();
                run(
                    &mut transport,
                    &FakeTokenSource { next: "x".into() },
                    &mut token,
                    &mut sink,
                )
                .unwrap_err();
            });

            // Then the heartbeat and the stream-tagged frame counters were recorded.
            let snapshot = snapshotter.snapshot().into_vec();
            let counter = |name: &str, want_stream: Option<&str>| {
                let (key, _, _, value) = snapshot
                    .iter()
                    .find(|(key, ..)| key.key().name() == name)
                    .unwrap_or_else(|| panic!("counter {name} not recorded"));
                if let Some(stream) = want_stream {
                    assert!(
                        key.key()
                            .labels()
                            .any(|l| l.key() == "stream" && l.value() == stream),
                        "missing stream:{stream} label on {name}"
                    );
                }
                match value {
                    DebugValue::Counter(v) => *v,
                    other => panic!("expected counter for {name}, got {other:?}"),
                }
            };
            assert_eq!(counter("nordpool.heartbeats", None), 1);
            assert_eq!(counter("nordpool.frames", Some("LocalView")), 1);
            assert_eq!(
                counter("nordpool.frame_bytes", Some("LocalView")),
                frame_len
            );
        }

        #[test]
        fn run_forwards_localview_bytes_to_sink_and_exits_on_server_error() {
            // Given a session that hands back Connected, then one LocalView snapshot frame (a single
            // book), then a fatal server Error to end the loop; and a fake sink recording the exact
            // bytes it is handed so we can check they are the verbatim wire frame.
            let transport = FakeTransport::default();
            let sink = FakeSink::default();
            transport.push_incoming(ServerPayload::Connected(ConnectedPayload {
                heartbeat_interval_ms: 5000,
            }));
            let local_view_payload = ServerPayload::LocalView(LocalViewPayload {
                is_snapshot: true,
                delivery_area_id: 40,
                local_views: vec![LocalView {
                    market_type: 1,
                    contract_id: 4242,
                    revision_no: 7,
                    buy_orders: vec![],
                    sell_orders: vec![],
                }],
            });
            let expected_bytes = ServerFrame {
                payload: Some(local_view_payload.clone()),
            }
            .encode_to_vec();
            transport.push_incoming(local_view_payload);
            transport.push_incoming(ServerPayload::Error(ErrorPayload {
                message: "stop".into(),
            }));

            // When — run drives the loop until the queued Error frame terminates it. The
            // inner block scopes the run-owned transport clone so its Drop fires (setting
            // `closed`) before the assertions below.
            let err = {
                let mut transport_attempt = transport.clone();
                let mut sink_attempt = sink.clone();
                let mut token = fresh_token();
                run(
                    &mut transport_attempt,
                    &FakeTokenSource { next: "x".into() },
                    &mut token,
                    &mut sink_attempt,
                )
                .unwrap_err()
            };

            // Then the loop terminated on the server Error (surfaced as ServerError "stop"), it had
            // first sent its post-connect Subscribe, the sink received exactly the one LocalView
            // frame's verbatim bytes, and the dropped transport clone marked itself closed.
            let server = err
                .downcast_ref::<ServerError>()
                .expect("expected ServerError");
            assert_eq!(server.message, "stop");

            let sent = transport.sent_frames();
            assert!(matches!(sent[0].payload, Some(ClientPayload::Subscribe(_))));

            let received = sink.received.lock().unwrap();
            assert_eq!(received.len(), 1);
            assert_eq!(received[0], expected_bytes);

            assert!(*transport.closed.lock().unwrap());
        }

        #[test]
        fn run_refreshes_token_when_it_needs_refresh() {
            // Given a connected session (Connected, then a Heartbeat to drive one loop pass, then a
            // fatal Error to stop) and an already-expired token, so the post-recv refresh check
            // fires on the first iteration; the source is primed to return "new-token".
            let mut transport = FakeTransport::default();
            transport.push_incoming(ServerPayload::Connected(ConnectedPayload {
                heartbeat_interval_ms: 5000,
            }));
            // Heartbeat lets the loop run one iteration so the post-recv token check fires.
            transport.push_incoming(ServerPayload::Heartbeat(Heartbeat {}));
            transport.push_incoming(ServerPayload::Error(ErrorPayload {
                message: "done".into(),
            }));
            let mut sink = FakeSink::default();
            // Token already past its expiry — needs_refresh() returns true on first check.
            let mut token = Token {
                access_token: "old-token".into(),
                expires_at: Instant::now() - Duration::from_secs(1),
            };
            let source = FakeTokenSource {
                next: "new-token".into(),
            };

            // When the read loop runs — during the session it notices the token needs refreshing
            // and refreshes mid-stream rather than waiting for a reconnect.
            let _ = run(&mut transport, &source, &mut token, &mut sink);

            // Then a TokenRefresh frame carrying the new token was sent to the server, and the
            // caller's token was updated in place to match.
            let sent = transport.sent_frames();
            let refresh = sent
                .iter()
                .find_map(|f| match &f.payload {
                    Some(ClientPayload::TokenRefresh(tr)) => Some(tr),
                    _ => None,
                })
                .expect("expected a TokenRefresh frame");
            assert_eq!(refresh.token, "new-token");
            // The caller's token should have been updated in place.
            assert_eq!(token.access_token, "new-token");
        }

        #[test]
        fn run_subscribes_to_localview_after_connected() {
            // Given a session that completes the handshake (Connected) then immediately errors, so
            // the loop sends its post-connect subscriptions and stops before processing data.
            let mut transport = FakeTransport::default();
            transport.push_incoming(ServerPayload::Connected(ConnectedPayload {
                heartbeat_interval_ms: 5000,
            }));
            // Terminate the loop right after the subscribe is sent.
            transport.push_incoming(ServerPayload::Error(ErrorPayload {
                message: "done".into(),
            }));
            let mut sink = FakeSink::default();
            let mut token = fresh_token();

            // When the read loop runs — after Connected it issues subscribe_all, the first of which
            // is the LocalView subscription for the connector's configured delivery area.
            let _ = run(
                &mut transport,
                &FakeTokenSource { next: "x".into() },
                &mut token,
                &mut sink,
            );

            // Then the very first frame sent is a Subscribe with a LocalView target scoped to the
            // configured delivery area, confirming the connector self-subscribes its one area on
            // connect.
            let sent = transport.sent_frames();
            let Some(ClientPayload::Subscribe(sub)) = sent[0].payload.clone() else {
                panic!(
                    "expected first sent frame to be Subscribe, got {:?}",
                    sent[0].payload
                );
            };
            let Some(SubscribeTarget::LocalView(lv)) = sub.target else {
                panic!("expected LocalView target, got {:?}", sub.target);
            };
            assert_eq!(
                lv.delivery_areas,
                crate::config::get()
                    .connector
                    .delivery_area_eics
                    .iter()
                    .map(|area| area.id())
                    .collect::<Vec<_>>()
            );
        }
    }

    /// Error classification driving the reconnect loop: the one taxonomy behind both the retry
    /// decision and the reconnect counter's label.
    mod error_classification {
        use super::super::errors::{Reason, classify};
        use super::*;

        fn retry(label: &'static str) -> Reason {
            Reason {
                label,
                recoverable: true,
            }
        }
        fn stop(label: &'static str) -> Reason {
            Reason {
                label,
                recoverable: false,
            }
        }

        #[test]
        fn classify_pairs_every_error_with_its_label_and_its_fate() {
            // Given one error of every kind the driver surfaces, each paired with the label the
            // reconnect counter should carry and whether the loop should retry it: transient
            // socket drops, auth blips and handshake hiccups reconnect; a message type we don't
            // speak, a server-side rejection, and anything unrecognized stop the loop.
            let cases: Vec<(anyhow::Error, Reason)> = vec![
                (
                    TransportError::Closed {
                        code: None,
                        reason: String::new(),
                    }
                    .into(),
                    retry("peer_closed"),
                ),
                (
                    TransportError::Stale(Duration::from_secs(21)).into(),
                    retry("stale"),
                ),
                (TransportError::Ws("boom".into()).into(), retry("websocket")),
                (TransportError::Io("eof".into()).into(), retry("io")),
                (
                    TransportError::Handshake("dns".into()).into(),
                    retry("handshake"),
                ),
                (
                    TransportError::UnexpectedMessageType.into(),
                    stop("unexpected_message_type"),
                ),
                (AuthError::Http("sso down".into()).into(), retry("auth")),
                (AuthError::Body("bad json".into()).into(), retry("auth")),
                (
                    ConnectedTimeout(Duration::from_secs(15)).into(),
                    retry("connected_timeout"),
                ),
                (
                    UnexpectedHandshakeFrame { kind: "Heartbeat" }.into(),
                    retry("unexpected_handshake_frame"),
                ),
                (
                    MissingHeartbeatInterval(0).into(),
                    retry("missing_heartbeat_interval"),
                ),
                (
                    ServerSilent {
                        silent_for: Duration::from_secs(25),
                        heartbeat_interval: Duration::from_secs(5),
                    }
                    .into(),
                    retry("server_silent"),
                ),
                (
                    ServerError {
                        message: "no".into(),
                    }
                    .into(),
                    stop("other"),
                ),
                (anyhow::anyhow!("mystery"), stop("other")),
            ];

            // When / Then each error classifies to exactly that pair, and both accessors report
            // it — one taxonomy, so an error can never be labelled without also being given a
            // fate, nor the two drift apart.
            for (err, expected) in cases {
                assert_eq!(classify(&err), expected, "for {err:#}");
                assert_eq!(reason_label(&err), expected.label, "for {err:#}");
                assert_eq!(is_recoverable(&err), expected.recoverable, "for {err:#}");
            }
        }
    }

    /// The `run_with_reconnect` wrapper: fatal-error propagation and retry-after-recoverable.
    mod reconnect {
        use super::*;

        #[test]
        fn run_with_reconnect_bails_immediately_on_fatal_run_error() {
            // Given a connect that always succeeds and a session whose run ends in a fatal server
            // Error ("stop"), with a counter tracking how many times connect is invoked.
            let transport = FakeTransport::default();
            transport.push_incoming(ServerPayload::Connected(ConnectedPayload {
                heartbeat_interval_ms: 5000,
            }));
            transport.push_incoming(ServerPayload::Error(ErrorPayload {
                message: "stop".into(),
            }));
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_c = attempts.clone();
            let transport_c = transport.clone();
            let connect = move || -> Result<(FakeTransport, Token)> {
                attempts_c.fetch_add(1, Ordering::SeqCst);
                Ok((transport_c.clone(), fresh_token()))
            };

            // When we run the reconnect wrapper, whose job is to retry recoverable failures but
            // propagate fatal ones — here the session fails fatally.
            let err = run_with_reconnect(
                connect,
                &FakeTokenSource { next: "x".into() },
                FakeSink::default(),
            )
            .unwrap_err();

            // Then the fatal ServerError propagates out unchanged and connect ran only once — the
            // wrapper did not retry a non-recoverable error.
            assert_eq!(err.downcast_ref::<ServerError>().unwrap().message, "stop");
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
        }

        #[test]
        fn run_with_reconnect_propagates_fatal_connect_error() {
            // Given a connect that fails with an unclassified ("mystery") error, which is not
            // recoverable, and a counter on how often it is called.
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_c = attempts.clone();
            let connect = move || -> Result<(FakeTransport, Token)> {
                attempts_c.fetch_add(1, Ordering::SeqCst);
                Err(anyhow::anyhow!("mystery"))
            };

            // When we run the reconnect wrapper — the failure happens in connect itself, before any
            // session, and is not a recoverable category.
            let err = run_with_reconnect(
                connect,
                &FakeTokenSource { next: "x".into() },
                FakeSink::default(),
            )
            .unwrap_err();

            // Then the error propagates verbatim and connect ran only once — an unknown connect
            // error is fatal, not retried.
            assert_eq!(err.to_string(), "mystery");
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
        }

        #[test]
        fn run_with_reconnect_retries_after_recoverable_transport_error() {
            // Given two attempts staged behind the connect closure:
            // Attempt 1: connect succeeds, transport recv returns a peer-Close → recoverable,
            // wrapper backs off and tries again.
            let transport_1 = FakeTransport::default();
            transport_1.push_incoming(ServerPayload::Connected(ConnectedPayload {
                heartbeat_interval_ms: 5000,
            }));
            transport_1.push_recv_error(TransportError::Closed {
                code: Some(1000),
                reason: "bye".into(),
            });
            // Attempt 2: connect succeeds, transport returns a fatal server Error.
            let transport_2 = FakeTransport::default();
            transport_2.push_incoming(ServerPayload::Connected(ConnectedPayload {
                heartbeat_interval_ms: 5000,
            }));
            transport_2.push_incoming(ServerPayload::Error(ErrorPayload {
                message: "stop".into(),
            }));
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_c = attempts.clone();
            let connect = move || -> Result<(FakeTransport, Token)> {
                let n = attempts_c.fetch_add(1, Ordering::SeqCst);
                let t = if n == 0 {
                    transport_1.clone()
                } else {
                    transport_2.clone()
                };
                Ok((t, fresh_token()))
            };

            // When we run the reconnect wrapper across both attempts — the first session's recv
            // error is recoverable so the wrapper backs off and reconnects.
            let err = run_with_reconnect(
                connect,
                &FakeTokenSource { next: "x".into() },
                FakeSink::default(),
            )
            .unwrap_err();

            // Then it stopped on the second attempt's fatal ServerError, having called connect
            // twice — proving the recoverable transport error triggered exactly one reconnect.
            assert_eq!(err.downcast_ref::<ServerError>().unwrap().message, "stop");
            assert_eq!(attempts.load(Ordering::SeqCst), 2);
        }

        #[test]
        fn run_with_reconnect_retries_after_recoverable_connect_error() {
            // Given a connect that fails recoverably the first time (an AuthError, as if SSO
            // blipped) and succeeds the second time into a session that ends in a fatal Error —
            // exercising a recoverable failure during connect rather than mid-session.
            let transport = FakeTransport::default();
            transport.push_incoming(ServerPayload::Connected(ConnectedPayload {
                heartbeat_interval_ms: 5000,
            }));
            transport.push_incoming(ServerPayload::Error(ErrorPayload {
                message: "stop".into(),
            }));
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_c = attempts.clone();
            let connect = move || -> Result<(FakeTransport, Token)> {
                let n = attempts_c.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    // Recoverable: AuthError feeds same backoff.
                    Err(AuthError::Http("sso blip".into()).into())
                } else {
                    Ok((transport.clone(), fresh_token()))
                }
            };

            // When we run the reconnect wrapper — the first connect error is recoverable, so it
            // backs off and tries connecting again.
            let err = run_with_reconnect(
                connect,
                &FakeTokenSource { next: "x".into() },
                FakeSink::default(),
            )
            .unwrap_err();

            // Then it stopped on the second attempt's fatal ServerError after two connect calls,
            // confirming a recoverable connect error is retried just like a recoverable session one.
            assert_eq!(err.downcast_ref::<ServerError>().unwrap().message, "stop");
            assert_eq!(attempts.load(Ordering::SeqCst), 2);
        }
    }

    /// The reconnect loop's handling of the scheduled-reconnect `SessionExpired` signal. The
    /// deadline math itself is unit-tested in the `reconnect` submodule.
    mod session_reconnect {
        use super::*;

        #[test]
        fn run_with_reconnect_reconnects_on_session_expired_then_continues() {
            // Given attempt 1 ends with the scheduled-reconnect signal (here surfaced from the
            // connect closure), and attempt 2 hits a fatal server error to stop the loop.
            let transport = FakeTransport::default();
            transport.push_incoming(ServerPayload::Connected(ConnectedPayload {
                heartbeat_interval_ms: 5000,
            }));
            transport.push_incoming(ServerPayload::Error(ErrorPayload {
                message: "stop".into(),
            }));
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_c = attempts.clone();
            let connect = move || -> Result<(FakeTransport, Token)> {
                let n = attempts_c.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    Err(SessionExpired.into())
                } else {
                    Ok((transport.clone(), fresh_token()))
                }
            };

            // When we run the reconnect wrapper — the first attempt returns the SessionExpired
            // sentinel, the deliberate scheduled-recycle signal that should reconnect immediately.
            let err = run_with_reconnect(
                connect,
                &FakeTokenSource { next: "x".into() },
                FakeSink::default(),
            )
            .unwrap_err();

            // Then SessionExpired was treated as a reconnect (not fatal): a second attempt ran.
            assert_eq!(err.downcast_ref::<ServerError>().unwrap().message, "stop");
            assert_eq!(attempts.load(Ordering::SeqCst), 2);
        }
    }

    /// The silence watchdog: a session with no server frame for `heartbeat_timeout_factor`
    /// announced heartbeat intervals is recycled; any frame resets the clock.
    mod silence {
        use super::*;

        #[test]
        fn run_returns_server_silent_when_no_frame_arrives_within_the_limit() {
            // Given a session that completes the handshake announcing a 10ms heartbeat interval
            // (so the default factor 5 puts the silence limit at 50ms), then goes completely
            // silent — recv keeps returning None, but the socket never errors. This is the
            // dead-feed case WS ping/pong cannot see: the peer's socket is alive, its
            // application is not.
            let mut transport = FakeTransport::default();
            transport.push_incoming(ServerPayload::Connected(ConnectedPayload {
                heartbeat_interval_ms: 10,
            }));
            let mut sink = FakeSink::default();
            let mut token = fresh_token();
            let started = Instant::now();

            // When the read loop runs against the silent transport.
            let err = run(
                &mut transport,
                &FakeTokenSource { next: "x".into() },
                &mut token,
                &mut sink,
            )
            .unwrap_err();

            // Then it returns ServerSilent only after the limit elapsed, the error reports at
            // least that much silence, and it is classified recoverable so the reconnect loop
            // recycles the session instead of giving up.
            let limit = Duration::from_millis(50);
            assert!(
                started.elapsed() >= limit,
                "tripped before the limit: {:?}",
                started.elapsed()
            );
            let silent = err
                .downcast_ref::<ServerSilent>()
                .expect("expected ServerSilent");
            assert!(silent.silent_for >= limit, "{:?}", silent.silent_for);
            assert_eq!(silent.heartbeat_interval, Duration::from_millis(10));
            assert!(is_recoverable(&err), "silence should drive a reconnect");
        }

        /// Hands back Connected (announcing a 40ms heartbeat interval ⇒ 200ms silence limit),
        /// then a Heartbeat every 60ms — each inside the limit but past it cumulatively — and
        /// finally a peer Close at ~240ms.
        struct HeartbeatsThenClose {
            started: Option<Instant>,
            emitted: u32,
        }

        impl Transport for HeartbeatsThenClose {
            fn recv(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
                let started = *self.started.get_or_insert_with(Instant::now);
                if self.emitted == 0 {
                    self.emitted = 1;
                    let frame = ServerFrame {
                        payload: Some(ServerPayload::Connected(ConnectedPayload {
                            heartbeat_interval_ms: 40,
                        })),
                    };
                    return Ok(Some(frame.encode_to_vec()));
                }
                if started.elapsed() < Duration::from_millis(60) * self.emitted {
                    return Ok(None);
                }
                if self.emitted <= 3 {
                    self.emitted += 1;
                    let frame = ServerFrame {
                        payload: Some(ServerPayload::Heartbeat(Heartbeat {})),
                    };
                    Ok(Some(frame.encode_to_vec()))
                } else {
                    Err(TransportError::Closed {
                        code: None,
                        reason: "done".into(),
                    })
                }
            }
            fn send(&mut self, _bytes: Vec<u8>) -> Result<(), TransportError> {
                Ok(())
            }
        }

        #[test]
        fn any_frame_resets_the_silence_clock() {
            // Given a session whose heartbeats arrive every 60ms against a 200ms silence limit:
            // the session runs ~240ms total, past the limit, but no single gap exceeds it.
            let mut transport = HeartbeatsThenClose {
                started: None,
                emitted: 0,
            };
            let mut sink = FakeSink::default();
            let mut token = fresh_token();

            // When the read loop runs to the transport's eventual peer Close.
            let err = run(
                &mut transport,
                &FakeTokenSource { next: "x".into() },
                &mut token,
                &mut sink,
            )
            .unwrap_err();

            // Then the session ended on the Close, not on ServerSilent — each heartbeat reset
            // the watchdog, so cumulative session time never counts as silence.
            let te = err
                .downcast_ref::<TransportError>()
                .expect("expected the peer Close, not a watchdog trip");
            assert!(matches!(te, TransportError::Closed { .. }), "{te:?}");
        }
    }

    /// Graceful shutdown winning over every other path: classification, handshake, mid-session,
    /// and the reconnect loop.
    mod shutdown {
        use super::*;

        #[test]
        fn shutdown_requested_is_not_recoverable() {
            // So the reconnect classifier propagates it (stop) instead of mapping it to Ok (retry).
            assert!(!is_recoverable(&ShutdownRequested.into()));
        }

        #[test]
        fn wait_for_connected_stops_when_shutdown_requested() {
            let _reset = ShutdownOnDrop;
            // Given a shutdown already requested and a transport with no Connected frame.
            let mut transport = FakeTransport::default();
            request_shutdown();

            // When / Then the handshake loop exits at once with the sentinel, not a timeout.
            let err = wait_for_connected(&mut transport, Duration::from_secs(1)).unwrap_err();
            assert!(err.downcast_ref::<ShutdownRequested>().is_some());
        }

        #[test]
        fn run_stops_when_shutdown_requested_mid_session() {
            let _reset = ShutdownOnDrop;
            // Given a connected session whose transport requests shutdown once its frames drain.
            let mut transport = FakeTransport::default();
            transport.push_incoming(ServerPayload::Connected(ConnectedPayload {
                heartbeat_interval_ms: 5000,
            }));
            transport.shutdown_when_drained();
            let mut sink = FakeSink::default();
            let mut token = fresh_token();

            // When — the read loop exits on its next shutdown check.
            let err = run(
                &mut transport,
                &FakeTokenSource { next: "x".into() },
                &mut token,
                &mut sink,
            )
            .unwrap_err();

            // Then run returns the ShutdownRequested sentinel rather than running until the
            // transport errors — the shutdown flag wins over normal session continuation.
            assert!(err.downcast_ref::<ShutdownRequested>().is_some());
        }

        #[test]
        fn run_with_reconnect_stops_on_shutdown_without_retrying() {
            let _reset = ShutdownOnDrop;
            // Given a connect that requests shutdown and then fails recoverably: the shutdown arm
            // must win over the is_recoverable arm, so the loop stops instead of reconnecting.
            let attempts = Arc::new(AtomicUsize::new(0));
            let attempts_c = attempts.clone();
            let connect = move || -> Result<(FakeTransport, Token)> {
                attempts_c.fetch_add(1, Ordering::SeqCst);
                request_shutdown();
                Err(AuthError::Http("blip".into()).into())
            };

            // When we run the reconnect wrapper — connect both flags shutdown and returns a
            // recoverable error, so both the shutdown arm and the is_recoverable arm could match.
            let err = run_with_reconnect(
                connect,
                &FakeTokenSource { next: "x".into() },
                FakeSink::default(),
            )
            .unwrap_err();

            // Then it stopped with ShutdownRequested after a single attempt.
            assert!(err.downcast_ref::<ShutdownRequested>().is_some());
            assert_eq!(attempts.load(Ordering::SeqCst), 1);
        }
    }

    /// The generic `retry_with_backoff` helper and its `Backoff` policy.
    mod retry {
        use std::cell::Cell;
        use std::time::Duration;

        use crate::driver::retry::{Backoff, retry_with_backoff};

        fn backoff(initial_ms: u64, max_ms: u64, reset_after_ms: u64) -> Backoff {
            Backoff {
                initial: Duration::from_millis(initial_ms),
                max: Duration::from_millis(max_ms),
                reset_after: Duration::from_millis(reset_after_ms),
            }
        }

        #[test]
        fn propagates_error_from_first_run_without_sleeping() {
            // Given an attempt that errors on its first run.
            let calls = Cell::new(0);
            let mut sleeps = Vec::new();

            // When we drive that attempt through retry_with_backoff (recording sleeps instead of
            // actually sleeping) — the loop only retries on Ok, never on Err.
            let result = retry_with_backoff(
                &backoff(1, 8, 1_000),
                |d| sleeps.push(d),
                || {
                    calls.set(calls.get() + 1);
                    Err(anyhow::anyhow!("fatal"))
                },
            );

            // Then it stops at once, with no backoff.
            assert_eq!(result.unwrap_err().to_string(), "fatal");
            assert_eq!(calls.get(), 1);
            assert!(sleeps.is_empty());
        }

        #[test]
        fn retries_while_ok_then_propagates_error() {
            // Given an attempt that asks to retry (Ok) twice, then errors.
            let calls = Cell::new(0);
            let mut sleeps = Vec::new();

            // When we drive that attempt through retry_with_backoff (recording sleeps) — each Ok
            // means "keep going", so the loop retries until the attempt finally returns Err.
            let result = retry_with_backoff(
                &backoff(1, 8, 1_000),
                |d| sleeps.push(d),
                || {
                    calls.set(calls.get() + 1);
                    if calls.get() <= 2 {
                        Ok(())
                    } else {
                        Err(anyhow::anyhow!("stop"))
                    }
                },
            );

            // Then it ran once per Ok plus the failing run, sleeping after each Ok.
            assert_eq!(result.unwrap_err().to_string(), "stop");
            assert_eq!(calls.get(), 3);
            assert_eq!(sleeps.len(), 2);
        }

        #[test]
        fn backoff_doubles_each_retry_and_caps_at_max() {
            // Given reset_after so large no run counts as healthy, so the wait only grows.
            let calls = Cell::new(0);
            let mut sleeps = Vec::new();

            // When five retries (Ok) occur before an error stops the loop.
            let result = retry_with_backoff(
                &backoff(1, 8, 3_600_000),
                |d| sleeps.push(d),
                || {
                    calls.set(calls.get() + 1);
                    if calls.get() <= 5 {
                        Ok(())
                    } else {
                        Err(anyhow::anyhow!("stop"))
                    }
                },
            );

            // Then the waits double until clamped to max.
            assert!(result.is_err());
            let millis: Vec<u128> = sleeps.iter().map(Duration::as_millis).collect();
            assert_eq!(millis, vec![1, 2, 4, 8, 8]);
        }

        #[test]
        fn backoff_resets_to_initial_after_a_healthy_run() {
            // Given reset_after = 0, every run counts as "healthy", so the wait resets each time.
            let calls = Cell::new(0);
            let mut sleeps = Vec::new();

            // When three retries (Ok) occur before an error stops the loop.
            let result = retry_with_backoff(
                &backoff(2, 16, 0),
                |d| sleeps.push(d),
                || {
                    calls.set(calls.get() + 1);
                    if calls.get() <= 3 {
                        Ok(())
                    } else {
                        Err(anyhow::anyhow!("stop"))
                    }
                },
            );

            // Then the wait never grows past initial.
            assert!(result.is_err());
            let millis: Vec<u128> = sleeps.iter().map(Duration::as_millis).collect();
            assert_eq!(millis, vec![2, 2, 2]);
        }
    }
}
