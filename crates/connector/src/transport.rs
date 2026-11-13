use std::time::Instant;

use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

mod connect;
mod errors;
pub use connect::connect;
pub use errors::TransportError;
use errors::would_block;

use crate::config;

/// Abstraction over a binary message transport.
pub trait Transport {
    /// Read one binary application frame. Returns `None` on read timeout or a
    /// transport-level control frame (Ping/Pong) — the caller's loop is expected to
    /// call again. Returns `Err(TransportError::Closed { .. })` when the peer initiates
    /// a close, and `Err(TransportError::Stale(_))` if the peer has stopped answering
    /// our pings.
    fn recv(&mut self) -> Result<Option<Vec<u8>>, TransportError>;
    /// Send one binary application frame.
    fn send(&mut self, bytes: Vec<u8>) -> Result<(), TransportError>;
}

pub struct WsTransport {
    socket: WebSocket<MaybeTlsStream<std::net::TcpStream>>,
    /// When we last sent a Ping; `None` until the first one. Drives the send cadence.
    last_ping_sent: Option<Instant>,
    /// Whether that Ping is still awaiting its Pong. One Ping is in flight at a time; the
    /// connection is stale if it goes unanswered for `pong_timeout`. Any Pong clears it.
    awaiting_pong: bool,
}

impl WsTransport {
    pub(crate) fn new(socket: WebSocket<MaybeTlsStream<std::net::TcpStream>>) -> Self {
        Self {
            socket,
            last_ping_sent: None,
            awaiting_pong: false,
        }
    }

    pub fn recv(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        let message = self.read_message()?;
        self.keepalive()?;
        Ok(message)
    }

    fn read_message(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        match self.socket.read() {
            Ok(Message::Binary(b)) => {
                metrics::counter!("nordpool.websocket.binary_received").increment(1);
                Ok(Some(b))
            }
            Ok(Message::Close(frame)) => {
                let (code, reason) = match frame {
                    Some(f) => (Some(u16::from(f.code)), f.reason.to_string()),
                    None => (None, String::new()),
                };
                tracing::info!(?code, %reason, "peer sent Close");
                Err(TransportError::Closed { code, reason })
            }
            Ok(Message::Ping(_)) => {
                tracing::debug!("remote websocket ping (auto-pong queued by tungstenite)");
                Ok(None)
            }
            Ok(Message::Pong(_)) => {
                metrics::counter!("nordpool.websocket.pongs_received").increment(1);
                // Only our outstanding Ping yields an RTT sample; an unsolicited Pong
                // (allowed by the RFC) has no send time to measure against.
                if self.awaiting_pong
                    && let Some(sent) = self.last_ping_sent
                {
                    metrics::histogram!("nordpool.websocket.ping_rtt_ms")
                        .record(sent.elapsed().as_secs_f64() * 1000.0);
                }
                self.awaiting_pong = false;
                tracing::debug!("remote websocket pong");
                Ok(None)
            }
            Ok(_) => Err(TransportError::UnexpectedMessageType),
            Err(tungstenite::Error::Io(e)) if would_block(&e) => Ok(None),
            Err(e) => Err(TransportError::ws(e)),
        }
    }

    fn keepalive(&mut self) -> Result<(), TransportError> {
        if self.awaiting_pong {
            self.check_stale()
        } else {
            self.maybe_send_ping()
        }
    }

    fn maybe_send_ping(&mut self) -> Result<(), TransportError> {
        let cadence_due = self
            .last_ping_sent
            .is_none_or(|t| t.elapsed() >= config::get().connector.ping_every);
        if cadence_due {
            tracing::debug!("sending websocket ping");
            self.socket
                .send(Message::Ping(Vec::new()))
                .map_err(TransportError::ws)?;
            metrics::counter!("nordpool.websocket.pings_sent").increment(1);
            self.last_ping_sent = Some(Instant::now());
            self.awaiting_pong = true;
        }
        Ok(())
    }

    fn check_stale(&self) -> Result<(), TransportError> {
        let since_ping = self
            .last_ping_sent
            .expect("awaiting_pong is only set after a Ping is sent")
            .elapsed();
        if since_ping > config::get().connector.pong_timeout {
            return Err(TransportError::Stale(since_ping));
        }
        Ok(())
    }

    pub fn send(&mut self, bytes: Vec<u8>) -> Result<(), TransportError> {
        self.socket
            .send(Message::Binary(bytes))
            .map_err(TransportError::ws)
    }
}

impl Drop for WsTransport {
    /// Send a graceful WebSocket Close so the peer sees a proper closing handshake
    /// rather than a bare TCP FIN. Runs on normal scope exit and during panic
    /// unwinding. Any error from `close` is ignored — the connection is going away
    /// either way.
    fn drop(&mut self) {
        let _ = self.socket.close(None);
    }
}

impl Transport for WsTransport {
    fn recv(&mut self) -> Result<Option<Vec<u8>>, TransportError> {
        WsTransport::recv(self)
    }
    fn send(&mut self, bytes: Vec<u8>) -> Result<(), TransportError> {
        WsTransport::send(self, bytes)
    }
}

#[cfg(test)]
mod tests {
    use std::io::{Error as IoError, ErrorKind};
    use std::net::TcpListener;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::{Duration, Instant};

    use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    use rustls::{ClientConfig, RootCertStore, ServerConfig, ServerConnection, StreamOwned};
    use tungstenite::{Connector, Message};

    use crate::auth::Token;

    use super::*;

    #[test]
    fn would_block_matches_would_block_kind() {
        assert!(would_block(&IoError::from(ErrorKind::WouldBlock)));
    }

    #[test]
    fn would_block_matches_timed_out_kind() {
        assert!(would_block(&IoError::from(ErrorKind::TimedOut)));
    }

    #[test]
    fn would_block_rejects_other_kinds() {
        for kind in [
            ErrorKind::ConnectionReset,
            ErrorKind::ConnectionAborted,
            ErrorKind::Interrupted,
            ErrorKind::BrokenPipe,
            ErrorKind::UnexpectedEof,
        ] {
            assert!(
                !would_block(&IoError::from(kind)),
                "expected not would-block for {kind:?}"
            );
        }
    }

    #[test]
    fn transport_error_handshake_display() {
        assert_eq!(
            TransportError::Handshake("nope".into()).to_string(),
            "handshake failed: nope"
        );
    }

    #[test]
    fn transport_error_ws_display() {
        assert_eq!(
            TransportError::Ws("broken".into()).to_string(),
            "websocket error: broken"
        );
    }

    #[test]
    fn transport_error_io_display() {
        assert_eq!(
            TransportError::Io("eof".into()).to_string(),
            "io error: eof"
        );
    }

    #[test]
    fn transport_error_unexpected_message_type_display() {
        assert_eq!(
            TransportError::UnexpectedMessageType.to_string(),
            "unexpected message type"
        );
    }

    #[test]
    fn transport_error_stale_display() {
        assert_eq!(
            TransportError::Stale(Duration::from_secs(30)).to_string(),
            "connection stale: no pong for 30s"
        );
    }

    #[test]
    fn transport_error_closed_display_with_payload() {
        assert_eq!(
            TransportError::Closed {
                code: Some(1000),
                reason: "bye".into(),
            }
            .to_string(),
            "peer closed connection: code=Some(1000) reason=\"bye\""
        );
    }

    #[test]
    fn transport_error_closed_display_without_payload() {
        assert_eq!(
            TransportError::Closed {
                code: None,
                reason: String::new(),
            }
            .to_string(),
            "peer closed connection: code=None reason=\"\""
        );
    }

    /// Type of a tungstenite WebSocket sitting on top of our rustls server side. Spelled
    /// out so the test helper below can name it in its `FnOnce(ServerWs)` parameter.
    type ServerWs = tungstenite::WebSocket<StreamOwned<ServerConnection, std::net::TcpStream>>;

    /// Spin up a local wss:// server with a self-signed cert and connect to it using
    /// `connect_with_connector` with a trust store that accepts that cert. Returns the
    /// client-side `WsTransport` and the server thread's join handle. The caller
    /// supplies the server-side behavior as a closure receiving the accepted WebSocket.
    fn connect_to_local_wss<F>(server_fn: F) -> (WsTransport, std::thread::JoinHandle<()>)
    where
        F: FnOnce(ServerWs) + Send + 'static,
    {
        let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate self-signed cert");
        let cert_der: CertificateDer<'static> = cert.cert.der().clone();
        let key_der: PrivateKeyDer<'static> =
            PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()));

        // Bind 127.0.0.1 explicitly so "wss://localhost:port/..." resolves to the same
        // address regardless of how localhost is configured in /etc/hosts.
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind listener");
        let port = listener.local_addr().expect("local_addr").port();

        // Select the provider explicitly: both rustls backends are compiled in (ureq pulls
        // `ring`, our deps pull `aws_lc_rs`), so the bare `builder()` would panic.
        let server_config = Arc::new(
            ServerConfig::builder_with_provider(Arc::new(
                rustls::crypto::aws_lc_rs::default_provider(),
            ))
            .with_protocol_versions(rustls::DEFAULT_VERSIONS)
            .expect("server protocol versions")
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .expect("build server config"),
        );

        let server = std::thread::spawn(move || {
            let (tcp, _) = listener.accept().expect("accept");
            let conn = ServerConnection::new(server_config).expect("server conn");
            let stream = StreamOwned::new(conn, tcp);
            let ws = tungstenite::accept(stream).expect("ws accept");
            server_fn(ws);
        });

        let mut roots = RootCertStore::empty();
        roots.add(cert_der).expect("add cert to roots");
        let client_config = ClientConfig::builder_with_provider(Arc::new(
            rustls::crypto::aws_lc_rs::default_provider(),
        ))
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)
        .expect("client protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth();
        let connector = Connector::Rustls(Arc::new(client_config));

        let token = Token {
            access_token: "tok".into(),
            expires_at: Instant::now() + Duration::from_secs(60),
        };
        let url = url::Url::parse(&format!("wss://localhost:{port}/test")).expect("parse url");
        let transport = super::connect::connect_with_connector(&url, &token, connector)
            .expect("client connect");

        (transport, server)
    }

    /// End-to-end check: stand up a local wss:// echo server, drive
    /// `connect_with_connector` against it, and verify send/recv round-trips and that
    /// the received binary frame is counted.
    #[test]
    fn connect_send_recv_against_local_wss_server() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        // Given a real local wss:// server that echoes every binary frame back, and a local
        // metrics recorder so we can observe the binary-received counter the transport emits.
        let (mut transport, server) = connect_to_local_wss(|mut ws| {
            loop {
                match ws.read() {
                    Ok(Message::Binary(b)) => {
                        let _ = ws.send(Message::Binary(b));
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });
        let payload = b"hello".to_vec();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // When we send a binary frame and poll recv() for the echo — exercising the full
        // send + receive round-trip over TLS, the way the driver loop drives the transport.
        transport.send(payload.clone()).expect("client send");
        let echoed = metrics::with_local_recorder(&recorder, || {
            for _ in 0..5 {
                match transport.recv() {
                    Ok(Some(bytes)) => return Some(bytes),
                    Ok(None) => continue,
                    Err(e) => panic!("recv error: {e}"),
                }
            }
            None
        });

        // Then the bytes round-trip unchanged and the transport recorded exactly one
        // binary-received frame — proving both the data path and its instrumentation work.
        assert_eq!(echoed.as_deref(), Some(payload.as_slice()));
        let snapshot = snapshotter.snapshot().into_vec();
        let (_, _, _, value) = snapshot
            .iter()
            .find(|(key, ..)| key.key().name() == "nordpool.websocket.binary_received")
            .expect("binary counter not recorded");
        match value {
            DebugValue::Counter(v) => assert_eq!(*v, 1),
            other => panic!("expected counter, got {other:?}"),
        }

        drop(transport);
        server.join().expect("server thread");
    }

    /// When `recv` is called and the cadence is due, it sends a Ping (setting
    /// `awaiting_pong`). The server responds with a Pong, and a later `recv` clears
    /// `awaiting_pong`.
    #[test]
    fn pong_clears_outstanding_ping() {
        // Given a connected transport and a server that pongs every inbound ping, so the
        // keepalive round-trip can complete.
        let (mut transport, server) = connect_to_local_wss(|mut ws| {
            loop {
                match ws.read() {
                    Ok(Message::Ping(p)) => {
                        let _ = ws.send(Message::Pong(p));
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });
        // Force the next recv() to send a Ping by aging last_ping_sent past PING_EVERY.
        transport.last_ping_sent = Some(Instant::now() - Duration::from_secs(11));

        // When — recv() sends the Ping and the server's Pong is read back. Loop a few times
        // to absorb read-timeout slack (the pong may land in a later recv than the ping).
        for _ in 0..5 {
            let _ = transport.recv();
        }

        // Then — a fresh Ping was sent (last_ping_sent advanced to ~now) and it is no longer
        // outstanding, so the Pong must have arrived and cleared it.
        assert!(
            transport
                .last_ping_sent
                .is_some_and(|t| t.elapsed() < Duration::from_secs(5)),
            "expected a Ping to have been sent this round",
        );
        assert!(
            !transport.awaiting_pong,
            "expected the Pong to clear the outstanding ping",
        );

        drop(transport);
        server.join().expect("server thread");
    }

    /// A Pong answering our outstanding Ping records one round-trip sample in the
    /// `nordpool.websocket.ping_rtt_ms` histogram and bumps the pings-sent and
    /// pongs-received counters once each.
    #[test]
    fn pong_records_ping_rtt_sample() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        // Given — a server that pongs every ping.
        let (mut transport, server) = connect_to_local_wss(|mut ws| {
            loop {
                match ws.read() {
                    Ok(Message::Ping(p)) => {
                        let _ = ws.send(Message::Pong(p));
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });
        transport.last_ping_sent = Some(Instant::now() - Duration::from_secs(11));

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // When — recv() sends the Ping and the server's Pong is read back. Loop a few times
        // to absorb read-timeout slack (the pong may land in a later recv than the ping).
        metrics::with_local_recorder(&recorder, || {
            for _ in 0..5 {
                let _ = transport.recv();
                let ping_sent_this_round = transport
                    .last_ping_sent
                    .is_some_and(|t| t.elapsed() < Duration::from_secs(5));
                if !transport.awaiting_pong && ping_sent_this_round {
                    break;
                }
            }
        });

        // Then — exactly one RTT sample, and a plausible one (sub-second on loopback).
        let snapshot = snapshotter.snapshot().into_vec();
        let metric = |name: &str| {
            snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .map(|(_, _, _, value)| value)
                .unwrap_or_else(|| panic!("{name} not recorded"))
        };
        match metric("nordpool.websocket.ping_rtt_ms") {
            DebugValue::Histogram(samples) => {
                assert_eq!(samples.len(), 1);
                let rtt_ms = samples[0].into_inner();
                assert!(
                    (0.0..1000.0).contains(&rtt_ms),
                    "implausible loopback rtt: {rtt_ms}ms"
                );
            }
            other => panic!("expected histogram, got {other:?}"),
        }

        // And the one Ping out and one Pong back were each counted.
        for name in [
            "nordpool.websocket.pings_sent",
            "nordpool.websocket.pongs_received",
        ] {
            match metric(name) {
                DebugValue::Counter(v) => assert_eq!(*v, 1, "{name}"),
                other => panic!("expected counter for {name}, got {other:?}"),
            }
        }

        drop(transport);
        server.join().expect("server thread");
    }

    /// When a Ping has been outstanding longer than `PONG_TIMEOUT`, the next `recv()`
    /// returns `TransportError::Stale(_)` — overriding whatever the read result would
    /// have been. The clock starts at the ping send, not at the last pong receipt.
    #[test]
    fn recv_returns_stale_when_pong_timeout_elapsed() {
        // Given a connected transport whose state is hand-set to an unanswered ping sent 21s ago
        // (past the 20s pong_timeout); the server drains but never pongs, so the timeout stands.
        let (mut transport, server) = connect_to_local_wss(|mut ws| {
            loop {
                match ws.read() {
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });
        // Outstanding ping: we sent it 21s ago and never got a pong.
        transport.last_ping_sent = Some(Instant::now() - Duration::from_secs(21));
        transport.awaiting_pong = true;

        // When recv() is called — the keepalive check runs and finds the ping has gone unanswered
        // past pong_timeout, the liveness signal that the peer has stopped responding.
        let result = transport.recv();

        // Then recv() reports the connection as Stale (driving a reconnect upstream), and the
        // reported duration is at least the 21s the ping was outstanding — the clock runs from the
        // ping send, not the last pong.
        match result {
            Err(TransportError::Stale(d)) => {
                assert!(d >= Duration::from_secs(21), "stale duration: {d:?}");
            }
            other => panic!("expected Stale, got {other:?}"),
        }

        drop(transport);
        server.join().expect("server thread");
    }

    /// tungstenite auto-handles inbound Pings: reading a Ping returns it to the caller
    /// AND queues a Pong reply internally that flushes on the next read/write. This
    /// test locks in that contract so we can rely on it instead of explicitly replying
    /// in our `Message::Ping` arm — if it ever breaks (e.g., a tungstenite upgrade
    /// changes the default), this test fires and we know to add the explicit reply back.
    #[cfg(unix)]
    #[test]
    fn tungstenite_auto_pongs_inbound_ping() {
        use std::os::unix::net::UnixStream;
        use tungstenite::WebSocket;
        use tungstenite::protocol::Role;

        // Given — two WebSockets wired together by a Unix socketpair, no TCP/TLS.
        let (client_stream, server_stream) = UnixStream::pair().expect("socketpair");
        client_stream
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        server_stream
            .set_read_timeout(Some(Duration::from_millis(200)))
            .unwrap();
        let mut client = WebSocket::from_raw_socket(client_stream, Role::Client, None);
        let mut server = WebSocket::from_raw_socket(server_stream, Role::Server, None);
        let payload: Vec<u8> = b"hi".to_vec();

        // When — server sends a Ping; client reads it; flush drives the auto-queued Pong.
        server
            .send(Message::Ping(payload.clone()))
            .expect("server send ping");
        let msg = client.read().expect("client read ping");
        client.flush().expect("client flush");

        // Then — the client surfaced the Ping AND the server now sees a matching Pong,
        // even though our client code never called `send(Message::Pong(...))`.
        assert!(matches!(msg, Message::Ping(_)));
        match server.read().expect("server read pong") {
            Message::Pong(p) => assert_eq!(&p[..], payload.as_slice()),
            other => panic!("expected Pong, got {other:?}"),
        }
    }

    /// When a Ping is outstanding (`awaiting_pong`), `maybe_send_ping` must not send
    /// another Ping even if `PING_EVERY` has elapsed — one ping in flight at a time.
    #[test]
    fn does_not_send_ping_while_previous_is_unanswered() {
        // Given — server counts inbound pings and intentionally never pongs.
        let ping_count = Arc::new(AtomicUsize::new(0));
        let counter = ping_count.clone();
        let (mut transport, server) = connect_to_local_wss(move |mut ws| {
            loop {
                match ws.read() {
                    Ok(Message::Ping(_)) => {
                        counter.fetch_add(1, Ordering::SeqCst);
                    }
                    Ok(Message::Close(_)) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        });
        // Outstanding state: ping sent 11s ago (cadence is due), no pong since.
        transport.last_ping_sent = Some(Instant::now() - Duration::from_secs(11));
        transport.awaiting_pong = true;

        // When recv() runs with the ping cadence due but a ping still outstanding — the single
        // call that would re-ping if the "one ping in flight at a time" rule were not enforced.
        let _ = transport.recv();

        // Then — give the server a moment to receive anything that may have hit the
        // wire, then assert nothing did.
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            ping_count.load(Ordering::SeqCst),
            0,
            "expected no ping to be sent while previous is unanswered"
        );

        drop(transport);
        server.join().expect("server thread");
    }

    /// When the server initiates a Close, `recv` should surface `TransportError::Closed`
    /// carrying the peer's code and reason — not a generic Ws error.
    #[test]
    fn recv_returns_closed_with_peer_code_and_reason() {
        use tungstenite::protocol::CloseFrame;
        use tungstenite::protocol::frame::coding::CloseCode;

        // Given a server that initiates a graceful WebSocket Close carrying a specific code (1011)
        // and reason, then drives its state machine to complete the closing handshake.
        let (mut transport, server) = connect_to_local_wss(|mut ws| {
            let frame = CloseFrame {
                code: CloseCode::Error, // 1011
                reason: "server going away".into(),
            };
            let _ = ws.close(Some(frame));
            // Drive the state machine until the closing handshake completes — flushes
            // our pending Close and reads the client's response.
            while ws.read().is_ok() {}
        });

        // When the client polls recv() until it gets an error — the peer-initiated Close should
        // surface as a transport error rather than be silently swallowed or echoed as data.
        let mut got = None;
        for _ in 0..10 {
            match transport.recv() {
                Err(e) => {
                    got = Some(e);
                    break;
                }
                Ok(_) => continue,
            }
        }

        // Then the error is specifically TransportError::Closed carrying the peer's exact code and
        // reason (not a generic Ws error), so the reconnect logic can distinguish a clean peer
        // close from a transport fault.
        match got {
            Some(TransportError::Closed { code, reason }) => {
                assert_eq!(code, Some(1011));
                assert_eq!(reason, "server going away");
            }
            other => panic!("expected TransportError::Closed, got {other:?}"),
        }

        drop(transport);
        server.join().expect("server thread");
    }
}
