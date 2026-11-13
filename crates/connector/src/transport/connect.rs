use std::net::TcpStream;
use std::sync::Arc;

use rustls::{ClientConfig, RootCertStore};
use tungstenite::client::IntoClientRequest;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Connector, WebSocket, client_tls_with_config};
use url::Url;

use crate::auth::Token;
use crate::config;

use super::{TransportError, WsTransport};

/// Open the WebSocket and present the bearer token on the HTTP upgrade.
pub fn connect(url: &Url, token: &Token) -> Result<WsTransport, TransportError> {
    let connector = Connector::Rustls(Arc::new(client_config()?));
    connect_with_connector(url, token, connector)
}

/// Like [`connect`] but takes a caller-supplied TLS connector. Used by integration
/// tests that need to trust a self-signed cert from a local test server.
pub(super) fn connect_with_connector(
    url: &Url,
    token: &Token,
    connector: Connector,
) -> Result<WsTransport, TransportError> {
    let tcp = tcp_connect(url)?;
    // Bound the TLS + WebSocket-upgrade handshake: a peer that accepts TCP then stalls would
    // otherwise hang here forever, since the steady-state timeouts below are only applied after
    // the handshake returns. A timed-out handshake read surfaces (via tungstenite) as the
    // recoverable `Handshake` error. `apply_socket_timeouts` resets to the session values after.
    let handshake_timeout = Some(config::get().connector.handshake_timeout);
    tcp.set_read_timeout(handshake_timeout)
        .and_then(|()| tcp.set_write_timeout(handshake_timeout))
        .map_err(|e| TransportError::Io(e.to_string()))?;
    let request = connect_request(url, token)?;

    let (socket, _resp) =
        client_tls_with_config(request, tcp, None, Some(connector)).map_err(|e| match e {
            tungstenite::HandshakeError::Failure(tungstenite::Error::Http(resp)) => {
                let status = resp.status();
                let body = resp
                    .into_body()
                    .map(|b| String::from_utf8_lossy(&b).into_owned())
                    .unwrap_or_default();
                TransportError::Handshake(format!("HTTP {status}: {body}"))
            }
            other => TransportError::Handshake(other.to_string()),
        })?;

    apply_socket_timeouts(&socket)?;
    Ok(WsTransport::new(socket))
}

/// Build the WebSocket upgrade request for `url`, carrying the bearer token.
fn connect_request(
    url: &Url,
    token: &Token,
) -> Result<tungstenite::handshake::client::Request, TransportError> {
    let mut request = url
        .as_str()
        .into_client_request()
        .map_err(TransportError::handshake)?;
    request.headers_mut().insert(
        "Authorization",
        format!("Bearer {}", token.access_token).parse().unwrap(),
    );
    Ok(request)
}

/// Resolve the URL's host and open a TCP connection to it, trying each resolved address with
/// a bounded `connect_timeout` (the plain `TcpStream::connect` has no timeout, so a black-holed
/// SYN would block for the OS connect timeout). Returns the last error if every address fails.
fn tcp_connect(url: &Url) -> Result<TcpStream, TransportError> {
    let addrs = url
        .socket_addrs(|| None)
        .map_err(|e| TransportError::Handshake(format!("resolve: {e}")))?;
    let timeout = config::get().connector.handshake_timeout;
    let mut last_err = None;
    for addr in addrs {
        match TcpStream::connect_timeout(&addr, timeout) {
            Ok(stream) => return Ok(stream),
            Err(e) => last_err = Some(e),
        }
    }
    Err(TransportError::Handshake(format!(
        "tcp connect: {}",
        last_err.map_or_else(|| "no addresses resolved".to_string(), |e| e.to_string())
    )))
}

/// Apply the configured read and write timeouts to the socket.
///
/// The `read_timeout` bounds how long a single `recv()` blocks before returning `None`,
/// setting the cadence at which the driver loop wakes — shutdown latency, stale-pong
/// detection, and the token refresh check.
///
/// The `write_timeout` bounds a single `send`. Without it a write to a silently-dead
/// connection (no FIN/RST, kernel send buffer full) blocks forever, and because the
/// stale-pong check lives in the same driver loop, that freezes the whole connector with
/// no reconnect. A timed-out write returns a recoverable error instead, driving a reconnect.
fn apply_socket_timeouts(
    socket: &WebSocket<MaybeTlsStream<TcpStream>>,
) -> Result<(), TransportError> {
    let cfg = &config::get().connector;
    let tcp = match socket.get_ref() {
        MaybeTlsStream::Plain(s) => s,
        MaybeTlsStream::Rustls(s) => s.get_ref(),
        _ => return Ok(()),
    };
    tcp.set_read_timeout(Some(cfg.read_timeout))
        .map_err(|e| TransportError::Io(e.to_string()))?;
    tcp.set_write_timeout(Some(cfg.write_timeout))
        .map_err(|e| TransportError::Io(e.to_string()))
}

/// Build a rustls `ClientConfig`, selecting the `aws_lc_rs` provider explicitly.
///
/// The process default is ambiguous here: `ureq` (token fetch) enables rustls's `ring`
/// backend alongside its defaults, and our own `rustls` dep pulls `aws_lc_rs`, so both
/// are compiled in and `ClientConfig::builder()` — including the one tungstenite builds
/// when handed no connector — would panic. Picking the provider locally with
/// `builder_with_provider` avoids touching process-global state.
pub(super) fn client_config() -> Result<ClientConfig, TransportError> {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    Ok(
        ClientConfig::builder_with_provider(
            Arc::new(rustls::crypto::aws_lc_rs::default_provider()),
        )
        .with_protocol_versions(rustls::DEFAULT_VERSIONS)
        .map_err(TransportError::handshake)?
        .with_root_certificates(roots)
        .with_no_client_auth(),
    )
}
