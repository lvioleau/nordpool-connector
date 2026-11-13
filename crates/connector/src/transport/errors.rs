//! Transport-level error taxonomy, shared by the `Transport` trait and the connect path.

use std::time::Duration;

#[derive(Debug, thiserror::Error)]
pub enum TransportError {
    #[error("handshake failed: {0}")]
    Handshake(String),
    #[error("websocket error: {0}")]
    Ws(String),
    #[error("io error: {0}")]
    Io(String),
    #[error("unexpected message type")]
    UnexpectedMessageType,
    #[error("connection stale: no pong for {0:?}")]
    Stale(Duration),
    #[error("peer closed connection: code={code:?} reason={reason:?}")]
    Closed { code: Option<u16>, reason: String },
}

impl TransportError {
    pub(crate) fn handshake<E: std::fmt::Display>(e: E) -> Self {
        Self::Handshake(e.to_string())
    }
    pub(crate) fn ws<E: std::fmt::Display>(e: E) -> Self {
        Self::Ws(e.to_string())
    }
}

/// Whether an IO error is just the read timeout firing rather than a real failure. The
/// socket has a read timeout set (see `apply_read_timeout`), so a tick with no data
/// surfaces as `WouldBlock` or `TimedOut` depending on the platform; both mean "nothing
/// to read yet", which `recv` maps to `Ok(None)` so the caller loops again.
pub(super) fn would_block(e: &std::io::Error) -> bool {
    matches!(
        e.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    )
}
