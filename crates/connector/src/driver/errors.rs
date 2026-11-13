//! The driver's error taxonomy plus the helpers that classify and name frames for it:
//! [`is_recoverable`] decides whether the reconnect loop retries, and [`describe`] names a
//! server payload for log lines and error messages.

use std::time::Duration;

use crate::auth::AuthError;
use crate::proto::pmd::server_frame::Payload as ServerPayload;
use crate::transport::TransportError;

#[derive(Debug, thiserror::Error)]
#[error("server error: {message}")]
pub struct ServerError {
    pub message: String,
}

#[derive(Debug, thiserror::Error)]
#[error("timed out waiting for Connected from server after {0:?}")]
pub struct ConnectedTimeout(pub Duration);

#[derive(Debug, thiserror::Error)]
#[error("received unexpected {kind} frame before Connected during handshake")]
pub struct UnexpectedHandshakeFrame {
    pub kind: &'static str,
}

/// The `Connected` handshake frame announced no usable heartbeat interval, so the silence
/// watchdog cannot be armed. The session is refused (recoverably) rather than run unguarded.
#[derive(Debug, thiserror::Error)]
#[error("server announced no heartbeat interval (heartbeat_interval_ms = {0})")]
pub struct MissingHeartbeatInterval(pub i32);

/// The server stopped sending frames: nothing (heartbeat or data) arrived for `silent_for`,
/// past the watchdog limit derived from the `Connected` frame's announced heartbeat interval.
/// WS pongs only prove the peer's socket is alive; the application-level heartbeat proves the
/// feed is. Recoverable — the reconnect pulls fresh snapshots from a live session.
#[derive(Debug, thiserror::Error)]
#[error(
    "server silent: no frame for {silent_for:?} (announced heartbeat interval {heartbeat_interval:?})"
)]
pub struct ServerSilent {
    pub silent_for: Duration,
    pub heartbeat_interval: Duration,
}

/// Sentinel `run` raises when the session reaches its scheduled reconnect deadline
/// (`ConnectorConfig::session_reconnect`). Not a failure — `run_with_reconnect` matches it with
/// its own arm and treats it as a deliberate reconnect (resubscribe → fresh snapshots), so it
/// neither escalates backoff nor counts as an error. Like `ShutdownRequested`, it's deliberately
/// absent from [`is_recoverable`] (it stops the loop's *current* session rather than retrying it).
#[derive(Debug, thiserror::Error)]
#[error("session reached its scheduled reconnect")]
pub(super) struct SessionExpired;

/// What the reconnect loop needs to know about one error: the short, stable label that tags the
/// reconnect counter (so dashboards can split reconnects by cause) and whether retrying can
/// plausibly recover from it. The two travel together because they are one decision — naming an
/// error and deciding its fate — and keeping them in a single [`classify`] match means a new
/// variant cannot be given a label without also being given a fate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Reason {
    pub(crate) label: &'static str,
    pub(crate) recoverable: bool,
}

/// Retry after this one: a transient failure a fresh session can get past.
const fn recoverable(label: &'static str) -> Reason {
    Reason {
        label,
        recoverable: true,
    }
}

/// Give up on this one: reconnecting would hit the same wall.
const fn fatal(label: &'static str) -> Reason {
    Reason {
        label,
        recoverable: false,
    }
}

/// The one classification of a driver error. Transient transport drops, auth blips, and
/// handshake hiccups are recoverable; `ServerError` and `ShutdownRequested` are deliberately
/// absent from the recoverable set — they stop the loop, and fall through to `other`.
pub(crate) fn classify(e: &anyhow::Error) -> Reason {
    if let Some(te) = e.downcast_ref::<TransportError>() {
        return match te {
            TransportError::Closed { .. } => recoverable("peer_closed"),
            TransportError::Stale(_) => recoverable("stale"),
            TransportError::Ws(_) => recoverable("websocket"),
            TransportError::Io(_) => recoverable("io"),
            TransportError::Handshake(_) => recoverable("handshake"),
            TransportError::UnexpectedMessageType => fatal("unexpected_message_type"),
        };
    }
    if e.downcast_ref::<AuthError>().is_some() {
        return recoverable("auth");
    }
    if e.downcast_ref::<ConnectedTimeout>().is_some() {
        return recoverable("connected_timeout");
    }
    if e.downcast_ref::<UnexpectedHandshakeFrame>().is_some() {
        return recoverable("unexpected_handshake_frame");
    }
    if e.downcast_ref::<MissingHeartbeatInterval>().is_some() {
        return recoverable("missing_heartbeat_interval");
    }
    if e.downcast_ref::<ServerSilent>().is_some() {
        return recoverable("server_silent");
    }
    fatal("other")
}

/// Whether the reconnect loop should retry after `e` rather than give up.
pub(crate) fn is_recoverable(e: &anyhow::Error) -> bool {
    classify(e).recoverable
}

/// A short, stable label classifying an error, used to tag the reconnect counter.
pub(crate) fn reason_label(e: &anyhow::Error) -> &'static str {
    classify(e).label
}

/// A short, stable name for a server payload variant, for logs and error messages.
pub(crate) fn describe(p: &ServerPayload) -> &'static str {
    match p {
        ServerPayload::Connected(_) => "Connected",
        ServerPayload::Error(_) => "Error",
        ServerPayload::Heartbeat(_) => "Heartbeat",
        ServerPayload::LocalView(_) => "LocalView",
        ServerPayload::DeliveryArea(_) => "DeliveryArea",
        ServerPayload::Contract(_) => "Contract",
        ServerPayload::Ticker(_) => "Ticker",
        ServerPayload::H2hCapacity(_) => "H2HCapacity",
        ServerPayload::PublicStatistic(_) => "PublicStatistic",
        ServerPayload::AtcCapacity(_) => "AtcCapacity",
    }
}
