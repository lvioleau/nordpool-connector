//! Global upstream-feed liveness flag: `true` while a market-data session is connected — the
//! watch twin of the `nordpool.connected` gauge (both flip together in `mark_connected`). The
//! driver publishes transitions; the gRPC server's health task subscribes and mirrors them onto
//! the feed services' statuses. Global like the shutdown flag, so the driver's public entry
//! points keep their signatures; publishing with no receivers (gRPC server disabled) is free.

use std::sync::LazyLock;

use tokio::sync::watch;

static FEED_UP: LazyLock<watch::Sender<bool>> = LazyLock::new(|| watch::channel(false).0);

/// Subscribe to the driver's session-liveness transitions: `false` until a session completes
/// its handshake, `true` while it is connected, `false` again when it ends — including a
/// silence ([`ServerSilent`](super::ServerSilent)) or scheduled recycle. A new receiver
/// observes the current value immediately, so a late subscriber never waits for the next
/// transition to learn the state.
pub fn feed_status() -> watch::Receiver<bool> {
    FEED_UP.subscribe()
}

pub(super) fn set_feed_up(up: bool) {
    FEED_UP.send_replace(up);
}
