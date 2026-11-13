//! Cooperative shutdown: a process-wide flag set from a signal handler, polled by the
//! loops (plus its awaitable watch twin for async waiters), and the [`ShutdownRequested`]
//! sentinel they raise so the reconnect machinery stops and `main` can exit cleanly.

use std::sync::LazyLock;

use tokio::sync::watch;

/// Production: a process-wide atomic flag the signal handler and the loops share across
/// threads. Set by [`request_shutdown`] (typically from a SIGTERM signal handler in `main`).
#[cfg(not(test))]
mod flag {
    use std::sync::atomic::{AtomicBool, Ordering};

    static SHUTDOWN: AtomicBool = AtomicBool::new(false);

    pub(super) fn set() {
        SHUTDOWN.store(true, Ordering::Relaxed);
    }
    pub(super) fn get() -> bool {
        SHUTDOWN.load(Ordering::Relaxed)
    }
}

/// Tests: a thread-local flag. libtest runs each test on its own thread, so this isolates a
/// shutdown test's writes from the rest of the parallel suite without any locking.
#[cfg(test)]
mod flag {
    use std::cell::Cell;

    thread_local! {
        static SHUTDOWN: Cell<bool> = const { Cell::new(false) };
    }

    pub(super) fn set() {
        SHUTDOWN.with(|f| f.set(true));
    }
    pub(super) fn get() -> bool {
        SHUTDOWN.with(Cell::get)
    }
    pub(super) fn clear() {
        SHUTDOWN.with(|f| f.set(false));
    }
}

/// The awaitable twin of the flag, for waiters that cannot poll — the gRPC server's drain
/// and each subscriber stream await it. Process-global like the flag (and unlike it, shared
/// across test threads — no test reads it, they wire their own channels); the static sender
/// never drops, so a receiver seeing the channel close cannot happen in-process.
static SHUTDOWN_WATCH: LazyLock<watch::Sender<bool>> = LazyLock::new(|| watch::channel(false).0);

/// Ask the connector to stop at the next loop iteration. Idempotent.
pub fn request_shutdown() {
    flag::set();
    if !SHUTDOWN_WATCH.send_replace(true) {
        tracing::info!("shutdown requested");
    }
}

/// Subscribe to the shutdown request as an awaitable signal: `false` until
/// [`request_shutdown`], `true` from then on, never back. Await it with
/// `wait_for(|&requested| requested)` — not `changed()` — so a receiver obtained after the
/// request still resolves immediately.
pub fn shutdown_watch() -> watch::Receiver<bool> {
    SHUTDOWN_WATCH.subscribe()
}

pub(crate) fn shutdown_requested() -> bool {
    flag::get()
}

/// Clear the flag (tests only) so a test thread reused by libtest starts clean.
#[cfg(test)]
pub(crate) fn reset() {
    flag::clear();
}

/// Sentinel that an attempt aborted because a shutdown was requested. It is non-recoverable
/// (so `retry_with_backoff` propagates it rather than retrying) and is translated back to a
/// graceful `Ok(())` by `main` — the one place shutdown becomes success.
#[derive(Debug, thiserror::Error)]
#[error("shutdown requested")]
pub struct ShutdownRequested;
