//! Shared subscriber concurrency limiting for the streaming services.
//!
//! Each gRPC stream is capped by a [`Semaphore`]: [`SubscriberLimit::acquire`] takes a permit when
//! a client subscribes and hands back a [`SubscriberGuard`] that holds it for the lifetime of the
//! response stream, releasing the permit (and decrementing the live-subscriber gauge) when the
//! stream is dropped — i.e. when the client disconnects. A [`SubscriberLimit`] bundles the
//! semaphore with the two labels that vary per feed (its gauge metric and over-cap message), so a
//! feed just holds one and calls `acquire`.

use std::sync::Arc;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tonic::Status;

/// Tracks one live subscriber for its stream's lifetime: holds the concurrency permit and keeps the
/// feed's live-subscriber gauge in step. Drop it (tonic drops the stream on disconnect) and the
/// permit is released and the gauge decremented. Move it onto the response stream so its lifetime
/// matches the subscription's.
pub(crate) struct SubscriberGuard {
    _permit: OwnedSemaphorePermit,
    metric: &'static str,
}

impl SubscriberGuard {
    fn new(permit: OwnedSemaphorePermit, metric: &'static str) -> Self {
        metrics::gauge!(metric).increment(1.0);
        Self {
            _permit: permit,
            metric,
        }
    }
}

impl Drop for SubscriberGuard {
    fn drop(&mut self) {
        metrics::gauge!(self.metric).decrement(1.0);
    }
}

/// Bounds concurrent subscribers on one streaming feed: a [`Semaphore`] plus the two labels that
/// vary per feed — the `nordpool.*.subscribers` gauge it keeps in step and the message returned
/// when the cap is reached. A feed holds one of these and calls [`acquire`](Self::acquire).
pub(crate) struct SubscriberLimit {
    semaphore: Arc<Semaphore>,
    subscribers_metric: &'static str,
    limit_message: &'static str,
}

impl SubscriberLimit {
    pub(crate) fn new(
        max_subscribers: usize,
        subscribers_metric: &'static str,
        limit_message: &'static str,
    ) -> Self {
        Self {
            semaphore: Arc::new(Semaphore::new(max_subscribers)),
            subscribers_metric,
            limit_message,
        }
    }

    /// Take a subscriber slot for the lifetime of a stream, or reject the subscribe with
    /// `RESOURCE_EXHAUSTED` when the cap is reached. The returned [`SubscriberGuard`] must be moved
    /// onto the response stream so the permit is held until the client disconnects.
    // tonic's `Status` is large by design, so the `Err` variant is unavoidably large here.
    #[allow(clippy::result_large_err)]
    pub(crate) fn acquire(&self) -> Result<SubscriberGuard, Status> {
        let permit = Arc::clone(&self.semaphore)
            .try_acquire_owned()
            .map_err(|_| Status::resource_exhausted(self.limit_message))?;
        Ok(SubscriberGuard::new(permit, self.subscribers_metric))
    }
}
