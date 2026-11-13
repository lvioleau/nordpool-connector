//! A [`FrameSink`] decorator that runs the wrapped sink on its own thread behind a bounded
//! queue, so a slow or blocking sink (disk, S3, ...) doesn't stall the driver's receive
//! loop. The driver thread only copies the frame and enqueues it; the worker thread owns
//! the inner sink and does the actual work. `run` and `TeeSink` stay untouched — queueing
//! is opt-in per sink by wrapping it in this.

use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};

use crossbeam_channel::{
    Receiver, RecvTimeoutError, SendTimeoutError, Sender, TrySendError, bounded,
};

use crate::proto::pmd::server_frame::Payload as ServerPayload;

use super::FrameSink;

/// One queued frame: the decoded payload, the original wire bytes, and the receive time,
/// owned so they can cross the thread boundary (the inner sink's `handle` wants all three,
/// like every sink). Carrying the receive time through the queue keeps it the *arrival*
/// instant — stamping it on the worker would record write time after queue delay instead.
type QueuedFrame = (ServerPayload, Vec<u8>, DateTime<Utc>);

/// Runs an inner [`FrameSink`] on a dedicated thread fed by a bounded channel.
pub struct QueuedSink {
    // `Option` so `Drop` can drop the sender (closing the channel to signal the worker)
    // before joining; `Some` for the whole live lifetime.
    tx: Option<Sender<QueuedFrame>>,
    worker: Option<JoinHandle<()>>,
    // The inner sink's most recent error, surfaced to the driver on the next `handle` so the
    // worker thread cannot swallow one — it reaches the driver exactly as it would on the
    // synchronous path, and there it is fatal.
    last_error: Arc<Mutex<Option<anyhow::Error>>>,
    enqueue_timeout: Duration,
    /// Set by an enqueue that waited out the timeout, cleared by the next one that fits.
    /// While it holds, a full queue drops on the spot instead of waiting again — see
    /// [`handle`](QueuedSink::handle).
    stalled: bool,
    /// The worker's name, labelling this sink's metrics.
    name: String,
}

/// What one enqueue attempt did, decided while the sender is borrowed and acted on after.
enum Enqueued {
    Sent,
    /// The queue was full for the whole timeout: this frame is gone.
    Dropped,
    /// The worker is no longer receiving.
    WorkerGone,
}

impl QueuedSink {
    /// Wrap `inner`, running it on a worker thread that pulls from a bounded queue of
    /// `capacity` frames. `name` labels the worker thread and this sink's metrics.
    /// `capacity` is what absorbs a writer that has fallen behind — depth divided by the
    /// frame rate is the stall it covers losslessly. `enqueue_timeout` bounds how long the
    /// driver waits once even that is exhausted, and `poll_interval` bounds how often the
    /// worker wakes to sample its queue depth when the stream is quiet.
    pub fn new(
        name: &str,
        inner: Box<dyn FrameSink + Send>,
        capacity: usize,
        enqueue_timeout: Duration,
        poll_interval: Duration,
    ) -> anyhow::Result<Self> {
        let (tx, rx) = bounded::<QueuedFrame>(capacity);
        let last_error = Arc::new(Mutex::new(None));
        let worker_error = last_error.clone();
        let name = name.to_string();
        let worker_name = name.clone();
        let worker = std::thread::Builder::new()
            .name(name.clone())
            .spawn(move || run_worker(rx, inner, worker_error, poll_interval, worker_name))?;
        Ok(Self {
            tx: Some(tx),
            worker: Some(worker),
            last_error,
            enqueue_timeout,
            stalled: false,
            name,
        })
    }

    fn take_error(&self) -> Option<anyhow::Error> {
        self.last_error.lock().ok().and_then(|mut slot| slot.take())
    }
}

impl FrameSink for QueuedSink {
    /// Enqueue one frame, waiting only while waiting can still achieve something.
    ///
    /// The queue being full means the worker is behind, and `try_send` failing is the earliest
    /// point that is knowable — so it is reported there (`nordpool.sink.blocked`,
    /// `block_seconds`) rather than only if it ends in a drop. Blocking is not free: the driver
    /// is not receiving while it waits, so the disk is holding up the live feeds, and *that* is
    /// the thing worth alerting on.
    ///
    /// It stays bounded, though. A worker that is genuinely stuck makes the wait pure cost, and
    /// paying it per frame would throttle the receive loop to one frame per timeout — an archive
    /// fault becoming a client outage. So a wait that times out latches `stalled`, and while
    /// that holds frames are dropped immediately; the next one that fits clears it. Losing
    /// archive frames beats stalling the feeds, and neither is fatal: the process keeps serving
    /// and the counters say what it lost.
    fn handle(
        &mut self,
        payload: &ServerPayload,
        bytes: &[u8],
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        // Surface an inner-sink error from an earlier frame rather than letting the worker thread
        // swallow it — it is fatal, so it must reach the driver. The archive settles its own disk
        // faults and only errors here for the ones it will not serve through.
        if let Some(err) = self.take_error() {
            return Err(err);
        }
        let frame = (payload.clone(), bytes.to_vec(), received_at);
        // Only None mid-`Drop`; a live sink always has its sender.
        let Some(tx) = self.tx.as_ref() else {
            anyhow::bail!("queued sink is shutting down")
        };

        let outcome = match tx.try_send(frame) {
            Ok(()) => Enqueued::Sent,
            Err(TrySendError::Disconnected(_)) => Enqueued::WorkerGone,
            Err(TrySendError::Full(frame)) if self.stalled => {
                // Already known stuck: don't pay the wait again. The frame drops here.
                let _ = frame;
                Enqueued::Dropped
            }
            Err(TrySendError::Full(frame)) => {
                metrics::counter!("nordpool.sink.blocked", "sink" => self.name.clone())
                    .increment(1);
                let started = Instant::now();
                let sent = tx.send_timeout(frame, self.enqueue_timeout);
                // Recorded whether or not the wait paid off — it measures how long the sink
                // held the receive loop, which the feeds felt either way.
                metrics::histogram!("nordpool.sink.block_seconds", "sink" => self.name.clone())
                    .record(started.elapsed().as_secs_f64());
                match sent {
                    Ok(()) => Enqueued::Sent,
                    Err(SendTimeoutError::Timeout(_)) => Enqueued::Dropped,
                    Err(SendTimeoutError::Disconnected(_)) => Enqueued::WorkerGone,
                }
            }
        };

        match outcome {
            Enqueued::Sent => {
                if self.stalled {
                    self.stalled = false;
                    tracing::info!(sink = %self.name, "queued sink draining again");
                }
                Ok(())
            }
            Enqueued::Dropped => {
                if !self.stalled {
                    self.stalled = true;
                    tracing::error!(
                        sink = %self.name,
                        timeout = ?self.enqueue_timeout,
                        "queued sink full and not draining; dropping frames to keep the feeds running"
                    );
                }
                metrics::counter!("nordpool.sink.frames_dropped", "sink" => self.name.clone())
                    .increment(1);
                Ok(())
            }
            // The worker only ends by panicking, which is a bug rather than a disk fault;
            // prefer its recorded cause if one landed.
            Enqueued::WorkerGone => Err(self
                .take_error()
                .unwrap_or_else(|| anyhow::anyhow!("queued sink worker stopped"))),
        }
    }
}

impl Drop for QueuedSink {
    fn drop(&mut self) {
        // Drop the sender so the worker sees the channel close, drains what's queued, and
        // drops the inner sink (running its flush-on-Drop, e.g. the archive) — then join so all
        // of that completes before we return.
        self.tx.take();
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            tracing::error!("queued sink worker panicked");
        }
    }
}

/// Pull frames until the sender is dropped (`Drop` / shutdown), sampling the queue depth at
/// most once per `poll_interval`. Inner-sink errors are recorded for the driver to observe
/// but don't stop the worker, so a transient failure recovers instead of wedging the queue.
/// `inner` drops on this thread once the channel closes, so its flush-on-`Drop` runs after
/// the queue has fully drained.
fn run_worker(
    rx: Receiver<QueuedFrame>,
    mut inner: Box<dyn FrameSink + Send>,
    last_error: Arc<Mutex<Option<anyhow::Error>>>,
    poll_interval: Duration,
    name: String,
) {
    let mut last_tick = Instant::now();
    loop {
        match rx.recv_timeout(poll_interval) {
            Ok((payload, bytes, received_at)) => {
                record_error(&last_error, inner.handle(&payload, &bytes, received_at))
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => break,
        }
        if last_tick.elapsed() >= poll_interval {
            metrics::gauge!("nordpool.sink.queue_depth", "sink" => name.clone())
                .set(rx.len() as f64);
            last_tick = Instant::now();
        }
    }
}

/// Record an inner-sink error (if any) for the next `handle` to surface to the driver.
fn record_error(slot: &Mutex<Option<anyhow::Error>>, result: anyhow::Result<()>) {
    if let Err(e) = result {
        tracing::warn!(error = format!("{e:#}"), "queued sink inner sink failed");
        if let Ok(mut slot) = slot.lock() {
            *slot = Some(e);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::pmd::Heartbeat;

    fn any_payload() -> ServerPayload {
        ServerPayload::Heartbeat(Heartbeat {})
    }

    /// Queue `capacity` deep enough that no test fills it, paired with an `enqueue_timeout` no
    /// test waits out — so a test only meets the full-queue path when it asks for it (see the
    /// stall test, which sizes its own).
    const QUEUE_CAPACITY: usize = 8;
    const ENQUEUE_TIMEOUT: Duration = Duration::from_secs(30);
    /// `poll_interval`: how often the worker wakes to check for a stopping process. Short, so a
    /// test's drop is quick.
    const POLL_INTERVAL: Duration = Duration::from_millis(10);

    fn received_at() -> DateTime<Utc> {
        DateTime::UNIX_EPOCH
    }

    #[derive(Clone, Default)]
    struct CountingSink {
        received: Arc<Mutex<Vec<Vec<u8>>>>,
    }
    impl FrameSink for CountingSink {
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

    #[derive(Default)]
    struct ErroringSink;
    impl FrameSink for ErroringSink {
        fn handle(
            &mut self,
            _payload: &ServerPayload,
            _bytes: &[u8],
            _recv_time: DateTime<Utc>,
        ) -> anyhow::Result<()> {
            Err(anyhow::anyhow!("kaboom"))
        }
    }

    #[test]
    fn queued_sink_forwards_frames_in_order_and_drains_on_drop() {
        // Given a QueuedSink wrapping a recording inner sink that runs on the worker thread; the
        // test keeps a clone of the inner sink so it can read what the worker actually processed.
        let inner = CountingSink::default();
        let mut queued = QueuedSink::new(
            "test-queue",
            Box::new(inner.clone()),
            QUEUE_CAPACITY,
            ENQUEUE_TIMEOUT,
            POLL_INTERVAL,
        )
        .unwrap();

        // When two frames are enqueued (the driver thread only copies and hands off) and the sink
        // is dropped — Drop closes the channel so the worker drains the backlog before the join.
        queued.handle(&any_payload(), b"a", received_at()).unwrap();
        queued.handle(&any_payload(), b"b", received_at()).unwrap();
        drop(queued);

        // Then the worker processed both frames in enqueue order, so nothing queued was lost on
        // shutdown and the queue preserves FIFO ordering across the thread boundary.
        let received = inner.received.lock().unwrap();
        assert_eq!(received.len(), 2);
        assert_eq!(received[0], b"a");
        assert_eq!(received[1], b"b");
    }

    #[test]
    fn queued_sink_surfaces_inner_error_to_the_caller() {
        use std::time::Duration;

        // Given a QueuedSink whose inner sink fails on every frame, so the worker will record an
        // error that the next handle call must surface — moving a sink onto a worker thread must
        // not turn a fatal failure into a silent one.
        let mut queued = QueuedSink::new(
            "test-queue-err",
            Box::new(ErroringSink),
            QUEUE_CAPACITY,
            ENQUEUE_TIMEOUT,
            POLL_INTERVAL,
        )
        .unwrap();

        // When frames are enqueued, the worker processes them asynchronously and records the
        // failure; a later handle surfaces it. Poll until it does (bounded so the test can't
        // hang).
        let mut surfaced = false;
        for _ in 0..1000 {
            match queued.handle(&any_payload(), b"x", received_at()) {
                Ok(()) => std::thread::sleep(Duration::from_millis(1)),
                Err(_) => {
                    surfaced = true;
                    break;
                }
            }
        }

        // Then the inner sink's error reached the caller, so the driver still sees it.
        assert!(
            surfaced,
            "inner sink error was never surfaced to the caller"
        );
    }

    /// Blocks inside `handle` until the test sends it a release token — a stand-in for a
    /// stalled (not failed) disk write.
    struct StallingSink {
        release: std::sync::mpsc::Receiver<()>,
    }
    impl FrameSink for StallingSink {
        fn handle(
            &mut self,
            _payload: &ServerPayload,
            _bytes: &[u8],
            _recv_time: DateTime<Utc>,
        ) -> anyhow::Result<()> {
            let _ = self.release.recv();
            Ok(())
        }
    }

    #[test]
    fn a_stalled_worker_drops_frames_instead_of_stalling_the_driver() {
        use std::time::Duration;

        // Given a QueuedSink of capacity 1 whose inner sink hangs inside handle (a stalled
        // write, not a failed one — it returns no error to record), with a short enqueue
        // timeout. The worker takes the first frame and stalls; the second fills the queue.
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        let mut queued = QueuedSink::new(
            "test-queue-stall",
            Box::new(StallingSink {
                release: release_rx,
            }),
            1,
            Duration::from_millis(50),
            POLL_INTERVAL,
        )
        .unwrap();
        queued.handle(&any_payload(), b"a", received_at()).unwrap();
        queued.handle(&any_payload(), b"b", received_at()).unwrap();

        // When a third frame meets the full queue.
        let started = std::time::Instant::now();
        queued.handle(&any_payload(), b"c", received_at()).unwrap();
        let first = started.elapsed();

        // Then it waits out the timeout — brief backpressure must still block and recover, so
        // giving up instantly would drop frames a working writer would have caught up on — and
        // then returns Ok: a disk that cannot keep up costs archive frames, never the process.
        assert!(
            first >= Duration::from_millis(50),
            "gave up before the timeout: {first:?}"
        );

        // And the next frame drops immediately rather than paying the timeout again: at a
        // hundred-odd frames a second, waiting per frame would throttle the receive loop to
        // one frame per timeout and turn an archive fault into a client outage.
        let started = std::time::Instant::now();
        queued.handle(&any_payload(), b"d", received_at()).unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(25),
            "second frame paid the timeout again: {:?}",
            started.elapsed()
        );

        // Unblock the worker so Drop's drain-and-join can complete.
        release_tx.send(()).unwrap();
        release_tx.send(()).unwrap();
    }
}
