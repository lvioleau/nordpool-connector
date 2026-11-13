//! Logging setup. The level filter comes from `RUST_LOG` (default `info`); all events are
//! emitted as JSON (one object per line, fields flattened to top-level keys) to stdout for a
//! log shipper to parse. Settings come straight from the environment, not the INI, because
//! tracing is installed before the config file / secrets load — so the load itself can be
//! logged.

use tracing_appender::non_blocking::WorkerGuard;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer, fmt};

/// Install the global tracing subscriber: JSON to stdout, level from `RUST_LOG` (default
/// `info`).
///
/// Stdout runs through a non-blocking (lossy) writer: a background thread drains a bounded
/// queue, and lines are *dropped* under back-pressure rather than blocking the caller. This is
/// load-bearing — a stalled stdout consumer (a paused terminal back-pressuring the pipe, or a
/// wedged log driver) would otherwise block whatever thread is logging inside `write()`, and
/// since the driver logs on its hot path that freezes the whole connector. The returned guard
/// owns the background thread; keep it alive for the whole process or buffered lines are lost
/// on exit.
pub fn init() -> WorkerGuard {
    let filter = std::env::var("RUST_LOG").unwrap_or_else(|_| "info".to_string());
    let (writer, guard) = tracing_appender::non_blocking(std::io::stdout());
    tracing_subscriber::registry()
        .with(
            fmt::layer()
                .json()
                .flatten_event(true)
                .with_writer(writer)
                .with_filter(EnvFilter::new(filter)),
        )
        .init();
    log_panics();
    guard
}

/// Also report panics through `tracing`, so they arrive as JSON at error level like everything
/// else. The default hook writes plain text to stderr, which a log pipeline expecting one JSON
/// object per line does not parse — so the panic that killed a thread reads as an unstructured
/// line at whatever level the shipper guesses, while the events either side of it are structured.
/// That matters most for the threads whose death the process survives: a panicking uploader leaves
/// the connector serving its feeds with nothing shipping, and this is the log that says so at the
/// moment it happens rather than at shutdown.
///
/// The previous hook still runs afterwards, so the stderr line (and any backtrace `RUST_BACKTRACE`
/// asks for) is unchanged — this adds a record, it does not replace one.
fn log_panics() {
    let previous = std::panic::take_hook();
    std::panic::set_hook(Box::new(move |info| {
        let thread = std::thread::current();
        tracing::error!(
            thread = thread.name().unwrap_or("unnamed"),
            location = %info.location().map_or_else(|| "unknown".to_string(), ToString::to_string),
            "panic: {}",
            panic_message(info.payload())
        );
        previous(info);
    }));
}

/// The panic's message, for the two payload types `panic!` produces: a bare literal arrives as
/// `&str`, a formatted one as `String`. Anything else — a payload from `panic_any` — has no text
/// to report, and reporting the panic without its message still beats reporting nothing.
fn panic_message(payload: &(dyn std::any::Any + Send)) -> String {
    payload
        .downcast_ref::<&str>()
        .map(|s| (*s).to_string())
        .or_else(|| payload.downcast_ref::<String>().cloned())
        .unwrap_or_else(|| "no message".to_string())
}

#[cfg(test)]
mod tests {
    use std::io::Write;
    use std::sync::{Arc, Mutex};

    use tracing_subscriber::fmt::MakeWriter;

    use super::*;

    #[test]
    fn a_panic_is_reported_through_tracing_as_json() {
        // Given the panic hook installed over a no-op previous hook (so the default stderr line
        // doesn't clutter the test run), and a subscriber capturing this thread's JSON output.
        std::panic::set_hook(Box::new(|_| {}));
        log_panics();
        let captured = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::registry().with(
            fmt::layer()
                .json()
                .flatten_event(true)
                .with_writer(Capture(Arc::clone(&captured))),
        );

        // When a panic happens — caught so the test itself survives it.
        tracing::subscriber::with_default(subscriber, || {
            let _ = std::panic::catch_unwind(|| {
                panic!("segment path outside the staging dir: /data/x.tfrecord.gz")
            });
        });
        let _ = std::panic::take_hook();

        // Then it arrives as one JSON object at ERROR, carrying the message, the thread it killed
        // and where it happened — parsed by the same pipeline as every other event, rather than as
        // the default hook's bare stderr text.
        let logged = String::from_utf8(captured.lock().unwrap().clone()).unwrap();
        assert!(logged.contains(r#""level":"ERROR""#), "{logged}");
        assert!(
            logged.contains("panic: segment path outside the staging dir: /data/x.tfrecord.gz"),
            "{logged}"
        );
        assert!(logged.contains(r#""thread":"#), "{logged}");
        assert!(logged.contains("logging.rs:"), "{logged}");
    }

    /// Collects a subscriber's output into a buffer the test can assert on.
    struct Capture(Arc<Mutex<Vec<u8>>>);

    impl<'a> MakeWriter<'a> for Capture {
        type Writer = CaptureWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CaptureWriter(Arc::clone(&self.0))
        }
    }

    struct CaptureWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for CaptureWriter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[test]
    fn panic_message_reads_both_payload_shapes() {
        // A `panic!` with a bare literal carries a `&str`; one with formatting carries a `String`.
        assert_eq!(panic_message(&"unknown segment"), "unknown segment");
        assert_eq!(panic_message(&"formatted: 7".to_string()), "formatted: 7");
        // Anything else (a `panic_any` payload) still reports, just without a message.
        assert_eq!(panic_message(&7_u32), "no message");
    }
}
