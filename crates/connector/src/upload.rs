//! Best-effort uploader that drains archive segments to object storage (S3 in ECS, a local
//! `file://` path in dev/CI), on its own thread with its own tokio runtime and decoupled from the
//! [`ArchiveSink`](crate::ArchiveSink). The **layout is the writer's**: the uploader mirrors each
//! file's path relative to the staging dir under `base` and computes no partition of its own.
//!
//! - a finalized `*.tfrecord.gz` is PUT to `<base>/<type>/<date>/<name>` and the local copy
//!   deleted — the object store is the archive of record, the local dir a small ephemeral
//!   staging area;
//! - an open `*.tfrecord.partial` is **never uploaded**, so the uploader never reads a file being
//!   written. The archive's age-based finalize bounds hard-kill loss to ~`segment_max_age` plus
//!   one scan. A graceful shutdown finalizes every partial first, so the final drain ships them
//!   through the same path.
//!
//! Polling, not notification: a periodic scan needs no shared state with the `QueuedSink` worker.
//! At-least-once — a failure logs, counts, and leaves the file for a later scan, and re-PUTting an
//! immutable, uniquely named segment is harmless.
//!
//! A pass reasons about a `Ledger` rather than the walk that fed it: entries arrive from the walk
//! and leave where their file does, never from a walk that failed to list one. A segment that has
//! genuinely vanished is logged and counted (`cause:vanished`) — nothing but this uploader deletes
//! a finalized segment, so it means undelivered data was destroyed. The ledger also holds the
//! backoff, so a store outage costs a pass only what is due rather than the whole backlog, and it
//! selects **oldest data first**, so a pass cut short leaves the newest behind.
//!
//! The **final drain is the exception**: every pending segment regardless of backoff, bounded by
//! `[connector.archive.upload] shutdown_grace_secs`. Deferring bets on a later pass, and at
//! shutdown the staging dir goes away with the task.
//!
//! Every PUT carries the configured `tags` (S3 `x-amz-tagging`), built once into a [`TagSet`] and
//! nothing derived per file — reading the layout would tie the uploader to a shape only the writer
//! owns. The event date is still selectable, as a level of the object key.
//!
//! This module owns the worker thread and the store wiring; the rest splits by what it knows:
//! `listing` walks, `ledger` remembers and decides when a segment is due, `pass` runs one scan.

mod ledger;
mod listing;
mod pass;
#[cfg(test)]
mod testing;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use anyhow::{Context, anyhow};
use object_store::aws::AmazonS3Builder;
use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, RetryConfig, TagSet};

use crate::config::UploadConfig;
use ledger::Ledger;
use pass::{Attempt, Uploads};

/// Owns the uploader worker thread. Dropping it (or calling [`shutdown`](Uploader::shutdown))
/// signals the thread to stop after one final drain and joins it.
pub struct Uploader {
    shutdown: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Uploader {
    /// Build the store from `cfg.uri` — failing fast on a bad URI or unbuildable backend — then
    /// spawn the worker over `archive_dir`, the local staging root.
    pub fn start(archive_dir: PathBuf, cfg: &UploadConfig) -> anyhow::Result<Self> {
        let retry = RetryConfig {
            max_retries: cfg.max_retries,
            retry_timeout: cfg.retry_timeout,
            ..Default::default()
        };
        let (store, base) = build_object_store(&cfg.uri, retry)
            .with_context(|| format!("building object store for {:?}", cfg.uri))?;
        let mut tags = TagSet::default();
        for (key, value) in &cfg.tags {
            tags.push(key, value);
        }
        let uploads = Uploads {
            dir: archive_dir,
            store,
            base,
            concurrency: cfg.concurrency,
            scan_interval: cfg.scan_interval,
            max_backoff: cfg.max_backoff,
            shutdown_grace: cfg.shutdown_grace,
            tags,
        };
        let shutdown = Arc::new(AtomicBool::new(false));

        tracing::info!(
            uri = %cfg.uri,
            scan_interval_secs = uploads.scan_interval.as_secs(),
            concurrency = uploads.concurrency,
            max_backoff_secs = uploads.max_backoff.as_secs(),
            shutdown_grace_secs = uploads.shutdown_grace.as_secs(),
            "archive uploader enabled"
        );

        let worker_shutdown = Arc::clone(&shutdown);
        let handle = thread::Builder::new()
            .name("archive-uploader".into())
            .spawn(move || worker(uploads, &worker_shutdown))
            .context("spawning archive uploader thread")?;

        Ok(Self {
            shutdown,
            handle: Some(handle),
        })
    }

    /// Signal stop and join. The worker runs one final drain after seeing the flag, so a segment
    /// finalized just before it — `ArchiveSink::drop` included — still ships.
    pub fn shutdown(mut self) {
        self.stop_and_join();
    }

    fn stop_and_join(&mut self) {
        // The handle is `Some` only on the first stop, so this logs/joins exactly once even
        // though both `shutdown()` and `Drop` call here.
        if let Some(handle) = self.handle.take() {
            tracing::info!("archive uploader stopping — running final drain");
            self.shutdown.store(true, Ordering::Release);
            if handle.join().is_err() {
                tracing::error!("archive uploader thread panicked");
            } else {
                tracing::info!("archive uploader stopped");
            }
        }
    }
}

impl Drop for Uploader {
    fn drop(&mut self) {
        // Belt-and-suspenders: if shutdown() wasn't called explicitly, still stop and join.
        self.stop_and_join();
    }
}

/// The worker thread body. Owns a single-threaded tokio runtime (the object-store client is
/// async) and drains the archive dir every `scan_interval` until stopped, then once more.
fn worker(uploads: Uploads, shutdown: &AtomicBool) {
    let runtime = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            tracing::error!(error = %e, "archive uploader could not start its runtime");
            return;
        }
    };

    // The retry state lives here, across passes, for as long as the uploader runs.
    let mut ledger = Ledger::default();
    loop {
        // Checked before the pass, so a flag set *during* one still gets a final drain: the
        // interruptible sleep returns at once and the next iteration takes the branch below.
        if shutdown.load(Ordering::Acquire) {
            final_drain(&runtime, &uploads, &mut ledger);
            count_scan();
            break;
        }
        runtime.block_on(uploads.drain(&mut ledger, Instant::now(), Attempt::Due));
        count_scan();
        sleep_interruptible(uploads.scan_interval, shutdown);
    }
}

/// The uploader's liveness signal, counted after every completed pass. A dead worker is otherwise
/// invisible: `pending_uploads` would just stop moving, which looks like a gauge with nothing to
/// report. Alert on this going quiet.
fn count_scan() {
    metrics::counter!("nordpool.archive.upload_scans").increment(1);
}

/// The last pass before the process exits: every pending segment, backoff or not, under a deadline.
///
/// The backoff bets on a cheaper later pass, and there is none — the staging dir goes with the
/// task, so a deferred segment is an abandoned one. The deadline stops that cutting the other way:
/// one unreachable-store PUT can hold `object_store`'s ladder for `retry_timeout`, and the gRPC
/// drain still has to run.
fn final_drain(runtime: &tokio::runtime::Runtime, uploads: &Uploads, ledger: &mut Ledger) {
    let drain = uploads.drain(ledger, Instant::now(), Attempt::Everything);
    // The timer is armed inside the runtime context: `timeout` registers with the reactor as it is
    // constructed, so building it outside `block_on` panics.
    let outcome =
        runtime.block_on(async { tokio::time::timeout(uploads.shutdown_grace, drain).await });
    if outcome.is_err() {
        // Logged rather than counted: the exporter flushes from a background thread this exiting
        // process will not wait for. On ephemeral staging, left behind means gone.
        tracing::warn!(
            grace_secs = uploads.shutdown_grace.as_secs(),
            pending = ledger.len(),
            "archive upload final drain hit its deadline; segments left unshipped"
        );
    }
}

/// The key partition a segment lives under: `<base>/<type>[/<date>]` — the single source of truth
/// for the archive's object-store layout, shared by the uploader that writes it and `depth-replay`
/// that lists under it (date `None` lists every day). Change the order here and both ends move
/// together. A segment carries every subscribed area's frames, so there is no per-area level.
pub fn segment_prefix(base: &ObjectPath, frame_type: &str, date: Option<&str>) -> ObjectPath {
    let prefix = base.clone().join(frame_type);
    match date {
        Some(date) => prefix.join(date),
        None => prefix,
    }
}

/// Sleep up to `total`, waking early if the shutdown flag is set, so stop is responsive even
/// with a long scan interval.
fn sleep_interruptible(total: Duration, shutdown: &AtomicBool) {
    const STEP: Duration = Duration::from_millis(200);
    let mut slept = Duration::ZERO;
    while slept < total {
        if shutdown.load(Ordering::Acquire) {
            return;
        }
        let step = STEP.min(total - slept);
        thread::sleep(step);
        slept += step;
    }
}

/// Pick the backend by URI scheme and return the store plus the base [`ObjectPath`] (the URI's
/// path prefix). `s3://bucket/prefix` builds an S3 store via `from_env` (which wires the region
/// and the credential chain, including the ECS task-role container endpoint) rooted at the
/// bucket, with `prefix` as the base path and `retry` as its per-PUT retry policy; `file:///abs/path`
/// uses a `LocalFileSystem` rooted at the filesystem root with `abs/path` as the base — the dev/CI
/// path, no network, no AWS, and no retry layer (so `retry` is S3-only).
fn build_object_store(
    uri: &str,
    retry: RetryConfig,
) -> anyhow::Result<(Arc<dyn ObjectStore>, ObjectPath)> {
    let url = url::Url::parse(uri)
        .with_context(|| format!("parsing [connector.archive.upload] uri {uri:?}"))?;
    let prefix = ObjectPath::from(url.path().trim_start_matches('/'));
    match url.scheme() {
        "s3" => {
            let store = AmazonS3Builder::from_env()
                .with_url(uri)
                .with_retry(retry)
                .build()
                .context("building S3 object store")?;
            Ok((Arc::new(store), prefix))
        }
        "file" => {
            let store = LocalFileSystem::new();
            Ok((Arc::new(store), prefix))
        }
        other => Err(anyhow!(
            "unsupported [connector.archive.upload] uri scheme {other:?} (expected s3:// or file://)"
        )),
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::fs;

    use super::testing::{TempDir, staged_path};
    use super::*;

    #[test]
    fn shutdown_drains_segments_finalized_after_last_scan() {
        // Given a running uploader with a scan interval long enough that the periodic pass
        // never fires during the test, pointed at a file:// destination
        let staging = TempDir::new();
        let dest = TempDir::new();
        let cfg = UploadConfig {
            uri: format!("file://{}", dest.path().display()),
            scan_interval: Duration::from_secs(3600),
            concurrency: 4,
            max_backoff: Duration::from_secs(3600),
            tags: BTreeMap::from([("delivery_area".to_string(), "40".to_string())]),
            max_retries: 10,
            retry_timeout: Duration::from_secs(180),
            shutdown_grace: Duration::from_secs(10),
        };
        let uploader = Uploader::start(staging.path().to_path_buf(), &cfg).unwrap();

        // When a segment is finalized after start (as ArchiveSink::drop would on shutdown), laid out
        // under <type>/<date>/ exactly as the archive writes it.
        let name = "localview-20260619T101010.000Z-000.tfrecord.gz";
        let staged = staged_path(&staging, "localview", name);
        fs::create_dir_all(staged.parent().unwrap()).unwrap();
        fs::write(&staged, b"sealed").unwrap();

        // and the uploader is shut down (forcing one final drain after the stop flag)
        uploader.shutdown();

        // Then the segment finalized after the last periodic scan is still shipped, partitioned
        // under <type>/<date>/. The file:// dest dir is itself the object-store prefix.
        let shipped = dest.path().join("localview/2026-06-19").join(name);
        assert!(
            shipped.exists(),
            "final drain should ship the late segment: {shipped:?}"
        );
    }

    #[test]
    fn segment_prefix_is_type_then_optional_date() {
        // The single source of truth for the upload/replay layout: <base>/<type>[/<date>].
        let base = ObjectPath::from("bkt");
        assert_eq!(
            segment_prefix(&base, "localview", Some("2026-06-19")).as_ref(),
            "bkt/localview/2026-06-19"
        );
        // A `None` date stops at the type level, so a replay can list every day for the type.
        assert_eq!(
            segment_prefix(&base, "localview", None).as_ref(),
            "bkt/localview"
        );
    }

    #[test]
    fn build_object_store_rejects_unknown_scheme() {
        let err = build_object_store("gs://bucket/prefix", RetryConfig::default()).unwrap_err();
        assert!(err.to_string().contains("unsupported"), "{err}");
    }

    #[test]
    fn build_object_store_parses_file_uri_prefix() {
        let (_store, base) =
            build_object_store("file:///tmp/archive-out", RetryConfig::default()).unwrap();
        assert_eq!(base.as_ref(), "tmp/archive-out");
    }
}
