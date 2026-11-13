//! One scan pass: which segments to attempt, how many at once, and what each outcome means.

use std::collections::HashMap;
use std::io::ErrorKind;
use std::path::{Path as StdPath, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, PutOptions, PutPayload, TagSet};
use tokio::task::{Id as TaskId, JoinError, JoinSet};

use super::ledger::Ledger;
use super::listing::list_segments;

/// Everything a scan pass needs and nothing that changes between them: where the segments are,
/// where they go, and the policy for getting them there. Built once in
/// [`Uploader::start`](super::Uploader::start).
pub(super) struct Uploads {
    /// Local staging root the pass walks; the archive writer owns its layout.
    pub(super) dir: PathBuf,
    pub(super) store: Arc<dyn ObjectStore>,
    /// Key prefix from the upload URI's path.
    pub(super) base: ObjectPath,
    /// Segments in flight at once (see
    /// [`UploadConfig::concurrency`](crate::config::UploadConfig::concurrency)).
    pub(super) concurrency: usize,
    /// Gap between passes; also the unit of a failing segment's retry backoff.
    pub(super) scan_interval: Duration,
    /// Ceiling on that backoff, so a long-failing segment is still retried periodically.
    pub(super) max_backoff: Duration,
    /// Deadline on the final drain (see [`Attempt::Everything`]).
    pub(super) shutdown_grace: Duration,
    /// The static object tags every PUT carries.
    pub(super) tags: TagSet,
}

/// What became of one attempted segment.
#[derive(Debug)]
enum Outcome {
    /// In the store, local copy reclaimed.
    Shipped,
    /// Its file was already gone. Nothing but this uploader deletes a finalized segment, so the
    /// data was destroyed undelivered — reported, not quietly forgotten.
    Vanished,
    /// Still on disk for a later pass; the message is what gets recorded against it.
    Failed(String),
}

/// Which of the ledger's segments a pass attempts.
#[derive(Clone, Copy)]
pub(super) enum Attempt {
    /// Only those whose retry backoff has elapsed — every periodic pass. Deferring costs nothing
    /// there, because the next pass is coming and the file is still on disk waiting for it.
    Due,
    /// Every pending segment, backoff or not — the final drain. There is no next pass and the
    /// staging dir goes away with the task, so a segment not attempted here is not deferred but
    /// lost; the store may also have recovered since the failure that set the backoff. Bounded by
    /// `shutdown_grace` instead, so a broken store cannot hold the process past its stop timeout.
    Everything,
}

impl Uploads {
    /// One scan pass over `dir`: each finalized `*.tfrecord.gz` is PUT to `<base>` + its path
    /// relative to the staging root, then deleted locally. Open `.partial`s are skipped — the
    /// uploader never reads a file being written, and mirroring the path rather than parsing it
    /// leaves the layout the writer's to change.
    ///
    /// Never fatal: failures are logged, counted and recorded against the segment, which decides
    /// when it may be tried again. [`Attempt::Due`] leaves a backing-off segment alone, so an
    /// outage costs a pass only what is due; [`Attempt::Everything`] ignores the backoff because
    /// it is the last pass. Either way selection arrives oldest data first, so a pass cut short
    /// leaves the newest data behind.
    ///
    /// `concurrency` bounds the segments in flight, and so the pass's peak memory — each upload
    /// holds its whole segment from the read until the PUT resolves. The window refills one at a
    /// time as uploads finish, rather than queueing work that would hold memory anyway.
    pub(super) async fn drain(&self, ledger: &mut Ledger, now: Instant, attempt: Attempt) {
        // The walk only teaches the ledger what is on disk; everything after this reasons about
        // the ledger, which knows every segment seen and not yet shipped.
        ledger.remember(&list_segments(&self.dir));
        // On a periodic pass a failing segment is left alone until its backoff elapses. During a
        // store outage that is nearly all of them, which is what keeps a pass from grinding
        // through the whole backlog every scan to collect the same errors.
        let mut selected = match attempt {
            Attempt::Due => ledger.due(now),
            Attempt::Everything => ledger.all(),
        }
        .into_iter();

        let mut tasks: JoinSet<(PathBuf, Outcome)> = JoinSet::new();
        // Which segment each in-flight task carries: a panicking task returns nothing, not even
        // its path, so this is what a `JoinError` is resolved against. Bounded by `concurrency`.
        let mut in_flight: HashMap<TaskId, PathBuf> = HashMap::new();
        loop {
            while tasks.len() < self.concurrency {
                let Some(path) = selected.next() else { break };
                // Every entry came from a walk rooted at the same `self.dir` this strips against,
                // and `strip_prefix` is lexical, so a key always builds. The key it would invent
                // otherwise is where a segment's only remaining copy would go.
                let Some(key) = self.object_key_for(&path) else {
                    panic!("segment path outside the staging dir: {}", path.display())
                };
                let id = self.spawn_upload(&mut tasks, path.clone(), key);
                in_flight.insert(id, path);
            }
            let Some(result) = tasks.join_next_with_id().await else {
                break;
            };
            match result {
                Ok((id, (path, outcome))) => {
                    in_flight.remove(&id);
                    match outcome {
                        Outcome::Shipped => ledger.shipped(&path),
                        Outcome::Vanished => ledger.vanished(&path),
                        Outcome::Failed(error) => {
                            ledger.failed(&path, error, now, self.scan_interval, self.max_backoff)
                        }
                    }
                }
                Err(e) => self.record_join_error(&e, &mut in_flight, ledger, now),
            }
        }

        // Published after the pass, so the gauges describe where the backlog now stands rather
        // than what this pass happened to attempt.
        ledger.publish_state(now);
    }

    /// Where one segment ships: its staged path mirrored under `base`. `None` when the path does
    /// not sit under `dir` at all, which its origin already rules out — see the caller.
    fn object_key_for(&self, path: &StdPath) -> Option<ObjectPath> {
        let rel = path.strip_prefix(&self.dir).ok()?;
        Some(object_key(&self.base, rel))
    }

    /// Spawn one segment's upload onto `tasks`, reporting the path back with its outcome so the
    /// caller can record it against the ledger.
    /// Record a task that ended without returning its `(path, outcome)`. The `JoinError` carries
    /// only a task id, which `in_flight` maps back to the segment.
    ///
    /// Cancellation is the shutdown deadline dropping the `JoinSet`, with nothing worth recording.
    /// A panic is a bug, and its segment backs off like any other failure — otherwise one that
    /// panics deterministically stays due at zero failures and is retried by every pass.
    fn record_join_error(
        &self,
        error: &JoinError,
        in_flight: &mut HashMap<TaskId, PathBuf>,
        ledger: &mut Ledger,
        now: Instant,
    ) {
        let path = in_flight.remove(&error.id());
        if error.is_cancelled() {
            return;
        }
        tracing::warn!(error = %error, segment = ?path, "archive upload task panicked");
        metrics::counter!("nordpool.archive.upload_failures", "cause" => "panic").increment(1);
        if let Some(path) = path {
            ledger.failed(
                &path,
                "upload task panicked".to_string(),
                now,
                self.scan_interval,
                self.max_backoff,
            );
        }
    }

    /// Returns the task's id, which the caller maps back to `path` — a `JoinError` carries
    /// nothing else.
    fn spawn_upload(
        &self,
        tasks: &mut JoinSet<(PathBuf, Outcome)>,
        path: PathBuf,
        key: ObjectPath,
    ) -> TaskId {
        let store = Arc::clone(&self.store);
        let tags = self.tags.clone();
        tasks
            .spawn(async move {
                let outcome = upload_one(store.as_ref(), &path, &key, tags).await;
                (path, outcome)
            })
            .id()
    }
}

/// Read `path` and PUT it to `key`, logging and counting `upload_failures` here, where the cause
/// is known.
///
/// A `NotFound` read is not a failure to retry but a segment **gone**: nothing except this uploader
/// deletes a finalized segment, so data was destroyed before it reached the store. The read goes
/// through `tokio::fs`, since a blocking one would hold the pass's current-thread runtime and stall
/// every other in-flight PUT.
async fn put_segment(
    store: &dyn ObjectStore,
    path: &StdPath,
    key: &ObjectPath,
    tags: TagSet,
) -> Result<u64, Outcome> {
    let bytes = match tokio::fs::read(path).await {
        Ok(b) => b,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            tracing::error!(
                path = %path.display(),
                "archive segment disappeared before it was uploaded — undelivered data lost"
            );
            metrics::counter!("nordpool.archive.upload_failures", "cause" => "vanished")
                .increment(1);
            return Err(Outcome::Vanished);
        }
        Err(e) => {
            tracing::warn!(error = %e, path = %path.display(), "reading archive segment for upload failed");
            metrics::counter!("nordpool.archive.upload_failures", "cause" => "read").increment(1);
            return Err(Outcome::Failed(e.to_string()));
        }
    };
    let len = bytes.len() as u64;
    let opts = PutOptions {
        tags,
        ..Default::default()
    };
    if let Err(e) = store.put_opts(key, PutPayload::from(bytes), opts).await {
        tracing::warn!(error = %e, key = %key, "uploading archive segment failed");
        metrics::counter!("nordpool.archive.upload_failures", "cause" => "put").increment(1);
        return Err(Outcome::Failed(e.to_string()));
    }
    Ok(len)
}

/// each component of `rel` (`<type>/<date>/<name>`). The archive writer fixed this layout when it
/// created the file; the uploader copies it verbatim, so there is no partition logic here.
fn object_key(base: &ObjectPath, rel: &StdPath) -> ObjectPath {
    let mut key = base.clone();
    for part in rel.components().filter_map(|c| c.as_os_str().to_str()) {
        key = key.join(part);
    }
    key
}

/// Upload one segment and delete the local copy. A failure — the PUT, or the delete after a
/// successful PUT — leaves the file for the next pass, where re-PUTting identical bytes is
/// harmless.
async fn upload_one(
    store: &dyn ObjectStore,
    path: &StdPath,
    key: &ObjectPath,
    tags: TagSet,
) -> Outcome {
    let len = match put_segment(store, path, key, tags).await {
        Ok(len) => len,
        Err(outcome) => return outcome,
    };
    metrics::counter!("nordpool.archive.uploaded").increment(1);
    metrics::counter!("nordpool.archive.upload_bytes").increment(len);
    tracing::info!(
        source = %path.display(),
        destination = %key,
        bytes = len,
        "uploaded archive segment"
    );

    // Blocking, unlike the read above: a metadata call costs microseconds, and staying sync keeps
    // any await — and so any cancellation point, the shutdown deadline's included — from landing
    // between a successful PUT and reclaiming its bytes.
    //
    // The emptied `<type>/<date>/` directory is deliberately left behind. Removing it would race
    // the writer, whose `open_segment` creates the day directory and then the segment file in it:
    // a prune landing between those two calls fails the create with ENOENT, and an ArchiveSink
    // error takes the process down. An empty directory costs an inode and a readdir per scan; the
    // writer reuses today's, so only past days linger, in what is a staging area either way.
    match std::fs::remove_file(path) {
        Ok(()) => Outcome::Shipped,
        Err(e) => {
            // Shipped but not deleted: harmless for the data (the re-PUT next pass is
            // idempotent) but the disk is not reclaimed, so it counts as a failure — otherwise
            // the backlog climbs with nothing to explain it.
            tracing::warn!(error = %e, path = %path.display(), "deleting uploaded archive segment failed");
            metrics::counter!("nordpool.archive.upload_failures", "cause" => "remove_file")
                .increment(1);
            Outcome::Failed(e.to_string())
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use object_store::local::LocalFileSystem;

    use super::super::testing::{
        TempDir, base_tags, block_on, local_store, staged_path, staging_with,
    };
    use super::*;

    /// easy to step over in a test.
    fn uploads(
        dir: &StdPath,
        store: Arc<dyn ObjectStore>,
        base: ObjectPath,
        concurrency: usize,
    ) -> Uploads {
        Uploads {
            dir: dir.to_path_buf(),
            store,
            base,
            concurrency,
            scan_interval: Duration::from_secs(60),
            max_backoff: Duration::from_secs(3600),
            shutdown_grace: Duration::from_secs(10),
            tags: base_tags(),
        }
    }

    /// Run one pass with a throwaway ledger, for the tests that don't care about retry state.
    fn drain_once(uploads: &Uploads) {
        block_on(uploads.drain(&mut Ledger::default(), Instant::now(), Attempt::Due));
    }
    #[test]
    fn drain_ships_finalized_and_skips_the_open_partial() {
        // Given a staging dir holding both segment states a scan encounters: one finalized
        // `.tfrecord.gz` (sealed, immutable, ready to ship) and one open `.tfrecord.partial`
        // (still being written) under the same `localview` type subdir.
        let staging = staging_with(
            "localview",
            &["localview-20260619T101010.000Z-000.tfrecord.gz"],
            &["localview-20260619T101111.000Z-001.tfrecord.partial"],
        );
        let (store, base, dest) = local_store();
        let local_partial = staged_path(
            &staging,
            "localview",
            "localview-20260619T101111.000Z-001.tfrecord.partial",
        );
        let local_final = staged_path(
            &staging,
            "localview",
            "localview-20260619T101010.000Z-000.tfrecord.gz",
        );

        // When a single drain pass runs.
        drain_once(&uploads(staging.path(), store, base, 4));

        // Then the sealed segment is uploaded under its own `.tfrecord.gz` name and removed locally —
        // the normal finalized-segment lifecycle: once it is safely in the object store there is
        // no reason to keep the local copy.
        let finalized = dest
            .path()
            .join("localview/2026-06-19/localview-20260619T101010.000Z-000.tfrecord.gz");
        assert!(finalized.exists(), "sealed segment should be uploaded");
        assert!(
            !local_final.exists(),
            "sealed segment should be deleted locally"
        );

        // And the open partial is not uploaded at all — only complete segments ship (the
        // writer's age-based finalize bounds how long it stays open) — and it stays on disk,
        // untouched, for the writer to finish.
        let day_dest = dest.path().join("localview/2026-06-19");
        let uploaded: Vec<_> = fs::read_dir(&day_dest)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        assert_eq!(
            uploaded,
            vec!["localview-20260619T101010.000Z-000.tfrecord.gz"],
            "only the finalized segment may be uploaded"
        );
        assert!(
            local_partial.exists(),
            "the open partial must not be deleted"
        );
    }

    #[test]
    fn upload_deletes_local_after_success() {
        // Given a single sealed segment staged for upload — local disk is only a transient
        // staging area for the object store, which is the archive of record.
        let staging = staging_with(
            "contract",
            &["contract-20260619T090000.000Z-000.tfrecord.gz"],
            &[],
        );
        let (store, base, _dest) = local_store();
        let local = staged_path(
            &staging,
            "contract",
            "contract-20260619T090000.000Z-000.tfrecord.gz",
        );

        // When the drain runs and the PUT succeeds (a local file:// store never fails the PUT).
        drain_once(&uploads(staging.path(), store, base, 4));

        // Then the local copy is removed, so a successful upload reclaims the disk rather than
        // letting already-shipped segments accumulate on an ephemeral volume.
        assert!(
            !local.exists(),
            "local segment should be deleted after upload"
        );

        // And its now-empty day directory is left alone: it belongs to the writer, which creates
        // it and then the segment file in it, so removing it here would race that pair and fail
        // an archive write with ENOENT. An empty directory is the cheaper of the two.
        assert!(
            local.parent().unwrap().is_dir(),
            "the day directory belongs to the writer and must survive the upload"
        );
    }

    #[test]
    fn object_key_partitions_by_type_and_date_under_the_base_prefix() {
        // Given a non-empty base prefix ("archive/run1", the upload-URI path) and a segment under a
        // type subdir, so the test can check the full partitioned key — base prefix, the per-type
        // dir, and the per-day partition.
        let staging = staging_with(
            "public_statistic",
            &["public_statistic-20260619T120000.000Z-000.tfrecord.gz"],
            &[],
        );
        let dest = TempDir::new();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(dest.path()).unwrap());
        let base = ObjectPath::from("archive/run1");

        // When the drain runs against the prefixed store.
        drain_once(&uploads(staging.path(), store, base, 4));

        // Then the object key is composed as <prefix>/<type>/<date>/<name> — the type subdir and the
        // UTC day (parsed from the filename) are mirrored into the object store under the configured
        // base prefix.
        let key = dest.path().join(
            "archive/run1/public_statistic/2026-06-19/public_statistic-20260619T120000.000Z-000.tfrecord.gz",
        );
        assert!(
            key.exists(),
            "object key should be <prefix>/<type>/<date>/<name>"
        );
    }

    #[test]
    fn every_segment_ships_when_the_backlog_exceeds_the_concurrency_cap() {
        // Given more staged segments than the pass may upload at once — the shape of a backlog
        // built up during a store outage, which is exactly when the cap binds.
        let staging = staging_with(
            "localview",
            &[
                "localview-20260619T101010.000Z-000.tfrecord.gz",
                "localview-20260619T101110.000Z-001.tfrecord.gz",
                "localview-20260619T101210.000Z-002.tfrecord.gz",
                "localview-20260619T101310.000Z-003.tfrecord.gz",
                "localview-20260619T101410.000Z-004.tfrecord.gz",
            ],
            &[],
        );
        let (store, base, dest) = local_store();

        // When a pass runs with a cap of one, the tightest the config allows.
        drain_once(&uploads(staging.path(), store, base, 1));

        // Then every segment still ships and every local copy is reclaimed: the cap paces the
        // pass, it does not truncate it — a segment left behind would be silent data loss once
        // the staging disk is recycled.
        let day = dest.path().join("localview/2026-06-19");
        let mut uploaded: Vec<String> = fs::read_dir(&day)
            .unwrap()
            .map(|e| e.unwrap().file_name().into_string().unwrap())
            .collect();
        uploaded.sort();
        assert_eq!(uploaded.len(), 5, "{uploaded:?}");
        assert!(
            files_under(staging.path()).is_empty(),
            "every shipped segment should be deleted locally"
        );
    }

    /// Every segment remaining under `dir`, recursively.
    fn files_under(dir: &StdPath) -> Vec<PathBuf> {
        list_segments(dir)
    }

    /// A store rooted at a regular file, so every PUT under it fails — a stand-in for an
    /// unreachable or refusing object store, without the network.
    fn failing_store(blocker: &TempDir) -> Arc<dyn ObjectStore> {
        let root = blocker.path().join("not-a-dir");
        fs::write(&root, b"x").unwrap();
        Arc::new(LocalFileSystem::new_with_prefix(&root).unwrap())
    }

    #[test]
    fn a_failing_segment_backs_off_and_is_retried_once_its_wait_elapses() {
        // Given a segment whose upload fails — a refusing store, the shape of an outage or a
        // permission problem.
        let staging = staging_with(
            "localview",
            &["localview-20260619T101010.000Z-000.tfrecord.gz"],
            &[],
        );
        let segment = staged_path(
            &staging,
            "localview",
            "localview-20260619T101010.000Z-000.tfrecord.gz",
        );
        let blocker = TempDir::new();
        let uploads = uploads(
            staging.path(),
            failing_store(&blocker),
            ObjectPath::default(),
            4,
        );
        let mut ledger = Ledger::default();
        let start = Instant::now();

        // When a first pass attempts it and fails.
        block_on(uploads.drain(&mut ledger, start, Attempt::Due));
        assert_eq!(ledger.len(), 1, "the failure should be remembered");
        let after_first = ledger.get(&segment).unwrap().failures;

        // Then a pass a moment later leaves it alone rather than re-reading and re-PUTting it:
        // during an outage that is the whole backlog, and retrying all of it every scan is what
        // makes a pass — and the shutdown drain — cost the entire ledger's retry ladders.
        block_on(uploads.drain(&mut ledger, start + Duration::from_secs(1), Attempt::Due));
        assert_eq!(
            ledger.get(&segment).unwrap().failures,
            after_first,
            "a deferred segment must not be attempted again"
        );

        // And once its backoff has elapsed it is attempted again, so a segment is never
        // abandoned — the store may have come back.
        block_on(uploads.drain(&mut ledger, start + Duration::from_secs(600), Attempt::Due));
        assert!(
            ledger.get(&segment).unwrap().failures > after_first,
            "an elapsed backoff must let the segment retry"
        );
    }

    #[test]
    fn the_final_drain_attempts_a_segment_still_inside_its_backoff() {
        // Given a segment that has just failed against a refusing store, so its backoff has not
        // elapsed — and a store that now works, the shape of a deploy landing shortly after a
        // transient outage recovered.
        let staging = staging_with(
            "localview",
            &["localview-20260619T101010.000Z-000.tfrecord.gz"],
            &[],
        );
        let segment = staged_path(
            &staging,
            "localview",
            "localview-20260619T101010.000Z-000.tfrecord.gz",
        );
        let blocker = TempDir::new();
        let mut ledger = Ledger::default();
        let start = Instant::now();
        let failing = uploads(
            staging.path(),
            failing_store(&blocker),
            ObjectPath::default(),
            4,
        );
        block_on(failing.drain(&mut ledger, start, Attempt::Due));
        assert!(
            segment.exists(),
            "the failed segment should still be staged"
        );
        let (store, base, dest) = local_store();
        let working = uploads(staging.path(), store, base, 4);

        // When the periodic pass runs a moment later it still defers — the backoff is a bet that a
        // later pass is coming, and for a periodic pass it is.
        block_on(working.drain(&mut ledger, start + Duration::from_secs(1), Attempt::Due));
        assert!(
            segment.exists(),
            "a periodic pass must leave a deferred segment alone"
        );

        // Then the final drain attempts it anyway and it ships. At shutdown the bet is off: the
        // staging dir goes away with the task, so honouring the backoff here would not defer the
        // segment but lose it.
        block_on(working.drain(
            &mut ledger,
            start + Duration::from_secs(2),
            Attempt::Everything,
        ));
        assert!(
            dest.path()
                .join("localview/2026-06-19/localview-20260619T101010.000Z-000.tfrecord.gz")
                .exists(),
            "the final drain should ship a segment whose backoff has not elapsed"
        );
        assert!(!segment.exists(), "and reclaim the local copy");
        assert_eq!(ledger.len(), 0, "and forget it");
    }

    #[test]
    fn a_vanished_segment_is_reported_as_lost_and_a_shipped_one_forgotten() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        // Given two failing segments recorded in the ledger.
        let staging = staging_with(
            "localview",
            &[
                "localview-20260619T101010.000Z-000.tfrecord.gz",
                "localview-20260619T101110.000Z-001.tfrecord.gz",
            ],
            &[],
        );
        let blocker = TempDir::new();
        let mut ledger = Ledger::default();
        let failing = uploads(
            staging.path(),
            failing_store(&blocker),
            ObjectPath::default(),
            4,
        );
        block_on(failing.drain(&mut ledger, Instant::now(), Attempt::Due));
        assert_eq!(ledger.len(), 2);

        // When one segment is removed behind the uploader's back and the other now ships (a
        // working store, and every segment's backoff long elapsed).
        fs::remove_file(staged_path(
            &staging,
            "localview",
            "localview-20260619T101110.000Z-001.tfrecord.gz",
        ))
        .unwrap();
        let (store, base, _dest) = local_store();
        let working = uploads(staging.path(), store, base, 4);
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || {
            block_on(working.drain(
                &mut ledger,
                Instant::now() + Duration::from_secs(7200),
                Attempt::Due,
            ));
        });

        // Then the ledger is empty: the shipped one is forgotten, and the vanished one dropped
        // because retrying a file that no longer exists would report a backlog that does not.
        assert_eq!(ledger.len(), 0);

        // And the loss is reported rather than quietly swallowed. Nothing but this uploader deletes
        // a finalized segment, so a missing one means undelivered archive data was destroyed —
        // its own cause, under the counter one monitor already watches.
        let snapshot = snapshotter.snapshot().into_vec();
        let (_, _, _, value) = snapshot
            .iter()
            .find(|(key, ..)| {
                key.key().name() == "nordpool.archive.upload_failures"
                    && key
                        .key()
                        .labels()
                        .any(|l| l.key() == "cause" && l.value() == "vanished")
            })
            .expect("a vanished segment is counted under cause:vanished");
        assert_eq!(*value, DebugValue::Counter(1));
    }

    #[test]
    fn an_unreadable_archive_dir_counts_and_keeps_reporting_its_backlog() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        // Given an archive root the pass cannot list — here a regular file where the directory
        // should be, standing in for a lost mount or a permission change. The walk finds no
        // segments, which must not be read as "nothing pending": that would be an all-clear
        // announcing the absence of exactly the files it failed to look for.
        let root = TempDir::new();
        let unreadable = root.path().join("archive");
        fs::write(&unreadable, b"x").unwrap();
        let (store, base, _dest) = local_store();
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        // And a ledger already tracking a segment's failures under that root, which the pass now
        // has no way to see.
        let mut ledger = Ledger::default();
        let tracked = unreadable.join("localview/2026-06-19/localview-x-000.tfrecord.gz");
        ledger.remember(std::slice::from_ref(&tracked));
        ledger.failed(
            &tracked,
            "boom".into(),
            Instant::now(),
            Duration::from_secs(60),
            Duration::from_secs(3600),
        );

        // When a drain pass runs against it.
        let uploads = uploads(&unreadable, store, base, 4);
        metrics::with_local_recorder(&recorder, || {
            block_on(uploads.drain(&mut ledger, Instant::now(), Attempt::Due));
        });

        // Then the entry survives: the stat under the unreadable root fails with something other
        // than NotFound, which proves nothing about the file — and forgetting on that would
        // silently reset the segment's backoff, making the next readable pass retry the whole
        // backlog at once.
        assert!(
            ledger.get(&tracked).is_some(),
            "a directory the walk could not read must not evict its segments"
        );

        // Then the unreadable directory is counted as an upload failure under `cause:list`, so
        // one monitor covers every way a segment fails to ship...
        let snapshot = snapshotter.snapshot().into_vec();
        let (key, _, _, value) = snapshot
            .iter()
            .find(|(key, ..)| key.key().name() == "nordpool.archive.upload_failures")
            .expect("listing failure counted");
        assert!(
            key.key()
                .labels()
                .any(|l| l.key() == "cause" && l.value() == "list"),
            "expected cause:list"
        );
        assert_eq!(*value, DebugValue::Counter(1));

        // ...and the backlog still reports the segment the walk couldn't see, rather than the 0
        // that would silence the very alert this should raise. The ledger accumulates by
        // observation, so a walk that learns nothing leaves the count exactly as it was.
        let (_, _, _, pending) = snapshot
            .iter()
            .find(|(key, ..)| key.key().name() == "nordpool.archive.pending_uploads")
            .expect("backlog published");
        assert_eq!(*pending, DebugValue::Gauge(1.0.into()));
    }

    #[test]
    #[should_panic(expected = "outside the staging dir")]
    fn a_segment_outside_the_staging_dir_is_fatal() {
        // Given a ledger entry for a file that exists but sits outside the staging root, so its
        // path cannot be made relative to it — the one shape `object_key_for` rejects. Entries
        // only ever come from a walk rooted there, so this is unreachable; the point is what
        // happens if that ever stops being true.
        let staging = TempDir::new();
        let name = "localview-20260619T101010.000Z-000.tfrecord.gz";
        let elsewhere = staging_with("localview", &[name], &[]);
        let stray = staged_path(&elsewhere, "localview", name);
        let (store, base, _dest) = local_store();
        let uploads = uploads(staging.path(), store, base, 4);
        let mut ledger = Ledger::default();
        ledger.remember(std::slice::from_ref(&stray));

        // When a pass works through it, then it fails loudly rather than shipping the segment to
        // an object key invented from a layout the uploader does not own — that key is where the
        // only remaining copy of a segment would go.
        block_on(uploads.drain(&mut ledger, Instant::now(), Attempt::Due));
    }

    #[test]
    fn a_panicking_upload_task_is_recorded_against_the_segment_it_was_carrying() {
        // Given a segment in the ledger, in flight on a task that panics — so it returns neither
        // its outcome nor the path it was given, and the JoinError names only the task's id.
        let staging = staging_with(
            "localview",
            &["localview-20260619T101010.000Z-000.tfrecord.gz"],
            &[],
        );
        let segment = staged_path(
            &staging,
            "localview",
            "localview-20260619T101010.000Z-000.tfrecord.gz",
        );
        let (store, base, _dest) = local_store();
        let uploads = uploads(staging.path(), store, base, 4);
        let mut ledger = Ledger::default();
        ledger.remember(std::slice::from_ref(&segment));
        let now = Instant::now();

        // When the pass records that JoinError, mapping the id back through `in_flight`.
        block_on(async {
            let mut tasks: JoinSet<(PathBuf, Outcome)> = JoinSet::new();
            let id = tasks
                .spawn(async { panic!("a bug in the upload path") })
                .id();
            let mut in_flight = HashMap::from([(id, segment.clone())]);
            let error = tasks
                .join_next_with_id()
                .await
                .expect("one task")
                .expect_err("it panicked");
            uploads.record_join_error(&error, &mut in_flight, &mut ledger, now);
            assert!(in_flight.is_empty(), "the mapping is consumed");
        });

        // Then the segment took a failure and is waiting out a backoff, rather than staying due at
        // zero failures — which is what had it attempted again on every pass, forever.
        assert_eq!(
            ledger.get(&segment).map(|segment| segment.failures),
            Some(1)
        );
        assert!(
            !ledger.due(now).contains(&segment),
            "a panicked segment must back off like any other failure"
        );
    }

    #[test]
    fn a_cancelled_upload_task_leaves_its_segment_alone() {
        // Given a segment in flight on a task cancelled by the shutdown deadline dropping the
        // JoinSet — the process is exiting and the staging dir goes with it.
        let staging = staging_with(
            "localview",
            &["localview-20260619T101010.000Z-000.tfrecord.gz"],
            &[],
        );
        let segment = staged_path(
            &staging,
            "localview",
            "localview-20260619T101010.000Z-000.tfrecord.gz",
        );
        let (store, base, _dest) = local_store();
        let uploads = uploads(staging.path(), store, base, 4);
        let mut ledger = Ledger::default();
        ledger.remember(std::slice::from_ref(&segment));
        let now = Instant::now();

        // When the resulting JoinError is recorded.
        block_on(async {
            let mut tasks: JoinSet<(PathBuf, Outcome)> = JoinSet::new();
            let handle = tasks.spawn(async {
                std::future::pending::<()>().await;
                unreachable!()
            });
            let id = handle.id();
            handle.abort();
            let mut in_flight = HashMap::from([(id, segment.clone())]);
            let error = tasks
                .join_next_with_id()
                .await
                .expect("one task")
                .expect_err("it was cancelled");
            assert!(error.is_cancelled());
            uploads.record_join_error(&error, &mut in_flight, &mut ledger, now);
        });

        // Then nothing is recorded against it: there is no later pass for a backoff to defer to,
        // and counting a shutdown as an upload failure would page for an ordinary stop.
        assert_eq!(
            ledger.get(&segment).map(|segment| segment.failures),
            Some(0)
        );
    }

    #[test]
    fn upload_failure_is_non_fatal_and_leaves_file() {
        // Given a store deliberately rooted at a regular *file* (not a directory), so any PUT that
        // tries to create a child path under it fails — a stand-in for a real upload failure (S3
        // outage, permission error) without needing the network.
        let staging = staging_with(
            "localview",
            &["localview-20260619T101010.000Z-000.tfrecord.gz"],
            &[],
        );
        let blocker = TempDir::new();
        let file_root = blocker.path().join("not-a-dir");
        fs::write(&file_root, b"x").unwrap();
        let store: Arc<dyn ObjectStore> =
            Arc::new(LocalFileSystem::new_with_prefix(&file_root).unwrap());
        let local = staged_path(
            &staging,
            "localview",
            "localview-20260619T101010.000Z-000.tfrecord.gz",
        );

        // When the drain runs against the broken store — an upload failure must be non-fatal: the
        // worker logs and counts it but the pass completes normally, never unwinding the thread.
        drain_once(&uploads(staging.path(), store, ObjectPath::default(), 4));

        // Then the local segment is left untouched on disk — at-least-once delivery means a failed
        // upload is never deleted, so the next scan can retry it.
        assert!(
            local.exists(),
            "failed upload must leave the file for retry"
        );
    }
}
