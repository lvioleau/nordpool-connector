//! What the uploader knows about the segments waiting to ship: every one it has seen and not yet
//! shipped, with the failures and backoff that decide when each may be tried again.

use std::collections::HashMap;
use std::path::{Path as StdPath, PathBuf};
use std::time::{Duration, Instant, SystemTime};

use crate::sink::segment_open_time;

/// Every finalized segment the uploader has seen on disk and not yet shipped.
///
/// An entry leaves only where its file does — [`shipped`](Self::shipped) or
/// [`vanished`](Self::vanished) — never because a walk failed to list it, since an unreadable
/// directory looks exactly like an empty one.
///
/// Not durable: a restart retries everything once, which is right anyway (new credentials, new
/// network path). Data age survives regardless — it comes from the filename, not this map.
#[derive(Default)]
pub(super) struct Ledger {
    segments: HashMap<PathBuf, Segment>,
}

/// One segment's upload history: when its data starts, how many attempts have failed, and when
/// the next may run.
pub(super) struct Segment {
    /// When the segment's *data* starts, read from its filename — so the age gauge and the upload
    /// order measure undelivered history rather than this process's uptime. An unstamped name
    /// (nothing the archive writes) falls back to discovery time and so ships last.
    opened_at: SystemTime,
    pub(super) failures: u32,
    /// `None` until the segment has failed — an unattempted segment is due immediately.
    next_try: Option<Instant>,
}

impl Segment {
    /// A segment just discovered: never attempted, so due now.
    fn new(path: &StdPath) -> Self {
        let opened_at = path
            .file_name()
            .and_then(|name| name.to_str())
            .and_then(segment_open_time)
            .unwrap_or_else(SystemTime::now);
        Self {
            opened_at,
            failures: 0,
            next_try: None,
        }
    }

    fn is_due(&self, now: Instant) -> bool {
        self.next_try.is_none_or(|next_try| now >= next_try)
    }

    /// One more failure, and the next try pushed out exponentially; returns the wait to log.
    /// The only writer of `next_try`, as [`is_due`](Self::is_due) is its only reader.
    fn record_failure(
        &mut self,
        now: Instant,
        scan_interval: Duration,
        max_backoff: Duration,
    ) -> Duration {
        self.failures += 1;
        let wait = backoff(self.failures, scan_interval, max_backoff);
        self.next_try = Some(now + wait);
        wait
    }
}

impl Ledger {
    /// Fold one walk of the staging dir in. Add-only: absence proves nothing here, since a walk
    /// that could not read a directory reports it the same way as one that found nothing.
    pub(super) fn remember(&mut self, segments: &[PathBuf]) {
        for path in segments {
            self.segments
                .entry(path.clone())
                .or_insert_with(|| Segment::new(path));
        }
    }

    /// What may be attempted now: never attempted, or backoff elapsed. Cloned out so the caller
    /// can record outcomes against the ledger while working through them.
    pub(super) fn due(&self, now: Instant) -> Vec<PathBuf> {
        self.oldest_first(|segment| segment.is_due(now))
    }

    /// Everything awaiting upload, due or not — the shutdown drain, where deferring a segment
    /// abandons it rather than putting it off.
    pub(super) fn all(&self) -> Vec<PathBuf> {
        self.oldest_first(|_| true)
    }

    /// The selected segments, oldest data first: a pass can be cut short (shutdown deadline, a
    /// store failing again), and what it leaves behind should be the newest data. The path breaks
    /// ties, which only arise across frame types, so a pass's order is reproducible.
    fn oldest_first(&self, keep: impl Fn(&Segment) -> bool) -> Vec<PathBuf> {
        let mut selected: Vec<_> = self
            .segments
            .iter()
            .filter(|(_, segment)| keep(segment))
            .collect();
        selected.sort_by(|(a_path, a), (b_path, b)| {
            a.opened_at.cmp(&b.opened_at).then(a_path.cmp(b_path))
        });
        selected.into_iter().map(|(path, _)| path.clone()).collect()
    }

    /// Publish the backlog's size *and* how stuck it is — five segments held for six hours is a
    /// worse position than fifty a minute old, and the count alone can't tell them apart:
    /// `pending_uploads`, `upload_deferred` (waiting out a backoff), `upload_max_failures` (a blip
    /// leaves everything at 1) and `upload_oldest_pending_secs` (how much history the staging disk
    /// is the only copy of).
    pub(super) fn publish_state(&self, now: Instant) {
        let deferred = self
            .segments
            .values()
            .filter(|segment| !segment.is_due(now))
            .count();
        let max_failures = self
            .segments
            .values()
            .map(|segment| segment.failures)
            .max()
            .unwrap_or(0);
        // Wall clock here, monotonic for the backoff: this compares against a stamp written into
        // a filename by a possibly long-dead process.
        let wall_now = SystemTime::now();
        let oldest = self
            .segments
            .values()
            .map(|segment| {
                wall_now
                    .duration_since(segment.opened_at)
                    .unwrap_or_default()
            })
            .max()
            .unwrap_or_default();

        metrics::gauge!("nordpool.archive.pending_uploads").set(self.segments.len() as f64);
        metrics::gauge!("nordpool.archive.upload_deferred").set(deferred as f64);
        metrics::gauge!("nordpool.archive.upload_max_failures").set(f64::from(max_failures));
        metrics::gauge!("nordpool.archive.upload_oldest_pending_secs").set(oldest.as_secs_f64());
    }

    /// Record a failed attempt against a segment and back it off.
    ///
    /// # Panics
    ///
    /// If `path` names no entry — impossible for a pass, which only attempts what it selected
    /// here. Swallowing it would leave the segment at zero failures, retried by every pass and
    /// backing off never; the dead thread at least trips the `upload_scans` monitor.
    pub(super) fn failed(
        &mut self,
        path: &StdPath,
        error: String,
        now: Instant,
        scan_interval: Duration,
        max_backoff: Duration,
    ) {
        let Some(segment) = self.segments.get_mut(path) else {
            panic!(
                "recording a failure against an unknown segment: {}",
                path.display()
            )
        };
        let wait = segment.record_failure(now, scan_interval, max_backoff);
        tracing::warn!(
            path = %path.display(),
            failures = segment.failures,
            retry_in_secs = wait.as_secs(),
            error = %error,
            "archive segment upload failing"
        );
    }

    /// Forget a segment that made it to the store.
    pub(super) fn shipped(&mut self, path: &StdPath) {
        self.segments.remove(path);
    }

    /// Forget a segment already gone when a pass went to read it — data destroyed undelivered,
    /// which the caller counts and logs. It is not coming back, and keeping it would report a
    /// backlog that does not exist.
    pub(super) fn vanished(&mut self, path: &StdPath) {
        self.segments.remove(path);
    }

    /// Segments awaiting upload — the backlog itself, since an entry lives exactly as long as the
    /// file it describes.
    pub(super) fn len(&self) -> usize {
        self.segments.len()
    }

    #[cfg(test)]
    pub(super) fn get(&self, path: &StdPath) -> Option<&Segment> {
        self.segments.get(path)
    }
}

/// How long to wait before retrying a segment that has failed `failures` times: one scan
/// interval, doubling, capped at `max_backoff`. The first retry therefore lands on the next
/// pass as before, and only a persistently failing segment backs off far enough to stop
/// costing anything.
fn backoff(failures: u32, scan_interval: Duration, max_backoff: Duration) -> Duration {
    // Clamp the exponent before shifting: the multiplier saturates long before this matters,
    // but `2u32.pow(32)` would panic on the way there.
    let doublings = failures.saturating_sub(1).min(16);
    scan_interval
        .saturating_mul(2u32.saturating_pow(doublings))
        .min(max_backoff)
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};

    use super::*;

    /// A segment path named the way the archive writer names them, opened `age` ago.
    fn staged(label: &str, age: Duration) -> PathBuf {
        let opened: chrono::DateTime<chrono::Utc> = (SystemTime::now() - age).into();
        PathBuf::from(format!(
            "{label}-{}-000.tfrecord.gz",
            opened.format("%Y%m%dT%H%M%S%.3fZ")
        ))
    }

    #[test]
    fn published_state_says_how_stuck_the_backlog_is_not_just_how_big() {
        // Given three segments whose data starts an hour back, two of which have failed — once and
        // twice — so both are waiting out a backoff, and one that has never been attempted.
        let scan = Duration::from_secs(60);
        let max = Duration::from_secs(3600);
        let now = Instant::now();
        let mut ledger = Ledger::default();
        let paths: Vec<PathBuf> = ["a", "b", "c"]
            .iter()
            .map(|label| staged(label, Duration::from_secs(3600)))
            .collect();
        for path in &paths {
            ledger.segments.insert(path.clone(), Segment::new(path));
        }
        ledger.failed(&paths[0], "boom".into(), now, scan, max);
        ledger.failed(&paths[1], "boom".into(), now, scan, max);
        ledger.failed(&paths[1], "boom".into(), now, scan, max);

        // When the ledger publishes its state.
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || ledger.publish_state(now));
        let snapshot = snapshotter.snapshot().into_vec();
        let gauge = |name: &str| {
            let (_, _, _, value) = snapshot
                .iter()
                .find(|(key, ..)| key.key().name() == name)
                .unwrap_or_else(|| panic!("{name} not published"));
            match value {
                DebugValue::Gauge(v) => v.into_inner(),
                other => panic!("expected a gauge for {name}, got {other:?}"),
            }
        };

        // Then the size is only the first of four answers: two of the three are backing off, the
        // worst has failed twice — a count that climbing separates "will resolve itself" from
        // "will not" — and the oldest has been undelivered for an hour, which is the measure of
        // how much history the staging disk is the only copy of.
        assert_eq!(gauge("nordpool.archive.pending_uploads"), 3.0);
        assert_eq!(gauge("nordpool.archive.upload_deferred"), 2.0);
        assert_eq!(gauge("nordpool.archive.upload_max_failures"), 2.0);
        let oldest = gauge("nordpool.archive.upload_oldest_pending_secs");
        assert!(
            (3600.0..3605.0).contains(&oldest),
            "expected ~3600s of undelivered history, got {oldest}"
        );
    }

    #[test]
    fn a_segments_age_comes_from_its_name_so_it_survives_a_restart() {
        // Given a fresh ledger — a just-restarted process — discovering a segment the archive
        // opened two hours ago and never managed to ship.
        let path = staged("localview", Duration::from_secs(7200));
        let mut ledger = Ledger::default();
        ledger.segments.insert(path.clone(), Segment::new(&path));

        // When it publishes the backlog's state.
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();
        metrics::with_local_recorder(&recorder, || ledger.publish_state(Instant::now()));

        // Then the age is the data's, read from the filename — not this process's, which would be
        // ~0 and would silence the staleness alert on exactly the crash-looping connector that
        // needs it most.
        let (_, _, _, value) = snapshotter
            .snapshot()
            .into_vec()
            .into_iter()
            .find(|(key, ..)| key.key().name() == "nordpool.archive.upload_oldest_pending_secs")
            .expect("age published");
        let DebugValue::Gauge(age) = value else {
            panic!("expected a gauge, got {value:?}")
        };
        assert!(
            (7200.0..7205.0).contains(&age.into_inner()),
            "expected the name's two hours, got {age:?}"
        );
    }

    #[test]
    fn selection_ships_the_oldest_data_first_across_frame_types() {
        // Given three pending segments of different ages and types, remembered in whatever order
        // the map happens to hold them.
        let newest = staged("localview", Duration::from_secs(60));
        let oldest = staged("contract", Duration::from_secs(7200));
        let middle = staged("public_statistic", Duration::from_secs(1800));
        let mut ledger = Ledger::default();
        for path in [&newest, &oldest, &middle] {
            ledger.segments.insert(path.clone(), Segment::new(path));
        }

        // When a pass asks what to upload — whether it is a periodic pass or the final drain.
        let due = ledger.due(Instant::now());
        let all = ledger.all();

        // Then the oldest data goes first, across types rather than grouped by them: a pass that
        // gets cut short should leave the newest data behind, not an arbitrary subset. Iteration
        // order of the map must not leak into delivery order.
        assert_eq!(due, vec![oldest.clone(), middle.clone(), newest.clone()]);
        assert_eq!(all, vec![oldest, middle, newest]);
    }

    #[test]
    fn backoff_doubles_from_one_scan_interval_and_stops_at_the_ceiling() {
        let scan = Duration::from_secs(60);
        let max = Duration::from_secs(600);
        // The first retry lands on the next pass, then each failure doubles the wait...
        assert_eq!(backoff(1, scan, max), Duration::from_secs(60));
        assert_eq!(backoff(2, scan, max), Duration::from_secs(120));
        assert_eq!(backoff(3, scan, max), Duration::from_secs(240));
        // ...until it reaches the ceiling, so a long-failing segment is still retried on a
        // schedule rather than abandoned — and the count can't overflow the doubling.
        assert_eq!(backoff(9, scan, max), max);
        assert_eq!(backoff(u32::MAX, scan, max), max);
    }
}
