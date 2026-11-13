//! Scheduled session recycling: deciding when to deliberately end a WebSocket session so the
//! reconnect re-subscribes and the server replays fresh snapshots, aligning archive segment
//! boundaries to the schedule (e.g. the UTC day). The read loop in `run` raises the
//! [`SessionExpired`](super::errors::SessionExpired) sentinel when [`reconnect_requested`] fires;
//! that sentinel lives with the other driver errors in `errors.rs`.

use std::time::{Duration, Instant};

use chrono::{DateTime, Timelike, Utc};

use crate::config::SessionReconnect;

/// The monotonic instant at which a session on `schedule` should recycle. `DailyUtc` resolves to
/// the next UTC midnight; `Every` to `now + interval`.
pub(super) fn session_deadline(schedule: SessionReconnect) -> Instant {
    let from_now = match schedule {
        SessionReconnect::DailyUtc => time_until_next_utc_midnight(Utc::now()),
        SessionReconnect::Every(interval) => interval,
    };
    Instant::now() + from_now
}

/// Whether the scheduled-reconnect `deadline` (if any) has arrived. `None` (no schedule) is
/// never due. Mirrors `shutdown_requested()` so the read loop reads as two parallel checks.
pub(super) fn reconnect_requested(deadline: Option<Instant>) -> bool {
    deadline.is_some_and(|at| Instant::now() >= at)
}

/// Wall-clock duration from `now` until the next UTC midnight (00:00:00Z). Pure (no clock read)
/// so the day arithmetic is unit-testable; the caller offsets a monotonic `Instant` by it.
fn time_until_next_utc_midnight(now: DateTime<Utc>) -> Duration {
    const DAY: u64 = 24 * 60 * 60;
    // `% 1e9` drops the leap-second flag chrono folds into the nanosecond field.
    let elapsed = Duration::from_secs(u64::from(now.num_seconds_from_midnight()))
        + Duration::from_nanos(u64::from(now.nanosecond() % 1_000_000_000));
    Duration::from_secs(DAY).saturating_sub(elapsed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    #[test]
    fn time_until_next_utc_midnight_is_the_remainder_of_the_day() {
        // Given three wall-clock instants spread across one UTC day (start of day, one hour
        // before midnight, one second before midnight) — chosen so the expected remainder is a
        // round, hand-checkable number. When / Then the pure day-arithmetic returns the exact
        // gap to the next 00:00:00Z: a full day from midnight, an hour from 23:00, a second from
        // 23:59:59 — this is the offset the caller adds to a monotonic Instant to set the
        // daily-recycle deadline, so it must be precise.
        let at = |h, m, s| Utc.with_ymd_and_hms(2026, 6, 19, h, m, s).unwrap();
        assert_eq!(
            time_until_next_utc_midnight(at(0, 0, 0)),
            Duration::from_secs(86_400)
        );
        assert_eq!(
            time_until_next_utc_midnight(at(23, 0, 0)),
            Duration::from_secs(3_600)
        );
        assert_eq!(
            time_until_next_utc_midnight(at(23, 59, 59)),
            Duration::from_secs(1)
        );
    }

    #[test]
    fn reconnect_requested_is_due_only_past_a_set_deadline() {
        // Given / When / Then the three cases the read loop relies on: with no schedule
        // configured (`None`) a reconnect is never due, so a connector without session recycling
        // is never interrupted; a deadline already in the past is due (the recycle should fire on
        // the next poll); a deadline still in the future is not yet due (keep the session
        // running). The check is `Instant::now() >= deadline`.
        assert!(!reconnect_requested(None));
        assert!(reconnect_requested(Some(
            Instant::now() - Duration::from_secs(1)
        )));
        assert!(!reconnect_requested(Some(
            Instant::now() + Duration::from_secs(60)
        )));
    }
}
