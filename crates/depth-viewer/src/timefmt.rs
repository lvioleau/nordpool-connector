//! Paris-local time formatting for contract delivery windows. Nord Pool intraday delivery
//! windows are quoted in Europe/Paris, so render them there (handles CET/CEST automatically).

use chrono::TimeZone;
use chrono_tz::Europe::Paris;

/// `MM-DD HH:MM` in Paris local time for an epoch-ms instant.
fn paris(ms: i64) -> chrono::DateTime<chrono_tz::Tz> {
    Paris
        .timestamp_millis_opt(ms)
        .single()
        .unwrap_or_else(|| Paris.timestamp_nanos(0))
}

/// `HH:MM:SS` in Paris local time for an epoch-ms instant.
pub fn hms(ms: i64) -> String {
    paris(ms).format("%H:%M:%S").to_string()
}

/// A compact `HH:MM–HH:MM` delivery window in Paris time (times only), for the one-line list.
pub fn delivery_window_compact(start_ms: i64, end_ms: i64) -> String {
    let (s, e) = (paris(start_ms), paris(end_ms));
    format!("{}–{}", s.format("%H:%M"), e.format("%H:%M"))
}

/// A delivery window `start–end` in Paris time; the date is shown once when both ends fall on
/// the same day, otherwise on each end.
pub fn delivery_window(start_ms: i64, end_ms: i64) -> String {
    let (s, e) = (paris(start_ms), paris(end_ms));
    if s.date_naive() == e.date_naive() {
        format!("{}–{}", s.format("%m-%d %H:%M"), e.format("%H:%M"))
    } else {
        format!("{}→{}", s.format("%m-%d %H:%M"), e.format("%m-%d %H:%M"))
    }
}
