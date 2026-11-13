//! Time-column types shared by the two Parquet writers. Every instant either writer records is a
//! UTC one — our receive time for an archived frame, or a contract's own UTC-based schedule — so
//! the columns say so rather than leaving a reader to guess a zone.

use arrow::datatypes::{DataType, TimeUnit};
use chrono::{DateTime, Utc};

/// Time zone stamped on every time column. The domain's timestamps are `DateTime<Utc>`, so the
/// columns say so: a reader (pandas, polars, duckdb) then yields tz-aware UTC instants instead of
/// naive local-looking ones.
pub(crate) const UTC_TZ: &str = "UTC";

/// Type of a column of receive timestamps — our own observation times, which the archive records
/// to the nanosecond.
pub(crate) fn time_utc_nanos() -> DataType {
    DataType::Timestamp(TimeUnit::Nanosecond, Some(UTC_TZ.into()))
}

/// Type of a column of wire timestamps (a contract's delivery window, its update and open/close
/// times). Nord Pool sends those as unix millis, so the column keeps that resolution rather than
/// padding to nanos precision the feed never had.
pub(crate) fn time_utc_millis() -> DataType {
    DataType::Timestamp(TimeUnit::Millisecond, Some(UTC_TZ.into()))
}

/// The column form of a domain timestamp: unix nanos, matching [`time_utc_nanos`]. A time outside
/// the nanosecond range (roughly 1678–2262) has none, and reads as the epoch.
pub(crate) fn nanos(time: DateTime<Utc>) -> i64 {
    time.timestamp_nanos_opt().unwrap_or(0)
}
