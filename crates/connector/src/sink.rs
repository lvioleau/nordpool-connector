mod archive;
mod book_metrics;
mod contract;
mod contract_metrics;
mod localview;
mod queued;
mod statistic;
mod statistic_metrics;
mod tee;

pub use archive::ArchiveSink;
pub(crate) use archive::segment_open_time;
pub use contract::ContractSink;
pub use localview::{LocalViewSink, to_book_updates};
pub use queued::QueuedSink;
pub use statistic::StatisticSink;
pub use tee::TeeSink;

use chrono::{DateTime, Utc};

use crate::proto::pmd;
use crate::proto::pmd::server_frame::Payload as ServerPayload;
use nordpool_marketdata::{MarketType, ProductType};

/// Map the wire `market_type` enum value to the domain [`MarketType`]. Shared by the `LocalView`,
/// `Contract` and `PublicStatistic` translations — every feed's wire message carries one.
pub(super) fn map_market_type(raw: i32) -> MarketType {
    use pmd::MarketType as Wire;
    match Wire::try_from(raw).unwrap_or(Wire::None) {
        Wire::Local => MarketType::Local,
        Wire::Xbid => MarketType::Xbid,
        Wire::None => MarketType::Unknown,
    }
}

/// The `market_type` metrics tag value: the domain label lowercased. Case is a metrics-side
/// convention — every tag value we emit is lowercase — so it lives here rather than on the
/// domain enum, whose label is its variant name (and a Parquet column value in replay).
pub(super) fn market_type_tag(market_type: MarketType) -> String {
    market_type.as_label().to_ascii_lowercase()
}

/// The `product_type` metrics tag value; the [`market_type_tag`] convention applied to the
/// contract gauge's other tag.
pub(super) fn product_type_tag(product_type: ProductType) -> String {
    product_type.as_label().to_ascii_lowercase()
}

/// Translate a wire timestamp to the domain's [`DateTime<Utc>`]. Every PMD timestamp field is unix
/// millis, so this is the one shape needed — shared by the `Contract` and `PublicStatistic`
/// translations and by the `LocalView` order stamps. A value outside the representable range (a
/// wire field never carries one; the range spans ±262 000 years) falls back to the epoch rather
/// than propagating an error through a translation that has no other failure mode.
pub(super) fn from_wire_millis(millis: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_millis(millis).unwrap_or_default()
}

/// A timestamp from a unix-nanos literal, so the sink tests can keep writing times as small round
/// numbers.
#[cfg(test)]
pub(super) fn ts(nanos: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_nanos(nanos)
}

/// Sink for every market-data `ServerFrame` received from the server. The driver decodes
/// each frame once and hands sinks the decoded payload (for routing and translation), the
/// original wire bytes (so pass-through sinks like the archive write them untouched —
/// re-encoding would drop fields our compiled proto doesn't know about), and the wall-clock
/// instant the frame was received (which the feed sinks stamp onto everything they publish and
/// the archive serializes alongside the bytes and uses to drive event-time segment rotation).
/// Implementations forward to whatever downstream the connector feeds — the raw-frame archive,
/// the in-process order book behind the depth feed, an in-memory buffer in tests.
pub trait FrameSink {
    fn handle(
        &mut self,
        payload: &ServerPayload,
        bytes: &[u8],
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()>;
}
