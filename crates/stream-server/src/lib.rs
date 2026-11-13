//! gRPC streaming server: depth books and contract reference data.
//!
//! A `broadcast` feed of [`DepthView`](nordpool_marketdata::DepthView)
//! fanned out to gRPC subscribers: a producer (the connector's `LocalViewSink`) publishes
//! into a [`DepthChannels`]; [`serve`] streams each subscribed client the books it asked for.
//! The producer side is just a `broadcast::Sender` plus a seed map, so this crate stays decoupled
//! from how the books are reconstructed.
//!
//! [`serve`] runs two services on one runtime: the [`depth`] `DepthBookService` and the
//! [`contracts`] `ContractService`. Both translate domain types to the shared [`proto`] wire
//! module.

// Flag any `pub` item that isn't reachable from outside the crate — it should be
// `pub(crate)` (or narrower). Keeps the public API deliberate.
#![warn(unreachable_pub)]

use chrono::{DateTime, Utc};
use prost_types::Timestamp;

use nordpool_marketdata::MarketType;

mod contracts;
mod depth;
mod feed;
mod grpc;
mod statistics;
mod subscriber;

// Generated protobuf shared by both gRPC services. Its `pub` items are unreachable inside this
// private module, which is expected — exempt it from the crate's `unreachable_pub` lint.
#[allow(unreachable_pub)]
mod proto {
    include!(concat!(
        env!("OUT_DIR"),
        "/nordpool.marketdata.v1.rs"
    ));

    /// Compiled protobuf schema, served over gRPC reflection so tools like
    /// `grpcurl` can discover the API without being handed the `.proto`.
    pub(crate) const FILE_DESCRIPTOR_SET: &[u8] =
        include_bytes!(concat!(env!("OUT_DIR"), "/marketdata_descriptor.bin"));
}

/// Map the domain market type to its wire enum. Shared by the depth and contract services.
pub(crate) fn to_market_type(market_type: MarketType) -> proto::MarketType {
    match market_type {
        MarketType::Unknown => proto::MarketType::None,
        MarketType::Local => proto::MarketType::Local,
        MarketType::Xbid => proto::MarketType::Xbid,
    }
}

/// Wire form of a domain timestamp: a `google.protobuf.Timestamp`, which every time field on the
/// feed protos now is. Both halves are already normalized the way the wire type wants them —
/// negative seconds with a non-negative nanos remainder before the epoch — since chrono floors its
/// seconds; `% 1e9` drops the leap-second flag chrono folds into the nanosecond field.
pub(crate) fn to_timestamp(time: DateTime<Utc>) -> Timestamp {
    Timestamp {
        seconds: time.timestamp(),
        nanos: (time.timestamp_subsec_nanos() % 1_000_000_000) as i32,
    }
}

/// A timestamp from a unix-nanos literal, so the crate's tests can keep writing times as small
/// round numbers.
#[cfg(test)]
pub(crate) fn ts(nanos: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_nanos(nanos)
}

pub use contracts::SharedContracts;
pub use feed::{
    AreaDepthChannel, ContractChannel, ContractReceiver, ContractSender, ContractUpdate,
    DepthChannels, DepthReceiver, DepthSender, DepthUpdate, StatisticChannels, StatisticReceiver,
    StatisticSender, StatisticUpdate,
};
pub use grpc::{ServeChannels, serve};
pub use statistics::SharedStatistics;
