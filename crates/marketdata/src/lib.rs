//! Pure order-book data structures and update logic for Nord Pool intraday
//! markets, reconstructed from `LocalView` snapshot + delta semantics.
//!
//! This crate intentionally has no knowledge of the wire format, transport, or
//! sinks. It works on minimal domain types (`Order`, `OrderId`, `Price`, etc.)
//! that the connector binary translates into from the protobuf `LocalView`.
//!
//! [`OrderBook`] reconstructs one book's L2 view (per-price-level total quantity
//! and order count, split by side) from `LocalView` snapshot + delta semantics;
//! [`OrderBookStore`] keys one `OrderBook` per [`BookKey`].

// Flag any `pub` item that isn't reachable from outside the crate — it should be
// `pub(crate)` (or narrower). Keeps the public API deliberate.
#![warn(unreachable_pub)]

use chrono::{DateTime, Utc};

mod contract;
mod eic;
mod orderbook;
mod statistic;

pub use contract::{Contract, ContractAreaState, ContractState, ContractStore, ProductType};
pub use eic::EicCode;
pub use orderbook::{DepthStore, DepthView, OrderBook, OrderBookStore, PriceLevel};
pub use statistic::{PublicStatistic, StatisticStore, Tendency};

pub type OrderId = i64;
pub type Price = i64;
pub type Quantity = i32;

/// A Nord Pool delivery-area id as it appears on the PMD wire (e.g. `40`). Only the wire
/// boundary speaks this: subscriptions go out by id and inbound frames carry it. Everywhere
/// else the domain uses [`EicCode`], converted at the boundary via [`EicCode::from_id`] /
/// [`EicCode::id`].
pub type DeliveryAreaId = i32;

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct BookKey {
    pub market_type: MarketType,
    pub contract_id: i64,
}

#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub enum MarketType {
    Unknown,
    Local,
    Xbid,
}

impl MarketType {
    /// Stable identifier — the variant name, used as a Parquet column value by the callers.
    pub fn as_label(self) -> &'static str {
        match self {
            MarketType::Unknown => "Unknown",
            MarketType::Local => "Local",
            MarketType::Xbid => "Xbid",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum Side {
    Buy,
    Sell,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct Order {
    pub id: OrderId,
    pub price: Price,
    pub quantity: Quantity,
    /// A per-order timestamp — *not* a queue-priority stamp. Nord Pool refreshes it under
    /// the same `id` on a **modify without a priority change** (a priority-changing modify is
    /// instead a cancel + a new order, i.e. a new `id`), and keeps it on a **clip fill** (an
    /// iceberg's filled clip is replaced by a fresh clip sent to the back of the queue,
    /// keeping the same `id` and `created_at`). So under a stable `id`, a changed
    /// `created_at` marks a modify and an unchanged one a clip fill.
    pub created_at: DateTime<Utc>,
}

/// One book's worth of an incoming update in domain form: the buy/sell orders for a
/// single book. Whether the batch is a snapshot or a delta is a property of the wire
/// *frame*, not of any one book, so it travels alongside a batch of these rather than
/// inside each one. The wire `LocalView` is translated into this before it reaches the
/// store, keeping the protobuf format out of this crate.
#[derive(Debug, Clone, Eq, PartialEq)]
pub struct BookUpdate {
    pub key: BookKey,
    pub revision_no: i64,
    pub buys: Vec<Order>,
    pub sells: Vec<Order>,
}

/// A timestamp from a unix-nanos literal, so the crate's tests can keep writing times as small
/// round numbers.
#[cfg(test)]
pub(crate) fn ts(nanos: i64) -> DateTime<Utc> {
    DateTime::from_timestamp_nanos(nanos)
}
