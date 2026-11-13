//! Public trading statistics for the subscribed delivery areas, reconstructed from the
//! `PublicStatistic` feed's snapshot + delta semantics — the per-contract market summary
//! (last trade, day high/low, VWAP, turnover, …) alongside the order books and contract
//! reference data. Pure domain: no protobuf, no IO. The connector translates the wire
//! `PublicStatistic` into [`PublicStatistic`] and folds it into the [`StatisticStore`], keyed by
//! delivery area; the stream-server reads one area's slice to seed each `SubscribeStatistics`
//! stream.

use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};

use crate::{EicCode, MarketType};

/// Direction the contract's last trade moved its price relative to the prior trade.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Tendency {
    Unknown,
    Up,
    Down,
    Equal,
}

/// Public trading statistics for one contract in one delivery area. The optional fields are `None`
/// until the server has a value to publish (e.g. no trade has happened yet today), so a freshly
/// listed contract carries mostly `None`.
#[derive(Debug, Clone, PartialEq)]
pub struct PublicStatistic {
    pub delivery_area_eic: EicCode,
    pub market_type: MarketType,
    pub contract_id: i64,
    pub last_price: Option<i64>,
    pub last_quantity: Option<i64>,
    pub last_trade_time: Option<DateTime<Utc>>,
    pub highest_price: Option<i64>,
    pub lowest_price: Option<i64>,
    pub vwap: Option<i64>,
    pub turnover: Option<i64>,
    pub day_ahead_price: Option<i64>,
    pub tendency: Tendency,
}

/// Per-contract statistics for every subscribed delivery area, keyed first by [`EicCode`]
/// and then by contract id — the statistics twin of [`OrderBookStore`](crate::OrderBookStore)'s
/// area keying. The connector subscribes to a handful of areas, so the outer map is tiny; it is a
/// `BTreeMap` for deterministic iteration order (matching the crate's other area-keyed maps).
#[derive(Debug, Default)]
pub struct StatisticStore {
    areas: BTreeMap<EicCode, HashMap<i64, PublicStatistic>>,
    /// Receive time of the last frame applied — `None` until the first. Set by every `apply`.
    /// The store spans areas, so the stamp is global, not per area.
    received_at: Option<DateTime<Utc>>,
    /// Frame sequence: increments with every `apply` — 0 before the first. Global like the
    /// stamp: one frame's per-area batches share its seq. Frames apply in seq order, so a batch
    /// stamped `seq <=` a snapshot's `seq` is already reflected in it.
    seq: u64,
}

impl StatisticStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one frame's statistics into the books, routing each by its own `delivery_area_eic`.
    /// `is_snapshot` is a frame-level property shared by the whole batch: a snapshot is the full
    /// current state across every subscribed area, so it clears all areas first, then each statistic
    /// is upserted by (area, contract id). (Unlike a `LocalView` frame, a statistic frame is not
    /// scoped to one area — each entry carries its own, so the routing happens per entry here.)
    /// `received_at` is the frame's receive time, kept as the store's freshness stamp; the frame
    /// `seq` advances with it.
    pub fn apply(
        &mut self,
        statistics: &[PublicStatistic],
        is_snapshot: bool,
        received_at: DateTime<Utc>,
    ) {
        if is_snapshot {
            self.areas.clear();
        }
        for statistic in statistics {
            self.areas
                .entry(statistic.delivery_area_eic)
                .or_default()
                .insert(statistic.contract_id, statistic.clone());
        }
        self.received_at = Some(received_at);
        self.seq += 1;
    }

    /// Receive time of the last frame applied — how fresh the store is; `None` until the first
    /// frame.
    pub fn received_at(&self) -> Option<DateTime<Utc>> {
        self.received_at
    }

    /// Sequence number of the last frame applied — 0 until the first.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn get(&self, delivery_area_eic: EicCode, contract_id: i64) -> Option<&PublicStatistic> {
        self.areas.get(&delivery_area_eic)?.get(&contract_id)
    }

    /// One delivery area's statistics (empty if the area has none) — the slice used to seed a
    /// per-area subscriber.
    pub fn area(&self, delivery_area_eic: EicCode) -> impl Iterator<Item = &PublicStatistic> {
        self.areas
            .get(&delivery_area_eic)
            .into_iter()
            .flat_map(HashMap::values)
    }

    /// Total statistics across every delivery area.
    pub fn len(&self) -> usize {
        self.areas.values().map(HashMap::len).sum()
    }

    pub fn is_empty(&self) -> bool {
        self.areas.values().all(HashMap::is_empty)
    }

    /// Every tracked statistic across all areas (each carries its own `delivery_area_eic`).
    pub fn iter(&self) -> impl Iterator<Item = &PublicStatistic> {
        self.areas.values().flat_map(HashMap::values)
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

    fn statistic(id: i64, area: EicCode, last_price: Option<i64>) -> PublicStatistic {
        PublicStatistic {
            delivery_area_eic: area,
            market_type: MarketType::Local,
            contract_id: id,
            last_price,
            last_quantity: None,
            last_trade_time: None,
            highest_price: None,
            lowest_price: None,
            vwap: None,
            turnover: None,
            day_ahead_price: None,
            tendency: Tendency::Up,
        }
    }

    #[test]
    fn snapshot_replaces_the_set_and_delta_upserts_by_id() {
        // Given a book seeded by a snapshot of two contracts' statistics in area 40, received at
        // t=10.
        let mut book = StatisticStore::new();
        book.apply(
            &[
                statistic(1, EicCode::AREA_FR, Some(100)),
                statistic(2, EicCode::AREA_FR, Some(200)),
            ],
            true,
            ts(10),
        );

        // When a delta received at t=20 updates contract 1's statistic and adds contract 3.
        book.apply(
            &[
                statistic(1, EicCode::AREA_FR, Some(150)),
                statistic(3, EicCode::AREA_FR, Some(300)),
            ],
            false,
            ts(20),
        );

        // Then contract 1 is replaced in place, the untouched contract 2 survives, 3 is new, and
        // the freshness stamp advanced to the delta's receive time.
        assert_eq!(book.len(), 3);
        assert_eq!(book.get(EicCode::AREA_FR, 1).unwrap().last_price, Some(150));
        assert_eq!(book.get(EicCode::AREA_FR, 2).unwrap().last_price, Some(200));
        assert_eq!(book.get(EicCode::AREA_FR, 3).unwrap().last_price, Some(300));
        assert_eq!(book.received_at(), Some(ts(20)));
        assert_eq!(book.seq(), 2);
    }

    #[test]
    fn apply_routes_statistics_by_their_own_delivery_area() {
        // Given a single frame carrying statistics for two different areas.
        let mut book = StatisticStore::new();

        // When applied as a snapshot.
        book.apply(
            &[
                statistic(1, EicCode::AREA_FR, Some(100)),
                statistic(2, EicCode::AREA_NO1, Some(200)),
            ],
            true,
            ts(0),
        );

        // Then each statistic lands under its own area, and one area's slice excludes the other's.
        assert_eq!(
            book.area(EicCode::AREA_FR)
                .map(|s| s.contract_id)
                .collect::<Vec<_>>(),
            [1]
        );
        assert_eq!(
            book.area(EicCode::AREA_NO1)
                .map(|s| s.contract_id)
                .collect::<Vec<_>>(),
            [2]
        );
        assert!(book.get(EicCode::AREA_NO1, 1).is_none());
    }

    #[test]
    fn snapshot_clears_every_area_before_repopulating() {
        // Given statistics across two areas.
        let mut book = StatisticStore::new();
        book.apply(
            &[
                statistic(1, EicCode::AREA_FR, Some(100)),
                statistic(2, EicCode::AREA_NO1, Some(200)),
            ],
            true,
            ts(0),
        );

        // When a later snapshot mentions only area 40 — a snapshot is the full current state, so
        // area 50's now-absent statistic must go.
        book.apply(&[statistic(1, EicCode::AREA_FR, Some(110))], true, ts(0));

        // Then area 40 is replaced and area 50 is emptied.
        assert_eq!(book.len(), 1);
        assert!(book.get(EicCode::AREA_NO1, 2).is_none());
    }
}
