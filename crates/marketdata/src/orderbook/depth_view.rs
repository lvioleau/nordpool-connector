use std::collections::HashMap;

use chrono::{DateTime, Utc};

use super::{OrderBook, PriceLevel};
use crate::BookKey;

/// A point-in-time view of a book's top `depth` price levels per side, best-first
/// (`bids` high→low, `asks` low→high). The number of levels is a caller policy (the
/// connector reads it from `[server] depth`), not a fixed property of the book — a thinner
/// book carries fewer; an empty side carries none.
#[derive(Debug, Clone, PartialEq)]
pub struct DepthView {
    pub key: BookKey,
    pub revision: i64,
    pub bids: Vec<PriceLevel>,
    pub asks: Vec<PriceLevel>,
}

impl DepthView {
    /// Project `book`'s best `depth` levels per side.
    pub fn from_book(key: BookKey, book: &OrderBook, depth: usize) -> Self {
        Self {
            key,
            revision: book.last_revision(),
            bids: book.bid_levels().take(depth).collect(),
            asks: book.ask_levels().take(depth).collect(),
        }
    }
}

/// The served depth state for one delivery area: each book's latest top-N [`DepthView`] keyed by
/// [`BookKey`], plus the receive time and frame sequence of the last frame applied — the depth
/// twin of [`ContractStore`](crate::ContractStore) / [`StatisticStore`](crate::StatisticStore).
/// Unlike those it is per delivery area (a `LocalView` frame is area-scoped), and it holds
/// projections rather than reconstruction state — the full books live in
/// [`OrderBookStore`](crate::OrderBookStore); this is what a new subscriber is seeded from.
#[derive(Debug, Default)]
pub struct DepthStore {
    views: HashMap<BookKey, DepthView>,
    /// Receive time of the last frame applied — `None` until the first. Set by every `apply`,
    /// even one whose batch is empty, because the frame still proves the area's state current as
    /// of its arrival.
    received_at: Option<DateTime<Utc>>,
    /// Frame sequence: increments with every `apply` — 0 before the first. Frames apply in seq
    /// order, so a batch stamped `seq <=` a snapshot's `seq` is already reflected in it.
    seq: u64,
}

impl DepthStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one frame's changed views into the store: `is_snapshot` first clears the set (the
    /// caller's suppression state resets with a snapshot, so its `changed` is the area's full
    /// new book set), then each view is upserted by key. `received_at` is the frame's receive
    /// time, kept as the store's freshness stamp; the frame `seq` advances with it.
    pub fn apply(&mut self, changed: &[DepthView], is_snapshot: bool, received_at: DateTime<Utc>) {
        if is_snapshot {
            self.views.clear();
        }
        for view in changed {
            self.views.insert(view.key, view.clone());
        }
        self.received_at = Some(received_at);
        self.seq += 1;
    }

    pub fn get(&self, key: &BookKey) -> Option<&DepthView> {
        self.views.get(key)
    }

    pub fn len(&self) -> usize {
        self.views.len()
    }

    pub fn is_empty(&self) -> bool {
        self.views.is_empty()
    }

    /// Every tracked book's latest top-N view — the set a new subscriber is seeded from.
    pub fn iter(&self) -> impl Iterator<Item = &DepthView> {
        self.views.values()
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{MarketType, ts};

    fn view(contract_id: i64, price: i64) -> DepthView {
        DepthView {
            key: BookKey {
                market_type: MarketType::Local,
                contract_id,
            },
            revision: 1,
            bids: vec![PriceLevel {
                price,
                total_quantity: 1,
                order_count: 1,
                created_at: ts(0),
                updated_at: ts(0),
            }],
            asks: vec![],
        }
    }

    #[test]
    fn snapshot_replaces_the_set_and_delta_upserts_by_key() {
        // Given a store seeded by a snapshot of two books, received at t=10.
        let mut store = DepthStore::new();
        store.apply(&[view(1, 100), view(2, 200)], true, ts(10));

        // When a delta received at t=20 moves book 1's top and adds book 3.
        store.apply(&[view(1, 150), view(3, 300)], false, ts(20));

        // Then book 1 is replaced in place, the untouched book 2 survives, 3 is new, and the
        // freshness stamp and frame seq advanced with the delta.
        assert_eq!(store.len(), 3);
        assert_eq!(store.get(&view(1, 0).key).unwrap().bids[0].price, 150);
        assert_eq!(store.get(&view(2, 0).key).unwrap().bids[0].price, 200);
        assert_eq!(store.received_at(), Some(ts(20)));
        assert_eq!(store.seq(), 2);

        // When a later snapshot, received at t=30, carries only book 1 — a snapshot is the
        // area's full current state, so anything absent has gone away.
        store.apply(&[view(1, 100)], true, ts(30));

        // Then the stale books are pruned and the stamp and seq follow the frame.
        assert_eq!(store.len(), 1);
        assert!(store.iter().all(|v| v.key.contract_id == 1));
        assert_eq!(store.received_at(), Some(ts(30)));
        assert_eq!(store.seq(), 3);
    }
}
