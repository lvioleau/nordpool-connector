use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, Utc};

use super::OrderBook;
use crate::{BookKey, BookUpdate, EicCode, Side};

/// Order books for every subscribed delivery area, indexed first by [`EicCode`] and
/// then by [`BookKey`]. A `LocalView` frame is scoped to one delivery area, so snapshot
/// semantics are per-area: a snapshot for one area replaces only that area's books, leaving the
/// other areas untouched. The connector subscribes to a handful of areas at most, so the outer
/// map is tiny; it is a `BTreeMap` for deterministic iteration order (matching the crate's
/// per-price-level `BTreeMap`s).
#[derive(Debug, Default)]
pub struct OrderBookStore {
    areas: BTreeMap<EicCode, HashMap<BookKey, OrderBook>>,
}

impl OrderBookStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one frame's worth of domain updates for `delivery_area_eic` into that area's books.
    /// `is_snapshot` is a frame-level property shared by the whole batch: a snapshot first clears
    /// that area's books — those it doesn't mention must not linger — then every update's orders
    /// are upserted into its book and that book's revision advanced. Books in other delivery areas
    /// are never touched. `received_at` is our receive time for the frame; it stamps the
    /// creation time of any price level this frame opens.
    pub fn apply(
        &mut self,
        delivery_area_eic: EicCode,
        updates: &[BookUpdate],
        is_snapshot: bool,
        received_at: DateTime<Utc>,
    ) {
        let books = self.areas.entry(delivery_area_eic).or_default();
        if is_snapshot {
            books.clear();
        }
        for update in updates {
            let book = books.entry(update.key).or_default();
            for (side, orders) in [(Side::Buy, &update.buys), (Side::Sell, &update.sells)] {
                for order in orders {
                    book.upsert(side, order, received_at);
                }
            }
            book.set_last_revision(update.revision_no);
        }
    }

    pub fn book(&self, delivery_area_eic: EicCode, key: &BookKey) -> Option<&OrderBook> {
        self.areas.get(&delivery_area_eic)?.get(key)
    }

    /// Total books across every delivery area.
    pub fn book_count(&self) -> usize {
        self.areas.values().map(HashMap::len).sum()
    }

    /// Iterate every tracked book with its key, across all delivery areas, in arbitrary order.
    /// The delivery area is dropped because the only caller (the aggregate book-state gauges)
    /// sums over it; reach a specific area's book through [`book`](Self::book).
    pub fn iter(&self) -> impl Iterator<Item = (&BookKey, &OrderBook)> {
        self.areas.values().flat_map(HashMap::iter)
    }
}
