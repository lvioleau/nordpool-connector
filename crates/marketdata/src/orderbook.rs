use std::collections::btree_map::Entry;
use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeMap, HashMap};
use std::hash::{Hash, Hasher};

use chrono::{DateTime, Utc};

use crate::{Order, OrderId, Price, Quantity, Side};

mod depth_view;
mod store;

pub use depth_view::{DepthStore, DepthView};
pub use store::OrderBookStore;

/// Internal aggregate held per price in the level maps: the summed visible quantity, the number of
/// distinct orders resting at that price, and two receive timestamps — `created_at` captured when
/// the level opened and held fixed for its lifetime, `updated_at` refreshed every time the level is
/// touched (a join, resize, or partial remove that leaves it non-empty). Projected to the public
/// [`PriceLevel`] (which also carries the price) when iterated.
#[derive(Debug, Default, Clone, Copy, Eq, PartialEq)]
struct Level {
    total_quantity: i64,
    order_count: u32,
    created_at: DateTime<Utc>,
    updated_at: DateTime<Utc>,
}

/// One aggregated price level of the book, as yielded by [`OrderBook::bid_levels`]
/// and [`OrderBook::ask_levels`].
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub struct PriceLevel {
    pub price: Price,
    pub total_quantity: i64,
    pub order_count: u32,
    /// Our receive time for the frame that opened this level.
    pub created_at: DateTime<Utc>,
    /// Our receive time for the frame that last touched this level.
    pub updated_at: DateTime<Utc>,
}

impl PriceLevel {
    fn new(price: Price, level: &Level) -> Self {
        Self {
            price,
            total_quantity: level.total_quantity,
            order_count: level.order_count,
            created_at: level.created_at,
            updated_at: level.updated_at,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct BookOrder {
    side: Side,
    price: Price,
    quantity: Quantity,
}

/// L2 reconstruction of a single order book. Tracks every live order by id so
/// price-level totals can be maintained incrementally: an update adjusts the
/// affected level by the order's quantity change, and an order leaving the book
/// (`quantity == 0`) decrements its level. Levels are kept in a `BTreeMap` per
/// side; read them best-price-first via [`bid_levels`](Self::bid_levels) /
/// [`ask_levels`](Self::ask_levels), or take the top via
/// [`best_bid`](Self::best_bid) / [`best_ask`](Self::best_ask).
#[derive(Debug, Default)]
pub struct OrderBook {
    orders: HashMap<OrderId, BookOrder>,
    bids: BTreeMap<Price, Level>,
    asks: BTreeMap<Price, Level>,
    last_revision: i64,
}

impl OrderBook {
    pub fn last_revision(&self) -> i64 {
        self.last_revision
    }

    /// Record the revision an update carried; the caller (the feed protocol driver)
    /// advances this after applying the update's orders.
    pub fn set_last_revision(&mut self, revision_no: i64) {
        self.last_revision = revision_no;
    }

    pub fn order_count(&self) -> usize {
        self.orders.len()
    }

    pub fn bid_order_count(&self) -> u32 {
        self.bids.values().map(|level| level.order_count).sum()
    }

    pub fn ask_order_count(&self) -> u32 {
        self.asks.values().map(|level| level.order_count).sum()
    }

    pub fn bid_level_count(&self) -> usize {
        self.bids.len()
    }

    pub fn ask_level_count(&self) -> usize {
        self.asks.len()
    }

    pub fn bid_quantity(&self) -> i64 {
        self.bids.values().map(|level| level.total_quantity).sum()
    }

    pub fn ask_quantity(&self) -> i64 {
        self.asks.values().map(|level| level.total_quantity).sum()
    }

    pub fn bid_levels(&self) -> impl Iterator<Item = PriceLevel> + '_ {
        self.bids
            .iter()
            .rev()
            .map(|(price, level)| PriceLevel::new(*price, level))
    }

    pub fn ask_levels(&self) -> impl Iterator<Item = PriceLevel> + '_ {
        self.asks
            .iter()
            .map(|(price, level)| PriceLevel::new(*price, level))
    }

    pub fn best_bid(&self) -> Option<PriceLevel> {
        self.bid_levels().next()
    }

    pub fn best_ask(&self) -> Option<PriceLevel> {
        self.ask_levels().next()
    }

    /// Digest of the top-`depth` levels per side, ignoring the revision. Hashes only each level's
    /// visible market state (price, quantity, order count) plus its `created_at` — *not*
    /// `updated_at`, which advances on every touch and would otherwise defeat the suppression
    /// of updates that don't move the visible top.
    pub fn top_hash(&self, depth: usize) -> u64 {
        // Domain separator between the sides so a level shifting from bid to ask can't
        // collide with a different split that concatenates to the same level sequence.
        const SIDE_SEPARATOR: u64 = 0x5350_4c49_545f_4253; // "SPLIT_BS"
        let mut hasher = DefaultHasher::new();
        for level in self.bid_levels().take(depth) {
            hash_top_level(&mut hasher, &level);
        }
        SIDE_SEPARATOR.hash(&mut hasher);
        for level in self.ask_levels().take(depth) {
            hash_top_level(&mut hasher, &level);
        }
        hasher.finish()
    }

    /// Apply one order's absolute state: insert it, update it in place, or — when
    /// `quantity == 0` — remove it. `received_at` is our receive time for the carrying
    /// frame; it stamps the creation time of any level this order opens.
    pub fn upsert(&mut self, side: Side, order: &Order, received_at: DateTime<Utc>) {
        if order.quantity == 0 {
            self.remove(order.id, received_at);
            return;
        }

        match self.orders.get(&order.id).copied() {
            Some(existing) if existing.side == side && existing.price == order.price => {
                // Same id, same price, quantity changed — apply the signed change to the
                // level total; order count holds. Two expected operations land here, told
                // apart by created_at (a priority-changing modify would be a cancel + a
                // new id instead, so it never reaches this arm):
                //  - created_at changed  -> a modify without a priority change;
                //  - created_at unchanged -> a clip fill: the filled clip is replaced by a
                //    fresh clip (sent to the back of the queue) under the same id and the same
                //    created_at.
                self.level_resize(
                    side,
                    order.price,
                    existing.quantity,
                    order.quantity,
                    received_at,
                );
            }
            Some(existing) => {
                // Same id, different price: an iceberg refresh — the price moves by the
                // configured delta only when a clip is *fully* filled, and the fresh clip
                // keeps the order id but enters the queue anew (fresh created_at). Its
                // quantity can be below the clip size: a smaller remaining reserve, or a
                // partial execution on entry when the delta-stepped price crosses resting
                // orders. A price-changing *modify* would be a cancel + new id. Move the
                // order to its new level.
                self.level_remove(
                    existing.side,
                    existing.price,
                    existing.quantity,
                    received_at,
                );
                self.level_add(side, order.price, order.quantity, received_at);
            }
            None => {
                self.level_add(side, order.price, order.quantity, received_at);
            }
        }

        self.orders.insert(
            order.id,
            BookOrder {
                side,
                price: order.price,
                quantity: order.quantity,
            },
        );
    }

    fn remove(&mut self, id: OrderId, received_at: DateTime<Utc>) {
        let Some(existing) = self.orders.remove(&id) else {
            // A delete for an id we never tracked — a missed add, a double delete, or a
            // stale/replayed message. Nothing to remove; flag it but stay correct.
            tracing::warn!(order_id = id, "remove for unknown order id; ignoring");
            return;
        };
        self.level_remove(
            existing.side,
            existing.price,
            existing.quantity,
            received_at,
        );
    }

    /// Reset the book to empty — a snapshot replaces state wholesale (and may carry any
    /// revision, so the prior `last_revision` carries no meaning afterwards).
    pub fn clear(&mut self) {
        self.orders.clear();
        self.bids.clear();
        self.asks.clear();
    }

    fn side_levels(&mut self, side: Side) -> &mut BTreeMap<Price, Level> {
        match side {
            Side::Buy => &mut self.bids,
            Side::Sell => &mut self.asks,
        }
    }

    fn level_add(
        &mut self,
        side: Side,
        price: Price,
        quantity: Quantity,
        received_at: DateTime<Utc>,
    ) {
        match self.side_levels(side).entry(price) {
            // A brand-new level: stamp both timestamps with our receive time for the opening frame.
            Entry::Vacant(slot) => {
                slot.insert(Level {
                    total_quantity: i64::from(quantity),
                    order_count: 1,
                    created_at: received_at,
                    updated_at: received_at,
                });
            }
            // Joining an existing level: adjust totals and refresh updated_at, but leave created_at.
            Entry::Occupied(mut slot) => {
                let level = slot.get_mut();
                level.total_quantity += i64::from(quantity);
                level.order_count += 1;
                level.updated_at = received_at;
            }
        }
    }

    fn level_remove(
        &mut self,
        side: Side,
        price: Price,
        quantity: Quantity,
        received_at: DateTime<Utc>,
    ) {
        let levels = self.side_levels(side);
        let Some(level) = levels.get_mut(&price) else {
            return;
        };
        // A level should never hold fewer orders or less quantity than is being removed.
        // A breach means a missed add, a double remove, or a stale/replayed message; flag
        // it but stay correct (saturate rather than underflow into a huge/negative count).
        if level.order_count == 0 || level.total_quantity < i64::from(quantity) {
            tracing::warn!(
                side = ?side,
                price,
                remove_quantity = quantity,
                level_order_count = level.order_count,
                level_quantity = level.total_quantity,
                "level_remove would take the level below empty; clamping"
            );
        }
        level.total_quantity = (level.total_quantity - i64::from(quantity)).max(0);
        level.order_count = level.order_count.saturating_sub(1);
        if level.order_count == 0 {
            levels.remove(&price);
        } else {
            level.updated_at = received_at;
        }
    }

    fn level_resize(
        &mut self,
        side: Side,
        price: Price,
        existing_quantity: Quantity,
        new_quantity: Quantity,
        received_at: DateTime<Utc>,
    ) {
        // Widen before subtracting: the level total is an i64, and a wire quantity outside the
        // expected non-negative range would otherwise overflow the i32 subtraction — a debug
        // panic, a wrapped total in release.
        let delta_quantity = i64::from(new_quantity) - i64::from(existing_quantity);
        let level = self
            .side_levels(side)
            .get_mut(&price)
            .expect("level present: resize is only called for an order already at this price");
        level.total_quantity += delta_quantity;
        level.updated_at = received_at;
    }
}

/// Feed one level's market-defining state into the [`OrderBook::top_hash`] digest: price, total
/// quantity, order count, and `created_at`. Deliberately excludes `updated_at` (see
/// `top_hash`).
fn hash_top_level(hasher: &mut DefaultHasher, level: &PriceLevel) {
    level.price.hash(hasher);
    level.total_quantity.hash(hasher);
    level.order_count.hash(hasher);
    level.created_at.hash(hasher);
}

#[cfg(test)]
mod tests {
    mod book {
        use crate::*;

        /// Test stand-in for the feed protocol the connector's `LocalViewSink` drives in
        /// production: a snapshot resets the book, each order is upserted, the revision advances.
        fn apply(
            book: &mut OrderBook,
            is_snapshot: bool,
            revision_no: i64,
            buys: &[Order],
            sells: &[Order],
        ) {
            if is_snapshot {
                book.clear();
            }
            for order in buys {
                book.upsert(Side::Buy, order, ts(0));
            }
            for order in sells {
                book.upsert(Side::Sell, order, ts(0));
            }
            book.set_last_revision(revision_no);
        }

        fn order(id: OrderId, price: Price, quantity: Quantity) -> Order {
            Order {
                id,
                price,
                quantity,
                created_at: ts(0),
            }
        }

        fn level(book: &OrderBook, side: Side, price: Price) -> Option<PriceLevel> {
            match side {
                Side::Buy => book.bid_levels().find(|l| l.price == price),
                Side::Sell => book.ask_levels().find(|l| l.price == price),
            }
        }

        /// Expected price level; both timestamps are 0 because the `apply` helper opens and touches
        /// levels with a receive time of 0 (the timestamp tests upsert directly and assert on them).
        fn pl(price: Price, total_quantity: i64, order_count: u32) -> PriceLevel {
            PriceLevel {
                price,
                total_quantity,
                order_count,
                created_at: ts(0),
                updated_at: ts(0),
            }
        }

        #[test]
        fn snapshot_aggregates_orders_into_levels() {
            // Given an empty book.
            let mut book = OrderBook::default();

            // When a snapshot lands two buys resting at the same price (100), a third buy
            // a tick lower (99), and a single sell (101).
            apply(
                &mut book,
                true,
                1,
                &[order(1, 100, 5), order(2, 100, 3), order(3, 99, 4)],
                &[order(4, 101, 2)],
            );

            // Then the two same-price buys collapse into one level summing their quantities
            // (5 + 3 = 8) and counting both orders, best bid/ask sit at the inside prices,
            // and the book tracks all four orders at the snapshot's revision.
            assert_eq!(level(&book, Side::Buy, 100), Some(pl(100, 8, 2)));
            assert_eq!(book.best_bid().map(|l| l.price), Some(100));
            assert_eq!(book.best_ask().map(|l| l.price), Some(101));
            assert_eq!(book.order_count(), 4);
            assert_eq!(book.last_revision(), 1);
        }

        #[test]
        fn level_iterators_yield_best_price_first() {
            // Given an empty book.
            let mut book = OrderBook::default();

            // When a snapshot lands bids and asks in deliberately scrambled price order.
            apply(
                &mut book,
                true,
                1,
                &[order(1, 100, 5), order(2, 102, 3), order(3, 101, 4)],
                &[order(4, 110, 1), order(5, 108, 2)],
            );

            // Then the iterators sort each side best-first regardless of insertion order:
            // bids descend from the highest price, asks ascend from the lowest.
            assert_eq!(
                book.bid_levels().map(|l| l.price).collect::<Vec<_>>(),
                vec![102, 101, 100]
            );
            assert_eq!(
                book.ask_levels().map(|l| l.price).collect::<Vec<_>>(),
                vec![108, 110]
            );
        }

        #[test]
        fn delta_quantity_reduction_adjusts_level_total_only() {
            // Given a single level holding two orders at price 100, totalling 8.
            let mut book = OrderBook::default();
            apply(
                &mut book,
                true,
                1,
                &[order(1, 100, 5), order(2, 100, 3)],
                &[],
            );

            // When a delta shrinks one of those orders (id 1) from 5 to 2, same price.
            apply(&mut book, false, 2, &[order(1, 100, 2)], &[]);

            // Then the level total drops by the quantity delta (8 - 3 = 5) but the order
            // count is unchanged — the order resized in place, it didn't leave.
            assert_eq!(level(&book, Side::Buy, 100), Some(pl(100, 5, 2)));
        }

        #[test]
        fn delta_zero_quantity_removes_order_and_collapses_empty_level() {
            // Given a book with one bid (the only order on its level) and one ask.
            let mut book = OrderBook::default();
            apply(&mut book, true, 1, &[order(1, 100, 5)], &[order(2, 101, 4)]);

            // When a delta sets the lone bid's quantity to 0 — the wire's way of removing it.
            apply(&mut book, false, 2, &[order(1, 100, 0)], &[]);

            // Then the order is gone, and because it was the last one at that price the level
            // collapses entirely (no empty husk left behind); only the ask remains.
            assert_eq!(level(&book, Side::Buy, 100), None);
            assert_eq!(book.best_bid(), None);
            assert_eq!(book.order_count(), 1);
        }

        #[test]
        fn delta_removing_unknown_order_is_a_no_op() {
            // Given a book holding exactly one order (id 1).
            let mut book = OrderBook::default();
            apply(&mut book, true, 1, &[order(1, 100, 5)], &[]);

            // When a delete (quantity 0) arrives for id 2, which the book never tracked — a
            // missed add, a double delete, or a stale/replayed message.
            apply(&mut book, false, 2, &[order(2, 100, 0)], &[]);

            // Then the book stays correct: the unrelated existing order and its level are
            // untouched rather than the stray delete corrupting the level totals.
            assert_eq!(level(&book, Side::Buy, 100), Some(pl(100, 5, 1)));
            assert_eq!(book.order_count(), 1);
        }

        #[test]
        fn delta_new_order_at_existing_level_increments_count() {
            // Given a level at price 100 opened by a single order of quantity 5.
            let mut book = OrderBook::default();
            apply(&mut book, true, 1, &[order(1, 100, 5)], &[]);

            // When a delta adds a second, distinct order (id 2) at that same price.
            apply(&mut book, false, 2, &[order(2, 100, 3)], &[]);

            // Then the level aggregates both: quantity sums to 8 and the order count rises to 2.
            assert_eq!(level(&book, Side::Buy, 100), Some(pl(100, 8, 2)));
        }

        #[test]
        fn price_change_under_same_id_moves_order_between_levels() {
            // Given two orders sharing price 100 (an iceberg-refresh scenario: the same id
            // can reprice when a clip fully fills).
            let mut book = OrderBook::default();
            apply(
                &mut book,
                true,
                1,
                &[order(1, 100, 5), order(2, 100, 3)],
                &[],
            );

            // When a delta reprices order 1 down to 99, keeping its id.
            apply(&mut book, false, 2, &[order(1, 99, 5)], &[]);

            // Then it is removed from the old level (leaving order 2's quantity 3 behind) and
            // added to a freshly opened level at 99 — totals on both levels stay consistent.
            assert_eq!(level(&book, Side::Buy, 100), Some(pl(100, 3, 1)));
            assert_eq!(level(&book, Side::Buy, 99), Some(pl(99, 5, 1)));
        }

        #[test]
        fn delta_with_backwards_revision_still_applies() {
            // Given a book left at revision 5 by a snapshot.
            let mut book = OrderBook::default();
            apply(&mut book, true, 5, &[order(1, 100, 5)], &[]);

            // When a delta arrives carrying a *lower* revision (3) than the book already holds.
            apply(&mut book, false, 3, &[order(2, 100, 3)], &[]);

            // Then the book applies it anyway and records the revision verbatim — revision is
            // bookkeeping the caller threads through, not a gate the book enforces.
            assert_eq!(level(&book, Side::Buy, 100), Some(pl(100, 8, 2)));
            assert_eq!(book.last_revision(), 3);
        }

        #[test]
        fn snapshot_replaces_prior_state() {
            // Given a book populated with a bid and an ask by a first snapshot.
            let mut book = OrderBook::default();
            apply(&mut book, true, 1, &[order(1, 100, 5)], &[order(2, 101, 4)]);

            // When a second snapshot arrives carrying only a single, different bid.
            apply(&mut book, true, 2, &[order(3, 98, 7)], &[]);

            // Then it wholesale-replaces the prior state rather than merging: the old bid and
            // ask are gone, leaving just the new bid and an empty ask side.
            assert_eq!(book.order_count(), 1);
            assert_eq!(book.best_bid().map(|l| l.price), Some(98));
            assert_eq!(book.best_ask(), None);
        }

        #[test]
        fn a_resize_across_the_whole_i32_range_does_not_overflow() {
            // Given a level holding the largest quantity the wire's i32 field can carry.
            let mut book = OrderBook::default();
            book.upsert(Side::Buy, &order(1, 100, i32::MAX), ts(0));

            // When that order resizes to a negative quantity — outside the range a well-behaved
            // feed sends, and the one case where the difference of two wire quantities does not
            // fit in an i32.
            book.upsert(Side::Buy, &order(1, 100, -2), ts(0));

            // Then the level total follows the resize, computed in the i64 it is stored in,
            // rather than overflowing the subtraction (a debug panic, a wrapped total in release).
            assert_eq!(
                level(&book, Side::Buy, 100).map(|l| l.total_quantity),
                Some(-2)
            );
        }

        #[test]
        fn level_creation_time_is_set_when_opened_and_kept_on_join() {
            // Given a level opened by an order whose frame we received at t=1000 (unix nanos).
            let mut book = OrderBook::default();
            book.upsert(Side::Buy, &order(1, 100, 5), ts(1000));
            assert_eq!(level(&book, Side::Buy, 100).unwrap().created_at, ts(1000));

            // When a newer order joins the same level on a frame received at t=2000.
            book.upsert(Side::Buy, &order(2, 100, 3), ts(2000));

            // Then the level keeps the time it opened at, not the joiner's.
            assert_eq!(level(&book, Side::Buy, 100).unwrap().created_at, ts(1000));
        }

        #[test]
        fn reopening_an_emptied_level_uses_the_new_receive_time() {
            // Given a level opened at t=1000, then emptied by removing its order.
            let mut book = OrderBook::default();
            book.upsert(Side::Buy, &order(1, 100, 5), ts(1000));
            book.upsert(Side::Buy, &order(1, 100, 0), ts(1000));
            assert_eq!(level(&book, Side::Buy, 100), None);

            // When a different order reopens the same price on a later frame received at t=3000.
            book.upsert(Side::Buy, &order(2, 100, 4), ts(3000));

            // Then the level carries the new opening frame's receive time.
            assert_eq!(level(&book, Side::Buy, 100).unwrap().created_at, ts(3000));
        }

        #[test]
        fn repricing_into_a_new_level_uses_the_update_receive_time() {
            // Given an order resting at price 100, opened on a frame received at t=1000.
            let mut book = OrderBook::default();
            book.upsert(Side::Buy, &order(1, 100, 5), ts(1000));

            // When it reprices under the same id onto empty price 99 on a frame received at t=2000.
            book.upsert(Side::Buy, &order(1, 99, 5), ts(2000));

            // Then the freshly-opened level is stamped with that frame's receive time.
            assert_eq!(level(&book, Side::Buy, 99).unwrap().created_at, ts(2000));
        }

        #[test]
        fn resizing_a_level_advances_updated_at_but_keeps_created_at() {
            // Given a level opened on a frame received at t=1000.
            let mut book = OrderBook::default();
            book.upsert(Side::Buy, &order(1, 100, 5), ts(1000));
            let opened = level(&book, Side::Buy, 100).unwrap();
            assert_eq!((opened.created_at, opened.updated_at), (ts(1000), ts(1000)));

            // When the resting order is resized on a later frame received at t=2000.
            book.upsert(Side::Buy, &order(1, 100, 8), ts(2000));

            // Then the level keeps its creation time but its updated time advances to t=2000.
            let resized = level(&book, Side::Buy, 100).unwrap();
            assert_eq!(
                (resized.created_at, resized.updated_at),
                (ts(1000), ts(2000))
            );
        }

        #[test]
        fn removing_one_order_of_a_surviving_level_advances_updated_at() {
            // Given a two-order level opened on a frame received at t=1000.
            let mut book = OrderBook::default();
            book.upsert(Side::Buy, &order(1, 100, 5), ts(1000));
            book.upsert(Side::Buy, &order(2, 100, 3), ts(1000));

            // When one of the two orders is removed on a later frame received at t=2000, leaving the
            // level non-empty.
            book.upsert(Side::Buy, &order(1, 100, 0), ts(2000));

            // Then the surviving level keeps its creation time but its updated time advances.
            let survived = level(&book, Side::Buy, 100).unwrap();
            assert_eq!(
                (survived.created_at, survived.updated_at),
                (ts(1000), ts(2000))
            );
        }
    }

    mod store {
        use crate::*;

        const AREA: EicCode = EicCode::AREA_FR;

        fn key() -> BookKey {
            BookKey {
                market_type: MarketType::Local,
                contract_id: 4242,
            }
        }

        fn order(id: i64, quantity: i32) -> Order {
            Order {
                id,
                price: 100,
                quantity,
                created_at: ts(0),
            }
        }

        fn update(
            key: BookKey,
            revision_no: i64,
            buys: Vec<Order>,
            sells: Vec<Order>,
        ) -> BookUpdate {
            BookUpdate {
                key,
                revision_no,
                buys,
                sells,
            }
        }

        #[test]
        fn apply_stamps_new_levels_with_the_frame_receive_time() {
            // Given an empty store.
            let mut store = OrderBookStore::new();

            // When a frame received at t=1234 (unix nanos) opens a level.
            store.apply(
                AREA,
                &[update(key(), 1, vec![order(1, 5)], vec![])],
                true,
                ts(1234),
            );

            // Then the opened level carries that receive time as its creation time.
            assert_eq!(
                store
                    .book(AREA, &key())
                    .unwrap()
                    .best_bid()
                    .unwrap()
                    .created_at,
                ts(1234)
            );
        }

        #[test]
        fn apply_routes_orders_to_the_keyed_book() {
            // Given an empty store.
            let mut store = OrderBookStore::new();

            // When a snapshot update for one key folds in two buys and one sell.
            store.apply(
                AREA,
                &[update(
                    key(),
                    7,
                    vec![order(1, 5), order(2, 3)],
                    vec![order(3, 2)],
                )],
                true,
                ts(0),
            );

            // Then the store creates the book under that key and routes the orders to the
            // right sides — two on the bid, one on the ask — at the update's revision.
            let book = store.book(AREA, &key()).expect("book exists");
            assert_eq!(book.best_bid().map(|l| l.order_count), Some(2));
            assert_eq!(book.best_ask().map(|l| l.order_count), Some(1));
            assert_eq!(book.last_revision(), 7);
        }

        #[test]
        fn store_tracks_multiple_books() {
            // Given two distinct keys differing only by contract id.
            let mut store = OrderBookStore::new();
            let key_a = key();
            let key_b = BookKey {
                contract_id: 9999,
                ..key_a
            };

            // When a single apply carries one update per key.
            store.apply(
                AREA,
                &[
                    update(key_a, 1, vec![order(1, 5)], vec![]),
                    update(key_b, 1, vec![], vec![order(2, 3)]),
                ],
                true,
                ts(0),
            );

            // Then the store keeps a separate book per key — two in all — each holding only
            // its own update's orders (key_b's lone sell shows on its ask side).
            assert_eq!(store.book_count(), 2);
            assert_eq!(
                store
                    .book(AREA, &key_b)
                    .unwrap()
                    .best_ask()
                    .map(|l| l.order_count),
                Some(1)
            );
        }

        #[test]
        fn snapshot_apply_drops_books_it_doesnt_mention() {
            // Given two books from a first snapshot.
            let mut store = OrderBookStore::new();
            let key_a = key();
            let key_b = BookKey {
                contract_id: 9999,
                ..key_a
            };
            store.apply(
                AREA,
                &[
                    update(key_a, 1, vec![order(1, 5)], vec![]),
                    update(key_b, 1, vec![order(2, 3)], vec![]),
                ],
                true,
                ts(0),
            );
            assert_eq!(store.book_count(), 2);

            // When a later snapshot mentions only key_a.
            store.apply(
                AREA,
                &[update(key_a, 2, vec![order(1, 5)], vec![])],
                true,
                ts(0),
            );

            // Then key_b's stale book is gone, not lingering.
            assert_eq!(store.book_count(), 1);
            assert!(store.book(AREA, &key_b).is_none());
        }

        #[test]
        fn delta_apply_preserves_books_it_doesnt_mention() {
            // Given two books from a snapshot.
            let mut store = OrderBookStore::new();
            let key_a = key();
            let key_b = BookKey {
                contract_id: 9999,
                ..key_a
            };
            store.apply(
                AREA,
                &[
                    update(key_a, 1, vec![order(1, 5)], vec![]),
                    update(key_b, 1, vec![order(2, 3)], vec![]),
                ],
                true,
                ts(0),
            );

            // When a delta touches only key_a.
            store.apply(
                AREA,
                &[update(key_a, 2, vec![order(3, 7)], vec![])],
                false,
                ts(0),
            );

            // Then key_b is untouched, not dropped.
            assert_eq!(store.book_count(), 2);
            assert!(store.book(AREA, &key_b).is_some());
        }

        #[test]
        fn snapshot_for_one_area_leaves_other_areas_untouched() {
            // Given the same contract keyed into two different delivery areas — distinct books
            // that happen to share a BookKey but live under different area indexes.
            let mut store = OrderBookStore::new();
            let other = EicCode::AREA_NO1;
            store.apply(
                AREA,
                &[update(key(), 1, vec![order(1, 5)], vec![])],
                true,
                ts(0),
            );
            store.apply(
                other,
                &[update(key(), 1, vec![order(2, 3)], vec![])],
                true,
                ts(0),
            );
            assert_eq!(store.book_count(), 2);

            // When a fresh snapshot for AREA arrives that no longer mentions the contract — a
            // snapshot replaces only its own area's books.
            store.apply(AREA, &[], true, ts(0));

            // Then AREA's book is gone but the other area's identical-key book survives: snapshot
            // semantics are scoped per delivery area, not global.
            assert_eq!(store.book_count(), 1);
            assert!(store.book(AREA, &key()).is_none());
            assert_eq!(
                store
                    .book(other, &key())
                    .and_then(|b| b.best_bid())
                    .map(|l| l.order_count),
                Some(1)
            );
        }
    }

    mod projection {
        use crate::*;

        /// The depth most tests project at; larger than any book they build, so all levels show.
        const DEPTH: usize = 20;

        /// Test stand-in for the feed protocol: snapshot resets, per-order upserts, revision.
        fn apply(
            book: &mut OrderBook,
            is_snapshot: bool,
            revision_no: i64,
            buys: &[Order],
            sells: &[Order],
        ) {
            if is_snapshot {
                book.clear();
            }
            for order in buys {
                book.upsert(Side::Buy, order, ts(0));
            }
            for order in sells {
                book.upsert(Side::Sell, order, ts(0));
            }
            book.set_last_revision(revision_no);
        }

        fn key() -> BookKey {
            BookKey {
                market_type: MarketType::Local,
                contract_id: 101,
            }
        }

        fn order(id: i64, price: i64, quantity: i32) -> Order {
            Order {
                id,
                price,
                quantity,
                created_at: ts(0),
            }
        }

        #[test]
        fn from_book_captures_best_levels_and_revision() {
            // Given a book with two levels on each side, built at revision 7.
            let mut book = OrderBook::default();
            apply(
                &mut book,
                true,
                7,
                &[order(1, 100, 5), order(2, 99, 3)],
                &[order(3, 101, 2), order(4, 102, 8)],
            );

            // When projected into a depth snapshot at a depth larger than the book.
            let top = DepthView::from_book(key(), &book, DEPTH);

            // Then both sides are best-first, deeper levels included.
            assert_eq!(top.revision, 7);
            assert_eq!(
                top.bids
                    .iter()
                    .map(|l| (l.price, l.total_quantity))
                    .collect::<Vec<_>>(),
                vec![(100, 5), (99, 3)]
            );
            assert_eq!(
                top.asks
                    .iter()
                    .map(|l| (l.price, l.total_quantity))
                    .collect::<Vec<_>>(),
                vec![(101, 2), (102, 8)]
            );
        }

        #[test]
        fn from_book_yields_empty_sides_for_empty_book() {
            // Given
            let book = OrderBook::default();

            // When / Then
            let top = DepthView::from_book(key(), &book, DEPTH);
            assert!(top.bids.is_empty());
            assert!(top.asks.is_empty());
            assert_eq!(top.revision, 0);
        }

        #[test]
        fn from_book_caps_each_side_at_requested_depth() {
            // Given more bid levels than the requested depth.
            let mut book = OrderBook::default();
            let buys: Vec<Order> = (0..DEPTH as i64 + 5)
                .map(|i| order(i, 1000 - i, 1))
                .collect();
            apply(&mut book, true, 1, &buys, &[]);

            // When projected at DEPTH.
            let top = DepthView::from_book(key(), &book, DEPTH);

            // Then only the best DEPTH are kept, best-first.
            assert_eq!(top.bids.len(), DEPTH);
            assert_eq!(top.bids[0].price, 1000);
            assert_eq!(top.bids[DEPTH - 1].price, 1000 - (DEPTH as i64 - 1));
        }
    }

    mod top_hash {
        use crate::*;

        /// Test stand-in for the feed protocol: snapshot resets, per-order upserts, revision.
        fn apply(
            book: &mut OrderBook,
            is_snapshot: bool,
            revision_no: i64,
            buys: &[Order],
            sells: &[Order],
        ) {
            if is_snapshot {
                book.clear();
            }
            for order in buys {
                book.upsert(Side::Buy, order, ts(0));
            }
            for order in sells {
                book.upsert(Side::Sell, order, ts(0));
            }
            book.set_last_revision(revision_no);
        }

        fn order(id: i64, price: i64, quantity: i32) -> Order {
            Order {
                id,
                price,
                quantity,
                created_at: ts(0),
            }
        }

        #[test]
        fn identical_tops_hash_equally_and_revision_is_ignored() {
            // Given two books with the same orders but different revisions.
            let mut a = OrderBook::default();
            apply(&mut a, true, 1, &[order(1, 100, 5)], &[order(2, 101, 3)]);
            let mut b = OrderBook::default();
            apply(&mut b, true, 99, &[order(1, 100, 5)], &[order(2, 101, 3)]);

            // When / Then the digest ignores the revision — equal tops, equal hash.
            assert_eq!(a.top_hash(20), b.top_hash(20));
        }

        #[test]
        fn a_change_within_depth_changes_the_hash() {
            // Given a book and its hash.
            let mut book = OrderBook::default();
            apply(&mut book, true, 1, &[order(1, 100, 5)], &[order(2, 101, 3)]);
            let before = book.top_hash(20);

            // When the quantity of a visible top level changes.
            apply(&mut book, false, 2, &[order(1, 100, 9)], &[]);

            // Then the digest moves.
            assert_ne!(before, book.top_hash(20));
        }

        #[test]
        fn a_change_beyond_depth_does_not_change_the_hash() {
            // Given three bid levels, hashed at depth 2 (so the third is invisible).
            let mut book = OrderBook::default();
            apply(
                &mut book,
                true,
                1,
                &[order(1, 100, 5), order(2, 99, 5), order(3, 98, 5)],
                &[],
            );
            let before = book.top_hash(2);

            // When only the third-deepest level changes.
            apply(&mut book, false, 2, &[order(3, 98, 7)], &[]);

            // Then the top-2 digest is unchanged, but a digest deep enough to see it moves.
            assert_eq!(before, book.top_hash(2));
            assert_ne!(book.top_hash(2), book.top_hash(3));
        }

        #[test]
        fn the_side_split_is_part_of_the_digest() {
            // Given two books holding the same two byte-identical levels, but split differently
            // across sides: both on the bid side vs one per side.
            let mut both_bids = OrderBook::default();
            apply(
                &mut both_bids,
                true,
                1,
                &[order(1, 100, 5), order(2, 99, 5)],
                &[],
            );
            let mut one_each = OrderBook::default();
            apply(
                &mut one_each,
                true,
                1,
                &[order(1, 100, 5)],
                &[order(2, 99, 5)],
            );

            // When / Then the side separator keeps the two arrangements from colliding.
            assert_ne!(both_bids.top_hash(20), one_each.top_hash(20));
        }
    }
}
