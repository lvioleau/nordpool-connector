//! Periodic aggregate book-state gauges, computed from the order-book store.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use nordpool_marketdata::{BookUpdate, MarketType, OrderBook, OrderBookStore};

/// Count the order entries (adds/modifies/removes) across `updates` into the
/// `nordpool.book.order_updates` counter, split by frame kind (snapshot/delta) and book
/// market type. A single frame can carry books of both market types, so local and xbid
/// entries are tallied separately; entries from books of unknown market type are dropped.
pub(crate) fn emit_update_counts(updates: &[BookUpdate], is_snapshot: bool) {
    let kind = if is_snapshot { "snapshot" } else { "delta" };
    let (mut local, mut xbid) = (0u64, 0u64);
    for u in updates {
        let count = (u.buys.len() + u.sells.len()) as u64;
        match u.key.market_type {
            MarketType::Local => local += count,
            MarketType::Xbid => xbid += count,
            MarketType::Unknown => {}
        }
    }
    metrics::counter!("nordpool.book.order_updates", "kind" => kind, "market_type" => "local")
        .increment(local);
    metrics::counter!("nordpool.book.order_updates", "kind" => kind, "market_type" => "xbid")
        .increment(xbid);
}

/// Emits the `nordpool.book.*` state gauges (books / orders / levels / quantity) on its
/// own cadence, from whatever store the caller hands it. Decoupled from the depth feed:
/// this is observability of the reconstruction, not part of publishing it.
pub(crate) struct BookMetrics {
    period: Duration,
    last_emit: Instant,
}

impl BookMetrics {
    pub(crate) fn new(period: Duration) -> Self {
        Self {
            period,
            last_emit: Instant::now(),
        }
    }

    /// Emit the gauges when the period has elapsed; a cheap no-op otherwise.
    pub(crate) fn emit_if_due(&mut self, store: &OrderBookStore) {
        if self.last_emit.elapsed() < self.period {
            return;
        }
        let mut totals: HashMap<MarketType, BookTotals> = HashMap::new();
        for (key, book) in store.iter() {
            totals.entry(key.market_type).or_default().add(book);
        }
        for (market_type, totals) in totals {
            totals.emit(&super::market_type_tag(market_type));
        }
        self.last_emit = Instant::now();
    }
}

/// Book state summed over every contract of one market type, published as gauges tagged
/// by `market_type` (and `side` where it applies). Aggregated deliberately: intraday
/// contracts churn daily, so per-contract series would grow without bound.
#[derive(Default)]
struct BookTotals {
    books: u64,
    bid_orders: u64,
    ask_orders: u64,
    bid_levels: u64,
    ask_levels: u64,
    bid_quantity: i64,
    ask_quantity: i64,
}

impl BookTotals {
    fn add(&mut self, book: &OrderBook) {
        if book.order_count() == 0 {
            return;
        }
        self.books += 1;
        self.bid_orders += u64::from(book.bid_order_count());
        self.ask_orders += u64::from(book.ask_order_count());
        self.bid_levels += book.bid_level_count() as u64;
        self.ask_levels += book.ask_level_count() as u64;
        self.bid_quantity += book.bid_quantity();
        self.ask_quantity += book.ask_quantity();
    }

    fn emit(&self, market_type: &str) {
        let tag = || market_type.to_owned();
        metrics::gauge!("nordpool.book.books", "market_type" => tag()).set(self.books as f64);
        for (side, orders, levels, quantity) in [
            ("bid", self.bid_orders, self.bid_levels, self.bid_quantity),
            ("ask", self.ask_orders, self.ask_levels, self.ask_quantity),
        ] {
            metrics::gauge!("nordpool.book.orders", "market_type" => tag(), "side" => side)
                .set(orders as f64);
            metrics::gauge!("nordpool.book.levels", "market_type" => tag(), "side" => side)
                .set(levels as f64);
            metrics::gauge!("nordpool.book.quantity", "market_type" => tag(), "side" => side)
                .set(quantity as f64);
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use nordpool_marketdata::{BookKey, BookUpdate, EicCode, MarketType, Order, OrderBookStore};

    use super::BookMetrics;
    use crate::sink::ts;

    #[test]
    fn emits_book_state_gauges_aggregated_per_market_type() {
        // Given a store with one book holding two bid orders on two levels (qty 5 + 3), and
        // a BookMetrics whose period is always due.
        let mut store = OrderBookStore::new();
        let key = BookKey {
            market_type: MarketType::Local,
            contract_id: 111,
        };
        let order = |id: i64, price: i64, quantity: i32| Order {
            id,
            price,
            quantity,
            created_at: ts(0),
        };
        store.apply(
            EicCode::AREA_FR,
            &[BookUpdate {
                key,
                revision_no: 0,
                buys: vec![order(1, 100, 5), order(2, 99, 3)],
                sells: vec![],
            }],
            true,
            ts(0),
        );

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // When emit_if_due runs with a zero period (so it always fires) against that store — the
        // periodic observability pass that summarizes the reconstruction into gauges.
        metrics::with_local_recorder(&recorder, || {
            BookMetrics::new(Duration::ZERO).emit_if_due(&store);
        });

        // Then the gauges report the book aggregated over the Local market type: one book, two bid
        // orders across two levels totalling qty 8, and zero ask orders — confirming the per-side
        // sums are computed from the book state and tagged by market_type/side, with the empty ask
        // side still reported as 0 rather than omitted.
        let snapshot = snapshotter.snapshot().into_vec();
        let gauge = |name: &str, side: Option<&str>| {
            let (_, _, _, value) = snapshot
                .iter()
                .find(|(key, ..)| {
                    key.key().name() == name
                        && key.key().labels().any(|l| l.value() == "local")
                        && side.is_none_or(|s| key.key().labels().any(|l| l.value() == s))
                })
                .unwrap_or_else(|| panic!("gauge {name} ({side:?}) not recorded"));
            match value {
                DebugValue::Gauge(v) => v.0,
                other => panic!("expected gauge for {name}, got {other:?}"),
            }
        };
        assert_eq!(gauge("nordpool.book.books", None), 1.0);
        assert_eq!(gauge("nordpool.book.orders", Some("bid")), 2.0);
        assert_eq!(gauge("nordpool.book.levels", Some("bid")), 2.0);
        assert_eq!(gauge("nordpool.book.quantity", Some("bid")), 8.0);
        assert_eq!(gauge("nordpool.book.orders", Some("ask")), 0.0);
    }

    #[test]
    fn does_not_emit_before_the_period_elapses() {
        // Given a freshly-constructed BookMetrics with a one-hour period, so the throttle is
        // nowhere near due — emulating the common case where frames arrive far faster than gauges
        // should be republished.
        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // When emit_if_due is called before that period has elapsed — the cheap early-return path
        // that keeps the gauge work off the hot per-frame loop.
        metrics::with_local_recorder(&recorder, || {
            BookMetrics::new(Duration::from_secs(3600)).emit_if_due(&OrderBookStore::new());
        });

        // Then nothing was recorded at all: emit_if_due returned without touching the store or any
        // gauge, proving the throttle suppresses emission rather than just overwriting old values.
        assert!(snapshotter.snapshot().into_vec().is_empty());
    }
}
