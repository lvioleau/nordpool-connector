//! The viewer's domain types: a timestamped top-N book snapshot (what both the gRPC live feed
//! and the replay Parquet reduce to) and the segment model the chart draws — horizontal bars,
//! one per (side, price) over the interval that level held a constant quantity.

use std::collections::HashMap;

#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Clone, Debug)]
pub struct Level {
    pub price: i64,
    pub quantity: i64,
    /// Carried from the feed; not yet shown by the chart.
    #[allow(dead_code)]
    pub order_count: u32,
}

/// The periodic product shapes the connector tracks; every other shape folds into `Other`.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ProductType {
    P15,
    P30,
    P60,
    Other,
}

impl ProductType {
    pub fn label(self) -> &'static str {
        match self {
            ProductType::P15 => "P15",
            ProductType::P30 => "P30",
            ProductType::P60 => "P60",
            ProductType::Other => "OTH",
        }
    }
}

/// Reference data for one contract, from the stream-server's `SubscribeContracts` stream. Carries
/// only what the viewer shows.
#[derive(Clone, Debug)]
pub struct ContractMeta {
    pub contract_id: i64,
    pub name: String,
    /// From `SubscribeContracts`; the viewer maps a snapshot's `contract_id` to this for the
    /// market filter and the detail panel (depth snapshots no longer carry a market type).
    pub market_type: String,
    pub product_type: ProductType,
    /// Delivery window in epoch milliseconds (UTC).
    pub delivery_start_ms: i64,
    pub delivery_end_ms: i64,
    /// Active in at least one of its delivery areas.
    pub active: bool,
}

#[derive(Clone, Debug)]
pub struct TimestampedSnapshot {
    /// Epoch milliseconds. Live = receive wall-clock; replay = archived event time.
    pub t: i64,
    pub contract_id: i64,
    pub revision: i64,
    /// Best-first (bids high→low, asks low→high), capped at the feed's top-N.
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// One drawn rectangle: a price level holding `qty` over `[t_start, t_end]`. `t_end == None`
/// means still open (drawn to "now").
#[derive(Clone, Debug)]
pub struct Segment {
    pub side: Side,
    pub price: i64,
    pub qty: i64,
    pub t_start: i64,
    pub t_end: Option<i64>,
}

/// A reconstructed book at one instant, levels best-first: bids high→low, asks low→high.
#[derive(Clone, Debug, Default)]
pub struct BookState {
    pub bids: Vec<(i64, i64)>, // (price, quantity)
    pub asks: Vec<(i64, i64)>,
}

/// Folds a stream of full-state snapshots for **one** book into segments. Because each
/// snapshot is full top-N state (not a delta), the fold is a plain set-diff per side and is
/// robust to dropped messages.
pub struct SegmentBook {
    pub completed: Vec<Segment>,
    active: HashMap<(Side, i64), Segment>,
    pub last_t: i64,
    retention_ms: i64,
}

impl SegmentBook {
    pub fn new(retention_ms: i64) -> Self {
        Self {
            completed: Vec::new(),
            active: HashMap::new(),
            last_t: 0,
            retention_ms,
        }
    }

    pub fn apply(&mut self, snap: &TimestampedSnapshot) {
        self.fold_side(Side::Bid, &snap.bids, snap.t);
        self.fold_side(Side::Ask, &snap.asks, snap.t);
        self.last_t = snap.t;
        self.prune();
    }

    /// Close segments whose quantity changed or whose level left the top-N; open new ones for
    /// changed or newly-present levels; leave unchanged levels extending.
    fn fold_side(&mut self, side: Side, levels: &[Level], t: i64) {
        let mut present = Vec::with_capacity(levels.len());
        for lv in levels {
            present.push(lv.price);
            let key = (side, lv.price);
            match self.active.get_mut(&key) {
                Some(open) if open.qty == lv.quantity => {} // unchanged: extends
                Some(open) => {
                    let mut closed = open.clone();
                    closed.t_end = Some(t);
                    self.completed.push(closed);
                    *open = Segment {
                        side,
                        price: lv.price,
                        qty: lv.quantity,
                        t_start: t,
                        t_end: None,
                    };
                }
                None => {
                    self.active.insert(
                        key,
                        Segment {
                            side,
                            price: lv.price,
                            qty: lv.quantity,
                            t_start: t,
                            t_end: None,
                        },
                    );
                }
            }
        }
        // Close any open level on this side that vanished from the top-N.
        self.active.retain(|&(s, price), open| {
            if s == side && !present.contains(&price) {
                let mut closed = open.clone();
                closed.t_end = Some(t);
                self.completed.push(closed);
                false
            } else {
                true
            }
        });
    }

    fn prune(&mut self) {
        let cutoff = self.last_t - self.retention_ms;
        if self
            .completed
            .first()
            .is_some_and(|s| s.t_end.unwrap_or(self.last_t) < cutoff)
        {
            self.completed
                .retain(|s| s.t_end.unwrap_or(self.last_t) >= cutoff);
        }
    }

    /// All segments, completed then still-open.
    pub fn segments(&self) -> impl Iterator<Item = &Segment> {
        self.completed.iter().chain(self.active.values())
    }

    /// Reconstruct the visible book at instant `t` from the folded segments — the level set
    /// whose segment spans `t`. `t` past `last_t` yields the current (still-open) book.
    pub fn at(&self, t: i64) -> BookState {
        let mut book = BookState::default();
        for s in self.segments() {
            let covers = t >= s.t_start && s.t_end.is_none_or(|end| t < end);
            if !covers {
                continue;
            }
            match s.side {
                Side::Bid => book.bids.push((s.price, s.qty)),
                Side::Ask => book.asks.push((s.price, s.qty)),
            }
        }
        book.bids.sort_unstable_by_key(|b| std::cmp::Reverse(b.0));
        book.asks.sort_unstable_by_key(|a| a.0);
        book
    }
}

/// One public-statistic observation for a contract at a receive instant — the step values the
/// chart plots over time. Prices are scaled integers like the book (×100). A field is `None` until
/// the server has a value to publish, breaking that line until it does.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct StatPoint {
    /// Epoch milliseconds, from the statistic frame's `received_at_nanos` (or receipt time on the
    /// seed, whose `received_at_nanos` is 0).
    pub t: i64,
    pub day_ahead_price: Option<i64>,
    pub vwap: Option<i64>,
    /// Cumulative traded quantity for the day; the rise between observations is the executed
    /// quantity over the interval the volume bars draw.
    pub turnover: Option<i64>,
}

/// Executed quantity over one interval, derived from the rise in cumulative turnover between two
/// consecutive observations. The volume pane draws one bar per interval.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ExecBar {
    pub t_start: i64,
    pub t_end: i64,
    pub qty: i64,
}

/// One contract's statistic observation, as the source feeds it to the app: a [`StatPoint`] tagged
/// with the contract it belongs to.
#[derive(Clone, Copy, Debug)]
pub struct StatSample {
    pub contract_id: i64,
    pub point: StatPoint,
}

/// A time-ordered series of public-statistic observations for one contract. The chart draws each
/// value (day-ahead price, VWAP) as a step function: a value holds from its timestamp until the
/// next observation. Consecutive points with identical values collapse so the series stays compact.
pub struct StatSeries {
    pub points: Vec<StatPoint>,
    retention_ms: i64,
}

impl StatSeries {
    pub fn new(retention_ms: i64) -> Self {
        Self {
            points: Vec::new(),
            retention_ms,
        }
    }

    /// Append an observation. Out-of-order points (older than the last) are dropped, and a point
    /// whose values match the last recorded one is collapsed into it (the step is unchanged).
    pub fn push(&mut self, p: StatPoint) {
        if let Some(last) = self.points.last() {
            if p.t < last.t {
                return;
            }
            if last.day_ahead_price == p.day_ahead_price
                && last.vwap == p.vwap
                && last.turnover == p.turnover
            {
                return;
            }
        }
        self.points.push(p);
        self.prune();
    }

    /// Executed quantity per interval, from the rise in cumulative turnover between consecutive
    /// observations. Intervals where either turnover is missing, or where it doesn't increase (a
    /// day rollover resets the cumulative total, or simply no trade), produce no bar.
    pub fn exec_bars(&self) -> Vec<ExecBar> {
        self.points
            .windows(2)
            .filter_map(|w| {
                let (prev, cur) = (&w[0], &w[1]);
                let qty = cur.turnover? - prev.turnover?;
                (qty > 0).then_some(ExecBar {
                    t_start: prev.t,
                    t_end: cur.t,
                    qty,
                })
            })
            .collect()
    }

    /// Drop points older than the retention window, but keep the most recent one before the cutoff
    /// so the left edge of the visible step still carries the value that held there.
    fn prune(&mut self) {
        let Some(cutoff) = self.points.last().map(|p| p.t - self.retention_ms) else {
            return;
        };
        if let Some(keep_from) = self.points.iter().rposition(|p| p.t < cutoff) {
            self.points.drain(..keep_from);
        }
    }
}

/// Parse the `area` query parameter out of a URL search string (e.g.
/// `"?area=10YFI-1--------U&x=1"`) into a delivery-area EIC code. Returns `None` when the
/// search is absent or the parameter is missing or empty, so the caller can fall back to a
/// default.
pub fn delivery_area_from_query(search: Option<&str>) -> Option<String> {
    search?
        .trim_start_matches('?')
        .split('&')
        .find_map(|pair| pair.strip_prefix("area="))
        .filter(|value| !value.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn snap(t: i64, bids: &[(i64, i64)], asks: &[(i64, i64)]) -> TimestampedSnapshot {
        let lv = |&(price, quantity): &(i64, i64)| Level {
            price,
            quantity,
            order_count: 1,
        };
        TimestampedSnapshot {
            t,
            contract_id: 1,
            revision: t,
            bids: bids.iter().map(lv).collect(),
            asks: asks.iter().map(lv).collect(),
        }
    }

    #[test]
    fn unchanged_level_extends_one_segment() {
        // Given two consecutive snapshots that report the same bid level (price 100, qty 5) —
        // the level held a constant quantity across the interval, with no change to record.
        let mut book = SegmentBook::new(i64::MAX);
        book.apply(&snap(0, &[(100, 5)], &[]));
        book.apply(&snap(10, &[(100, 5)], &[]));
        // Then the level stays one still-open segment that simply extends to cover both
        // instants — an unchanged level never closes, so nothing lands in `completed`.
        assert_eq!(book.completed.len(), 0);
        assert_eq!(book.segments().count(), 1);
    }

    #[test]
    fn quantity_change_closes_and_opens() {
        // Given a bid level at price 100 whose quantity changes between snapshots (5 at t=0,
        // then 8 at t=10) — the same price now holds a different size, a distinct depth state.
        let mut book = SegmentBook::new(i64::MAX);
        book.apply(&snap(0, &[(100, 5)], &[]));
        book.apply(&snap(10, &[(100, 8)], &[]));
        // Then the original qty-5 segment is closed off at t=10 (the moment the quantity moved)
        // and a fresh qty-8 segment opens, so the level is now two drawn bars, not one.
        assert_eq!(book.completed.len(), 1);
        assert_eq!(book.completed[0].t_end, Some(10));
        assert_eq!(book.completed[0].qty, 5);
        assert_eq!(book.segments().count(), 2);
    }

    #[test]
    fn at_reconstructs_book_at_past_and_live_instants() {
        // Given three snapshots over time where the top bid re-quotes (100 goes 5→8 at t=10)
        // and a lower bid (99) drops out of the top-N at t=20, while the lone ask is steady —
        // a history with both a quantity change and a vanishing level for `at` to reconstruct.
        let mut book = SegmentBook::new(i64::MAX);
        book.apply(&snap(0, &[(100, 5), (99, 3)], &[(101, 4)]));
        book.apply(&snap(10, &[(100, 8), (99, 3)], &[(101, 4)])); // 100 re-quotes 5→8
        book.apply(&snap(20, &[(100, 8)], &[(101, 4)])); // 99 vanishes

        // When reconstructing the book at t=5 — an instant inside the first interval, before any
        // change — `at` returns the level set whose segments span that time, sorted best-first.
        // Then it shows the original quantities (100@5, 99@3), proving past state is recoverable.
        let early = book.at(5);
        assert_eq!(early.bids, vec![(100, 5), (99, 3)]);
        assert_eq!(early.asks, vec![(101, 4)]);

        // When querying at t == last_t (20), and beyond it — `at` treats still-open segments as
        // extending to "now", so this yields the current live book.
        // Then only the surviving 100@8 bid remains; 99 is gone and the ask is unchanged.
        let live = book.at(20);
        assert_eq!(live.bids, vec![(100, 8)]);
        assert_eq!(live.asks, vec![(101, 4)]);
        assert_eq!(book.at(i64::MAX).bids, vec![(100, 8)]);
    }

    #[test]
    fn vanished_level_closes() {
        // Given two bid levels at t=0 (100@5 and 99@3), where the second snapshot at t=10 reports
        // only the 100 level — the 99 level has left the top-N, the way depth shrinks on the feed.
        let mut book = SegmentBook::new(i64::MAX);
        book.apply(&snap(0, &[(100, 5), (99, 3)], &[]));
        book.apply(&snap(10, &[(100, 5)], &[]));
        // Then the vanished 99 level is closed at t=10 (the snapshot that dropped it), while the
        // still-present, unchanged 100 level keeps extending as a single open segment.
        assert_eq!(book.completed.len(), 1);
        assert_eq!(book.completed[0].price, 99);
        assert_eq!(book.completed[0].t_end, Some(10));
    }

    fn stat(t: i64, day_ahead: Option<i64>, vwap: Option<i64>) -> StatPoint {
        StatPoint {
            t,
            day_ahead_price: day_ahead,
            vwap,
            turnover: None,
        }
    }

    fn stat_turnover(t: i64, turnover: Option<i64>) -> StatPoint {
        StatPoint {
            t,
            day_ahead_price: None,
            vwap: None,
            turnover,
        }
    }

    #[test]
    fn stat_series_collapses_repeated_values_and_drops_out_of_order() {
        // Given a series fed three observations: two with identical values, then one whose vwap
        // moved — and finally a stale point timestamped before the last.
        let mut s = StatSeries::new(i64::MAX);
        s.push(stat(0, Some(100), Some(90)));
        s.push(stat(10, Some(100), Some(90))); // identical values

        // When the values change at t=20, then a stale t=15 point arrives.
        s.push(stat(20, Some(100), Some(95)));
        s.push(stat(15, Some(100), Some(99))); // older than the last: dropped

        // Then only the two distinct steps remain — the repeat collapsed, the stale point ignored.
        assert_eq!(s.points.len(), 2);
        assert_eq!(s.points[0], stat(0, Some(100), Some(90)));
        assert_eq!(s.points[1], stat(20, Some(100), Some(95)));
    }

    #[test]
    fn stat_series_prunes_old_points_keeping_one_carry_in() {
        // Given a series with a 100ms retention and points spanning well past it.
        let mut s = StatSeries::new(100);
        s.push(stat(0, Some(1), None));
        s.push(stat(50, Some(2), None));
        s.push(stat(120, Some(3), None));

        // When a point at t=200 arrives, the cutoff is t=100 (200 - retention).
        s.push(stat(200, Some(4), None));

        // Then points before the cutoff are pruned except the most recent one (t=50), kept so the
        // step entering the window still carries its value.
        assert_eq!(
            s.points.iter().map(|p| p.t).collect::<Vec<_>>(),
            vec![50, 120, 200]
        );
    }

    #[test]
    fn exec_bars_are_turnover_rises_skipping_gaps_and_rollovers() {
        // Given a series whose cumulative turnover rises, then resets (a day rollover), then has a
        // gap where turnover is unknown before resuming.
        let mut s = StatSeries::new(i64::MAX);
        s.push(stat_turnover(0, Some(100)));
        s.push(stat_turnover(10, Some(250))); // +150 executed
        s.push(stat_turnover(20, Some(50))); // rollover: decrease, no bar
        s.push(stat_turnover(30, None)); // unknown turnover, no bar either side
        s.push(stat_turnover(40, Some(80))); // resumes, but prev is unknown → no bar

        // When deriving the executed-quantity bars from turnover rises.
        let bars = s.exec_bars();

        // Then only the single genuine rise (t=0→10, +150) becomes a bar; the rollover and the gaps
        // produce none.
        assert_eq!(bars.len(), 1);
        assert_eq!(
            bars[0],
            ExecBar {
                t_start: 0,
                t_end: 10,
                qty: 150
            }
        );
    }

    #[test]
    fn delivery_area_parsed_from_query_else_none() {
        // The area is read out of the page URL's query string; anything else falls back to None so
        // the caller can apply its default.
        assert_eq!(
            delivery_area_from_query(Some("?area=10YFI-1--------U")),
            Some("10YFI-1--------U".to_string())
        );
        assert_eq!(
            delivery_area_from_query(Some("?x=1&area=10YSE-4--------9&y=2")),
            Some("10YSE-4--------9".to_string())
        );
        assert_eq!(delivery_area_from_query(Some("?foo=1")), None);
        assert_eq!(delivery_area_from_query(Some("?area=")), None);
        assert_eq!(delivery_area_from_query(Some("")), None);
        assert_eq!(delivery_area_from_query(None), None);
    }
}
