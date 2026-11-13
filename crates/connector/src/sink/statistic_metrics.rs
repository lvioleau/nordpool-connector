//! Statistic-count gauge, computed from the statistic book — the public-statistic twin of
//! [`emit_contract_counts`](super::contract_metrics::emit_contract_counts). Emitted once per
//! `PublicStatistic` frame (the only thing that changes the book), so no time-based throttling.

use std::collections::HashMap;

use nordpool_marketdata::{EicCode, MarketType, StatisticStore};

/// Emit `nordpool.statistics` for the `book`: the number of contracts with a published statistic,
/// grouped by `delivery_area` and `market_type`. Aggregated over contract ids (no per-contract tag)
/// to keep the series count bounded as intraday contracts churn.
pub(crate) fn emit_statistic_counts(book: &StatisticStore) {
    let mut counts: HashMap<(EicCode, MarketType), u64> = HashMap::new();
    for statistic in book.iter() {
        *counts
            .entry((statistic.delivery_area_eic, statistic.market_type))
            .or_default() += 1;
    }
    for ((area, market_type), count) in counts {
        metrics::gauge!(
            "nordpool.statistics",
            "market_type" => super::market_type_tag(market_type),
            "delivery_area" => area.eic_code(),
        )
        .set(count as f64);
    }
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use nordpool_marketdata::{EicCode, MarketType, PublicStatistic, StatisticStore, Tendency};

    use super::emit_statistic_counts;
    use crate::sink::ts;

    fn statistic(id: i64, area: EicCode) -> PublicStatistic {
        PublicStatistic {
            delivery_area_eic: area,
            market_type: MarketType::Local,
            contract_id: id,
            last_price: Some(100),
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
    fn emits_statistic_counts_per_area_and_market() {
        // Given a store with two statistics in FR and one in NO1.
        let mut book = StatisticStore::new();
        book.apply(
            &[
                statistic(1, EicCode::AREA_FR),
                statistic(2, EicCode::AREA_FR),
                statistic(3, EicCode::AREA_NO1),
            ],
            true,
            ts(0),
        );

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // When the per-frame observability pass tallies statistics into the bounded
        // (delivery_area, market_type) series.
        metrics::with_local_recorder(&recorder, || emit_statistic_counts(&book));

        // Then FR counts 2 and NO1 counts 1, each under its own delivery area (tagged by its
        // EIC code).
        let snapshot = snapshotter.snapshot().into_vec();
        let gauge = |area: &str| {
            let (_, _, _, value) = snapshot
                .iter()
                .find(|(key, ..)| {
                    key.key().name() == "nordpool.statistics"
                        && key
                            .key()
                            .labels()
                            .any(|l| l.key() == "delivery_area" && l.value() == area)
                })
                .unwrap_or_else(|| panic!("gauge for area {area} not recorded"));
            match value {
                DebugValue::Gauge(v) => v.into_inner(),
                other => panic!("expected gauge, got {other:?}"),
            }
        };
        assert_eq!(gauge("10YFR-RTE------C"), 2.0);
        assert_eq!(gauge("10YNO-1--------2"), 1.0);
    }
}
