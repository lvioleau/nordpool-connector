//! Contract-count gauge, computed from the global contract store — the reference-data twin of
//! [`BookMetrics`](super::book_metrics::BookMetrics). Emitted once per `Contract` frame (the
//! only thing that changes the store), so no time-based throttling: there is nothing to
//! re-emit between contract updates.

use std::collections::HashMap;

use nordpool_marketdata::{ContractStore, MarketType, ProductType};

/// Emit `nordpool.contracts` from the global `store`: the number of contracts grouped by
/// `market_type` and `product_type` (the lowercase tag spellings from
/// [`market_type_tag`](super::market_type_tag) / [`product_type_tag`](super::product_type_tag) —
/// the domain enum already collapses every non-periodic wire product into `Other`). A contract
/// counts once however many areas it trades in, and aggregating over contract ids (no
/// per-contract tag) keeps the series count bounded as intraday contracts churn. Only
/// combinations present in the store are emitted (a group that empties out keeps its last value
/// until it reappears).
pub(crate) fn emit_contract_counts(store: &ContractStore) {
    let mut counts: HashMap<(MarketType, ProductType), u64> = HashMap::new();
    for contract in store.iter() {
        *counts
            .entry((contract.market_type, contract.product_type))
            .or_default() += 1;
    }
    for ((market_type, product_type), count) in counts {
        metrics::gauge!(
            "nordpool.contracts",
            "market_type" => super::market_type_tag(market_type),
            "product_type" => super::product_type_tag(product_type),
        )
        .set(count as f64);
    }
}

#[cfg(test)]
mod tests {
    use metrics_util::debugging::{DebugValue, DebuggingRecorder};
    use nordpool_marketdata::{
        Contract, ContractAreaState, ContractState, ContractStore, EicCode, MarketType, ProductType,
    };

    use super::emit_contract_counts;
    use crate::sink::ts;

    fn contract(id: i64, product: ProductType, areas: &[(EicCode, ContractState)]) -> Contract {
        Contract {
            contract_id: id,
            name: format!("C{id}"),
            market_id: "M".into(),
            market_type: MarketType::Local,
            product_type: product,
            delivery_start: ts(0),
            delivery_end: ts(0),
            duration_seconds: 0,
            updated_at: ts(0),
            areas: areas
                .iter()
                .map(|&(delivery_area_eic, state)| ContractAreaState {
                    delivery_area_eic,
                    state,
                    phase_start: ts(0),
                    phase_end: ts(0),
                })
                .collect(),
        }
    }

    #[test]
    fn emits_contract_counts_per_market_and_product() {
        // Given a global store: a P60 contract trading in two areas, and two P15 contracts
        // trading in one.
        let mut store = ContractStore::new();
        store.apply(
            &[
                contract(
                    1,
                    ProductType::P60Min,
                    &[
                        (EicCode::AREA_FR, ContractState::Active),
                        (EicCode::AREA_CH, ContractState::Frozen),
                    ],
                ),
                contract(
                    2,
                    ProductType::P15Min,
                    &[(EicCode::AREA_FR, ContractState::Active)],
                ),
                contract(
                    3,
                    ProductType::P15Min,
                    &[(EicCode::AREA_FR, ContractState::Active)],
                ),
            ],
            true,
            ts(0),
        );

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // When emit_contract_counts fires once for the store.
        metrics::with_local_recorder(&recorder, || {
            emit_contract_counts(&store);
        });

        // Then the P60 contract counts once despite trading in two areas, and the two P15
        // contracts sum into their own series rather than collapsing with it.
        let snapshot = snapshotter.snapshot().into_vec();
        let gauge = |product: &str| {
            let (_, _, _, value) = snapshot
                .iter()
                .find(|(key, ..)| {
                    key.key().name() == "nordpool.contracts"
                        && key
                            .key()
                            .labels()
                            .any(|l| l.key() == "product_type" && l.value() == product)
                })
                .unwrap_or_else(|| panic!("gauge for {product} not recorded"));
            match value {
                DebugValue::Gauge(v) => v.into_inner(),
                other => panic!("expected gauge, got {other:?}"),
            }
        };
        assert_eq!(gauge("p60min"), 1.0);
        assert_eq!(gauge("p15min"), 2.0);
    }
}
