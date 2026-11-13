use std::collections::HashSet;

use chrono::{DateTime, Utc};

use nordpool_marketdata::{Contract, ContractAreaState, ContractState, EicCode, ProductType};
use nordpool_stream_server::{ContractChannel, ContractSender, ContractUpdate, SharedContracts};

use crate::proto::pmd;
use crate::proto::pmd::server_frame::Payload as ServerPayload;

use super::FrameSink;
use super::contract_metrics::emit_contract_counts;
use super::{from_wire_millis, map_market_type};

/// Maintains the contract reference set for the connector's configured delivery areas and feeds
/// the gRPC `ContractService`. The contract feed is global, so each `Contract` frame is
/// filtered to the configured areas: a contract trading in none of them is dropped, and a kept
/// contract's area-state list is trimmed to the configured areas. The kept batch is translated
/// into domain [`Contract`]s, folded into the shared
/// [`ContractStore`](nordpool_marketdata::ContractStore) under its `Mutex` (which the
/// `SubscribeContracts` stream seeds from), and broadcast over `sender` for live subscribers
/// (who narrow further to their requested areas). Non-`Contract` payloads are ignored. Also
/// emits the per-frame contract gauges.
pub struct ContractSink {
    contracts: SharedContracts,
    /// Broadcast sender the gRPC server subscribes each contract client onto.
    sender: ContractSender,
    /// The configured delivery areas — the global filter over the feed's contracts.
    delivery_area_eics: HashSet<EicCode>,
}

impl ContractSink {
    pub fn new(channel: ContractChannel, delivery_areas: &[EicCode]) -> Self {
        Self {
            contracts: channel.seed,
            sender: channel.updates,
            delivery_area_eics: delivery_areas.iter().copied().collect(),
        }
    }
}

impl FrameSink for ContractSink {
    fn handle(
        &mut self,
        payload: &ServerPayload,
        _bytes: &[u8],
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        if let ServerPayload::Contract(payload) = payload {
            // Keep only contracts trading in at least one configured area. Filtering before
            // conversion avoids cloning names/ids for contracts we'd discard.
            let contracts: Vec<Contract> = payload
                .contracts
                .iter()
                .filter(|c| trades_in_any(c, &self.delivery_area_eics))
                .map(|c| to_domain_contract(c, &self.delivery_area_eics))
                .collect();
            let seq = {
                // Apply under the lock, before the broadcast below, so a frame a new subscriber
                // misses on the delta feed is already in the seed it reads on connect. The lock is
                // held for the whole apply so a concurrent reader (seeding a stream) never observes
                // a torn frame — and the apply carries the frame's receive time and bumps the frame
                // seq, so the store's freshness stamp and seq move atomically with its content.
                // Reads are rare and the set small, so the brief stall is fine. Recover a poisoned
                // lock rather than propagating — a poison only means a reader/writer panicked, the
                // book itself is still consistent.
                let mut store = self.contracts.lock().unwrap_or_else(|e| e.into_inner());
                store.apply(&contracts, payload.is_snapshot, received_at);
                // Refresh the contract-count gauge from the store just updated.
                emit_contract_counts(&store);
                store.seq()
            };
            // A frame that touched no contracts in our areas yields an empty batch — the store
            // still needs the apply above (a snapshot may have cleared it), but an empty delta
            // carries no upserts, so there is nothing to broadcast. Suppress it here rather than
            // on every subscriber's stream.
            if !contracts.is_empty() {
                let _ = self.sender.send(ContractUpdate {
                    received_at,
                    seq,
                    contracts,
                });
            }
        }
        Ok(())
    }
}

/// The wire area state's [`EicCode`], if it is one of the configured `delivery_area_eics`. One
/// resolver for the filter and the translation below: a wire id with no known EIC can never be
/// configured, so it falls out with the out-of-configuration areas.
fn configured_area(
    s: &crate::proto::pmd::ContractDeliveryAreaState,
    delivery_area_eics: &HashSet<EicCode>,
) -> Option<EicCode> {
    EicCode::from_id(s.delivery_area_id).filter(|eic| delivery_area_eics.contains(eic))
}

/// Whether the wire contract trades in at least one of `delivery_area_eics` — the sink's
/// contract-level filter over the global feed. Guarantees every contract handed to
/// [`to_domain_contract`] keeps a non-empty trimmed area list.
fn trades_in_any(c: &crate::proto::pmd::Contract, delivery_area_eics: &HashSet<EicCode>) -> bool {
    c.delivery_area_states
        .iter()
        .any(|s| configured_area(s, delivery_area_eics).is_some())
}

/// Translate one protobuf `Contract` into its domain [`Contract`], keeping only the area states
/// within `delivery_area_eics` — the connector serves those areas and nothing downstream reads
/// the rest. The only place that knows both the wire and domain contract types.
fn to_domain_contract(
    c: &crate::proto::pmd::Contract,
    delivery_area_eics: &HashSet<EicCode>,
) -> Contract {
    Contract {
        contract_id: c.contract_id,
        name: c.contract_name.clone(),
        market_id: c.market_id.clone(),
        market_type: map_market_type(c.market_type),
        product_type: map_product_type(c.product_type),
        delivery_start: from_wire_millis(c.delivery_start_ms),
        delivery_end: from_wire_millis(c.delivery_end_ms),
        duration_seconds: c.duration_seconds,
        updated_at: from_wire_millis(c.updated_at_ms),
        areas: c
            .delivery_area_states
            .iter()
            .filter_map(|s| {
                configured_area(s, delivery_area_eics).map(|eic| ContractAreaState {
                    delivery_area_eic: eic,
                    state: map_contract_state(s.state),
                    phase_start: from_wire_millis(s.open_at_ms),
                    phase_end: from_wire_millis(s.closed_at_ms),
                })
            })
            .collect(),
    }
}

/// Map the wire `product_type` enum value to the domain [`ProductType`]: the periodic products
/// carry over, every block/custom shape collapses to `Other`.
fn map_product_type(raw: i32) -> ProductType {
    match pmd::ProductType::try_from(raw) {
        Ok(pmd::ProductType::P15Min) => ProductType::P15Min,
        Ok(pmd::ProductType::P30Min) => ProductType::P30Min,
        Ok(pmd::ProductType::P60Min) => ProductType::P60Min,
        _ => ProductType::Other,
    }
}

/// Map the wire `state` enum value to the domain [`ContractState`]; the wire's four-letter
/// abbreviations are the only place those spellings appear.
fn map_contract_state(raw: i32) -> ContractState {
    match pmd::ContractState::try_from(raw) {
        Ok(pmd::ContractState::Acti) => ContractState::Active,
        Ok(pmd::ContractState::Hibe) => ContractState::Hibernated,
        Ok(pmd::ContractState::Iact) => ContractState::Inactive,
        Ok(pmd::ContractState::Frzn) => ContractState::Frozen,
        Ok(pmd::ContractState::None) | Err(_) => ContractState::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use nordpool_marketdata::{ContractState, EicCode, ProductType};
    use nordpool_stream_server::ContractChannel;

    use chrono::{DateTime, TimeDelta, Utc};

    use super::ContractSink;
    use crate::proto::pmd::server_frame::Payload as ServerPayload;
    use crate::proto::pmd::{Contract, ContractDeliveryAreaState, ContractPayload};
    use crate::sink::{FrameSink, from_wire_millis, ts};

    /// Broadcast `capacity` for the test channel: more messages than any test publishes, so no
    /// subscriber lags.
    const BROADCAST_CAPACITY: usize = 64;

    fn received_at() -> DateTime<Utc> {
        DateTime::UNIX_EPOCH
    }

    fn area_state(delivery_area_id: i32, state: i32) -> ContractDeliveryAreaState {
        phased_area_state(delivery_area_id, state, 0, 0)
    }

    /// An area state carrying an explicit phase window — the two fields [`area_state`] leaves at
    /// the epoch.
    fn phased_area_state(
        delivery_area_id: i32,
        state: i32,
        open_at_ms: i64,
        closed_at_ms: i64,
    ) -> ContractDeliveryAreaState {
        ContractDeliveryAreaState {
            delivery_area_id,
            state,
            open_at_ms,
            closed_at_ms,
        }
    }

    fn wire_contract(id: i64, areas: Vec<ContractDeliveryAreaState>) -> Contract {
        Contract {
            contract_id: id,
            contract_name: format!("C{id}"),
            market_type: 1,
            market_id: "NP".into(),
            product_type: 3, // PRODUCT_TYPE_P60_MIN
            delivery_start_ms: 1000,
            delivery_end_ms: 2000,
            duration_seconds: 3600,
            resolution_seconds: 0,
            predefined: false,
            updated_at_ms: 42,
            delivery_area_states: areas,
        }
    }

    #[test]
    fn handle_keeps_configured_area_contracts_trimmed_and_drops_the_rest() {
        // Given a sink configured for areas 40/FR and 41/CH, sharing the contract store with
        // (would-be) gRPC readers, plus a contract delta subscriber.
        let channel = ContractChannel::new(BROADCAST_CAPACITY);
        let contracts = channel.seed.clone();
        let mut contract_rx = channel.updates.subscribe();
        let mut sink = ContractSink::new(channel, &[EicCode::AREA_FR, EicCode::AREA_CH]);

        // When a contract snapshot frame is handled: contract 111 active in areas 40 and 41 and
        // frozen in area 99, contract 222 trading only in area 99. The feed is global, so the
        // sink must filter to the configured areas — keep 111 with its area-state list trimmed
        // to 40 and 41, drop 222 — then store and broadcast the kept batch.
        let frame = ServerPayload::Contract(ContractPayload {
            is_snapshot: true,
            contracts: vec![
                wire_contract(
                    111,
                    vec![area_state(40, 1), area_state(41, 1), area_state(99, 4)],
                ),
                wire_contract(222, vec![area_state(99, 1)]),
            ],
        });
        sink.handle(&frame, b"", received_at() + TimeDelta::seconds(5))
            .unwrap();

        // Then the frame's receive time is stamped on the store, so the next connect/lag seed
        // can carry how fresh the set is.
        assert_eq!(
            contracts.lock().unwrap().received_at(),
            Some(ts(5_000_000_000))
        );

        // Then the store holds only the kept contract with only the configured areas' states —
        // the out-of-configuration area 99 state is trimmed away.
        let store = contracts.lock().unwrap();
        let matched: Vec<i64> = store
            .matching(Some(ProductType::P60Min))
            .map(|c| c.contract_id)
            .collect();
        assert_eq!(matched, vec![111]);
        assert!(store.get(222).is_none());
        let c = store.get(111).expect("contract tracked");
        assert_eq!(c.name, "C111");
        assert_eq!(c.duration_seconds, 3600);
        assert_eq!(c.updated_at, from_wire_millis(42));
        assert_eq!(c.state_in(EicCode::AREA_FR), Some(ContractState::Active));
        assert_eq!(c.state_in(EicCode::AREA_CH), Some(ContractState::Active));
        assert_eq!(c.state_in(EicCode::AREA_AT), None);

        // And the same filtered batch was broadcast to the contract delta feed, carrying the
        // frame's seq (the store's first applied frame).
        let batch = contract_rx.try_recv().expect("contract delta broadcast");
        assert_eq!(
            batch
                .contracts
                .iter()
                .map(|c| c.contract_id)
                .collect::<Vec<_>>(),
            vec![111]
        );
        assert_eq!(batch.seq, 1);
    }

    #[test]
    fn handle_carries_each_area_phase_window() {
        // Given a sink configured for areas 40/FR and 41/CH.
        let channel = ContractChannel::new(BROADCAST_CAPACITY);
        let contracts = channel.seed.clone();
        let mut sink = ContractSink::new(channel, &[EicCode::AREA_FR, EicCode::AREA_CH]);

        // When one contract arrives active in FR — a phase running from the trading open to the
        // scheduled gate closure — and inactive in CH, where the phase instead ends when trading
        // is due to open. The two ends carry different meanings per state, so the translation has
        // to pass them through as they stand.
        let frame = ServerPayload::Contract(ContractPayload {
            is_snapshot: true,
            contracts: vec![wire_contract(
                111,
                vec![
                    phased_area_state(40, 1, 1_000, 9_000),
                    phased_area_state(41, 3, 2_000, 8_000),
                ],
            )],
        });
        sink.handle(&frame, b"", received_at()).unwrap();

        // Then each area keeps its own window rather than sharing one, and both ends survive as
        // the domain's DateTime<Utc>.
        let store = contracts.lock().unwrap();
        let c = store.get(111).expect("contract tracked");
        let area = |eic| {
            *c.areas
                .iter()
                .find(|a| a.delivery_area_eic == eic)
                .expect("area state kept")
        };

        let fr = area(EicCode::AREA_FR);
        assert_eq!(fr.state, ContractState::Active);
        assert_eq!(fr.phase_start, from_wire_millis(1_000));
        assert_eq!(fr.phase_end, from_wire_millis(9_000));

        let ch = area(EicCode::AREA_CH);
        assert_eq!(ch.state, ContractState::Inactive);
        assert_eq!(ch.phase_start, from_wire_millis(2_000));
        assert_eq!(ch.phase_end, from_wire_millis(8_000));
    }

    #[test]
    fn handle_suppresses_the_broadcast_when_no_contract_survives_the_filter() {
        // Given a sink configured for area 40/FR with a delta subscriber attached.
        let channel = ContractChannel::new(BROADCAST_CAPACITY);
        let contracts = channel.seed.clone();
        let mut contract_rx = channel.updates.subscribe();
        let mut sink = ContractSink::new(channel, &[EicCode::AREA_FR]);
        // And a store already holding a contract from an earlier snapshot.
        sink.handle(
            &ServerPayload::Contract(ContractPayload {
                is_snapshot: true,
                contracts: vec![wire_contract(111, vec![area_state(40, 1)])],
            }),
            b"",
            received_at(),
        )
        .unwrap();
        contract_rx.try_recv().expect("snapshot batch broadcast");

        // When a later snapshot frame carries only out-of-area contracts.
        sink.handle(
            &ServerPayload::Contract(ContractPayload {
                is_snapshot: true,
                contracts: vec![wire_contract(222, vec![area_state(99, 1)])],
            }),
            b"",
            received_at(),
        )
        .unwrap();

        // Then the snapshot still cleared the store (nothing in our areas remains), but the
        // empty batch was not broadcast — subscribers are not woken for frames that carry
        // nothing for the configured areas.
        assert!(contracts.lock().unwrap().is_empty());
        assert!(contract_rx.try_recv().is_err());
    }
}
