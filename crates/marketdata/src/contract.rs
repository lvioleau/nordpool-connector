//! Contract reference data reconstructed from the `Contract` feed's snapshot + delta
//! semantics — the metadata side of the same domain the order books live in. Pure domain: no
//! protobuf, no IO. The contract feed is global (not per delivery area): the connector keeps
//! the contracts trading in at least one of its configured areas, translates each — trimming
//! its area-state list to the configured areas — and folds it into the single
//! [`ContractStore`]; the stream-server seeds each `SubscribeContracts` stream from the store,
//! filtered to the areas the subscriber asked for.

use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::{EicCode, MarketType};

/// Tradeable product shape. The connector cares only about the 15/30/60-minute periodic
/// products; every other wire `ProductType` (blocks, custom shapes, unspecified) collapses to
/// [`Other`](ProductType::Other), so the rest of the workspace never sees protobuf.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, strum_macros::IntoStaticStr)]
pub enum ProductType {
    P15Min,
    P30Min,
    P60Min,
    Other,
}

impl ProductType {
    /// Stable identifier — the variant name, for callers that need a printable label.
    pub fn as_label(self) -> &'static str {
        self.into()
    }
}

/// A contract's lifecycle state in one delivery area.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContractState {
    Unknown,
    Active,
    Hibernated,
    Inactive,
    Frozen,
}

/// A contract's lifecycle [`ContractState`] in one delivery area, with the validity window of
/// the *phase* that state belongs to — the domain twin of the wire `ContractDeliveryAreaState`.
///
/// The window is **not** the contract's trading window. Nord Pool stamps it per phase: every
/// state transition starts a new phase and rewrites both ends, so the pair describes only the
/// state currently held. `phase_start` is when the contract entered [`state`](Self::state) —
/// for a freshly announced contract that is the announcement instant itself — and `phase_end`
/// is when that phase is due to end, which means different things per state: the scheduled
/// *opening* while the contract is [`Inactive`](ContractState::Inactive) and still pending, the
/// gate closure while it is [`Active`](ContractState::Active). The final inactive phase after
/// gate closure collapses the window to a point, both ends sitting on the closure instant.
///
/// So `phase_end` alone cannot say whether a contract has expired — paired with `state` it can:
/// inactive with a `phase_end` in the past is over, inactive with one in the future has not
/// opened yet. The wire spells these `open_at_ms` / `closed_at_ms`, whose names invite reading
/// them as a trading window; they are renamed here so the phase semantics travel with the type.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ContractAreaState {
    pub delivery_area_eic: EicCode,
    pub state: ContractState,
    /// When the contract entered [`state`](Self::state) in this area.
    pub phase_start: DateTime<Utc>,
    /// When that phase is due to end — see the type docs; not the contract's trading close.
    pub phase_end: DateTime<Utc>,
}

/// A tradeable contract's reference metadata, carrying a per-delivery-area state list: one
/// [`Contract`] holds the contract's state in every area the writer keeps (the connector trims
/// the list to its configured areas before folding it in).
#[derive(Debug, Clone, PartialEq)]
pub struct Contract {
    pub contract_id: i64,
    pub name: String,
    pub market_id: String,
    pub market_type: MarketType,
    pub product_type: ProductType,
    pub delivery_start: DateTime<Utc>,
    pub delivery_end: DateTime<Utc>,
    pub duration_seconds: i32,
    pub updated_at: DateTime<Utc>,
    pub areas: Vec<ContractAreaState>,
}

impl Contract {
    /// Whether the contract trades in `delivery_area_eic`.
    pub fn trades_in(&self, delivery_area_eic: EicCode) -> bool {
        self.areas
            .iter()
            .any(|a| a.delivery_area_eic == delivery_area_eic)
    }

    /// The contract's state in `delivery_area_eic`, or `None` if it doesn't trade there.
    pub fn state_in(&self, delivery_area_eic: EicCode) -> Option<ContractState> {
        self.areas
            .iter()
            .find(|a| a.delivery_area_eic == delivery_area_eic)
            .map(|a| a.state)
    }
}

/// The current set of all known contracts, keyed by id. Maintained from the feed's snapshot +
/// delta semantics: a snapshot replaces the whole set (contracts it omits are gone), a delta
/// upserts by id. Contracts are never removed by a delta — they change [`ContractState`]
/// instead. The contract feed is global, so this is one flat store spanning every delivery
/// area; per-area views are projected by the readers ([`Contract::trades_in`]).
#[derive(Debug, Default)]
pub struct ContractStore {
    contracts: HashMap<i64, Contract>,
    /// Receive time of the last frame applied — `None` until the first. Set by every `apply`,
    /// even one whose batch is empty, because the frame still proves the set current as of its
    /// arrival.
    received_at: Option<DateTime<Utc>>,
    /// Frame sequence: increments with every `apply` — 0 before the first. Frames apply in seq
    /// order, so a batch stamped `seq <=` a snapshot's `seq` is already reflected in it.
    seq: u64,
}

impl ContractStore {
    pub fn new() -> Self {
        Self::default()
    }

    /// Fold one frame's contracts into the store. `is_snapshot` is a frame-level property
    /// shared by the whole batch: a snapshot first clears the set (the frame replaces it
    /// whole), then each contract is upserted by id. `received_at` is the frame's receive time,
    /// kept as the store's freshness stamp; the frame `seq` advances with it.
    pub fn apply(&mut self, contracts: &[Contract], is_snapshot: bool, received_at: DateTime<Utc>) {
        if is_snapshot {
            self.contracts.clear();
        }
        for contract in contracts {
            self.contracts
                .insert(contract.contract_id, contract.clone());
        }
        self.received_at = Some(received_at);
        self.seq += 1;
    }

    /// Receive time of the last frame applied — how fresh the set is; `None` until the first
    /// frame.
    pub fn received_at(&self) -> Option<DateTime<Utc>> {
        self.received_at
    }

    /// Sequence number of the last frame applied — 0 until the first.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    pub fn get(&self, contract_id: i64) -> Option<&Contract> {
        self.contracts.get(&contract_id)
    }

    /// The contract's state in `delivery_area_eic`, or `None` if the contract is unknown or
    /// doesn't trade there.
    pub fn state(&self, contract_id: i64, delivery_area_eic: EicCode) -> Option<ContractState> {
        self.contracts
            .get(&contract_id)
            .and_then(|c| c.state_in(delivery_area_eic))
    }

    pub fn len(&self) -> usize {
        self.contracts.len()
    }

    pub fn is_empty(&self) -> bool {
        self.contracts.is_empty()
    }

    pub fn iter(&self) -> impl Iterator<Item = &Contract> {
        self.contracts.values()
    }

    /// Contracts matching the product filter; `None` matches everything. Iteration order is
    /// unspecified (backed by a `HashMap`).
    pub fn matching(&self, product_type: Option<ProductType>) -> impl Iterator<Item = &Contract> {
        self.contracts
            .values()
            .filter(move |c| product_type.is_none_or(|p| c.product_type == p))
    }
}

#[cfg(test)]
mod tests {
    use crate::*;

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
    fn snapshot_replaces_the_set_and_delta_upserts_by_id() {
        // Given a store seeded by a snapshot of two contracts, received at t=10.
        let mut store = ContractStore::new();
        store.apply(
            &[
                contract(
                    1,
                    ProductType::P60Min,
                    &[(EicCode::AREA_FR, ContractState::Active)],
                ),
                contract(
                    2,
                    ProductType::P60Min,
                    &[(EicCode::AREA_FR, ContractState::Active)],
                ),
            ],
            true,
            ts(10),
        );

        // When a delta received at t=20 updates contract 1 and adds contract 3.
        store.apply(
            &[
                contract(
                    1,
                    ProductType::P15Min,
                    &[(EicCode::AREA_FR, ContractState::Frozen)],
                ),
                contract(
                    3,
                    ProductType::P15Min,
                    &[(EicCode::AREA_FR, ContractState::Active)],
                ),
            ],
            false,
            ts(20),
        );

        // Then contract 1 is replaced in place, the untouched contract 2 survives, 3 is new, and
        // the freshness stamp advanced to the delta's receive time.
        assert_eq!(store.len(), 3);
        assert_eq!(store.get(1).unwrap().product_type, ProductType::P15Min);
        assert_eq!(
            store.state(1, EicCode::AREA_FR),
            Some(ContractState::Frozen)
        );
        assert!(store.get(2).is_some());
        assert_eq!(store.received_at(), Some(ts(20)));
        assert_eq!(store.seq(), 2);

        // When a later snapshot, received at t=30, mentions only contract 1.
        store.apply(
            &[contract(
                1,
                ProductType::P60Min,
                &[(EicCode::AREA_FR, ContractState::Active)],
            )],
            true,
            ts(30),
        );

        // Then the stale contracts are gone — a snapshot replaces the whole set — and the stamp
        // follows the frame.
        assert_eq!(store.len(), 1);
        assert!(store.get(2).is_none());
        assert_eq!(store.received_at(), Some(ts(30)));
    }

    #[test]
    fn state_is_per_area_and_none_for_unknown_contract_or_area() {
        // Given one contract trading in two areas with different states there.
        let mut store = ContractStore::new();
        store.apply(
            &[contract(
                1,
                ProductType::P60Min,
                &[
                    (EicCode::AREA_FR, ContractState::Frozen),
                    (EicCode::AREA_CH, ContractState::Active),
                ],
            )],
            true,
            ts(0),
        );

        // Then state resolves per area, and both an unknown contract and an area the contract
        // doesn't trade in yield None.
        assert_eq!(
            store.state(1, EicCode::AREA_FR),
            Some(ContractState::Frozen)
        );
        assert_eq!(
            store.state(1, EicCode::AREA_CH),
            Some(ContractState::Active)
        );
        assert_eq!(store.state(1, EicCode::AREA_AT), None);
        assert_eq!(store.state(999, EicCode::AREA_FR), None);
    }

    #[test]
    fn trades_in_reflects_the_area_state_list() {
        let c = contract(
            1,
            ProductType::P60Min,
            &[
                (EicCode::AREA_FR, ContractState::Active),
                (EicCode::AREA_CH, ContractState::Inactive),
            ],
        );

        assert!(c.trades_in(EicCode::AREA_FR));
        assert!(c.trades_in(EicCode::AREA_CH));
        assert!(!c.trades_in(EicCode::AREA_AT));
    }

    #[test]
    fn matching_filters_by_product() {
        // Given a store of three contracts spanning two product shapes: two P60Min (ids 1, 3)
        // and one P15Min (id 2), so a product filter has both a matching and a non-matching set.
        let mut store = ContractStore::new();
        store.apply(
            &[
                contract(
                    1,
                    ProductType::P60Min,
                    &[(EicCode::AREA_FR, ContractState::Active)],
                ),
                contract(
                    2,
                    ProductType::P15Min,
                    &[(EicCode::AREA_FR, ContractState::Active)],
                ),
                contract(
                    3,
                    ProductType::P60Min,
                    &[(EicCode::AREA_FR, ContractState::Active)],
                ),
            ],
            true,
            ts(0),
        );

        // When iterating with a `Some(P60Min)` filter — the product-scoped view the stream-server
        // could use to narrow a feed (ids sorted only because HashMap iteration order is unspecified).
        let mut ids: Vec<i64> = store
            .matching(Some(ProductType::P60Min))
            .map(|c| c.contract_id)
            .collect();
        ids.sort();

        // Then only the two P60Min contracts come through, and a `None` filter (the wildcard)
        // returns all three — confirming the filter selects, not just truncates.
        assert_eq!(ids, vec![1, 3]);
        assert_eq!(store.matching(None).count(), 3);
    }
}
