//! The producer→server broadcast channels: depth-book views and contract deltas.
//!
//! A blocking producer broadcasts each changed [`DepthView`] into a `tokio::sync::broadcast`.
//! Each gRPC subscriber gets its own receiver. A parallel channel carries contract-feed deltas.
//!
//! `broadcast::Sender::send` is a non-blocking sync call, so the producer feeds it straight
//! from its blocking thread; no bridge thread or runtime is needed on the producer side.
//!
//! Every message is the full current top-N depth of one book (state, not a delta), so a
//! subscriber that lags and drops messages stays correct as soon as it receives any later
//! update for that book; a fresh subscriber (or a lagged one) is re-synced by seeding from the
//! per-area seed map, not by a periodic re-broadcast.
//!
//! The depth feed is **per delivery area**: a `LocalView` frame is scoped to one area, and each
//! gRPC depth subscriber asks for one area, so each area gets its own seed store and its own
//! broadcast (see [`AreaDepthChannel`]). The set of areas is fixed at startup, so the outer map is
//! immutable behind an `Arc` — looked up by [`EicCode`], never mutated or locked.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use tokio::sync::broadcast;

use nordpool_marketdata::{
    Contract, ContractStore, DepthStore, DepthView, EicCode, PublicStatistic, StatisticStore,
};

use crate::contracts::SharedContracts;
use crate::statistics::SharedStatistics;

/// One inbound frame's changed books for one delivery area, plus our receive time for that frame.
/// The domain twin of the wire `proto::DepthUpdate` (and of [`ContractSender`]'s batch): broadcast
/// as a unit so a subscriber sees the frame's receive timestamp alongside the books that changed
/// in it.
#[derive(Debug, Clone)]
pub struct DepthUpdate {
    /// Our receive timestamp for the frame these books changed in.
    pub received_at: DateTime<Utc>,
    /// The area's frame sequence for the frame these books changed in (see
    /// [`DepthStore::seq`](nordpool_marketdata::DepthStore::seq)) — what lets the depth service
    /// drop a new subscriber's duplicate prefix exactly.
    pub seq: u64,
    /// The books whose visible top-N changed in that frame, each full top-N state (not a delta).
    pub views: Vec<DepthView>,
}

/// Depth-feed sender. Each value is one inbound frame's [`DepthUpdate`] for one delivery area. Held
/// by the producer (to publish) and by the server (which calls
/// [`subscribe`](broadcast::Sender::subscribe) once per client).
pub type DepthSender = broadcast::Sender<DepthUpdate>;

/// Depth-feed receiver, one per subscriber.
pub type DepthReceiver = broadcast::Receiver<DepthUpdate>;

/// One contract frame's changed contracts, plus our receive time for that frame. The domain twin of
/// the wire `proto::ContractUpdate` (and the contract counterpart of [`DepthUpdate`]): broadcast as
/// a unit so a subscriber sees the frame's receive timestamp alongside the contracts it changed.
#[derive(Debug, Clone)]
pub struct ContractUpdate {
    /// Our receive timestamp for the frame these contracts changed in.
    pub received_at: DateTime<Utc>,
    /// The feed's frame sequence for the frame these contracts changed in (see
    /// [`ContractStore::seq`](nordpool_marketdata::ContractStore::seq)) — what lets the contract
    /// service drop a new subscriber's duplicate prefix exactly.
    pub seq: u64,
    /// The contracts a single frame upserted (keyed by id). The *snapshot* on connect comes from
    /// the shared [`ContractStore`](nordpool_marketdata::ContractStore), not this channel — this
    /// carries only the incremental changes after a client is attached.
    pub contracts: Vec<Contract>,
}

/// Sender side of the contract delta feed: each value is one frame's [`ContractUpdate`]. Held by
/// the producer and by the server.
pub type ContractSender = broadcast::Sender<ContractUpdate>;

/// Receiver side of the contract delta feed, one per subscriber.
pub type ContractReceiver = broadcast::Receiver<ContractUpdate>;

/// One statistic frame's changed entries for one delivery area, plus our receive time for that
/// frame. The statistic counterpart of [`DepthUpdate`]: broadcast as a unit on that area's channel
/// so a subscriber sees the frame's receive timestamp alongside the statistics it changed. The
/// *snapshot* on connect comes from the shared [`StatisticStore`](nordpool_marketdata::StatisticStore),
/// not this channel — this carries only the incremental changes after a client is attached.
#[derive(Debug, Clone)]
pub struct StatisticUpdate {
    /// Our receive timestamp for the frame these statistics changed in.
    pub received_at: DateTime<Utc>,
    /// The feed's frame sequence for the frame these statistics changed in (see
    /// [`StatisticStore::seq`](nordpool_marketdata::StatisticStore::seq)) — global like the store,
    /// so one frame's per-area batches share it. What lets the statistic service drop a new
    /// subscriber's duplicate prefix exactly.
    pub seq: u64,
    /// The statistics a single frame upserted for one delivery area (keyed by contract id).
    pub statistics: Vec<PublicStatistic>,
}

/// Sender side of the public-statistic delta feed: each value is one frame's [`StatisticUpdate`].
pub type StatisticSender = broadcast::Sender<StatisticUpdate>;

/// Receiver side of the public-statistic delta feed, one per subscriber.
pub type StatisticReceiver = broadcast::Receiver<StatisticUpdate>;

/// One delivery area's depth channel: its [`DepthStore`] seed (each book's latest top-N view,
/// plus the freshness stamp and frame seq its `apply` moves with them) and the broadcast its
/// changed books are published onto. The per-area twin of [`DepthChannels`] (which indexes one of
/// these per area) — the producer keeps the seed current and broadcasts; the depth gRPC service
/// reads the seed to seed a new/lagged subscriber and subscribes the broadcast for live deltas.
pub struct AreaDepthChannel {
    /// This area's seed store. A plain `Mutex` — no inner `Arc`, because sharing between the
    /// producer (the connector's `LocalViewSink`) and the gRPC service comes from the
    /// [`DepthChannels`] outer `Arc`. Not copy-on-write either: the writer applies one frame per
    /// (brief) critical section and the rare reader clones only this small store of top-N
    /// projections, never the order books. The producer keeps it current; the gRPC service reads
    /// it to **seed** a newly-connected subscriber — and re-seed a lagged one — with full state,
    /// replacing the old periodic sweep.
    pub seed: Mutex<DepthStore>,
    /// Broadcast of this area's changed books, one frame's batch per send.
    pub updates: DepthSender,
}

impl AreaDepthChannel {
    fn new(capacity: usize) -> Self {
        Self {
            seed: Mutex::new(DepthStore::new()),
            updates: broadcast::channel(capacity).0,
        }
    }
}

/// Create the contract delta broadcast channel and return its sender. `capacity` bounds how many
/// contract frames a slow subscriber can fall behind before it sees `Lagged` and re-syncs from
/// the shared store.
pub(crate) fn contract_channel(capacity: usize) -> ContractSender {
    broadcast::channel(capacity).0
}

/// Create the public-statistic delta broadcast channel and return its sender. `capacity` bounds how
/// many statistic frames a slow subscriber can fall behind before it sees `Lagged` and re-syncs.
pub(crate) fn statistic_channel(capacity: usize) -> StatisticSender {
    broadcast::channel(capacity).0
}

/// The depth feed's producer↔server boundary: one [`AreaDepthChannel`] (seed map + broadcast) per
/// served delivery area, in an immutable map keyed by [`EicCode`]. Created once with the
/// full set of areas, then `clone`d to both sides — the connector's `LocalViewSink` (which writes
/// the seed and broadcasts) and the gRPC `DepthFeed` (which reads the seed and subscribes
/// receivers). Cloning is cheap (an `Arc`); both clones share the same per-area maps and channels.
/// The area index is a `BTreeMap` to match the order-book store's area level (the outer map is tiny
/// and looked up by area).
#[derive(Clone)]
pub struct DepthChannels {
    areas: Arc<BTreeMap<EicCode, AreaDepthChannel>>,
}

impl DepthChannels {
    /// Create a fresh set of depth channels serving `delivery_areas`: one [`AreaDepthChannel`]
    /// per area, each with an empty seed map and a broadcast of the given ring-buffer `capacity`
    /// (how many recent updates are retained for a receiver that falls behind before it sees
    /// `Lagged`).
    pub fn new(capacity: usize, delivery_areas: &[EicCode]) -> Self {
        let areas = delivery_areas
            .iter()
            .map(|&area| (area, AreaDepthChannel::new(capacity)))
            .collect();
        Self {
            areas: Arc::new(areas),
        }
    }

    /// The depth state for `delivery_area_eic`, or `None` if this channel doesn't serve that area.
    pub fn area(&self, delivery_area_eic: EicCode) -> Option<&AreaDepthChannel> {
        self.areas.get(&delivery_area_eic)
    }
}

/// The contract feed's producer↔server boundary: the broadcast `updates` sender plus the shared
/// `seed` [`ContractStore`](nordpool_marketdata::ContractStore). Created once and `clone`d to the
/// connector's `ContractSink` (writer) and the gRPC `ContractFeed` (reader); both clones share the
/// same store and channel. The contract feed is not per-area: the store holds whole contracts
/// (each with its per-area state list) spanning all the areas the writer keeps, and each
/// subscriber's view is filtered to its requested areas by the contract service.
#[derive(Clone)]
pub struct ContractChannel {
    pub seed: SharedContracts,
    pub updates: ContractSender,
}

impl ContractChannel {
    /// Create a fresh contract channel: an empty [`ContractStore`](nordpool_marketdata::ContractStore)
    /// and a broadcast channel of the given `capacity`.
    pub fn new(capacity: usize) -> Self {
        Self {
            seed: Arc::new(Mutex::new(ContractStore::new())),
            updates: contract_channel(capacity),
        }
    }
}

/// The public-statistic feed's producer↔server boundary, keyed per delivery area like the depth
/// feed's [`DepthChannels`]: one shared area-keyed [`StatisticStore`](nordpool_marketdata::StatisticStore)
/// (`seed`) read to seed any area's subscriber, plus one broadcast `updates` sender per served area
/// (the area-keyed delta feeds). Created once with the full set of areas, then `clone`d to the
/// connector's `StatisticSink` (writer) and the gRPC `StatisticFeed` (reader); cloning is cheap (the
/// store is one `Arc<Mutex<…>>`, the sender map an `Arc<BTreeMap>`). The set of served areas is
/// exactly the sender map's keys.
#[derive(Clone)]
pub struct StatisticChannels {
    seed: SharedStatistics,
    areas: Arc<BTreeMap<EicCode, StatisticSender>>,
}

impl StatisticChannels {
    /// Create fresh statistic channels serving `delivery_areas`: one empty shared
    /// [`StatisticStore`](nordpool_marketdata::StatisticStore) plus one broadcast of the given
    /// ring-buffer `capacity` per area (how many recent updates are retained for a receiver that
    /// falls behind before it sees `Lagged`).
    pub fn new(capacity: usize, delivery_areas: &[EicCode]) -> Self {
        let areas = delivery_areas
            .iter()
            .map(|&area| (area, statistic_channel(capacity)))
            .collect();
        Self {
            seed: Arc::new(Mutex::new(StatisticStore::new())),
            areas: Arc::new(areas),
        }
    }

    /// The shared area-keyed store (plus its freshness stamp) both sides read/write: the writer
    /// applies frames to it, the reader seeds one area's slice from it.
    pub fn seed(&self) -> &SharedStatistics {
        &self.seed
    }

    /// This area's delta broadcast sender, or `None` if this channel doesn't serve that area.
    pub fn area(&self, delivery_area_eic: EicCode) -> Option<&StatisticSender> {
        self.areas.get(&delivery_area_eic)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ts;
    use nordpool_marketdata::{BookKey, MarketType};

    fn top(contract_id: i64) -> DepthView {
        DepthView {
            key: BookKey {
                market_type: MarketType::Local,
                contract_id,
            },
            revision: 1,
            bids: vec![],
            asks: vec![],
        }
    }

    #[tokio::test]
    async fn every_subscriber_of_an_area_receives_each_update() {
        // Given an area's depth broadcast with two independent subscribers, each holding its own
        // receiver — the shape the gRPC server uses, one receiver per connected client.
        let channel = DepthChannels::new(16, &[EicCode::AREA_FR]);
        let area = channel.area(EicCode::AREA_FR).expect("area FR served");
        let mut a = area.updates.subscribe();
        let mut b = area.updates.subscribe();

        // When the producer broadcasts a frame's batch (here one book, contract 1).
        area.updates
            .send(DepthUpdate {
                received_at: ts(0),
                seq: 1,
                views: vec![top(1)],
            })
            .unwrap();

        // Then every subscriber receives that same batch — the broadcast fans one send out to
        // all receivers, which is what lets each client see every book's top change.
        assert_eq!(a.recv().await.unwrap().views[0].key.contract_id, 1);
        assert_eq!(b.recv().await.unwrap().views[0].key.contract_id, 1);
    }

    #[test]
    fn area_returns_none_for_an_unserved_delivery_area() {
        // Given a channel serving only areas FR and NO1.
        let channel = DepthChannels::new(16, &[EicCode::AREA_FR, EicCode::AREA_NO1]);

        // Then a served area resolves and an unserved one does not — the lookup the depth service
        // uses to reject `Subscribe` requests for areas this connector doesn't carry.
        assert!(channel.area(EicCode::AREA_FR).is_some());
        assert!(channel.area(EicCode::AREA_AT).is_none());
    }
}
