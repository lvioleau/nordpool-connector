//! gRPC `ContractService`: streams contract reference data filtered by delivery area.
//!
//! The shared [`ContractStore`] holds whole contracts (each with its per-area state list)
//! for all the areas the connector keeps; a subscriber names a set of delivery areas and
//! receives only the contracts trading in at least one of them. On connect a client receives
//! its filtered current set as the first message, then each contract frame's changed batch,
//! filtered the same way; live deltas after the seed arrive over a `broadcast`. A lagged
//! subscriber (one that can't keep up with the broadcast) has its stream dropped and reconnects
//! to re-seed. Composed into the server by [`serve`](crate::serve) alongside the
//! `DepthBookService`.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use tokio::sync::watch;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use nordpool_marketdata::{
    Contract as DomainContract, ContractState as DomainContractState, ContractStore, EicCode,
    ProductType as DomainProductType,
};

use crate::feed::ContractChannel;
use crate::proto::contract_service_server::{ContractService, ContractServiceServer};
use crate::proto::{self, SubscribeContractsRequest};
use crate::subscriber::SubscriberLimit;
use crate::{to_market_type, to_timestamp};

/// The contract store, shared between the connector sink (writer) and the gRPC
/// [`ContractService`] (reader). A plain `Mutex`, deliberately: the writer applies each
/// (infrequent) contract frame under the lock, and the reader builds the connect/lag seed under
/// the lock. A read needs both a rare subscribe/lag and a contract frame to coincide, and the
/// set is small, so the brief stall a colliding read puts on the writer is acceptable — not
/// worth copy-on-write. The store carries its own freshness stamp
/// ([`ContractStore::received_at`]), updated by the same `apply` that changes the set, so
/// a seed read can never observe a stamp out of step with the content. Live deltas after the
/// seed travel over a separate [`ContractSender`].
pub type SharedContracts = Arc<Mutex<ContractStore>>;

/// Build the configured `ContractService` server: the streaming `SubscribeContracts` over the
/// contract `channel` (shared store seed + delta feed), capped at `max_subscribers`.
pub(crate) fn contract_service(
    channel: ContractChannel,
    max_subscribers: usize,
    shutdown: watch::Receiver<bool>,
) -> ContractServiceServer<ContractFeed> {
    ContractServiceServer::new(ContractFeed::new(channel, max_subscribers, shutdown))
}

/// Streams contract reference data for the subscriber's delivery areas: the filtered current
/// set as the first message, then each contract frame's changed batch filtered the same way.
/// Seeds each stream from the shared [`ContractStore`], then forwards the delta feed; on lag it
/// drops the stream (the client reconnects).
pub(crate) struct ContractFeed {
    /// The producer↔server boundary: the shared store seeded on connect and the delta broadcast
    /// subscribers attach to — a clone of the same channel the connector's `ContractSink` writes.
    channel: ContractChannel,
    /// Bounds concurrent contract subscribers, mirroring the depth feed.
    limit: SubscriberLimit,
    /// The process shutdown watch: every `SubscribeContracts` stream cuts itself when it fires,
    /// which is what lets the server's graceful drain complete (see [`serve`](crate::serve)).
    shutdown: watch::Receiver<bool>,
}

impl ContractFeed {
    fn new(
        channel: ContractChannel,
        max_subscribers: usize,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            channel,
            limit: SubscriberLimit::new(
                max_subscribers,
                "nordpool.contracts.subscribers",
                "contract subscriber limit reached",
            ),
            shutdown,
        }
    }
}

type ContractStream = Pin<Box<dyn Stream<Item = Result<proto::ContractUpdate, Status>> + Send>>;

#[tonic::async_trait]
impl ContractService for ContractFeed {
    type SubscribeContractsStream = ContractStream;

    #[allow(clippy::result_large_err)]
    async fn subscribe_contracts(
        &self,
        request: Request<SubscribeContractsRequest>,
    ) -> Result<Response<Self::SubscribeContractsStream>, Status> {
        // The requested areas are a filter over the global set: only contracts trading in at
        // least one of them are streamed. An empty set would be an always-empty stream — reject
        // it as a malformed request rather than serving silence — and so would a set of EICs we
        // can't resolve at all, so an unknown EIC string is a malformed request too (a *known*
        // area this connector isn't configured for is fine: it just never matches).
        let requested = request.into_inner().eic_codes;
        tracing::info!(delivery_areas = ?requested, "contracts Subscribe");
        if requested.is_empty() {
            return Err(Status::invalid_argument(
                "at least one delivery area EIC code is required",
            ));
        }
        let areas: HashSet<EicCode> = requested
            .iter()
            .map(|eic| {
                EicCode::from_eic(eic)
                    .ok_or_else(|| Status::invalid_argument(format!("unknown EIC code {eic:?}")))
            })
            .collect::<Result<_, _>>()?;

        let guard = self.limit.acquire()?;

        // Attach to the delta feed BEFORE reading the seed: any frame applied to the store after our
        // seed is then guaranteed to be waiting in `rx`, so the seed + the deltas cover every frame
        // with no gap.
        let rx = self.channel.updates.subscribe();
        let seed = seed(&self.channel.seed, &areas);
        let seed_seq = seed.seq;

        let deltas = BroadcastStream::new(rx).filter_map(move |result| {
            // The guard lives as long as this closure (and so the stream), releasing the permit
            // and decrementing the gauge when the client disconnects.
            let _ = &guard;
            match result {
                Ok(batch) => {
                    // The subscribe/seed overlap can queue frames the seed already reflects.
                    // Frames apply in seq order, so `seq <= seed.seq` identifies them exactly —
                    // drop that duplicate prefix rather than have the client transiently regress
                    // to older state. Everything past the seed's seq is strictly newer.
                    if (batch.seq as i64) <= seed_seq {
                        return None;
                    }
                    let contracts: Vec<proto::Contract> = batch
                        .contracts
                        .iter()
                        .filter(|c| trades_in_any(c, &areas))
                        .map(to_contract)
                        .collect();
                    // A batch that touched no contract in the subscriber's areas carries nothing
                    // for it — suppress the empty message rather than waking the client.
                    if contracts.is_empty() {
                        return None;
                    }
                    Some(Ok(proto::ContractUpdate {
                        received_at: Some(to_timestamp(batch.received_at)),
                        seq: batch.seq as i64,
                        contracts,
                    }))
                }
                Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                    // A lag means the client can't keep up; re-sending the full set would only push
                    // it more data it can't drain. End the stream with a terminal status (tonic
                    // stops at the first Err) and free the slot — a live client reconnects.
                    tracing::warn!(skipped, "contract subscriber lagged; dropping");
                    metrics::counter!("nordpool.contracts.lagged").increment(1);
                    Some(Err(Status::resource_exhausted("subscriber lagged")))
                }
            }
        });

        // Cut the stream when a shutdown is requested — subscriptions are infinite, so ending
        // them here is what lets the server's graceful drain complete. wait_for (not changed)
        // so a stream whose receiver was cloned after the request still ends; a closed watch
        // means the process is exiting anyway, so it cuts too. The chained terminal status
        // keeps the client contract uniform: any terminal error ⇒ reconnect and re-seed.
        let mut shutdown = self.shutdown.clone();
        let stream = futures_util::StreamExt::take_until(
            tokio_stream::once(Ok(seed)).chain(deltas),
            async move {
                let _ = shutdown.wait_for(|&requested| requested).await;
            },
        )
        .chain(tokio_stream::once(Err(Status::unavailable(
            "server shutting down",
        ))));
        Ok(Response::new(Box::pin(stream)))
    }
}

/// Whether the contract trades in at least one of `areas` — the subscription filter, applied to
/// the seed and to every delta batch. Selects whole contracts: a match streams the contract
/// with its whole stored area-state list, not just the requested areas' entries.
fn trades_in_any(contract: &DomainContract, areas: &HashSet<EicCode>) -> bool {
    contract
        .areas
        .iter()
        .any(|a| areas.contains(&a.delivery_area_eic))
}

/// Build the connect/lag seed: a `proto::ContractUpdate` with the current set filtered to the
/// subscriber's `areas`. Only the filter + clone happen under the lock — the wire conversion (the
/// expensive half) runs after it is released, since the writer's `apply` shares this lock on the
/// driver's receive loop. The store's `apply` updates set and freshness stamp together, so the
/// seed is never torn and its `received_at` — the store's last applied frame time — always
/// agrees with its content.
fn seed(contracts: &SharedContracts, areas: &HashSet<EicCode>) -> proto::ContractUpdate {
    // A poisoned lock means a reader/writer panicked while holding it; the data is still
    // consistent, so recover the guard rather than propagating.
    let (matching, received_at, seq): (Vec<DomainContract>, Option<DateTime<Utc>>, u64) = {
        let store = contracts.lock().unwrap_or_else(|e| e.into_inner());
        (
            store
                .iter()
                .filter(|c| trades_in_any(c, areas))
                .cloned()
                .collect(),
            store.received_at(),
            store.seq(),
        )
    };
    proto::ContractUpdate {
        received_at: received_at.map(to_timestamp),
        seq: seq as i64,
        contracts: matching.iter().map(to_contract).collect(),
    }
}

fn to_contract(c: &DomainContract) -> proto::Contract {
    proto::Contract {
        contract_id: c.contract_id,
        name: c.name.clone(),
        market_id: c.market_id.clone(),
        market_type: to_market_type(c.market_type) as i32,
        product_type: to_product_type(c.product_type) as i32,
        delivery_start: Some(to_timestamp(c.delivery_start)),
        delivery_end: Some(to_timestamp(c.delivery_end)),
        duration_seconds: c.duration_seconds,
        updated_at: Some(to_timestamp(c.updated_at)),
        delivery_areas: c
            .areas
            .iter()
            .map(|a| proto::ContractDeliveryArea {
                eic_code: a.delivery_area_eic.eic_code().to_string(),
                state: to_contract_state(a.state) as i32,
                phase_start: Some(to_timestamp(a.phase_start)),
                phase_end: Some(to_timestamp(a.phase_end)),
            })
            .collect(),
    }
}

fn to_product_type(product_type: DomainProductType) -> proto::ProductType {
    match product_type {
        DomainProductType::P15Min => proto::ProductType::P15Min,
        DomainProductType::P30Min => proto::ProductType::P30Min,
        DomainProductType::P60Min => proto::ProductType::P60Min,
        DomainProductType::Other => proto::ProductType::Other,
    }
}

fn to_contract_state(state: DomainContractState) -> proto::ContractState {
    match state {
        DomainContractState::Unknown => proto::ContractState::None,
        DomainContractState::Active => proto::ContractState::Active,
        DomainContractState::Hibernated => proto::ContractState::Hibernated,
        DomainContractState::Inactive => proto::ContractState::Inactive,
        DomainContractState::Frozen => proto::ContractState::Frozen,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::ContractUpdate;
    use crate::ts;
    use nordpool_marketdata::{
        Contract, ContractAreaState, ContractState, MarketType, ProductType,
    };

    fn contract(id: i64, product: ProductType, areas: &[EicCode]) -> Contract {
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
                .map(|&delivery_area_eic| ContractAreaState {
                    delivery_area_eic,
                    state: ContractState::Active,
                    phase_start: ts(0),
                    phase_end: ts(0),
                })
                .collect(),
        }
    }

    fn shared(store: ContractStore) -> SharedContracts {
        Arc::new(Mutex::new(store))
    }

    /// A shutdown receiver that never fires: the sender is leaked to keep the watch open — a
    /// closed watch reads as shutdown and would cut every stream.
    fn no_shutdown() -> watch::Receiver<bool> {
        let (tx, rx) = watch::channel(false);
        std::mem::forget(tx);
        rx
    }

    fn channel(store: ContractStore) -> ContractChannel {
        ContractChannel {
            seed: shared(store),
            updates: crate::feed::contract_channel(16),
        }
    }

    /// Contracts spanning three areas: 1 and 3 trade in FR, 2 in CH, 4 only in AT.
    fn seeded_store() -> ContractStore {
        let mut store = ContractStore::new();
        store.apply(
            &[
                contract(1, ProductType::P60Min, &[EicCode::AREA_FR]),
                contract(2, ProductType::P15Min, &[EicCode::AREA_CH]),
                contract(
                    3,
                    ProductType::P60Min,
                    &[EicCode::AREA_FR, EicCode::AREA_CH],
                ),
                contract(4, ProductType::P60Min, &[EicCode::AREA_AT]),
            ],
            true,
            ts(0),
        );
        store
    }

    fn areas(list: &[EicCode]) -> HashSet<EicCode> {
        list.iter().copied().collect()
    }

    #[test]
    fn seed_filters_the_set_to_the_requested_areas() {
        // Given a global contract store spanning areas FR, CH and AT.
        let contracts = shared(seeded_store());

        // When building the connect/lag seed for a subscriber asking for areas FR and CH, after
        // an (empty) delta frame received at t=777 refreshed the store's stamp and seq.
        contracts.lock().unwrap().apply(&[], false, ts(777));
        let update = seed(&contracts, &areas(&[EicCode::AREA_FR, EicCode::AREA_CH]));

        // Then the seed carries that stamp and the last applied frame's seq — how fresh the
        // seeded set is, and where the delta stream picks up.
        assert_eq!(update.received_at, Some(to_timestamp(ts(777))));
        assert_eq!(update.seq, 2);
        let got = update.contracts;

        // Then only the contracts trading in at least one requested area come through (4 trades
        // only in AT and is dropped), converted to wire form with the full area-state list kept —
        // contract 3 carries both its areas even though a single-area request would also match it.
        let mut ids: Vec<i64> = got.iter().map(|c| c.contract_id).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);
        let c3 = got.iter().find(|c| c.contract_id == 3).unwrap();
        let mut c3_areas: Vec<&str> = c3
            .delivery_areas
            .iter()
            .map(|a| a.eic_code.as_str())
            .collect();
        c3_areas.sort();
        assert_eq!(
            c3_areas,
            vec![EicCode::AREA_CH.eic_code(), EicCode::AREA_FR.eic_code()]
        );
        assert_eq!(
            c3.delivery_areas[0].state,
            proto::ContractState::Active as i32
        );
    }

    #[test]
    fn seed_carries_each_area_phase_window() {
        // Given one contract active in FR but inactive and still pending in CH, so its two phase
        // windows mean different things — the FR one ends at the gate closure, the CH one at the
        // scheduled opening — and differ end for end.
        let mut c = contract(
            9,
            ProductType::P60Min,
            &[EicCode::AREA_FR, EicCode::AREA_CH],
        );
        for area in &mut c.areas {
            if area.delivery_area_eic == EicCode::AREA_CH {
                area.state = ContractState::Inactive;
                area.phase_start = ts(200);
                area.phase_end = ts(800);
            } else {
                area.phase_start = ts(100);
                area.phase_end = ts(900);
            }
        }
        let mut store = ContractStore::new();
        store.apply(&[c], true, ts(0));

        // When the seed is built for a subscriber asking for both areas.
        let update = seed(
            &shared(store),
            &areas(&[EicCode::AREA_FR, EicCode::AREA_CH]),
        );

        // Then each area carries its own window on the wire, paired with its own state — the two
        // together being what lets a client tell an expired contract from one yet to open.
        let got = &update.contracts[0];
        let area = |eic: EicCode| {
            got.delivery_areas
                .iter()
                .find(|a| a.eic_code == eic.eic_code())
                .expect("area state on the wire")
        };

        let fr = area(EicCode::AREA_FR);
        assert_eq!(fr.state, proto::ContractState::Active as i32);
        assert_eq!(fr.phase_start, Some(to_timestamp(ts(100))));
        assert_eq!(fr.phase_end, Some(to_timestamp(ts(900))));

        let ch = area(EicCode::AREA_CH);
        assert_eq!(ch.state, proto::ContractState::Inactive as i32);
        assert_eq!(ch.phase_start, Some(to_timestamp(ts(200))));
        assert_eq!(ch.phase_end, Some(to_timestamp(ts(800))));
    }

    #[tokio::test]
    async fn subscribe_contracts_seeds_filtered_set_then_streams_filtered_deltas() {
        // Given a global store spanning areas FR, CH and AT plus the broadcast delta feed they
        // share — the full producer/server wiring a real subscribe runs against.
        let channel = channel(seeded_store());
        let tx = channel.updates.clone();
        let feed = ContractFeed::new(channel, 8, no_shutdown());

        // When a client subscribes to area FR only.
        let response = feed
            .subscribe_contracts(Request::new(SubscribeContractsRequest {
                eic_codes: vec![EicCode::AREA_FR.to_string()],
            }))
            .await
            .expect("subscribe accepted");
        let mut stream = response.into_inner();

        // Then the first message is the current set filtered to FR (ids 1 and 3), so the
        // client begins with complete reference data for its areas before any deltas.
        let seed = stream.next().await.unwrap().unwrap();
        let mut ids: Vec<i64> = seed.contracts.iter().map(|c| c.contract_id).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 3]);

        // And a delta batch is filtered the same way: a batch touching only area AT (contract 5)
        // is suppressed entirely, while a later batch with an FR contract (6) is forwarded
        // without the out-of-area contract 7. (The seeded store's seq is 1, so the deltas carry
        // the frames that followed it.)
        tx.send(ContractUpdate {
            received_at: ts(0),
            seq: 2,
            contracts: vec![contract(5, ProductType::P60Min, &[EicCode::AREA_AT])],
        })
        .unwrap();
        tx.send(ContractUpdate {
            received_at: ts(0),
            seq: 3,
            contracts: vec![
                contract(
                    6,
                    ProductType::P60Min,
                    &[EicCode::AREA_FR, EicCode::AREA_AT],
                ),
                contract(7, ProductType::P60Min, &[EicCode::AREA_CH]),
            ],
        })
        .unwrap();
        let delta = stream.next().await.unwrap().unwrap();
        assert_eq!(
            delta
                .contracts
                .iter()
                .map(|c| c.contract_id)
                .collect::<Vec<_>>(),
            vec![6]
        );
        assert_eq!(delta.seq, 3);
    }

    #[tokio::test]
    async fn subscribe_contracts_drops_deltas_the_seed_already_reflects() {
        // Given a store whose seed reflects one applied frame (seq 1) — the state a subscriber's
        // seed message is built from.
        let channel = channel(seeded_store());
        let tx = channel.updates.clone();
        let feed = ContractFeed::new(channel, 8, no_shutdown());

        // When a client subscribes and the queue holds a frame the seed already covers (seq 1 —
        // the subscribe/seed overlap) followed by a genuinely new one (seq 2), both in its areas.
        let mut stream = feed
            .subscribe_contracts(Request::new(SubscribeContractsRequest {
                eic_codes: vec![EicCode::AREA_FR.to_string()],
            }))
            .await
            .expect("subscribe accepted")
            .into_inner();
        tx.send(ContractUpdate {
            received_at: ts(0),
            seq: 1,
            contracts: vec![contract(1, ProductType::P60Min, &[EicCode::AREA_FR])],
        })
        .unwrap();
        tx.send(ContractUpdate {
            received_at: ts(0),
            seq: 2,
            contracts: vec![contract(8, ProductType::P60Min, &[EicCode::AREA_FR])],
        })
        .unwrap();

        // Then after the seed (seq 1) the duplicate seq-1 batch is dropped and the first delta
        // delivered is seq 2 — the stream is strictly newer than the seed.
        let seed = stream.next().await.unwrap().unwrap();
        assert_eq!(seed.seq, 1);
        let delta = stream.next().await.unwrap().unwrap();
        assert_eq!(delta.seq, 2);
        assert_eq!(delta.contracts[0].contract_id, 8);
    }

    #[tokio::test]
    async fn subscribe_contracts_rejects_an_empty_area_set() {
        // Given a feed over a populated global store — the request's areas are the filter, so an
        // empty set could only ever produce an empty stream.
        let feed = ContractFeed::new(channel(seeded_store()), 8, no_shutdown());

        // When subscribing with no delivery areas, the request is rejected INVALID_ARGUMENT
        // rather than accepted as a stream that would never carry a contract.
        let err = feed
            .subscribe_contracts(Request::new(SubscribeContractsRequest {
                eic_codes: vec![],
            }))
            .await
            .err()
            .expect("empty area set rejected");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn subscribe_contracts_ends_the_stream_with_unavailable_on_shutdown() {
        // Given a client already subscribed and seeded — a stream that would otherwise be
        // infinite.
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let feed = ContractFeed::new(channel(seeded_store()), 8, shutdown_rx);
        let mut stream = feed
            .subscribe_contracts(Request::new(SubscribeContractsRequest {
                eic_codes: vec![EicCode::AREA_FR.to_string()],
            }))
            .await
            .expect("subscribe accepted")
            .into_inner();
        assert!(stream.next().await.unwrap().is_ok()); // the seed

        // When a shutdown is requested.
        shutdown_tx.send(true).unwrap();

        // Then the stream ends with a terminal UNAVAILABLE — not a clean OK end, so the client's
        // one rule (terminal status ⇒ reconnect and re-seed) covers shutdown too — and nothing
        // follows it.
        let end = stream.next().await.unwrap();
        assert_eq!(
            end.expect_err("terminal status").code(),
            tonic::Code::Unavailable
        );
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn subscribe_contracts_enforces_the_max_subscriber_cap() {
        // Given a contract feed whose subscriber semaphore has a single permit, so only one client
        // may stream contract data at a time — the same concurrency cap the depth feed enforces.
        let feed = ContractFeed::new(channel(ContractStore::new()), 1, no_shutdown());
        let request = || {
            Request::new(SubscribeContractsRequest {
                eic_codes: vec![EicCode::AREA_FR.to_string()],
            })
        };

        // When the first client subscribes, it acquires the sole permit and is accepted.
        let first = feed.subscribe_contracts(request()).await;
        assert!(first.is_ok());

        // Then a second subscribe with no permit left is rejected with RESOURCE_EXHAUSTED, and once
        // the first stream is dropped its permit is released, so a later subscribe succeeds again.
        let second = feed.subscribe_contracts(request()).await;
        assert_eq!(
            second.err().expect("second subscribe rejected").code(),
            tonic::Code::ResourceExhausted
        );
        drop(first);
        assert!(feed.subscribe_contracts(request()).await.is_ok());
    }
}
