use anyhow::Result;
use nordpool_marketdata::EicCode;

use crate::proto::pmd::client_frame::Payload as ClientPayload;
use crate::proto::pmd::subscribe::Target as SubscribeTarget;
use crate::proto::pmd::{
    AtcCapacitiesSubscription, CapacityFlow, ContractSubscription, DeliveryAreaSubscription,
    H2hCapacitySubscription, LocalViewSubscription, PublicStatisticSubscription, Subscribe,
    TickerSubscription,
};
use crate::transport::Transport;

use super::send;

/// Issue every subscription the connector cares about for our `delivery_areas`. Area-scoped
/// subscriptions (LocalView, ATC, H2H, PublicStatistic) carry the whole set; global ones
/// (DeliveryAreas, Contracts, Ticker) ignore it. Capacity uses one wildcard `CapacityFlow` per
/// area (empty `connected_delivery_areas`). This is the EIC→id boundary: the domain speaks
/// [`EicCode`], the PMD wire speaks numeric ids, and the conversion happens here, once.
pub fn subscribe_all<T: Transport>(transport: &mut T, delivery_areas: &[EicCode]) -> Result<()> {
    let ids: Vec<i32> = delivery_areas.iter().map(|area| area.id()).collect();
    subscribe_to_localview(transport, ids.clone())?;
    subscribe_to_delivery_areas(transport)?;
    subscribe_to_contracts(transport)?;
    subscribe_to_ticker(transport)?;

    let capacity_flows = ids
        .iter()
        .map(|&delivery_area| CapacityFlow {
            delivery_area,
            connected_delivery_areas: vec![],
        })
        .collect::<Vec<_>>();
    subscribe_to_atc_capacity(transport, capacity_flows.clone())?;
    subscribe_to_h2h_capacity(transport, capacity_flows)?;

    subscribe_to_public_statistics(transport, ids)?;
    Ok(())
}

/// Subscribe to LocalView (per-area order book aggregates) for the given delivery
/// areas. The server sends a snapshot per area on subscription, then deltas.
pub fn subscribe_to_localview<T: Transport>(
    transport: &mut T,
    delivery_areas: Vec<i32>,
) -> Result<()> {
    tracing::info!(?delivery_areas, "subscribing to LocalView");
    send(
        transport,
        ClientPayload::Subscribe(Subscribe {
            target: Some(SubscribeTarget::LocalView(LocalViewSubscription {
                delivery_areas,
            })),
        }),
    )
}

/// Subscribe to delivery areas. The server always pushes the complete snapshot, so
/// there's no per-area filter and `DeliveryAreaSubscription` carries no fields.
pub fn subscribe_to_delivery_areas<T: Transport>(transport: &mut T) -> Result<()> {
    tracing::info!("subscribing to DeliveryAreas");
    send(
        transport,
        ClientPayload::Subscribe(Subscribe {
            target: Some(SubscribeTarget::DeliveryArea(DeliveryAreaSubscription {})),
        }),
    )
}

/// Subscribe to contracts. Global scope (all areas, all markets); first message is a
/// snapshot, subsequent ones are deltas. `ContractSubscription` carries no fields.
pub fn subscribe_to_contracts<T: Transport>(transport: &mut T) -> Result<()> {
    tracing::info!("subscribing to Contracts");
    send(
        transport,
        ClientPayload::Subscribe(Subscribe {
            target: Some(SubscribeTarget::Contract(ContractSubscription {})),
        }),
    )
}

/// Subscribe to the public-trades ticker. Global scope (all markets, all areas); the
/// first message is a snapshot of the 2000 most recent trades, subsequent ones are
/// deltas. `TickerSubscription` carries no fields.
pub fn subscribe_to_ticker<T: Transport>(transport: &mut T) -> Result<()> {
    tracing::info!("subscribing to Ticker");
    send(
        transport,
        ClientPayload::Subscribe(Subscribe {
            target: Some(SubscribeTarget::Ticker(TickerSubscription {})),
        }),
    )
}

/// Subscribe to ATC (available transfer capacity) flows. Each `CapacityFlow` names a
/// primary delivery area plus an optional list of connected areas to filter — an empty
/// `connected_delivery_areas` is a wildcard meaning "all flows involving this area".
/// Subscription is bidirectional per pair.
pub fn subscribe_to_atc_capacity<T: Transport>(
    transport: &mut T,
    flows: Vec<CapacityFlow>,
) -> Result<()> {
    tracing::info!(?flows, "subscribing to AtcCapacity");
    send(
        transport,
        ClientPayload::Subscribe(Subscribe {
            target: Some(SubscribeTarget::AtcCapacity(AtcCapacitiesSubscription {
                flows,
            })),
        }),
    )
}

/// Subscribe to H2H (hub-to-hub) capacity flows. Same `CapacityFlow` semantics as
/// `subscribe_to_atc_capacity` — empty `connected_delivery_areas` means wildcard,
/// flows are bidirectional per pair.
pub fn subscribe_to_h2h_capacity<T: Transport>(
    transport: &mut T,
    flows: Vec<CapacityFlow>,
) -> Result<()> {
    tracing::info!(?flows, "subscribing to H2HCapacity");
    send(
        transport,
        ClientPayload::Subscribe(Subscribe {
            target: Some(SubscribeTarget::H2hCapacity(H2hCapacitySubscription {
                flows,
            })),
        }),
    )
}

/// Subscribe to public statistics for the given delivery areas (last price, volume,
/// VWAP, etc., per contract). Server delivers in conflated mode — periodic snapshots
/// rather than every tick.
pub fn subscribe_to_public_statistics<T: Transport>(
    transport: &mut T,
    delivery_areas: Vec<i32>,
) -> Result<()> {
    tracing::info!(?delivery_areas, "subscribing to PublicStatistic");
    send(
        transport,
        ClientPayload::Subscribe(Subscribe {
            target: Some(SubscribeTarget::PublicStatistic(
                PublicStatisticSubscription { delivery_areas },
            )),
        }),
    )
}
