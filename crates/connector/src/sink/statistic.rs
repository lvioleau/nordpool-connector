use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use nordpool_marketdata::{EicCode, PublicStatistic, Tendency};
use nordpool_stream_server::{StatisticChannels, StatisticUpdate};

use crate::proto::pmd;
use crate::proto::pmd::server_frame::Payload as ServerPayload;

use super::statistic_metrics::emit_statistic_counts;
use super::{FrameSink, from_wire_millis, map_market_type};

/// Maintains public-statistic reference data for the connector's subscribed delivery areas and feeds
/// the gRPC `PublicStatisticService`. Each `PublicStatistic` frame is translated into domain
/// [`PublicStatistic`]s (keeping only statistics for served areas), folded into the shared
/// area-keyed [`StatisticStore`](nordpool_marketdata::StatisticStore) under its `Mutex` (which the
/// `SubscribeStatistics` stream seeds one area's slice from), then each changed statistic is
/// broadcast on **its own area's** channel for live subscribers. Non-`PublicStatistic` payloads are
/// ignored. Also emits the per-frame statistic gauges. The statistic twin of
/// [`LocalViewSink`](super::LocalViewSink) on the area-keyed shape.
pub struct StatisticSink {
    /// The per-area statistic channels (shared seed store + per-area broadcasts) the gRPC server
    /// reads. Each statistic's delivery area selects which area's broadcast this sink writes.
    channel: StatisticChannels,
}

impl StatisticSink {
    pub fn new(channel: StatisticChannels) -> Self {
        Self { channel }
    }
}

impl FrameSink for StatisticSink {
    fn handle(
        &mut self,
        payload: &ServerPayload,
        _bytes: &[u8],
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        if let ServerPayload::PublicStatistic(payload) = payload {
            // Keep only statistics for areas this connector serves (a broadcast channel exists
            // for them — a wire id with no known EIC can never be served); translate each to
            // domain form.
            let statistics: Vec<PublicStatistic> = payload
                .public_statistics
                .iter()
                .filter_map(|s| {
                    let eic = EicCode::from_id(s.delivery_area_id)
                        .filter(|&eic| self.channel.area(eic).is_some())?;
                    Some(to_domain_statistic(s, eic))
                })
                .collect();
            let seq = {
                // Apply to the shared store before the broadcasts below, so a frame a new subscriber
                // misses on a delta feed is already in the seed it reads on connect. The apply
                // carries the frame's receive time and bumps the frame seq, so the store's freshness
                // stamp and seq move atomically with its content. Recover a poisoned lock rather
                // than propagating — the store itself is still consistent.
                let mut book = self
                    .channel
                    .seed()
                    .lock()
                    .unwrap_or_else(|e| e.into_inner());
                book.apply(&statistics, payload.is_snapshot, received_at);
                emit_statistic_counts(&book);
                book.seq()
            };
            // Broadcast each area's changed statistics on that area's own channel — a statistic
            // frame can span areas, so split it by delivery area first.
            let mut by_area: BTreeMap<EicCode, Vec<PublicStatistic>> = BTreeMap::new();
            for statistic in statistics {
                by_area
                    .entry(statistic.delivery_area_eic)
                    .or_default()
                    .push(statistic);
            }
            for (area, statistics) in by_area {
                // The area is served (we filtered above), so the channel is present; a send error
                // only means no subscribers, which is fine — the store is still maintained.
                if let Some(updates) = self.channel.area(area) {
                    let _ = updates.send(StatisticUpdate {
                        received_at,
                        seq,
                        statistics,
                    });
                }
            }
        }
        Ok(())
    }
}

/// Translate one protobuf `PublicStatistic` into its domain [`PublicStatistic`]. The caller has
/// already resolved the wire area id to `delivery_area_eic`. The only place that knows both the
/// wire and domain statistic types.
fn to_domain_statistic(
    s: &crate::proto::pmd::PublicStatistic,
    delivery_area_eic: EicCode,
) -> PublicStatistic {
    PublicStatistic {
        delivery_area_eic,
        market_type: map_market_type(s.market_type),
        contract_id: s.contract_id,
        last_price: s.last_price,
        last_quantity: s.last_quantity,
        last_trade_time: s.last_trade_time_ms.map(from_wire_millis),
        highest_price: s.highest_price,
        lowest_price: s.lowest_price,
        vwap: s.vwap,
        turnover: s.turnover,
        day_ahead_price: s.day_ahead_price,
        tendency: map_tendency(s.tendency),
    }
}

/// Map the wire `tendency` enum value to the domain [`Tendency`].
fn map_tendency(raw: i32) -> Tendency {
    match pmd::PublicStatisticTendency::try_from(raw) {
        Ok(pmd::PublicStatisticTendency::Up) => Tendency::Up,
        Ok(pmd::PublicStatisticTendency::Down) => Tendency::Down,
        Ok(pmd::PublicStatisticTendency::Equal) => Tendency::Equal,
        Ok(pmd::PublicStatisticTendency::None) | Err(_) => Tendency::Unknown,
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, TimeDelta, Utc};
    use nordpool_marketdata::EicCode;
    use nordpool_stream_server::StatisticChannels;

    use super::StatisticSink;
    use crate::proto::pmd::server_frame::Payload as ServerPayload;
    use crate::proto::pmd::{PublicStatistic, PublicStatisticPayload};
    use crate::sink::{FrameSink, ts};

    /// Broadcast `capacity` for the test channel: more messages than any test publishes, so no
    /// subscriber lags.
    const BROADCAST_CAPACITY: usize = 64;

    fn received_at() -> DateTime<Utc> {
        DateTime::UNIX_EPOCH
    }

    fn wire_statistic(contract_id: i64, area: i32, last_price: Option<i64>) -> PublicStatistic {
        PublicStatistic {
            delivery_area_id: area,
            market_type: 1,
            contract_id,
            last_price,
            last_quantity: None,
            last_trade_time_ms: None,
            highest_price: None,
            lowest_price: None,
            vwap: None,
            turnover: None,
            day_ahead_price: None,
            tendency: 1, // PUBLIC_STATISTIC_TENDENCY_UP
        }
    }

    #[test]
    fn handle_applies_statistic_frame_to_the_shared_store_and_broadcasts_per_area() {
        // Given a sink serving two areas (40/FR, 41/CH), with a subscriber on each area's broadcast.
        let channel =
            StatisticChannels::new(BROADCAST_CAPACITY, &[EicCode::AREA_FR, EicCode::AREA_CH]);
        let seed = channel.seed().clone();
        let mut rx_fr = channel.area(EicCode::AREA_FR).unwrap().subscribe();
        let mut rx_ch = channel.area(EicCode::AREA_CH).unwrap().subscribe();
        let mut sink = StatisticSink::new(channel);

        // When a snapshot frame carries one area-40 statistic, one area-41 statistic, and one for
        // id 99 (no such EIC) — the sink must route each to its own area and drop the unknown one.
        let frame = ServerPayload::PublicStatistic(PublicStatisticPayload {
            is_snapshot: true,
            public_statistics: vec![
                wire_statistic(111, 40, Some(500)),
                wire_statistic(222, 41, Some(900)),
                wire_statistic(333, 99, Some(123)),
            ],
        });
        sink.handle(&frame, b"", received_at() + TimeDelta::seconds(5))
            .unwrap();

        // Then the frame's receive time is stamped on the store, so the next connect/lag seed
        // can carry how fresh it is.
        assert_eq!(seed.lock().unwrap().received_at(), Some(ts(5_000_000_000)));

        // Then the shared store holds the two served statistics under their areas (the unknown 99
        // dropped), each carrying its translated last_price.
        let book = seed.lock().unwrap();
        assert_eq!(book.len(), 2);
        assert_eq!(
            book.get(EicCode::AREA_FR, 111).unwrap().last_price,
            Some(500)
        );
        assert_eq!(
            book.get(EicCode::AREA_CH, 222).unwrap().last_price,
            Some(900)
        );
        assert!(book.get(EicCode::AREA_AT, 333).is_none());
        drop(book);

        // And each area's broadcast carried only that area's statistic — areas don't leak — with
        // both area batches sharing the one frame's seq.
        let batch_fr = rx_fr.try_recv().unwrap();
        assert_eq!(batch_fr.statistics[0].contract_id, 111, "area FR batch");
        assert_eq!(batch_fr.seq, 1);
        assert_eq!(
            rx_ch.try_recv().unwrap().statistics[0].contract_id,
            222,
            "area CH batch"
        );
        assert!(rx_fr.try_recv().is_err(), "area FR saw only its own batch");
        assert!(rx_ch.try_recv().is_err(), "area CH saw only its own batch");
    }
}
