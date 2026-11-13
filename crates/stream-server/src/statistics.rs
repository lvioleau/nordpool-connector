//! gRPC `PublicStatisticService`: streams public trading statistics, per delivery area.
//!
//! On connect a client names a delivery area; it receives that area's full current set as the first
//! message, then that area's changed batch per statistic frame. The full set is seeded from a shared
//! area-keyed [`StatisticStore`] under a `Mutex` (the connector sink holds the master `Arc` and
//! mutates it in place); live deltas after the seed arrive over that area's `broadcast`. A subscribe
//! for an area this connector doesn't serve is rejected `NOT_FOUND`; a lagged subscriber has its
//! stream dropped and reconnects to re-seed. Composed into the server by [`serve`](crate::serve)
//! alongside the depth and contract services. Structurally a twin of
//! [`DepthBookService`](crate::depth).

use std::pin::Pin;
use std::sync::{Arc, Mutex};

use chrono::{DateTime, Utc};
use tokio::sync::watch;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use nordpool_marketdata::{
    EicCode, PublicStatistic as DomainStatistic, StatisticStore, Tendency as DomainTendency,
};

use crate::feed::{StatisticChannels, StatisticUpdate};
use crate::proto::public_statistic_service_server::{
    PublicStatisticService, PublicStatisticServiceServer,
};
use crate::proto::{self, SubscribeStatisticsRequest};
use crate::subscriber::SubscriberLimit;
use crate::{to_market_type, to_timestamp};

/// The shared area-keyed statistic store, shared between the connector sink (writer) and the gRPC
/// [`PublicStatisticService`] (reader). A plain `Mutex`: the writer applies each (infrequent)
/// statistic frame under the lock, the reader builds one area's connect/lag seed under the lock, and
/// the per-area set is small. The store carries its own freshness stamp
/// ([`StatisticStore::received_at`]), updated by the same `apply` that changes the store, so
/// a seed read can never observe a stamp out of step with the content.
pub type SharedStatistics = Arc<Mutex<StatisticStore>>;

/// Build the configured `PublicStatisticService` server: the streaming `SubscribeStatistics` over
/// the statistic `channel` (one per-area broadcast + the shared seed store), capped at
/// `max_subscribers`.
pub(crate) fn statistic_service(
    channel: StatisticChannels,
    max_subscribers: usize,
    shutdown: watch::Receiver<bool>,
) -> PublicStatisticServiceServer<StatisticFeed> {
    PublicStatisticServiceServer::new(StatisticFeed::new(channel, max_subscribers, shutdown))
}

/// Streams public statistics for one delivery area: the area's full current set as the first
/// message, then each statistic frame's changed batch for that area. Seeds each stream from the
/// shared [`StatisticStore`], then forwards the area's delta feed; on lag it drops the stream (the
/// client reconnects).
pub(crate) struct StatisticFeed {
    /// The producer↔server boundary: the shared store seeded on connect and the per-area broadcasts
    /// subscribers attach to — a clone of the same channel the connector's `StatisticSink` writes.
    /// The set of served areas is exactly this channel's keys; a subscribe for an absent area is
    /// rejected.
    channel: StatisticChannels,
    /// Bounds concurrent statistic subscribers, mirroring the depth feed.
    limit: SubscriberLimit,
    /// The process shutdown watch: every `SubscribeStatistics` stream cuts itself when it fires,
    /// which is what lets the server's graceful drain complete (see [`serve`](crate::serve)).
    shutdown: watch::Receiver<bool>,
}

impl StatisticFeed {
    fn new(
        channel: StatisticChannels,
        max_subscribers: usize,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            channel,
            limit: SubscriberLimit::new(
                max_subscribers,
                "nordpool.statistics.subscribers",
                "statistic subscriber limit reached",
            ),
            shutdown,
        }
    }
}

type StatisticStream = Pin<Box<dyn Stream<Item = Result<proto::StatisticUpdate, Status>> + Send>>;

#[tonic::async_trait]
impl PublicStatisticService for StatisticFeed {
    type SubscribeStatisticsStream = StatisticStream;

    #[allow(clippy::result_large_err)]
    async fn subscribe_statistics(
        &self,
        request: Request<SubscribeStatisticsRequest>,
    ) -> Result<Response<Self::SubscribeStatisticsStream>, Status> {
        // Select the requested area among those this server serves; an EIC we don't know or an
        // area this connector doesn't carry means the client is pointed at the wrong connector.
        let requested = request.into_inner().eic_code;
        tracing::info!(delivery_area = %requested, "statistics Subscribe");
        let Some((requested_area, updates)) =
            EicCode::from_eic(&requested).and_then(|eic| Some((eic, self.channel.area(eic)?)))
        else {
            return Err(Status::not_found(format!(
                "delivery area {requested:?} not served"
            )));
        };

        let guard = self.limit.acquire()?;

        // Attach to the area's broadcast BEFORE reading the seed: any frame applied to the store
        // after our seed is then guaranteed to be waiting in `rx`, so the seed + the deltas cover
        // every frame with no gap.
        let rx = updates.subscribe();
        let seed = seed(self.channel.seed(), requested_area);
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
                    Some(Ok(to_update(&batch)))
                }
                Err(BroadcastStreamRecvError::Lagged(skipped)) => {
                    // A lag means the client can't keep up; re-sending the full set would only push
                    // it more data it can't drain. End the stream with a terminal status (tonic
                    // stops at the first Err) and free the slot — a live client reconnects.
                    tracing::warn!(skipped, "statistic subscriber lagged; dropping");
                    metrics::counter!("nordpool.statistics.lagged").increment(1);
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

/// Build the connect/lag seed: a `proto::StatisticUpdate` with one delivery area's full current
/// set. Only the clone of that area's slice happens under the lock — the wire conversion (the
/// expensive half) runs after it is released, since the writer applies frames under this lock on
/// the driver's receive loop. The store's `apply` updates content and freshness stamp together,
/// so the seed's `received_at` — the store's last applied frame time — always agrees with
/// its content.
fn seed(statistics: &SharedStatistics, delivery_area_eic: EicCode) -> proto::StatisticUpdate {
    // A poisoned lock means a reader/writer panicked while holding it; the data is still
    // consistent, so recover the guard rather than propagating.
    let (area, received_at, seq): (Vec<DomainStatistic>, Option<DateTime<Utc>>, u64) = {
        let store = statistics.lock().unwrap_or_else(|e| e.into_inner());
        (
            store.area(delivery_area_eic).cloned().collect(),
            store.received_at(),
            store.seq(),
        )
    };
    proto::StatisticUpdate {
        received_at: received_at.map(to_timestamp),
        seq: seq as i64,
        statistics: area.iter().map(to_statistic).collect(),
    }
}

/// Convert a delta batch to wire form, carrying the frame's receive time and seq.
fn to_update(batch: &StatisticUpdate) -> proto::StatisticUpdate {
    proto::StatisticUpdate {
        received_at: Some(to_timestamp(batch.received_at)),
        seq: batch.seq as i64,
        statistics: batch.statistics.iter().map(to_statistic).collect(),
    }
}

fn to_statistic(s: &DomainStatistic) -> proto::Statistic {
    proto::Statistic {
        contract_id: s.contract_id,
        market_type: to_market_type(s.market_type) as i32,
        eic_code: s.delivery_area_eic.eic_code().to_string(),
        last_price: s.last_price,
        last_quantity: s.last_quantity,
        last_trade_time: s.last_trade_time.map(to_timestamp),
        highest_price: s.highest_price,
        lowest_price: s.lowest_price,
        vwap: s.vwap,
        turnover: s.turnover,
        day_ahead_price: s.day_ahead_price,
        tendency: to_tendency(s.tendency) as i32,
    }
}

fn to_tendency(tendency: DomainTendency) -> proto::StatisticTendency {
    match tendency {
        DomainTendency::Unknown => proto::StatisticTendency::None,
        DomainTendency::Up => proto::StatisticTendency::Up,
        DomainTendency::Down => proto::StatisticTendency::Down,
        DomainTendency::Equal => proto::StatisticTendency::Equal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ts;
    use nordpool_marketdata::{MarketType, PublicStatistic, Tendency};

    fn statistic(id: i64, area: EicCode, last_price: Option<i64>) -> PublicStatistic {
        PublicStatistic {
            delivery_area_eic: area,
            market_type: MarketType::Local,
            contract_id: id,
            last_price,
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

    /// A statistic-channels set serving the given areas, with the shared store seeded by `seed`.
    fn channels(areas: &[EicCode], seed: &[PublicStatistic]) -> StatisticChannels {
        let channel = StatisticChannels::new(64, areas);
        channel.seed().lock().unwrap().apply(seed, true, ts(0));
        channel
    }

    /// A shutdown receiver that never fires: the sender is leaked to keep the watch open — a
    /// closed watch reads as shutdown and would cut every stream.
    fn no_shutdown() -> watch::Receiver<bool> {
        let (tx, rx) = watch::channel(false);
        std::mem::forget(tx);
        rx
    }

    fn batch(seq: u64, statistics: Vec<PublicStatistic>) -> StatisticUpdate {
        StatisticUpdate {
            received_at: ts(0),
            seq,
            statistics,
        }
    }

    #[test]
    fn seed_converts_one_areas_full_set_to_proto() {
        // Given a store serving area FR, seeded with three statistics there (ids 1, 2, 3).
        let channel = channels(
            &[EicCode::AREA_FR],
            &[
                statistic(1, EicCode::AREA_FR, Some(100)),
                statistic(2, EicCode::AREA_FR, Some(200)),
                statistic(3, EicCode::AREA_FR, None),
            ],
        );

        // When building the connect/lag seed message for area FR, after an (empty) delta frame
        // received at t=1234 refreshed the store's stamp.
        channel.seed().lock().unwrap().apply(&[], false, ts(1234));
        let got = seed(channel.seed(), EicCode::AREA_FR);

        // Then every statistic is present, converted to wire form (optional fields mapped, the
        // seed carrying the store's stamp and last applied frame seq), and the seed is the
        // complete set, never filtered.
        assert_eq!(got.received_at, Some(to_timestamp(ts(1234))));
        assert_eq!(got.seq, 2);
        let mut ids: Vec<i64> = got.statistics.iter().map(|s| s.contract_id).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);
        let one = got.statistics.iter().find(|s| s.contract_id == 1).unwrap();
        assert_eq!(one.last_price, Some(100));
        assert_eq!(one.tendency, proto::StatisticTendency::Up as i32);
        assert_eq!(
            got.statistics
                .iter()
                .find(|s| s.contract_id == 3)
                .unwrap()
                .last_price,
            None
        );
    }

    #[test]
    fn to_update_converts_the_whole_batch_to_proto() {
        // Given an incremental delta batch of two changed statistics from the feed's 7th frame.
        let batch = batch(
            7,
            vec![
                statistic(1, EicCode::AREA_FR, Some(100)),
                statistic(2, EicCode::AREA_FR, Some(200)),
            ],
        );

        // When converting that delta batch to the wire StatisticUpdate streamed to subscribers,
        // every statistic is carried through and the frame's seq is preserved.
        let got = to_update(&batch);
        let mut ids: Vec<i64> = got.statistics.iter().map(|s| s.contract_id).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2]);
        assert_eq!(got.seq, 7);
    }

    #[tokio::test]
    async fn subscribe_statistics_seeds_full_set_then_streams_deltas() {
        // Given an area-FR store already holding three statistics plus its broadcast delta feed.
        let channel = channels(
            &[EicCode::AREA_FR],
            &[
                statistic(1, EicCode::AREA_FR, Some(100)),
                statistic(2, EicCode::AREA_FR, Some(200)),
                statistic(3, EicCode::AREA_FR, None),
            ],
        );
        let tx = channel.area(EicCode::AREA_FR).unwrap().clone();
        let dir = StatisticFeed::new(channel, 8, no_shutdown());

        // When a client subscribes naming the served area (FR).
        let response = dir
            .subscribe_statistics(Request::new(SubscribeStatisticsRequest {
                eic_code: EicCode::AREA_FR.to_string(),
            }))
            .await
            .expect("subscribe accepted");
        let mut stream = response.into_inner();

        // Then the first message is area FR's full current set (ids 1, 2, 3) seeded from the store.
        let seed = stream.next().await.unwrap().unwrap();
        let mut ids: Vec<i64> = seed.statistics.iter().map(|s| s.contract_id).collect();
        ids.sort();
        assert_eq!(ids, vec![1, 2, 3]);

        // And a statistic frame's delta broadcast after the seed (a new contract 4, seq past the
        // seeded store's) is forwarded.
        tx.send(batch(2, vec![statistic(4, EicCode::AREA_FR, Some(400))]))
            .unwrap();
        let delta = stream.next().await.unwrap().unwrap();
        assert_eq!(
            delta
                .statistics
                .iter()
                .map(|s| s.contract_id)
                .collect::<Vec<_>>(),
            vec![4]
        );
    }

    #[tokio::test]
    async fn subscribe_statistics_drops_deltas_the_seed_already_reflects() {
        // Given a store whose seed reflects one applied frame (seq 1).
        let channel = channels(
            &[EicCode::AREA_FR],
            &[statistic(1, EicCode::AREA_FR, Some(100))],
        );
        let tx = channel.area(EicCode::AREA_FR).unwrap().clone();
        let dir = StatisticFeed::new(channel, 8, no_shutdown());

        // When a client subscribes and the queue holds a frame the seed already covers (seq 1 —
        // the subscribe/seed overlap) followed by a genuinely new one (seq 2).
        let mut stream = dir
            .subscribe_statistics(Request::new(SubscribeStatisticsRequest {
                eic_code: EicCode::AREA_FR.to_string(),
            }))
            .await
            .expect("subscribe accepted")
            .into_inner();
        tx.send(batch(1, vec![statistic(1, EicCode::AREA_FR, Some(100))]))
            .unwrap();
        tx.send(batch(2, vec![statistic(2, EicCode::AREA_FR, Some(200))]))
            .unwrap();

        // Then after the seed (seq 1) the duplicate seq-1 batch is dropped and the first delta
        // delivered is seq 2 — the stream is strictly newer than the seed.
        let seed = stream.next().await.unwrap().unwrap();
        assert_eq!(seed.seq, 1);
        let delta = stream.next().await.unwrap().unwrap();
        assert_eq!(delta.seq, 2);
        assert_eq!(delta.statistics[0].contract_id, 2);
    }

    #[tokio::test]
    async fn subscribe_statistics_streams_only_the_requested_areas_statistics() {
        // Given a feed serving two areas, each seeded with its own statistic.
        let channel = channels(
            &[EicCode::AREA_FR, EicCode::AREA_NO1],
            &[
                statistic(1, EicCode::AREA_FR, Some(100)),
                statistic(2, EicCode::AREA_NO1, Some(200)),
            ],
        );
        let tx40 = channel.area(EicCode::AREA_FR).unwrap().clone();
        let tx50 = channel.area(EicCode::AREA_NO1).unwrap().clone();
        let dir = StatisticFeed::new(channel, 8, no_shutdown());

        // When a client subscribes to area FR.
        let mut stream = dir
            .subscribe_statistics(Request::new(SubscribeStatisticsRequest {
                eic_code: EicCode::AREA_FR.to_string(),
            }))
            .await
            .expect("subscribe accepted")
            .into_inner();

        // Then it is seeded with FR's statistic only (not NO1's).
        let seed = stream.next().await.unwrap().unwrap();
        assert_eq!(
            seed.statistics
                .iter()
                .map(|s| s.contract_id)
                .collect::<Vec<_>>(),
            vec![1]
        );

        // ...and a delta on the other area is never delivered, while one on the subscribed area is.
        let _ = tx50.send(batch(2, vec![statistic(9, EicCode::AREA_NO1, Some(900))]));
        tx40.send(batch(2, vec![statistic(3, EicCode::AREA_FR, Some(300))]))
            .unwrap();
        let delta = stream.next().await.unwrap().unwrap();
        assert_eq!(delta.statistics[0].contract_id, 3);
    }

    #[tokio::test]
    async fn subscribe_statistics_ends_the_stream_with_unavailable_on_shutdown() {
        // Given a client already subscribed and seeded — a stream that would otherwise be
        // infinite.
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let channel = channels(
            &[EicCode::AREA_FR],
            &[statistic(1, EicCode::AREA_FR, Some(100))],
        );
        let dir = StatisticFeed::new(channel, 8, shutdown_rx);
        let mut stream = dir
            .subscribe_statistics(Request::new(SubscribeStatisticsRequest {
                eic_code: EicCode::AREA_FR.to_string(),
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
    async fn subscribe_statistics_guards_the_delivery_area() {
        // Given a feed configured to serve exactly one delivery area (FR).
        let dir = StatisticFeed::new(channels(&[EicCode::AREA_FR], &[]), 8, no_shutdown());
        let subscribe = |eic: &str| {
            dir.subscribe_statistics(Request::new(SubscribeStatisticsRequest {
                eic_code: eic.into(),
            }))
        };

        // Then subscribing to the served area FR succeeds, while a known-but-unserved area (AT)
        // and an unknown EIC string are rejected NOT_FOUND — the guard accepts only a served area.
        assert!(subscribe(EicCode::AREA_FR.eic_code()).await.is_ok());
        assert_eq!(
            subscribe(EicCode::AREA_AT.eic_code())
                .await
                .err()
                .expect("unserved area rejected")
                .code(),
            tonic::Code::NotFound
        );
        assert_eq!(
            subscribe("10YNO-SUCH-AREA-")
                .await
                .err()
                .expect("unknown eic rejected")
                .code(),
            tonic::Code::NotFound
        );
    }
}
