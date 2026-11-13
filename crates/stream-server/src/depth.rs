//! gRPC `DepthBookService`: streams batches of depth views (top-N levels per side).
//!
//! On connect a subscriber is **seeded** with one `DepthUpdate` carrying the current top-N of every
//! book in its requested delivery area (read from that area's seed map the producer keeps), then
//! streamed that area's changed books as one `DepthUpdate` batch. A subscriber that lags (cannot
//! keep up with the broadcast) has its stream dropped — re-seeding a consumer that can't drain the
//! feed would only deepen its backlog, so it reconnects to re-seed instead. Each `DepthView` is
//! full current top-N of one book (state, not a delta). The request's `eic_code` selects
//! one of the areas this server serves: an area this connector doesn't carry is rejected
//! `NOT_FOUND`; on a match that area's whole set is streamed (no per-contract filter). Composed
//! into the server by [`serve`](crate::serve) alongside the `ContractService`.

use std::pin::Pin;
use std::sync::Mutex;

use chrono::{DateTime, Utc};
use tokio::sync::watch;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::{Stream, StreamExt};
use tonic::{Request, Response, Status};

use nordpool_marketdata::{DepthStore, DepthView, EicCode, PriceLevel};

use crate::feed::{DepthChannels, DepthUpdate};
use crate::proto::depth_book_service_server::{DepthBookService, DepthBookServiceServer};
use crate::proto::{self, Level, SubscribeDepthRequest};
use crate::subscriber::SubscriberLimit;
use crate::{to_market_type, to_timestamp};

/// Build the configured `DepthBookService` server: the streaming `Subscribe` over the depth
/// `channel` (one per-area broadcast + seed map per served area), capped at `max_subscribers`.
pub(crate) fn depth_book_service(
    channel: DepthChannels,
    max_subscribers: usize,
    shutdown: watch::Receiver<bool>,
) -> DepthBookServiceServer<DepthFeed> {
    DepthBookServiceServer::new(DepthFeed::new(channel, max_subscribers, shutdown))
}

pub(crate) struct DepthFeed {
    /// The producer↔server boundary: the per-area broadcasts subscribers attach to and the per-area
    /// seed maps read on connect — a clone of the same channel the connector's `LocalViewSink`
    /// writes. The set of served delivery areas is exactly this channel's keys; a `Subscribe` for
    /// an area not present is rejected.
    channel: DepthChannels,
    /// Bounds concurrent subscribers; a permit is held for the lifetime of each `Subscribe` stream.
    limit: SubscriberLimit,
    /// The process shutdown watch: every `Subscribe` stream cuts itself when it fires, which is
    /// what lets the server's graceful drain complete (see [`serve`](crate::serve)).
    shutdown: watch::Receiver<bool>,
}

impl DepthFeed {
    fn new(
        channel: DepthChannels,
        max_subscribers: usize,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            channel,
            limit: SubscriberLimit::new(
                max_subscribers,
                "nordpool.depth.subscribers",
                "depth subscriber limit reached",
            ),
            shutdown,
        }
    }
}

type DepthUpdateStream = Pin<Box<dyn Stream<Item = Result<proto::DepthUpdate, Status>> + Send>>;

#[tonic::async_trait]
impl DepthBookService for DepthFeed {
    type SubscribeStream = DepthUpdateStream;

    // The stream item is Result<_, Status>; Status is large by tonic's design,
    // so result_large_err is unavoidable for a streaming handler.
    #[allow(clippy::result_large_err)]
    async fn subscribe(
        &self,
        request: Request<SubscribeDepthRequest>,
    ) -> Result<Response<Self::SubscribeStream>, Status> {
        // Select the requested area among those this server serves; an EIC we don't know or an
        // area this connector doesn't carry means the client is pointed at the wrong connector.
        // Within the area we stream every book (no per-contract filter).
        let requested = request.into_inner().eic_code;
        tracing::info!(delivery_area = %requested, "depth Subscribe");
        let Some(area) = EicCode::from_eic(&requested).and_then(|eic| self.channel.area(eic))
        else {
            return Err(Status::not_found(format!(
                "delivery area {requested:?} not served"
            )));
        };

        let guard = self.limit.acquire()?;

        // Attach to the area's broadcast BEFORE reading its seed: any frame applied after our seed is
        // then guaranteed to be waiting in `rx`, so the seed + deltas cover every change with no gap.
        let rx = area.updates.subscribe();
        let seed = seed(&area.seed);
        let seed_seq = seed.seq;

        let deltas = BroadcastStream::new(rx).filter_map(move |result| {
            // The guard lives as long as this closure (and so the stream), releasing the permit and
            // decrementing the gauge when the client disconnects.
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
                    // A lag means the client can't keep up; re-seeding would only push it the whole
                    // book set it already can't drain. End the stream with a terminal status (tonic
                    // stops at the first Err) and free the slot — a live client reconnects.
                    tracing::warn!(skipped, "depth-book subscriber lagged; dropping");
                    metrics::counter!("nordpool.depth.lagged").increment(1);
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

/// Build the connect/lag seed: one `DepthUpdate` with the current top-N of every book in one
/// delivery area. Clones only that area's small map of top-N projections under a brief lock —
/// never the order books, and not the wire conversion: the producer takes this lock per frame on
/// the driver's receive loop, so the proto building (the expensive half) happens after the lock
/// is released. The store's `apply` updates views, freshness stamp, and seq together, so the
/// seed's `received_at` and `seq` — the area's last applied frame — always agree with its
/// content.
fn seed(seed: &Mutex<DepthStore>) -> proto::DepthUpdate {
    let (views, received_at, seq): (Vec<DepthView>, Option<DateTime<Utc>>, u64) = {
        let guard = seed.lock().unwrap_or_else(|e| e.into_inner());
        (
            guard.iter().cloned().collect(),
            guard.received_at(),
            guard.seq(),
        )
    };
    proto::DepthUpdate {
        received_at: received_at.map(to_timestamp),
        seq: seq as i64,
        views: views.iter().map(to_proto).collect(),
    }
}

/// Convert one frame's changed books (a broadcast batch) into one wire `DepthUpdate`, carrying the
/// frame's receive time and seq.
fn to_update(batch: &DepthUpdate) -> proto::DepthUpdate {
    proto::DepthUpdate {
        received_at: Some(to_timestamp(batch.received_at)),
        seq: batch.seq as i64,
        views: batch.views.iter().map(to_proto).collect(),
    }
}

/// Convert a domain depth view into its wire form.
fn to_proto(top: &DepthView) -> proto::DepthView {
    proto::DepthView {
        contract_id: top.key.contract_id,
        market_type: to_market_type(top.key.market_type) as i32,
        revision: top.revision,
        bids: top.bids.iter().copied().map(to_level).collect(),
        asks: top.asks.iter().copied().map(to_level).collect(),
    }
}

fn to_level(level: PriceLevel) -> Level {
    Level {
        price: level.price,
        total_quantity: level.total_quantity,
        order_count: level.order_count,
        created_at: Some(to_timestamp(level.created_at)),
        updated_at: Some(to_timestamp(level.updated_at)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::feed::DepthChannels;
    use crate::ts;
    use nordpool_marketdata::{BookKey, MarketType};

    fn batch(seq: u64, views: Vec<DepthView>) -> DepthUpdate {
        DepthUpdate {
            received_at: ts(0),
            seq,
            views,
        }
    }

    /// A shutdown receiver that never fires: the sender is leaked to keep the watch open — a
    /// closed watch reads as shutdown and would cut every stream.
    fn no_shutdown() -> watch::Receiver<bool> {
        let (tx, rx) = watch::channel(false);
        std::mem::forget(tx);
        rx
    }

    fn view(contract_id: i64, price: i64) -> DepthView {
        DepthView {
            key: BookKey {
                market_type: MarketType::Local,
                contract_id,
            },
            revision: 1,
            bids: vec![PriceLevel {
                price,
                total_quantity: 1,
                order_count: 1,
                created_at: ts(0),
                updated_at: ts(0),
            }],
            asks: vec![],
        }
    }

    #[tokio::test]
    async fn subscribe_seeds_current_books_then_streams_deltas() {
        // Given a depth feed serving area FR whose seed map already holds book 1's current top-N (a
        // bid at 100), as if the producer had recorded it before any client connected — this is the
        // state a new subscriber to FR should be seeded from.
        let channel = DepthChannels::new(64, &[EicCode::AREA_FR]);
        channel
            .area(EicCode::AREA_FR)
            .unwrap()
            .seed
            .lock()
            .unwrap()
            .apply(&[view(1, 100)], true, ts(7_000));
        let tx = channel.area(EicCode::AREA_FR).unwrap().updates.clone();
        let feed = DepthFeed::new(channel, 8, no_shutdown());

        // When a client subscribes naming the served area (FR) — a valid Subscribe request.
        let response = feed
            .subscribe(Request::new(SubscribeDepthRequest {
                eic_code: EicCode::AREA_FR.to_string(),
            }))
            .await
            .expect("subscribe accepted");
        let mut stream = response.into_inner();

        // Then the first message is the seed batch: the current top-N of book 1 read from the map,
        // stamped with the area's last applied frame time and seq, so the client starts in sync —
        // and knows how fresh that sync is — without waiting for the book's next change.
        let seed = stream.next().await.unwrap().unwrap();
        assert_eq!(seed.views.len(), 1);
        assert_eq!(seed.views[0].contract_id, 1);
        assert_eq!(seed.views[0].bids[0].price, 100);
        assert_eq!(seed.received_at, Some(to_timestamp(ts(7_000))));
        assert_eq!(seed.seq, 1);

        // ...and a later frame's batch (book 2, seq past the seed's) is forwarded live on the same
        // stream, showing the seed-then-deltas handover works with no gap.
        tx.send(batch(2, vec![view(2, 200)])).unwrap();
        let delta = stream.next().await.unwrap().unwrap();
        assert_eq!(delta.views[0].contract_id, 2);
        assert_eq!(delta.views[0].bids[0].price, 200);
        assert_eq!(delta.seq, 2);
    }

    #[tokio::test]
    async fn subscribe_drops_deltas_the_seed_already_reflects() {
        // Given an area whose seed reflects one applied frame (seq 1) — the state a subscriber's
        // seed message is built from.
        let channel = DepthChannels::new(64, &[EicCode::AREA_FR]);
        channel
            .area(EicCode::AREA_FR)
            .unwrap()
            .seed
            .lock()
            .unwrap()
            .apply(&[view(1, 100)], true, ts(0));
        let tx = channel.area(EicCode::AREA_FR).unwrap().updates.clone();
        let feed = DepthFeed::new(channel, 8, no_shutdown());

        // When a client subscribes and the queue holds a frame the seed already covers (seq 1 —
        // the subscribe/seed overlap) followed by a genuinely new one (seq 2).
        let mut stream = feed
            .subscribe(Request::new(SubscribeDepthRequest {
                eic_code: EicCode::AREA_FR.to_string(),
            }))
            .await
            .expect("subscribe accepted")
            .into_inner();
        tx.send(batch(1, vec![view(1, 100)])).unwrap();
        tx.send(batch(2, vec![view(2, 200)])).unwrap();

        // Then after the seed the duplicate seq-1 batch is dropped and the first delta delivered
        // is seq 2 — the stream is strictly newer than the seed, so a client can never observe a
        // transient regression to pre-seed state.
        let seed = stream.next().await.unwrap().unwrap();
        assert_eq!(seed.seq, 1);
        let delta = stream.next().await.unwrap().unwrap();
        assert_eq!(delta.seq, 2);
        assert_eq!(delta.views[0].contract_id, 2);
    }

    #[tokio::test]
    async fn subscribe_streams_only_the_requested_areas_books() {
        // Given a feed serving two areas, each seeded with its own book (FR has book 1, NO1
        // has book 2) — a subscriber must see only the area it asked for, on both the seed and the
        // live deltas.
        let channel = DepthChannels::new(64, &[EicCode::AREA_FR, EicCode::AREA_NO1]);
        channel
            .area(EicCode::AREA_FR)
            .unwrap()
            .seed
            .lock()
            .unwrap()
            .apply(&[view(1, 100)], true, ts(0));
        channel
            .area(EicCode::AREA_NO1)
            .unwrap()
            .seed
            .lock()
            .unwrap()
            .apply(&[view(2, 200)], true, ts(0));
        let tx40 = channel.area(EicCode::AREA_FR).unwrap().updates.clone();
        let tx50 = channel.area(EicCode::AREA_NO1).unwrap().updates.clone();
        let feed = DepthFeed::new(channel, 8, no_shutdown());

        // When a client subscribes to area FR.
        let mut stream = feed
            .subscribe(Request::new(SubscribeDepthRequest {
                eic_code: EicCode::AREA_FR.to_string(),
            }))
            .await
            .expect("subscribe accepted")
            .into_inner();

        // Then it is seeded with FR's book 1 only (not NO1's book 2).
        let seed = stream.next().await.unwrap().unwrap();
        assert_eq!(seed.views.len(), 1);
        assert_eq!(seed.views[0].contract_id, 1);

        // ...and a delta broadcast on the *other* area is never delivered, while a delta on the
        // subscribed area is — each area's broadcast is isolated. (The area-50 send has no receiver
        // precisely because this subscriber is on FR only, so its `SendError` is expected.)
        let _ = tx50.send(batch(2, vec![view(9, 900)]));
        tx40.send(batch(2, vec![view(3, 300)])).unwrap();
        let delta = stream.next().await.unwrap().unwrap();
        assert_eq!(delta.views[0].contract_id, 3);
    }

    #[tokio::test]
    async fn subscribe_enforces_the_max_subscriber_cap() {
        // Given a depth feed whose subscriber semaphore has exactly one permit, so only one client
        // may stream at a time — the back-pressure cap that protects the server from unbounded fan-out.
        let feed = DepthFeed::new(
            DepthChannels::new(16, &[EicCode::AREA_FR]),
            1,
            no_shutdown(),
        );
        let request = || {
            Request::new(SubscribeDepthRequest {
                eic_code: EicCode::AREA_FR.to_string(),
            })
        };

        // When the first client subscribes, it acquires the sole permit and is accepted.
        let first = feed.subscribe(request()).await;
        assert!(first.is_ok());

        // Then a second subscribe with no permit left is rejected up front with RESOURCE_EXHAUSTED,
        // rather than being accepted onto a starved connection.
        let second = feed.subscribe(request()).await;
        assert_eq!(
            second.err().expect("second subscribe rejected").code(),
            tonic::Code::ResourceExhausted
        );

        // And dropping the first response stream (the client disconnecting) releases its permit
        // synchronously (the guard rides on the stream), so the freed slot lets a new subscribe succeed.
        drop(first);
        assert!(feed.subscribe(request()).await.is_ok());
    }

    #[tokio::test]
    async fn subscribe_ends_the_stream_with_unavailable_on_shutdown() {
        // Given a client already subscribed and seeded — a stream that would otherwise be
        // infinite.
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let channel = DepthChannels::new(64, &[EicCode::AREA_FR]);
        channel
            .area(EicCode::AREA_FR)
            .unwrap()
            .seed
            .lock()
            .unwrap()
            .apply(&[view(1, 100)], true, ts(0));
        let feed = DepthFeed::new(channel, 8, shutdown_rx);
        let mut stream = feed
            .subscribe(Request::new(SubscribeDepthRequest {
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

        // And a subscribe arriving after the request is cut the same way, before streaming
        // anything (its wait_for resolves immediately — a late stream must not outlive the drain).
        let mut late = feed
            .subscribe(Request::new(SubscribeDepthRequest {
                eic_code: EicCode::AREA_FR.to_string(),
            }))
            .await
            .expect("subscribe accepted")
            .into_inner();
        let end = late.next().await.unwrap();
        assert_eq!(
            end.expect_err("terminal status").code(),
            tonic::Code::Unavailable
        );
    }

    #[tokio::test]
    async fn subscribe_guards_the_delivery_area() {
        // Given a feed configured to serve exactly one delivery area (FR) — the request's area
        // selects among the served areas, and an area this connector doesn't carry is rejected.
        let feed = DepthFeed::new(
            DepthChannels::new(16, &[EicCode::AREA_FR]),
            8,
            no_shutdown(),
        );
        let subscribe = |eic: &str| {
            feed.subscribe(Request::new(SubscribeDepthRequest {
                eic_code: eic.into(),
            }))
        };

        // Then subscribing to the served area FR succeeds, while an empty EIC (unspecified
        // default), a known-but-unserved area (AT), and an unknown EIC string are all rejected
        // NOT_FOUND — the guard accepts only a served area.
        assert!(subscribe(EicCode::AREA_FR.eic_code()).await.is_ok());
        assert_eq!(
            subscribe("")
                .await
                .err()
                .expect("unspecified rejected")
                .code(),
            tonic::Code::NotFound
        );
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
