use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use chrono::{DateTime, Utc};

use nordpool_marketdata::{BookKey, BookUpdate, DepthView, EicCode, Order, OrderBookStore};
use nordpool_stream_server::{DepthChannels, DepthUpdate};

use crate::proto::pmd::LocalViewPayload;
use crate::proto::pmd::server_frame::Payload as ServerPayload;

use super::FrameSink;
use super::book_metrics::{BookMetrics, emit_update_counts};
use super::{from_wire_millis, map_market_type, market_type_tag};

/// Reconstructs local order books from `LocalView` frames and feeds the depth gRPC server. Each
/// frame is scoped to one delivery area; it is translated into domain [`BookUpdate`]s and folded
/// into that area's books in the [`OrderBookStore`]; then, for every book whose visible top moved
/// (suppression via [`OrderBook::top_hash`] against `last_hashes`), a fresh top-`depth`
/// [`DepthView`] is recorded in that area's seed map (so the server can seed new/lagged
/// subscribers) **and then** broadcast on that area's channel. Non-`LocalView` payloads are
/// ignored. Also drives the periodic book-state gauges off the store.
///
/// [`OrderBook::top_hash`]: nordpool_marketdata::OrderBook::top_hash
pub struct LocalViewSink {
    store: OrderBookStore,
    /// The per-area depth channels (seed maps + broadcasts) the gRPC server reads. A frame's
    /// delivery area selects which area's seed map and broadcast this sink writes.
    channel: DepthChannels,
    /// Per-area, per-book digest of the last broadcast top-N, to suppress updates that didn't move
    /// the top. Keyed first by delivery area (a `BTreeMap`, matching the store/channel area index)
    /// because a `BookKey` (a contract id) can recur across areas as a distinct book.
    last_hashes: BTreeMap<EicCode, HashMap<BookKey, u64>>,
    /// Price levels per side carried in each view (and compared for suppression).
    depth: usize,
    book_metrics: BookMetrics,
}

impl LocalViewSink {
    /// Build a sink that broadcasts a book's top-`depth` view over its delivery area's channel
    /// whenever its visible top changes, recording each in that area's seed map the gRPC
    /// `Subscribe` stream reads. The `nordpool.book.*` state gauges refresh every `book_state_period`.
    pub fn new(channel: DepthChannels, book_state_period: Duration, depth: usize) -> Self {
        Self {
            store: OrderBookStore::new(),
            channel,
            last_hashes: BTreeMap::new(),
            depth,
            book_metrics: BookMetrics::new(book_state_period),
        }
    }
}

impl FrameSink for LocalViewSink {
    fn handle(
        &mut self,
        payload: &ServerPayload,
        _bytes: &[u8],
        received_at: DateTime<Utc>,
    ) -> anyhow::Result<()> {
        let ServerPayload::LocalView(payload) = payload else {
            return Ok(());
        };
        // The area id is server-controlled input: an id with no known EIC, or an area we never
        // subscribed to, is a wire anomaly, not a programming error — drop it (warn, stay
        // correct) before it pollutes the store with books no subscriber can read. One lookup
        // both guards the frame and yields the channel it will be published on, so the "only a
        // served area gets this far" invariant is carried rather than re-asserted.
        let Some((delivery_area_eic, area)) = EicCode::from_id(payload.delivery_area_id)
            .and_then(|eic| self.channel.area(eic).map(|area| (eic, area)))
        else {
            tracing::warn!(
                delivery_area_id = payload.delivery_area_id,
                "LocalView frame for unknown or unserved delivery area; dropped"
            );
            return Ok(());
        };

        let updates = to_book_updates(payload);
        emit_update_counts(&updates, payload.is_snapshot);
        self.store.apply(
            delivery_area_eic,
            &updates,
            payload.is_snapshot,
            received_at,
        );

        // A snapshot frame replaced the area's books, so its suppression hashes are cleared
        // first — they then track exactly the new book set.
        let last_hashes = self.last_hashes.entry(delivery_area_eic).or_default();
        if payload.is_snapshot {
            last_hashes.clear();
        }
        let changed = changed_views(
            &self.store,
            delivery_area_eic,
            &updates,
            last_hashes,
            self.depth,
        );

        // Apply the whole frame to the seed BEFORE the broadcast below: the store's `apply`
        // moves the snapshot clear, the changed views, the freshness stamp, and the frame seq in
        // one critical section, so a subscriber that subscribes then reads the seed can never
        // observe a half-applied frame or a stamp/seq out of step with the views — and sees
        // every change in the seed or the broadcast, never neither. Every applied frame stamps
        // and counts — even one that moved no visible top — because it still proves the area's
        // state current as of its arrival.
        let seq = {
            let mut seed = area.seed.lock().unwrap_or_else(|e| e.into_inner());
            seed.apply(&changed, payload.is_snapshot, received_at);
            seed.seq()
        };

        for view in &changed {
            metrics::counter!(
                "nordpool.depth.views_sent",
                "market_type" => market_type_tag(view.key.market_type),
            )
            .increment(1);
        }
        // Broadcast the area's changed books as one batch (the depth twin of the contract feed).
        // An empty frame — nothing moved its top — carries no views, so there is nothing to send.
        // The send errors only when there are no subscribers; the books are still maintained
        // locally, so ignore it.
        if !changed.is_empty() {
            let _ = area.updates.send(DepthUpdate {
                received_at,
                seq,
                views: changed,
            });
        }

        self.book_metrics.emit_if_due(&self.store);
        Ok(())
    }
}

/// The frame's books whose visible top moved, as fresh top-`depth` views, refreshing
/// `last_hashes` as it goes. Books whose update didn't move the top are skipped — a deeper
/// change, or a re-send of identical orders, still advances the revision but is nothing a
/// subscriber can see. Hashing the top-N is allocation-free; only a real change builds a view.
fn changed_views(
    store: &OrderBookStore,
    delivery_area_eic: EicCode,
    updates: &[BookUpdate],
    last_hashes: &mut HashMap<BookKey, u64>,
    depth: usize,
) -> Vec<DepthView> {
    let mut changed = Vec::new();
    for update in updates {
        let key = update.key;
        let book = store
            .book(delivery_area_eic, &key)
            .expect("book present after store.apply");
        let hash = book.top_hash(depth);
        if last_hashes.get(&key) == Some(&hash) {
            continue;
        }
        last_hashes.insert(key, hash);
        changed.push(DepthView::from_book(key, book, depth));
    }
    changed
}

/// Translate a protobuf `LocalView` payload into domain [`BookUpdate`]s — one per book in
/// the payload.
pub fn to_book_updates(payload: &LocalViewPayload) -> Vec<BookUpdate> {
    payload
        .local_views
        .iter()
        .map(|lv| BookUpdate {
            key: BookKey {
                market_type: map_market_type(lv.market_type),
                contract_id: lv.contract_id,
            },
            revision_no: lv.revision_no,
            buys: lv.buy_orders.iter().map(to_domain_order).collect(),
            sells: lv.sell_orders.iter().map(to_domain_order).collect(),
        })
        .collect()
}

fn to_domain_order(o: &crate::proto::pmd::Order) -> Order {
    Order {
        id: o.order_id,
        price: o.price,
        quantity: o.quantity,
        created_at: from_wire_millis(o.created_at_ms),
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use nordpool_marketdata::{
        BookKey, BookUpdate, EicCode, MarketType, Order as DomainOrder, OrderBookStore,
    };
    use nordpool_stream_server::DepthChannels;

    use chrono::{DateTime, Utc};

    use super::{LocalViewSink, to_book_updates};
    use crate::proto::pmd::server_frame::Payload as ServerPayload;
    use crate::proto::pmd::{LocalView, LocalViewPayload};
    use crate::sink::{FrameSink, ts};

    /// Broadcast `capacity` for the test channels: more messages than any test publishes, so no
    /// subscriber lags.
    const BROADCAST_CAPACITY: usize = 64;
    /// `book_state_period` no test reaches, keeping the periodic gauges out of the way.
    const BOOK_STATE_PERIOD: Duration = Duration::from_secs(3600);
    /// `depth`: price levels per side the sink projects — the live feed's default `[server] depth`.
    const DEPTH: usize = 20;

    fn received_at() -> DateTime<Utc> {
        DateTime::UNIX_EPOCH
    }

    fn lv(contract_id: i64, buys: Vec<crate::proto::pmd::Order>) -> LocalView {
        LocalView {
            market_type: 1,
            contract_id,
            revision_no: 1,
            buy_orders: buys,
            sell_orders: vec![],
        }
    }

    fn proto_order(id: i64, qty: i32) -> crate::proto::pmd::Order {
        crate::proto::pmd::Order {
            order_id: id,
            price: 100,
            quantity: qty,
            created_at_ms: 0,
        }
    }

    fn frame(is_snapshot: bool, views: Vec<LocalView>) -> ServerPayload {
        frame_in(40, is_snapshot, views)
    }

    fn frame_in(delivery_area_id: i32, is_snapshot: bool, views: Vec<LocalView>) -> ServerPayload {
        ServerPayload::LocalView(LocalViewPayload {
            is_snapshot,
            delivery_area_id,
            local_views: views,
        })
    }

    #[test]
    fn to_book_updates_translates_each_local_view_into_its_own_book() {
        // Given a snapshot payload carrying two distinct LocalViews (contracts 111 and 222),
        // the first with two bids on the same price and the second with one — each LocalView
        // is meant to become its own book keyed by contract id.
        let mut store = OrderBookStore::new();
        let payload = LocalViewPayload {
            is_snapshot: true,
            delivery_area_id: 40,
            local_views: vec![
                lv(111, vec![proto_order(1, 5), proto_order(2, 3)]),
                lv(222, vec![proto_order(3, 1)]),
            ],
        };

        // When the payload is translated and folded into the store as a snapshot — the protocol's
        // "here is the full current state" frame that establishes the book set.
        store.apply(
            EicCode::AREA_FR,
            &to_book_updates(&payload),
            payload.is_snapshot,
            ts(0),
        );

        // Then each LocalView produced exactly one book (two total), and contract 111's best bid
        // aggregates both of its orders into a single price level (order_count 2) — confirming the
        // two same-price orders landed in the same book, not split apart.
        assert_eq!(store.book_count(), 2);
        assert_eq!(
            store
                .book(
                    EicCode::AREA_FR,
                    &BookKey {
                        market_type: MarketType::Local,
                        contract_id: 111,
                    }
                )
                .unwrap()
                .best_bid()
                .map(|level| level.order_count),
            Some(2)
        );
    }

    #[test]
    fn to_book_updates_tracks_every_market_type() {
        // Given a snapshot payload mixing both wire market types in one frame: an XBID LocalView
        // (market_type 2) and a Local one (the helper's default market_type 1) — a frame can carry
        // books of either type, and the key's market_type must follow each view.
        let mut store = OrderBookStore::new();
        let xbid = LocalView {
            market_type: 2,
            ..lv(111, vec![proto_order(1, 5)])
        };
        let payload = LocalViewPayload {
            is_snapshot: true,
            delivery_area_id: 40,
            local_views: vec![xbid, lv(222, vec![proto_order(2, 3)])],
        };

        // When the mixed-type payload is translated and applied as a snapshot.
        store.apply(
            EicCode::AREA_FR,
            &to_book_updates(&payload),
            payload.is_snapshot,
            ts(0),
        );

        // Then both views became books (two total) and the XBID view is keyed under
        // MarketType::Xbid — proving the wire market_type was mapped per view, not assumed uniform.
        assert_eq!(store.book_count(), 2);
        assert!(
            store
                .book(
                    EicCode::AREA_FR,
                    &BookKey {
                        market_type: MarketType::Xbid,
                        contract_id: 111,
                    }
                )
                .is_some()
        );
    }

    #[test]
    fn to_book_updates_translates_payload_to_domain() {
        // Given a minimal one-view payload (contract 111, a single bid at price 100 qty 5) — the
        // smallest case that exercises every field of the wire-to-domain translation.
        let payload = LocalViewPayload {
            is_snapshot: true,
            delivery_area_id: 40,
            local_views: vec![lv(111, vec![proto_order(1, 5)])],
        };

        // When the protobuf payload is converted to domain updates.
        let updates = to_book_updates(&payload);

        // Then the produced BookUpdate matches field-for-field: the key carries the view's
        // market_type/contract_id, revision_no carries over, buys hold the translated order and
        // sells are empty — pinning the exact mapping.
        assert_eq!(
            updates,
            vec![BookUpdate {
                key: BookKey {
                    market_type: MarketType::Local,
                    contract_id: 111,
                },
                revision_no: 1,
                buys: vec![DomainOrder {
                    id: 1,
                    price: 100,
                    quantity: 5,
                    created_at: ts(0),
                }],
                sells: vec![],
            }]
        );
    }

    #[test]
    fn handle_records_changed_books_in_the_seed_map_and_broadcasts() {
        // Given a sink serving area 40 and a reader-side channel clone (as the gRPC service holds),
        // sharing that area's seed map and a depth subscriber.
        let channel = DepthChannels::new(BROADCAST_CAPACITY, &[EicCode::AREA_FR]);
        let reader = channel.clone();
        let mut rx = reader.area(EicCode::AREA_FR).unwrap().updates.subscribe();
        let mut sink = LocalViewSink::new(channel, BOOK_STATE_PERIOD, DEPTH);

        // When a snapshot frame with two books, received 5s after the epoch, is handled.
        sink.handle(
            &frame(
                true,
                vec![
                    lv(111, vec![proto_order(1, 5)]),
                    lv(222, vec![proto_order(2, 3)]),
                ],
            ),
            b"",
            received_at() + Duration::from_secs(5),
        )
        .unwrap();

        // Then both books land in the seed map (so a future subscriber can be seeded) AND both are
        // broadcast to the live subscriber — the two delivery paths a depth client depends on, and
        // map-before-broadcast means a client is covered whichever one it catches. The frame's
        // receive time and seq are stamped with them so the next seed can carry its freshness and
        // the broadcast batch can be matched against it.
        let seed = reader.area(EicCode::AREA_FR).unwrap().seed.lock().unwrap();
        assert_eq!(seed.len(), 2);
        assert_eq!(seed.received_at(), Some(ts(5_000_000_000)));
        assert_eq!(seed.seq(), 1);
        drop(seed);
        let mut broadcast: Vec<i64> = Vec::new();
        while let Ok(batch) = rx.try_recv() {
            broadcast.extend(batch.views.iter().map(|s| s.key.contract_id));
        }
        broadcast.sort();
        assert_eq!(broadcast, vec![111, 222]);
    }

    #[test]
    fn handle_keeps_delivery_areas_independent() {
        // Given a sink serving two areas (FR, wire id 40, and CH, wire id 41), with a subscriber
        // on each area's broadcast —
        // the same contract id 111 trades in both, at different prices, so it must reconstruct as
        // two distinct books, one per area, never colliding.
        let channel = DepthChannels::new(BROADCAST_CAPACITY, &[EicCode::AREA_FR, EicCode::AREA_CH]);
        let reader = channel.clone();
        let mut rx_fr = reader.area(EicCode::AREA_FR).unwrap().updates.subscribe();
        let mut rx_ch = reader.area(EicCode::AREA_CH).unwrap().updates.subscribe();
        let mut sink = LocalViewSink::new(channel, BOOK_STATE_PERIOD, DEPTH);

        // When a snapshot for area 40/FR (contract 111 @ price via proto_order, qty 5) and a
        // separate snapshot for area 41/CH (contract 111, qty 9) are each handled.
        sink.handle(
            &frame_in(40, true, vec![lv(111, vec![proto_order(1, 5)])]),
            b"",
            received_at(),
        )
        .unwrap();
        sink.handle(
            &frame_in(41, true, vec![lv(111, vec![proto_order(2, 9)])]),
            b"",
            received_at(),
        )
        .unwrap();

        // Then each area's seed map holds its own book 111 with its own depth (qty 5 vs 9), and each
        // area's broadcast carried only that area's change — areas don't leak into one another.
        let key = BookKey {
            market_type: MarketType::Local,
            contract_id: 111,
        };
        let quantity = |area: EicCode| {
            reader
                .area(area)
                .unwrap()
                .seed
                .lock()
                .unwrap()
                .get(&key)
                .unwrap()
                .bids[0]
                .total_quantity
        };
        assert_eq!(quantity(EicCode::AREA_FR), 5);
        assert_eq!(quantity(EicCode::AREA_CH), 9);
        assert_eq!(rx_fr.try_recv().unwrap().views[0].bids[0].total_quantity, 5);
        assert_eq!(rx_ch.try_recv().unwrap().views[0].bids[0].total_quantity, 9);
        // Neither area's broadcast saw a second message (the other area's frame).
        assert!(rx_fr.try_recv().is_err());
        assert!(rx_ch.try_recv().is_err());
    }

    #[test]
    fn handle_drops_frames_for_an_unserved_delivery_area() {
        // Given a sink serving only area 40/FR, with a subscriber on that area's broadcast. The
        // frame's delivery area comes off the wire, so the sink must tolerate one it never
        // subscribed to (a served-elsewhere id or one with no known EIC at all — a server
        // anomaly either way) rather than panic and crash-loop the connector.
        let channel = DepthChannels::new(BROADCAST_CAPACITY, &[EicCode::AREA_FR]);
        let reader = channel.clone();
        let mut rx = reader.area(EicCode::AREA_FR).unwrap().updates.subscribe();
        let mut sink = LocalViewSink::new(channel, BOOK_STATE_PERIOD, DEPTH);

        // When a snapshot arrives for the known-but-unserved area 42/AT, one for id 99 (no such
        // EIC), then a normal snapshot for area 40.
        sink.handle(
            &frame_in(42, true, vec![lv(111, vec![proto_order(1, 5)])]),
            b"",
            received_at(),
        )
        .expect("unserved-area frame must be dropped, not fail the sink");
        sink.handle(
            &frame_in(99, true, vec![lv(111, vec![proto_order(1, 5)])]),
            b"",
            received_at(),
        )
        .expect("unknown-id frame must be dropped, not fail the sink");
        sink.handle(
            &frame_in(40, true, vec![lv(222, vec![proto_order(2, 3)])]),
            b"",
            received_at(),
        )
        .unwrap();

        // Then the dropped frames left no trace — area 40's seed holds only its own book and
        // its broadcast carried only the area-40 batch — and the sink kept working after the
        // drops.
        let area = reader.area(EicCode::AREA_FR).unwrap();
        let seed = area.seed.lock().unwrap();
        assert_eq!(seed.len(), 1);
        assert!(seed.iter().all(|v| v.key.contract_id == 222));
        assert_eq!(rx.try_recv().unwrap().views[0].key.contract_id, 222);
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn handle_suppresses_updates_that_dont_move_the_top() {
        // Given a sink with one book established by a snapshot.
        let channel = DepthChannels::new(BROADCAST_CAPACITY, &[EicCode::AREA_FR]);
        let mut rx = channel.area(EicCode::AREA_FR).unwrap().updates.subscribe();
        let mut sink = LocalViewSink::new(channel, BOOK_STATE_PERIOD, DEPTH);
        sink.handle(
            &frame(true, vec![lv(111, vec![proto_order(1, 5)])]),
            b"",
            received_at(),
        )
        .unwrap();
        rx.try_recv().expect("view broadcast");

        // When a delta re-sends the identical order, so the book's revision advances but its
        // top-N is byte-for-byte the same — the case top_hash suppression exists to catch.
        sink.handle(
            &frame(false, vec![lv(111, vec![proto_order(1, 5)])]),
            b"",
            received_at(),
        )
        .unwrap();

        // Then nothing is broadcast: the unchanged top_hash matches last_hashes, so the sink skips
        // building and sending a redundant view, sparing every subscriber a no-op message.
        assert!(rx.try_recv().is_err());
    }

    #[test]
    fn handle_snapshot_prunes_stale_books_from_the_seed_map() {
        // Given a seed map populated by an initial snapshot of two books (contracts 111 and 222) —
        // the state a subscriber would be seeded from before the next snapshot arrives.
        let channel = DepthChannels::new(BROADCAST_CAPACITY, &[EicCode::AREA_FR]);
        let reader = channel.clone();
        let _rx = reader.area(EicCode::AREA_FR).unwrap().updates.subscribe();
        let mut sink = LocalViewSink::new(channel, BOOK_STATE_PERIOD, DEPTH);
        sink.handle(
            &frame(
                true,
                vec![
                    lv(111, vec![proto_order(1, 5)]),
                    lv(222, vec![proto_order(2, 3)]),
                ],
            ),
            b"",
            received_at(),
        )
        .unwrap();

        // When a fresh snapshot arrives that mentions only contract 111 — meaning 222 no longer
        // trades; a snapshot is the full current state, so anything absent has gone away.
        sink.handle(
            &frame(true, vec![lv(111, vec![proto_order(1, 5)])]),
            b"",
            received_at(),
        )
        .unwrap();

        // Then the seed map holds only 111: the snapshot cleared the map first so the now-stale 222
        // is pruned, preventing a new subscriber from being seeded a book that no longer exists.
        let area = reader.area(EicCode::AREA_FR).unwrap();
        let seed = area.seed.lock().unwrap();
        assert_eq!(seed.len(), 1);
        assert!(seed.iter().all(|v| v.key.contract_id == 111));
    }

    #[test]
    fn handle_counts_total_orders_per_frame_by_kind() {
        use metrics_util::debugging::{DebugValue, DebuggingRecorder};

        let recorder = DebuggingRecorder::new();
        let snapshotter = recorder.snapshotter();

        // When two frames are handled in sequence: a snapshot carrying 3 orders spread across two
        // books (2 + 1), then a delta carrying 1 — the order-entry count is the sum over both
        // sides of every book in a frame, and it must be attributed to the frame's kind.
        metrics::with_local_recorder(&recorder, || {
            let channel = DepthChannels::new(BROADCAST_CAPACITY, &[EicCode::AREA_FR]);
            let _rx = channel.area(EicCode::AREA_FR).unwrap().updates.subscribe();
            let mut sink = LocalViewSink::new(channel, BOOK_STATE_PERIOD, DEPTH);
            sink.handle(
                &frame(
                    true,
                    vec![
                        lv(111, vec![proto_order(1, 5), proto_order(2, 3)]),
                        lv(222, vec![proto_order(3, 1)]),
                    ],
                ),
                b"",
                received_at(),
            )
            .unwrap();
            sink.handle(
                &frame(false, vec![lv(111, vec![proto_order(4, 2)])]),
                b"",
                received_at(),
            )
            .unwrap();
        });

        // Then nordpool.book.order_updates holds 3 under kind=snapshot and 1 under kind=delta — the
        // totals are tallied separately per frame kind, so dashboards can distinguish full-state
        // replays from incremental churn.
        let snapshot = snapshotter.snapshot().into_vec();
        let updates = |kind: &str| {
            let (_, _, _, value) = snapshot
                .iter()
                .find(|(key, ..)| {
                    key.key().name() == "nordpool.book.order_updates"
                        && key.key().labels().any(|l| l.value() == kind)
                })
                .unwrap_or_else(|| panic!("updates counter for kind {kind} not recorded"));
            match value {
                DebugValue::Counter(v) => *v,
                other => panic!("expected counter, got {other:?}"),
            }
        };
        assert_eq!(updates("snapshot"), 3);
        assert_eq!(updates("delta"), 1);
    }
}
