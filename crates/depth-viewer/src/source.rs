//! The source abstraction. Live and mock both reduce to the same thing — a stream of
//! [`TimestampedSnapshot`] — which the app pumps into per-contract segment books.
//!
//! Transport is gRPC-Web over `fetch` (`tonic-web-wasm-client`); the browser event loop is
//! single-threaded with no tokio reactor, so streams are thread-local and timers are browser
//! timers. (There is no replay path — the browser has no filesystem.)

use crate::model::{ContractMeta, Level, ProductType, StatPoint, StatSample, TimestampedSnapshot};
use anyhow::{Context, Result};
use futures::StreamExt;
use futures::stream;
use web_time::{SystemTime, UNIX_EPOCH};

use crate::marketdata::contract_service_client::ContractServiceClient;
use crate::marketdata::depth_book_service_client::DepthBookServiceClient;
use crate::marketdata::public_statistic_service_client::PublicStatisticServiceClient;
use crate::marketdata::{
    DepthView, MarketType, SubscribeContractsRequest, SubscribeDepthRequest,
    SubscribeStatisticsRequest,
};

/// The boxed stream type the source yields. Thread-local (not `Send`): gRPC-Web streams run on the
/// browser's single-threaded event loop.
pub type DynStream<T> = futures::stream::LocalBoxStream<'static, T>;

pub type DepthStream = DynStream<TimestampedSnapshot>;
pub type ContractStream = DynStream<Vec<ContractMeta>>;
pub type StatStream = DynStream<Vec<StatSample>>;

/// Box a stream into [`DynStream`].
pub(crate) fn platform_box<S>(s: S) -> DynStream<S::Item>
where
    S: futures::Stream + 'static,
{
    s.boxed_local()
}

/// Sleep for `d` on the browser's timer (no tokio reactor exists under wasm).
pub(crate) async fn sleep(d: std::time::Duration) {
    gloo_timers::future::TimeoutFuture::new(d.as_millis() as u32).await;
}

/// Connection state of the live source, pushed to the UI so it can show a status light. Mock
/// reports `Connected` while its stream runs and `Disconnected` once it ends.
#[derive(Clone, Copy, PartialEq)]
pub enum ConnStatus {
    Connecting,
    Connected,
    Disconnected,
}

/// What the background source pushes to the UI: depth snapshots, (slowly) contract reference
/// data, and connection-state transitions. Merged onto one channel so the app drives a single loop.
pub enum Update {
    Snapshot(TimestampedSnapshot),
    Contracts(Vec<ContractMeta>),
    Statistics(Vec<StatSample>),
    Status(ConnStatus),
}

/// Which source the app opens.
#[derive(Clone)]
pub enum SourceKind {
    /// Subscribe to the connector's stream-server over gRPC-Web. `eic_code` guards both
    /// streams — it must name an area the server serves, else the subscribe is rejected.
    Live { endpoint: String, eic_code: String },
    /// Synthetic walking book, for running standalone with no server.
    Mock,
}

/// Epoch milliseconds for a wire `google.protobuf.Timestamp` — the viewer's whole model is in
/// milliseconds (chart x-axis, delivery windows), so every time field converts on arrival.
fn ms(t: &prost_types::Timestamp) -> i64 {
    t.seconds * 1_000 + i64::from(t.nanos) / 1_000_000
}

/// Current wall-clock in epoch milliseconds.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

pub async fn open_stream(kind: SourceKind) -> Result<DepthStream> {
    match kind {
        SourceKind::Live { endpoint, eic_code } => live_stream(endpoint, eic_code).await,
        SourceKind::Mock => Ok(mock_stream()),
    }
}

/// Open the contract reference-data feed. Live streams `SubscribeContracts`; mock synthesizes a
/// single contract.
pub async fn open_contracts(kind: SourceKind) -> Result<Option<ContractStream>> {
    match kind {
        SourceKind::Live { endpoint, eic_code } => {
            Ok(Some(live_contracts(endpoint, eic_code).await?))
        }
        SourceKind::Mock => Ok(Some(mock_contracts())),
    }
}

/// Open the public-statistic feed. Live streams `SubscribeStatistics`; mock synthesizes a slowly
/// wandering day-ahead price and VWAP for the mock contract.
pub async fn open_statistics(kind: SourceKind) -> Result<Option<StatStream>> {
    match kind {
        SourceKind::Live { endpoint, eic_code } => {
            Ok(Some(live_statistics(endpoint, eic_code).await?))
        }
        SourceKind::Mock => Ok(Some(mock_statistics())),
    }
}

/// Open a depth client over gRPC-Web (`fetch`). The generated client is generic over the
/// transport, so every call site after this is the ordinary tonic client API.
fn connect_depth(endpoint: String) -> DepthBookServiceClient<tonic_web_wasm_client::Client> {
    DepthBookServiceClient::new(tonic_web_wasm_client::Client::new(endpoint))
}

fn connect_contracts(endpoint: String) -> ContractServiceClient<tonic_web_wasm_client::Client> {
    ContractServiceClient::new(tonic_web_wasm_client::Client::new(endpoint))
}

fn connect_statistics(
    endpoint: String,
) -> PublicStatisticServiceClient<tonic_web_wasm_client::Client> {
    PublicStatisticServiceClient::new(tonic_web_wasm_client::Client::new(endpoint))
}

/// Map the proto contract enums to the pure model. A streamed contract carries its full
/// delivery-area state list, so `active` is judged in the viewer's one area, not anywhere.
fn meta_from_proto(c: &crate::marketdata::Contract, eic_code: &str) -> ContractMeta {
    use crate::marketdata::{ContractState, ProductType as Pt};
    let product_type = match crate::marketdata::ProductType::try_from(c.product_type) {
        Ok(Pt::P15Min) => ProductType::P15,
        Ok(Pt::P30Min) => ProductType::P30,
        Ok(Pt::P60Min) => ProductType::P60,
        _ => ProductType::Other,
    };
    let active = c.delivery_areas.iter().any(|a| {
        a.eic_code == eic_code && ContractState::try_from(a.state) == Ok(ContractState::Active)
    });
    ContractMeta {
        contract_id: c.contract_id,
        name: c.name.clone(),
        market_type: market_label(c.market_type),
        product_type,
        delivery_start_ms: c.delivery_start.as_ref().map(ms).unwrap_or_default(),
        delivery_end_ms: c.delivery_end.as_ref().map(ms).unwrap_or_default(),
        active,
    }
}

/// Stream `SubscribeContracts` filtered to the viewer's one area: the server sends the current
/// filtered set first, then each changed batch. Each `ContractUpdate` becomes one
/// `Vec<ContractMeta>` the app merges by id.
async fn live_contracts(endpoint: String, eic_code: String) -> Result<ContractStream> {
    let mut client = connect_contracts(endpoint);
    let stream = client
        .subscribe_contracts(SubscribeContractsRequest {
            eic_codes: vec![eic_code.clone()],
        })
        .await
        .context("subscribing to ContractService")?
        .into_inner();
    let s = stream.filter_map(move |result| {
        let eic_code = eic_code.clone();
        async move {
            match result {
                Ok(update) => Some(
                    update
                        .contracts
                        .iter()
                        .map(|c| meta_from_proto(c, &eic_code))
                        .collect(),
                ),
                Err(e) => {
                    tracing::warn!("SubscribeContracts stream error: {e}");
                    None
                }
            }
        }
    });
    Ok(platform_box(s))
}

/// One synthetic contract matching the mock book, so the panel has product/delivery data to show.
fn mock_contracts() -> ContractStream {
    let start = (now_ms() / 900_000 + 1) * 900_000; // next 15-min boundary
    let meta = ContractMeta {
        contract_id: 593028,
        name: "PH-20260618-13Q1".into(),
        market_type: "LOCAL".into(),
        product_type: ProductType::P15,
        delivery_start_ms: start,
        delivery_end_ms: start + 900_000,
        active: true,
    };
    platform_box(stream::once(async move { vec![meta] }))
}

/// Stream `SubscribeStatistics`: the server sends the full set first, then each changed batch. Each
/// `StatisticUpdate` becomes one `Vec<StatSample>` the app folds into per-contract step series. The
/// x-axis instant is the frame's `received_at`; on the seed (which carries none until the first
/// frame) it falls back to receipt wall-clock so the seed lands at "now" rather than the epoch.
async fn live_statistics(endpoint: String, eic_code: String) -> Result<StatStream> {
    let mut client = connect_statistics(endpoint);
    let stream = client
        .subscribe_statistics(SubscribeStatisticsRequest { eic_code })
        .await
        .context("subscribing to PublicStatisticService")?
        .into_inner();
    let s = stream.filter_map(|result| async move {
        match result {
            Ok(update) => {
                let t = update.received_at.as_ref().map(ms).unwrap_or_else(now_ms);
                Some(
                    update
                        .statistics
                        .iter()
                        .map(|s| StatSample {
                            contract_id: s.contract_id,
                            point: StatPoint {
                                t,
                                day_ahead_price: s.day_ahead_price,
                                vwap: s.vwap,
                                turnover: s.turnover,
                            },
                        })
                        .collect(),
                )
            }
            Err(e) => {
                tracing::warn!("SubscribeStatistics stream error: {e}");
                None
            }
        }
    });
    Ok(platform_box(s))
}

/// A synthetic statistic for the mock contract: a day-ahead price and VWAP that wander slowly around
/// the mock mid, emitted every couple of seconds so the chart has step lines to draw with no server.
fn mock_statistics() -> StatStream {
    const CONTRACT: i64 = 593028;
    let s = stream::unfold((0i64, 0i64), |(step, turnover)| async move {
        sleep(std::time::Duration::from_millis(2000)).await;
        let day_ahead = 5000 + ((step % 12) - 6) * 20;
        let vwap = 5000 + ((step % 8) - 4) * 12;
        let turnover = turnover + 100 + (step % 7) * 80; // cumulative; the per-step rise is volume
        let sample = StatSample {
            contract_id: CONTRACT,
            point: StatPoint {
                t: now_ms(),
                day_ahead_price: Some(day_ahead),
                vwap: Some(vwap),
                turnover: Some(turnover),
            },
        };
        Some((vec![sample], (step + 1, turnover)))
    });
    platform_box(s)
}

/// The mock book's mid, derived from wall-clock so the snapshot stream lands on band prices.
fn mock_mid() -> i64 {
    let tick = now_ms() / 250;
    5000 + ((tick / 4) % 40 - 20) * 10
}

/// Map the proto enum to the same short label depth-replay writes to Parquet.
fn market_label(raw: i32) -> String {
    match MarketType::try_from(raw) {
        Ok(MarketType::Local) => "LOCAL",
        Ok(MarketType::Xbid) => "XBID",
        _ => "UNKNOWN",
    }
    .to_string()
}

fn from_proto(snap: DepthView, t_ms: i64) -> TimestampedSnapshot {
    let level = |l: &crate::marketdata::Level| Level {
        price: l.price,
        quantity: l.total_quantity,
        order_count: l.order_count,
    };
    TimestampedSnapshot {
        t: t_ms,
        contract_id: snap.contract_id,
        revision: snap.revision,
        bids: snap.bids.iter().map(level).collect(),
        asks: snap.asks.iter().map(level).collect(),
    }
}

/// Subscribe to the served area's books and stamp each snapshot with receipt time — the
/// live feed carries no timestamp of its own.
async fn live_stream(endpoint: String, eic_code: String) -> Result<DepthStream> {
    let mut client = connect_depth(endpoint);
    let streaming = client
        .subscribe(SubscribeDepthRequest { eic_code })
        .await
        .context("opening Subscribe stream")?
        .into_inner();

    // Each message is a DepthUpdate batch (the seed, or one frame's changed books); flatten it into
    // one TimestampedSnapshot per book, stamping the whole batch with a single receipt time.
    let mapped = streaming.flat_map(|msg| {
        let t = now_ms();
        let snaps = msg
            .map(|update| {
                update
                    .views
                    .into_iter()
                    .map(|view| from_proto(view, t))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        stream::iter(snaps)
    });
    Ok(platform_box(mapped))
}

/// A synthetic two-sided book whose mid wanders, emitting a snapshot ~4×/sec so the app has
/// something live to draw without a server.
fn mock_stream() -> DepthStream {
    const CONTRACT: i64 = 593028;
    const DEPTH: i64 = 12;
    const TICK: i64 = 10; // price scaled by 100 → 0.10 per tick
    let s = stream::unfold(0i64, |step| async move {
        sleep(std::time::Duration::from_millis(250)).await;
        let mid = mock_mid(); // wall-clock derived
        let phase = step % 16;
        let qty = |k: i64| 1000 + ((step + k * 7) % 13) * 250 + phase * 50;
        let mut bids = Vec::new();
        let mut asks = Vec::new();
        for i in 1..=DEPTH {
            bids.push(Level {
                price: mid - i * TICK,
                quantity: qty(i),
                order_count: (1 + (i % 4)) as u32,
            });
            asks.push(Level {
                price: mid + i * TICK,
                quantity: qty(i + 3),
                order_count: (1 + ((i + 1) % 4)) as u32,
            });
        }
        let snap = TimestampedSnapshot {
            t: now_ms(),
            contract_id: CONTRACT,
            revision: step,
            bids,
            asks,
        };
        Some((snap, step + 1))
    });
    platform_box(s)
}
