//! The eframe app: a background tokio thread pumps the snapshot source into a channel; each
//! frame drains it into per-contract segment books, then draws the controls, the right panel,
//! and the central chart. The selected contract drives the chart.

use std::collections::HashMap;
use std::time::Duration;

use crate::model::{ContractMeta, ProductType, SegmentBook, Side, StatSeries, TimestampedSnapshot};
use anyhow::Result;
use crossbeam_channel::{Receiver, Sender};
use eframe::egui::text::LayoutJob;
use eframe::egui::{self, Color32, FontId, RichText, TextFormat};
use futures::stream::{self, StreamExt};

use crate::chart::{self, ChartView, PRICE_SCALE, QtyMode};
use crate::source::{
    ConnStatus, SourceKind, Update, now_ms, open_contracts, open_statistics, open_stream,
    platform_box, sleep,
};
use crate::timefmt;

const INITIAL_BACKOFF: Duration = Duration::from_millis(500);
const MAX_BACKOFF: Duration = Duration::from_secs(10);

const WINDOW_PRESETS: [(&str, f64); 6] = [
    ("10s", 10_000.0),
    ("1m", 60_000.0),
    ("5m", 300_000.0),
    ("15m", 900_000.0),
    ("1h", 3_600_000.0),
    ("6h", 21_600_000.0),
];

pub struct App {
    rx: Receiver<Update>,
    retention_ms: i64,
    books: HashMap<i64, SegmentBook>,
    latest: HashMap<i64, TimestampedSnapshot>,
    contracts: HashMap<i64, ContractMeta>,
    stats: HashMap<i64, StatSeries>,
    order: Vec<i64>, // discovered contract ids, sorted
    selected: Option<i64>,
    market_filter: MarketFilter,
    product_filter: ProductFilter,
    active_only: bool,
    view: ChartView,
    /// Depth snapshots received (one per book update from the depth feed).
    depth_received: u64,
    /// Contract reference-data updates received (one per `SubscribeContracts` batch).
    contracts_received: u64,
    /// Public-statistic updates received (one per `SubscribeStatistics` batch).
    stats_received: u64,
    /// Connection state of the source, driving the top-right status light.
    status: ConnStatus,
    /// Epoch-ms under the cursor on the chart last frame; drives the book ladder (None = live).
    hover_t: Option<i64>,
    /// (side, price) level the cursor is over in the book ladder; outlined in the chart.
    hover_level: Option<(Side, i64)>,
    // Rolling refresh/throughput rates shown in the top bar.
    fps: f32,
    ups: f32, // book updates per second
    sps: f32, // statistic updates per second
    rate_t0: f64,
    rate_n0: u64,
    rate_s0: u64,
}

#[derive(PartialEq, Clone, Copy)]
enum MarketFilter {
    All,
    Local,
    Xbid,
}

impl MarketFilter {
    /// `market` is the contract's market type from the contract feed, or `None` when the depth
    /// feed surfaced a book before its contract reference data arrived. An unknown market matches
    /// only `All`.
    fn matches(self, market: Option<&str>) -> bool {
        match self {
            MarketFilter::All => true,
            MarketFilter::Local => market == Some("LOCAL"),
            MarketFilter::Xbid => market == Some("XBID"),
        }
    }
    fn label(self) -> &'static str {
        match self {
            MarketFilter::All => "ALL",
            MarketFilter::Local => "LOCAL",
            MarketFilter::Xbid => "XBID",
        }
    }
}

#[derive(PartialEq, Clone, Copy)]
enum ProductFilter {
    All,
    Only(ProductType),
}

impl ProductFilter {
    /// Match against a contract's product, when known. A specific filter hides contracts whose
    /// reference data hasn't arrived yet (can't confirm the product).
    fn matches(self, product: Option<ProductType>) -> bool {
        match self {
            ProductFilter::All => true,
            ProductFilter::Only(p) => product == Some(p),
        }
    }
    fn label(self) -> &'static str {
        match self {
            ProductFilter::All => "ALL",
            ProductFilter::Only(p) => p.label(),
        }
    }
}

impl App {
    pub fn new(cc: &eframe::CreationContext<'_>, kind: SourceKind, retention_ms: i64) -> Self {
        let (tx, rx) = crossbeam_channel::unbounded();
        let ctx = cc.egui_ctx.clone();
        // The source is async; egui owns the main thread, so drive the streams on the browser
        // event loop (single-threaded, no tokio reactor) and wake the UI on each update. Snapshots
        // and (slow) contract reference data merge onto one channel.
        wasm_bindgen_futures::spawn_local(run_source(kind, tx, ctx));

        let view = ChartView::default();
        Self {
            rx,
            retention_ms,
            books: HashMap::new(),
            latest: HashMap::new(),
            contracts: HashMap::new(),
            stats: HashMap::new(),
            order: Vec::new(),
            selected: None,
            market_filter: MarketFilter::Xbid,
            product_filter: ProductFilter::Only(ProductType::P60),
            active_only: true,
            view,
            depth_received: 0,
            contracts_received: 0,
            stats_received: 0,
            status: ConnStatus::Connecting,
            hover_t: None,
            hover_level: None,
            fps: 0.0,
            ups: 0.0,
            sps: 0.0,
            rate_t0: 0.0,
            rate_n0: 0,
            rate_s0: 0,
        }
    }

    /// Smooth the frame rate (EMA) and sample the update rate over ~0.5s windows.
    fn tick_rates(&mut self, ctx: &egui::Context) {
        let (now, dt) = ctx.input(|i| (i.time, i.unstable_dt));
        if dt > 0.0 {
            let inst = 1.0 / dt;
            self.fps = if self.fps == 0.0 {
                inst
            } else {
                0.9 * self.fps + 0.1 * inst
            };
        }
        if self.rate_t0 == 0.0 {
            self.rate_t0 = now;
            self.rate_n0 = self.depth_received;
            self.rate_s0 = self.stats_received;
        }
        let elapsed = now - self.rate_t0;
        if elapsed >= 0.5 {
            self.ups = ((self.depth_received - self.rate_n0) as f64 / elapsed) as f32;
            self.sps = ((self.stats_received - self.rate_s0) as f64 / elapsed) as f32;
            self.rate_t0 = now;
            self.rate_n0 = self.depth_received;
            self.rate_s0 = self.stats_received;
        }
    }

    fn drain(&mut self) {
        while let Ok(update) = self.rx.try_recv() {
            match update {
                Update::Snapshot(snap) => self.apply_snapshot(snap),
                Update::Contracts(metas) => {
                    self.contracts_received += 1;
                    for m in metas {
                        self.contracts.insert(m.contract_id, m);
                    }
                }
                Update::Statistics(samples) => {
                    self.stats_received += 1;
                    for s in samples {
                        self.stats
                            .entry(s.contract_id)
                            .or_insert_with(|| StatSeries::new(self.retention_ms))
                            .push(s.point);
                    }
                }
                Update::Status(status) => self.status = status,
            }
        }
    }

    fn apply_snapshot(&mut self, snap: TimestampedSnapshot) {
        self.depth_received += 1;
        let id = snap.contract_id;
        if !self.books.contains_key(&id) {
            self.books.insert(id, SegmentBook::new(self.retention_ms));
            self.order.push(id);
            self.order.sort_unstable();
        }
        self.books.get_mut(&id).unwrap().apply(&snap);
        self.latest.insert(id, snap);
        if self.selected.is_none() {
            self.selected = Some(id);
        }
    }

    fn controls(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            for (label, ms) in WINDOW_PRESETS {
                let active = (self.view.ms_per_view - ms).abs() < 1.0;
                if ui.selectable_label(active, label).clicked() {
                    self.view.set_window_ms(ms);
                }
            }
            ui.separator();
            let live = self.view.follow_live;
            let btn = egui::Button::new(if live { "● Live" } else { "Go Live" }).fill(if live {
                chart_green()
            } else {
                ui.visuals().widgets.inactive.bg_fill
            });
            if ui.add(btn).clicked() {
                if live {
                    self.view.follow_live = false;
                } else {
                    self.view.go_live();
                }
            }
            ui.separator();
            ui.label("qty:");
            ui.selectable_value(&mut self.view.qty_mode, QtyMode::Auto, "auto");
            ui.selectable_value(&mut self.view.qty_mode, QtyMode::Fixed, "fixed");
            if self.view.qty_mode == QtyMode::Fixed {
                ui.add(
                    egui::DragValue::new(&mut self.view.fixed_unit_per_px)
                        .speed(1.0)
                        .range(1.0..=10_000.0)
                        .prefix("qty/px "),
                );
            }
            ui.separator();
            ui.label("mid qty:");
            ui.add(
                egui::DragValue::new(&mut self.view.mid_depth_qty)
                    .speed(10.0)
                    .range(1.0..=1_000_000.0),
            );
            ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
                self.conn_indicator(ui);
                ui.separator();
                ui.label(
                    RichText::new(format!(
                        "{} depth · {} contract · {} stat · {:.0} depth/s · {:.1} stat/s · {:.0} fps",
                        self.depth_received,
                        self.contracts_received,
                        self.stats_received,
                        self.ups,
                        self.sps,
                        self.fps
                    ))
                    .color(AXIS),
                );
            });
        });
    }

    /// Top-right connection light: a coloured dot plus a word — green/Connected, amber/Connecting,
    /// red/Disconnected. In a right-to-left layout the dot lands hard against the corner.
    fn conn_indicator(&self, ui: &mut egui::Ui) {
        let (color, text) = match self.status {
            ConnStatus::Connected => (chart_green(), "Connected"),
            ConnStatus::Connecting => (AMBER, "Connecting…"),
            ConnStatus::Disconnected => (chart_red(), "Disconnected"),
        };
        let (rect, _) = ui.allocate_exact_size(egui::vec2(12.0, 12.0), egui::Sense::hover());
        ui.painter().circle_filled(rect.center(), 5.0, color);
        ui.label(RichText::new(text).color(color).strong());
    }

    fn right_panel(&mut self, ui: &mut egui::Ui) {
        ui.heading(RichText::new("CONTRACT").size(12.0).color(AXIS));
        ui.horizontal(|ui| {
            ui.label(RichText::new("market").color(AXIS).size(11.0));
            for f in [MarketFilter::All, MarketFilter::Local, MarketFilter::Xbid] {
                if ui
                    .selectable_label(self.market_filter == f, f.label())
                    .clicked()
                {
                    self.market_filter = f;
                }
            }
        });
        ui.horizontal(|ui| {
            ui.label(RichText::new("product").color(AXIS).size(11.0));
            for f in [
                ProductFilter::All,
                ProductFilter::Only(ProductType::P15),
                ProductFilter::Only(ProductType::P30),
                ProductFilter::Only(ProductType::P60),
            ] {
                if ui
                    .selectable_label(self.product_filter == f, f.label())
                    .clicked()
                {
                    self.product_filter = f;
                }
            }
        });
        ui.checkbox(&mut self.active_only, "active only");
        ui.separator();

        // Keep each contract on one line (no wrap); rows wider than the panel scroll/clip.
        ui.scope(|ui| {
            ui.style_mut().wrap_mode = Some(egui::TextWrapMode::Extend);
            // Disabled selectable so the header's left padding matches the selectable rows below.
            ui.add_enabled(false, egui::SelectableLabel::new(false, contract_header()));
            egui::ScrollArea::vertical()
                .id_salt("contracts")
                .max_height(ui.available_height() * 0.5)
                .show(ui, |ui| {
                    for id in &self.order {
                        let Some(snap) = self.latest.get(id) else {
                            continue;
                        };
                        let meta = self.contracts.get(id);
                        if !self
                            .market_filter
                            .matches(meta.map(|m| m.market_type.as_str()))
                        {
                            continue;
                        }
                        let product = meta.map(|m| m.product_type);
                        if !self.product_filter.matches(product) {
                            continue;
                        }
                        if self.active_only && !meta.is_some_and(|m| m.active) {
                            continue;
                        }
                        let selected = self.selected == Some(*id);
                        if ui
                            .selectable_label(selected, contract_row(*id, snap, meta))
                            .clicked()
                            && self.selected != Some(*id)
                        {
                            self.selected = Some(*id);
                            // Re-fit the price axis to the newly selected contract.
                            self.view.price_inited = false;
                        }
                    }
                });
        });

        ui.separator();
        ui.heading(RichText::new("DETAILS").size(12.0).color(AXIS));
        match self.selected.and_then(|id| self.latest.get(&id)) {
            Some(snap) => {
                let meta = self.selected.and_then(|id| self.contracts.get(&id));
                egui::Grid::new("details").num_columns(2).show(ui, |ui| {
                    row(ui, "Contract", &snap.contract_id.to_string());
                    if let Some(m) = meta {
                        row(ui, "Name", &m.name);
                        row(ui, "Product", m.product_type.label());
                        row(
                            ui,
                            "Delivery",
                            &timefmt::delivery_window(m.delivery_start_ms, m.delivery_end_ms),
                        );
                        row(ui, "State", if m.active { "active" } else { "inactive" });
                    }
                    row(
                        ui,
                        "Market",
                        meta.map(|m| m.market_type.as_str()).unwrap_or("—"),
                    );
                    row(ui, "Revision", &snap.revision.to_string());
                    let bb = snap.bids.first();
                    let ba = snap.asks.first();
                    ui.label("Best bid");
                    ui.colored_label(
                        chart_green(),
                        bb.map(|l| format!("{} @ {:.2}", l.quantity, l.price as f64 / PRICE_SCALE))
                            .unwrap_or_else(|| "—".into()),
                    );
                    ui.end_row();
                    ui.label("Best ask");
                    ui.colored_label(
                        chart_red(),
                        ba.map(|l| format!("{} @ {:.2}", l.quantity, l.price as f64 / PRICE_SCALE))
                            .unwrap_or_else(|| "—".into()),
                    );
                    ui.end_row();
                    row(
                        ui,
                        "Levels",
                        &format!("{} / {}", snap.bids.len(), snap.asks.len()),
                    );
                });
            }
            None => {
                ui.label(RichText::new("waiting for data…").color(AXIS));
            }
        }

        ui.separator();
        self.book_ladder(ui);
    }

    /// A classic depth ladder for the selected book: bids on the left (best on top), asks on the
    /// right (best on top). Shows the book at the chart's hovered instant, else the live state.
    fn book_ladder(&mut self, ui: &mut egui::Ui) {
        let Some(book) = self.selected.and_then(|id| self.books.get(&id)) else {
            self.hover_level = None;
            return;
        };
        let live = self.hover_t.is_none_or(|h| h >= book.last_t);
        let t = if live {
            book.last_t
        } else {
            self.hover_t.unwrap()
        };
        let state = book.at(t);
        // The level highlighted coming in (set last frame by the chart or the ladder) marks the row
        // to shade; the row the cursor is on this frame becomes the new outgoing highlight.
        let highlight = self.hover_level;
        let mut hover_level = None;

        ui.horizontal(|ui| {
            ui.heading(RichText::new("BOOK").size(12.0).color(AXIS));
            let caption = if live {
                "live".to_string()
            } else {
                format!("@ {}", timefmt::hms(t))
            };
            ui.label(RichText::new(caption).size(11.0).color(AXIS));
        });

        let cell = |ui: &mut egui::Ui, text: String, color: Color32, hl: bool| {
            let mut rt = RichText::new(text).monospace().color(color);
            if hl {
                rt = rt.background_color(LADDER_HL);
            }
            ui.add(egui::Label::new(rt).sense(egui::Sense::hover()))
        };
        egui::ScrollArea::vertical()
            .id_salt("ladder")
            .show(ui, |ui| {
                egui::Grid::new("ladder")
                    .num_columns(4)
                    .striped(true)
                    .show(ui, |ui| {
                        for h in ["bid qty", "bid", "ask", "ask qty"] {
                            ui.label(RichText::new(h).size(10.0).color(AXIS));
                        }
                        ui.end_row();
                        let price = |p: i64| format!("{:.2}", p as f64 / PRICE_SCALE);
                        for i in 0..state.bids.len().max(state.asks.len()) {
                            match state.bids.get(i) {
                                Some(&(p, q)) => {
                                    let hl = highlight == Some((Side::Bid, p));
                                    let r = cell(ui, q.to_string(), chart_green(), hl)
                                        | cell(ui, price(p), chart_green(), hl);
                                    if r.hovered() {
                                        hover_level = Some((Side::Bid, p));
                                    }
                                }
                                None => {
                                    ui.label("");
                                    ui.label("");
                                }
                            }
                            match state.asks.get(i) {
                                Some(&(p, q)) => {
                                    let hl = highlight == Some((Side::Ask, p));
                                    let r = cell(ui, price(p), chart_red(), hl)
                                        | cell(ui, q.to_string(), chart_red(), hl);
                                    if r.hovered() {
                                        hover_level = Some((Side::Ask, p));
                                    }
                                }
                                None => {
                                    ui.label("");
                                    ui.label("");
                                }
                            }
                            ui.end_row();
                        }
                    });
            });

        self.hover_level = hover_level;
    }
}

/// Drive the source forever: connect, pump until the stream ends, report the status transitions,
/// and (for the live source) keep retrying with exponential backoff. Replay/mock run once.
async fn run_source(kind: SourceKind, tx: Sender<Update>, ctx: egui::Context) {
    let reconnect = matches!(kind, SourceKind::Live { .. });
    let mut backoff = INITIAL_BACKOFF;
    loop {
        let _ = tx.send(Update::Status(ConnStatus::Connecting));
        ctx.request_repaint();
        match pump_once(&kind, &tx, &ctx).await {
            // A clean end means we did connect, so reconnect promptly rather than backing off.
            Ok(()) => {
                tracing::info!("source stream ended");
                backoff = INITIAL_BACKOFF;
            }
            Err(e) => tracing::warn!("source connection failed: {e:#}"),
        }
        let _ = tx.send(Update::Status(ConnStatus::Disconnected));
        ctx.request_repaint();
        if !reconnect {
            break;
        }
        sleep(backoff).await;
        backoff = (backoff * 2).min(MAX_BACKOFF);
    }
}

/// Internal item of the merged pump stream: a UI update to forward, or the marker that the primary
/// depth stream has ended.
enum Pump {
    Update(Update),
    DepthEnded,
}

/// One connect-and-pump attempt. Opening the depth stream is the connection signal — its failure
/// returns `Err` (driving a reconnect); the contract feed is best-effort. The depth stream ending
/// (a clean server close, or a drop after the subscriber lagged) ends the pump so the caller
/// reconnects — even while the secondary contract feed is still live, which `stream::select` alone
/// would let keep the merge running forever. A `DepthEnded` marker chained onto the depth stream
/// carries that end through the merge.
async fn pump_once(kind: &SourceKind, tx: &Sender<Update>, ctx: &egui::Context) -> Result<()> {
    let snaps = platform_box(
        open_stream(kind.clone())
            .await?
            .map(|s| Pump::Update(Update::Snapshot(s)))
            .chain(stream::once(async { Pump::DepthEnded })),
    );
    let contracts = match open_contracts(kind.clone()).await {
        Ok(Some(c)) => platform_box(c.map(|c| Pump::Update(Update::Contracts(c)))),
        Ok(None) => platform_box(stream::empty::<Pump>()),
        Err(e) => {
            tracing::warn!("contract feed error: {e:#}");
            platform_box(stream::empty::<Pump>())
        }
    };
    let statistics = match open_statistics(kind.clone()).await {
        Ok(Some(s)) => platform_box(s.map(|s| Pump::Update(Update::Statistics(s)))),
        Ok(None) => platform_box(stream::empty::<Pump>()),
        Err(e) => {
            tracing::warn!("statistic feed error: {e:#}");
            platform_box(stream::empty::<Pump>())
        }
    };
    let _ = tx.send(Update::Status(ConnStatus::Connected));
    ctx.request_repaint();
    let mut merged = stream::select(snaps, stream::select(contracts, statistics));
    while let Some(item) = merged.next().await {
        match item {
            Pump::Update(update) => {
                if tx.send(update).is_err() {
                    return Ok(()); // UI gone
                }
                ctx.request_repaint();
            }
            Pump::DepthEnded => return Ok(()),
        }
    }
    Ok(())
}

// Fixed monospace column widths (in chars) so list rows line up under the header.
const COL_ID: usize = 8;
const COL_PROD: usize = 4;
const COL_DELIV: usize = 12;
const COL_QP: usize = 12; // qty@price

fn list_font() -> FontId {
    FontId::monospace(12.0)
}

/// The contract-list header, padded to the same columns as the rows.
fn contract_header() -> LayoutJob {
    let mut job = LayoutJob::default();
    let f = list_font();
    for (w, h) in [
        (COL_ID, "id"),
        (COL_PROD, "prod"),
        (COL_DELIV, "delivery"),
        (COL_QP, "bid"),
        (COL_QP, "ask"),
    ] {
        job.append(&format!("{h:<w$}"), 0.0, fmt(f.clone(), AXIS));
    }
    job.append("lvls", 0.0, fmt(f, AXIS));
    job
}

/// A one-line contract list entry: id · product · delivery (Paris) · best bid / best ask
/// (qty@price) · level counts. Reference fields show "—" until contract data arrives.
fn contract_row(id: i64, snap: &TimestampedSnapshot, meta: Option<&ContractMeta>) -> LayoutJob {
    let f = list_font();
    let fg = Color32::from_rgb(0xcf, 0xd6, 0xdf);

    let product = meta.map(|m| m.product_type.label()).unwrap_or("—");
    let window = meta
        .map(|m| timefmt::delivery_window_compact(m.delivery_start_ms, m.delivery_end_ms))
        .unwrap_or_else(|| "—".into());
    let qp =
        |l: &crate::model::Level| format!("{}@{:.2}", l.quantity, l.price as f64 / PRICE_SCALE);
    let bid = snap.bids.first().map(qp).unwrap_or_else(|| "—".into());
    let ask = snap.asks.first().map(qp).unwrap_or_else(|| "—".into());

    let mut job = LayoutJob::default();
    job.append(&format!("{id:<COL_ID$}"), 0.0, fmt(f.clone(), fg));
    job.append(&format!("{product:<COL_PROD$}"), 0.0, fmt(f.clone(), fg));
    job.append(&format!("{window:<COL_DELIV$}"), 0.0, fmt(f.clone(), AXIS));
    job.append(
        &format!("{bid:<COL_QP$}"),
        0.0,
        fmt(f.clone(), chart_green()),
    );
    job.append(&format!("{ask:<COL_QP$}"), 0.0, fmt(f.clone(), chart_red()));
    job.append(
        &format!("{}/{}", snap.bids.len(), snap.asks.len()),
        0.0,
        fmt(f, AXIS),
    );
    job
}

fn fmt(font_id: FontId, color: Color32) -> TextFormat {
    TextFormat {
        font_id,
        color,
        ..Default::default()
    }
}

impl eframe::App for App {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.drain();
        self.tick_rates(ctx);
        // Repaint every frame so the FPS readout reflects the real refresh rate.
        ctx.request_repaint();

        egui::TopBottomPanel::top("controls").show(ctx, |ui| self.controls(ui));
        egui::SidePanel::right("panel")
            .resizable(true)
            .default_width(440.0)
            .min_width(320.0)
            .show(ctx, |ui| self.right_panel(ui));
        egui::CentralPanel::default().show(ctx, |ui| {
            let book = self.selected.and_then(|id| self.books.get(&id));
            let stats = self.selected.and_then(|id| self.stats.get(&id));
            let last_t = book.map_or(0.0, |b| b.last_t as f64);
            // Wall-clock drives "now" so open bars keep growing between updates.
            let now = (now_ms() as f64).max(last_t);
            // The ladder (right panel) reads this one frame later — imperceptible, and avoids
            // ordering the chart before the side panel it shares width with. A level under the
            // cursor on the chart overrides what the ladder set this frame; if the cursor isn't
            // over a chart level we keep the ladder's choice.
            let hover = chart::draw(ui, &mut self.view, book, stats, now, self.hover_level);
            self.hover_t = hover.t;
            if hover.level.is_some() {
                self.hover_level = hover.level;
            }
        });
    }
}

const AXIS: Color32 = Color32::from_rgb(0x7d, 0x87, 0x94);
const AMBER: Color32 = Color32::from_rgb(0xd2, 0xa1, 0x06);
// Behind a ladder cell whose level is the one highlighted in the chart (matches the chart's
// outline colour, dimmed so the cell text stays readable).
const LADDER_HL: Color32 = Color32::from_rgba_premultiplied(0x55, 0x4b, 0x1f, 0xff);
fn chart_green() -> Color32 {
    Color32::from_rgb(0x1f, 0x9d, 0x63)
}
fn chart_red() -> Color32 {
    Color32::from_rgb(0xd0, 0x46, 0x3b)
}

fn row(ui: &mut egui::Ui, k: &str, v: &str) {
    ui.label(k);
    ui.label(v);
    ui.end_row();
}
