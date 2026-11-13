//! The chart widget: a time (X) × price (Y) plane of horizontal bars, one per price level over
//! the interval it held a constant quantity, vertical thickness encoding quantity. egui's
//! immediate mode means we redraw from the segment book every frame; two affine scales give
//! independent per-axis pan/zoom, and `follow_live` pins "now" to the right edge until the
//! user pans into the past.

use crate::model::{ExecBar, SegmentBook, Side, StatPoint, StatSeries};
use eframe::egui::{self, Align2, Color32, FontId, Pos2, Rect, Sense, Shape, Stroke};

type PriceQty = (i64, i64);

const PAD_L: f32 = 8.0;
const PAD_R: f32 = 60.0;
const PAD_T: f32 = 6.0;
const PAD_B: f32 = 20.0;

const BID: Color32 = Color32::from_rgb(0x1f, 0x9d, 0x63);
const ASK: Color32 = Color32::from_rgb(0xd0, 0x46, 0x3b);
const GRID: Color32 = Color32::from_rgb(0x1b, 0x20, 0x27);
const AXIS_TEXT: Color32 = Color32::from_rgb(0x7d, 0x87, 0x94);
const BG: Color32 = Color32::from_rgb(0x0e, 0x11, 0x16);
const HIGHLIGHT: Color32 = Color32::from_rgb(0xff, 0xe0, 0x66);
// Brighter than the bar fills so the VWAP lines read on top of same-side bars.
const BID_VWAP: Color32 = Color32::from_rgb(0x5a, 0xe0, 0xa0);
const ASK_VWAP: Color32 = Color32::from_rgb(0xf0, 0x82, 0x76);
// Public-statistic step lines: day-ahead price (solid) and VWAP (dashed), both yellow.
const STAT_YELLOW: Color32 = Color32::from_rgb(0xf5, 0xd0, 0x42);
// Executed-volume bars in the bottom pane.
const VOLUME: Color32 = Color32::from_rgb(0x55, 0x82, 0xb8);

/// The executed-volume pane's share of the plot height, and the gap separating it from the price
/// pane above.
const VOL_PANE_FRAC: f32 = 0.22;
const VOL_PANE_GAP: f32 = 10.0;
/// Fixed pixel width of an executed-volume bar, drawn at the instant the execution was observed.
const VOL_BAR_W: f32 = 5.0;

/// Prices arrive as scaled integers (×100 here); divide for display.
pub const PRICE_SCALE: f64 = 100.0;

/// How close (pixels) the cursor must be to a level's band to count as hovering it.
const HOVER_PX: f32 = 6.0;

/// What the cursor is over on the chart, fed back to the app each frame.
pub struct Hover {
    /// Epoch-ms instant under the cursor (drives the book ladder's time), else None.
    pub t: Option<i64>,
    /// The (side, price) level under/near the cursor, highlighted in both chart and ladder.
    pub level: Option<(Side, i64)>,
}

#[derive(PartialEq, Clone, Copy)]
pub enum QtyMode {
    Auto,
    Fixed,
}

pub struct ChartView {
    pub t_right: f64, // ms at the right edge
    pub ms_per_view: f64,
    pub price_mid: f64,
    pub price_per_view: f64,
    pub price_inited: bool,
    pub follow_live: bool,
    pub qty_mode: QtyMode,
    pub fixed_unit_per_px: f64, // fixed mode: scaled-qty per pixel of thickness
    /// Target quantity the mid line's VWAP fills to on each side. Larger = deeper, steadier.
    pub mid_depth_qty: f64,
}

impl Default for ChartView {
    fn default() -> Self {
        Self {
            t_right: 0.0,
            ms_per_view: 5.0 * 60_000.0,
            price_mid: 0.0,
            price_per_view: 200.0,
            price_inited: false,
            follow_live: true,
            qty_mode: QtyMode::Auto,
            fixed_unit_per_px: 50.0,
            mid_depth_qty: 1000.0,
        }
    }
}

impl ChartView {
    pub fn set_window_ms(&mut self, ms: f64) {
        self.ms_per_view = ms;
    }

    pub fn go_live(&mut self) {
        self.follow_live = true;
    }

    /// Fit the price axis to the best bid/ask band over the period that will be shown, centered
    /// on its mid — so a freshly selected contract lands with the top of book in view and zoomed
    /// to how far it travels over the window. No-op (returns without setting `price_inited`) until
    /// the book has top-of-book in that window, so it keeps retrying until data arrives.
    fn fit_price(&mut self, book: &SegmentBook, now: f64) {
        let t_right = if self.follow_live { now } else { self.t_right };
        let t0 = t_right - self.ms_per_view;
        let vis: Vec<&crate::model::Segment> = book
            .segments()
            .filter(|s| {
                let end = s.t_end.map(|e| e as f64).unwrap_or(now);
                end >= t0 && (s.t_start as f64) <= t_right
            })
            .collect();

        // Sweep the window for the best-bid/ask band: lowest best bid to highest best ask.
        let (mut lo, mut hi) = (f64::INFINITY, f64::NEG_INFINITY);
        const N: i64 = 64;
        for k in 0..=N {
            let t = (t0 + (t_right - t0) * k as f64 / N as f64) as i64;
            let (mut bb, mut ba) = (i64::MIN, i64::MAX);
            for s in &vis {
                if t >= s.t_start && s.t_end.is_none_or(|e| t < e) {
                    match s.side {
                        Side::Bid => bb = bb.max(s.price),
                        Side::Ask => ba = ba.min(s.price),
                    }
                }
            }
            for p in [bb, ba] {
                if p != i64::MIN && p != i64::MAX {
                    lo = lo.min(p as f64);
                    hi = hi.max(p as f64);
                }
            }
        }
        if !lo.is_finite() || !hi.is_finite() {
            return; // no top-of-book in the window yet
        }

        let span = hi - lo;
        self.price_mid = (lo + hi) / 2.0;
        // 1.5× the band leaves margin above/below; floor at one price unit so a flat book still
        // shows a sensible range rather than collapsing onto a single line.
        self.price_per_view = (span * 1.5).max(PRICE_SCALE);
        self.price_inited = true;
    }
}

/// Draw the chart into the available space and fold user input back into `view`. `now` is the
/// current logical instant (epoch ms): wall-clock for live feeds, the last record for replay —
/// open segments and the live edge extend to it, so the view keeps scrolling and the rectangles
/// keep growing even between updates. `highlight` is a (side, price) level the user is hovering
/// in the book ladder — its bars get a bright outline. Returns the instant and the level under the
/// cursor: a level the cursor is over/near on the chart takes precedence over `highlight`, and is
/// reported back so the ladder can highlight the matching row.
pub fn draw(
    ui: &mut egui::Ui,
    view: &mut ChartView,
    book: Option<&SegmentBook>,
    stats: Option<&StatSeries>,
    now: f64,
    highlight: Option<(Side, i64)>,
) -> Hover {
    let (response, painter) = ui.allocate_painter(ui.available_size(), Sense::click_and_drag());
    let rect = response.rect;
    painter.rect_filled(rect, 0.0, BG);

    let left = rect.left() + PAD_L;
    let right = rect.right() - PAD_R;
    let top = rect.top() + PAD_T;
    // The plane splits into a price pane (top) and an executed-volume pane (bottom) that share the
    // time axis. `chart_bottom` carries the time axis (labels, the full-height crosshair); `bottom`
    // and `plot_h` are the price pane the existing price drawing maps into.
    let chart_bottom = rect.bottom() - PAD_B;
    let vol_h = ((chart_bottom - top) * VOL_PANE_FRAC).clamp(24.0, 180.0);
    let vol_top = chart_bottom - vol_h;
    let bottom = vol_top - VOL_PANE_GAP;
    let plot_w = (right - left).max(1.0) as f64;
    let plot_h = (bottom - top).max(1.0) as f64;

    if view.follow_live && now > 0.0 {
        view.t_right = now;
    }
    if !view.price_inited
        && let Some(b) = book
    {
        view.fit_price(b, now);
    }

    // --- input: wheel zooms the price (Y) axis; the time window is set by the buttons. Drag
    // pans both axes, double-click resets. ---
    if let Some(p) = response.hover_pos() {
        let scroll = ui.input(|i| i.raw_scroll_delta.y) as f64;
        if scroll != 0.0 && p.y <= bottom {
            let factor = (-scroll * 0.0015).exp();
            let p_at = price_of(p.y, top, plot_h, view);
            view.price_per_view = (view.price_per_view * factor).clamp(1.0, 1e9);
            view.price_mid =
                p_at + ((p.y as f64 - top as f64 - plot_h / 2.0) / plot_h) * view.price_per_view;
        }
    }
    if response.dragged() {
        let d = response.drag_delta();
        if d.x != 0.0 {
            view.t_right -= (d.x as f64 / plot_w) * view.ms_per_view;
            view.follow_live = false;
        }
        view.price_mid += (d.y as f64 / plot_h) * view.price_per_view;
    }
    if response.double_clicked() {
        view.price_inited = false;
        view.follow_live = true;
    }

    let left_t = view.t_right - view.ms_per_view;
    // Open segments (no t_end) are live until `now`, whether or not we are following.
    let live_right = now;

    draw_grid(
        &painter,
        view,
        left,
        right,
        top,
        bottom,
        chart_bottom,
        plot_w,
        plot_h,
    );

    let mut cursor_level: Option<(Side, i64)> = None;
    if let Some(book) = book {
        // Clip everything book-derived to the price pane so bands and lines for levels near the
        // pane's bottom edge don't bleed into the volume pane below.
        let plot = painter.with_clip_rect(Rect::from_min_max(
            Pos2::new(left, top),
            Pos2::new(right, bottom),
        ));
        let vis: Vec<&crate::model::Segment> = book
            .segments()
            .filter(|s| {
                let end = s.t_end.map(|e| e as f64).unwrap_or(live_right);
                end >= left_t && (s.t_start as f64) <= view.t_right
            })
            .collect();

        // Height encodes quantity. Auto picks a scale where even the largest level's band can't
        // reach the nearest neighbouring level (so stacks never overlap); Fixed is a manual
        // quantity-per-pixel the user dials in.
        let px_per_qty = match view.qty_mode {
            QtyMode::Fixed => 1.0 / view.fixed_unit_per_px,
            QtyMode::Auto => {
                let max = vis.iter().map(|s| s.qty).max().unwrap_or(1).max(1) as f64;
                let gap = min_level_gap_px(&vis, plot_h, view).unwrap_or((plot_h / 8.0).min(40.0));
                gap * 0.9 / max
            }
        };

        draw_depth_tint(
            &plot, view, &vis, left, right, top, bottom, plot_w, plot_h, left_t, live_right,
        );

        // The level the cursor is over/near: among segments spanning the cursor's instant, the one
        // whose drawn band is closest in Y (0 if the cursor is inside it), within HOVER_PX.
        if let Some(p) = response.hover_pos()
            && p.x >= left
            && p.x <= right
            && p.y >= top
            && p.y <= bottom
        {
            let ti = t_of(p.x, left, plot_w, view) as i64;
            let mut best = HOVER_PX;
            for s in &vis {
                if ti < s.t_start || s.t_end.is_some_and(|e| ti >= e) {
                    continue;
                }
                let y = y_of(s.price as f64, top, plot_h, view);
                let th = ((s.qty as f64 * px_per_qty) as f32).max(1.0);
                let (y0, y1) = if s.side == Side::Bid {
                    (y, y + th)
                } else {
                    (y - th, y)
                };
                let d = (y0 - p.y).max(p.y - y1).max(0.0);
                if d < best {
                    best = d;
                    cursor_level = Some((s.side, s.price));
                }
            }
        }
        // A chart hover wins over a ladder hover for what gets outlined.
        let active = cursor_level.or(highlight);

        let mut hl_rects: Vec<Rect> = Vec::new();
        for s in &vis {
            let end = s.t_end.map(|e| e as f64).unwrap_or(live_right);
            let x0 =
                (x_of(s.t_start.max(left_t as i64) as f64, left, plot_w, view)).clamp(left, right);
            let x1 = (x_of(end.min(view.t_right), left, plot_w, view)).clamp(left, right);
            if x1 <= x0 {
                continue;
            }
            // Anchor the band on the price line and grow it away from the spread: down for bids,
            // up for asks. Larger size = a deeper band.
            let y = y_of(s.price as f64, top, plot_h, view);
            let th = ((s.qty as f64 * px_per_qty) as f32).max(1.0);
            let (y0, y1, color) = if s.side == Side::Bid {
                (y, y + th, BID)
            } else {
                (y - th, y, ASK)
            };
            let band = Rect::from_min_max(Pos2::new(x0, y0), Pos2::new(x1, y1));
            plot.rect_filled(band, 0.0, color);
            if active == Some((s.side, s.price)) {
                hl_rects.push(band);
            }
        }

        // Outline the hovered level on top of every fill so it reads above neighbouring bands.
        for band in hl_rects {
            plot.rect_stroke(band, 0.0, Stroke::new(1.5, HIGHLIGHT));
        }

        draw_vwap_lines(
            &plot, view, &vis, left, right, top, plot_w, plot_h, left_t, live_right,
        );
    }

    if let Some(stats) = stats {
        draw_stat_lines(
            &painter, view, stats, left, right, top, bottom, plot_w, plot_h, live_right,
        );
        draw_volume_bars(
            &painter,
            view,
            stats,
            left,
            right,
            vol_top,
            chart_bottom,
            plot_w,
            left_t,
        );
    }

    // crosshair + readout. The vertical time line spans both panes; the horizontal price line and
    // its readout only make sense in the price pane.
    let mut hovered_t = None;
    if let Some(p) = response.hover_pos()
        && p.x >= left
        && p.x <= right
        && p.y >= top
        && p.y <= chart_bottom
    {
        hovered_t = Some(t_of(p.x, left, plot_w, view) as i64);
        let cross = Stroke::new(1.0, Color32::from_rgb(0x5a, 0x65, 0x73));
        painter.line_segment([Pos2::new(p.x, top), Pos2::new(p.x, chart_bottom)], cross);
        if p.y <= bottom {
            painter.line_segment([Pos2::new(left, p.y), Pos2::new(right, p.y)], cross);
            let price = price_of(p.y, top, plot_h, view) / PRICE_SCALE;
            painter.text(
                Pos2::new(right + 4.0, p.y),
                Align2::LEFT_CENTER,
                format!("{price:.2}"),
                FontId::monospace(11.0),
                Color32::from_rgb(0xcf, 0xd6, 0xdf),
            );
        }
        painter.text(
            Pos2::new(p.x, chart_bottom + 10.0),
            Align2::CENTER_CENTER,
            hhmmss(t_of(p.x, left, plot_w, view) as i64),
            FontId::monospace(11.0),
            Color32::from_rgb(0xcf, 0xd6, 0xdf),
        );
    }
    Hover {
        t: hovered_t,
        level: cursor_level,
    }
}

/// `bottom` is the price pane's bottom (price gridlines stop there); `chart_bottom` is the time
/// axis below the volume pane, where the vertical time lines reach and the time labels sit.
#[allow(clippy::too_many_arguments)]
fn draw_grid(
    painter: &egui::Painter,
    view: &ChartView,
    left: f32,
    right: f32,
    top: f32,
    bottom: f32,
    chart_bottom: f32,
    plot_w: f64,
    plot_h: f64,
) {
    let stroke = Stroke::new(1.0, GRID);
    let font = FontId::monospace(11.0);

    // Price (Y) gridlines + labels in the right gutter.
    let p_top = price_of(top, top, plot_h, view);
    let p_bot = price_of(bottom, top, plot_h, view);
    let p_step = nice_step((p_top - p_bot) / 6.0);
    if p_step > 0.0 {
        let mut p = (p_bot / p_step).ceil() * p_step;
        while p <= p_top {
            let y = y_of(p, top, plot_h, view).round();
            painter.line_segment([Pos2::new(left, y), Pos2::new(right, y)], stroke);
            painter.text(
                Pos2::new(right + 4.0, y),
                Align2::LEFT_CENTER,
                format!("{:.2}", p / PRICE_SCALE),
                font.clone(),
                AXIS_TEXT,
            );
            p += p_step;
        }
    }

    // Time (X) gridlines span both panes; labels along the very bottom.
    let left_t = view.t_right - view.ms_per_view;
    let t_step = nice_time_step(view.ms_per_view / 6.0);
    let mut t = (left_t / t_step).ceil() * t_step;
    while t <= view.t_right {
        let x = x_of(t, left, plot_w, view).round();
        painter.line_segment([Pos2::new(x, top), Pos2::new(x, chart_bottom)], stroke);
        painter.text(
            Pos2::new(x, chart_bottom + 10.0),
            Align2::CENTER_CENTER,
            hhmmss(t as i64),
            font.clone(),
            AXIS_TEXT,
        );
        t += t_step;
    }
}

/// Smallest pixel gap between adjacent price levels on the same side among the visible segments,
/// so the auto scale can keep a level's band from reaching its neighbour. None when there are
/// fewer than two distinct prices on both sides.
fn min_level_gap_px(vis: &[&crate::model::Segment], plot_h: f64, view: &ChartView) -> Option<f64> {
    let mut min_gap = f64::INFINITY;
    for side in [Side::Bid, Side::Ask] {
        let mut prices: Vec<i64> = vis
            .iter()
            .filter(|s| s.side == side)
            .map(|s| s.price)
            .collect();
        prices.sort_unstable();
        prices.dedup();
        for w in prices.windows(2) {
            min_gap = min_gap.min((w[1] - w[0]) as f64);
        }
    }
    min_gap
        .is_finite()
        .then(|| min_gap / view.price_per_view * plot_h)
}

/// Shade the background to mark the tradable band: a faint green wash from the best-bid step line
/// down to the bottom, a faint red wash from the best-ask step line up to the top. Both lines are
/// reconstructed from the visible segments and follow the level history, so the tint is a staircase
/// that tracks where the top of book sat over time. Sampled in thin columns across the plot.
#[allow(clippy::too_many_arguments)]
fn draw_depth_tint(
    painter: &egui::Painter,
    view: &ChartView,
    vis: &[&crate::model::Segment],
    left: f32,
    right: f32,
    top: f32,
    bottom: f32,
    plot_w: f64,
    plot_h: f64,
    left_t: f64,
    live_right: f64,
) {
    if vis.is_empty() {
        return;
    }
    let bid_tint = Color32::from_rgba_unmultiplied(0x0c, 0x33, 0x21, 0xd0);
    let ask_tint = Color32::from_rgba_unmultiplied(0x47, 0x18, 0x13, 0xd0);

    let best_at = |t: f64| -> (Option<i64>, Option<i64>) {
        let ti = t as i64;
        let (mut bb, mut ba) = (i64::MIN, i64::MAX);
        for s in vis {
            if ti >= s.t_start && s.t_end.is_none_or(|e| ti < e) {
                match s.side {
                    Side::Bid => bb = bb.max(s.price),
                    Side::Ask => ba = ba.min(s.price),
                }
            }
        }
        (
            (bb != i64::MIN).then_some(bb),
            (ba != i64::MAX).then_some(ba),
        )
    };

    let step = 2.0_f32;
    let mut x = left;
    while x < right {
        let x1 = (x + step).min(right);
        let t = t_of((x + x1) / 2.0, left, plot_w, view);
        if t >= left_t && t <= live_right {
            let (bb, ba) = best_at(t);
            if let Some(bb) = bb {
                let y = y_of(bb as f64, top, plot_h, view).clamp(top, bottom);
                if y < bottom {
                    painter.rect_filled(
                        Rect::from_min_max(Pos2::new(x, y), Pos2::new(x1, bottom)),
                        0.0,
                        bid_tint,
                    );
                }
            }
            if let Some(ba) = ba {
                let y = y_of(ba as f64, top, plot_h, view).clamp(top, bottom);
                if y > top {
                    painter.rect_filled(
                        Rect::from_min_max(Pos2::new(x, top), Pos2::new(x1, y)),
                        0.0,
                        ask_tint,
                    );
                }
            }
        }
        x += step;
    }
}

/// VWAP of filling `target` quantity against one side's levels, walked best-first (`descending`
/// for bids: highest price first; ascending for asks). The deepest level taken is filled only up to
/// the target. None if the side is empty; if total depth is below the target the VWAP covers all of
/// it.
fn depth_vwap(mut levels: Vec<(i64, i64)>, descending: bool, target: f64) -> Option<f64> {
    if descending {
        levels.sort_unstable_by_key(|&(p, _)| std::cmp::Reverse(p));
    } else {
        levels.sort_unstable_by_key(|&(p, _)| p);
    }
    let mut remaining = target;
    let mut notional = 0.0;
    for (p, q) in levels {
        if remaining <= 0.0 {
            break;
        }
        let take = (q as f64).min(remaining);
        notional += p as f64 * take;
        remaining -= take;
    }
    let filled = target - remaining;
    (filled > 0.0).then(|| notional / filled)
}

/// Three dashed VWAP lines over time, reconstructed from the visible segments and sampled across
/// the plot: the bid-side VWAP (green) and ask-side VWAP (red) of filling `view.mid_depth_qty` on
/// each side, plus their midpoint (white). Pricing a fixed notional rather than the touch makes the
/// lines steady — a small order at the touch barely moves a 1000-lot VWAP. Each line breaks where
/// its side is empty; the mid breaks where either is. If a side holds less than the target, its
/// VWAP covers all available depth.
#[allow(clippy::too_many_arguments)]
fn draw_vwap_lines(
    painter: &egui::Painter,
    view: &ChartView,
    vis: &[&crate::model::Segment],
    left: f32,
    right: f32,
    top: f32,
    plot_w: f64,
    plot_h: f64,
    left_t: f64,
    live_right: f64,
) {
    if vis.is_empty() {
        return;
    }

    let target = view.mid_depth_qty;
    let vwaps_at = |t: f64| -> (Option<f64>, Option<f64>) {
        let ti = t as i64;
        let (mut bids, mut asks): (Vec<PriceQty>, Vec<PriceQty>) = (Vec::new(), Vec::new());
        for s in vis {
            if ti >= s.t_start && s.t_end.is_none_or(|e| ti < e) {
                match s.side {
                    Side::Bid => bids.push((s.price, s.qty)),
                    Side::Ask => asks.push((s.price, s.qty)),
                }
            }
        }
        (
            depth_vwap(bids, true, target),
            depth_vwap(asks, false, target),
        )
    };

    let flush = |run: &mut Vec<Pos2>, stroke: Stroke| {
        if run.len() >= 2 {
            painter.extend(Shape::dashed_line(run, stroke, 6.0, 4.0));
        }
        run.clear();
    };
    let bid_stroke = Stroke::new(1.0, BID_VWAP);
    let ask_stroke = Stroke::new(1.0, ASK_VWAP);
    let mid_stroke = Stroke::new(1.0, Color32::WHITE);
    let (mut bid_run, mut ask_run, mut mid_run): (Vec<Pos2>, Vec<Pos2>, Vec<Pos2>) =
        (Vec::new(), Vec::new(), Vec::new());

    let mut x = left;
    while x <= right {
        let t = t_of(x, left, plot_w, view);
        let (bv, av) = if t >= left_t && t <= live_right {
            vwaps_at(t)
        } else {
            (None, None)
        };
        match bv {
            Some(p) => bid_run.push(Pos2::new(x, y_of(p, top, plot_h, view))),
            None => flush(&mut bid_run, bid_stroke),
        }
        match av {
            Some(p) => ask_run.push(Pos2::new(x, y_of(p, top, plot_h, view))),
            None => flush(&mut ask_run, ask_stroke),
        }
        match (bv, av) {
            (Some(b), Some(a)) => {
                mid_run.push(Pos2::new(x, y_of((b + a) / 2.0, top, plot_h, view)))
            }
            _ => flush(&mut mid_run, mid_stroke),
        }
        x += 2.0;
    }
    flush(&mut bid_run, bid_stroke);
    flush(&mut ask_run, ask_stroke);
    flush(&mut mid_run, mid_stroke);
}

/// Draw the public-statistic step lines for the selected contract over time: the day-ahead price
/// (solid yellow) and VWAP (dashed yellow), both on the price axis. Each is a step function — a
/// value holds from its observation until the next — and breaks where that field is absent. Clipped
/// to the plot so values outside the visible price band don't bleed into the gutter.
#[allow(clippy::too_many_arguments)]
fn draw_stat_lines(
    painter: &egui::Painter,
    view: &ChartView,
    stats: &StatSeries,
    left: f32,
    right: f32,
    top: f32,
    bottom: f32,
    plot_w: f64,
    plot_h: f64,
    live_right: f64,
) {
    let plot = painter.with_clip_rect(Rect::from_min_max(
        Pos2::new(left, top),
        Pos2::new(right, bottom),
    ));
    draw_step(
        &plot,
        view,
        &stats.points,
        |p| p.day_ahead_price,
        Stroke::new(1.5, STAT_YELLOW),
        false,
        left,
        plot_w,
        top,
        plot_h,
        live_right,
    );
    draw_step(
        &plot,
        view,
        &stats.points,
        |p| p.vwap,
        Stroke::new(1.5, STAT_YELLOW),
        true,
        left,
        plot_w,
        top,
        plot_h,
        live_right,
    );
}

/// Draw one value of the statistic series as a step function: horizontal at each point's value,
/// stepping vertically at the next point's time, with the final value held to `live_right` ("now").
/// A `None` value breaks the line. `dashed` selects a dashed stroke (VWAP) over a solid one.
#[allow(clippy::too_many_arguments)]
fn draw_step(
    painter: &egui::Painter,
    view: &ChartView,
    points: &[StatPoint],
    value: impl Fn(&StatPoint) -> Option<i64>,
    stroke: Stroke,
    dashed: bool,
    left: f32,
    plot_w: f64,
    top: f32,
    plot_h: f64,
    live_right: f64,
) {
    let emit = |run: &mut Vec<Pos2>| {
        if run.len() >= 2 {
            if dashed {
                painter.extend(Shape::dashed_line(run, stroke, 6.0, 4.0));
            } else {
                painter.add(Shape::line(run.clone(), stroke));
            }
        }
        run.clear();
    };

    let mut run: Vec<Pos2> = Vec::new();
    let mut prev_y: Option<f32> = None;
    for p in points {
        match value(p) {
            Some(v) => {
                let x = x_of(p.t as f64, left, plot_w, view);
                let y = y_of(v as f64, top, plot_h, view);
                if let Some(py) = prev_y {
                    run.push(Pos2::new(x, py)); // horizontal at the held value
                }
                run.push(Pos2::new(x, y)); // step to the new value
                prev_y = Some(y);
            }
            None => {
                emit(&mut run);
                prev_y = None;
            }
        }
    }
    if let Some(py) = prev_y {
        run.push(Pos2::new(x_of(live_right, left, plot_w, view), py));
    }
    emit(&mut run);
}

/// Draw the executed-quantity bars in the bottom pane: one fixed-width bar per execution, placed
/// at the instant the turnover rise was observed, its height the executed quantity. The y-axis is
/// independent of price and auto-scales to the largest visible bar (labelled in the right gutter).
/// Bars grow up from the time axis (`vol_bottom`); the pane top is `vol_top`.
#[allow(clippy::too_many_arguments)]
fn draw_volume_bars(
    painter: &egui::Painter,
    view: &ChartView,
    stats: &StatSeries,
    left: f32,
    right: f32,
    vol_top: f32,
    vol_bottom: f32,
    plot_w: f64,
    left_t: f64,
) {
    // Delimit the pane with a faint top border so it reads as its own plot.
    painter.line_segment(
        [Pos2::new(left, vol_top), Pos2::new(right, vol_top)],
        Stroke::new(1.0, GRID),
    );

    let bars = stats.exec_bars();
    let vis: Vec<&ExecBar> = bars
        .iter()
        .filter(|b| b.t_end as f64 >= left_t && (b.t_end as f64) <= view.t_right)
        .collect();
    let Some(max) = vis.iter().map(|b| b.qty).max().filter(|&m| m > 0) else {
        return;
    };

    // Bars are clipped to the pane so one sliding off an edge shrinks instead of smearing.
    let pane = painter.with_clip_rect(Rect::from_min_max(
        Pos2::new(left, vol_top),
        Pos2::new(right, vol_bottom),
    ));
    let pane_h = (vol_bottom - vol_top) as f64;
    for b in &vis {
        let x = x_of(b.t_end as f64, left, plot_w, view);
        let h = (b.qty as f64 / max as f64 * pane_h * 0.92) as f32;
        let bar = Rect::from_min_max(
            Pos2::new(x - VOL_BAR_W / 2.0, vol_bottom - h),
            Pos2::new(x + VOL_BAR_W / 2.0, vol_bottom),
        );
        pane.rect_filled(bar, 0.0, VOLUME);
    }

    painter.text(
        Pos2::new(right + 4.0, vol_top),
        Align2::LEFT_TOP,
        max.to_string(),
        FontId::monospace(11.0),
        AXIS_TEXT,
    );
}

fn x_of(t: f64, left: f32, plot_w: f64, view: &ChartView) -> f32 {
    let left_t = view.t_right - view.ms_per_view;
    left + ((t - left_t) / view.ms_per_view * plot_w) as f32
}
fn t_of(x: f32, left: f32, plot_w: f64, view: &ChartView) -> f64 {
    let left_t = view.t_right - view.ms_per_view;
    left_t + (x as f64 - left as f64) / plot_w * view.ms_per_view
}
fn y_of(price: f64, top: f32, plot_h: f64, view: &ChartView) -> f32 {
    top + (plot_h / 2.0 - (price - view.price_mid) / view.price_per_view * plot_h) as f32
}
fn price_of(y: f32, top: f32, plot_h: f64, view: &ChartView) -> f64 {
    view.price_mid - (y as f64 - top as f64 - plot_h / 2.0) / plot_h * view.price_per_view
}

/// A "nice" step (1/2/5 × 10^n) near `raw`, for price gridlines.
fn nice_step(raw: f64) -> f64 {
    if raw <= 0.0 {
        return 0.0;
    }
    let mag = 10f64.powf(raw.log10().floor());
    let norm = raw / mag;
    let step = if norm < 1.5 {
        1.0
    } else if norm < 3.0 {
        2.0
    } else if norm < 7.0 {
        5.0
    } else {
        10.0
    };
    step * mag
}

const TIME_STEPS: [f64; 11] = [
    1_000.0,
    5_000.0,
    10_000.0,
    30_000.0,
    60_000.0,
    300_000.0,
    600_000.0,
    1_800_000.0,
    3_600_000.0,
    7_200_000.0,
    21_600_000.0,
];
fn nice_time_step(raw: f64) -> f64 {
    *TIME_STEPS
        .iter()
        .find(|&&s| s >= raw)
        .unwrap_or(&TIME_STEPS[TIME_STEPS.len() - 1])
}

/// Format epoch-ms as local HH:MM:SS without pulling in a date crate.
fn hhmmss(ms: i64) -> String {
    let secs = ms.div_euclid(1000);
    let s = secs.rem_euclid(86_400);
    format!("{:02}:{:02}:{:02}", s / 3600, (s % 3600) / 60, s % 60)
}
