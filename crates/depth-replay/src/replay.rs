//! The replay loop: fold archived frames into order books and emit throttled depth rows.

use std::collections::{BTreeSet, HashMap, HashSet};
use std::fs;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail};
use chrono::{DateTime, TimeDelta, Utc};
use nordpool_connector::proto::archive::TimestampedFrame;
use nordpool_connector::proto::pmd::ServerFrame;
use nordpool_connector::proto::pmd::server_frame::Payload as ServerPayload;
use nordpool_connector::sink::to_book_updates;
use nordpool_marketdata::{BookKey, EicCode, OrderBookStore};
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use parquet::basic::Compression;
use prost::Message;
use tokio::runtime::Runtime;

use crate::contract_writer::ContractWriter;
use crate::reader;
use crate::writer::DepthWriter;

#[derive(Debug, Default)]
pub struct Stats {
    pub frames: u64,
    pub rows: u64,
    /// Distinct books emitted (localview) or distinct contracts dumped (contract).
    pub entities: usize,
    pub bytes_read: u64,
    pub bytes_written: u64,
}

/// What a localview replay writes and how: the one delivery area to reconstruct (see [`run`]),
/// the per-book event-time throttle, the price levels per side, and the Parquet codec. Grouped
/// because they are exactly the CLI's localview-only arguments.
pub struct DepthOptions {
    /// The delivery area to replay, or `None` to latch the range's first and reject a second.
    pub area: Option<EicCode>,
    /// Minimum event-time gap between two emitted rows for the same book; `ZERO` emits every
    /// book-changing frame.
    pub throttle: Duration,
    /// Price levels per side written into each row.
    pub levels: usize,
    pub compression: Compression,
}

/// A frame's archived receive timestamp. A segment carrying none — or a malformed one — reads as
/// the epoch, the same floor the writer's own stamp has.
fn recv_time(frame: &TimestampedFrame) -> DateTime<Utc> {
    frame
        .timestamp
        .as_ref()
        .and_then(|t| DateTime::from_timestamp(t.seconds, u32::try_from(t.nanos).ok()?))
        .unwrap_or_default()
}

/// Read every segment in order, reconstruct the books, and write throttled top-`levels`
/// snapshots to a Parquet file at `output`. Segments are fetched from `object_store` (S3, file://,
/// or local) via `rt`; the reconstruction and Parquet write stay synchronous.
///
/// **One delivery area per replay.** The archive multiplexes every subscribed area into one
/// stream, but the output rows carry no area, and the throttle gate is keyed by [`BookKey`] —
/// which a contract shares across areas. Replaying two areas at once would therefore both
/// interleave them into indistinguishable rows and let one area's emission suppress the other's.
/// `area` selects one explicitly (frames for the rest are skipped); when it is `None` the first
/// frame's area is latched and a second one fails the run rather than conflating them.
pub fn run(
    rt: &Runtime,
    object_store: &dyn ObjectStore,
    segments: &[ObjectPath],
    output: &Path,
    options: DepthOptions,
) -> anyhow::Result<Stats> {
    let DepthOptions {
        area,
        throttle,
        levels,
        compression,
    } = options;
    // A throttle beyond TimeDelta's range (~292 million years) can only mean "never re-emit".
    let throttle = TimeDelta::from_std(throttle).unwrap_or(TimeDelta::MAX);
    let mut store = OrderBookStore::new();
    // Last emitted event-time (the archive's receive timestamp) per book — the throttle gate.
    // Keyed by BookKey alone, which only holds because exactly one area is ever replayed.
    let mut last_emit: HashMap<BookKey, DateTime<Utc>> = HashMap::new();
    let mut writer = DepthWriter::create(output, compression)?;
    let mut frames = 0u64;
    let mut bytes_read = 0u64;
    // The area actually being replayed: `area` when given, else the first frame's. Latched on the
    // first frame that passes the filter.
    let mut replayed: Option<EicCode> = None;
    // Areas the `area` filter skipped, so an EIC that matches nothing can name the alternatives.
    let mut skipped: BTreeSet<EicCode> = BTreeSet::new();

    for path in segments {
        let (segment_frames, segment_bytes) = reader::read_segment(rt, object_store, path)?;
        bytes_read += segment_bytes;
        tracing::debug!(path = ?path, bytes = segment_bytes, frames = segment_frames.len(), "read segment");
        for tframe in segment_frames {
            let recv = recv_time(&tframe);
            let Some(payload) = ServerFrame::decode(tframe.frame.as_slice())?.payload else {
                continue;
            };
            // The localview archive only holds LocalView frames; ignore anything else
            // defensively rather than failing.
            let ServerPayload::LocalView(view) = payload else {
                continue;
            };

            let Some(frame_area) = EicCode::from_id(view.delivery_area_id) else {
                tracing::warn!(
                    delivery_area_id = view.delivery_area_id,
                    "frame for unknown delivery area; skipped"
                );
                continue;
            };
            if area.is_some_and(|selected| selected != frame_area) {
                skipped.insert(frame_area);
                continue;
            }
            match replayed {
                None => {
                    tracing::info!(eic_code = %frame_area, "replaying delivery area");
                    replayed = Some(frame_area);
                }
                Some(seen) if seen != frame_area => bail!(
                    "the archive holds LocalView frames for more than one delivery area \
                     ({seen} and {frame_area}) — pass --eic-code to pick the one to replay, \
                     since a replay reconstructs a single area"
                ),
                Some(_) => {}
            }
            frames += 1;

            let updates = to_book_updates(&view);
            store.apply(frame_area, &updates, view.is_snapshot, recv);
            if view.is_snapshot {
                // A snapshot dropped the books it omits; reset their throttle state too or a
                // re-appearing book wouldn't re-emit.
                last_emit.clear();
            }

            for update in &updates {
                let key = update.key;
                let due = last_emit
                    .get(&key)
                    .is_none_or(|prev| recv - prev >= throttle);
                if due {
                    let book = store
                        .book(frame_area, &key)
                        .expect("book present after applying updates");
                    let bids = book.bid_levels().take(levels).collect::<Vec<_>>();
                    let asks = book.ask_levels().take(levels).collect::<Vec<_>>();
                    writer.push_levels(
                        recv,
                        frame_area,
                        key,
                        book.last_revision(),
                        &bids,
                        &asks,
                    )?;
                    last_emit.insert(key, recv);
                }
            }
        }
    }

    let rows = writer.finish()?;
    // A selected area that matched nothing while other areas were skipped is a wrong --eic-code,
    // not an empty range: name what the archive actually holds instead of writing an empty file.
    if let (Some(selected), None, false) = (area, replayed, skipped.is_empty()) {
        let available: Vec<String> = skipped.iter().map(EicCode::to_string).collect();
        bail!(
            "no LocalView frames for delivery area {selected} in this range — it holds {}",
            available.join(", ")
        );
    }
    Ok(Stats {
        frames,
        rows,
        entities: last_emit.len(),
        bytes_read,
        bytes_written: file_len(output)?,
    })
}

/// Read every segment in order and dump each frame's contracts verbatim — one Parquet row per
/// contract delivery area — to `output`. No reconstruction or throttling: contracts are
/// reference data, so the full snapshot/delta stream is written as-is (the `is_snapshot`
/// column distinguishes the two).
pub fn run_contracts(
    rt: &Runtime,
    object_store: &dyn ObjectStore,
    segments: &[ObjectPath],
    output: &Path,
) -> anyhow::Result<Stats> {
    let mut writer = ContractWriter::create(output)?;
    let mut frames = 0u64;
    let mut bytes_read = 0u64;
    let mut seen: HashSet<i64> = HashSet::new();

    for path in segments {
        let (segment_frames, segment_bytes) = reader::read_segment(rt, object_store, path)?;
        bytes_read += segment_bytes;
        tracing::debug!(path = ?path, bytes = segment_bytes, frames = segment_frames.len(), "read segment");
        for tframe in segment_frames {
            let recv = recv_time(&tframe);
            let Some(payload) = ServerFrame::decode(tframe.frame.as_slice())?.payload else {
                continue;
            };
            let ServerPayload::Contract(view) = payload else {
                continue;
            };
            frames += 1;
            for contract in &view.contracts {
                seen.insert(contract.contract_id);
                writer.push_contract(recv, view.is_snapshot, contract)?;
            }
        }
    }

    let rows = writer.finish()?;
    Ok(Stats {
        frames,
        rows,
        entities: seen.len(),
        bytes_read,
        bytes_written: file_len(output)?,
    })
}

pub(crate) fn file_len(path: &Path) -> anyhow::Result<u64> {
    fs::metadata(path)
        .map(|m| m.len())
        .with_context(|| format!("stat output {path:?}"))
}
