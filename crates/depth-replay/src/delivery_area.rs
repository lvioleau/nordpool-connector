//! Delivery-area summary: fold the archived `delivery_area` reference feed into its final
//! state and dump one CSV row per area.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use anyhow::Context;
use nordpool_connector::proto::pmd::server_frame::Payload as ServerPayload;
use nordpool_connector::proto::pmd::{DeliveryArea, ServerFrame};
use object_store::ObjectStore;
use object_store::path::Path as ObjectPath;
use prost::Message;
use tokio::runtime::Runtime;

use crate::contract_writer::product_type_label;
use crate::reader;
use crate::replay::{Stats, file_len};

/// Read the segments in order — the caller passes the latest snapshot-led run — fold the
/// delivery-area frames into the final set (a snapshot replaces it, a delta upserts), and
/// write the surviving areas to `output` as CSV, one row per area, ordered by id.
pub fn run(
    rt: &Runtime,
    object_store: &dyn ObjectStore,
    segments: &[ObjectPath],
    output: &Path,
) -> anyhow::Result<Stats> {
    let mut areas: BTreeMap<i32, DeliveryArea> = BTreeMap::new();
    let mut frames = 0u64;
    let mut bytes_read = 0u64;

    for path in segments {
        let (segment_frames, segment_bytes) = reader::read_segment(rt, object_store, path)?;
        bytes_read += segment_bytes;
        tracing::debug!(path = ?path, bytes = segment_bytes, frames = segment_frames.len(), "read segment");
        for tframe in segment_frames {
            let Some(payload) = ServerFrame::decode(tframe.frame.as_slice())?.payload else {
                continue;
            };
            let ServerPayload::DeliveryArea(payload) = payload else {
                continue;
            };
            frames += 1;
            if payload.is_snapshot {
                areas.clear();
            }
            for area in payload.delivery_areas {
                areas.insert(area.delivery_area_id, area);
            }
        }
    }

    let mut writer = BufWriter::new(
        File::create(output).with_context(|| format!("creating output {output:?}"))?,
    );
    writeln!(
        writer,
        "delivery_area_id,area_code,eic_code,currency_code,time_zone,country_iso_code,product_types"
    )?;
    for area in areas.values() {
        let product_types = area
            .product_types
            .iter()
            .map(|&raw| product_type_label(raw))
            .collect::<Vec<_>>()
            .join("|");
        writeln!(
            writer,
            "{},{},{},{},{},{},{}",
            area.delivery_area_id,
            field(&area.area_code),
            field(&area.eic_code),
            field(&area.currency_code),
            field(&area.time_zone),
            field(&area.country_iso_code),
            field(&product_types),
        )?;
    }
    writer.flush().context("flushing csv output")?;

    Ok(Stats {
        frames,
        rows: areas.len() as u64,
        entities: areas.len(),
        bytes_read,
        bytes_written: file_len(output)?,
    })
}

/// Quote a CSV field only when it needs it (comma, quote, or line break inside the value).
fn field(s: &str) -> Cow<'_, str> {
    if s.contains([',', '"', '\n', '\r']) {
        Cow::Owned(format!("\"{}\"", s.replace('"', "\"\"")))
    } else {
        Cow::Borrowed(s)
    }
}
