//! Parquet sink for contract reference data. One row per contract delivery area (long
//! format), so a contract trading in N areas writes N rows; a contract with no areas writes a
//! single row with the area columns null. Enum fields are written as the proto's canonical
//! names via the generated `as_str_name()`, with the raw integer as a fallback for values our
//! compiled schema doesn't know.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;

use anyhow::Context;
use arrow::array::{
    ArrayRef, BooleanArray, Int32Array, Int64Array, RecordBatch, StringArray,
    TimestampMillisecondArray, TimestampNanosecondArray,
};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::{DateTime, Utc};
use nordpool_connector::proto::pmd::{self, Contract, ContractDeliveryAreaState};
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::parquet_time::{UTC_TZ, nanos, time_utc_millis, time_utc_nanos};

const BATCH_ROWS: usize = 8192;

/// Column buffers, one per schema field, drained into a `RecordBatch` on [`flush`](Self::flush).
/// Arrow builds its arrays from primitives, so a time column buffers the integer its own unit
/// counts in — nanos for `timestamp_nanos`, millis for the `*_ms` wire stamps — and gains its
/// timestamp type and UTC zone when the array is built.
pub struct ContractWriter {
    writer: ArrowWriter<File>,
    schema: Arc<Schema>,
    timestamp_nanos: Vec<i64>,
    is_snapshot: Vec<bool>,
    contract_id: Vec<i64>,
    name: Vec<String>,
    market_id: Vec<String>,
    market_type: Vec<String>,
    product_type: Vec<String>,
    delivery_start_ms: Vec<i64>,
    delivery_end_ms: Vec<i64>,
    duration_seconds: Vec<i32>,
    resolution_seconds: Vec<i32>,
    predefined: Vec<bool>,
    updated_at_ms: Vec<i64>,
    delivery_area_id: Vec<Option<i32>>,
    contract_state: Vec<Option<String>>,
    open_at_ms: Vec<Option<i64>>,
    closed_at_ms: Vec<Option<i64>>,
    rows: u64,
}

impl ContractWriter {
    pub fn create(output: &Path) -> anyhow::Result<Self> {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", time_utc_nanos(), false),
            Field::new("is_snapshot", DataType::Boolean, false),
            Field::new("contract_id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
            Field::new("market_id", DataType::Utf8, false),
            Field::new("market_type", DataType::Utf8, false),
            Field::new("product_type", DataType::Utf8, false),
            Field::new("delivery_start", time_utc_millis(), false),
            Field::new("delivery_end", time_utc_millis(), false),
            Field::new("duration_seconds", DataType::Int32, false),
            Field::new("resolution_seconds", DataType::Int32, false),
            Field::new("predefined", DataType::Boolean, false),
            Field::new("updated_at", time_utc_millis(), false),
            Field::new("delivery_area_id", DataType::Int32, true),
            Field::new("contract_state", DataType::Utf8, true),
            Field::new("open_at", time_utc_millis(), true),
            Field::new("closed_at", time_utc_millis(), true),
        ]));
        let file =
            File::create(output).with_context(|| format!("creating parquet file {output:?}"))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .build();
        let writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .context("creating parquet writer")?;
        Ok(Self {
            writer,
            schema,
            timestamp_nanos: Vec::new(),
            is_snapshot: Vec::new(),
            contract_id: Vec::new(),
            name: Vec::new(),
            market_id: Vec::new(),
            market_type: Vec::new(),
            product_type: Vec::new(),
            delivery_start_ms: Vec::new(),
            delivery_end_ms: Vec::new(),
            duration_seconds: Vec::new(),
            resolution_seconds: Vec::new(),
            predefined: Vec::new(),
            updated_at_ms: Vec::new(),
            delivery_area_id: Vec::new(),
            contract_state: Vec::new(),
            open_at_ms: Vec::new(),
            closed_at_ms: Vec::new(),
            rows: 0,
        })
    }

    /// Append a contract — one row per delivery area, or a single area-null row when it has
    /// none — flushing a batch once the buffer fills.
    pub fn push_contract(
        &mut self,
        timestamp: DateTime<Utc>,
        is_snapshot: bool,
        c: &Contract,
    ) -> anyhow::Result<()> {
        if c.delivery_area_states.is_empty() {
            self.push_row(timestamp, is_snapshot, c, None);
        } else {
            for area in &c.delivery_area_states {
                self.push_row(timestamp, is_snapshot, c, Some(area));
            }
        }
        if self.timestamp_nanos.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    fn push_row(
        &mut self,
        timestamp: DateTime<Utc>,
        is_snapshot: bool,
        c: &Contract,
        area: Option<&ContractDeliveryAreaState>,
    ) {
        self.timestamp_nanos.push(nanos(timestamp));
        self.is_snapshot.push(is_snapshot);
        self.contract_id.push(c.contract_id);
        self.name.push(c.contract_name.clone());
        self.market_id.push(c.market_id.clone());
        self.market_type.push(market_type_label(c.market_type));
        self.product_type.push(product_type_label(c.product_type));
        self.delivery_start_ms.push(c.delivery_start_ms);
        self.delivery_end_ms.push(c.delivery_end_ms);
        self.duration_seconds.push(c.duration_seconds);
        self.resolution_seconds.push(c.resolution_seconds);
        self.predefined.push(c.predefined);
        self.updated_at_ms.push(c.updated_at_ms);
        self.delivery_area_id.push(area.map(|a| a.delivery_area_id));
        self.contract_state
            .push(area.map(|a| contract_state_label(a.state)));
        self.open_at_ms.push(area.map(|a| a.open_at_ms));
        self.closed_at_ms.push(area.map(|a| a.closed_at_ms));
    }

    fn flush(&mut self) -> anyhow::Result<()> {
        if self.timestamp_nanos.is_empty() {
            return Ok(());
        }
        let columns: Vec<ArrayRef> = vec![
            Arc::new(
                TimestampNanosecondArray::from(std::mem::take(&mut self.timestamp_nanos))
                    .with_timezone(UTC_TZ),
            ),
            Arc::new(BooleanArray::from(std::mem::take(&mut self.is_snapshot))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.contract_id))),
            Arc::new(StringArray::from_iter_values(std::mem::take(
                &mut self.name,
            ))),
            Arc::new(StringArray::from_iter_values(std::mem::take(
                &mut self.market_id,
            ))),
            Arc::new(StringArray::from_iter_values(std::mem::take(
                &mut self.market_type,
            ))),
            Arc::new(StringArray::from_iter_values(std::mem::take(
                &mut self.product_type,
            ))),
            Arc::new(
                TimestampMillisecondArray::from(std::mem::take(&mut self.delivery_start_ms))
                    .with_timezone(UTC_TZ),
            ),
            Arc::new(
                TimestampMillisecondArray::from(std::mem::take(&mut self.delivery_end_ms))
                    .with_timezone(UTC_TZ),
            ),
            Arc::new(Int32Array::from(std::mem::take(&mut self.duration_seconds))),
            Arc::new(Int32Array::from(std::mem::take(
                &mut self.resolution_seconds,
            ))),
            Arc::new(BooleanArray::from(std::mem::take(&mut self.predefined))),
            Arc::new(
                TimestampMillisecondArray::from(std::mem::take(&mut self.updated_at_ms))
                    .with_timezone(UTC_TZ),
            ),
            Arc::new(Int32Array::from(std::mem::take(&mut self.delivery_area_id))),
            Arc::new(StringArray::from_iter(std::mem::take(
                &mut self.contract_state,
            ))),
            Arc::new(
                TimestampMillisecondArray::from(std::mem::take(&mut self.open_at_ms))
                    .with_timezone(UTC_TZ),
            ),
            Arc::new(
                TimestampMillisecondArray::from(std::mem::take(&mut self.closed_at_ms))
                    .with_timezone(UTC_TZ),
            ),
        ];
        let batch =
            RecordBatch::try_new(self.schema.clone(), columns).context("building record batch")?;
        self.rows += batch.num_rows() as u64;
        self.writer.write(&batch).context("writing record batch")?;
        Ok(())
    }

    /// Flush any buffered rows, close the file (writing the Parquet footer), and return the
    /// total rows written.
    pub fn finish(mut self) -> anyhow::Result<u64> {
        self.flush()?;
        self.writer.close().context("closing parquet writer")?;
        Ok(self.rows)
    }
}

/// The proto enum's canonical name (e.g. `MARKET_TYPE_LOCAL`), or the raw integer for a value
/// our compiled schema doesn't know.
fn market_type_label(raw: i32) -> String {
    pmd::MarketType::try_from(raw).map_or_else(|_| raw.to_string(), |e| e.as_str_name().to_string())
}

pub(crate) fn product_type_label(raw: i32) -> String {
    pmd::ProductType::try_from(raw)
        .map_or_else(|_| raw.to_string(), |e| e.as_str_name().to_string())
}

fn contract_state_label(raw: i32) -> String {
    pmd::ContractState::try_from(raw)
        .map_or_else(|_| raw.to_string(), |e| e.as_str_name().to_string())
}
