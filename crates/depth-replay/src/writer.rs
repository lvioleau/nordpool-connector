//! Parquet sink for reconstructed depth levels. **Wide format**: one row per emitted book
//! snapshot, with each side's price levels carried as list columns (`bid_price`,
//! `bid_quantity`, `bid_order_count`, `bid_created_at`, `bid_updated_at` and the `ask_*`
//! equivalents), index 0 = best. The caller has already capped each side to the configured
//! number of levels, so the lists hold at most that many entries. The two per-level stamps are
//! our receive time for the frame that opened the level and for the one that last touched it —
//! the same values the live depth feed publishes, on the same clock as the row's `timestamp`.
//!
//! A row identifies its book the way the domain does — `eic_code` + `market_type` +
//! `contract_id`, the delivery area plus the two halves of a `BookKey`. The area is constant
//! for a file (a replay covers one) but written per row all the same, so rows stay
//! self-describing once several files are concatenated; both string columns are one repeated
//! value per file and dictionary-encode away to nothing.
//!
//! Rows are buffered into column builders and, once a batch fills, the assembled
//! `RecordBatch` is handed over a bounded channel to a dedicated writer thread that owns the
//! `ArrowWriter` and does the Arrow→Parquet encode + compression. That encode is the dominant
//! replay cost, so running it on its own core lets it overlap the (sequential) book
//! reconstruction on the main thread. The bounded channel keeps memory in check and applies
//! backpressure when the writer falls behind.

use std::fs::File;
use std::path::Path;
use std::sync::Arc;
use std::thread::JoinHandle;

use anyhow::Context;
use arrow::array::{
    ArrayRef, Int64Array, Int64Builder, ListBuilder, RecordBatch, StringArray,
    TimestampNanosecondArray, TimestampNanosecondBuilder, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema};
use chrono::{DateTime, Utc};
use crossbeam_channel::{Sender, bounded};

use crate::parquet_time::{UTC_TZ, nanos, time_utc_nanos};
use nordpool_marketdata::{BookKey, EicCode, PriceLevel};
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{EnabledStatistics, WriterProperties};

const BATCH_ROWS: usize = 8192;
/// Bounded queue of assembled batches awaiting encode. Small: a couple in flight is enough to
/// overlap encode with the fold without letting memory grow.
const CHANNEL_CAPACITY: usize = 8;

/// A non-nullable list column of `dt` whose child field matches what `ListBuilder` emits
/// (`new_list_field` → child named `"item"`), so `RecordBatch::try_new` accepts the arrays.
fn list_of(dt: DataType) -> DataType {
    DataType::List(Arc::new(Field::new_list_field(dt, true)))
}

pub struct DepthWriter {
    /// Sends assembled batches to the writer thread; `None` once `finish` has closed it.
    tx: Option<Sender<RecordBatch>>,
    /// The writer thread: encodes batches and writes the Parquet file, returning on channel
    /// close. `None` once joined.
    handle: Option<JoinHandle<anyhow::Result<()>>>,
    schema: Arc<Schema>,
    /// The row's time column, buffered as the unix nanos Arrow builds its array from; it gains
    /// its timestamp type and UTC zone when that array is built. The per-level stamps instead
    /// ride typed builders (see [`SideBuilders`]).
    timestamp_nanos: Vec<i64>,
    /// The book's delivery area and market type, as their stable domain identifiers
    /// (`EicCode::eic_code` / `MarketType::as_label`) — `&'static str`, so a row costs no
    /// allocation.
    eic_code: Vec<&'static str>,
    market_type: Vec<&'static str>,
    contract_id: Vec<i64>,
    revision: Vec<i64>,
    bids: SideBuilders,
    asks: SideBuilders,
    rows: u64,
}

/// The five list columns describing one side of a row's book: each level's price, total
/// quantity, order count, and the two receive-time stamps. Held together so a row appends to
/// all five as a unit — a side whose columns fell out of step would misalign every later row.
struct SideBuilders {
    price: ListBuilder<Int64Builder>,
    quantity: ListBuilder<Int64Builder>,
    order_count: ListBuilder<UInt32Builder>,
    created_at: ListBuilder<TimestampNanosecondBuilder>,
    updated_at: ListBuilder<TimestampNanosecondBuilder>,
}

impl SideBuilders {
    fn new() -> Self {
        Self {
            price: ListBuilder::new(Int64Builder::new()),
            quantity: ListBuilder::new(Int64Builder::new()),
            order_count: ListBuilder::new(UInt32Builder::new()),
            created_at: ListBuilder::new(TimestampNanosecondBuilder::new().with_timezone(UTC_TZ)),
            updated_at: ListBuilder::new(TimestampNanosecondBuilder::new().with_timezone(UTC_TZ)),
        }
    }

    /// The schema's five fields for this side, `<side>_price` through `<side>_updated_at` — the
    /// order [`finish`](Self::finish) returns its arrays in.
    fn fields(side: &str) -> Vec<Field> {
        vec![
            Field::new(format!("{side}_price"), list_of(DataType::Int64), false),
            Field::new(format!("{side}_quantity"), list_of(DataType::Int64), false),
            Field::new(
                format!("{side}_order_count"),
                list_of(DataType::UInt32),
                false,
            ),
            Field::new(
                format!("{side}_created_at"),
                list_of(time_utc_nanos()),
                false,
            ),
            Field::new(
                format!("{side}_updated_at"),
                list_of(time_utc_nanos()),
                false,
            ),
        ]
    }

    /// Append one row's levels for this side (already capped to the configured depth,
    /// best-first). An empty side appends five empty lists, not nulls.
    fn push(&mut self, levels: &[PriceLevel]) {
        for level in levels {
            self.price.values().append_value(level.price);
            self.quantity.values().append_value(level.total_quantity);
            self.order_count.values().append_value(level.order_count);
            self.created_at
                .values()
                .append_value(nanos(level.created_at));
            self.updated_at
                .values()
                .append_value(nanos(level.updated_at));
        }
        self.price.append(true);
        self.quantity.append(true);
        self.order_count.append(true);
        self.created_at.append(true);
        self.updated_at.append(true);
    }

    /// Take the buffered rows as the five column arrays, in [`fields`](Self::fields) order.
    fn finish(&mut self) -> Vec<ArrayRef> {
        vec![
            Arc::new(self.price.finish()),
            Arc::new(self.quantity.finish()),
            Arc::new(self.order_count.finish()),
            Arc::new(self.created_at.finish()),
            Arc::new(self.updated_at.finish()),
        ]
    }
}

impl DepthWriter {
    pub fn create(output: &Path, compression: Compression) -> anyhow::Result<Self> {
        let mut fields = vec![
            Field::new("timestamp", time_utc_nanos(), false),
            Field::new("eic_code", DataType::Utf8, false),
            Field::new("market_type", DataType::Utf8, false),
            Field::new("contract_id", DataType::Int64, false),
            Field::new("revision", DataType::Int64, false),
        ];
        fields.extend(SideBuilders::fields("bid"));
        fields.extend(SideBuilders::fields("ask"));
        let schema = Arc::new(Schema::new(fields));
        let file =
            File::create(output).with_context(|| format!("creating parquet file {output:?}"))?;
        let props = WriterProperties::builder()
            .set_compression(compression)
            .set_statistics_enabled(EnabledStatistics::None)
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props))
            .context("creating parquet writer")?;

        let (tx, rx) = bounded::<RecordBatch>(CHANNEL_CAPACITY);
        let handle = std::thread::Builder::new()
            .name("depth-parquet-writer".into())
            .spawn(move || -> anyhow::Result<()> {
                for batch in rx {
                    writer.write(&batch).context("writing record batch")?;
                }
                writer.close().context("closing parquet writer")?;
                Ok(())
            })
            .context("spawning parquet writer thread")?;

        Ok(Self {
            tx: Some(tx),
            handle: Some(handle),
            schema,
            timestamp_nanos: Vec::new(),
            eic_code: Vec::new(),
            market_type: Vec::new(),
            contract_id: Vec::new(),
            revision: Vec::new(),
            bids: SideBuilders::new(),
            asks: SideBuilders::new(),
            rows: 0,
        })
    }

    /// Append one row for the book `key` in delivery area `area`: its bid and ask levels become
    /// the row's list columns, flushing a batch once the buffer fills. The caller has already
    /// capped each side to the configured number of levels.
    pub fn push_levels(
        &mut self,
        timestamp: DateTime<Utc>,
        area: EicCode,
        key: BookKey,
        revision: i64,
        bids: &[PriceLevel],
        asks: &[PriceLevel],
    ) -> anyhow::Result<()> {
        self.timestamp_nanos.push(nanos(timestamp));
        self.eic_code.push(area.eic_code());
        self.market_type.push(key.market_type.as_label());
        self.contract_id.push(key.contract_id);
        self.revision.push(revision);

        self.bids.push(bids);
        self.asks.push(asks);

        if self.timestamp_nanos.len() >= BATCH_ROWS {
            self.flush()?;
        }
        Ok(())
    }

    /// Assemble the buffered rows into a `RecordBatch` and hand it to the writer thread.
    fn flush(&mut self) -> anyhow::Result<()> {
        if self.timestamp_nanos.is_empty() {
            return Ok(());
        }
        let mut columns: Vec<ArrayRef> = vec![
            Arc::new(
                TimestampNanosecondArray::from(std::mem::take(&mut self.timestamp_nanos))
                    .with_timezone(UTC_TZ),
            ),
            Arc::new(StringArray::from(std::mem::take(&mut self.eic_code))),
            Arc::new(StringArray::from(std::mem::take(&mut self.market_type))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.contract_id))),
            Arc::new(Int64Array::from(std::mem::take(&mut self.revision))),
        ];
        columns.extend(self.bids.finish());
        columns.extend(self.asks.finish());
        let batch =
            RecordBatch::try_new(self.schema.clone(), columns).context("building record batch")?;
        self.rows += batch.num_rows() as u64;
        // A send error means the writer thread exited early (it only drops the receiver on
        // error/panic); join it to surface the real cause rather than a generic send error.
        if self.tx.as_ref().expect("writer open").send(batch).is_err() {
            return Err(self.join_writer().unwrap_or_else(|| {
                anyhow::anyhow!("parquet writer thread stopped before all batches were written")
            }));
        }
        Ok(())
    }

    /// Join the writer thread, returning its error (or panic) if it failed, `None` on success.
    fn join_writer(&mut self) -> Option<anyhow::Error> {
        match self.handle.take()?.join() {
            Ok(Ok(())) => None,
            Ok(Err(e)) => Some(e),
            Err(_) => Some(anyhow::anyhow!("parquet writer thread panicked")),
        }
    }

    /// Flush any buffered rows, close the channel so the writer thread writes the footer and
    /// exits, then join it. Returns the total rows written (one per emitted book snapshot).
    pub fn finish(mut self) -> anyhow::Result<u64> {
        self.flush()?;
        // Dropping the sender ends the writer thread's `for batch in rx` loop.
        self.tx.take();
        if let Some(err) = self.join_writer() {
            return Err(err);
        }
        Ok(self.rows)
    }
}
