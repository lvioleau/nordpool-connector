//! Offline replay of the connector's binary archive into Parquet. Reads the gzipped
//! TFRecord-framed `TimestampedFrame` segments from an object store — `s3://`, `file://`, or a
//! local path — under the `<frame-type>[/<date>]` partition the uploader writes, and, depending
//! on `--frame-type`:
//!
//! - `localview` — reconstructs each order book through the same translation the live
//!   connector uses and dumps the top `--levels` price levels per book, throttled to at most
//!   one snapshot per book per `--throttle-ms` (event-time, from each frame's archived
//!   receive timestamp).
//! - `contract` — dumps the contract reference data verbatim, one row per contract delivery
//!   area, with the frame's receive time and snapshot/delta flag.
//! - `delivery-area` — summarizes the delivery-area reference data: folds only the latest
//!   snapshot-led run into the current set of areas and writes one CSV row per area (the one
//!   mode whose output is CSV, not Parquet).
//!
//! A batch tool, not a service: it reads a fixed set of segments, writes one output file,
//! and exits. Configuration is CLI-only (no INI).

mod contract_writer;
mod delivery_area;
mod parquet_time;
mod reader;
mod replay;
mod writer;

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Context;
use clap::{Parser, ValueEnum};
use nordpool_connector::segment_prefix;
use nordpool_marketdata::EicCode;
use parquet::basic::{Compression, ZstdLevel};

use replay::Stats;

fn mib(bytes: u64) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

/// Resolve a `--eic-code` argument to its delivery area, rejecting an EIC the domain doesn't know.
fn parse_eic_code(raw: &str) -> Result<EicCode, String> {
    EicCode::from_eic(raw).ok_or_else(|| format!("unknown ENTSO-E EIC code {raw:?}"))
}

/// Which archived frame type to replay. Selects the expected segment contents and the output
/// written (Parquet, or CSV for the delivery-area summary).
#[derive(Clone, Copy, PartialEq, ValueEnum)]
enum FrameType {
    /// Reconstruct order books and dump top-of-book depth levels.
    Localview,
    /// Dump contract reference data, one row per delivery area.
    Contract,
    /// Summarize the current delivery areas from the latest snapshot-led run, one CSV row
    /// per area.
    DeliveryArea,
}

impl FrameType {
    fn label(self) -> &'static str {
        match self {
            FrameType::Localview => "localview",
            FrameType::Contract => "contract",
            FrameType::DeliveryArea => "delivery_area",
        }
    }
}

/// Parquet compression codec for the localview output. Faster codecs trade a larger file for
/// less CPU in the write path, which dominates the replay at high `--levels`.
#[derive(Clone, Copy, ValueEnum)]
enum Codec {
    /// zstd level 1: smallest file, but the heaviest of these on CPU.
    Zstd,
    /// Snappy: noticeably faster than zstd, larger file.
    Snappy,
    /// LZ4 (raw): fastest codec, largest compressed file.
    Lz4,
    /// No compression: lowest CPU, largest file by far.
    None,
}

impl Codec {
    fn to_parquet(self) -> Compression {
        match self {
            Codec::Zstd => Compression::ZSTD(ZstdLevel::default()),
            Codec::Snappy => Compression::SNAPPY,
            Codec::Lz4 => Compression::LZ4_RAW,
            Codec::None => Compression::UNCOMPRESSED,
        }
    }
}

#[derive(Parser)]
#[command(about = "Replay the connector's binary archive into Parquet")]
struct Args {
    /// The archive root: `s3://bucket[/prefix]`, `file:///abs/path`, or a local directory. The
    /// segments to replay are located under `<input>/<frame-type>[/<date>]`, read in key order
    /// (chronological) with any still-open `*.partial` last. S3 credentials and region come from the
    /// environment (AWS_* / the ECS task role), like the connector's uploader.
    #[arg(long)]
    input: String,

    /// UTC day to replay, `YYYY-MM-DD`. Narrows the listing to that one day's partition; omit to
    /// replay every archived day for the frame type.
    #[arg(long)]
    date: Option<String>,

    /// Output file path (overwritten if it exists): Parquet, or CSV for
    /// `--frame-type delivery-area`.
    #[arg(long)]
    output: PathBuf,

    /// Which frame type the input holds, selecting the output schema.
    #[arg(long, value_enum, default_value_t = FrameType::Localview)]
    frame_type: FrameType,

    /// (localview only) The delivery area to replay, as its ENTSO-E EIC code (e.g.
    /// `10YFI-1--------U`). A replay reconstructs exactly one area — the archive multiplexes
    /// every subscribed area into one stream, and the output rows carry no area — so frames for
    /// the others are skipped. Omit it for a single-area archive: the first frame's area is used
    /// and a second one fails the run rather than conflating the two.
    #[arg(long, value_parser = parse_eic_code)]
    eic_code: Option<EicCode>,

    /// (localview only) Minimum event-time gap, in milliseconds, between emitted snapshots
    /// for a given book. A book emits its first update immediately, then at most once per
    /// interval. 0 emits every book-changing frame.
    #[arg(long, default_value_t = 1000)]
    throttle_ms: u64,

    /// (localview only) Number of price levels per side to write. Default matches the live
    /// feed's default depth (`[server] depth`); larger reaches deeper into the book.
    #[arg(long, default_value_t = 20)]
    levels: usize,

    /// (localview only) Parquet compression codec for the output.
    #[arg(long, value_enum, default_value_t = Codec::Zstd)]
    compression: Codec,
}

fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();

    // One current-thread runtime drives the (async) object store; the replay itself is sync.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("building tokio runtime")?;
    let (store, base) = reader::build_object_store(&args.input)
        .with_context(|| format!("opening archive at {:?}", args.input))?;

    // Locate segments under <base>/<type>[/<date>] using the connector's shared layout helper, so
    // the writer and reader can never disagree on the partition order. A missing --date stops at the
    // type level and lists all days.
    let prefix = segment_prefix(&base, args.frame_type.label(), args.date.as_deref());
    let segments = reader::list_segments(&rt, store.as_ref(), &prefix)
        .with_context(|| format!("listing segments under {prefix}"))?;
    if segments.is_empty() {
        anyhow::bail!("no *.tfrecord.gz or *.tfrecord.partial segments found under {prefix}");
    }

    // Every replay must begin at a snapshot: the archive resets the segment counter to 0 on a
    // snapshot, so a counter other than 000 means the range begins mid-stream with no base state.
    // The delivery-area summary only wants the *current* state, so it reads just the newest
    // snapshot-led run; the replaying modes instead consume the whole range and require its first
    // segment to be snapshot-led — for a single `--date` that guards the day, for all days the
    // very first segment, which is all the continuous reconstruction needs.
    let segments = if args.frame_type == FrameType::DeliveryArea {
        reader::latest_snapshot_run(segments)?
    } else {
        let first = segments[0].filename().unwrap_or_default();
        match reader::segment_index(first) {
            Some(0) => {}
            other => anyhow::bail!(
                "first segment {first:?} is not snapshot-led (expected counter 000, got {other:?}) — \
                 the replay range does not start with a snapshot"
            ),
        }
        segments
    };

    tracing::info!(
        frame_type = args.frame_type.label(),
        date = args.date.as_deref().unwrap_or("(all)"),
        prefix = %prefix,
        segments = segments.len(),
        output = ?args.output,
        "starting replay"
    );

    let started = Instant::now();
    let stats = match args.frame_type {
        FrameType::Localview => replay::run(
            &rt,
            store.as_ref(),
            &segments,
            &args.output,
            replay::DepthOptions {
                area: args.eic_code,
                throttle: Duration::from_millis(args.throttle_ms),
                levels: args.levels,
                compression: args.compression.to_parquet(),
            },
        )?,
        FrameType::Contract => replay::run_contracts(&rt, store.as_ref(), &segments, &args.output)?,
        FrameType::DeliveryArea => {
            delivery_area::run(&rt, store.as_ref(), &segments, &args.output)?
        }
    };
    log_complete(args.frame_type, &stats, started.elapsed());
    Ok(())
}

/// Emit the completion summary with volumes and throughput. `entities` is books (localview),
/// distinct contracts (contract), or delivery areas (delivery-area).
fn log_complete(frame_type: FrameType, stats: &Stats, elapsed: Duration) {
    // Guard the divisor so a sub-microsecond run doesn't report infinite rates.
    let secs = elapsed.as_secs_f64().max(1e-9);
    tracing::info!(
        frame_type = frame_type.label(),
        frames = stats.frames,
        rows = stats.rows,
        entities = stats.entities,
        read_mib = format!("{:.1}", mib(stats.bytes_read)),
        written_mib = format!("{:.1}", mib(stats.bytes_written)),
        elapsed_s = format!("{:.2}", elapsed.as_secs_f64()),
        read_mib_per_s = format!("{:.1}", mib(stats.bytes_read) / secs),
        written_mib_per_s = format!("{:.1}", mib(stats.bytes_written) / secs),
        frames_per_s = format!("{:.0}", stats.frames as f64 / secs),
        rows_per_s = format!("{:.0}", stats.rows as f64 / secs),
        "replay complete"
    );
}

#[cfg(test)]
mod tests {
    use std::fs::{self, File};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use nordpool_connector::proto::archive::{SegmentHeader, TimestampedFrame};
    use nordpool_connector::proto::pmd::server_frame::Payload as ServerPayload;
    use nordpool_connector::proto::pmd::{
        Contract, ContractDeliveryAreaState, ContractPayload, DeliveryArea, DeliveryAreaPayload,
        LocalView, LocalViewPayload, Order, ServerFrame,
    };
    use nordpool_marketdata::EicCode;
    use object_store::ObjectStore;
    use object_store::local::LocalFileSystem;
    use object_store::path::Path as ObjectPath;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::basic::Compression;
    use prost::Message;
    use tokio::runtime::Runtime;

    use crate::{delivery_area, replay};

    fn runtime() -> Runtime {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
    }

    /// A `LocalFileSystem` object store rooted at `dir`, so a segment written there is addressed
    /// by its bare filename (the same shape `list_segments` returns).
    fn store_for(dir: &std::path::Path) -> Arc<dyn ObjectStore> {
        Arc::new(LocalFileSystem::new_with_prefix(dir).unwrap())
    }

    fn temp_dir(tag: &str) -> PathBuf {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let dir =
            std::env::temp_dir().join(format!("nordpool-replay-{tag}-{}-{n}", std::process::id()));
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn order(id: i64, price: i64, quantity: i32) -> Order {
        Order {
            order_id: id,
            price,
            quantity,
            created_at_ms: 0,
        }
    }

    /// One single-book LocalView frame (LOCAL, contract 111, area 40).
    fn local_view_frame(
        is_snapshot: bool,
        revision: i64,
        buys: Vec<Order>,
        sells: Vec<Order>,
    ) -> ServerFrame {
        local_view_frame_in(40, 111, is_snapshot, revision, buys, sells)
    }

    /// The same, for an explicit delivery area and contract — the multiplexed shape the archive
    /// holds when the connector subscribes to several areas.
    fn local_view_frame_in(
        delivery_area_id: i32,
        contract_id: i64,
        is_snapshot: bool,
        revision: i64,
        buys: Vec<Order>,
        sells: Vec<Order>,
    ) -> ServerFrame {
        ServerFrame {
            payload: Some(ServerPayload::LocalView(LocalViewPayload {
                is_snapshot,
                delivery_area_id,
                local_views: vec![LocalView {
                    market_type: 1,
                    contract_id,
                    revision_no: revision,
                    buy_orders: buys,
                    sell_orders: sells,
                }],
            })),
        }
    }

    fn tframe(at: SystemTime, frame: ServerFrame) -> TimestampedFrame {
        TimestampedFrame {
            timestamp: Some(at.into()),
            frame: frame.encode_to_vec(),
        }
    }

    /// Write frames to one gzipped TFRecord `.tfrecord.gz` segment under `dir`, as the archive
    /// would (a version-1 `SegmentHeader` record first), and return its object key relative to a
    /// `store_for(dir)` store (its bare filename).
    fn write_segment(dir: &std::path::Path, frames: &[TimestampedFrame]) -> ObjectPath {
        let name = "localview-0-000.tfrecord.gz";
        let header = SegmentHeader {
            version: nordpool_connector::tfrecord::FORMAT_VERSION,
            frame_type: "localview".into(),
            opened_at: None,
            seq: 0,
        };
        let mut buf = nordpool_connector::tfrecord::encode_record(&header.encode_to_vec());
        for frame in frames {
            buf.extend_from_slice(&nordpool_connector::tfrecord::encode_record(
                &frame.encode_to_vec(),
            ));
        }
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        std::io::Write::write_all(&mut encoder, &buf).unwrap();
        fs::write(dir.join(name), encoder.finish().unwrap()).unwrap();
        ObjectPath::from(name)
    }

    /// Every row's value of a string column, in file order.
    fn read_parquet_strings(path: &std::path::Path, column: &str) -> Vec<String> {
        use arrow::array::{Array, StringArray};
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let mut values = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let array = batch
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .clone();
            values.extend((0..array.len()).map(|i| array.value(i).to_string()));
        }
        values
    }

    /// Every row's `contract_id`, in file order.
    fn read_parquet_contract_ids(path: &std::path::Path) -> Vec<i64> {
        use arrow::array::Int64Array;
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let mut ids = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let column = batch
                .column_by_name("contract_id")
                .unwrap()
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .clone();
            ids.extend(column.values());
        }
        ids
    }

    fn read_parquet_rows(path: &std::path::Path) -> usize {
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        reader.map(|b| b.unwrap().num_rows()).sum()
    }

    /// The `(bid_levels, ask_levels)` list length of each wide-format row, in file order.
    fn read_parquet_level_counts(path: &std::path::Path) -> Vec<(usize, usize)> {
        use arrow::array::{Array, ListArray};
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let mut counts = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let list = |name: &str| {
                batch
                    .column_by_name(name)
                    .unwrap()
                    .as_any()
                    .downcast_ref::<ListArray>()
                    .unwrap()
                    .clone()
            };
            let (bid, ask) = (list("bid_price"), list("ask_price"));
            for i in 0..batch.num_rows() {
                counts.push((bid.value(i).len(), ask.value(i).len()));
            }
        }
        counts
    }

    #[test]
    fn throttle_emits_one_snapshot_per_book_per_interval() {
        // Given a snapshot at t=0 and two deltas, one inside the 1s window and one past it.
        let dir = temp_dir("throttle");
        let t0 = UNIX_EPOCH;
        let within = UNIX_EPOCH + Duration::from_millis(500);
        let past = UNIX_EPOCH + Duration::from_millis(1200);
        let segment = write_segment(
            &dir,
            &[
                tframe(
                    t0,
                    local_view_frame(true, 1, vec![order(1, 100, 5)], vec![order(2, 110, 3)]),
                ),
                tframe(
                    within,
                    local_view_frame(false, 2, vec![order(3, 99, 4)], vec![]),
                ),
                tframe(
                    past,
                    local_view_frame(false, 3, vec![order(4, 98, 2)], vec![]),
                ),
            ],
        );
        let out = dir.join("out.parquet");

        // When the archive is replayed with a 1s throttle.
        let stats = replay::run(
            &runtime(),
            store_for(&dir).as_ref(),
            &[segment],
            &out,
            replay::DepthOptions {
                area: None,
                throttle: Duration::from_millis(1000),
                levels: 20,
                compression: Compression::UNCOMPRESSED,
            },
        )
        .unwrap();

        // Then the mid-window delta is suppressed: only the snapshot (t=0) and the past-window
        // delta (t=1200) emit — one wide row each. The snapshot row has 1 bid + 1 ask; the
        // second has 3 bids (prices 100, 99, 98) + 1 ask.
        assert_eq!(stats.frames, 3);
        assert_eq!(stats.entities, 1);
        assert_eq!(stats.rows, 2);
        assert_eq!(read_parquet_rows(&out), 2);
        assert_eq!(read_parquet_level_counts(&out), vec![(1, 1), (3, 1)]);
        // Byte volumes are populated for the throughput log: input bytes read, Parquet bytes
        // written (== the output file size).
        assert!(stats.bytes_read > 0);
        assert_eq!(stats.bytes_written, fs::metadata(&out).unwrap().len());

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn zero_throttle_emits_every_book_changing_frame() {
        // Given one book's snapshot followed by two deltas, each 1ms apart — closer together
        // than any sane throttle window, so they only all survive if throttling is disabled.
        let dir = temp_dir("nothrottle");
        let segment = write_segment(
            &dir,
            &[
                tframe(
                    UNIX_EPOCH,
                    local_view_frame(true, 1, vec![order(1, 100, 5)], vec![]),
                ),
                tframe(
                    UNIX_EPOCH + Duration::from_millis(1),
                    local_view_frame(false, 2, vec![order(2, 99, 4)], vec![]),
                ),
                tframe(
                    UNIX_EPOCH + Duration::from_millis(2),
                    local_view_frame(false, 3, vec![order(3, 98, 2)], vec![]),
                ),
            ],
        );
        let out = dir.join("out.parquet");

        // When replayed with a zero throttle, which disables the per-book event-time gate so
        // every book-changing frame is allowed to emit a row.
        let stats = replay::run(
            &runtime(),
            store_for(&dir).as_ref(),
            &[segment],
            &out,
            replay::DepthOptions {
                area: None,
                throttle: Duration::ZERO,
                levels: 20,
                compression: Compression::UNCOMPRESSED,
            },
        )
        .unwrap();

        // Then every frame emits one wide row, with the book's growing bid depth (1, 2, 3 levels)
        // and no asks.
        assert_eq!(stats.rows, 3);
        assert_eq!(read_parquet_rows(&out), 3);
        assert_eq!(
            read_parquet_level_counts(&out),
            vec![(1, 0), (2, 0), (3, 0)]
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn levels_caps_the_written_depth_per_side() {
        // Given a snapshot whose book has 5 distinct bid price levels.
        let dir = temp_dir("levels");
        let buys = vec![
            order(1, 100, 5),
            order(2, 99, 5),
            order(3, 98, 5),
            order(4, 97, 5),
            order(5, 96, 5),
        ];
        let segment = write_segment(
            &dir,
            &[tframe(UNIX_EPOCH, local_view_frame(true, 1, buys, vec![]))],
        );
        let out = dir.join("out.parquet");

        // When replayed with `--levels 2`, requesting only the top 2 price levels per side —
        // a shallower projection than the book actually holds.
        let stats = replay::run(
            &runtime(),
            store_for(&dir).as_ref(),
            &[segment],
            &out,
            replay::DepthOptions {
                area: None,
                throttle: Duration::ZERO,
                levels: 2,
                compression: Compression::UNCOMPRESSED,
            },
        )
        .unwrap();

        // Then the single wide row caps the bid side at the best 2 levels (no asks present).
        assert_eq!(stats.rows, 1);
        assert_eq!(read_parquet_rows(&out), 1);
        assert_eq!(read_parquet_level_counts(&out), vec![(2, 0)]);

        fs::remove_dir_all(&dir).ok();
    }

    /// Every row's values of a nanosecond-timestamp list column, in file order.
    fn read_parquet_time_lists(path: &std::path::Path, column: &str) -> Vec<Vec<i64>> {
        use arrow::array::{Array, ListArray, TimestampNanosecondArray};
        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
            .unwrap()
            .build()
            .unwrap();
        let mut rows = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let list = batch
                .column_by_name(column)
                .unwrap()
                .as_any()
                .downcast_ref::<ListArray>()
                .unwrap()
                .clone();
            for i in 0..list.len() {
                let values = list.value(i);
                let times = values
                    .as_any()
                    .downcast_ref::<TimestampNanosecondArray>()
                    .unwrap();
                rows.push(times.values().to_vec());
            }
        }
        rows
    }

    #[test]
    fn level_rows_carry_the_open_and_last_touch_times() {
        // Given a level opened by a snapshot received at t=0 and joined by a second order on a
        // delta received at t=1s — the level's creation time must survive the join while its
        // touch time follows the newer frame.
        let dir = temp_dir("level-times");
        let segment = write_segment(
            &dir,
            &[
                tframe(
                    UNIX_EPOCH,
                    local_view_frame(true, 1, vec![order(1, 100, 5)], vec![]),
                ),
                tframe(
                    UNIX_EPOCH + Duration::from_secs(1),
                    local_view_frame(false, 2, vec![order(2, 100, 3)], vec![]),
                ),
            ],
        );
        let out = dir.join("out.parquet");

        // When both frames are replayed with the throttle off, so each emits its own row.
        let stats = replay::run(
            &runtime(),
            store_for(&dir).as_ref(),
            &[segment],
            &out,
            replay::DepthOptions {
                area: None,
                throttle: Duration::ZERO,
                levels: 20,
                compression: Compression::UNCOMPRESSED,
            },
        )
        .unwrap();

        // Then both rows stamp the level's creation at the opening frame (t=0), while the second
        // row's touch time advances to the joining frame — the level-age signal the live depth
        // feed publishes, reproduced offline.
        assert_eq!(stats.rows, 2);
        let second = Duration::from_secs(1).as_nanos() as i64;
        assert_eq!(
            read_parquet_time_lists(&out, "bid_created_at"),
            vec![vec![0], vec![0]]
        );
        assert_eq!(
            read_parquet_time_lists(&out, "bid_updated_at"),
            vec![vec![0], vec![second]]
        );
        // An empty side carries an empty list, not a null.
        assert_eq!(
            read_parquet_time_lists(&out, "ask_created_at"),
            vec![Vec::<i64>::new(), Vec::new()]
        );

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn time_columns_are_utc_timestamps() {
        // Given a replayed archive — any frame will do, the schema is fixed at file creation.
        let dir = temp_dir("utc-columns");
        let segment = write_segment(
            &dir,
            &[tframe(
                UNIX_EPOCH,
                local_view_frame(true, 1, vec![order(1, 100, 5)], vec![]),
            )],
        );
        let out = dir.join("out.parquet");

        // When it is written to Parquet.
        replay::run(
            &runtime(),
            store_for(&dir).as_ref(),
            &[segment],
            &out,
            replay::DepthOptions {
                area: None,
                throttle: Duration::ZERO,
                levels: 20,
                compression: Compression::UNCOMPRESSED,
            },
        )
        .unwrap();

        // Then the row stamp and the per-level stamps are nanosecond timestamps tagged UTC, so a
        // reader yields instants rather than naive local-looking times.
        use arrow::datatypes::{DataType, TimeUnit};
        let utc = DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()));
        let schema = ParquetRecordBatchReaderBuilder::try_new(File::open(&out).unwrap())
            .unwrap()
            .schema()
            .clone();
        assert_eq!(
            schema.field_with_name("timestamp").unwrap().data_type(),
            &utc
        );
        let DataType::List(item) = schema
            .field_with_name("bid_created_at")
            .unwrap()
            .data_type()
        else {
            panic!("bid_created_at is a list column");
        };
        assert_eq!(item.data_type(), &utc);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn rows_identify_a_book_by_area_market_type_and_contract() {
        // Given one snapshot frame in area 40/FR carrying contract 111 twice — once on each
        // market type, which the domain keys as two distinct books. The contract id alone cannot
        // tell those rows apart.
        let dir = temp_dir("book-identity");
        let frame = ServerFrame {
            payload: Some(ServerPayload::LocalView(LocalViewPayload {
                is_snapshot: true,
                delivery_area_id: 40,
                local_views: vec![
                    LocalView {
                        market_type: 1, // LOCAL
                        contract_id: 111,
                        revision_no: 1,
                        buy_orders: vec![order(1, 100, 5)],
                        sell_orders: vec![],
                    },
                    LocalView {
                        market_type: 2, // XBID
                        contract_id: 111,
                        revision_no: 1,
                        buy_orders: vec![order(2, 200, 9)],
                        sell_orders: vec![],
                    },
                ],
            })),
        };
        let segment = write_segment(&dir, &[tframe(UNIX_EPOCH, frame)]);
        let out = dir.join("out.parquet");

        // When the frame is replayed.
        let stats = replay::run(
            &runtime(),
            store_for(&dir).as_ref(),
            &[segment],
            &out,
            replay::DepthOptions {
                area: Some(EicCode::AREA_FR),
                throttle: Duration::ZERO,
                levels: 20,
                compression: Compression::UNCOMPRESSED,
            },
        )
        .unwrap();

        // Then each book gets its own row, and the rows are told apart by their market type —
        // while both carry the replayed delivery area, so a row stays self-describing after
        // several areas' files are concatenated.
        assert_eq!(stats.rows, 2);
        assert_eq!(read_parquet_contract_ids(&out), vec![111, 111]);
        assert_eq!(
            read_parquet_strings(&out, "market_type"),
            vec!["Local", "Xbid"]
        );
        assert_eq!(
            read_parquet_strings(&out, "eic_code"),
            vec![EicCode::AREA_FR.eic_code(); 2]
        );

        fs::remove_dir_all(&dir).ok();
    }

    /// A two-area archive segment: area 40/FR holds contract 111, area 41/CH holds contract 222,
    /// each a snapshot — what the archive looks like whenever the connector subscribes to more
    /// than one delivery area.
    fn two_area_segment(dir: &std::path::Path) -> ObjectPath {
        write_segment(
            dir,
            &[
                tframe(
                    UNIX_EPOCH,
                    local_view_frame_in(40, 111, true, 1, vec![order(1, 100, 5)], vec![]),
                ),
                tframe(
                    UNIX_EPOCH + Duration::from_millis(1),
                    local_view_frame_in(41, 222, true, 1, vec![order(2, 200, 9)], vec![]),
                ),
            ],
        )
    }

    #[test]
    fn a_multi_area_range_without_an_eic_code_is_an_error() {
        // Given an archive range holding two delivery areas — the output rows carry no area and
        // the throttle gate is keyed by book alone, so replaying both would interleave them into
        // indistinguishable rows and let one area's emission suppress the other's.
        let dir = temp_dir("multi-area");
        let segment = two_area_segment(&dir);
        let out = dir.join("out.parquet");

        // When it is replayed without naming an area.
        let err = replay::run(
            &runtime(),
            store_for(&dir).as_ref(),
            &[segment],
            &out,
            replay::DepthOptions {
                area: None,
                throttle: Duration::ZERO,
                levels: 20,
                compression: Compression::UNCOMPRESSED,
            },
        )
        .expect_err("a second delivery area must fail the replay");

        // Then it fails naming both areas and pointing at --eic-code, rather than silently
        // conflating them into one file.
        let message = err.to_string();
        assert!(message.contains("more than one delivery area"), "{message}");
        assert!(message.contains(EicCode::AREA_FR.eic_code()), "{message}");
        assert!(message.contains(EicCode::AREA_CH.eic_code()), "{message}");
        assert!(message.contains("--eic-code"), "{message}");

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_eic_code_replays_only_that_delivery_area() {
        // Given the same two-area range.
        let dir = temp_dir("one-area");
        let segment = two_area_segment(&dir);
        let out = dir.join("out.parquet");

        // When one area is named.
        let stats = replay::run(
            &runtime(),
            store_for(&dir).as_ref(),
            &[segment],
            &out,
            replay::DepthOptions {
                area: Some(EicCode::AREA_FR),
                throttle: Duration::ZERO,
                levels: 20,
                compression: Compression::UNCOMPRESSED,
            },
        )
        .unwrap();

        // Then only that area's frame is folded and written — the other area's book never reaches
        // the output, so every row in the file belongs to the one area asked for.
        assert_eq!(stats.frames, 1);
        assert_eq!(stats.rows, 1);
        assert_eq!(read_parquet_contract_ids(&out), vec![111]);

        fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn an_eic_code_that_matches_nothing_names_the_areas_that_are_there() {
        // Given the two-area range and an area the archive doesn't hold — a mistyped or
        // wrong-connector EIC, which would otherwise just write an empty file.
        let dir = temp_dir("absent-area");
        let segment = two_area_segment(&dir);
        let out = dir.join("out.parquet");

        // When that area is named.
        let err = replay::run(
            &runtime(),
            store_for(&dir).as_ref(),
            &[segment],
            &out,
            replay::DepthOptions {
                area: Some(EicCode::AREA_AT),
                throttle: Duration::ZERO,
                levels: 20,
                compression: Compression::UNCOMPRESSED,
            },
        )
        .expect_err("an area absent from the range must fail the replay");

        // Then the error names the requested area and the ones actually present, so the mistake
        // is obvious from the message alone.
        let message = err.to_string();
        assert!(message.contains(EicCode::AREA_AT.eic_code()), "{message}");
        assert!(message.contains(EicCode::AREA_FR.eic_code()), "{message}");
        assert!(message.contains(EicCode::AREA_CH.eic_code()), "{message}");

        fs::remove_dir_all(&dir).ok();
    }

    fn area_state(id: i32, state: i32) -> ContractDeliveryAreaState {
        ContractDeliveryAreaState {
            delivery_area_id: id,
            state,
            open_at_ms: 0,
            closed_at_ms: 0,
        }
    }

    fn contract(id: i64, areas: Vec<ContractDeliveryAreaState>) -> Contract {
        Contract {
            contract_id: id,
            contract_name: format!("C{id}"),
            market_type: 1,
            market_id: "NP".into(),
            product_type: 3,
            delivery_start_ms: 1000,
            delivery_end_ms: 2000,
            duration_seconds: 3600,
            resolution_seconds: 0,
            predefined: false,
            updated_at_ms: 42,
            delivery_area_states: areas,
        }
    }

    #[test]
    fn contract_frame_type_dumps_one_row_per_delivery_area() {
        // Given a contract snapshot frame: one contract in two areas, one in a single area, one
        // with no areas at all.
        let dir = temp_dir("contracts");
        let frame = ServerFrame {
            payload: Some(ServerPayload::Contract(ContractPayload {
                is_snapshot: true,
                contracts: vec![
                    contract(1, vec![area_state(40, 1), area_state(41, 4)]),
                    contract(2, vec![area_state(40, 1)]),
                    contract(3, vec![]),
                ],
            })),
        };
        let segment = write_segment(&dir, &[tframe(UNIX_EPOCH, frame)]);
        let out = dir.join("contracts.parquet");

        // When replayed in contract mode, which dumps the reference data verbatim (no book
        // reconstruction) — one Parquet row per contract per delivery area.
        let stats =
            replay::run_contracts(&runtime(), store_for(&dir).as_ref(), &[segment], &out).unwrap();

        // Then one row per delivery area (2 + 1) plus one area-null row for the area-less
        // contract = 4, across 3 distinct contracts.
        assert_eq!(stats.frames, 1);
        assert_eq!(stats.entities, 3);
        assert_eq!(stats.rows, 4);
        assert_eq!(read_parquet_rows(&out), 4);

        // And the time columns are UTC-tagged timestamps: our receive time to the nanosecond, the
        // contract's own schedule at the millisecond resolution the wire sends it in.
        use arrow::datatypes::{DataType, TimeUnit};
        let schema = ParquetRecordBatchReaderBuilder::try_new(File::open(&out).unwrap())
            .unwrap()
            .schema()
            .clone();
        let field = |name: &str| schema.field_with_name(name).unwrap().data_type().clone();
        assert_eq!(
            field("timestamp"),
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        let millis_utc = DataType::Timestamp(TimeUnit::Millisecond, Some("UTC".into()));
        for column in ["delivery_start", "delivery_end", "updated_at", "open_at"] {
            assert_eq!(field(column), millis_utc, "column {column}");
        }

        fs::remove_dir_all(&dir).ok();
    }

    fn area(id: i32, code: &str) -> DeliveryArea {
        DeliveryArea {
            delivery_area_id: id,
            eic_code: format!("10Y{code}"),
            currency_code: "EUR".into(),
            area_code: code.into(),
            time_zone: "CET".into(),
            country_iso_code: "NO".into(),
            product_types: vec![1, 3],
        }
    }

    fn area_frame(is_snapshot: bool, areas: Vec<DeliveryArea>) -> ServerFrame {
        ServerFrame {
            payload: Some(ServerPayload::DeliveryArea(DeliveryAreaPayload {
                is_snapshot,
                delivery_areas: areas,
            })),
        }
    }

    #[test]
    fn delivery_area_mode_folds_the_run_into_one_csv_row_per_area() {
        // Given a delivery-area snapshot (areas 41, 40) followed by a delta that renames
        // area 41 and adds area 42.
        let dir = temp_dir("areas");
        let segment = write_segment(
            &dir,
            &[
                tframe(
                    UNIX_EPOCH,
                    area_frame(true, vec![area(41, "NO1"), area(40, "SE4")]),
                ),
                tframe(
                    UNIX_EPOCH + Duration::from_secs(1),
                    area_frame(false, vec![area(41, "NO1X"), area(42, "FI")]),
                ),
            ],
        );
        let out = dir.join("areas.csv");

        // When the run is folded into the delivery-area CSV summary.
        let stats =
            delivery_area::run(&runtime(), store_for(&dir).as_ref(), &[segment], &out).unwrap();

        // Then the CSV holds a header plus one row per surviving area, ordered by id, with
        // the delta's upsert winning over the snapshot for area 41.
        assert_eq!(stats.frames, 2);
        assert_eq!(stats.entities, 3);
        assert_eq!(stats.rows, 3);
        let csv = fs::read_to_string(&out).unwrap();
        let lines: Vec<&str> = csv.lines().collect();
        assert_eq!(
            lines,
            vec![
                "delivery_area_id,area_code,eic_code,currency_code,time_zone,country_iso_code,product_types",
                "40,SE4,10YSE4,EUR,CET,NO,PRODUCT_TYPE_P15_MIN|PRODUCT_TYPE_P60_MIN",
                "41,NO1X,10YNO1X,EUR,CET,NO,PRODUCT_TYPE_P15_MIN|PRODUCT_TYPE_P60_MIN",
                "42,FI,10YFI,EUR,CET,NO,PRODUCT_TYPE_P15_MIN|PRODUCT_TYPE_P60_MIN",
            ]
        );

        fs::remove_dir_all(&dir).ok();
    }
}
