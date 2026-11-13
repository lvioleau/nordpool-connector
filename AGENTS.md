# AGENTS.md

Guidance for AI agents working on this Rust workspace. The README has the user-facing overview; this file has the agent-facing conventions, invariants, and gotchas.

## What this is

The connector reads Nord Pool's intraday PMD binary (protobuf v2) WebSocket API and fans every frame out to two destinations: a raw-frame disk archive (optionally shipped to S3) and an in-process order-book reconstruction, re-published over gRPC as three streaming feeds — top-N depth, contract reference data, and public trading statistics. It subscribes to one or more delivery areas (`[connector] delivery_area`, comma-separated ENTSO-E EIC codes, resolved to the domain `EicCode` enum — Nord Pool's numeric ids exist only at the PMD wire boundary): depth and statistics are reconstructed and served **per area**; the contract store keeps contracts trading in **at least one configured area**, their states trimmed to the configured areas (each subscription filters to its requested areas); the archive's S3 tags use the **first (primary) area**.

Five crates:

- **`connector`** — ingest: WebSocket transport, driver (reconnect/keepalive), sinks, config, binary entry. The only crate that knows the PMD wire format; all wire→domain translation lives in its sinks.
- **`marketdata`** — the pure shared domain: L2 order books + the `DepthView` projection, `ContractStore`, `StatisticStore`, and the `EicCode` delivery-area enum (generated from `delivery_areas.csv`). No protobuf, no IO, no async.
- **`stream-server`** — the gRPC server library: the producer↔server channels (seed + broadcast) and the three streaming services.
- **`depth-replay`** — offline batch tool: reads archive segments back from the object store and dumps depth or contract data to Parquet, or a delivery-area CSV summary.
- **`depth-viewer`** — wasm-only eframe/egui browser viewer over gRPC-Web, built with trunk (served by the compose `viewer` service; `trunk serve` for iteration).

## Build & test

```sh
cargo check                                    # fast type-check, whole workspace
cargo build [--release]
cargo test --workspace                         # all tests (~220, sub-second)
cargo test -p nordpool-connector --lib         # one crate (also: nordpool-marketdata, nordpool-stream-server)
cargo run -p nordpool-depth-replay -- --input s3://bucket --date 2026-06-24 --eic-code 10YFR-RTE------C --output depth.parquet
cargo run -p nordpool-depth-replay -- --input s3://bucket --frame-type contract --output contracts.parquet
cargo run -p nordpool-depth-replay -- --input s3://bucket --frame-type delivery-area --output areas.csv
grpcui -plaintext -port 8082 localhost:50051   # poke a running gRPC server from the browser (grpcurl for CLI); reflection-driven, includes grpc.health.v1
```

Prerequisites: a Rust toolchain (pinned by `rust-toolchain.toml`) and `protoc`. Several binaries exist, so always pass `-p`. The `depth-viewer` is wasm-only — build it with `trunk` from `crates/depth-viewer` (or the compose `viewer` service), never a workspace-wide `cargo build --target wasm32`.

## Module map

```text
Cargo.toml                       workspace — connector, marketdata, stream-server, depth-replay, depth-viewer
nordpool.ini                     all config; values resolve ${ENV} refs on load (NORDPOOL_CONFIG overrides path)
compose.yaml                     dev/test stack for `podman compose`: MinIO (+ bucket bootstrap), dd-agent,
                                 connector, viewer (Caddy + wasm bundle at :8080)
crates/
  connector/                     depends on marketdata + stream-server
    build.rs                     PMD + archive-envelope protobuf codegen (prost-build)
    src/
      main.rs                    entry: load config, signals, TeeSink wiring, spawns serve() + Uploader
      lib.rs                     public re-exports + generated protobuf modules (`proto::pmd`, `proto::archive`)
      config.rs (+ config/)      Config from INI; logging setup; the ONLY env readers
      auth.rs                    OAuth2 password grant (Token, TokenSource)
      driver.rs (+ driver/)      run / run_with_reconnect; errors, shutdown flag, feed-status watch
                                 (feed_status), scheduled recycle, retry/backoff, subscriptions
                                 (subscribe_all + 7 subscribe_to_*)
      transport.rs (+ …)         Transport trait, WsTransport, WS+TLS handshake, keepalive
      sink.rs                    FrameSink trait + shared wire→domain helpers (map_market_type, from_wire_millis)
      sink/tee.rs                TeeSink — fan-out, stops on first error
      sink/queued.rs             QueuedSink — any sink on a worker thread behind a bounded queue
      sink/archive.rs (+ writer) ArchiveSink — verbatim frames to rotating segments under <type>/<date>/
      sink/localview.rs          LocalViewSink — LocalView → OrderBookStore → suppression → seed + broadcast
      sink/contract.rs           ContractSink — Contract → shared ContractStore (configured areas) + delta broadcast
      sink/statistic.rs          StatisticSink — PublicStatistic → shared StatisticStore + per-area broadcast
      sink/*_metrics.rs          periodic book / contract / statistic gauges
      upload.rs (+ upload/)      Uploader — finalized segments → object_store (S3 / file://);
                                 worker thread + store wiring here, then listing (walk the staging
                                 dir), ledger (segments awaiting upload + retry backoff), pass
                                 (one scan), testing (shared test fixtures)
  marketdata/                    pure domain; deps are `tracing`, `strum_macros`, `chrono`
    delivery_areas.csv           reference area dump (data of record) — regenerate with depth-replay --frame-type delivery-area
    src/lib.rs                   domain types (BookKey, MarketType, Order, Side, BookUpdate, …)
    src/eic.rs                   EicCode — hardcoded delivery-area enum (id ↔ EIC ↔ area code), test-pinned to the CSV
    src/orderbook.rs (+ store, depth_view)  OrderBook L2 reconstruction, OrderBookStore, DepthView, DepthStore, top_hash
    src/contract.rs              Contract (per-area state list) + ContractStore (all kept areas)
    src/statistic.rs             PublicStatistic + StatisticStore (area-keyed)
  stream-server/                 depends on marketdata + tonic
    build.rs                     depth protobuf codegen (tonic-build + reflection descriptor)
    src/lib.rs                   re-exports + generated `proto` wire module
    src/feed.rs                  DepthChannels (per-area seed + broadcast), ContractChannel, StatisticChannels
    src/grpc.rs                  ServeChannels + serve — all three services on one runtime (+ tonic-web, reflection)
    src/depth.rs, contracts.rs, statistics.rs   the three streaming services (seed → deltas, drop on lag)
    src/subscriber.rs            SubscriberLimit — per-service cap semaphore + subscriber/lag metrics
  depth-replay/                  archive → Parquet / CSV (depends on connector + marketdata)
    src/{main,reader,replay,writer,contract_writer,delivery_area}.rs   clap CLI, segment reader, replay fold, Parquet/CSV writers
    src/parquet_time.rs          the UTC time-column types both Parquet writers share
  depth-viewer/                  wasm-only egui viewer (trunk + gRPC-Web; Caddyfile/Dockerfile.caddy serve it)
    src/{main,app,chart,source,timefmt}.rs   wasm entry + UI + gRPC-Web source (wasm-only)
    src/model.rs                 pure view model — the only host-tested part
proto/
  nps/intraday/pmd/v2/pmd.proto  inbound wire format (connector) — Nord Pool's package, don't rename
  nordpool/archive/v1/segment.proto   archive segment records: SegmentHeader + TimestampedFrame (recv timestamp + verbatim ServerFrame bytes)
  nordpool/marketdata/v1/{types,depth,contract,statistic}.proto   outbound feeds (stream-server + depth-viewer)
scripts/                         local dev only (prod deploys are containerized elsewhere): dev-stack.sh
                                 (compose wrapper: aws-vault secret fetch + podman compose),
                                 dd-agent.Containerfile (+ dd-agent-*.yaml), dd-agent-stats.sh
```

## Workspace dependency invariants

- **`marketdata` is the pure shared domain.** No protobuf, no IO, no async, no build script; `tracing`, `strum_macros` and `chrono` only. **Every domain timestamp is a `chrono::DateTime<Utc>`** (`Order::created_at`, `PriceLevel::created_at`/`updated_at`, `Contract::delivery_start`/`delivery_end`/`updated_at`, `PublicStatistic::last_trade_time`, each store's `received_at`, which is `None` until the first frame) — epoch integers live only at the wire boundaries, and only where the format forces them: PMD millis in (`sink::from_wire_millis`) and Parquet nanos out (`depth-replay`). The outbound feed protos carry `google.protobuf.Timestamp` (`stream-server`'s `to_timestamp`), so the domain type survives the whole way to a client. The hardcoded `EicCode` in `src/eic.rs` is pinned variant-for-variant to the checked-in `delivery_areas.csv` by a unit test — regenerate the CSV and append the variant when Nord Pool adds an area. Never make it depend on another workspace crate or on prost/tonic, and don't emit metrics from it — measure at its callers.
- **`connector` depends on `marketdata` + `stream-server`.** The driver decodes each frame once and hands every sink `(&ServerPayload, &[u8], received_at)` — the decoded payload, the original wire bytes (archived verbatim, never re-encoded), and the receive timestamp as a `DateTime<Utc>` (archived — the archive converts it back to `SystemTime` for its own file bookkeeping — and carried on every feed update as the wire's `received_at` timestamp).
- **`stream-server` depends on `marketdata` (+ tonic), never on `connector`.** It works on domain `DepthView` / `Contract` / `PublicStatistic` — protobuf stays out of it except its own generated depth wire module.
- **`depth-replay` depends on `connector` + `marketdata`** — it deliberately reuses the connector's single wire→domain translation (`sink::to_book_updates`) and reads the same `<type>/<date>` key layout the archive writes (`segment_prefix`), from `s3://`, `file://`, or a local dir. A localview replay covers **one delivery area** (`--eic-code`, else the range's first area — a second one is an error): the archive multiplexes every area into one stream, while the Parquet rows carry no area and the throttle gate is keyed by `BookKey` alone.
- **`depth-viewer` shares no Rust types with the server** — it compiles the marketdata protos for wasm itself and talks gRPC-Web.
- **The three wire formats stay separate**: inbound PMD, the archive envelope (wraps but never parses the PMD bytes), and the outbound depth proto. None of them belong in `marketdata`.
- **Config lives in the connector binary.** One INI (`nordpool.ini` / `NORDPOOL_CONFIG`), loaded once in `main` (`config::init`), read anywhere via `config::get()` (falls back to the bundled `config/defaults.ini`, compiled in via `include_str!`, in tests — there is no `impl Default` on the config structs; the built-in values live in that INI). `${VAR}` / `${VAR:-default}` refs resolve from the process environment at load; `config/` is the **only** place `std::env` is read. `stream-server` is config-free — parameterized via `serve`'s arguments and constructor args.

## Conventions

- **Tests: Given/When/Then.** Non-trivial unit tests use `// Given` / `// When` / `// Then` comments; "When" marks the action under test. Trivial 3-liners stay unlabeled.
- **Tests live inline** at the bottom of the module they test (`#[cfg(test)] mod tests`), grouped into named inner mods when large (see `driver.rs`). No external `tests.rs` files. Black-box integration tests go in `tests/`.
- **Comments: default to none.** Only for a non-obvious *why* (hidden constraint, subtle invariant, workaround). Never restate what the code does, never reference the current task/PR. Doc comments on public items are encouraged.
- **Prefer editing existing files**; don't add `*.md` docs unless asked. No emojis anywhere.
- **Tone of code**: no compatibility shims, no flags for one-shot changes, no handling for impossible cases. Validate only at system boundaries.

## Architecture in brief

**Driver loop.** `run_with_reconnect` → `run`: authenticate (OAuth2 password grant), open WSS, `subscribe_all` (7 subscriptions: `LocalView` and `PublicStatistic` per configured area; `Contract`, `DeliveryArea`, `Ticker` global; `AtcCapacity` / `H2hCapacity` per area with wildcard connected areas — `subscribe_all` is the one EIC→wire-id conversion point), then poll recv on a 100ms tick (drives shutdown latency, stale checks, token refresh). Ping every 10s, one in flight; stale if unanswered for 20s. A session with no server frame at all for `heartbeat_timeout_factor` (5 in the INI, never 0) announced heartbeat intervals is recycled as `ServerSilent` — pongs prove the socket, heartbeats prove the feed. All of these are `[connector]` config fields with defaults — change the INI, don't reintroduce consts.

**One session per account.** Nord Pool permits a single connection at a time, so one connector process runs against a given account — the deployment pins `replica_count = 1`. This is an upstream limit, not a capacity choice: the feeds cannot be made redundant by running more connectors, and any redundancy has to sit downstream of the one process. Deploys are the exception, and not by choice — capping the rolling update at 100% would give stop-then-start and no overlap at all, but ECS rejects that while the service's availability-zone rebalancing is enabled and the shared `ecs-fargate-service` module neither sets nor exposes that toggle, so for one task startup the incoming and outgoing tasks contend for the one allowed session.

**Reconnect.** Recoverable errors (`Closed`/`Stale`/`Ws`/`Io`/`Handshake`, `AuthError`, `ConnectedTimeout`, `ServerSilent`) retry with exponential backoff (500ms → 30s, reset after a ≥30s session); `ServerError`, decode and sink errors are fatal. **The archive therefore settles its own disk faults rather than reporting them** (decided 2026-07-26, reversing the earlier "keep it fatal" call): a failed write is logged, counted (`archive.write_failures`), and the frame is lost, because the gRPC feeds never touch that disk and ending the session over an IO error turns an archive outage into a client outage without saving a single frame — and a reconnect would not help, the socket being fine and the disk not. A fault is first **ridden out** rather than reported: `handle` retries the frame it is holding for up to `write_retry_secs` (default 30), spacing attempts by doubling from 200ms, so frames arriving meanwhile collect in the `QueuedSink` queue (16384 ≈ 95s of feed at peak) and land as soon as the disk returns — a fault shorter than the budget costs **nothing**. The waiting is on the worker thread deliberately: the driver never waits, since past `enqueue_timeout` its own enqueue drops and latches. Only once a frame is given up does `write_retry` become a gate, so later frames drop immediately (`archive.frames_dropped`) instead of each spending the budget again against a disk already known down — every attempt that opens a segment before failing to write leaves an orphaned `.partial`, which is what the doubling and the gate both bound. A stopping process abandons the wait within 200ms. **Two archive failures still end the process**: the disk being *full* (nothing frees space, every later frame fails identically, and `disk_free_bytes` / `min_free_space_mib` already warned), and anything that is not a disk fault at all — a delta with no snapshot to anchor it — because that segment stream would not replay. `session_reconnect` (`off`|`daily`|`<seconds>`) deliberately recycles the session so the re-subscribe pulls fresh snapshots and archive segments start a self-contained, snapshot-led block (`daily` ⇒ exact per-UTC-day archives).

**Sinks.** `main` composes a `TeeSink`. `ArchiveSink` (behind a `QueuedSink` — worker thread + bounded queue; a full queue counts `sink.blocked` and waits up to `enqueue_timeout`, then **drops** the frame and counts it rather than erroring, latching so later frames drop on the spot until one fits again — losing archive frames beats stalling the receive loop, while an inner-sink error still surfaces on the next `handle` so a worker thread can't swallow one — the archive settles its own disk faults and only errors for what it will not serve through) always runs, writing to the required `[connector.archive] dir` (config load fails if it's empty); the feed trio `LocalViewSink` + `ContractSink` + `StatisticSink` plus the gRPC server is gated on `[server] listen_addr`. `TeeSink` stops on the first error and propagates it — and since a sink error is fatal, that ends the process rather than driving a reconnect.

**Archive + upload.** `ArchiveSink` writes each frame verbatim (wrapped in a `TimestampedFrame` with the recv time) as TFRecord-framed records — masked CRC32C per record; the `tfrecord` module is the framing's single authority, shared with `depth-replay` — each segment led by a `SegmentHeader` record (format version, type label, open time, seq), one rotating segment per frame type under `<dir>/<type>/<date>/`. The open `.tfrecord.partial` stays uncompressed (incremental append + fsync; a torn tail is caught by the record CRCs; `depth-replay` sniffs the gzip magic rather than trusting the extension). Segments finalize (flush → fsync → gzip to a `.tmp` → rename to `.tfrecord.gz` → delete the partial) on block size, on a snapshot (fresh file, seq reset to 0), on shutdown, or on **age** — every arriving frame first sweeps **all** types' open segments and finalizes any open longer than `segment_max_age` (default 15 min), so the busy feeds close the quiet feeds' aged segments (total silence across all subscriptions defers the close, but then nothing new is accumulating); the age close opens no replacement: the type's next frame reopens lazily (delta ⇒ seq+1 continuation, snapshot ⇒ fresh seq 0), so silence creates no empty files. Orphaned `.partial`s are compressed at startup (idempotent across any crash point). The same frame path also checks the staging volume's free space (once per `fsync_interval`, via `statvfs` — the only `unsafe` in library code): it publishes `disk_free_bytes` and warns on crossing below `min_free_space_mib`, but deliberately does **not** intervene — nothing bounds the staging dir, and a full disk is the one archive failure that still ends the process, so the warning exists to be acted on rather than to be handled (every other disk fault is ridden out; see **Reconnect**). The optional `Uploader` (own thread + runtime, gated on `[connector.archive.upload] uri`) scans the dir on an interval and PUTs finalized segments to `object_store` (S3/`file://`), mirroring the local relative path — the writer is the single source of layout truth — then deletes the local copy: the store is the archive of record, the dir a staging area, so what survives a pass is exactly the backlog (`pending_uploads`). Open `.partial`s are **never uploaded**; the age rotation bounds hard-kill loss to ~`segment_max_age` + one scan. At-least-once, immutable, uniquely named segments make re-PUTs harmless (segment open times are nudged ≥1ms apart per type, so names never collide and lexical order is chronological). Every PUT carries the static `tags` and nothing derived per file — the uploader reads nothing out of a segment's path, so the writer owns the layout outright; the event date is selectable as a level of the object key. The gzip+rename to `.tfrecord.gz` is the handoff contract: the uploader only ever reads `.partial`s (never renames or deletes them), so it can't race the writer.

**Depth feed.** `LocalViewSink` owns the `OrderBookStore`, folds each `LocalView` in (snapshot clears, deltas upsert; `quantity == 0` deletes), then broadcasts a book's top-N `DepthView` only when the visible top changed — suppression via `OrderBook::top_hash` (8-byte digest) vs its own last-hashes. Order per frame: apply the whole frame to the per-area seed (`DepthStore::apply`: views + freshness stamp + frame `seq`, one critical section), **then** broadcast the batch carrying that frame's `seq`. There is **no periodic sweep**: a new subscriber is seeded once from the seed map, and a lagged subscriber's stream is **dropped** (terminal `RESOURCE_EXHAUSTED`) so it reconnects and re-seeds. The `seq` is a per-area frame counter (distinct from each view's per-book `revision`, Nord Pool's own number): the seed carries the last applied frame's seq, and the depth service drops queued deltas with `seq <=` it — the connect-window duplicate prefix — so a depth stream is strictly newer than its seed.

**Contract / statistic feeds.** Same seed-then-deltas shape. `ContractSink` keeps the contracts trading in at least one configured area — their area-state lists trimmed to the configured areas — in a shared `Mutex<ContractStore>` (apply under the lock, then broadcast the changed batch); the request's `eic_codes` set is a **filter** (only contracts trading in a requested area are streamed; empty set or an unknown EIC ⇒ `INVALID_ARGUMENT`). Each area state carries the inbound wire's `open_at_ms` / `closed_at_ms` as `phase_start` / `phase_end` — through the domain and out on the feed — renamed at both boundaries because they are **not** a trading window: Nord Pool stamps them per *phase*, rewriting both on every state transition, so they describe only the state currently held — `phase_end` is the scheduled opening while a contract is inactive-and-pending, the gate closure while it is active, and the final inactive phase collapses the window onto the closure instant. Expiry therefore reads `state` and `phase_end` together, never `phase_end` alone. `StatisticSink` keeps an area-keyed shared `StatisticStore` and splits each frame's changed statistics into per-area broadcasts (a statistic frame can span areas); the request's area **selects** the channel, like depth. Correctness rests on two ordering rules everywhere: writer updates the seed/store *before* broadcasting; reader subscribes to the broadcast *before* reading the seed — overlap only duplicates (idempotent upserts), never drops. All three feeds then remove that duplicate prefix server-side via a frame `seq` carried by each feed's store (per-area `DepthStore`; global `ContractStore`/`StatisticStore`) and bumped by its `apply` alongside content and freshness stamp — the statistic seq is store-global, so one frame's per-area batches share it and a single area's stream may skip numbers. The seed carries the last applied frame's seq and deltas with `seq <=` it are dropped, so every stream is strictly newer than its seed.

**gRPC server.** `serve(addr, max_subscribers, keepalive_interval, keepalive_timeout, channels)` — the scalars come straight from the `[server]` config section; `channels: ServeChannels` is the runtime wiring (feed_up watch, depth, contracts, statistics) — blocks on its own runtime (each stream's channel carries its own served areas; the contract stream filters per subscription) and hosts all three services plus reflection and the standard gRPC health service: the "" entry is static Serving (reachable means the process is alive), while the three feed services' entries mirror the driver's `feed_status` watch (Serving while the upstream session is connected, NotServing while it is down/reconnecting — so a client can tell stale-feed from healthy). Idle client connections are probed (HTTP/2 keepalive PING every `keepalive_interval`, closed after an unanswered `keepalive_timeout`, plus TCP keepalive for gRPC-Web/HTTP1) so a silently dead client's subscriber permits are reclaimed even on a quiet feed. Connect/lag seeds carry the feed's last-applied frame receive time in `received_at`, absent until the first frame (per area for depth via `DepthStore`, global for contracts/statistics — every feed's store carries its own stamp behind the same mutex as the content, moved by the same `apply`, so a seed can never carry a stamp out of step with its content), so a client can judge a snapshot's freshness without polling health. A `tonic-web` layer (`accept_http1`) lets browsers reach the same feeds. The listener is deliberately plaintext and unauthenticated: it only ever binds on a private network (ECS behind an internal security group; local dev) — if it ever needs wider exposure, terminate TLS/auth in front of it rather than adding credentials to the feed protocol. Its thread returning at all (e.g. bind failure) is a failure: it records the error and requests shutdown, and `main` exits with that error rather than running on feedless. Each handler is a plain `once(seed).chain(deltas)` stream — no spawned task; the subscriber-cap permit rides on the stream and frees on drop. Each service has its own semaphore sized by `max_subscribers` and its own `nordpool.<feed>.subscribers` / `.lagged` metrics.

**Order-book semantics.** `OrderBook` tracks orders by id and aggregates per-side `BTreeMap` price levels; level totals derive from tracked state, never wire deltas. A same-id reprice is a normal iceberg clip refresh and applies silently; the two integrity tripwires (`warn!` then stay correct) are a remove for an untracked id (ignored) and a level-remove below empty (clamped). On the `LocalView` feed, under a stable `order_id`: `created_at_ms` changed ⇒ a modify; unchanged ⇒ a clip fill.

**Shutdown.** SIGINT/SIGTERM/SIGHUP flip an atomic; loops poll it and return. Drop impls send the WebSocket Close, drain the queue, and finalize the open segment; `main` then calls `Uploader::shutdown`, whose final drain ships the just-finalized segments. That final drain is the one pass that **ignores the retry backoff** (`Attempt::Everything`): deferring is a bet on a cheaper later pass, and there isn't one — the staging dir goes away with the task. It is bounded by the upload section's own `shutdown_grace_secs` instead, which with `[server] shutdown_grace_secs` must fit inside the container's stop timeout. Never call `socket.close` explicitly — `WsTransport::drop` handles it; the `Transport` trait deliberately has no `close()`.

**Metrics.** Emitted through the `metrics` facade macros (no client object). `main` always installs the DogStatsD exporter; `[connector.metrics] dogstatsd_addr` defaults to `127.0.0.1:8125`, and since DogStatsD is UDP a run with no local agent just drops the packets — there is no export on/off gate. Emit where the semantic lives: driver (frames/bytes/heartbeats/reconnects/token refreshes), transport (ping RTT), sinks (book/contract/statistic gauges, depth views sent, queue depth, archive counters + staging-disk free space), uploader (uploads/failures/pending backlog), gRPC handlers (subscribers/lagged). Never from `marketdata`.

## Public API surface

Changing these is a breaking change worth thinking about.

- **`nordpool-connector`**: `Config`, `Credentials`, `Token`, `TokenSource`, `fetch_token`, `WsTransport`, `Transport`, `TransportError`, the driver errors, `run`, `run_with_reconnect`, `send`, `subscribe_all` + the 7 `subscribe_to_*`, `request_shutdown`, `feed_status` (the session-liveness watch the gRPC health service mirrors), the sinks (`FrameSink`, `TeeSink`, `QueuedSink`, `ArchiveSink`, `LocalViewSink`, `ContractSink`, `StatisticSink`), `Uploader`, `segment_prefix`, and the `transport` / `config` modules.
- **`nordpool-stream-server`**: `ServeChannels`, `serve`, `DepthChannels` (per-area `AreaDepthChannel` = seed map + broadcast), `ContractChannel`, `StatisticChannels`, `SharedContracts`, `SharedStatistics`, the `*Sender`/`*Receiver` aliases and `*Update` batch types. Each `*Channel(s)` is created once in `main` and cloned — one clone to the writing sink, one into `ServeChannels` for the reading service. There is no `Publisher`: suppression, seed writes, and broadcasts all live in the connector's sinks; stream-server provides channels, types, and `serve`.
- **`nordpool-marketdata`**: the book domain (`OrderBookStore`, `OrderBook`, `PriceLevel`, `DepthView`, `DepthStore`, `BookKey`, `MarketType`, `Order`, `Side`, `BookUpdate`, id/price/quantity aliases), `EicCode` (the delivery-area enum; `DeliveryAreaId` remains the raw wire-id alias), the contract domain (`Contract`, `ContractAreaState`, `ContractStore`, `ContractState`, `ProductType`), and the statistic domain (`PublicStatistic`, `StatisticStore`, `Tendency`).

Adding a new subscription means: a `subscribe_to_X` helper + `subscribe_all` entry + a `ServerPayload::X(_)` arm in `driver::run` + a unit test mirroring the others. Adding a new stream means: a new channel type in `feed.rs` + a `ServeChannels` field + its service module — not a longer arg list.

## Common gotchas

- **Config path is cwd-relative** — run binaries from the repo root or set `NORDPOOL_CONFIG`. Unit tests rely on the bundled `config/defaults.ini` via the `OnceLock` fallback, so they need no setup.
- **`delivery_area` is one ENTSO-E EIC code or a comma-separated list** (field `delivery_area_eics: Vec<EicCode>`; an unknown EIC fails config load); depth/statistics span all of them, the contract store keeps any contract trading in one of them (states trimmed to them), archive tags use the first.
- **Section gates**: empty `[server] listen_addr` ⇒ no feeds and no gRPC server; empty `[connector.archive.upload] uri` — or no such section at all ⇒ no uploader (the archive itself still runs); past that gate every key in the section is required. The archive and metrics have no on/off gate — both always run. Metrics falls back to a default `[connector.metrics] dogstatsd_addr` of `127.0.0.1:8125` when unconfigured (UDP, dropped silently without an agent); the archive's `[connector.archive] dir` is instead **required** — an empty one fails config load.
- **Tunables are config, not consts, and every default lives in an INI**: timeouts, backoff curve, token-refresh margin, upload retry budget — all of them are keys, and all are read with `req`, so an absent one fails config load rather than falling back to a value written in `config.rs`. The defaults are `nordpool.ini` and the bundled `config/defaults.ini`; there is no `impl Default` and no code-side fallback (`opt` is only for the sentinel keys that gate a subsystem off: `listen_addr`, the upload `uri`, `session_reconnect`, `tags`). `block_size` is configured in MiB (`block_size_mib`).
- **Three `build.rs` files**, all reading `../../proto`: connector (pmd + archive envelope), stream-server (marketdata + reflection), depth-viewer (marketdata, wasm target only). The marketdata protos are compiled twice (server + viewer) — keep them in sync; if you move protos, update every `build.rs`. The outbound feed protos carry areas as EIC strings and times as `google.protobuf.Timestamp` (so both crates need `prost-types`); only the inbound PMD wire uses numeric area ids and epoch-millis integers.
- **`cargo test --workspace` per-package summaries** are easy to misread as "0 tests ran" — failures appear above the summaries.
- **`object_store` is pinned exactly** (pre-1.0); don't bump it without checking.
- **Memory store**: if your agent has a persistent per-project memory directory, read its `MEMORY.md` index first — it may hold collaboration preferences.

## Things not to do without asking

- Rewriting `run` / `run_with_reconnect` / `connect_and_run` control flow.
- Making `marketdata` depend on any workspace crate or on protobuf/prost/tonic; pushing protobuf into `stream-server`'s service layer.
- Changing the depth wire format (the marketdata protos) without regenerating both server and viewer.
- Adding new dependencies — check the relevant `Cargo.toml` first; the marketdata crate is intentionally minimal.
- Bumping major versions of tungstenite, rustls, or prost.
- Using `panic!` / `expect` outside tests. Production code returns `Result`; the only `expect`s are for genuinely impossible cases (`config::init` double-init is a programming error).
- Reading the environment outside `config/`. Add a config field instead of a stray `std::env::var`.
