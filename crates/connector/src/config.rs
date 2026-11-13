//! Application configuration, loaded once from an INI file.
//!
//! The file path is `NORDPOOL_CONFIG` (default `nordpool.ini`). Every value may embed
//! `${VAR}` or `${VAR:-default}` references, substituted at load time by [`raw`] (the
//! string-level config) from the process environment. This module and its submodules
//! ([`raw`], [`logging`]) are the *only* places the connector reads the environment. Load
//! once in `main` via [`Config::load`] + [`init`], then read it anywhere via [`get`]. Unit
//! tests that never call [`init`] transparently see the bundled `config/defaults.ini` (the
//! built-in values, compiled in via `include_str!`), so they need no setup.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::OnceLock;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use configparser::ini::Ini;
use nordpool_marketdata::EicCode;

use crate::auth::Credentials;

pub mod logging;
mod raw;
use raw::RawConfig;

static CONFIG: OnceLock<Config> = OnceLock::new();

/// Install the process configuration. Call once, early in `main`, before anything reads it.
pub fn init(config: Config) {
    if CONFIG.set(config).is_err() {
        panic!("config already initialized");
    }
}

/// The process configuration installed by [`init`]. In production it panics if `init` never ran
/// (a programming error — `main` installs it before anything reads it). Under `cfg(test)` it
/// instead falls back to the bundled [`DEFAULTS_INI`], so unit tests need no setup; that fallback
/// and the defaults file are compiled only into the test build, never the shipped binary.
pub fn get() -> &'static Config {
    #[cfg(test)]
    {
        CONFIG.get_or_init(default_config)
    }
    #[cfg(not(test))]
    {
        CONFIG
            .get()
            .expect("config not initialized — config::init() must run in main before config::get()")
    }
}

/// The built-in test defaults. There is no `impl Default` on the config structs; the default
/// values live here, and this parses them into the `get()` fallback for a process that never
/// called [`init`] (unit tests only).
#[cfg(test)]
const DEFAULTS_INI: &str = include_str!("config/defaults.ini");

#[cfg(test)]
fn default_config() -> Config {
    // A malformed bundled default is a programming error, like double-`init`.
    let mut ini = Ini::new();
    ini.read(DEFAULTS_INI.to_string())
        .expect("bundled config/defaults.ini is valid INI");
    Config::from_ini(ini).expect("bundled config/defaults.ini parses to a valid Config")
}

#[derive(Debug, Clone)]
pub struct Config {
    pub connector: ConnectorConfig,
    /// Every section struct is always present; a feature is "off" when its own sentinel field is
    /// empty, read at the point of use via `enabled()`, rather than via an `Option` wrapper — so
    /// fields that are meaningful regardless of the toggle (e.g. `MetricsConfig::book_state_period`)
    /// stay reachable without discharging a `None`.
    ///
    /// Server is disabled when its `listen_addr` is unset ([`ServerConfig::enabled`]); the depth
    /// feed (POC) is then off. `listen_addr` is the one `Option` that stays, because a `SocketAddr`
    /// has no empty value to serve as the sentinel.
    pub server: ServerConfig,
    /// Metrics export is always installed; `dogstatsd_addr` is required (the INI defaults it to the
    /// local `127.0.0.1:8125`, a UDP no-op without an agent), so the `metrics::` macros stay live
    /// everywhere and `book_state_period` always applies.
    pub metrics: MetricsConfig,
    /// The raw-frame archive is always on (no gate), so `dir` is required — an absent or empty one
    /// fails config load. Only its nested `upload` stays opt-in.
    pub archive: ArchiveConfig,
}

#[derive(Debug, Clone)]
pub struct ConnectorConfig {
    pub ws_host: String,
    pub ws_port: u16,
    pub credentials: Credentials,
    /// The delivery areas the connector subscribes to (LocalView/depth, public statistics, and
    /// capacity flows are issued for each). At least one; expected to be a small handful. Parsed
    /// from the comma-separated `[connector] delivery_area` key (the INI key kept its singular
    /// name), whose entries are ENTSO-E EIC codes resolved to [`EicCode`] — the PMD wire ids the
    /// subscriptions need come from [`EicCode::id`]. The single-area contract feed and the
    /// archive partition use the first entry.
    pub delivery_area_eics: Vec<EicCode>,
    pub ping_every: Duration,
    pub pong_timeout: Duration,
    /// Multiplier on the server-announced heartbeat interval (from the `Connected` frame)
    /// bounding how long a session may go without any server frame — heartbeat or data —
    /// before it is treated as silent and recycled. WS pongs only prove the peer's socket is
    /// alive; the application-level heartbeat proves the feed is. At least 1 — the watchdog
    /// has no off switch (a handshake announcing no interval is itself refused).
    pub heartbeat_timeout_factor: u32,
    pub read_timeout: Duration,
    pub write_timeout: Duration,
    /// Bounds the transport connect: each TCP `connect_timeout` and each blocking read/write
    /// during the TLS + WebSocket-upgrade handshake. Without it a peer that accepts TCP then
    /// stalls (NAT blackhole, half-open) hangs the reconnect attempt forever — the steady-state
    /// timeouts are only applied *after* the handshake returns. A trip surfaces as a recoverable
    /// `Handshake` error and reconnects. Must comfortably exceed a real handshake's round-trips;
    /// it's a per-read bound, not a whole-handshake deadline.
    pub handshake_timeout: Duration,
    pub connected_timeout: Duration,
    pub token_http_timeout: Duration,
    pub token_refresh_margin: Duration,
    pub initial_backoff: Duration,
    pub max_backoff: Duration,
    pub backoff_reset_after: Duration,
    /// When set, the session is deliberately recycled on this schedule: the reconnect
    /// re-subscribes and the server replays fresh snapshots, so archive segment boundaries line
    /// up with the schedule (e.g. the UTC day) and each period's data is a self-contained,
    /// snapshot-led set. `None` ⇒ sessions run until an error or shutdown (today's behaviour).
    pub session_reconnect: Option<SessionReconnect>,
}

/// Schedule for deliberately recycling the WebSocket session (see
/// [`ConnectorConfig::session_reconnect`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SessionReconnect {
    /// Reconnect at the next UTC midnight (00:00:00Z), so segment boundaries align to the
    /// calendar day.
    DailyUtc,
    /// Reconnect every fixed interval.
    Every(Duration),
}

#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// `None` disables the gRPC depth feed. Unlike the other sections' sentinels this stays an
    /// `Option` because a `SocketAddr` has no empty value; [`ServerConfig::enabled`] reads it.
    pub listen_addr: Option<SocketAddr>,
    pub broadcast_capacity: usize,
    /// Price levels per side carried in each depth snapshot.
    pub depth: usize,
    /// Maximum number of concurrent depth-book subscribers; a new `Subscribe` over the cap is
    /// rejected with `RESOURCE_EXHAUSTED`.
    pub max_subscribers: usize,
    /// How often the gRPC server probes an idle client connection (HTTP/2 keepalive PING, plus
    /// TCP keepalive for gRPC-Web/HTTP1 clients), so a silently dead client is detected even
    /// when its feed has nothing to send.
    pub keepalive_interval: Duration,
    /// How long an unanswered keepalive PING may hang before the connection is closed and the
    /// dead client's subscriber permits are released.
    pub keepalive_timeout: Duration,
    /// How long `main` waits after a shutdown request for the gRPC server's graceful drain
    /// (subscriber streams ended with a terminal status, in-flight requests completed) before
    /// exiting with connections still open.
    pub shutdown_grace: Duration,
}

impl ServerConfig {
    /// Whether the gRPC depth feed is configured (a `listen_addr` was set).
    pub fn enabled(&self) -> bool {
        self.listen_addr.is_some()
    }
}

#[derive(Debug, Clone)]
pub struct MetricsConfig {
    /// `host:port` of a DogStatsD agent (required). Kept as a string (not `SocketAddr`) because a
    /// hostname like `datadog-agent:8125` is legitimate; the exporter resolves it at install time,
    /// which still fails at startup on a bad value. DogStatsD is UDP, so a local run with no agent
    /// listening can point at `127.0.0.1:8125` (the INI default) and the packets drop silently.
    pub dogstatsd_addr: String,
    /// Extra global tags stamped on every `nordpool.*` series (alongside the built-in `version:`),
    /// as `key:value` pairs. Lets the connector self-identify its metrics when it shares a
    /// DogStatsD agent with another app — agent-level tags apply to every series through that
    /// agent, so an app-identifying tag belongs on the client, not the shared agent. Empty = none.
    pub tags: Vec<(String, String)>,
    /// How often the `nordpool.book.*` state gauges are recomputed from the book store.
    pub book_state_period: Duration,
}

#[derive(Debug, Clone)]
pub struct ArchiveConfig {
    /// Base directory for archive segment files; one subdirectory per frame type.
    pub dir: PathBuf,
    /// Rotate (finalize) the current segment once it reaches this many bytes. Configured in whole
    /// MiB via the INI's `block_size_mib`.
    pub block_size: usize,
    /// Finalize an open segment once it has been open this long, even if it is under
    /// `block_size` — bounds how long any frame can sit in a not-yet-shipped segment. Every
    /// arriving frame sweeps all types' open segments for this (the busy feeds close the
    /// quiet feeds' aged segments); no new segment is opened until the type's next frame.
    pub segment_max_age: Duration,
    /// Flush and `fsync` the open segment at least this often even while it's actively
    /// being written, bounding the data lost if the process is hard-killed. Also the cadence of
    /// the free-space check below — the writer's one periodic-bookkeeping interval.
    pub fsync_interval: Duration,
    /// Warn once the filesystem holding `dir` has less than this much free, in bytes (configured in
    /// whole MiB via `min_free_space_mib`). Nothing bounds the staging dir itself: if uploads stop
    /// shipping, the backlog grows until a write fails for want of space — the one archive failure
    /// still fatal to the process, every other disk fault being ridden out. This does not prevent
    /// that — deleting undelivered data to save the process would be worse — it makes the approach
    /// visible while there is still time to act. Set 0 to warn never;
    /// the free-space gauge is published either way.
    pub min_free_space: u64,
    /// How long the archive rides out a disk that is refusing writes, in both senses: the budget
    /// it spends retrying one frame before giving that frame up, and how long it then waits
    /// before trying the disk again. Configured in whole seconds via `write_retry_secs`.
    ///
    /// The waiting happens on the queue's worker thread, where it is cheap — arriving frames
    /// collect in the `queue_capacity` buffer and land as soon as the disk returns — so this is
    /// really "how long a fault may last before it costs us frames". Keep it well under the
    /// queue's own depth in seconds of feed, or the buffer overflows while the writer is still
    /// hoping.
    pub write_retry: Duration,
    /// Bounds the in-memory queue in front of the (blocking) archive writer.
    pub queue_capacity: usize,
    /// Bounds how long the driver blocks enqueueing a frame when that queue is full — and so
    /// how long the disk may hold up the live feeds, since the driver is not receiving while it
    /// waits. `queue_capacity` is what absorbs a slow writer; by the time it is full the writer
    /// is stalled rather than behind, and waiting only moves the outage downstream to clients.
    /// Sub-second for that reason.
    pub enqueue_timeout: Duration,
    /// How often the writer thread wakes to check idle and fsync deadlines.
    pub poll_interval: Duration,
    /// `None` when `[connector.archive.upload] uri` resolves to empty or the whole section is
    /// absent — finalized segments stay local-only (today's behaviour). This is the one nested
    /// gate: the archive itself is always on, but shipping it to an object store is opt-in.
    pub upload: Option<UploadConfig>,
}

/// The `[connector.archive.upload]` section: the shipper, as distinct from the writer's own
/// `[connector.archive]` keys. Present only when its `uri` is set.
#[derive(Debug, Clone)]
pub struct UploadConfig {
    /// Destination object store: `s3://bucket/prefix` (prod) or `file:///abs/path` (dev/CI).
    /// Empty (or an absent section) is what gates the whole uploader off.
    pub uri: String,
    /// How often the uploader walks the archive dir for finalized `*.tfrecord.gz` to ship.
    /// A shipped segment is always deleted locally: the object store is the archive of record
    /// and the local dir a staging area (ECS ephemeral disk is small), so what is still on disk
    /// after a pass is exactly the backlog.
    pub scan_interval: Duration,
    /// How many segments one scan pass uploads concurrently. Each in-flight upload holds its
    /// whole segment in memory, so this is what bounds a pass's peak footprint: without it a
    /// backlog built up during a store outage is read into memory all at once, and the outage
    /// that filled the disk finishes the job by OOM-killing the task. At least 1.
    pub concurrency: usize,
    /// Ceiling on a failing segment's retry backoff. A segment that fails is retried after one
    /// `scan_interval`, then twice that, and so on up to this — so a persistently failing one
    /// stops costing a read, a PUT and a retry ladder on every single pass, while still being
    /// retried often enough to recover on its own once the store comes back.
    pub max_backoff: Duration,
    /// Static object tags applied to every uploaded segment (S3 `x-amz-tagging`), parsed from this
    /// section's `tags` as comma-separated `key=value` pairs — the S3 tagging form, not the
    /// `key:value` of `[connector.metrics] tags`. Absent or empty sends no tagging header at all.
    /// These are the only tags a PUT carries — the uploader adds nothing per file.
    pub tags: BTreeMap<String, String>,
    /// Per-PUT retry budget for the S3 backend (object_store's `RetryConfig`): the most retries and
    /// the wall-clock cap on a single failing upload before the error surfaces. Bounds how long one
    /// unreachable-store PUT blocks — notably the shutdown final drain, which otherwise grinds
    /// through the long default ladder. The at-least-once scan loop re-ships the file next pass
    /// regardless, so a tight budget only changes latency, not delivery. Ignored by `file://`.
    pub max_retries: usize,
    pub retry_timeout: Duration,
    /// Deadline for the shutdown final drain, which attempts every pending segment regardless of
    /// its retry backoff. At shutdown there is no later pass to defer to — the staging dir is
    /// ephemeral and goes with the task — so a segment not attempted there is one lost; this is
    /// what stops that attempt outliving the container's stop timeout when the store is the thing
    /// that is broken. It runs before the `[server] shutdown_grace` drain, so the two together
    /// must fit inside that timeout (ECS `stopTimeout`, 30s by default) or the kill takes the rest.
    pub shutdown_grace: Duration,
}

impl Config {
    /// Load from the path in `NORDPOOL_CONFIG`, defaulting to `nordpool.ini`.
    pub fn load() -> Result<Self> {
        Self::from_path(&config_path())
    }

    /// Load from a specific INI file path.
    pub fn from_path(path: &str) -> Result<Self> {
        Self::from_ini(load_ini(path)?).with_context(|| format!("parsing config from {path}"))
    }

    fn from_ini(ini: Ini) -> Result<Self> {
        let raw = RawConfig::new(ini);
        Ok(Self {
            connector: connector_from(&raw)?,
            server: server_from(&raw)?,
            metrics: metrics_from(&raw)?,
            archive: archive_from(&raw)?,
        })
    }
}

fn config_path() -> String {
    std::env::var("NORDPOOL_CONFIG").unwrap_or_else(|_| "nordpool.ini".to_string())
}

fn load_ini(path: &str) -> Result<Ini> {
    let mut ini = Ini::new();
    ini.load(path)
        .map_err(|e| anyhow!("loading config from {path}: {e}"))?;
    Ok(ini)
}

fn connector_from(r: &RawConfig) -> Result<ConnectorConfig> {
    let s = "connector";
    Ok(ConnectorConfig {
        ws_host: r.req(s, "ws_host")?,
        ws_port: r
            .req(s, "ws_port")?
            .parse()
            .context("parsing [connector] ws_port")?,
        credentials: credentials_from(r)?,
        delivery_area_eics: parse_delivery_areas(&r.req(s, "delivery_area")?)?,
        ping_every: secs(&r.req(s, "ping_every_secs")?)?,
        pong_timeout: secs(&r.req(s, "pong_timeout_secs")?)?,
        heartbeat_timeout_factor: heartbeat_timeout_factor_from(r)?,
        read_timeout: millis(&r.req(s, "read_timeout_ms")?)?,
        write_timeout: secs(&r.req(s, "write_timeout_secs")?)?,
        handshake_timeout: secs(&r.req(s, "handshake_timeout_secs")?)?,
        connected_timeout: secs(&r.req(s, "connected_timeout_secs")?)?,
        token_http_timeout: secs(&r.req(s, "token_http_timeout_secs")?)?,
        token_refresh_margin: secs(&r.req(s, "token_refresh_margin_secs")?)?,
        initial_backoff: millis(&r.req(s, "initial_backoff_ms")?)?,
        max_backoff: secs(&r.req(s, "max_backoff_secs")?)?,
        backoff_reset_after: secs(&r.req(s, "backoff_reset_after_secs")?)?,
        session_reconnect: session_reconnect_from(r)?,
    })
}

/// Parse the required `[connector] heartbeat_timeout_factor`. Zero is a fatal config error —
/// the silence watchdog has no off switch.
fn heartbeat_timeout_factor_from(r: &RawConfig) -> Result<u32> {
    let factor: u32 = r
        .req("connector", "heartbeat_timeout_factor")?
        .parse()
        .context("parsing [connector] heartbeat_timeout_factor")?;
    if factor == 0 {
        bail!("[connector] heartbeat_timeout_factor must be at least 1");
    }
    Ok(factor)
}

/// Parse `[connector] session_reconnect`: `off`/empty ⇒ `None`, `daily` ⇒ next UTC midnight,
/// or a number of seconds ⇒ a fixed interval.
fn session_reconnect_from(r: &RawConfig) -> Result<Option<SessionReconnect>> {
    match r.opt("connector", "session_reconnect")?.as_str() {
        "" | "off" => Ok(None),
        "daily" => Ok(Some(SessionReconnect::DailyUtc)),
        other => {
            let secs: u64 = other.parse().with_context(|| {
                format!(
                    "[connector] session_reconnect: expected off|daily|<seconds>, got {other:?}"
                )
            })?;
            Ok(Some(SessionReconnect::Every(Duration::from_secs(secs))))
        }
    }
}

/// Parse `[connector] delivery_area` into the set of subscribed delivery areas: a comma-separated
/// list of ENTSO-E EIC codes (a single code is the common case). Blank entries between commas are
/// skipped; an EIC we don't know or an empty list is a fatal config error.
fn parse_delivery_areas(s: &str) -> Result<Vec<EicCode>> {
    let areas = s
        .split(',')
        .map(str::trim)
        .filter(|entry| !entry.is_empty())
        .map(|entry| {
            EicCode::from_eic(entry).ok_or_else(|| {
                anyhow!("[connector] delivery_area entry {entry:?} is not a known EIC code")
            })
        })
        .collect::<Result<Vec<_>>>()?;
    if areas.is_empty() {
        bail!("[connector] delivery_area must list at least one delivery area");
    }
    Ok(areas)
}

fn credentials_from(r: &RawConfig) -> Result<Credentials> {
    let s = "connector.auth";
    Ok(Credentials {
        token_url: r.req(s, "token_url")?,
        client_id: r.req(s, "client_id")?,
        client_secret: r.req(s, "client_secret")?,
        scope: r.req(s, "scope")?,
        username: r.req(s, "username")?,
        password: r.req(s, "password")?,
    })
}

fn server_from(r: &RawConfig) -> Result<ServerConfig> {
    let s = "server";
    // An absent or empty listen_addr disables the depth feed (listen_addr = None); the remaining
    // keys are required — their defaults live in the INI, not here.
    let addr = r.opt(s, "listen_addr")?;
    let listen_addr = if addr.is_empty() {
        None
    } else {
        Some(
            addr.parse::<SocketAddr>()
                .with_context(|| format!("parsing [server] listen_addr {addr:?}"))?,
        )
    };
    Ok(ServerConfig {
        listen_addr,
        broadcast_capacity: r
            .req(s, "broadcast_capacity")?
            .parse()
            .context("parsing [server] broadcast_capacity")?,
        depth: r
            .req(s, "depth")?
            .parse()
            .context("parsing [server] depth")?,
        max_subscribers: r
            .req(s, "max_subscribers")?
            .parse()
            .context("parsing [server] max_subscribers")?,
        keepalive_interval: secs(&r.req(s, "keepalive_interval_secs")?)?,
        keepalive_timeout: secs(&r.req(s, "keepalive_timeout_secs")?)?,
        shutdown_grace: secs(&r.req(s, "shutdown_grace_secs")?)?,
    })
}

fn metrics_from(r: &RawConfig) -> Result<MetricsConfig> {
    // All keys are required — the defaults live in the INI, not here. Metrics export is always
    // installed; the INI's dogstatsd_addr default (127.0.0.1:8125) is a UDP no-op without an agent.
    let s = "connector.metrics";
    Ok(MetricsConfig {
        dogstatsd_addr: r.req(s, "dogstatsd_addr")?,
        tags: parse_metric_tags(&r.req(s, "tags")?).context("parsing [connector.metrics] tags")?,
        book_state_period: secs(&r.req(s, "book_state_period_secs")?)?,
    })
}

/// Parse comma-separated `key:value` global metric tags; an empty string yields none. Each token
/// must be `key:value` — a malformed one fails config load rather than silently dropping a tag.
/// (Distinct from the archive's `key=value` [`parse_tags`]: these are DogStatsD tags, which use
/// the `key:value` wire form.)
fn parse_metric_tags(raw: &str) -> Result<Vec<(String, String)>> {
    raw.split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(|t| {
            t.split_once(':')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
                .with_context(|| format!("metric tag {t:?} is not in key:value form"))
        })
        .collect()
}

fn archive_from(r: &RawConfig) -> Result<ArchiveConfig> {
    // The archive is always on (no gate); every key is required — the defaults live in the INI,
    // not here. An absent or empty dir gets a clearer message than the generic missing-key error.
    let s = "connector.archive";
    let dir = r.req(s, "dir")?;
    if dir.is_empty() {
        bail!("[connector.archive] dir must be set (the raw-frame archive always runs)");
    }
    let block_size_mib: usize = r
        .req(s, "block_size_mib")?
        .parse()
        .context("parsing [connector.archive] block_size_mib")?;
    Ok(ArchiveConfig {
        dir: PathBuf::from(dir),
        block_size: block_size_mib * 1024 * 1024,
        segment_max_age: secs(&r.req(s, "segment_max_age_secs")?)?,
        min_free_space: r
            .req(s, "min_free_space_mib")?
            .parse::<u64>()
            .context("parsing [connector.archive] min_free_space_mib")?
            * 1024
            * 1024,
        fsync_interval: secs(&r.req(s, "fsync_interval_secs")?)?,
        write_retry: secs(&r.req(s, "write_retry_secs")?)?,
        queue_capacity: r
            .req(s, "queue_capacity")?
            .parse()
            .context("parsing [connector.archive] queue_capacity")?,
        enqueue_timeout: millis(&r.req(s, "enqueue_timeout_ms")?)?,
        poll_interval: millis(&r.req(s, "poll_interval_ms")?)?,
        upload: upload_from(r)?,
    })
}

fn upload_from(r: &RawConfig) -> Result<Option<UploadConfig>> {
    // Upload is the archive's one nested gate: an empty (or absent) uri keeps segments local-only,
    // and the section's other keys are then never read. Past the gate they are all required.
    let s = "connector.archive.upload";
    let uri = r.opt(s, "uri")?;
    if uri.is_empty() {
        return Ok(None);
    }
    Ok(Some(UploadConfig {
        uri,
        scan_interval: secs(&r.req(s, "scan_interval_secs")?)?,
        concurrency: upload_concurrency_from(r, s)?,
        max_backoff: secs(&r.req(s, "max_backoff_secs")?)?,
        tags: parse_tags(&r.opt(s, "tags")?).context("parsing [connector.archive.upload] tags")?,
        max_retries: r
            .req(s, "max_retries")?
            .parse()
            .context("parsing [connector.archive.upload] max_retries")?,
        retry_timeout: secs(&r.req(s, "retry_timeout_secs")?)?,
        shutdown_grace: secs(&r.req(s, "shutdown_grace_secs")?)?,
    }))
}

/// Parse the required `[connector.archive.upload] concurrency`. Zero is a fatal config error: a
/// pass fills its upload window up to this many segments, so at zero it spawns nothing and every
/// pass — the shutdown drain included — silently ships none of the backlog.
fn upload_concurrency_from(r: &RawConfig, section: &str) -> Result<usize> {
    let concurrency: usize = r
        .req(section, "concurrency")?
        .parse()
        .context("parsing [connector.archive.upload] concurrency")?;
    if concurrency == 0 {
        bail!("[connector.archive.upload] concurrency must be at least 1");
    }
    Ok(concurrency)
}

/// Parse `k1=v1, k2=v2` into a tag map. Empty (the default) yields no tags; blank entries between
/// commas are skipped; an entry without `=` is a fatal config error. Keys and values are trimmed.
fn parse_tags(s: &str) -> Result<BTreeMap<String, String>> {
    let mut tags = BTreeMap::new();
    for entry in s.split(',') {
        let entry = entry.trim();
        if entry.is_empty() {
            continue;
        }
        let (key, value) = entry
            .split_once('=')
            .with_context(|| format!("tag entry {entry:?} is not key=value"))?;
        tags.insert(key.trim().to_string(), value.trim().to_string());
    }
    Ok(tags)
}

fn secs(s: &str) -> Result<Duration> {
    Ok(Duration::from_secs(
        s.parse()
            .with_context(|| format!("invalid seconds value {s:?}"))?,
    ))
}

fn millis(s: &str) -> Result<Duration> {
    Ok(Duration::from_millis(s.parse().with_context(|| {
        format!("invalid milliseconds value {s:?}")
    })?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_full_config_using_env_defaults() {
        // Given the full SAMPLE INI, whose every `${VAR}` reference carries a `:-` default
        // (so the test resolves without touching the process environment) and whose listen_addr
        // resolves to empty.
        let mut ini = Ini::new();
        ini.read(SAMPLE.to_string()).unwrap();

        // When we build the typed Config from that raw INI — the full load path that resolves
        // env refs and parses scalars.
        let cfg = Config::from_ini(ini).unwrap();

        // Then every kind of field round-trips: plain string literals, an EIC delivery_area,
        // and the seconds/millis durations all parse to their expected typed values; the server
        // stays gated off on its empty listen_addr, while metrics export is always installed with
        // its INI-provided local dogstatsd_addr.
        assert_eq!(cfg.connector.ws_host, "ws.example");
        assert_eq!(cfg.connector.delivery_area_eics, vec![EicCode::AREA_FR]);
        assert_eq!(cfg.connector.ping_every, Duration::from_secs(10));
        assert_eq!(cfg.connector.read_timeout, Duration::from_millis(100));
        assert_eq!(cfg.connector.write_timeout, Duration::from_secs(10));
        assert_eq!(cfg.connector.handshake_timeout, Duration::from_secs(10));
        assert_eq!(cfg.connector.token_http_timeout, Duration::from_secs(10));
        assert_eq!(cfg.connector.credentials.scope, "intraday_api");
        assert!(!cfg.server.enabled()); // empty listen_addr
        assert_eq!(cfg.metrics.dogstatsd_addr, "127.0.0.1:8125"); // INI-provided local default
        // The archive is always on; its required dir parses and upload stays off (no upload section).
        assert_eq!(cfg.archive.dir, PathBuf::from("/tmp/nordpool-archive"));
        assert!(cfg.archive.upload.is_none());
    }

    #[test]
    fn missing_archive_dir_is_a_fatal_config_error() {
        // Given the SAMPLE INI with the archive dir blanked out — the archive always runs, so a
        // missing dir has nowhere to write and must fail load rather than pick a default.
        let mut ini = Ini::new();
        ini.read(SAMPLE.replace("dir = /tmp/nordpool-archive", "dir ="))
            .unwrap();

        // When we build the typed Config, then the empty dir is a fatal error naming the key.
        let err = Config::from_ini(ini).unwrap_err();
        assert!(err.to_string().contains("dir"), "{err}");
    }

    #[test]
    fn populated_metrics_section_parses() {
        // Given the SAMPLE INI patched so dogstatsd_addr resolves to a real `host:port` instead of
        // empty — the explicit-agent case, distinct from the local-default fallback.
        let mut ini = Ini::new();
        ini.read(SAMPLE.replace(
            "dogstatsd_addr = ${DOGSTATSD_CFG_UNSET:-127.0.0.1:8125}",
            "dogstatsd_addr = 10.0.0.1:8125",
        ))
        .unwrap();

        // When we parse the config and pull out the (always-present) metrics section.
        let metrics = Config::from_ini(ini).unwrap().metrics;

        // Then the address is kept verbatim as a string (a hostname is legitimate, so it isn't
        // parsed to a SocketAddr) and the book-state gauge period parses from its seconds value.
        assert_eq!(metrics.dogstatsd_addr, "10.0.0.1:8125");
        // Global tags parse into trimmed key/value pairs stamped on every series.
        assert_eq!(
            metrics.tags,
            vec![
                ("service".to_string(), "nordpool-connector".to_string()),
                ("env".to_string(), "test".to_string()),
            ]
        );
        assert_eq!(metrics.book_state_period, Duration::from_secs(30));
    }

    #[test]
    fn parse_tags_splits_pairs_and_rejects_malformed() {
        // Empty (the "no extra tags" case) yields none.
        assert_eq!(parse_metric_tags("").unwrap(), vec![]);
        // Comma-separated key:value pairs are split and trimmed.
        assert_eq!(
            parse_metric_tags("service:nordpool-connector, env:dev").unwrap(),
            vec![
                ("service".to_string(), "nordpool-connector".to_string()),
                ("env".to_string(), "dev".to_string()),
            ]
        );
        // A token without a colon fails load rather than silently dropping.
        let err = parse_metric_tags("service:np,bare").unwrap_err();
        assert!(err.to_string().contains("bare"), "{err}");
    }

    #[test]
    fn populated_server_section_parses() {
        // Given the SAMPLE INI patched so listen_addr resolves to a real socket address instead
        // of empty — which is what flips the [server] gate (the depth feed) from off to on.
        let mut ini = Ini::new();
        ini.read(SAMPLE.replace(
            "listen_addr = ${GRPC_CFG_UNSET:-}",
            "listen_addr = 0.0.0.0:50051",
        ))
        .unwrap();

        // When we parse the config and pull out the (always-present) server section — a populated
        // listen_addr means the gRPC depth feed is enabled, so `enabled()` must now be true.
        let server = Config::from_ini(ini).unwrap().server;
        assert!(server.enabled());

        // Then listen_addr parses into a SocketAddr (unlike dogstatsd_addr, this one must be a
        // bindable address) and the three sizing knobs parse from their integer values.
        assert_eq!(server.listen_addr.unwrap().to_string(), "0.0.0.0:50051");
        assert_eq!(server.broadcast_capacity, 4096);
        assert_eq!(server.depth, 20);
        assert_eq!(server.max_subscribers, 10);
        // The keepalive and grace keys are required now; SAMPLE carries the standard values.
        assert_eq!(server.keepalive_interval, Duration::from_secs(30));
        assert_eq!(server.keepalive_timeout, Duration::from_secs(20));
        assert_eq!(server.shutdown_grace, Duration::from_secs(10));
    }

    #[test]
    fn archive_upload_section_is_gated_and_parses() {
        // Given SAMPLE plus the UPLOAD section, whose `uri` resolves to empty — the archive is
        // always on, and its one nested upload gate stays off.
        let mut ini = Ini::new();
        ini.read(format!("{SAMPLE}{UPLOAD}")).unwrap();
        let archive = Config::from_ini(ini).unwrap().archive;
        assert_eq!(archive.dir, PathBuf::from("/tmp/nordpool-archive"));
        assert!(archive.upload.is_none(), "an empty uri disables upload");

        // When the uri is instead populated with an `s3://` destination — the value that turns on
        // the uploader — the nested upload config is built and parsed.
        let mut ini = Ini::new();
        ini.read(upload_on()).unwrap();
        let upload = Config::from_ini(ini)
            .unwrap()
            .archive
            .upload
            .expect("upload present");
        assert_eq!(upload.uri, "s3://bucket/prefix");
        assert_eq!(upload.scan_interval, Duration::from_secs(5));
        assert_eq!(upload.tags.get("env").map(String::as_str), Some("prod"));
        assert_eq!(upload.tags.get("team").map(String::as_str), Some("trading"));
        assert_eq!(upload.max_retries, 3);
        assert_eq!(upload.retry_timeout, Duration::from_secs(20));
        assert_eq!(upload.concurrency, 8);
        assert_eq!(upload.max_backoff, Duration::from_secs(3600));
        assert_eq!(upload.shutdown_grace, Duration::from_secs(10));
    }

    #[test]
    fn archive_enqueue_timeout_is_required_and_parses() {
        // Given SAMPLE with the now-required enqueue_timeout_ms stripped out — the default lives
        // in the INI, not the code, so its absence is a fatal error naming the key.
        let mut ini = Ini::new();
        ini.read(SAMPLE.replace("enqueue_timeout_ms = 250\n", ""))
            .unwrap();
        let err = Config::from_ini(ini).unwrap_err();
        assert!(err.to_string().contains("enqueue_timeout_ms"), "{err}");

        // When the key is present, that value parses — in milliseconds, since the ceiling on
        // how long the disk may stall the receive loop is well under a second.
        let mut ini = Ini::new();
        ini.read(SAMPLE.replace("enqueue_timeout_ms = 250", "enqueue_timeout_ms = 500"))
            .unwrap();
        assert_eq!(
            Config::from_ini(ini).unwrap().archive.enqueue_timeout,
            Duration::from_millis(500)
        );
    }

    #[test]
    fn archive_segment_max_age_is_required_and_parses() {
        // Given SAMPLE with the now-required segment_max_age_secs stripped out — its absence is a
        // fatal error naming the key (the default lives in the INI, not the code).
        let mut ini = Ini::new();
        ini.read(SAMPLE.replace("segment_max_age_secs = 900\n", ""))
            .unwrap();
        let err = Config::from_ini(ini).unwrap_err();
        assert!(err.to_string().contains("segment_max_age_secs"), "{err}");

        // When the key is present, that value parses.
        let mut ini = Ini::new();
        ini.read(SAMPLE.replace("segment_max_age_secs = 900", "segment_max_age_secs = 60"))
            .unwrap();
        assert_eq!(
            Config::from_ini(ini).unwrap().archive.segment_max_age,
            Duration::from_secs(60)
        );
    }

    #[test]
    fn upload_retry_budget_is_required_once_upload_is_on() {
        // Given an enabled upload section with a retry key stripped out — every key in the section
        // is required once the gate is open, its default living in the INI rather than in the code.
        let mut ini = Ini::new();
        ini.read(upload_on().replace("max_retries         = 3\n", ""))
            .unwrap();

        // When the upload config is parsed without that key, then the load fails naming it rather
        // than silently picking a budget.
        let err = Config::from_ini(ini).unwrap_err();
        assert!(err.to_string().contains("max_retries"), "{err}");
    }

    #[test]
    fn upload_keys_are_not_read_while_the_gate_is_closed() {
        // Given SAMPLE alone, with no [connector.archive.upload] section at all — not even the keys
        // that are required past the gate, since an absent section reads as an empty uri.
        let mut ini = Ini::new();
        ini.read(SAMPLE.to_string()).unwrap();

        // When the config is parsed, then the load succeeds with upload off.
        assert!(Config::from_ini(ini).unwrap().archive.upload.is_none());
    }

    #[test]
    fn zero_upload_concurrency_is_a_fatal_config_error() {
        // Given an enabled upload section asking for no concurrency at all — the uploader would
        // wait forever for a permit that can never be issued, stalling the scan loop and the
        // shutdown drain with it, so it must fail the load rather than hang at runtime.
        let mut ini = Ini::new();
        ini.read(upload_on().replace("concurrency         = 8", "concurrency         = 0"))
            .unwrap();

        // Then the load fails naming the key.
        let err = Config::from_ini(ini).unwrap_err();
        assert!(err.to_string().contains("concurrency"), "{err}");
    }

    #[test]
    fn upload_tags_default_to_empty_and_reject_malformed_entries() {
        // Given an enabled upload section with the tags line stripped out entirely — exercising the
        // absent-key path, where the tag map defaults to empty rather than erroring (tags are the
        // section's one optional key).
        let mut ini = Ini::new();
        ini.read(upload_on().replace("tags                = env=prod, team=trading\n", ""))
            .unwrap();
        let upload = Config::from_ini(ini).unwrap().archive.upload.unwrap();
        assert!(upload.tags.is_empty());

        // When a tag list contains an entry with no `=` separator (`bogus`), parsing it is a
        // fatal config error rather than a silently-ignored entry — malformed tags must surface
        // loudly. Then the error names the offending entry so the operator can find it.
        let err = parse_tags("env=prod,bogus").unwrap_err();
        assert!(err.to_string().contains("bogus"), "{err}");
    }

    /// The shipper section, appended to [`SAMPLE`] (which carries the writer's own archive keys).
    /// Its `uri` resolves to empty, so upload is gated off until [`upload_on`] opens it.
    const UPLOAD: &str = "\
[connector.archive.upload]
uri                 = ${ARCHIVE_UPLOAD_CFG_UNSET:-}
scan_interval_secs  = 5
concurrency         = 8
max_backoff_secs    = 3600
tags                = env=prod, team=trading
max_retries         = 3
retry_timeout_secs  = 20
shutdown_grace_secs = 10
";

    /// [`SAMPLE`] + [`UPLOAD`] with the gate open — where every enabled-uploader test starts.
    fn upload_on() -> String {
        format!("{SAMPLE}{UPLOAD}").replace(
            "uri                 = ${ARCHIVE_UPLOAD_CFG_UNSET:-}",
            "uri                 = s3://bucket/prefix",
        )
    }

    #[test]
    fn session_reconnect_parses_off_daily_and_interval() {
        let parse = |val: &str| {
            let mut ini = Ini::new();
            ini.read(SAMPLE.replace(
                "delivery_area = 10YFR-RTE------C",
                &format!("delivery_area = 10YFR-RTE------C\nsession_reconnect = {val}"),
            ))
            .unwrap();
            Config::from_ini(ini).unwrap().connector.session_reconnect
        };
        // Absent in SAMPLE ⇒ off; explicit values parse.
        assert_eq!(
            Config::from_ini({
                let mut i = Ini::new();
                i.read(SAMPLE.to_string()).unwrap();
                i
            })
            .unwrap()
            .connector
            .session_reconnect,
            None
        );
        assert_eq!(parse("off"), None);
        assert_eq!(parse("daily"), Some(SessionReconnect::DailyUtc));
        assert_eq!(
            parse("3600"),
            Some(SessionReconnect::Every(Duration::from_secs(3600)))
        );
    }

    #[test]
    fn heartbeat_timeout_factor_is_required_and_parses() {
        // Given the SAMPLE INI with the key stripped out — its default lives in the INI, not the
        // code, so an absent one is a fatal error naming the key.
        let mut ini = Ini::new();
        ini.read(SAMPLE.replace("heartbeat_timeout_factor = 5\n", ""))
            .unwrap();
        let err = Config::from_ini(ini).unwrap_err();
        assert!(
            err.to_string().contains("heartbeat_timeout_factor"),
            "{err}"
        );

        // When the key is present, that value parses.
        let mut ini = Ini::new();
        ini.read(SAMPLE.replace(
            "heartbeat_timeout_factor = 5",
            "heartbeat_timeout_factor = 3",
        ))
        .unwrap();
        assert_eq!(
            Config::from_ini(ini)
                .unwrap()
                .connector
                .heartbeat_timeout_factor,
            3
        );
    }

    #[test]
    fn zero_heartbeat_timeout_factor_is_a_fatal_config_error() {
        // The silence watchdog has no off switch — a factor of 0 must fail the load loudly
        // rather than silently disarm it.
        let mut ini = Ini::new();
        ini.read(SAMPLE.replace(
            "heartbeat_timeout_factor = 5",
            "heartbeat_timeout_factor = 0",
        ))
        .unwrap();
        let err = Config::from_ini(ini).unwrap_err();
        assert!(
            err.to_string().contains("heartbeat_timeout_factor"),
            "{err}"
        );
    }

    #[test]
    fn non_integer_session_reconnect_is_a_fatal_config_error() {
        let mut ini = Ini::new();
        ini.read(SAMPLE.replace(
            "delivery_area = 10YFR-RTE------C",
            "delivery_area = 10YFR-RTE------C\nsession_reconnect = sometimes",
        ))
        .unwrap();
        let err = Config::from_ini(ini).unwrap_err();
        assert!(err.to_string().contains("session_reconnect"), "{err}");
    }

    #[test]
    fn non_integer_delivery_area_is_a_fatal_config_error() {
        let mut ini = Ini::new();
        ini.read(SAMPLE.replace("delivery_area = 10YFR-RTE------C", "delivery_area = abc"))
            .unwrap();
        let err = Config::from_ini(ini).unwrap_err();
        assert!(err.to_string().contains("delivery_area"), "{err}");
    }

    #[test]
    fn delivery_area_parses_a_comma_separated_list() {
        // Given the SAMPLE INI patched to subscribe to three delivery areas, with stray
        // whitespace around the entries — the multi-area form of the single key.
        let mut ini = Ini::new();
        ini.read(SAMPLE.replace(
            "delivery_area = 10YFR-RTE------C",
            "delivery_area = 10YFR-RTE------C, 10YCH-SWISSGRIDZ ,10YAT-APG------L",
        ))
        .unwrap();

        // When the config is parsed — the comma-separated list is split and each entry trimmed
        // and resolved to its area, in order.
        let cfg = Config::from_ini(ini).unwrap();

        // Then every listed area is subscribed, preserving order.
        assert_eq!(
            cfg.connector.delivery_area_eics,
            vec![EicCode::AREA_FR, EicCode::AREA_CH, EicCode::AREA_AT]
        );
    }

    #[test]
    fn empty_delivery_area_is_a_fatal_config_error() {
        // Given a delivery_area that resolves to blank — there is no area to subscribe to.
        let mut ini = Ini::new();
        ini.read(SAMPLE.replace("delivery_area = 10YFR-RTE------C", "delivery_area = "))
            .unwrap();

        // Then the config load fails rather than silently subscribing to nothing.
        let err = Config::from_ini(ini).unwrap_err();
        assert!(err.to_string().contains("delivery_area"), "{err}");
    }

    const SAMPLE: &str = "\
[connector]
ws_host = ws.example
ws_port = 443
delivery_area = 10YFR-RTE------C
ping_every_secs = 10
pong_timeout_secs = 20
heartbeat_timeout_factor = 5
read_timeout_ms = 100
write_timeout_secs = 10
handshake_timeout_secs = 10
connected_timeout_secs = 15
token_http_timeout_secs = 10
token_refresh_margin_secs = 60
initial_backoff_ms = 500
max_backoff_secs = 30
backoff_reset_after_secs = 30

[connector.auth]
token_url = ${SSO_CFG_UNSET:-https://sts.example/connect/token}
client_id = id
client_secret = secret
scope = intraday_api
username = user
password = pass

[connector.metrics]
dogstatsd_addr = ${DOGSTATSD_CFG_UNSET:-127.0.0.1:8125}
tags = service:nordpool-connector, env:test
book_state_period_secs = 30

[connector.archive]
dir = /tmp/nordpool-archive
block_size_mib = 64
segment_max_age_secs = 900
min_free_space_mib = 2048
fsync_interval_secs = 5
write_retry_secs = 30
queue_capacity = 1024
enqueue_timeout_ms = 250
poll_interval_ms = 1000

[server]
listen_addr = ${GRPC_CFG_UNSET:-}
broadcast_capacity = 4096
depth = 20
max_subscribers = 10
keepalive_interval_secs = 30
keepalive_timeout_secs = 20
shutdown_grace_secs = 10
";
}
