// Flag any `pub` item that isn't reachable from outside the crate — it should be
// `pub(crate)` (or narrower). Keeps the public API deliberate.
#![warn(unreachable_pub)]

pub mod auth;
pub mod config;
pub mod driver;
pub mod sink;
pub mod tfrecord;
pub mod transport;
pub mod upload;

pub mod proto {
    pub mod archive {
        include!(concat!(env!("OUT_DIR"), "/nordpool.archive.v1.rs"));
    }

    pub mod pmd {
        include!(concat!(env!("OUT_DIR"), "/nps.intraday.pmd.v2.rs"));
    }
}

pub use auth::{AuthError, Credentials, Token, TokenSource, fetch_token};
pub use config::Config;
pub use driver::{
    ServerError, ServerSilent, ShutdownRequested, feed_status, request_shutdown, run,
    run_with_reconnect, send, shutdown_watch, subscribe_all, subscribe_to_atc_capacity,
    subscribe_to_contracts, subscribe_to_delivery_areas, subscribe_to_h2h_capacity,
    subscribe_to_localview, subscribe_to_public_statistics, subscribe_to_ticker,
};
pub use sink::{
    ArchiveSink, ContractSink, FrameSink, LocalViewSink, QueuedSink, StatisticSink, TeeSink,
};
pub use transport::{Transport, TransportError, WsTransport};
pub use upload::{Uploader, segment_prefix};
