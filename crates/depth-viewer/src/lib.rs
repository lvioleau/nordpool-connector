//! The web (WebAssembly) order-book depth viewer: an eframe/egui app served in the browser,
//! talking to the connector's depth feed over gRPC-Web. `model` is the pure, transport-agnostic
//! domain (segment-folding for the chart) and is unit-tested on the host; the egui UI and the
//! gRPC-Web transport are wasm-only.

pub mod model;

#[cfg(target_arch = "wasm32")]
pub mod app;
#[cfg(target_arch = "wasm32")]
pub mod chart;
#[cfg(target_arch = "wasm32")]
pub mod source;
#[cfg(target_arch = "wasm32")]
pub mod timefmt;

#[cfg(target_arch = "wasm32")]
pub mod marketdata {
    tonic::include_proto!("nordpool.marketdata.v1");
}

#[cfg(target_arch = "wasm32")]
pub use app::App;
#[cfg(target_arch = "wasm32")]
pub use source::SourceKind;
