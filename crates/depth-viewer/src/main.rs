//! Entry point for the web depth viewer. The real entry is the wasm `main`, which mounts an
//! eframe `WebRunner` on the page `<canvas>` and drives the gRPC-Web live feed against the page's
//! own origin (Caddy serves the bundle and reverse-proxies the gRPC-Web path there). The crate is
//! WebAssembly-only; the native `main` is a stub so `cargo build`/`cargo test --workspace` stay
//! green (the host build still compiles + tests the pure `model`).

#[cfg(not(target_arch = "wasm32"))]
fn main() {
    eprintln!(
        "nordpool-depth-viewer is a WebAssembly app. Build it with trunk and serve it via Caddy — \
         `trunk serve` from crates/depth-viewer for the local loop."
    );
}

#[cfg(target_arch = "wasm32")]
fn main() {
    use nordpool_depth_viewer::{App, SourceKind, model};
    use wasm_bindgen::JsCast as _;

    console_error_panic_hook::set_once();
    tracing_wasm::set_as_global_default();

    let location = web_sys::window().expect("no window").location();
    let origin = location.origin().expect("no origin");
    // Area comes from the page URL (`?area=<EIC>`); default to FR when it's absent.
    let eic_code = model::delivery_area_from_query(location.search().ok().as_deref())
        .unwrap_or_else(|| "10YFR-RTE------C".to_string());
    let kind = SourceKind::Live {
        endpoint: origin,
        eic_code,
    };
    let retention_ms = 24 * 3_600_000;

    wasm_bindgen_futures::spawn_local(async move {
        let canvas = web_sys::window()
            .unwrap()
            .document()
            .unwrap()
            .get_element_by_id("depth_canvas")
            .expect("missing #depth_canvas element")
            .dyn_into::<web_sys::HtmlCanvasElement>()
            .expect("#depth_canvas is not a canvas");
        eframe::WebRunner::new()
            .start(
                canvas,
                eframe::WebOptions::default(),
                Box::new(move |cc| Ok(Box::new(App::new(cc, kind, retention_ms)))),
            )
            .await
            .expect("failed to start eframe");
    });
}
