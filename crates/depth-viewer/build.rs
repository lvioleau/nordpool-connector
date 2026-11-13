fn main() {
    // The marketdata protos + gRPC-Web clients are wasm-only (the `marketdata` module is gated to
    // wasm32), so only generate them for that target. The host build compiles just the pure
    // `model` and needs no protoc.
    if std::env::var("CARGO_CFG_TARGET_ARCH").as_deref() != Ok("wasm32") {
        return;
    }

    let protos = [
        "../../proto/nordpool/marketdata/v1/types.proto",
        "../../proto/nordpool/marketdata/v1/depth.proto",
        "../../proto/nordpool/marketdata/v1/contract.proto",
        "../../proto/nordpool/marketdata/v1/statistic.proto",
    ];
    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    ensure_protoc();
    // Client stubs only — the viewer consumes stream-server, it does not host the service. No
    // transport: hyper/h2 don't compile to wasm32; the transport is tonic-web-wasm-client, so the
    // generated `connect` ctor (which references tonic::transport) is dropped.
    tonic_build::configure()
        .build_server(false)
        .build_transport(false)
        .compile_protos(&protos, &["../../proto"])
        .expect("failed to compile marketdata protos");
}

/// Fall back to a vendored protoc when the environment doesn't provide one (e.g. CI without
/// protobuf-compiler installed). Locally the Nix/direnv shell supplies its own on PATH.
fn ensure_protoc() {
    if std::env::var_os("PROTOC").is_none() && !protoc_on_path() {
        let protoc = protoc_bin_vendored::protoc_bin_path().expect("vendored protoc not available");
        // SAFETY: single-threaded build script; set before any proto compilation.
        unsafe { std::env::set_var("PROTOC", protoc) };
    }
}

/// Whether a `protoc` binary is discoverable on `PATH` (each `PATH` entry joined
/// with `protoc`, plus `.exe` on Windows).
fn protoc_on_path() -> bool {
    let Some(path) = std::env::var_os("PATH") else {
        return false;
    };
    let names = if cfg!(windows) {
        &["protoc.exe", "protoc"][..]
    } else {
        &["protoc"][..]
    };
    std::env::split_paths(&path)
        .flat_map(|dir| names.iter().map(move |name| dir.join(name)))
        .any(|candidate| candidate.is_file())
}
