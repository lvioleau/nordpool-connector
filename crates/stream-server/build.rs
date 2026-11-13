fn main() {
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
    let out_dir = std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap());
    tonic_build::configure()
        // Emit the descriptor set so the server can expose gRPC reflection.
        .file_descriptor_set_path(out_dir.join("marketdata_descriptor.bin"))
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
