use std::process::Command;

fn main() {
    let protos = [
        "../../proto/nps/intraday/pmd/v2/pmd.proto",
        "../../proto/nordpool/archive/v1/segment.proto",
    ];
    for proto in protos {
        println!("cargo:rerun-if-changed={proto}");
    }

    ensure_protoc();
    prost_build::Config::new()
        .compile_protos(&protos, &["../../proto"])
        .expect("failed to compile protos");

    // Bake the build's git SHA into the binary (`env!("GIT_SHA")`) for the Datadog
    // `version:` tag — the crate version doesn't change per deploy, the SHA does.
    // "unknown" keeps non-git builds (tarballs, vendored CI) working.
    let sha = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .filter(|out| out.status.success())
        .map(|out| String::from_utf8_lossy(&out.stdout).trim().to_string())
        .unwrap_or_else(|| "unknown".to_string());
    println!("cargo:rustc-env=GIT_SHA={sha}");
    // Re-stamp when the checked-out commit moves (HEAD ref or the branch it points at).
    println!("cargo:rerun-if-changed=../../.git/HEAD");
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
