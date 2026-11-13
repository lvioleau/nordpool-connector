//! Shared fixtures for the upload modules' tests: a self-removing temp dir, segments staged
//! the way the archive writer lays them out, and a `LocalFileSystem` store to ship them to.
//! Test support, not tests — each module's own tests stay at the bottom of that module.

use std::fs;
use std::path::{Path as StdPath, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use object_store::local::LocalFileSystem;
use object_store::path::Path as ObjectPath;
use object_store::{ObjectStore, TagSet};

pub(super) fn base_tags() -> TagSet {
    let mut tags = TagSet::default();
    tags.push("delivery_area", "40");
    tags
}

/// A unique temp directory that removes itself on drop — keeps the tests hermetic without
/// pulling in the `tempfile` crate (the rest of the connector uses `std::env::temp_dir`).
pub(super) struct TempDir(PathBuf);

impl TempDir {
    pub(super) fn new() -> Self {
        static N: AtomicU64 = AtomicU64::new(0);
        let n = N.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("nordpool-upload-{}-{n}", std::process::id()));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    pub(super) fn path(&self) -> &StdPath {
        &self.0
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// The UTC day directory a segment name falls under, mirroring `ArchiveSink`'s layout:
/// `<label>-YYYYMMDDThhmmss…` → `YYYY-MM-DD`.
pub(super) fn day_dir(name: &str) -> String {
    let digits: String = name
        .split_once('-')
        .map(|(_, rest)| rest)
        .unwrap_or("")
        .chars()
        .take(8)
        .collect();
    format!("{}-{}-{}", &digits[..4], &digits[4..6], &digits[6..8])
}

/// The on-disk path a staged segment lives at, partitioned like the archive writes it:
/// `<root>/<label>/<date>/<name>`.
pub(super) fn staged_path(root: &TempDir, label: &str, name: &str) -> PathBuf {
    root.path().join(label).join(day_dir(name)).join(name)
}

/// A finalized segment plus an open partial under `<label>/<date>/`, returning the staging root.
pub(super) fn staging_with(label: &str, finalized: &[&str], partials: &[&str]) -> TempDir {
    let root = TempDir::new();
    for (names, bytes) in [
        (finalized, b"sealed-bytes".as_slice()),
        (partials, b"open-bytes"),
    ] {
        for name in names {
            let path = staged_path(&root, label, name);
            fs::create_dir_all(path.parent().unwrap()).unwrap();
            fs::write(path, bytes).unwrap();
        }
    }
    root
}

/// A `LocalFileSystem` store rooted at a fresh temp dir, with an empty base path.
pub(super) fn local_store() -> (Arc<dyn ObjectStore>, ObjectPath, TempDir) {
    let dest = TempDir::new();
    let store = LocalFileSystem::new_with_prefix(dest.path()).unwrap();
    (Arc::new(store), ObjectPath::default(), dest)
}

pub(super) fn block_on<F: std::future::Future>(f: F) -> F::Output {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(f)
}
