//! One walk of the staging directory: the finalized segments found.

use std::path::{Path as StdPath, PathBuf};

/// Walk `dir` recursively and collect the finalized `*.tfrecord.gz` segments — the only thing
/// the uploader ships. Open `.tfrecord.partial`s (and anything else) are the writer's business,
/// not ours. A directory we cannot read is logged and counted; the walk still reports every
/// segment it did find, and the uploader retries the whole tree next scan either way.
pub(super) fn list_segments(dir: &StdPath) -> Vec<PathBuf> {
    let mut segments = Vec::new();
    collect_segments(dir, &mut segments);
    segments
}

fn collect_segments(dir: &StdPath, segments: &mut Vec<PathBuf>) {
    let entries = match std::fs::read_dir(dir) {
        Ok(entries) => entries,
        Err(e) => return note_unreadable(&e, dir),
    };
    for entry in entries {
        let path = match entry {
            Ok(entry) => entry.path(),
            Err(e) => {
                note_unreadable(&e, dir);
                continue;
            }
        };
        if path.is_dir() {
            collect_segments(&path, segments);
        } else if path.extension().and_then(|x| x.to_str()) == Some("gz") {
            segments.push(path);
        }
    }
}

/// Report a directory the walk could not enumerate: logged, and counted like any other upload
/// failure so one monitor covers every way a segment fails to ship.
fn note_unreadable(error: &std::io::Error, dir: &StdPath) {
    tracing::warn!(error = %error, dir = %dir.display(), "archive uploader could not read archive dir");
    metrics::counter!("nordpool.archive.upload_failures", "cause" => "list").increment(1);
}
