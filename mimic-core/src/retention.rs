use std::fs;
use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::index::{versions_dir, FileIndex};

/// Delete version blobs and tombstones older than retention_hours.
pub fn purge_expired(
    index: &mut FileIndex,
    data_dir: &Path,
    retention_hours: u64,
) -> anyhow::Result<usize> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let cutoff = now.saturating_sub(retention_hours.saturating_mul(3600));
    let vdir = versions_dir(data_dir);
    let mut removed = 0usize;

    for (path, hist) in index.history.iter_mut() {
        let current_ts = index.files.get(path).map(|e| e.ts);
        let mut keep = Vec::new();
        for e in hist.drain(..) {
            let is_current = current_ts == Some(e.ts);
            if is_current || e.ts >= cutoff {
                keep.push(e);
            } else {
                let f = vdir.join(&e.stored_name);
                if f.exists() {
                    let _ = fs::remove_file(&f);
                    removed += 1;
                }
            }
        }
        *hist = keep;
    }
    index.history.retain(|_, v| !v.is_empty());

    let before = index.tombstones.len();
    index.tombstones.retain(|t| t.ts >= cutoff);
    removed += before - index.tombstones.len();

    Ok(removed)
}
