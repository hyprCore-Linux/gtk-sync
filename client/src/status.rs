//! Runtime status JSON for gtk-files (busy / per-file state / tombstones).

use mimic_core::index::Tombstone;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ActiveTransfer {
    pub path: String,
    /// `"up"` or `"down"`
    pub direction: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TombstoneStatus {
    pub path: String,
    pub ts: u64,
    pub kind: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientStatus {
    pub updated_at: u64,
    pub busy: bool,
    pub phase: String,
    pub active: Vec<ActiveTransfer>,
    /// Relative path → `up_to_date` | `syncing` | `pending` | `deleted`
    pub files: BTreeMap<String, String>,
    pub tombstones: Vec<TombstoneStatus>,
}

impl Default for ClientStatus {
    fn default() -> Self {
        Self {
            updated_at: now_ts(),
            busy: false,
            phase: "idle".into(),
            active: Vec::new(),
            files: BTreeMap::new(),
            tombstones: Vec::new(),
        }
    }
}

struct StatusInner {
    status: ClientStatus,
    path: PathBuf,
    /// Paths whose blob 404'd at a given remote ts — skip pending/retry until ts changes.
    unavailable: BTreeMap<String, u64>,
}

fn now_ts() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

fn global() -> &'static Mutex<StatusInner> {
    static STATUS: OnceLock<Mutex<StatusInner>> = OnceLock::new();
    STATUS.get_or_init(|| {
        Mutex::new(StatusInner {
            status: ClientStatus::default(),
            path: default_status_path(),
            unavailable: BTreeMap::new(),
        })
    })
}

pub fn default_status_path() -> PathBuf {
    if let Ok(runtime) = std::env::var("XDG_RUNTIME_DIR") {
        let dir = PathBuf::from(runtime).join("gtk-sync");
        return dir.join("status.json");
    }
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("gtk-sync")
        .join("status.json")
}

pub fn init() {
    let _ = global();
    write_now();
}

fn write_locked(inner: &mut StatusInner) {
    inner.status.updated_at = now_ts();
    if let Some(parent) = inner.path.parent() {
        let _ = std::fs::create_dir_all(parent);
    }
    let Ok(json) = serde_json::to_string_pretty(&inner.status) else {
        return;
    };
    let tmp = inner.path.with_extension("json.tmp");
    if std::fs::write(&tmp, json).is_ok() {
        let _ = std::fs::rename(&tmp, &inner.path);
    }
}

fn write_now() {
    if let Ok(mut g) = global().lock() {
        write_locked(&mut g);
    }
}

pub fn set_phase(phase: &str, busy: bool) {
    if let Ok(mut g) = global().lock() {
        g.status.phase = phase.to_string();
        g.status.busy = busy;
        if !busy {
            g.status.active.clear();
        }
        write_locked(&mut g);
    }
}

/// Mark paths as waiting to sync (e.g. a newly copied folder before each push).
pub fn mark_pending(paths: &[String]) {
    if paths.is_empty() {
        return;
    }
    if let Ok(mut g) = global().lock() {
        for path in paths {
            let cur = g.status.files.get(path).map(|s| s.as_str());
            if matches!(cur, Some("syncing")) {
                continue;
            }
            g.status.files.insert(path.clone(), "pending".into());
        }
        write_locked(&mut g);
    }
}

pub fn begin_transfer(path: &str, direction: &str) {
    if let Ok(mut g) = global().lock() {
        g.status.busy = true;
        g.status.phase = if direction == "down" {
            "pulling".into()
        } else {
            "pushing".into()
        };
        g.status.active.retain(|a| a.path != path);
        g.status.active.push(ActiveTransfer {
            path: path.to_string(),
            direction: direction.to_string(),
        });
        g.status.files.insert(path.to_string(), "syncing".into());
        write_locked(&mut g);
    }
}

pub fn end_transfer(path: &str, final_state: &str) {
    if let Ok(mut g) = global().lock() {
        g.status.active.retain(|a| a.path != path);
        if final_state.is_empty() {
            g.status.files.remove(path);
        } else {
            g.status
                .files
                .insert(path.to_string(), final_state.to_string());
        }
        if g.status.active.is_empty() {
            // Stay in pushing/pulling if more files are still queued as pending.
            let still_queued = g.status.files.values().any(|s| s == "pending");
            if still_queued {
                g.status.busy = true;
                if g.status.phase == "idle" {
                    g.status.phase = "pushing".into();
                }
            } else {
                g.status.busy = false;
                g.status.phase = "idle".into();
            }
        }
        write_locked(&mut g);
    }
}

/// Blob missing on the server for this version — do not keep the UI on "pending".
pub fn mark_unavailable(path: &str, ts: u64) {
    if let Ok(mut g) = global().lock() {
        g.unavailable.insert(path.to_string(), ts);
        g.status.files.remove(path);
        g.status.active.retain(|a| a.path != path);
        if g.status.active.is_empty()
            && !g.status.files.values().any(|s| s == "pending" || s == "syncing")
        {
            g.status.busy = false;
            g.status.phase = "idle".into();
        }
        write_locked(&mut g);
    }
}

pub fn clear_unavailable(path: &str) {
    if let Ok(mut g) = global().lock() {
        g.unavailable.remove(path);
    }
}

/// Blob missing on the server for this version — do not keep the UI on "pending".
#[allow(dead_code)]
pub fn is_unavailable(path: &str, ts: u64) -> bool {
    global()
        .lock()
        .map(|g| g.unavailable.get(path) == Some(&ts))
        .unwrap_or(false)
}

fn tombstones_to_status(tombstones: &[Tombstone]) -> Vec<TombstoneStatus> {
    tombstones
        .iter()
        .map(|t| TombstoneStatus {
            path: t.path.clone(),
            ts: t.ts,
            kind: match &t.kind {
                mimic_core::index::TombstoneKind::Delete => "delete".into(),
                mimic_core::index::TombstoneKind::Rename { .. } => "rename".into(),
            },
        })
        .collect()
}

/// Publish the remote tombstone list as soon as the index is fetched so gtk-files
/// Show deleted works without waiting for a long pull/push reconcile.
pub fn publish_tombstones(tombstones: &[Tombstone]) {
    if let Ok(mut g) = global().lock() {
        let next = filter_superseded_tombstones(tombstones_to_status(tombstones), &g.status.files);
        if g.status.tombstones.len() == next.len()
            && g
                .status
                .tombstones
                .iter()
                .zip(next.iter())
                .all(|(a, b)| a.path == b.path && a.ts == b.ts)
        {
            return;
        }
        for t in &next {
            // Don't clobber a live/pending/syncing mark with deleted.
            let cur = g.status.files.get(&t.path).map(|s| s.as_str());
            if matches!(cur, Some("up_to_date") | Some("pending") | Some("syncing")) {
                continue;
            }
            g.status.files.insert(t.path.clone(), "deleted".into());
        }
        g.status.tombstones = next;
        write_locked(&mut g);
    }
}

fn filter_superseded_tombstones(
    tombs: Vec<TombstoneStatus>,
    files: &BTreeMap<String, String>,
) -> Vec<TombstoneStatus> {
    tombs
        .into_iter()
        .filter(|t| {
            let prefix = format!("{}/", t.path);
            !files.iter().any(|(p, state)| {
                state != "deleted" && (*p == t.path || p.starts_with(&prefix))
            })
        })
        .collect()
}

/// Record a single local delete so Show deleted updates immediately.
pub fn note_tombstone(path: &str, ts: u64) {
    if let Ok(mut g) = global().lock() {
        g.status.files.insert(path.to_string(), "deleted".into());
        if let Some(existing) = g.status.tombstones.iter_mut().find(|t| t.path == path) {
            existing.ts = ts;
        } else {
            g.status.tombstones.push(TombstoneStatus {
                path: path.to_string(),
                ts,
                kind: "delete".into(),
            });
        }
        write_locked(&mut g);
    }
}

pub fn is_known_tombstone(path: &str) -> bool {
    global()
        .lock()
        .map(|g| g.status.tombstones.iter().any(|t| t.path == path))
        .unwrap_or(false)
}

/// Drop tombstones under a restored folder so gtk-files Show deleted updates immediately.
/// Safe from the one-shot CLI: hydrates from the on-disk status written by the daemon.
pub fn clear_tombstones_under(prefix: &str) {
    if let Ok(mut g) = global().lock() {
        if let Some(disk) = read_status_file(&g.path) {
            if disk.updated_at >= g.status.updated_at
                || disk.tombstones.len() > g.status.tombstones.len()
            {
                g.status = disk;
            }
        }
        let prefix_slash = format!("{prefix}/");
        g.status.tombstones.retain(|t| {
            t.path != prefix && !t.path.starts_with(&prefix_slash)
        });
        // Drop ancestor folder tombstones too (same as server restore-tree).
        let mut ancestor = prefix;
        while let Some((parent, _)) = ancestor.rsplit_once('/') {
            g.status.tombstones.retain(|t| t.path != parent);
            ancestor = parent;
        }
        let keys: Vec<String> = g.status.files.keys().cloned().collect();
        for path in keys {
            if path == prefix || path.starts_with(&prefix_slash) {
                if g.status.files.get(&path).map(|s| s.as_str()) == Some("deleted") {
                    g.status.files.insert(path, "up_to_date".into());
                }
            }
        }
        write_locked(&mut g);
    }
}

/// Replace the files map and tombstones after a full reconcile.
pub fn publish_reconcile(
    files: BTreeMap<String, String>,
    tombstones: &[Tombstone],
) {
    if let Ok(mut g) = global().lock() {
        // Keep in-flight / still-queued marks from the previous snapshot.
        let active_paths: Vec<String> = g.status.active.iter().map(|a| a.path.clone()).collect();
        let prev_pending: Vec<String> = g
            .status
            .files
            .iter()
            .filter(|(_, s)| *s == "pending" || *s == "syncing")
            .map(|(k, _)| k.clone())
            .collect();
        let mut files = files;
        for p in &active_paths {
            files.insert(p.clone(), "syncing".into());
        }
        for p in &prev_pending {
            // Don't resurrect a stale pending for a path we already reconciled,
            // or for an unavailable (404) blob.
            if files.contains_key(p) {
                continue;
            }
            if g.unavailable.contains_key(p) {
                continue;
            }
            files.insert(p.clone(), "pending".into());
        }
        for t in tombstones {
            let prefix = format!("{}/", t.path);
            let has_live = files.iter().any(|(p, state)| {
                state != "deleted" && (*p == t.path || p.starts_with(&prefix))
            });
            if has_live {
                continue;
            }
            files.insert(t.path.clone(), "deleted".into());
        }
        g.status.files = files;
        g.status.tombstones =
            filter_superseded_tombstones(tombstones_to_status(tombstones), &g.status.files);
        if g.status.active.is_empty() {
            g.status.busy = false;
            g.status.phase = "idle".into();
        }
        write_locked(&mut g);
    }
}

pub fn status_path() -> PathBuf {
    global()
        .lock()
        .map(|g| g.path.clone())
        .unwrap_or_else(|_| default_status_path())
}

#[allow(dead_code)]
pub fn path_display() -> String {
    status_path().display().to_string()
}

/// Helper for tests / tooling.
#[allow(dead_code)]
pub fn read_status_file(path: &Path) -> Option<ClientStatus> {
    let text = std::fs::read_to_string(path).ok()?;
    serde_json::from_str(&text).ok()
}
