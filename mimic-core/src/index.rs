use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::versioning::{content_hash, versioned_filename};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FileEntry {
    /// Relative path using `/` separators (logical path, no timestamp suffix)
    pub path: String,
    pub ts: u64,
    pub hash: String,
    pub size: u64,
    /// Versioned filename on disk relative to versions store
    pub stored_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum TombstoneKind {
    Delete,
    Rename { to: String },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Tombstone {
    pub path: String,
    pub kind: TombstoneKind,
    pub ts: u64,
    /// For directory deletes, all relative paths under the dir at delete time
    #[serde(default)]
    pub children: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct FileIndex {
    /// path -> current entry
    pub files: HashMap<String, FileEntry>,
    /// all historical versions (including current)
    pub history: HashMap<String, Vec<FileEntry>>,
    pub tombstones: Vec<Tombstone>,
}

impl FileIndex {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        if !path.exists() {
            return Ok(Self::default());
        }
        let text = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&text)?)
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_string_pretty(self)?)?;
        fs::rename(tmp, path)?;
        Ok(())
    }

    pub fn now_ts() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }

    /// Store a new version of `rel_path` from bytes into `versions_dir`.
    pub fn put_version(
        &mut self,
        versions_dir: &Path,
        rel_path: &str,
        data: &[u8],
        ts: Option<u64>,
    ) -> anyhow::Result<FileEntry> {
        let ts = ts.unwrap_or_else(Self::now_ts);
        let hash = content_hash(data);
        let file_name = Path::new(rel_path)
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or(rel_path);
        let stored_name = if let Some(parent) = Path::new(rel_path).parent() {
            if parent.as_os_str().is_empty() {
                versioned_filename(file_name, ts)
            } else {
                format!(
                    "{}/{}",
                    parent.to_string_lossy().replace('\\', "/"),
                    versioned_filename(file_name, ts)
                )
            }
        } else {
            versioned_filename(file_name, ts)
        };

        let dest = versions_dir.join(&stored_name);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent)?;
        }
        fs::write(&dest, data)?;

        let entry = FileEntry {
            path: rel_path.replace('\\', "/"),
            ts,
            hash,
            size: data.len() as u64,
            stored_name,
        };

        let hist = self.history.entry(entry.path.clone()).or_default();
        hist.push(entry.clone());
        hist.sort_by_key(|e| e.ts);

        // Only advance current if this is newer or equal
        let replace = match self.files.get(&entry.path) {
            Some(cur) => entry.ts >= cur.ts,
            None => true,
        };
        if replace {
            // Clear tombstone for this path if any
            self.tombstones.retain(|t| t.path != entry.path);
            self.files.insert(entry.path.clone(), entry.clone());
        }
        Ok(entry)
    }

    pub fn put_version_from_path(
        &mut self,
        versions_dir: &Path,
        rel_path: &str,
        src: &Path,
        ts: Option<u64>,
    ) -> anyhow::Result<FileEntry> {
        let mut f = fs::File::open(src)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        self.put_version(versions_dir, rel_path, &buf, ts)
    }

    pub fn add_tombstone(&mut self, tomb: Tombstone) {
        // Remove current file entries for path and children
        self.files.remove(&tomb.path);
        for c in &tomb.children {
            self.files.remove(c);
        }
        if matches!(tomb.kind, TombstoneKind::Rename { .. }) {
            // keep history
        }
        self.tombstones.retain(|t| t.path != tomb.path);
        self.tombstones.push(tomb);
    }

    pub fn versions_for(&self, path: &str) -> Vec<&FileEntry> {
        self.history
            .get(path)
            .map(|v| v.iter().collect())
            .unwrap_or_default()
    }

    pub fn read_blob(&self, versions_dir: &Path, path: &str, ts: u64) -> anyhow::Result<Vec<u8>> {
        let entry = self
            .history
            .get(path)
            .and_then(|vs| vs.iter().find(|e| e.ts == ts))
            .ok_or_else(|| anyhow::anyhow!("version not found: {path}@{ts}"))?;
        Ok(fs::read(versions_dir.join(&entry.stored_name))?)
    }

    pub fn merge_remote_index(&mut self, other: &FileIndex) {
        for (path, entry) in &other.files {
            let take = match self.files.get(path) {
                Some(cur) => entry.ts > cur.ts,
                None => {
                    // Don't resurrect if we have a newer tombstone
                    !self
                        .tombstones
                        .iter()
                        .any(|t| t.path == *path && t.ts >= entry.ts)
                }
            };
            if take {
                self.files.insert(path.clone(), entry.clone());
            }
        }
        for (path, hist) in &other.history {
            let dest = self.history.entry(path.clone()).or_default();
            for e in hist {
                if !dest.iter().any(|x| x.ts == e.ts && x.hash == e.hash) {
                    dest.push(e.clone());
                }
            }
            dest.sort_by_key(|e| e.ts);
        }
        for t in &other.tombstones {
            let exists = self
                .tombstones
                .iter()
                .any(|x| x.path == t.path && x.ts == t.ts);
            if !exists {
                if let Some(cur) = self.files.get(&t.path) {
                    if cur.ts < t.ts {
                        self.files.remove(&t.path);
                    }
                } else {
                    // ok
                }
                if self
                    .files
                    .get(&t.path)
                    .map(|c| c.ts < t.ts)
                    .unwrap_or(true)
                {
                    self.files.remove(&t.path);
                    for c in &t.children {
                        if self.files.get(c).map(|e| e.ts < t.ts).unwrap_or(true) {
                            self.files.remove(c);
                        }
                    }
                }
                self.tombstones.push(t.clone());
            }
        }
    }
}

pub fn fingerprint_bytes(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

pub fn relative_path(root: &Path, full: &Path) -> Option<String> {
    full.strip_prefix(root)
        .ok()
        .map(|p| p.to_string_lossy().replace('\\', "/"))
}

pub fn versions_dir(data_dir: &Path) -> PathBuf {
    data_dir.join("versions")
}

pub fn index_path(data_dir: &Path) -> PathBuf {
    data_dir.join("index.json")
}
