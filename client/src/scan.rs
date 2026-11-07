//! Local tree scan with a mtime/size hash cache so large sync roots are not
//! SHA-256'd from scratch every 10 seconds.

use mimic_core::{content_hash_path, is_ignored, is_ignored_dir_name};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Clone, Serialize, Deserialize, Default)]
pub struct HashCache {
    files: HashMap<String, CacheEnt>,
    /// Cached paths missing from the most recent walk. Not persisted.
    #[serde(skip)]
    last_absent: Vec<AbsentEnt>,
}

/// A path we had hashed before that is no longer on disk.
#[derive(Clone)]
pub struct AbsentEnt {
    pub path: String,
    pub mtime: u64,
    pub size: u64,
    pub hash: String,
}

#[derive(Clone, Serialize, Deserialize)]
struct CacheEnt {
    mtime: u64,
    size: u64,
    hash: String,
}

fn cache_path() -> PathBuf {
    dirs::cache_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("gtk-sync")
        .join("file-hashes.json")
}

impl HashCache {
    pub fn load() -> Self {
        std::fs::read_to_string(cache_path())
            .ok()
            .and_then(|t| serde_json::from_str(&t).ok())
            .unwrap_or_default()
    }

    pub fn save(&self) {
        let p = cache_path();
        if let Some(parent) = p.parent() {
            let _ = std::fs::create_dir_all(parent);
        }
        if let Ok(json) = serde_json::to_string(self) {
            let tmp = p.with_extension("json.tmp");
            if std::fs::write(&tmp, json).is_ok() {
                let _ = std::fs::rename(tmp, p);
            }
        }
    }

    /// Paths dropped by the latest [`local_file_map`] because they left the tree.
    pub fn take_absent(&mut self) -> Vec<AbsentEnt> {
        std::mem::take(&mut self.last_absent)
    }

    /// Record a file we just wrote (a pull) so the next walk does not rehash it.
    pub fn remember(&mut self, path: &str, mtime: u64, size: u64, hash: &str) {
        self.files.insert(
            path.to_string(),
            CacheEnt {
                mtime,
                size,
                hash: hash.to_string(),
            },
        );
    }

    /// Put a path back so a failed tombstone is retried on the next scan.
    pub fn remember_absent(&mut self, ent: &AbsentEnt) {
        self.files.insert(
            ent.path.clone(),
            CacheEnt {
                mtime: ent.mtime,
                size: ent.size,
                hash: ent.hash.clone(),
            },
        );
    }
}

/// Relative path → (mtime, sha256). Skips venvs, `.git`, symlinks, etc.
/// Cache misses are hashed across threads; unchanged files keep the mtime/size cache.
pub fn local_file_map(
    root: &Path,
    cache: &mut HashCache,
) -> anyhow::Result<HashMap<String, (u64, String)>> {
    let mut map = HashMap::new();
    let mut seen = HashSet::new();
    let mut misses: Vec<(String, PathBuf, u64, u64)> = Vec::new();
    let walker = WalkDir::new(root).into_iter().filter_entry(|e| {
        if e.depth() == 0 {
            return true;
        }
        let name = e.file_name().to_str().unwrap_or("");
        !is_ignored_dir_name(name)
    });
    for entry in walker.filter_map(|e| e.ok()) {
        if !entry.file_type().is_file() || entry.path_is_symlink() {
            continue;
        }
        let Some(rel) = mimic_core::index::relative_path(root, entry.path()) else {
            continue;
        };
        if is_ignored(&rel) {
            continue;
        }
        let meta = match entry.metadata() {
            Ok(m) => m,
            Err(_) => continue,
        };
        if meta.file_type().is_symlink() {
            continue;
        }
        let mtime = meta
            .modified()
            .ok()
            .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let size = meta.len();
        match cache.files.get(&rel) {
            Some(ent) if ent.mtime == mtime && ent.size == size => {
                seen.insert(rel.clone());
                map.insert(rel, (mtime, ent.hash.clone()));
            }
            _ => misses.push((rel, entry.path().to_path_buf(), mtime, size)),
        }
    }
    for (rel, mtime, size, hash) in hash_misses(&misses) {
        cache.files.insert(
            rel.clone(),
            CacheEnt {
                mtime,
                size,
                hash: hash.clone(),
            },
        );
        seen.insert(rel.clone());
        map.insert(rel, (mtime, hash));
    }
    cache.last_absent = cache
        .files
        .iter()
        .filter(|(k, _)| !seen.contains(*k))
        .map(|(k, ent)| AbsentEnt {
            path: k.clone(),
            mtime: ent.mtime,
            size: ent.size,
            hash: ent.hash.clone(),
        })
        .collect();
    cache.files.retain(|k, _| seen.contains(k));
    Ok(map)
}

fn hash_misses(misses: &[(String, PathBuf, u64, u64)]) -> Vec<(String, u64, u64, String)> {
    if misses.is_empty() {
        return Vec::new();
    }
    let workers = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1)
        .clamp(1, misses.len());
    let mut out = Vec::with_capacity(misses.len());
    std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(workers);
        for worker in 0..workers {
            handles.push(scope.spawn(move || {
                let mut local = Vec::new();
                for (rel, path, mtime, size) in misses.iter().skip(worker).step_by(workers) {
                    match content_hash_path(path) {
                        Ok(hash) => local.push((rel.clone(), *mtime, *size, hash)),
                        Err(e) => tracing::warn!("hash {rel}: {e}"),
                    }
                }
                local
            }));
        }
        for handle in handles {
            match handle.join() {
                Ok(part) => out.extend(part),
                Err(_) => tracing::warn!("hash worker panicked"),
            }
        }
    });
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn hashes_files_in_parallel_and_reuses_cache() {
        let dir = std::env::temp_dir().join(format!("gtk-sync-scan-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(dir.join("sub")).unwrap();
        fs::write(dir.join("a.txt"), b"alpha").unwrap();
        fs::write(dir.join("sub/b.txt"), b"beta").unwrap();
        let mut cache = HashCache::default();
        let map = local_file_map(&dir, &mut cache).unwrap();
        assert_eq!(map.len(), 2);
        let again = local_file_map(&dir, &mut cache).unwrap();
        assert_eq!(map, again);
        let _ = fs::remove_dir_all(&dir);
    }
}
