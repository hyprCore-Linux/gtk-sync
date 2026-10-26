use serde::{Deserialize, Serialize};

use crate::index::{FileEntry, Tombstone};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub name: String,
    pub retention_hours: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexResponse {
    pub files: Vec<FileEntry>,
    pub tombstones: Vec<Tombstone>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VersionsResponse {
    pub path: String,
    pub versions: Vec<FileEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutBlobMeta {
    pub path: String,
    pub ts: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PutBlobResponse {
    pub entry: FileEntry,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TombstoneRequest {
    pub tombstone: Tombstone,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreRequest {
    pub path: String,
    pub ts: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreResponse {
    pub entry: FileEntry,
}

/// Undelete a folder (or path prefix): resurrect latest version of every file under it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreTreeRequest {
    pub path: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestoreTreeResponse {
    pub path: String,
    /// Current file entries after resurrection (use these ts values to pull blobs).
    pub restored: Vec<FileEntry>,
    pub cleared_tombstones: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerInfo {
    pub name: String,
    pub host: String,
    pub port: u16,
    pub cert_fingerprint: String,
}
