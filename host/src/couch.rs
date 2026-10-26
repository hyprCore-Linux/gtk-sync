//! CouchDB-backed metadata store (current files, version history, tombstones).
//! File blobs stay on disk under `versions/`; this replaces monolithic index.json.

use anyhow::{anyhow, Context};
use base64::Engine;
use mimic_core::index::{FileEntry, Tombstone, TombstoneKind};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::path::Path;

#[derive(Clone)]
pub struct CouchStore {
    client: reqwest::Client,
    base: String,
    db: String,
    auth: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct VersionDoc {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_rev", skip_serializing_if = "Option::is_none")]
    rev: Option<String>,
    #[serde(rename = "type")]
    doc_type: String,
    path: String,
    ts: u64,
    hash: String,
    size: u64,
    stored_name: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct TombDoc {
    #[serde(rename = "_id")]
    id: String,
    #[serde(rename = "_rev", skip_serializing_if = "Option::is_none")]
    rev: Option<String>,
    #[serde(rename = "type")]
    doc_type: String,
    path: String,
    ts: u64,
    kind: String,
    #[serde(default)]
    rename_to: Option<String>,
    #[serde(default)]
    children: Vec<String>,
}

fn enc_path(path: &str) -> String {
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(path.as_bytes())
}

fn file_id(path: &str) -> String {
    format!("file:{}", enc_path(path))
}

fn version_id(path: &str, ts: u64) -> String {
    format!("ver:{}:{}", enc_path(path), ts)
}

fn tomb_id(path: &str, ts: u64) -> String {
    format!("tomb:{}:{}", enc_path(path), ts)
}

impl CouchStore {
    pub fn new(
        url: &str,
        db: &str,
        username: Option<&str>,
        password: Option<&str>,
    ) -> anyhow::Result<Self> {
        let base = url.trim_end_matches('/').to_string();
        let auth = match (username, password) {
            (Some(u), Some(p)) if !u.is_empty() => {
                let token =
                    base64::engine::general_purpose::STANDARD.encode(format!("{u}:{p}"));
                Some(format!("Basic {token}"))
            }
            _ => None,
        };
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()?;
        Ok(Self {
            client,
            base,
            db: db.to_string(),
            auth,
        })
    }

    fn db_url(&self) -> String {
        format!("{}/{}", self.base, self.db)
    }

    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(a) = &self.auth {
            req.header("Authorization", a)
        } else {
            req
        }
    }

    pub async fn ensure_db(&self) -> anyhow::Result<()> {
        // Ping
        let ping = self.apply_auth(self.client.get(&self.base)).send().await?;
        if !ping.status().is_success() {
            anyhow::bail!(
                "CouchDB not reachable at {}: HTTP {}",
                self.base,
                ping.status()
            );
        }

        let put = self
            .apply_auth(self.client.put(self.db_url()))
            .send()
            .await?;
        // 201 created, 412 already exists
        if put.status().as_u16() != 201 && put.status().as_u16() != 412 {
            let body = put.text().await.unwrap_or_default();
            anyhow::bail!("create db {}: {}", self.db, body);
        }

        // Mango indexes for type queries
        self.ensure_index("by-type", json!({ "fields": ["type"] }))
            .await?;
        self.ensure_index(
            "by-type-path",
            json!({ "fields": ["type", "path"] }),
        )
        .await?;
        self.ensure_index(
            "by-type-ts",
            json!({ "fields": ["type", "ts"] }),
        )
        .await?;
        Ok(())
    }

    async fn ensure_index(&self, name: &str, fields: Value) -> anyhow::Result<()> {
        let url = format!("{}/_index", self.db_url());
        let body = json!({
            "index": { "fields": fields["fields"] },
            "name": name,
            "type": "json"
        });
        let resp = self
            .apply_auth(self.client.post(&url))
            .json(&body)
            .send()
            .await?;
        // ignore conflicts / already exists
        let _ = resp;
        Ok(())
    }

    async fn find(&self, selector: Value, limit: u32) -> anyhow::Result<Vec<Value>> {
        let url = format!("{}/_find", self.db_url());
        let body = json!({
            "selector": selector,
            "limit": limit
        });
        let resp = self
            .apply_auth(self.client.post(&url))
            .json(&body)
            .send()
            .await
            .context("couch _find")?;
        if !resp.status().is_success() {
            let t = resp.text().await.unwrap_or_default();
            anyhow::bail!("_find failed: {t}");
        }
        let v: Value = resp.json().await?;
        Ok(v["docs"].as_array().cloned().unwrap_or_default())
    }

    async fn get_raw(&self, id: &str) -> anyhow::Result<Option<Value>> {
        let url = format!("{}/{}", self.db_url(), urlencoding(id));
        let resp = self.apply_auth(self.client.get(&url)).send().await?;
        if resp.status().as_u16() == 404 {
            return Ok(None);
        }
        if !resp.status().is_success() {
            anyhow::bail!("GET {id}: {}", resp.status());
        }
        Ok(Some(resp.json().await?))
    }

    async fn put_raw(&self, doc: &Value) -> anyhow::Result<()> {
        let id = doc["_id"]
            .as_str()
            .ok_or_else(|| anyhow!("doc missing _id"))?;
        let url = format!("{}/{}", self.db_url(), urlencoding(id));
        let mut attempt = doc.clone();
        for _ in 0..5 {
            let resp = self
                .apply_auth(self.client.put(&url))
                .json(&attempt)
                .send()
                .await?;
            if resp.status().is_success() {
                return Ok(());
            }
            if resp.status().as_u16() == 409 {
                // conflict — refresh rev
                if let Some(cur) = self.get_raw(id).await? {
                    if let Some(rev) = cur["_rev"].as_str() {
                        attempt["_rev"] = json!(rev);
                        continue;
                    }
                }
            }
            let t = resp.text().await.unwrap_or_default();
            anyhow::bail!("PUT {id}: {t}");
        }
        anyhow::bail!("PUT {id}: too many conflicts");
    }

    async fn delete_id(&self, id: &str) -> anyhow::Result<()> {
        if let Some(doc) = self.get_raw(id).await? {
            let rev = doc["_rev"].as_str().unwrap_or("");
            let url = format!("{}/{}?rev={}", self.db_url(), urlencoding(id), rev);
            let resp = self.apply_auth(self.client.delete(&url)).send().await?;
            if !resp.status().is_success() && resp.status().as_u16() != 404 {
                anyhow::bail!("DELETE {id}: {}", resp.status());
            }
        }
        Ok(())
    }

    pub async fn list_current_files(&self) -> anyhow::Result<Vec<FileEntry>> {
        let docs = self
            .find(json!({ "type": "file" }), 100_000)
            .await?;
        let mut out = Vec::with_capacity(docs.len());
        for d in docs {
            out.push(FileEntry {
                path: d["path"].as_str().unwrap_or("").to_string(),
                ts: d["ts"].as_u64().unwrap_or(0),
                hash: d["hash"].as_str().unwrap_or("").to_string(),
                size: d["size"].as_u64().unwrap_or(0),
                stored_name: d["stored_name"].as_str().unwrap_or("").to_string(),
            });
        }
        Ok(out)
    }

    pub async fn list_tombstones(&self) -> anyhow::Result<Vec<Tombstone>> {
        let docs = self
            .find(json!({ "type": "tombstone" }), 100_000)
            .await?;
        let mut out = Vec::new();
        for d in docs {
            let kind = match d["kind"].as_str().unwrap_or("delete") {
                "rename" => TombstoneKind::Rename {
                    to: d["rename_to"].as_str().unwrap_or("").to_string(),
                },
                _ => TombstoneKind::Delete,
            };
            let children = d["children"]
                .as_array()
                .map(|a| {
                    a.iter()
                        .filter_map(|x| x.as_str().map(|s| s.to_string()))
                        .collect()
                })
                .unwrap_or_default();
            out.push(Tombstone {
                path: d["path"].as_str().unwrap_or("").to_string(),
                kind,
                ts: d["ts"].as_u64().unwrap_or(0),
                children,
            });
        }
        Ok(out)
    }

    pub async fn versions_for(&self, path: &str) -> anyhow::Result<Vec<FileEntry>> {
        let docs = self
            .find(
                json!({ "type": "version", "path": path }),
                10_000,
            )
            .await?;
        let mut out: Vec<FileEntry> = docs
            .into_iter()
            .map(|d| FileEntry {
                path: d["path"].as_str().unwrap_or("").to_string(),
                ts: d["ts"].as_u64().unwrap_or(0),
                hash: d["hash"].as_str().unwrap_or("").to_string(),
                size: d["size"].as_u64().unwrap_or(0),
                stored_name: d["stored_name"].as_str().unwrap_or("").to_string(),
            })
            .collect();
        out.sort_by_key(|e| e.ts);
        Ok(out)
    }

    pub async fn get_version(&self, path: &str, ts: u64) -> anyhow::Result<Option<FileEntry>> {
        let id = version_id(path, ts);
        let Some(d) = self.get_raw(&id).await? else {
            return Ok(None);
        };
        Ok(Some(FileEntry {
            path: d["path"].as_str().unwrap_or("").to_string(),
            ts: d["ts"].as_u64().unwrap_or(0),
            hash: d["hash"].as_str().unwrap_or("").to_string(),
            size: d["size"].as_u64().unwrap_or(0),
            stored_name: d["stored_name"].as_str().unwrap_or("").to_string(),
        }))
    }

    /// Record a new version and advance current pointer if ts is newest.
    pub async fn put_version(&self, entry: &FileEntry) -> anyhow::Result<()> {
        let vid = version_id(&entry.path, entry.ts);
        let vdoc = VersionDoc {
            id: vid,
            rev: None,
            doc_type: "version".into(),
            path: entry.path.clone(),
            ts: entry.ts,
            hash: entry.hash.clone(),
            size: entry.size,
            stored_name: entry.stored_name.clone(),
        };
        self.put_raw(&serde_json::to_value(vdoc)?)
            .await?;

        let fid = file_id(&entry.path);
        let replace = match self.get_raw(&fid).await? {
            Some(cur) => entry.ts >= cur["ts"].as_u64().unwrap_or(0),
            None => true,
        };
        if replace {
            // Drop covering tombstones for this path
            let tombs = self
                .find(
                    json!({ "type": "tombstone", "path": entry.path }),
                    1000,
                )
                .await?;
            for t in tombs {
                if let Some(id) = t["_id"].as_str() {
                    let _ = self.delete_id(id).await;
                }
            }

            let mut fdoc = json!({
                "_id": fid,
                "type": "file",
                "path": entry.path,
                "ts": entry.ts,
                "hash": entry.hash,
                "size": entry.size,
                "stored_name": entry.stored_name,
            });
            if let Some(cur) = self.get_raw(&fid).await? {
                if let Some(rev) = cur["_rev"].as_str() {
                    fdoc["_rev"] = json!(rev);
                }
            }
            self.put_raw(&fdoc).await?;
        }
        Ok(())
    }

    /// Latest historical version for every path under `prefix` (exact or `prefix/…`).
    pub async fn latest_versions_under(&self, prefix: &str) -> anyhow::Result<Vec<FileEntry>> {
        let vers = self
            .find(json!({ "type": "version" }), 100_000)
            .await?;
        let prefix_slash = format!("{prefix}/");
        let mut best: std::collections::BTreeMap<String, FileEntry> =
            std::collections::BTreeMap::new();
        for d in vers {
            let path = d["path"].as_str().unwrap_or("").to_string();
            if path != prefix && !path.starts_with(&prefix_slash) {
                continue;
            }
            let entry = FileEntry {
                path: path.clone(),
                ts: d["ts"].as_u64().unwrap_or(0),
                hash: d["hash"].as_str().unwrap_or("").to_string(),
                size: d["size"].as_u64().unwrap_or(0),
                stored_name: d["stored_name"].as_str().unwrap_or("").to_string(),
            };
            match best.get(&path) {
                Some(cur) if cur.ts >= entry.ts => {}
                _ => {
                    best.insert(path, entry);
                }
            }
        }
        Ok(best.into_values().collect())
    }

    /// Point the current file doc at an existing version and drop exact-path tombstones.
    pub async fn resurrect_entry(&self, entry: &FileEntry) -> anyhow::Result<()> {
        let tombs = self
            .find(
                json!({ "type": "tombstone", "path": entry.path }),
                1000,
            )
            .await?;
        for t in tombs {
            if let Some(id) = t["_id"].as_str() {
                let _ = self.delete_id(id).await;
            }
        }

        let fid = file_id(&entry.path);
        let mut fdoc = json!({
            "_id": fid,
            "type": "file",
            "path": entry.path,
            "ts": entry.ts,
            "hash": entry.hash,
            "size": entry.size,
            "stored_name": entry.stored_name,
        });
        if let Some(cur) = self.get_raw(&fid).await? {
            if let Some(rev) = cur["_rev"].as_str() {
                fdoc["_rev"] = json!(rev);
            }
        }
        self.put_raw(&fdoc).await?;
        Ok(())
    }

    /// Delete every tombstone whose path is `prefix` or under `prefix/`.
    pub async fn clear_tombstones_under(&self, prefix: &str) -> anyhow::Result<usize> {
        let tombs = self
            .find(json!({ "type": "tombstone" }), 100_000)
            .await?;
        let prefix_slash = format!("{prefix}/");
        let mut n = 0usize;
        for d in tombs {
            let path = d["path"].as_str().unwrap_or("");
            if path == prefix || path.starts_with(&prefix_slash) {
                if let Some(id) = d["_id"].as_str() {
                    if self.delete_id(id).await.is_ok() {
                        n += 1;
                    }
                }
            }
        }
        Ok(n)
    }

    /// Clear tombstones for each ancestor of `prefix` (so a parent delete cannot
    /// wipe a restored subtree on the next client sync).
    pub async fn clear_ancestor_tombstones(&self, prefix: &str) -> anyhow::Result<usize> {
        let mut n = 0usize;
        let mut cur = prefix;
        while let Some((parent, _)) = cur.rsplit_once('/') {
            let tombs = self
                .find(json!({ "type": "tombstone", "path": parent }), 1000)
                .await?;
            for d in tombs {
                if let Some(id) = d["_id"].as_str() {
                    if self.delete_id(id).await.is_ok() {
                        n += 1;
                    }
                }
            }
            cur = parent;
        }
        Ok(n)
    }

    pub async fn add_tombstone(&self, tomb: &Tombstone) -> anyhow::Result<()> {
        // Remove current file docs
        let _ = self.delete_id(&file_id(&tomb.path)).await;
        for c in &tomb.children {
            let _ = self.delete_id(&file_id(c)).await;
        }

        let (kind, rename_to) = match &tomb.kind {
            TombstoneKind::Delete => ("delete".to_string(), None),
            TombstoneKind::Rename { to } => ("rename".to_string(), Some(to.clone())),
        };
        let doc = TombDoc {
            id: tomb_id(&tomb.path, tomb.ts),
            rev: None,
            doc_type: "tombstone".into(),
            path: tomb.path.clone(),
            ts: tomb.ts,
            kind,
            rename_to,
            children: tomb.children.clone(),
        };
        self.put_raw(&serde_json::to_value(doc)?).await?;
        Ok(())
    }

    /// Delete version docs (and return stored_names to unlink) older than cutoff,
    /// keeping the current file version.
    pub async fn purge_expired(&self, cutoff: u64) -> anyhow::Result<Vec<String>> {
        let mut unlink = Vec::new();
        let current: std::collections::HashMap<String, u64> = self
            .list_current_files()
            .await?
            .into_iter()
            .map(|e| (e.path, e.ts))
            .collect();

        let vers = self
            .find(json!({ "type": "version" }), 100_000)
            .await?;
        for d in vers {
            let path = d["path"].as_str().unwrap_or("");
            let ts = d["ts"].as_u64().unwrap_or(0);
            let is_current = current.get(path) == Some(&ts);
            if !is_current && ts < cutoff {
                if let Some(name) = d["stored_name"].as_str() {
                    unlink.push(name.to_string());
                }
                if let Some(id) = d["_id"].as_str() {
                    let _ = self.delete_id(id).await;
                }
            }
        }

        let tombs = self
            .find(json!({ "type": "tombstone" }), 100_000)
            .await?;
        for d in tombs {
            let ts = d["ts"].as_u64().unwrap_or(0);
            if ts < cutoff {
                if let Some(id) = d["_id"].as_str() {
                    let _ = self.delete_id(id).await;
                }
            }
        }
        Ok(unlink)
    }

    async fn bulk_delete_docs(&self, docs: &[Value]) -> anyhow::Result<usize> {
        if docs.is_empty() {
            return Ok(0);
        }
        let payload: Vec<Value> = docs
            .iter()
            .filter_map(|d| {
                let id = d["_id"].as_str()?;
                let rev = d["_rev"].as_str()?;
                Some(json!({
                    "_id": id,
                    "_rev": rev,
                    "_deleted": true,
                }))
            })
            .collect();
        let mut n = 0usize;
        for chunk in payload.chunks(500) {
            let url = format!("{}/_bulk_docs", self.db_url());
            let resp = self
                .apply_auth(self.client.post(&url))
                .json(&json!({ "docs": chunk }))
                .send()
                .await?;
            if !resp.status().is_success() {
                let t = resp.text().await.unwrap_or_default();
                anyhow::bail!("_bulk_docs: {t}");
            }
            n += chunk.len();
        }
        Ok(n)
    }

    /// Drop current-file and version docs whose on-disk blob is gone.
    ///
    /// Without this, a wiped `versions/` dir plus a surviving CouchDB index makes
    /// clients treat matching hashes as up-to-date and never re-upload.
    pub async fn drop_missing_blobs(&self, versions_dir: &Path) -> anyhow::Result<usize> {
        let mut doomed = Vec::new();
        for d in self.find(json!({ "type": "file" }), 100_000).await? {
            let name = d["stored_name"].as_str().unwrap_or("");
            if name.is_empty() || !versions_dir.join(name).is_file() {
                doomed.push(d);
            }
        }
        for d in self.find(json!({ "type": "version" }), 100_000).await? {
            let name = d["stored_name"].as_str().unwrap_or("");
            if name.is_empty() || !versions_dir.join(name).is_file() {
                doomed.push(d);
            }
        }
        let n = doomed.len();
        self.bulk_delete_docs(&doomed).await?;
        Ok(n)
    }

    /// Import from legacy FileIndex JSON (one-shot migration).
    pub async fn import_file_index(&self, index: &mimic_core::index::FileIndex) -> anyhow::Result<usize> {
        let mut n = 0usize;
        for hist in index.history.values() {
            for e in hist {
                self.put_version(e).await?;
                n += 1;
            }
        }
        for e in index.files.values() {
            // ensure current pointer (put_version may already have set it)
            self.put_version(e).await?;
        }
        for t in &index.tombstones {
            self.add_tombstone(t).await?;
            n += 1;
        }
        Ok(n)
    }
}

fn urlencoding(s: &str) -> String {
    // CouchDB doc ids with : need encoding in URL path
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
