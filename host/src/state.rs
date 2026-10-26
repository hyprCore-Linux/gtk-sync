use crate::couch::CouchStore;
use mimic_core::config::ServerConfig;
use mimic_core::index::{index_path, versions_dir, FileIndex};
use mimic_core::versioning::{content_hash, versioned_filename};
use std::path::{Path, PathBuf};
use std::sync::Arc;

pub struct AppState {
    pub config: ServerConfig,
    pub couch: CouchStore,
    pub versions: PathBuf,
    pub cert_fingerprint: String,
}

impl AppState {
    pub async fn new(config: ServerConfig) -> anyhow::Result<Arc<Self>> {
        let data = config.data_dir();
        std::fs::create_dir_all(&data)?;
        std::fs::create_dir_all(versions_dir(&data))?;

        let couch = CouchStore::new(
            &config.couch_url,
            &config.couch_db,
            if config.couch_user.is_empty() {
                None
            } else {
                Some(&config.couch_user)
            },
            if config.couch_password.is_empty() {
                None
            } else {
                Some(&config.couch_password)
            },
        )?;
        couch.ensure_db().await?;

        // One-shot migration from legacy index.json
        let legacy = index_path(&data);
        if legacy.exists() {
            match FileIndex::load(&legacy) {
                Ok(idx) if !idx.files.is_empty() || !idx.history.is_empty() || !idx.tombstones.is_empty() => {
                    tracing::info!("migrating {} to CouchDB…", legacy.display());
                    let n = couch.import_file_index(&idx).await?;
                    let bak = data.join("index.json.migrated");
                    let _ = std::fs::rename(&legacy, &bak);
                    tracing::info!("migrated {n} docs; renamed to {}", bak.display());
                }
                _ => {}
            }
        }

        let versions = versions_dir(&data);
        match couch.drop_missing_blobs(&versions).await {
            Ok(n) if n > 0 => tracing::warn!(
                "dropped {n} CouchDB entries whose blobs are missing under {}",
                versions.display()
            ),
            Err(e) => tracing::warn!("blob reconcile: {e}"),
            _ => {}
        }

        let cert_fingerprint = mimic_core::tls::cert_fingerprint_file(&config.cert_path)?;
        Ok(Arc::new(Self {
            versions,
            config,
            couch,
            cert_fingerprint,
        }))
    }

    pub async fn put_blob(
        &self,
        rel_path: &str,
        data: &[u8],
        ts: Option<u64>,
    ) -> anyhow::Result<mimic_core::index::FileEntry> {
        let ts = ts.unwrap_or_else(FileIndex::now_ts);
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

        let dest = self.versions.join(&stored_name);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(&dest, data)?;

        let entry = mimic_core::index::FileEntry {
            path: rel_path.replace('\\', "/"),
            ts,
            hash,
            size: data.len() as u64,
            stored_name,
        };
        self.couch.put_version(&entry).await?;
        Ok(entry)
    }

    pub async fn read_blob(&self, path: &str, ts: u64) -> anyhow::Result<Vec<u8>> {
        let entry = self
            .couch
            .get_version(path, ts)
            .await?
            .ok_or_else(|| anyhow::anyhow!("version not found: {path}@{ts}"))?;
        Ok(std::fs::read(self.versions.join(&entry.stored_name))?)
    }
}
