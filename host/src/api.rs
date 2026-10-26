use crate::mdns;
use crate::peer;
use crate::state::AppState;
use axum::body::Bytes;
use axum::extract::{Query, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use axum_server::tls_rustls::RustlsConfig;
use base64::Engine;
use mimic_core::auth::verify_password;
use mimic_core::config::ServerConfig;
use mimic_core::index::{FileIndex, Tombstone, TombstoneKind};
use mimic_core::protocol::*;
use serde::Deserialize;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub async fn serve(config_path: PathBuf) -> anyhow::Result<()> {
    let config = ServerConfig::load(&config_path)?;
    let state = AppState::new(config.clone()).await?;

    // Retention purge loop (CouchDB metadata + on-disk blobs)
    {
        let st = state.clone();
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(300));
            loop {
                interval.tick().await;
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                let cutoff = now.saturating_sub(st.config.retention_hours.saturating_mul(3600));
                match st.couch.purge_expired(cutoff).await {
                    Ok(names) if !names.is_empty() => {
                        for name in &names {
                            let _ = std::fs::remove_file(st.versions.join(name));
                        }
                        tracing::info!("purged {} expired version blobs", names.len());
                    }
                    Err(e) => tracing::warn!("purge error: {e}"),
                    _ => {}
                }
                match st.couch.drop_missing_blobs(&st.versions).await {
                    Ok(n) if n > 0 => tracing::warn!(
                        "dropped {n} index entries whose blobs are missing on disk"
                    ),
                    Err(e) => tracing::warn!("blob reconcile: {e}"),
                    _ => {}
                }
            }
        });
    }

    let _mdns = mdns::advertise(&state.config, &state.cert_fingerprint)?;

    {
        let st = state.clone();
        tokio::spawn(async move {
            peer::peer_sync_loop(st).await;
        });
    }

    // Default axum limit is 2 MiB — far too small for real sync trees (IDE bundles, etc.).
    const MAX_BLOB_BYTES: usize = 512 * 1024 * 1024;

    let app = Router::new()
        .route("/v1/health", get(health))
        .route("/v1/index", get(get_index))
        .route("/v1/versions", get(get_versions))
        .route("/v1/blob", get(get_blob).put(put_blob))
        .route("/v1/tombstone", post(post_tombstone))
        .route("/v1/restore", post(post_restore))
        .route("/v1/restore-tree", post(post_restore_tree))
        .route("/v1/peer", get(get_peer_info))
        .layer(axum::extract::DefaultBodyLimit::max(MAX_BLOB_BYTES))
        .with_state(state.clone());

    let tls = RustlsConfig::from_pem_file(&config.cert_path, &config.key_path).await?;
    let addr: SocketAddr = format!("{}:{}", config.listen_addr, config.port).parse()?;
    tracing::info!(
        "gtk-sync listening on https://{addr} as {} (CouchDB {}/{})",
        config.instance_name,
        config.couch_url,
        config.couch_db
    );

    axum_server::bind_rustls(addr, tls)
        .serve(app.into_make_service())
        .await?;
    Ok(())
}

fn check_auth(headers: &HeaderMap, state: &AppState) -> Result<(), StatusCode> {
    let auth = headers
        .get(header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let rest = auth
        .strip_prefix("Basic ")
        .ok_or(StatusCode::UNAUTHORIZED)?;
    let decoded = base64::engine::general_purpose::STANDARD
        .decode(rest)
        .map_err(|_| StatusCode::UNAUTHORIZED)?;
    let pair = String::from_utf8(decoded).map_err(|_| StatusCode::UNAUTHORIZED)?;
    let (user, pass) = pair
        .split_once(':')
        .ok_or(StatusCode::UNAUTHORIZED)?;
    if user != state.config.username || !verify_password(pass, &state.config.password_hash) {
        return Err(StatusCode::UNAUTHORIZED);
    }
    Ok(())
}

async fn health(State(state): State<Arc<AppState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        name: state.config.instance_name.clone(),
        retention_hours: state.config.retention_hours,
    })
}

async fn get_peer_info(State(state): State<Arc<AppState>>) -> Json<PeerInfo> {
    Json(PeerInfo {
        name: state.config.instance_name.clone(),
        host: state.config.listen_addr.clone(),
        port: state.config.port,
        cert_fingerprint: state.cert_fingerprint.clone(),
    })
}

async fn get_index(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
) -> Result<Json<IndexResponse>, StatusCode> {
    check_auth(&headers, &state)?;
    let mut files = state
        .couch
        .list_current_files()
        .await
        .map_err(|e| {
            tracing::error!("index files: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    // Never advertise a current file whose blob is gone or that we refuse to sync.
    files.retain(|f| {
        !mimic_core::is_ignored(&f.path)
            && !f.stored_name.is_empty()
            && state.versions.join(&f.stored_name).is_file()
    });
    let tombstones = state.couch.list_tombstones().await.map_err(|e| {
        tracing::error!("index tombs: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(IndexResponse { files, tombstones }))
}

#[derive(Deserialize)]
struct PathQuery {
    path: String,
}

#[derive(Deserialize)]
struct BlobQuery {
    path: String,
    ts: u64,
}

async fn get_versions(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<PathQuery>,
) -> Result<Json<VersionsResponse>, StatusCode> {
    check_auth(&headers, &state)?;
    let versions = state.couch.versions_for(&q.path).await.map_err(|e| {
        tracing::error!("versions: {e}");
        StatusCode::INTERNAL_SERVER_ERROR
    })?;
    Ok(Json(VersionsResponse {
        path: q.path,
        versions,
    }))
}

async fn get_blob(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(q): Query<BlobQuery>,
) -> Result<impl IntoResponse, StatusCode> {
    check_auth(&headers, &state)?;
    let data = state.read_blob(&q.path, q.ts).await.map_err(|_| StatusCode::NOT_FOUND)?;
    Ok((
        [(header::CONTENT_TYPE, "application/octet-stream")],
        data,
    ))
}

async fn put_blob(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(meta): Query<PutBlobMeta>,
    body: Bytes,
) -> Result<Json<PutBlobResponse>, StatusCode> {
    check_auth(&headers, &state)?;
    if mimic_core::is_ignored(&meta.path) {
        return Err(StatusCode::BAD_REQUEST);
    }
    let entry = state
        .put_blob(&meta.path, &body, meta.ts)
        .await
        .map_err(|e| {
            tracing::error!("put_blob: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(Json(PutBlobResponse { entry }))
}

async fn post_tombstone(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<TombstoneRequest>,
) -> Result<StatusCode, StatusCode> {
    check_auth(&headers, &state)?;
    state
        .couch
        .add_tombstone(&req.tombstone)
        .await
        .map_err(|e| {
            tracing::error!("tombstone: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;
    Ok(StatusCode::NO_CONTENT)
}

async fn post_restore(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<RestoreRequest>,
) -> Result<Json<RestoreResponse>, StatusCode> {
    check_auth(&headers, &state)?;
    let data = state
        .read_blob(&req.path, req.ts)
        .await
        .map_err(|_| StatusCode::NOT_FOUND)?;
    let entry = state
        .put_blob(&req.path, &data, None)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(RestoreResponse { entry }))
}

/// Undelete a folder: resurrect a readable blob for every file under the path
/// and clear tombstones in that subtree so clients will not re-delete it.
async fn post_restore_tree(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(req): Json<RestoreTreeRequest>,
) -> Result<Json<RestoreTreeResponse>, StatusCode> {
    check_auth(&headers, &state)?;
    let prefix = req.path.trim_matches('/').to_string();
    if prefix.is_empty() {
        return Err(StatusCode::BAD_REQUEST);
    }

    let latest = state
        .couch
        .latest_versions_under(&prefix)
        .await
        .map_err(|e| {
            tracing::error!("restore-tree list: {e}");
            StatusCode::INTERNAL_SERVER_ERROR
        })?;

    let mut restored = Vec::new();
    for tip in latest {
        if tip.stored_name.is_empty() {
            continue;
        }
        // Prefer newest version whose blob still exists on disk.
        let mut versions = state
            .couch
            .versions_for(&tip.path)
            .await
            .unwrap_or_default();
        versions.sort_by_key(|e| std::cmp::Reverse(e.ts));
        let mut chosen = None;
        for entry in versions {
            if state.read_blob(&entry.path, entry.ts).await.is_ok() {
                chosen = Some(entry);
                break;
            }
        }
        let Some(entry) = chosen else {
            tracing::warn!(
                "restore-tree skip {}: no readable blob on disk",
                tip.path
            );
            continue;
        };
        if let Err(e) = state.couch.resurrect_entry(&entry).await {
            tracing::warn!("restore-tree resurrect {}: {e}", entry.path);
            continue;
        }
        restored.push(entry);
    }

    let cleared_tombstones = if restored.is_empty() {
        0
    } else {
        let under = state
            .couch
            .clear_tombstones_under(&prefix)
            .await
            .map_err(|e| {
                tracing::error!("restore-tree clear tombs: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        let ancestors = state
            .couch
            .clear_ancestor_tombstones(&prefix)
            .await
            .map_err(|e| {
                tracing::error!("restore-tree clear ancestor tombs: {e}");
                StatusCode::INTERNAL_SERVER_ERROR
            })?;
        under + ancestors
    };

    tracing::info!(
        "restore-tree {prefix}: {} files, cleared {cleared_tombstones} tombstones",
        restored.len()
    );
    Ok(Json(RestoreTreeResponse {
        path: prefix,
        restored,
        cleared_tombstones,
    }))
}

#[allow(dead_code)]
pub fn make_delete_tombstone(path: String, children: Vec<String>) -> Tombstone {
    Tombstone {
        path,
        kind: TombstoneKind::Delete,
        ts: FileIndex::now_ts(),
        children,
    }
}
