use crate::mdns;
use crate::state::AppState;
use base64::Engine;
use mimic_core::protocol::IndexResponse;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;

const TRANSFER_PERMITS: usize = 16;
const LARGE_FILE_BYTES: u64 = 8 * 1024 * 1024;
const LARGE_FILE_PERMITS: u32 = 8;

struct BlobPull {
    path: String,
    ts: u64,
    size: u64,
    url: String,
}

fn transfer_permits(size: u64) -> u32 {
    if size > LARGE_FILE_BYTES {
        LARGE_FILE_PERMITS
    } else {
        1
    }
}

pub async fn peer_sync_loop(state: Arc<AppState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(15));
    loop {
        interval.tick().await;
        if let Err(e) = sync_once(Arc::clone(&state)).await {
            tracing::debug!("peer sync: {e}");
        }
    }
}

async fn sync_once(state: Arc<AppState>) -> anyhow::Result<()> {
    if state.config.peer_password.is_empty() {
        return Ok(());
    }
    let peers = tokio::task::spawn_blocking(|| mdns::browse_peers(2000)).await??;
    let auth = basic_auth(&state.config.username, &state.config.peer_password);

    let client = reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        .timeout(Duration::from_secs(30))
        .build()?;

    for (name, host, port, _fp) in peers {
        if name.contains(&state.config.instance_name) && port == state.config.port {
            continue;
        }

        let base = format!("https://{host}:{port}");
        let idx_url = format!("{base}/v1/index");
        let resp = match client
            .get(&idx_url)
            .header("Authorization", &auth)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => r,
            Ok(r) => {
                tracing::debug!("peer {name} index status {}", r.status());
                continue;
            }
            Err(e) => {
                tracing::debug!("peer {name} unreachable: {e}");
                continue;
            }
        };
        let remote: IndexResponse = resp.json().await?;

        let local_files = state.couch.list_current_files().await?;
        let local_by_path: std::collections::HashMap<&str, &mimic_core::index::FileEntry> =
            local_files.iter().map(|e| (e.path.as_str(), e)).collect();
        let local_vers_paths: std::collections::HashSet<_> =
            local_files.iter().map(|e| e.path.clone()).collect();

        // Decide which current blobs to fetch, then download them together.
        // Metadata checks stay sequential so CouchDB is not stampeded.
        let mut current_blobs = Vec::new();
        for f in &remote.files {
            let local = local_by_path.get(f.path.as_str()).copied();
            let need = match local {
                Some(e) if e.hash == f.hash => !state.versions.join(&e.stored_name).is_file(),
                Some(e) if e.ts >= f.ts => false,
                _ => true,
            };
            if !need {
                continue;
            }
            // Have metadata *and* the blob — just refresh the current pointer.
            if let Some(local_ver) = state.couch.get_version(&f.path, f.ts).await? {
                if state.versions.join(&local_ver.stored_name).is_file() {
                    state.couch.put_version(f).await?;
                    continue;
                }
            }
            current_blobs.push(BlobPull {
                path: f.path.clone(),
                ts: f.ts,
                size: f.size,
                url: blob_url(&base, &f.path, f.ts),
            });
        }
        fetch_blobs(
            Arc::clone(&state),
            client.clone(),
            auth.clone(),
            current_blobs,
        )
        .await;

        // Import remote tombstones
        for t in &remote.tombstones {
            let _ = state.couch.add_tombstone(t).await;
        }

        // List history one path at a time, then download each path's blobs
        // in order (so the current pointer cannot move backward) while
        // different paths run together.
        let mut history: Vec<Vec<BlobPull>> = Vec::new();
        for path in local_vers_paths
            .iter()
            .chain(remote.files.iter().map(|f| &f.path))
        {
            let url = format!("{base}/v1/versions?path={}", urlencoding_encode(path));
            let Ok(r) = client.get(&url).header("Authorization", &auth).send().await else {
                continue;
            };
            if !r.status().is_success() {
                continue;
            }
            let Ok(vr) = r.json::<mimic_core::protocol::VersionsResponse>().await else {
                continue;
            };
            let mut missing = Vec::new();
            for e in vr.versions {
                if state.couch.get_version(&e.path, e.ts).await?.is_some() {
                    continue;
                }
                missing.push(BlobPull {
                    path: e.path.clone(),
                    ts: e.ts,
                    size: e.size,
                    url: blob_url(&base, &e.path, e.ts),
                });
            }
            if !missing.is_empty() {
                history.push(missing);
            }
        }
        fetch_history(Arc::clone(&state), client.clone(), auth.clone(), history).await;

        tracing::debug!("synced with peer {name} ({host}:{port})");
    }
    Ok(())
}

fn blob_url(base: &str, path: &str, ts: u64) -> String {
    format!("{base}/v1/blob?path={}&ts={ts}", urlencoding_encode(path))
}

async fn download_one(
    state: &AppState,
    client: &reqwest::Client,
    auth: &str,
    blob: &BlobPull,
) -> anyhow::Result<()> {
    let resp = client
        .get(&blob.url)
        .header("Authorization", auth)
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("{}: HTTP {}", blob.path, resp.status());
    }
    let bytes = resp.bytes().await?;
    state.put_blob(&blob.path, &bytes, Some(blob.ts)).await?;
    Ok(())
}

/// One blob per path. Safe to store concurrently.
async fn fetch_blobs(
    state: Arc<AppState>,
    client: reqwest::Client,
    auth: String,
    blobs: Vec<BlobPull>,
) {
    if blobs.is_empty() {
        return;
    }
    let sem = Arc::new(Semaphore::new(TRANSFER_PERMITS));
    let mut set = JoinSet::new();
    for blob in blobs {
        let weight = transfer_permits(blob.size);
        let Ok(permit) = sem.clone().acquire_many_owned(weight).await else {
            break;
        };
        let client = client.clone();
        let auth = auth.clone();
        let state = Arc::clone(&state);
        set.spawn(async move {
            let _permit = permit;
            download_one(&state, &client, &auth, &blob).await
        });
    }
    while let Some(joined) = set.join_next().await {
        if let Ok(Err(e)) = joined {
            tracing::debug!("peer blob: {e}");
        }
    }
}

/// Each inner list is one path and is stored in order. Paths run together.
async fn fetch_history(
    state: Arc<AppState>,
    client: reqwest::Client,
    auth: String,
    paths: Vec<Vec<BlobPull>>,
) {
    if paths.is_empty() {
        return;
    }
    let sem = Arc::new(Semaphore::new(TRANSFER_PERMITS));
    let mut set = JoinSet::new();
    for blobs in paths {
        let weight = blobs
            .iter()
            .map(|b| transfer_permits(b.size))
            .max()
            .unwrap_or(1);
        let Ok(permit) = sem.clone().acquire_many_owned(weight).await else {
            break;
        };
        let client = client.clone();
        let auth = auth.clone();
        let state = Arc::clone(&state);
        set.spawn(async move {
            let _permit = permit;
            for blob in &blobs {
                if let Err(e) = download_one(&state, &client, &auth, blob).await {
                    tracing::debug!("peer history blob: {e}");
                }
            }
        });
    }
    while let Some(joined) = set.join_next().await {
        if let Err(e) = joined {
            tracing::debug!("peer history task: {e}");
        }
    }
}

fn basic_auth(user: &str, pass: &str) -> String {
    let token = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
    format!("Basic {token}")
}

fn urlencoding_encode(s: &str) -> String {
    let mut out = String::new();
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'/' => {
                out.push(b as char);
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}
