use crate::discover;
use crate::scan;
use crate::status;
use base64::Engine;
use mimic_core::config::{ClientConfig, DiscoveredPeer};
use mimic_core::index::{FileIndex, Tombstone, TombstoneKind};
use mimic_core::is_ignored;
use mimic_core::protocol::{IndexResponse, PutBlobResponse, TombstoneRequest, VersionsResponse};
use notify::event::ModifyKind;
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, LazyLock, Mutex};
use std::time::Duration;

/// While applying remote tombstones locally, ignore Remove events so we don't
/// re-POST thousands of tombstones and starve the reconcile loop.
static APPLYING_REMOTE_DELETES: AtomicBool = AtomicBool::new(false);

/// Paths this process just wrote while pulling. The watcher must not upload
/// those writes, or a download is published again with a fresh timestamp.
static SELF_WRITES: LazyLock<Mutex<HashSet<String>>> = LazyLock::new(|| Mutex::new(HashSet::new()));

fn mark_self_write(rel: &str) {
    if let Ok(mut guard) = SELF_WRITES.lock() {
        guard.insert(rel.to_string());
    }
}

fn is_self_write(rel: &str) -> bool {
    SELF_WRITES.lock().map(|g| g.contains(rel)).unwrap_or(false)
}

fn clear_self_writes() {
    if let Ok(mut guard) = SELF_WRITES.lock() {
        guard.clear();
    }
}

pub struct HttpPeer {
    pub base: String,
    pub auth: String,
    pub name: String,
}

pub fn http_client() -> anyhow::Result<reqwest::Client> {
    Ok(reqwest::Client::builder()
        .danger_accept_invalid_certs(true)
        // Large IDE / binary blobs need more than the old 60s default.
        .timeout(Duration::from_secs(600))
        .build()?)
}

pub fn basic_auth(user: &str, pass: &str) -> String {
    let token = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{pass}"));
    format!("Basic {token}")
}

pub fn encode_path(s: &str) -> String {
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

pub async fn resolve_peers(cfg: &ClientConfig) -> Vec<HttpPeer> {
    let auth = basic_auth(&cfg.username, &cfg.password);
    let mut peers: Vec<DiscoveredPeer> = cfg
        .peers
        .iter()
        .filter(|p| !p.excluded)
        .cloned()
        .collect();

    for s in &cfg.static_peers {
        if let Some((host, port)) = s.split_once(':') {
            if let Ok(port) = port.parse() {
                peers.push(DiscoveredPeer {
                    name: format!("static-{host}"),
                    host: host.to_string(),
                    port,
                    cert_fingerprint: None,
                    excluded: false,
                });
            }
        }
    }

    if cfg.auto_discover {
        if let Ok(found) = discover::browse_async(Duration::from_secs(2)).await {
            for f in found {
                if !peers.iter().any(|p| p.host == f.host && p.port == f.port) {
                    peers.push(f);
                }
            }
        }
    }

    peers
        .into_iter()
        .filter(|p| !p.excluded)
        .fold(Vec::new(), |mut acc, p| {
            if !acc.iter().any(|x: &DiscoveredPeer| x.host == p.host && x.port == p.port) {
                acc.push(p);
            }
            acc
        })
        .into_iter()
        .map(|p| HttpPeer {
            base: format!("https://{}:{}", p.host, p.port),
            auth: auth.clone(),
            name: p.name,
        })
        .collect()
}

pub async fn run(config_path: PathBuf) -> anyhow::Result<()> {
    let cfg = ClientConfig::load(&config_path)?;
    ensure_client_root(&cfg.root)?;
    status::init();
    tracing::info!("status file: {}", status::status_path().display());

    let (tx, rx) = mpsc::channel();
    let mut watcher = RecommendedWatcher::new(
        move |res: Result<notify::Event, notify::Error>| {
            if let Ok(event) = res {
                let _ = tx.send(event);
            }
        },
        notify::Config::default(),
    )?;
    watcher.watch(&cfg.root, RecursiveMode::Recursive)?;

    let mut hash_cache = scan::HashCache::load();

    // Initial full sync — scanning is not "busy" for the sidebar (large trees
    // can take minutes); only pull/push transfers flip the syncing icon.
    status::set_phase("scanning", false);
    if let Err(e) = full_sync(&cfg, &rx, &mut hash_cache).await {
        tracing::warn!("initial sync: {e}");
    }
    hash_cache.save();
    status::set_phase("idle", false);

    let mut last_scan = std::time::Instant::now();
    loop {
        // Cap events per tick so a mass-delete cannot starve index reconcile /
        // status.json tombstone publish (needed for gtk-files Show deleted).
        let mut handled = 0u32;
        while handled < 24 {
            match rx.try_recv() {
                Ok(event) => {
                    if let Err(e) = handle_fs_event(&cfg, &event).await {
                        tracing::warn!("fs event: {e}");
                    }
                    handled += 1;
                }
                Err(_) => break,
            }
        }
        // Queued pull events have been skipped. Further edits should upload.
        if handled < 24 {
            clear_self_writes();
        }

        // Periodic reconcile
        if last_scan.elapsed() > Duration::from_secs(10) {
            status::set_phase("scanning", false);
            if let Err(e) = full_sync(&cfg, &rx, &mut hash_cache).await {
                tracing::debug!("periodic sync: {e}");
            }
            hash_cache.save();
            status::set_phase("idle", false);
            last_scan = std::time::Instant::now();
        }

        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

fn ensure_client_root(root: &Path) -> anyhow::Result<()> {
    std::fs::create_dir_all(root)?;
    Ok(())
}

enum PullResult {
    Written,
    MissingBlob,
}

async fn full_sync(
    cfg: &ClientConfig,
    rx: &mpsc::Receiver<notify::Event>,
    cache: &mut scan::HashCache,
) -> anyhow::Result<()> {
    let client = http_client()?;
    let peers = resolve_peers(cfg).await;
    if peers.is_empty() {
        tracing::debug!("no peers yet");
        status::publish_reconcile(std::collections::BTreeMap::new(), &[]);
        return Ok(());
    }

    // Merge remote indexes (LWW by ts)
    let mut merged = FileIndex::default();
    let mut peer_indexes: Vec<(HttpPeer, IndexResponse)> = Vec::new();

    for peer in &peers {
        let url = format!("{}/v1/index", peer.base);
        match client
            .get(&url)
            .header("Authorization", &peer.auth)
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                let idx: IndexResponse = r.json().await?;
                for f in &idx.files {
                    if is_ignored(&f.path) {
                        continue;
                    }
                    let take = match merged.files.get(&f.path) {
                        Some(cur) => f.ts > cur.ts,
                        None => true,
                    };
                    if take {
                        merged.files.insert(f.path.clone(), f.clone());
                    }
                    merged
                        .history
                        .entry(f.path.clone())
                        .or_default()
                        .push(f.clone());
                }
                for t in &idx.tombstones {
                    if is_ignored(&t.path) {
                        continue;
                    }
                    if !merged
                        .tombstones
                        .iter()
                        .any(|x| x.path == t.path && x.ts == t.ts)
                    {
                        merged.tombstones.push(t.clone());
                    }
                }
                peer_indexes.push((
                    HttpPeer {
                        base: peer.base.clone(),
                        auth: peer.auth.clone(),
                        name: peer.name.clone(),
                    },
                    idx,
                ));
            }
            Ok(r) => tracing::debug!("{} index: {}", peer.name, r.status()),
            Err(e) => tracing::debug!("{} unreachable: {e}", peer.name),
        }
    }

    // Publish tombstones immediately — do not wait for pull/push/hashing.
    status::publish_tombstones(&merged.tombstones);

    let local = scan::local_file_map(&cfg.root, cache)?;

    // Apply remote deletes (tombstones) locally if local file older/missing awareness
    APPLYING_REMOTE_DELETES.store(true, Ordering::SeqCst);
    for t in &merged.tombstones {
        if matches!(t.kind, TombstoneKind::Delete) {
            // Partial folder restore: live index files under this path must not be wiped
            // by a leftover parent tombstone.
            let prefix_slash = format!("{}/", t.path);
            let has_live = merged.files.keys().any(|p| {
                *p == t.path || p.starts_with(&prefix_slash)
            });
            if has_live {
                continue;
            }
            // A folder deleted and recreated in the same breath still has the
            // tombstone, but new files under it must not be wiped.
            let has_newer_local = local.iter().any(|(p, (mtime, _))| {
                (*p == t.path || p.starts_with(&prefix_slash)) && *mtime > t.ts
            });
            if has_newer_local {
                continue;
            }
            let paths: Vec<String> = std::iter::once(t.path.clone())
                .chain(t.children.iter().cloned())
                .collect();
            for p in paths {
                let full = cfg.root.join(&p);
                if full.exists() {
                    // Only delete if we don't have a newer local edit than tombstone
                    if let Some((mtime, _)) = local.get(&p) {
                        if *mtime > t.ts {
                            continue;
                        }
                    }
                    if full.is_dir() {
                        let _ = std::fs::remove_dir_all(&full);
                    } else {
                        let _ = std::fs::remove_file(&full);
                    }
                    tracing::info!("applied delete for {p}");
                }
            }
        }
    }
    APPLYING_REMOTE_DELETES.store(false, Ordering::SeqCst);
    // Drop notify backlog from remove_dir_all so we don't re-tombstone it.
    while rx.try_recv().is_ok() {}

    // Files that left the tree since the last scan (delete, or cut/paste rename).
    // Tombstone them before pull, or the server copy is written straight back.
    propagate_local_deletes(&client, &peers, &mut merged, cache).await;

    let mut missing_blobs: HashSet<String> = HashSet::new();

    // Pull newer remote files
    for (path, entry) in &merged.files {
        if is_ignored(path) {
            continue;
        }
        let need_pull = match local.get(path) {
            Some((_, hash)) if hash == &entry.hash => false,
            // Same-second edits must not lose to a server ts of "now".
            Some((mtime, _)) if *mtime >= entry.ts => false,
            _ => true,
        };
        if need_pull {
            let result = if let Some((peer, _)) = peer_indexes.iter().find(|(_, idx)| {
                idx.files.iter().any(|f| f.path == *path && f.ts == entry.ts)
            }) {
                pull_file(cfg, &client, peer, path, entry.ts).await
            } else if let Some((peer, _)) = peer_indexes.first() {
                pull_file(cfg, &client, peer, path, entry.ts).await
            } else {
                Ok(PullResult::Written)
            };
            match result {
                Ok(PullResult::MissingBlob) => {
                    missing_blobs.insert(path.clone());
                }
                Err(e) => tracing::warn!("pull {path}: {e}"),
                Ok(_) => {}
            }
        }
    }

    // Push local files that are newer, missing remotely, or whose server blob 404'd.
    let local = scan::local_file_map(&cfg.root, cache)?;
    let mut to_push: Vec<String> = Vec::new();
    for (path, (mtime, hash)) in &local {
        let remote = merged.files.get(path);
        let need_push = match remote {
            Some(e) if &e.hash == hash && !missing_blobs.contains(path) => false,
            Some(_) => true,
            None => true,
        };
        let _ = mtime;
        if need_push {
            to_push.push(path.clone());
        }
    }
    status::mark_pending(&to_push);
    let mut pushed: HashSet<String> = HashSet::new();
    for path in &to_push {
        match push_file_to_all(cfg, &client, &peers, path).await {
            Ok(()) => {
                pushed.insert(path.clone());
                status::clear_unavailable(path);
            }
            Err(e) => tracing::warn!("push {path}: {e}"),
        }
    }

    // Publish reconciled per-file states for gtk-files.
    let local = scan::local_file_map(&cfg.root, cache)?;
    let mut file_states = std::collections::BTreeMap::new();
    for (path, entry) in &merged.files {
        if is_ignored(path) {
            continue;
        }
        if missing_blobs.contains(path) && !pushed.contains(path) {
            file_states.insert(path.clone(), "pending".into());
            continue;
        }
        let state = match local.get(path) {
            Some((_, hash)) if hash == &entry.hash => "up_to_date",
            _ => "pending",
        };
        file_states.insert(path.clone(), state.to_string());
    }
    for path in local.keys() {
        if pushed.contains(path) {
            file_states.insert(path.clone(), "up_to_date".into());
        } else {
            file_states
                .entry(path.clone())
                .or_insert_with(|| "pending".into());
        }
    }
    status::publish_reconcile(file_states, &merged.tombstones);

    Ok(())
}

async fn pull_file(
    cfg: &ClientConfig,
    client: &reqwest::Client,
    peer: &HttpPeer,
    path: &str,
    ts: u64,
) -> anyhow::Result<PullResult> {
    status::begin_transfer(path, "down");
    let url = format!(
        "{}/v1/blob?path={}&ts={ts}",
        peer.base,
        encode_path(path)
    );
    let result = async {
        let resp = client
            .get(&url)
            .header("Authorization", &peer.auth)
            .send()
            .await?;
        if resp.status() == reqwest::StatusCode::NOT_FOUND {
            status::mark_unavailable(path, ts);
            tracing::warn!(
                "pull {path} from {}: 404 Not Found (blob missing) — will re-upload if present locally",
                peer.name
            );
            return Ok(PullResult::MissingBlob);
        }
        if !resp.status().is_success() {
            anyhow::bail!("pull {path} from {}: {}", peer.name, resp.status());
        }
        let data = resp.bytes().await?;
        let dest = cfg.root.join(path);
        if let Some(parent) = dest.parent() {
            std::fs::create_dir_all(parent)?;
        }
        mark_self_write(path);
        std::fs::write(&dest, &data)?;
        // Keep the file mtime on the version we downloaded so a stray re-upload
        // cannot outrank a newer edit.
        if let Ok(file) = std::fs::File::options().write(true).open(&dest) {
            let when = std::time::UNIX_EPOCH + Duration::from_secs(ts);
            let _ = file.set_modified(when);
        }
        status::clear_unavailable(path);
        tracing::info!("pulled {path}@{ts} from {}", peer.name);
        Ok(PullResult::Written)
    }
    .await;
    match &result {
        Ok(PullResult::MissingBlob) => return result,
        Ok(PullResult::Written) => {
            status::end_transfer(path, "up_to_date");
        }
        Err(_) => {
            status::end_transfer(path, "pending");
        }
    }
    result
}

async fn push_file_to_all(
    cfg: &ClientConfig,
    client: &reqwest::Client,
    peers: &[HttpPeer],
    path: &str,
) -> anyhow::Result<()> {
    status::begin_transfer(path, "up");
    let result = async {
        let full = cfg.root.join(path);
        // Stamp the version with the mtime captured before the read. A slow upload
        // of bytes from before an edit then cannot outrank that newer local write.
        let ts = file_mtime(&full).unwrap_or_else(FileIndex::now_ts);
        let data = std::fs::read(&full)?;
        let mut any_ok = false;
        let mut last_err: Option<String> = None;
        for peer in peers {
            let url = format!(
                "{}/v1/blob?path={}&ts={ts}",
                peer.base,
                encode_path(path)
            );
            match client
                .put(&url)
                .header("Authorization", &peer.auth)
                .body(data.clone())
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => {
                    let _: PutBlobResponse = r.json().await.unwrap_or(PutBlobResponse {
                        entry: mimic_core::index::FileEntry {
                            path: path.to_string(),
                            ts: 0,
                            hash: String::new(),
                            size: 0,
                            stored_name: String::new(),
                        },
                    });
                    tracing::info!(
                        "pushed {path} ({} bytes, ts={ts}) to {}",
                        data.len(),
                        peer.name
                    );
                    any_ok = true;
                }
                Ok(r) => {
                    let msg = format!("HTTP {}", r.status());
                    tracing::warn!("push {path} to {}: {msg}", peer.name);
                    last_err = Some(msg);
                }
                Err(e) => {
                    tracing::warn!("push {path} to {}: {e}", peer.name);
                    last_err = Some(e.to_string());
                }
            }
        }
        if any_ok {
            Ok(())
        } else {
            anyhow::bail!(last_err.unwrap_or_else(|| "push failed on all peers".into()))
        }
    }
    .await;
    status::end_transfer(
        path,
        if result.is_ok() {
            "up_to_date"
        } else {
            "pending"
        },
    );
    result
}

/// True when the server has a different, newer edit than the copy we last hashed.
/// A matching hash means the local file was simply removed (delete or rename).
fn remote_edit_wins(cached_hash: &str, cached_mtime: u64, remote_hash: &str, remote_ts: u64) -> bool {
    cached_hash != remote_hash && remote_ts > cached_mtime
}

async fn propagate_local_deletes(
    client: &reqwest::Client,
    peers: &[HttpPeer],
    merged: &mut mimic_core::index::FileIndex,
    cache: &mut scan::HashCache,
) {
    let absent = cache.take_absent();
    for ent in absent {
        if is_ignored(&ent.path) {
            continue;
        }
        let Some(remote) = merged.files.get(&ent.path) else {
            continue;
        };
        if remote_edit_wins(&ent.hash, ent.mtime, &remote.hash, remote.ts) {
            tracing::info!("remote newer than local delete, will pull {}", ent.path);
            continue;
        }
        let remote_path = remote.path.clone();
        match post_tombstone(client, peers, &remote_path, Vec::new()).await {
            Ok(()) => {
                merged.files.remove(&ent.path);
                tracing::info!("propagated local delete {remote_path}");
            }
            Err(e) => {
                tracing::warn!("tombstone {}: {e}", ent.path);
                cache.remember_absent(&ent);
            }
        }
    }
}

async fn post_tombstone(
    client: &reqwest::Client,
    peers: &[HttpPeer],
    path: &str,
    children: Vec<String>,
) -> anyhow::Result<()> {
    if status::is_known_tombstone(path) {
        return Ok(());
    }
    let tomb = Tombstone {
        path: path.to_string(),
        kind: TombstoneKind::Delete,
        ts: FileIndex::now_ts(),
        children,
    };
    let mut any_ok = false;
    let mut last_err: Option<String> = None;
    for peer in peers {
        let url = format!("{}/v1/tombstone", peer.base);
        match client
            .post(&url)
            .header("Authorization", &peer.auth)
            .json(&TombstoneRequest {
                tombstone: tomb.clone(),
            })
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                any_ok = true;
            }
            Ok(r) => last_err = Some(format!("HTTP {}", r.status())),
            Err(e) => last_err = Some(e.to_string()),
        }
    }
    if any_ok {
        status::note_tombstone(&tomb.path, tomb.ts);
        tracing::info!("tombstoned {}", tomb.path);
        Ok(())
    } else {
        anyhow::bail!(
            "{}",
            last_err.unwrap_or_else(|| "tombstone failed on all peers".into())
        )
    }
}

fn file_mtime(path: &Path) -> Option<u64> {
    let meta = std::fs::metadata(path).ok()?;
    let modified = meta.modified().ok()?;
    modified.duration_since(std::time::UNIX_EPOCH).ok().map(|d| d.as_secs())
}

fn is_regular_file(path: &Path) -> bool {
    path.is_file() && !path.is_symlink()
}

fn path_missing(path: &Path) -> bool {
    path.symlink_metadata().is_err()
}

async fn handle_fs_event(cfg: &ClientConfig, event: &notify::Event) -> anyhow::Result<()> {
    if APPLYING_REMOTE_DELETES.load(Ordering::Relaxed) {
        return Ok(());
    }

    let client = http_client()?;
    let peers = resolve_peers(cfg).await;
    if peers.is_empty() {
        return Ok(());
    }

    match event.kind {
        EventKind::Create(_)
        | EventKind::Modify(ModifyKind::Any)
        | EventKind::Modify(ModifyKind::Data(_))
        | EventKind::Modify(ModifyKind::Metadata(_))
        | EventKind::Modify(ModifyKind::Other) => {
            push_existing_files(cfg, &client, &peers, &event.paths).await;
        }
        // Rename (cut/paste, `mv`) is a name event: the destination still exists,
        // the source does not. Pushing only the destination leaves the old path
        // on the server, and the next scan pulls it back.
        EventKind::Modify(ModifyKind::Name(_)) => {
            let mut to_tombstone = Vec::new();
            let mut to_push_paths = Vec::new();
            for path in &event.paths {
                let Some(rel) = mimic_core::index::relative_path(&cfg.root, path) else {
                    continue;
                };
                if is_ignored(&rel) {
                    continue;
                }
                if is_regular_file(path) {
                    to_push_paths.push(path.clone());
                } else if path_missing(path) {
                    to_tombstone.push(rel);
                }
            }
            push_existing_files(cfg, &client, &peers, &to_push_paths).await;
            for rel in to_tombstone {
                if let Err(e) = post_tombstone(&client, &peers, &rel, Vec::new()).await {
                    tracing::warn!("tombstone {rel}: {e}");
                }
            }
        }
        EventKind::Remove(_) => {
            for path in &event.paths {
                if let Some(rel) = mimic_core::index::relative_path(&cfg.root, path) {
                    if is_ignored(&rel) {
                        continue;
                    }
                    if let Err(e) =
                        post_tombstone(&client, &peers, &rel, collect_former_children(cfg, &rel))
                            .await
                    {
                        tracing::warn!("tombstone {rel}: {e}");
                    }
                }
            }
        }
        _ => {}
    }
    Ok(())
}

async fn push_existing_files(
    cfg: &ClientConfig,
    client: &reqwest::Client,
    peers: &[HttpPeer],
    paths: &[PathBuf],
) {
    let mut to_push = Vec::new();
    for path in paths {
        if let Some(rel) = mimic_core::index::relative_path(&cfg.root, path) {
            if is_ignored(&rel) || !is_regular_file(path) || is_self_write(&rel) {
                continue;
            }
            to_push.push(rel);
        }
    }
    // Mark the whole burst pending first so gtk-files doesn't show checkmarks
    // on files that have not been uploaded yet.
    status::mark_pending(&to_push);
    for rel in &to_push {
        if let Err(e) = push_file_to_all(cfg, client, peers, rel).await {
            tracing::warn!("push {rel}: {e}");
        }
    }
}

fn collect_former_children(_cfg: &ClientConfig, _rel: &str) -> Vec<String> {
    // Without a local snapshot of the tree we cannot enumerate deleted dir children;
    // server retains versioned blobs; tombstone path covers the root delete.
    Vec::new()
}

pub async fn fetch_versions(
    cfg: &ClientConfig,
    path: &str,
) -> anyhow::Result<Vec<mimic_core::index::FileEntry>> {
    let client = http_client()?;
    let peers = resolve_peers(cfg).await;
    let mut all = Vec::new();
    for peer in peers {
        let url = format!("{}/v1/versions?path={}", peer.base, encode_path(path));
        if let Ok(r) = client
            .get(&url)
            .header("Authorization", &peer.auth)
            .send()
            .await
        {
            if r.status().is_success() {
                let resp: VersionsResponse = r.json().await?;
                for v in resp.versions {
                    if !all.iter().any(|e: &mimic_core::index::FileEntry| e.ts == v.ts && e.hash == v.hash)
                    {
                        all.push(v);
                    }
                }
            }
        }
    }
    all.sort_by_key(|e| e.ts);
    Ok(all)
}

#[cfg(test)]
mod tests {
    use super::remote_edit_wins;

    #[test]
    fn matching_hash_is_a_local_delete() {
        // Server timestamp is later than the file mtime after a normal upload.
        assert!(!remote_edit_wins("abc", 100, "abc", 150));
    }

    #[test]
    fn newer_remote_hash_is_kept() {
        assert!(remote_edit_wins("abc", 100, "def", 150));
    }

    #[test]
    fn older_remote_hash_loses_to_local_delete() {
        assert!(!remote_edit_wins("abc", 200, "def", 150));
    }
}
