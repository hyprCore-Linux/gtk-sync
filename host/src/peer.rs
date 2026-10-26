use crate::mdns;
use crate::state::AppState;
use base64::Engine;
use mimic_core::protocol::IndexResponse;
use std::sync::Arc;
use std::time::Duration;

pub async fn peer_sync_loop(state: Arc<AppState>) {
    let mut interval = tokio::time::interval(Duration::from_secs(15));
    loop {
        interval.tick().await;
        if let Err(e) = sync_once(&state).await {
            tracing::debug!("peer sync: {e}");
        }
    }
}

async fn sync_once(state: &AppState) -> anyhow::Result<()> {
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
        let local_vers_paths: std::collections::HashSet<_> =
            local_files.iter().map(|e| e.path.clone()).collect();

        // Pull missing / newer version blobs for remote current files
        for f in &remote.files {
            let local = local_files.iter().find(|e| e.path == f.path);
            let need = match local {
                Some(e) if e.hash == f.hash => {
                    !state.versions.join(&e.stored_name).is_file()
                }
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

            let url = format!("{base}/v1/blob?path={}&ts={}", urlencoding_encode(&f.path), f.ts);
            let blob = match client
                .get(&url)
                .header("Authorization", &auth)
                .send()
                .await
            {
                Ok(r) if r.status().is_success() => r.bytes().await?,
                _ => continue,
            };
            let _ = state.put_blob(&f.path, &blob, Some(f.ts)).await?;
        }

        // Import remote tombstones
        for t in &remote.tombstones {
            let _ = state.couch.add_tombstone(t).await;
        }

        // Pull extra history for paths we care about (current set)
        for path in local_vers_paths.iter().chain(remote.files.iter().map(|f| &f.path)) {
            let url = format!("{base}/v1/versions?path={}", urlencoding_encode(path));
            let Ok(r) = client
                .get(&url)
                .header("Authorization", &auth)
                .send()
                .await
            else {
                continue;
            };
            if !r.status().is_success() {
                continue;
            }
            let Ok(vr) = r.json::<mimic_core::protocol::VersionsResponse>().await else {
                continue;
            };
            for e in vr.versions {
                if state.couch.get_version(&e.path, e.ts).await?.is_some() {
                    continue;
                }
                let url = format!(
                    "{base}/v1/blob?path={}&ts={}",
                    urlencoding_encode(&e.path),
                    e.ts
                );
                let Ok(br) = client
                    .get(&url)
                    .header("Authorization", &auth)
                    .send()
                    .await
                else {
                    continue;
                };
                if !br.status().is_success() {
                    continue;
                }
                let Ok(blob) = br.bytes().await else {
                    continue;
                };
                let _ = state.put_blob(&e.path, &blob, Some(e.ts)).await;
            }
        }

        tracing::debug!("synced with peer {name} ({host}:{port})");
    }
    Ok(())
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
