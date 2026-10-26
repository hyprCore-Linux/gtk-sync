use crate::status;
use crate::sync::{encode_path, fetch_versions, http_client, resolve_peers};
use mimic_core::config::ClientConfig;
use mimic_core::protocol::{
    RestoreRequest, RestoreResponse, RestoreTreeRequest, RestoreTreeResponse,
};
use std::path::Path;

pub async fn list_versions(config_path: &Path, path: &str, json: bool) -> anyhow::Result<()> {
    let cfg = ClientConfig::load(config_path)?;
    let versions = fetch_versions(&cfg, path).await?;
    if json {
        let rows: Vec<serde_json::Value> = versions
            .iter()
            .map(|v| {
                serde_json::json!({
                    "ts": v.ts,
                    "size": v.size,
                    "hash": v.hash,
                    "stored_name": v.stored_name,
                })
            })
            .collect();
        println!("{}", serde_json::to_string(&rows)?);
        return Ok(());
    }
    if versions.is_empty() {
        println!("No versions found for {path}");
        return Ok(());
    }
    println!("Versions for {path}:");
    for v in versions {
        println!(
            "  ts={} size={} hash={}… stored={}",
            v.ts,
            v.size,
            &v.hash[..12.min(v.hash.len())],
            v.stored_name
        );
    }
    println!("Restore with: gtk-sync-client restore {path} <ts>");
    Ok(())
}

pub async fn restore(config_path: &Path, path: &str, ts: u64) -> anyhow::Result<()> {
    let cfg = ClientConfig::load(config_path)?;
    let client = http_client()?;
    let peers = resolve_peers(&cfg).await;
    if peers.is_empty() {
        anyhow::bail!("no peers available");
    }

    // Prefer server-side restore then pull, falling back to direct blob pull
    let mut restored = false;
    for peer in &peers {
        let url = format!("{}/v1/restore", peer.base);
        match client
            .post(&url)
            .header("Authorization", &peer.auth)
            .json(&RestoreRequest {
                path: path.to_string(),
                ts,
            })
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                let resp: RestoreResponse = r.json().await?;
                // Download the new current version
                let blob_url = format!(
                    "{}/v1/blob?path={}&ts={}",
                    peer.base,
                    encode_path(path),
                    resp.entry.ts
                );
                let data = client
                    .get(&blob_url)
                    .header("Authorization", &peer.auth)
                    .send()
                    .await?
                    .bytes()
                    .await?;
                let dest = cfg.root.join(path);
                if let Some(parent) = dest.parent() {
                    std::fs::create_dir_all(parent)?;
                }
                std::fs::write(&dest, &data)?;
                tracing::info!(
                    "restored {path} from ts={ts} -> new ts={} via {}",
                    resp.entry.ts,
                    peer.name
                );
                restored = true;

                // Push new version to other peers
                let body = data.to_vec();
                for other in &peers {
                    if other.base == peer.base {
                        continue;
                    }
                    let put_url = format!("{}/v1/blob?path={}", other.base, encode_path(path));
                    let _ = client
                        .put(&put_url)
                        .header("Authorization", &other.auth)
                        .body(body.clone())
                        .send()
                        .await;
                }
                break;
            }
            Ok(r) => tracing::debug!("restore via {}: {}", peer.name, r.status()),
            Err(e) => tracing::debug!("restore via {}: {e}", peer.name),
        }
    }

    if !restored {
        // Direct blob fetch of historical version
        for peer in &peers {
            let url = format!(
                "{}/v1/blob?path={}&ts={ts}",
                peer.base,
                encode_path(path)
            );
            if let Ok(r) = client
                .get(&url)
                .header("Authorization", &peer.auth)
                .send()
                .await
            {
                if r.status().is_success() {
                    let data = r.bytes().await?;
                    let dest = cfg.root.join(path);
                    if let Some(parent) = dest.parent() {
                        std::fs::create_dir_all(parent)?;
                    }
                    std::fs::write(&dest, &data)?;
                    // Re-upload as new version
                    for other in &peers {
                        let put_url =
                            format!("{}/v1/blob?path={}", other.base, encode_path(path));
                        let _ = client
                            .put(&put_url)
                            .header("Authorization", &other.auth)
                            .body(data.to_vec())
                            .send()
                            .await;
                    }
                    println!("Restored {path} from @{ts}");
                    return Ok(());
                }
            }
        }
        anyhow::bail!("could not restore {path}@{ts} from any peer");
    }

    println!("Restored {path} (from historical ts={ts})");
    Ok(())
}

/// Undelete a folder: server resurrects every file under `path`, then we pull blobs.
pub async fn restore_tree(config_path: &Path, path: &str) -> anyhow::Result<()> {
    let cfg = ClientConfig::load(config_path)?;
    let client = http_client()?;
    let peers = resolve_peers(&cfg).await;
    if peers.is_empty() {
        anyhow::bail!("no peers available");
    }

    let prefix = path.trim_matches('/').to_string();
    if prefix.is_empty() {
        anyhow::bail!("path must not be empty");
    }

    let mut response: Option<(crate::sync::HttpPeer, RestoreTreeResponse)> = None;
    for peer in &peers {
        let url = format!("{}/v1/restore-tree", peer.base);
        match client
            .post(&url)
            .header("Authorization", &peer.auth)
            .json(&RestoreTreeRequest {
                path: prefix.clone(),
            })
            .send()
            .await
        {
            Ok(r) if r.status().is_success() => {
                let body: RestoreTreeResponse = r.json().await?;
                response = Some((
                    crate::sync::HttpPeer {
                        base: peer.base.clone(),
                        auth: peer.auth.clone(),
                        name: peer.name.clone(),
                    },
                    body,
                ));
                break;
            }
            Ok(r) => tracing::warn!("restore-tree via {}: {}", peer.name, r.status()),
            Err(e) => tracing::warn!("restore-tree via {}: {e}", peer.name),
        }
    }

    let Some((peer, body)) = response else {
        anyhow::bail!("could not restore-tree {prefix} from any peer");
    };

    let mut ok = 0usize;
    let mut fail = 0usize;
    for entry in &body.restored {
        let blob_url = format!(
            "{}/v1/blob?path={}&ts={}",
            peer.base,
            encode_path(&entry.path),
            entry.ts
        );
        let result = async {
            let resp = client
                .get(&blob_url)
                .header("Authorization", &peer.auth)
                .send()
                .await?;
            if !resp.status().is_success() {
                anyhow::bail!("blob {}: {}", entry.path, resp.status());
            }
            let data = resp.bytes().await?;
            let dest = cfg.root.join(&entry.path);
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(&dest, &data)?;
            Ok::<(), anyhow::Error>(())
        }
        .await;

        match result {
            Ok(()) => ok += 1,
            Err(e) => {
                tracing::warn!("pull restored {}: {e}", entry.path);
                fail += 1;
            }
        }
    }

    // Ensure the folder exists even if it only had empty subdirs.
    let _ = std::fs::create_dir_all(cfg.root.join(&prefix));
    status::clear_tombstones_under(&prefix);

    println!(
        "Restored folder {prefix}: {ok} files ({} tombstones cleared{})",
        body.cleared_tombstones,
        if fail > 0 {
            format!(", {fail} failed")
        } else {
            String::new()
        }
    );
    if fail > 0 && ok == 0 {
        anyhow::bail!("restore-tree failed to pull any files for {prefix}");
    }
    Ok(())
}
