use crate::discover;
use mimic_core::config::{ClientConfig, DiscoveredPeer};
use std::io::{self, Write};
use std::path::PathBuf;
use std::time::Duration;

pub async fn setup(
    root: Option<PathBuf>,
    config: Option<PathBuf>,
    username: Option<String>,
    password: Option<String>,
    peers: Option<String>,
    non_interactive: bool,
    no_auto_discover: bool,
) -> anyhow::Result<()> {
    let root = match root {
        Some(r) => r,
        None if non_interactive => anyhow::bail!("--root required"),
        None => PathBuf::from(prompt("Folder to sync")?),
    };
    let root = PathBuf::from(root.to_string_lossy().trim_matches('"'));
    std::fs::create_dir_all(&root)?;

    let had_peers = peers.is_some();
    let mut discovered = if let Some(ref peers) = peers {
        parse_peers(peers)?
    } else if non_interactive {
        Vec::new()
    } else {
        tracing::info!("Discovering GTK-Sync servers via mDNS…");
        discover::browse_async(Duration::from_secs(3))
            .await
            .unwrap_or_default()
    };

    if discovered.is_empty() && !non_interactive {
        let extra = prompt("Server IP or domain [:port] (required)")?;
        if extra.is_empty() {
            anyhow::bail!("server address required");
        }
        discovered.extend(parse_peers(&extra)?);
    }

    if discovered.is_empty() {
        anyhow::bail!("at least one server --peers host[:port] is required");
    }

    let (username, password) = if non_interactive {
        (
            username.ok_or_else(|| anyhow::anyhow!("--username required"))?,
            password.ok_or_else(|| anyhow::anyhow!("--password required"))?,
        )
    } else {
        let u = username.unwrap_or_else(|| prompt("Username").unwrap_or_default());
        let p = password.unwrap_or_else(|| prompt("Password").unwrap_or_default());
        (u, p)
    };

    if username.is_empty() || password.is_empty() {
        anyhow::bail!("username and password required");
    }

    let auto_discover = !no_auto_discover && !had_peers;

    let cfg = ClientConfig {
        root,
        username,
        password,
        peers: discovered,
        static_peers: Vec::new(),
        auto_discover,
        pin_certs: true,
    };

    let config_path = config.unwrap_or_else(ClientConfig::default_path);
    cfg.save(&config_path)?;
    println!("Wrote {}", config_path.display());
    println!("Start with: gtk-sync-client run --config {}", config_path.display());
    Ok(())
}

fn parse_peers(raw: &str) -> anyhow::Result<Vec<DiscoveredPeer>> {
    let mut out = Vec::new();
    for s in raw.split(',') {
        let s = s.trim();
        if s.is_empty() {
            continue;
        }
        let (host, port) = if let Some((h, p)) = s.rsplit_once(':') {
            // Avoid treating bare IPv6 as host:port; require dotted or hostname with port
            if p.chars().all(|c| c.is_ascii_digit()) && !h.is_empty() {
                (h.to_string(), p.parse().unwrap_or(mimic_core::DEFAULT_PORT))
            } else {
                (s.to_string(), mimic_core::DEFAULT_PORT)
            }
        } else {
            (s.to_string(), mimic_core::DEFAULT_PORT)
        };
        out.push(DiscoveredPeer {
            name: format!("server-{host}"),
            host,
            port,
            cert_fingerprint: None,
            excluded: false,
        });
    }
    Ok(out)
}

fn prompt(msg: &str) -> anyhow::Result<String> {
    print!("{msg}: ");
    io::stdout().flush()?;
    let mut s = String::new();
    io::stdin().read_line(&mut s)?;
    Ok(s.trim().to_string())
}
