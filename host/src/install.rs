use mimic_core::auth::hash_password;
use mimic_core::config::ServerConfig;
use mimic_core::tls::generate_self_signed;
use mimic_core::{DEFAULT_PORT, DEFAULT_RETENTION_HOURS};
use std::io::{self, Write};
use std::path::PathBuf;

pub fn run(
    root: Option<PathBuf>,
    config_path: Option<PathBuf>,
    username: Option<String>,
    password: Option<String>,
    port: u16,
    retention_hours: u64,
    non_interactive: bool,
    instance_name: Option<String>,
) -> anyhow::Result<()> {
    let root = match root {
        Some(r) => r,
        None if non_interactive => anyhow::bail!("--root required in non-interactive mode"),
        None => {
            print!("Storage directory [/var/lib/gtk-sync]: ");
            io::stdout().flush()?;
            let mut s = String::new();
            io::stdin().read_line(&mut s)?;
            let trimmed = s.trim();
            if trimmed.is_empty() {
                PathBuf::from("/var/lib/gtk-sync")
            } else {
                PathBuf::from(trimmed)
            }
        }
    };
    std::fs::create_dir_all(&root)?;

    let username = match username {
        Some(u) => u,
        None if non_interactive => anyhow::bail!("--username required"),
        None => {
            print!("Username: ");
            io::stdout().flush()?;
            let mut s = String::new();
            io::stdin().read_line(&mut s)?;
            s.trim().to_string()
        }
    };

    let password = match password {
        Some(p) => p,
        None if non_interactive => anyhow::bail!("--password required"),
        None => {
            print!("Password: ");
            io::stdout().flush()?;
            let mut s = String::new();
            io::stdin().read_line(&mut s)?;
            s.trim().to_string()
        }
    };

    if username.is_empty() || password.is_empty() {
        anyhow::bail!("username and password are required");
    }

    let config_path =
        config_path.unwrap_or_else(|| PathBuf::from("/etc/gtk-sync/server.toml"));
    let data_dir = root.clone();
    std::fs::create_dir_all(data_dir.join("versions"))?;

    let cert_path = data_dir.join("cert.pem");
    let key_path = data_dir.join("key.pem");
    let instance_name = instance_name.unwrap_or_else(|| {
        hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "gtk-sync".into())
    });

    let fp = generate_self_signed(&cert_path, &key_path, &instance_name)?;
    tracing::info!("TLS cert fingerprint (SHA-256): {fp}");

    let cfg = ServerConfig {
        root: root.clone(),
        listen_addr: "0.0.0.0".into(),
        port: if port == 0 { DEFAULT_PORT } else { port },
        retention_hours: if retention_hours == 0 {
            DEFAULT_RETENTION_HOURS
        } else {
            retention_hours
        },
        username,
        password_hash: hash_password(&password)?,
        peer_password: password.clone(),
        cert_path,
        key_path,
        instance_name: instance_name.clone(),
        data_dir: Some(data_dir),
        couch_url: std::env::var("MIMIC_COUCH_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:5984".into()),
        couch_db: std::env::var("MIMIC_COUCH_DB").unwrap_or_else(|_| "gtk-sync".into()),
        couch_user: std::env::var("MIMIC_COUCH_USER").unwrap_or_default(),
        couch_password: std::env::var("MIMIC_COUCH_PASSWORD").unwrap_or_else(|_| password.clone()),
    };
    cfg.save(&config_path)?;
    println!("Wrote config to {}", config_path.display());
    println!("Storage: {}", root.display());
    println!("  versions/  cert.pem  key.pem");
    println!("  metadata: CouchDB {} / {}", cfg.couch_url, cfg.couch_db);
    println!("Cert fingerprint: {fp}");
    println!();
    println!("Start with: gtk-sync run --config {}", config_path.display());
    println!("Or enable systemd: sudo systemctl enable --now gtk-sync");
    Ok(())
}
