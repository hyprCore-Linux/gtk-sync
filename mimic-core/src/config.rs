use serde::{Deserialize, Serialize};
use std::path::PathBuf;

pub const DEFAULT_PORT: u16 = 8443;
pub const DEFAULT_RETENTION_HOURS: u64 = 24;
pub const MDNS_SERVICE: &str = "_gtk-sync._tcp.local.";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    pub root: PathBuf,
    pub listen_addr: String,
    pub port: u16,
    pub retention_hours: u64,
    pub username: String,
    /// Argon2 PHC string
    pub password_hash: String,
    /// Plaintext password for outbound peer sync (same mesh secret)
    #[serde(default)]
    pub peer_password: String,
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    /// Instance name advertised over mDNS
    pub instance_name: String,
    /// Data dir for versions + TLS (defaults to `root`)
    #[serde(default)]
    pub data_dir: Option<PathBuf>,
    /// CouchDB base URL for metadata (replaces index.json)
    #[serde(default = "default_couch_url")]
    pub couch_url: String,
    #[serde(default = "default_couch_db")]
    pub couch_db: String,
    /// Optional CouchDB admin / user (empty = no auth)
    #[serde(default)]
    pub couch_user: String,
    #[serde(default)]
    pub couch_password: String,
}

fn default_couch_url() -> String {
    "http://127.0.0.1:5984".into()
}

fn default_couch_db() -> String {
    "gtk-sync".into()
}

impl ServerConfig {
    pub fn data_dir(&self) -> PathBuf {
        self.data_dir.clone().unwrap_or_else(|| self.root.clone())
    }

    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }

    pub fn save(&self, path: &std::path::Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, toml::to_string_pretty(self)?)?;
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscoveredPeer {
    pub name: String,
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub cert_fingerprint: Option<String>,
    #[serde(default)]
    pub excluded: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientConfig {
    pub root: PathBuf,
    pub username: String,
    pub password: String,
    #[serde(default)]
    pub peers: Vec<DiscoveredPeer>,
    /// Extra static peers (host:port) if mDNS unavailable
    #[serde(default)]
    pub static_peers: Vec<String>,
    #[serde(default = "default_true")]
    pub auto_discover: bool,
    /// Accept any cert fingerprint on first connect and pin it
    #[serde(default = "default_true")]
    pub pin_certs: bool,
}

fn default_true() -> bool {
    true
}

impl ClientConfig {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        Ok(toml::from_str(&text)?)
    }

    pub fn save(&self, path: &std::path::Path) -> anyhow::Result<()> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        std::fs::write(path, toml::to_string_pretty(self)?)?;
        Ok(())
    }

    pub fn default_path() -> PathBuf {
        dirs_path().join("client.toml")
    }
}

fn dirs_path() -> PathBuf {
    dirs::config_dir()
        .unwrap_or_else(|| PathBuf::from("."))
        .join("gtk-sync")
}
