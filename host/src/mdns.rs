use mdns_sd::{ServiceDaemon, ServiceInfo};
use mimic_core::config::{ServerConfig, MDNS_SERVICE};
use std::collections::HashMap;

pub struct MdnsGuard {
    daemon: ServiceDaemon,
    fullname: String,
}

impl Drop for MdnsGuard {
    fn drop(&mut self) {
        let _ = self.daemon.unregister(&self.fullname);
        let _ = self.daemon.shutdown();
    }
}

pub fn advertise(config: &ServerConfig, cert_fingerprint: &str) -> anyhow::Result<MdnsGuard> {
    let daemon = ServiceDaemon::new()?;
    let host_name = format!("{}.local.", config.instance_name);
    let mut props = HashMap::new();
    props.insert("fp".to_string(), cert_fingerprint.to_string());
    props.insert("port".to_string(), config.port.to_string());

    let service = ServiceInfo::new(
        MDNS_SERVICE,
        &config.instance_name,
        &host_name,
        "",
        config.port,
        props,
    )?
    .enable_addr_auto();

    let fullname = service.get_fullname().to_string();
    daemon.register(service)?;
    tracing::info!("mDNS advertising {fullname} on port {}", config.port);
    Ok(MdnsGuard { daemon, fullname })
}

/// Browse for other mimic servers. Returns (name, host, port, fingerprint).
pub fn browse_peers(timeout_ms: u64) -> anyhow::Result<Vec<(String, String, u16, String)>> {
    let daemon = ServiceDaemon::new()?;
    let receiver = daemon.browse(MDNS_SERVICE)?;
    let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
    let mut out = Vec::new();
    while std::time::Instant::now() < deadline {
        let remain = deadline.saturating_duration_since(std::time::Instant::now());
        match receiver.recv_timeout(remain) {
            Ok(event) => {
                if let mdns_sd::ServiceEvent::ServiceResolved(info) = event {
                    let name = info.get_fullname().to_string();
                    let port = info.get_port();
                    let fp = info
                        .get_properties()
                        .get("fp")
                        .map(|v| v.val_str().to_string())
                        .unwrap_or_default();
                    for addr in info.get_addresses() {
                        let std::net::IpAddr::V4(v4) = *addr else {
                            continue;
                        };
                        out.push((name.clone(), v4.to_string(), port, fp.clone()));
                    }
                }
            }
            Err(_) => break,
        }
    }
    let _ = daemon.stop_browse(MDNS_SERVICE);
    let _ = daemon.shutdown();
    // Dedupe by host:port
    out.sort();
    out.dedup_by(|a, b| a.1 == b.1 && a.2 == b.2);
    Ok(out)
}
