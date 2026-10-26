use mdns_sd::{ServiceDaemon, ServiceEvent};
use mimic_core::config::{DiscoveredPeer, MDNS_SERVICE};
use std::time::{Duration, Instant};

pub fn browse(timeout: Duration) -> anyhow::Result<Vec<DiscoveredPeer>> {
    let daemon = ServiceDaemon::new()?;
    let receiver = daemon.browse(MDNS_SERVICE)?;
    let deadline = Instant::now() + timeout;
    let mut out: Vec<DiscoveredPeer> = Vec::new();

    while Instant::now() < deadline {
        let remain = deadline.saturating_duration_since(Instant::now());
        match receiver.recv_timeout(remain) {
            Ok(ServiceEvent::ServiceResolved(info)) => {
                let name = info.get_fullname().to_string();
                let port = info.get_port();
                let fp = info
                    .get_properties()
                    .get("fp")
                    .map(|v| v.val_str().to_string());
                for addr in info.get_addresses() {
                    // Prefer IPv4; skip IPv6 (zone IDs break HTTPS URLs)
                    let std::net::IpAddr::V4(v4) = *addr else {
                        continue;
                    };
                    let host = v4.to_string();
                    if out.iter().any(|p| p.host == host && p.port == port) {
                        continue;
                    }
                    out.push(DiscoveredPeer {
                        name: name.clone(),
                        host,
                        port,
                        cert_fingerprint: fp.clone(),
                        excluded: false,
                    });
                }
            }
            Ok(_) => {}
            Err(_) => break,
        }
    }
    let _ = daemon.stop_browse(MDNS_SERVICE);
    let _ = daemon.shutdown();
    Ok(out)
}

pub async fn browse_async(timeout: Duration) -> anyhow::Result<Vec<DiscoveredPeer>> {
    tokio::task::spawn_blocking(move || browse(timeout)).await?
}
