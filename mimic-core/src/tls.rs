use rcgen::{CertificateParams, KeyPair, SanType};
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use sha2::{Digest, Sha256};
use std::fs;
use std::path::Path;

/// Generate a self-signed cert+key PEM pair for `name` (hostname).
pub fn generate_self_signed(cert_path: &Path, key_path: &Path, name: &str) -> anyhow::Result<String> {
    if let Some(p) = cert_path.parent() {
        fs::create_dir_all(p)?;
    }
    if let Some(p) = key_path.parent() {
        fs::create_dir_all(p)?;
    }

    let mut params = CertificateParams::new(vec![name.to_string(), "localhost".into(), "127.0.0.1".into()])?;
    params.subject_alt_names = vec![
        SanType::DnsName(name.try_into()?),
        SanType::DnsName("localhost".try_into()?),
        SanType::IpAddress(std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)),
    ];

    let key_pair = KeyPair::generate()?;
    let cert = params.self_signed(&key_pair)?;
    let cert_pem = cert.pem();
    let key_pem = key_pair.serialize_pem();

    fs::write(cert_path, &cert_pem)?;
    fs::write(key_path, &key_pem)?;

    Ok(cert_fingerprint_pem(&cert_pem)?)
}

pub fn cert_fingerprint_pem(pem: &str) -> anyhow::Result<String> {
    let mut reader = std::io::BufReader::new(pem.as_bytes());
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("pem parse: {e}"))?;
    let der = certs
        .first()
        .ok_or_else(|| anyhow::anyhow!("no cert in pem"))?;
    let mut h = Sha256::new();
    h.update(der.as_ref());
    Ok(hex::encode(h.finalize()))
}

pub fn cert_fingerprint_file(cert_path: &Path) -> anyhow::Result<String> {
    let pem = fs::read_to_string(cert_path)?;
    cert_fingerprint_pem(&pem)
}

pub fn load_certs(cert_path: &Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let mut reader = std::io::BufReader::new(fs::File::open(cert_path)?);
    let certs = rustls_pemfile::certs(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("load certs: {e}"))?;
    Ok(certs)
}

pub fn load_private_key(key_path: &Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    let mut reader = std::io::BufReader::new(fs::File::open(key_path)?);
    let keys = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| anyhow::anyhow!("load key: {e}"))?;
    let key = keys
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("no private key in file"))?;
    Ok(PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(key)))
}
