use sha2::{Digest, Sha256};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VersionRef {
    pub logical_name: String,
    pub ts: u64,
}

/// `todo.txt` + 1785602919 -> `todo.txt-1785602919`
pub fn versioned_filename(logical: &str, ts: u64) -> String {
    format!("{logical}-{ts}")
}

/// Parse `todo.txt-1785602919` -> Some(("todo.txt", 1785602919))
pub fn parse_versioned_name(name: &str) -> Option<VersionRef> {
    let idx = name.rfind('-')?;
    let (logical, ts_str) = name.split_at(idx);
    let ts_str = &ts_str[1..];
    if logical.is_empty() {
        return None;
    }
    let ts: u64 = ts_str.parse().ok()?;
    // Require timestamp-looking suffix (all digits, reasonable length)
    if ts_str.len() < 9 {
        return None;
    }
    Some(VersionRef {
        logical_name: logical.to_string(),
        ts,
    })
}

pub fn logical_name_from_versioned(path: &Path) -> Option<String> {
    let name = path.file_name()?.to_str()?;
    parse_versioned_name(name).map(|v| {
        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                return format!(
                    "{}/{}",
                    parent.to_string_lossy().replace('\\', "/"),
                    v.logical_name
                );
            }
        }
        v.logical_name
    })
}

pub fn content_hash(data: &[u8]) -> String {
    let mut h = Sha256::new();
    h.update(data);
    hex::encode(h.finalize())
}

/// Stream a file through SHA-256 so large trees do not get read into RAM.
pub fn content_hash_path(path: &Path) -> anyhow::Result<String> {
    use std::io::Read;
    let mut f = std::fs::File::open(path)?;
    let mut h = Sha256::new();
    let mut buf = [0u8; 64 * 1024];
    loop {
        let n = f.read(&mut buf)?;
        if n == 0 {
            break;
        }
        h.update(&buf[..n]);
    }
    Ok(hex::encode(h.finalize()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_roundtrip() {
        let n = versioned_filename("todo.txt", 1785602919);
        assert_eq!(n, "todo.txt-1785602919");
        let p = parse_versioned_name(&n).unwrap();
        assert_eq!(p.logical_name, "todo.txt");
        assert_eq!(p.ts, 1785602919);
    }

    #[test]
    fn rejects_short_suffix() {
        assert!(parse_versioned_name("file-1").is_none());
    }
}
