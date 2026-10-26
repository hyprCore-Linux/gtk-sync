//! GTK-Sync shared types: config, auth, versioning, retention, TLS helpers.

pub mod auth;
pub mod config;
pub mod ignore;
pub mod index;
pub mod protocol;
pub mod retention;
pub mod tls;
pub mod versioning;

pub use auth::{hash_password, verify_password};
pub use config::{ClientConfig, ServerConfig, DEFAULT_PORT, DEFAULT_RETENTION_HOURS, MDNS_SERVICE};
pub use ignore::{is_ignored, is_ignored_dir_name};
pub use index::{FileEntry, FileIndex, Tombstone, TombstoneKind};
pub use protocol::*;
pub use retention::purge_expired;
pub use versioning::{
    content_hash, content_hash_path, logical_name_from_versioned, parse_versioned_name,
    versioned_filename, VersionRef,
};
