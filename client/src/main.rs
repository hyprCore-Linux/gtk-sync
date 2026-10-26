mod dialog;
mod discover;
mod restore;
mod scan;
mod status;
mod sync;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "gtk-sync-client", about = "GTK-Sync client")]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// First-run setup: server address, credentials, write config
    Setup {
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        username: Option<String>,
        #[arg(long)]
        password: Option<String>,
        /// Comma-separated host or host:port
        #[arg(long)]
        peers: Option<String>,
        #[arg(long)]
        non_interactive: bool,
        /// Disable mDNS auto-discovery (use configured peers only)
        #[arg(long)]
        no_auto_discover: bool,
    },
    /// Run the client sync daemon
    Run {
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// List versions of a relative path on discovered/configured servers
    Versions {
        path: String,
        #[arg(long)]
        config: Option<PathBuf>,
        /// Machine-readable JSON array for gtk-files
        #[arg(long)]
        json: bool,
    },
    /// Restore a path to a given timestamp (pushes new current version)
    Restore {
        path: String,
        ts: u64,
        #[arg(long)]
        config: Option<PathBuf>,
    },
    /// Restore a deleted folder and all file contents under it
    RestoreTree {
        path: String,
        #[arg(long)]
        config: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let _ = rustls::crypto::ring::default_provider().install_default();

    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::from_default_env().add_directive("gtk_sync_client=info".parse()?),
        )
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Commands::Setup {
            root,
            config,
            username,
            password,
            peers,
            non_interactive,
            no_auto_discover,
        } => {
            dialog::setup(
                root,
                config,
                username,
                password,
                peers,
                non_interactive,
                no_auto_discover,
            )
            .await
        }
        Commands::Run { config } => {
            let path = config.unwrap_or_else(mimic_core::ClientConfig::default_path);
            sync::run(path).await
        }
        Commands::Versions { path, config, json } => {
            let cfg_path = config.unwrap_or_else(mimic_core::ClientConfig::default_path);
            restore::list_versions(&cfg_path, &path, json).await
        }
        Commands::Restore { path, ts, config } => {
            let cfg_path = config.unwrap_or_else(mimic_core::ClientConfig::default_path);
            restore::restore(&cfg_path, &path, ts).await
        }
        Commands::RestoreTree { path, config } => {
            let cfg_path = config.unwrap_or_else(mimic_core::ClientConfig::default_path);
            restore::restore_tree(&cfg_path, &path).await
        }
    }
}
