mod api;
mod couch;
mod install;
mod mdns;
mod peer;
mod state;

use clap::{Parser, Subcommand};
use std::path::PathBuf;
use tracing_subscriber::EnvFilter;

#[derive(Parser, Debug)]
#[command(name = "gtk-sync", about = "GTK-Sync server")]
struct Cli {
    #[command(subcommand)]
    cmd: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Interactive first-time install: config, TLS, systemd hints
    Install {
        #[arg(long)]
        root: Option<PathBuf>,
        #[arg(long)]
        config: Option<PathBuf>,
        #[arg(long)]
        username: Option<String>,
        #[arg(long)]
        password: Option<String>,
        #[arg(long, default_value_t = mimic_core::DEFAULT_PORT)]
        port: u16,
        #[arg(long, default_value_t = mimic_core::DEFAULT_RETENTION_HOURS)]
        retention_hours: u64,
        #[arg(long)]
        non_interactive: bool,
        #[arg(long)]
        instance_name: Option<String>,
    },
    /// Run the server daemon
    Run {
        #[arg(long, default_value = "/etc/gtk-sync/server.toml")]
        config: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // rustls 0.23 requires an explicit process-level crypto provider when multiple are linked.
    let _ = rustls::crypto::ring::default_provider().install_default();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("gtk_sync=info".parse()?))
        .init();

    let cli = Cli::parse();
    match cli.cmd {
        Commands::Install {
            root,
            config,
            username,
            password,
            port,
            retention_hours,
            non_interactive,
            instance_name,
        } => install::run(
            root,
            config,
            username,
            password,
            port,
            retention_hours,
            non_interactive,
            instance_name,
        ),
        Commands::Run { config } => api::serve(config).await,
    }
}
