//! `fluxmq` — the FluxMQ broker binary.

use fluxmq_server::{Cli, ServerConfig};
use tracing_subscriber::EnvFilter;

#[cfg(all(feature = "jemalloc", not(target_env = "msvc")))]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let mut cli = Cli::parse_with_config_file()?;
    cli.read_secret_files()?;
    // Offline tools (`backup`, `restore`) default to quieter logs.
    let default_level = if cli.command.is_some() {
        "warn"
    } else {
        "info"
    };
    let filter = match cli.log.as_deref() {
        Some(f) => EnvFilter::try_new(f)?,
        None => EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(default_level)),
    };
    tracing_subscriber::fmt().with_env_filter(filter).init();
    if let Some(cmd) = cli.command.take() {
        return fluxmq_server::backup_cli::run(cmd).await;
    }

    let config = ServerConfig::from(cli);
    if let Err(e) = fluxmq_server::run(config, fluxmq_server::shutdown::signal()).await {
        tracing::error!("{e:#}");
        return Err(e);
    }
    Ok(())
}
