mod cmd;
mod config;
mod crf;
mod paths;
mod types;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "compression-pool", about = "Distributed video compression")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Work server: receives videos and runs ab-av1/ffmpeg.
    Worker {
        #[arg(long, default_value_t = 9111)]
        port: u16,
        #[arg(long, default_value_t = 1)]
        max_works: usize,
    },
    /// Coordinator: reads the configuration and distributes the work.
    Head {
        #[arg(long, default_value = "settings.toml")]
        settings: PathBuf,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Commands::Worker { port, max_works } => cmd::worker::run(port, max_works).await,
        Commands::Head { settings } => cmd::head::run(&settings).await,
    }
}
