pub mod client;
pub mod orchestrator;
pub mod queue;

use crate::config::Settings;
use anyhow::Result;
use std::path::Path;

pub async fn run(settings_path: &Path) -> Result<()> {
    let settings = Settings::load(settings_path)?;
    let config = settings.validate()?;
    let orchestrator = orchestrator::Orchestrator::new(config)?;
    orchestrator.run().await
}
