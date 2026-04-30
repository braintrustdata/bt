use anyhow::Result;
use clap::{Args, Subcommand};

use crate::args::BaseArgs;

mod pipeline;

#[derive(Debug, Clone, Args)]
pub struct DatasetsArgs {
    #[command(subcommand)]
    command: DatasetsCommands,
}

#[derive(Debug, Clone, Subcommand)]
enum DatasetsCommands {
    /// Run dataset pipeline workflows
    Pipeline(pipeline::PipelineArgs),
}

pub async fn run(base: BaseArgs, args: DatasetsArgs) -> Result<()> {
    match args.command {
        DatasetsCommands::Pipeline(args) => pipeline::run(base, args).await,
    }
}
