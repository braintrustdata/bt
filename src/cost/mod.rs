use anyhow::Result;
use clap::{Args, Subcommand};

use crate::{args::BaseArgs, project_context::resolve_project_command_context_with_auth_mode};

mod experiments;

pub(crate) use crate::project_context::ProjectContext as ResolvedContext;

#[derive(Debug, Clone, Args)]
#[command(
    about = "Estimate LLM cost for Braintrust resources",
    after_help = "\
Examples:
  bt cost experiments          Estimate LLM cost per experiment in the active project
  bt cost experiments --json   Emit structured per-experiment cost rows
"
)]
pub struct CostArgs {
    #[command(subcommand)]
    command: Option<CostCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum CostCommands {
    /// Estimate LLM cost per experiment in the active project
    Experiments,
}

pub async fn run(base: BaseArgs, args: CostArgs) -> Result<()> {
    // Every `bt cost` command is read-only: it only issues `GET /v1/experiment`
    // and `POST /btql` (a read query).
    let ctx = resolve_project_command_context_with_auth_mode(&base, true).await?;

    match args.command {
        None | Some(CostCommands::Experiments) => experiments::run(&ctx, base.json).await,
    }
}
