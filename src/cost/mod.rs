use std::io::{self, Write as _};

use anyhow::Result;
use clap::{Args, CommandFactory, Subcommand};

use crate::{args::BaseArgs, project_context::resolve_project_command_context_with_auth_mode};

mod experiments;
mod logs;
mod pricing;

pub(crate) use crate::project_context::ProjectContext as ResolvedContext;

#[derive(Debug, Clone, Args)]
#[command(
    about = "Estimate LLM cost for Braintrust resources",
    after_help = "\
Examples:
  bt cost experiments                       Estimate LLM cost per experiment
  bt cost logs                              Estimate log cost over the last 7 days
  bt cost logs --window 30d                 Estimate log cost over the last 30 days
  bt cost logs --pricing-file prices.toml   Price otherwise-unpriced token usage
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
    /// Estimate cost for logs in the active project
    Logs(logs::LogsArgs),
}

pub async fn run(base: BaseArgs, args: CostArgs) -> Result<()> {
    let Some(command) = args.command else {
        return print_help();
    };
    let ctx = resolve_project_command_context_with_auth_mode(&base, true).await?;

    match command {
        CostCommands::Experiments => experiments::run(&ctx, base.json).await,
        CostCommands::Logs(log_args) => logs::run(&ctx, log_args, base.json).await,
    }
}

fn print_help() -> Result<()> {
    let mut root = crate::Cli::command();
    let command = root
        .find_subcommand_mut("cost")
        .expect("cost is a registered subcommand");
    command.set_bin_name("bt cost");
    let mut stdout = io::stdout().lock();
    command.write_help(&mut stdout)?;
    writeln!(stdout)?;
    Ok(())
}
