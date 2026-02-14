use anyhow::Result;
use clap::{Parser, Subcommand};
use std::ffi::OsString;

mod args;
mod env;
#[cfg(unix)]
mod eval;
mod http;
mod login;
mod projects;
mod prompts;
mod self_update;
mod sql;
mod sync;
mod traces;
mod ui;
mod utils;

use crate::args::CLIArgs;

#[derive(Debug, Parser)]
#[command(name = "bt", about = "Braintrust CLI", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Run SQL queries against Braintrust
    Sql(CLIArgs<sql::SqlArgs>),
    /// View project traces in an interactive terminal UI
    #[command(visible_alias = "trace")]
    Traces(CLIArgs<traces::TracesArgs>),
    #[cfg(unix)]
    /// Run eval files
    Eval(CLIArgs<eval::EvalArgs>),
    /// Manage projects
    Projects(CLIArgs<projects::ProjectsArgs>),
    #[command(name = "self")]
    /// Self-management commands
    SelfCommand(self_update::SelfArgs),
    /// Manage prompts
    Prompts(CLIArgs<prompts::PromptsArgs>),
    /// Synchronize project logs between Braintrust and local NDJSON files
    Sync(CLIArgs<sync::SyncArgs>),
}

#[tokio::main]
async fn main() -> Result<()> {
    let argv: Vec<OsString> = std::env::args_os().collect();
    env::bootstrap_from_args(&argv)?;
    let cli = Cli::parse_from(argv);

    match cli.command {
        Commands::Sql(cmd) => sql::run(cmd.base, cmd.args).await?,
        Commands::Traces(cmd) => traces::run(cmd.base, cmd.args).await?,
        #[cfg(unix)]
        Commands::Eval(cmd) => eval::run(cmd.base, cmd.args).await?,
        Commands::Projects(cmd) => projects::run(cmd.base, cmd.args).await?,
        Commands::SelfCommand(args) => self_update::run(args).await?,
        Commands::Prompts(cmd) => prompts::run(cmd.base, cmd.args).await?,
        Commands::Sync(cmd) => sync::run(cmd.base, cmd.args).await?,
    }

    Ok(())
}
