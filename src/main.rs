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
mod self_update;
mod sql;
mod switch;
mod ui;

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
    #[cfg(unix)]
    /// Run eval files
    Eval(CLIArgs<eval::EvalArgs>),
    /// Manage projects
    Projects(CLIArgs<projects::ProjectsArgs>),
    #[command(name = "self")]
    /// Self-management commands
    SelfCommand(self_update::SelfArgs),
    /// Switch org and project context
    Switch(CLIArgs<switch::SwitchArgs>),
}

#[tokio::main]
async fn main() -> Result<()> {
    let argv: Vec<OsString> = std::env::args_os().collect();
    env::bootstrap_from_args(&argv)?;
    let cli = Cli::parse_from(argv);

    match cli.command {
        Commands::Sql(cmd) => sql::run(cmd.base, cmd.args).await?,
        #[cfg(unix)]
        Commands::Eval(cmd) => eval::run(cmd.base, cmd.args).await?,
        Commands::Projects(cmd) => projects::run(cmd.base, cmd.args).await?,
        Commands::SelfCommand(args) => self_update::run(args).await?,
        Commands::Switch(cmd) => switch::run(cmd.base, cmd.args).await?,
    }

    Ok(())
}
