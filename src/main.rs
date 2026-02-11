use anyhow::Result;
use clap::{Parser, Subcommand};
use std::ffi::OsString;

mod args;
mod config;
mod env;
#[cfg(unix)]
mod eval;
mod http;
mod init;
mod login;
mod projects;
mod self_update;
mod sql;
mod status;
mod switch;
mod traces;
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
    /// Initialize .bt config directory and files
    Init(CLIArgs<init::InitArgs>),
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
    /// Switch org and project context
    Switch(CLIArgs<switch::SwitchArgs>),
    /// Show current org and project context
    Status(CLIArgs<status::StatusArgs>),
    /// View and modify config
    Config(CLIArgs<config::ConfigArgs>),
}

#[tokio::main]
async fn main() -> Result<()> {
    let argv: Vec<OsString> = std::env::args_os().collect();
    env::bootstrap_from_args(&argv)?;

    let cli = Cli::parse_from(argv);

    let cfg = config::load().unwrap_or_default();

    match cli.command {
        Commands::Traces(cmd) => traces::run(cmd.base, cmd.args).await?,
        Commands::Init(cmd) => {
            // Don't merge config - init should prompt for project interactively
            init::run(cmd.base, cmd.args).await?
        }
        Commands::Sql(cmd) => {
            let (base, args) = cmd.with_config(&cfg);
            sql::run(base, args).await?
        }
        #[cfg(unix)]
        Commands::Eval(cmd) => {
            let (base, args) = cmd.with_config(&cfg);
            eval::run(base, args).await?
        }
        Commands::Projects(cmd) => {
            let (base, args) = cmd.with_config(&cfg);
            projects::run(base, args).await?
        }
        Commands::SelfCommand(args) => self_update::run(args).await?,
        Commands::Switch(cmd) => {
            let (base, args) = cmd.with_config(&cfg);
            switch::run(base, args).await?
        }
        Commands::Status(cmd) => {
            // Don't merge config - status command inspects config directly
            status::run(cmd.base, cmd.args).await?
        }
        Commands::Config(cmd) => config::run(cmd.base, cmd.args)?,
    }

    Ok(())
}
