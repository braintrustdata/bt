use anyhow::Result;
use clap::{Parser, Subcommand};
use std::ffi::OsString;

mod args;
mod auth;
mod config;
mod env;
#[cfg(unix)]
mod eval;
mod http;
mod init;
mod projects;
mod prompts;
mod self_update;
mod sql;
mod status;
mod switch;
mod sync;
mod traces;
mod ui;
mod utils;

use crate::args::CLIArgs;

const DEFAULT_CANARY_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "-canary.dev");
const CLI_VERSION: &str = match option_env!("BT_VERSION_STRING") {
    Some(version) => version,
    None => DEFAULT_CANARY_VERSION,
};

#[derive(Debug, Parser)]
#[command(name = "bt", about = "Braintrust CLI", version = CLI_VERSION)]
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
    /// Manage authentication profiles
    Auth(CLIArgs<auth::AuthArgs>),
    /// View logs, traces, and spans
    View(CLIArgs<traces::ViewArgs>),
    #[cfg(unix)]
    /// Run eval files
    Eval(CLIArgs<eval::EvalArgs>),
    /// Manage projects
    Projects(CLIArgs<projects::ProjectsArgs>),
    /// Manage prompts
    Prompts(CLIArgs<prompts::PromptsArgs>),
    #[command(name = "self")]
    /// Self-management commands
    SelfCommand(self_update::SelfArgs),
    /// Synchronize project logs between Braintrust and local NDJSON files
    Sync(CLIArgs<sync::SyncArgs>),
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
        Commands::Auth(cmd) => {
            let (base, args) = cmd.with_config(&cfg);
            auth::run(base, args).await?
        }
        Commands::View(cmd) => {
            let (base, args) = cmd.with_config(&cfg);
            traces::run(base, args).await?
        }
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
        Commands::Prompts(cmd) => {
            let (base, args) = cmd.with_config(&cfg);
            prompts::run(base, args).await?
        }
        Commands::Sync(cmd) => {
            let (base, args) = cmd.with_config(&cfg);
            sync::run(base, args).await?
        }
        Commands::SelfCommand(args) => self_update::run(args).await?,
        Commands::Switch(cmd) => {
            // Don't merge config - switch command inspects config directly
            switch::run(cmd.base, cmd.args).await?
        }
        Commands::Status(cmd) => {
            // Don't merge config - status command inspects config directly
            status::run(cmd.base, cmd.args).await?
        }
        Commands::Config(cmd) => config::run(cmd.base, cmd.args)?,
    }

    Ok(())
}
