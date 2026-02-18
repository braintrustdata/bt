use anyhow::Result;
use clap::{Parser, Subcommand};
use std::ffi::OsString;

mod args;
mod env;
#[cfg(unix)]
mod eval;
mod experiments;
mod functions;
mod http;
mod login;
mod projects;
mod prompts;
mod scorers;
mod self_update;
mod setup;
mod sql;
mod sync;
mod tools;
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
#[command(
    name = "bt",
    about = "Braintrust CLI",
    version,
    after_help = "Docs: https://braintrust.dev/docs"
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Configure Braintrust setup flows
    Setup(CLIArgs<setup::SetupArgs>),
    /// Manage workflow docs for coding agents
    Docs(CLIArgs<setup::DocsArgs>),
    /// Run SQL queries against Braintrust
    Sql(CLIArgs<sql::SqlArgs>),
    /// Manage login profiles and persistent auth
    Login(CLIArgs<login::LoginArgs>),
    /// View logs, traces, and spans
    View(CLIArgs<traces::ViewArgs>),
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
    /// Manage tools
    Tools(CLIArgs<tools::ToolsArgs>),
    /// Manage scorers
    Scorers(CLIArgs<scorers::ScorersArgs>),
    /// Manage experiments
    Experiments(CLIArgs<experiments::ExperimentsArgs>),
    /// Synchronize project logs between Braintrust and local NDJSON files
    Sync(CLIArgs<sync::SyncArgs>),
}

#[tokio::main]
async fn main() -> Result<()> {
    let argv: Vec<OsString> = std::env::args_os().collect();
    env::bootstrap_from_args(&argv)?;
    let cli = Cli::parse_from(argv);

    match cli.command {
        Commands::Setup(cmd) => setup::run_setup_top(cmd.base, cmd.args).await?,
        Commands::Docs(cmd) => setup::run_docs_top(cmd.base, cmd.args).await?,
        Commands::Sql(cmd) => sql::run(cmd.base, cmd.args).await?,
        Commands::Login(cmd) => login::run(cmd.base, cmd.args).await?,
        Commands::View(cmd) => traces::run(cmd.base, cmd.args).await?,
        #[cfg(unix)]
        Commands::Eval(cmd) => eval::run(cmd.base, cmd.args).await?,
        Commands::Projects(cmd) => projects::run(cmd.base, cmd.args).await?,
        Commands::SelfCommand(args) => self_update::run(args).await?,
        Commands::Prompts(cmd) => prompts::run(cmd.base, cmd.args).await?,
        Commands::Tools(cmd) => tools::run(cmd.base, cmd.args).await?,
        Commands::Scorers(cmd) => scorers::run(cmd.base, cmd.args).await?,
        Commands::Experiments(cmd) => experiments::run(cmd.base, cmd.args).await?,
        Commands::Sync(cmd) => sync::run(cmd.base, cmd.args).await?,
    }

    Ok(())
}
