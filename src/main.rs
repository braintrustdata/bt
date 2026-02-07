use anyhow::Result;
use clap::{Parser, Subcommand};

mod args;
#[cfg(unix)]
mod eval;
mod http;
mod login;
mod projects;
mod sql;
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
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Sql(cmd) => sql::run(cmd.base, cmd.args).await?,
        #[cfg(unix)]
        Commands::Eval(cmd) => eval::run(cmd.base, cmd.args).await?,
        Commands::Projects(cmd) => projects::run(cmd.base, cmd.args).await?,
    }

    Ok(())
}
