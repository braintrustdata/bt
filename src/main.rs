use anyhow::Result;
use clap::{Parser, Subcommand};

mod args;
mod eval;
mod login;
mod sql;

use crate::args::CLIArgs;

#[derive(Debug, Parser)]
#[command(name = "bt", about = "Braintrust CLI", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    Sql(CLIArgs<sql::SqlArgs>),
    Eval(CLIArgs<eval::EvalArgs>),
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Sql(cmd) => sql::run(cmd.base, cmd.args).await?,
        Commands::Eval(cmd) => eval::run(cmd.base, cmd.args).await?,
    }

    Ok(())
}
