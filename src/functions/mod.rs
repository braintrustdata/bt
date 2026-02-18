use anyhow::Result;
use clap::{Args, Subcommand};

use crate::{args::BaseArgs, project_command::resolve_project_command_context};

mod api;
mod delete;
mod list;
mod view;

#[derive(Debug, Clone, Args)]
pub struct FunctionsArgs {
    #[command(subcommand)]
    command: Option<FunctionsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum FunctionsCommands {
    /// List all functions
    List,
    /// View a function's details
    View(ViewArgs),
    /// Delete a function
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ViewArgs {
    /// Function slug (positional)
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,

    /// Function slug (flag)
    #[arg(long = "slug", short = 's')]
    slug_flag: Option<String>,
}

impl ViewArgs {
    fn slug(&self) -> Option<&str> {
        self.slug_positional
            .as_deref()
            .or(self.slug_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
pub struct DeleteArgs {
    /// Function slug (positional) of the function to delete
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,

    /// Function slug (flag) of the function to delete
    #[arg(long = "slug", short = 's')]
    slug_flag: Option<String>,

    /// Skip confirmation prompt (requires slug)
    #[arg(long, short = 'f')]
    force: bool,
}

impl DeleteArgs {
    fn slug(&self) -> Option<&str> {
        self.slug_positional
            .as_deref()
            .or(self.slug_flag.as_deref())
    }
}

pub async fn run(base: BaseArgs, args: FunctionsArgs) -> Result<()> {
    let ctx = resolve_project_command_context(&base).await?;
    let client = &ctx.client;
    let project = &ctx.project;

    match args.command {
        None | Some(FunctionsCommands::List) => {
            list::run(client, project, &ctx.login.login.org_name, base.json).await
        }
        Some(FunctionsCommands::View(f)) => view::run(client, project, f.slug(), base.json).await,
        Some(FunctionsCommands::Delete(f)) => delete::run(client, project, f.slug(), f.force).await,
    }
}
