use std::io::IsTerminal;

use anyhow::{anyhow, Result};
use clap::{Args, Subcommand};

use crate::{
    args::BaseArgs, http::ApiClient, login::login, projects::api::get_project_by_name,
    ui::select_project_interactive,
};

mod api;
mod delete;
mod list;
mod view;

#[derive(Debug, Clone, Args)]
pub struct PromptsArgs {
    #[command(subcommand)]
    command: Option<PromptsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum PromptsCommands {
    /// List all prompts
    List,
    /// View a prompt's content
    View(ViewArgs),
    /// Delete a prompt
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ViewArgs {
    /// Prompt slug (positional)
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,

    /// Prompt slug (flag)
    #[arg(long = "slug", short = 's')]
    slug_flag: Option<String>,

    /// Open in browser instead of showing in terminal
    #[arg(long)]
    web: bool,

    /// Show all model parameters and configuration
    #[arg(long)]
    verbose: bool,
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
    /// Prompt slug (positional) of the prompt to delete
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,

    /// Prompt slug (flag) of the prompt to delete
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

pub async fn run(base: BaseArgs, args: PromptsArgs) -> Result<()> {
    let ctx = login(&base).await?;
    let client = ApiClient::new(&ctx)?;
    let project = match base.project {
        Some(p) => p,
        None if std::io::stdin().is_terminal() => select_project_interactive(&client, None).await?,
        None => anyhow::bail!("--project required (or set BRAINTRUST_DEFAULT_PROJECT)"),
    };

    get_project_by_name(&client, &project)
        .await?
        .ok_or_else(|| anyhow!("project '{project}' not found"))?;

    match args.command {
        None | Some(PromptsCommands::List) => {
            list::run(&client, &project, &ctx.login.org_name, base.json).await
        }
        Some(PromptsCommands::View(p)) => {
            view::run(
                &client,
                &ctx.app_url,
                &project,
                &ctx.login.org_name,
                p.slug(),
                base.json,
                p.web,
                p.verbose,
            )
            .await
        }
        Some(PromptsCommands::Delete(p)) => delete::run(&client, &project, p.slug(), p.force).await,
    }
}
