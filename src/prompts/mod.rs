use std::path::PathBuf;

use anyhow::{anyhow, Result};
use clap::{Args, Subcommand};

use crate::{
    args::BaseArgs,
    auth::login,
    http::ApiClient,
    projects::api::{get_project_by_name, Project},
    ui::{is_interactive, select_project_interactive},
};

pub(crate) struct ResolvedContext {
    pub client: ApiClient,
    pub app_url: String,
    pub project: Project,
}

mod api;
mod delete;
mod list;
mod push;
mod view;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt prompts list
  bt prompts view my-prompt
  bt prompts push my-prompt --file prompt.json
  bt prompts push my-prompt --file prompt.json --environments staging,prod
  bt prompts delete my-prompt
")]
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
    /// Create or update a prompt
    Push(PushArgs),
    /// Delete a prompt
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
pub struct PushArgs {
    /// Prompt slug — used as the unique identifier for upsert (positional)
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,

    /// Prompt slug (flag)
    #[arg(long = "slug", short = 's')]
    slug_flag: Option<String>,

    /// Display name for the prompt (defaults to slug if omitted)
    #[arg(long, short = 'n')]
    name: Option<String>,

    /// Path to a JSON file containing prompt_data
    #[arg(long, short = 'f', value_name = "FILE")]
    file: Option<PathBuf>,

    /// Environments to associate with the prompt (comma-separated slugs)
    #[arg(
        long,
        env = "BT_PROMPTS_PUSH_ENVIRONMENTS",
        value_delimiter = ',',
        value_name = "ENV"
    )]
    environments: Vec<String>,

    /// Skip confirmation prompt
    #[arg(long, short = 'y')]
    yes: bool,
}

impl PushArgs {
    fn slug(&self) -> Option<&str> {
        self.slug_positional
            .as_deref()
            .or(self.slug_flag.as_deref())
    }
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
    let auth = login(&base).await?;
    let client = ApiClient::new(&auth)?;
    let project_name = match base
        .project
        .or_else(|| crate::config::load().ok().and_then(|c| c.project))
    {
        Some(p) => p,
        None if is_interactive() => select_project_interactive(&client, None, None).await?,
        None => anyhow::bail!("--project required (or set BRAINTRUST_DEFAULT_PROJECT)"),
    };

    let project = get_project_by_name(&client, &project_name)
        .await?
        .ok_or_else(|| anyhow!("project '{project_name}' not found"))?;

    let ctx = ResolvedContext {
        client,
        app_url: auth.app_url,
        project,
    };

    match args.command {
        None | Some(PromptsCommands::List) => list::run(&ctx, base.json).await,
        Some(PromptsCommands::View(p)) => {
            view::run(&ctx, p.slug(), base.json, p.web, p.verbose).await
        }
        Some(PromptsCommands::Push(p)) => {
            let slug = match p.slug() {
                Some(s) => s.to_owned(),
                None => anyhow::bail!("slug required. Use: bt prompts push <slug>"),
            };
            push::run(
                &ctx,
                &slug,
                p.name.as_deref(),
                p.file.as_deref(),
                p.environments,
                p.yes,
                base.json,
            )
            .await
        }
        Some(PromptsCommands::Delete(p)) => delete::run(&ctx, p.slug(), p.force).await,
    }
}
