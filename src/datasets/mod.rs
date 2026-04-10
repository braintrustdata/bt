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

pub(crate) mod api;
mod delete;
mod list;
mod view;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt datasets list
  bt datasets view my-dataset
  bt datasets delete my-dataset
")]
pub struct DatasetsArgs {
    #[command(subcommand)]
    command: Option<DatasetsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum DatasetsCommands {
    /// List all datasets
    List,
    /// View a dataset's metadata and sample rows
    View(ViewArgs),
    /// Delete a dataset
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ViewArgs {
    /// Dataset name (positional)
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Dataset name (flag)
    #[arg(long = "name", short = 'n')]
    name_flag: Option<String>,

    /// Open in browser instead of showing in terminal
    #[arg(long)]
    web: bool,

    /// Number of sample rows to display
    #[arg(long, default_value = "10")]
    limit: usize,
}

impl ViewArgs {
    fn name(&self) -> Option<&str> {
        self.name_positional
            .as_deref()
            .or(self.name_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
pub struct DeleteArgs {
    /// Dataset name (positional) of the dataset to delete
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Dataset name (flag) of the dataset to delete
    #[arg(long = "name", short = 'n')]
    name_flag: Option<String>,

    /// Skip confirmation prompt (requires name)
    #[arg(long, short = 'f')]
    force: bool,
}

impl DeleteArgs {
    fn name(&self) -> Option<&str> {
        self.name_positional
            .as_deref()
            .or(self.name_flag.as_deref())
    }
}

pub async fn run(base: BaseArgs, args: DatasetsArgs) -> Result<()> {
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
        None | Some(DatasetsCommands::List) => list::run(&ctx, base.json).await,
        Some(DatasetsCommands::View(v)) => {
            view::run(&ctx, v.name(), base.json, v.web, v.limit).await
        }
        Some(DatasetsCommands::Delete(d)) => delete::run(&ctx, d.name(), d.force).await,
    }
}
