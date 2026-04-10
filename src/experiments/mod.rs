use anyhow::{anyhow, bail, Result};
use clap::{Args, Subcommand};

use crate::{
    args::BaseArgs,
    auth::login,
    config,
    http::ApiClient,
    projects::api::{get_project_by_name, Project},
    ui::{self, is_interactive, select_project, with_spinner},
};

pub(crate) mod api;
mod delete;
mod list;
mod view;

use api::{self as experiments_api, Experiment};

pub(crate) struct ResolvedContext {
    pub client: ApiClient,
    pub app_url: String,
    pub project: Project,
}

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt experiments list
  bt experiments view my-experiment
  bt experiments delete my-experiment
")]
pub struct ExperimentsArgs {
    #[command(subcommand)]
    command: Option<ExperimentsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum ExperimentsCommands {
    /// List all experiments
    List,
    /// View an experiment
    View(ViewArgs),
    /// Delete an experiment
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
struct ViewArgs {
    /// Experiment name (positional)
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Experiment name (flag)
    #[arg(long = "name", short = 'n')]
    name_flag: Option<String>,

    /// Open in browser
    #[arg(long)]
    web: bool,
}

impl ViewArgs {
    fn name(&self) -> Option<&str> {
        self.name_positional
            .as_deref()
            .or(self.name_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
struct DeleteArgs {
    /// Experiment name (positional)
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Experiment name (flag)
    #[arg(long = "name", short = 'n')]
    name_flag: Option<String>,

    /// Skip confirmation
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

pub(crate) async fn select_experiment_interactive(
    client: &ApiClient,
    project: &str,
) -> Result<Experiment> {
    let mut experiments = with_spinner(
        "Loading experiments...",
        experiments_api::list_experiments(client, project),
    )
    .await?;

    if experiments.is_empty() {
        bail!("no experiments found");
    }

    experiments.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = experiments.iter().map(|e| e.name.as_str()).collect();
    let selection = ui::fuzzy_select("Select experiment", &names, 0)?;
    Ok(experiments[selection].clone())
}

pub async fn run(base: BaseArgs, args: ExperimentsArgs) -> Result<()> {
    let auth = login(&base).await?;
    let client = ApiClient::new(&auth)?;
    let config_project = config::load().ok().and_then(|c| c.project);
    let project_name = match base.project.as_deref().or(config_project.as_deref()) {
        Some(p) => p.to_string(),
        None if is_interactive() => select_project(&client, None, None).await?.name,
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
        None | Some(ExperimentsCommands::List) => list::run(&ctx, base.json).await,
        Some(ExperimentsCommands::View(v)) => view::run(&ctx, v.name(), base.json, v.web).await,
        Some(ExperimentsCommands::Delete(d)) => delete::run(&ctx, d.name(), d.force).await,
    }
}
