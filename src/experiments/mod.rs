use anyhow::{anyhow, bail, Result};
use clap::{Args, Subcommand};

use crate::{
    args::BaseArgs,
    auth::login,
    config,
    http::ApiClient,
    projects::api::get_project_by_name,
    ui::{self, is_interactive, select_project_interactive, with_spinner},
};

mod api;
mod delete;
mod list;
mod view;

use api::{self as experiments_api, Experiment};

#[derive(Debug, Clone, Args)]
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
    let ctx = login(&base).await?;
    let client = ApiClient::new(&ctx)?;
    let config_project = config::load().ok().and_then(|c| c.project);
    let project_name = match base.project.as_deref().or(config_project.as_deref()) {
        Some(p) => p.to_string(),
        None if is_interactive() => select_project_interactive(&client, None, None).await?,
        None => anyhow::bail!("--project required (or set BRAINTRUST_DEFAULT_PROJECT)"),
    };

    let project = get_project_by_name(&client, &project_name)
        .await?
        .ok_or_else(|| anyhow!("project '{project_name}' not found"))?;

    match args.command {
        None | Some(ExperimentsCommands::List) => {
            list::run(&client, &project, client.org_name(), base.json).await
        }
        Some(ExperimentsCommands::View(v)) => {
            view::run(
                &client,
                &ctx.app_url,
                &project,
                client.org_name(),
                v.name(),
                base.json,
                v.web,
            )
            .await
        }
        Some(ExperimentsCommands::Delete(d)) => {
            delete::run(&client, &project, d.name(), d.force).await
        }
    }
}
