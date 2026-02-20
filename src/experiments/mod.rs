use std::io::IsTerminal;

use anyhow::{anyhow, Result};
use clap::{Args, Subcommand};

use crate::{
    args::BaseArgs,
    http::ApiClient,
    login::login,
    projects::{api::get_project_by_name, switch::select_project_interactive},
};

mod api;
mod delete;
mod list;
mod view;

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

pub async fn run(base: BaseArgs, args: ExperimentsArgs) -> Result<()> {
    let ctx = login(&base).await?;
    let org_name = base.org.unwrap_or_else(|| ctx.login.org_name.clone());
    let client = ApiClient::new(&ctx)?.with_org_name(org_name.clone());
    let project = match base.project {
        Some(p) => p,
        None if std::io::stdin().is_terminal() => select_project_interactive(&client).await?,
        None => anyhow::bail!("--project required (or set BRAINTRUST_DEFAULT_PROJECT)"),
    };

    let resolved_project = get_project_by_name(&client, &project)
        .await?
        .ok_or_else(|| anyhow!("project '{project}' not found"))?;

    match args.command {
        None | Some(ExperimentsCommands::List) => {
            list::run(&client, &resolved_project, &org_name, base.json).await
        }
        Some(ExperimentsCommands::View(v)) => {
            view::run(
                &client,
                &ctx.app_url,
                &resolved_project,
                &org_name,
                v.name(),
                base.json,
                v.web,
            )
            .await
        }
        Some(ExperimentsCommands::Delete(d)) => {
            delete::run(&client, &resolved_project, d.name(), d.force).await
        }
    }
}
