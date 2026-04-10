use anyhow::{bail, Result};
use clap::{Args, Subcommand};

use crate::{
    args::BaseArgs,
    http::ApiClient,
    projects::context::{resolve_project_context, ProjectContext},
    ui::{self, with_spinner},
};

pub(crate) mod api;
mod delete;
mod list;
mod view;

use api::{self as experiments_api, Experiment};

pub(crate) type ResolvedContext = ProjectContext;

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
    match args.command {
        None | Some(ExperimentsCommands::List) => {
            let ctx = resolve_project_context(&base, true).await?;
            list::run(&ctx, base.json).await
        }
        Some(ExperimentsCommands::View(v)) => {
            let ctx = resolve_project_context(&base, true).await?;
            view::run(&ctx, v.name(), base.json, v.web).await
        }
        Some(ExperimentsCommands::Delete(d)) => {
            let ctx = resolve_project_context(&base, false).await?;
            delete::run(&ctx, d.name(), d.force).await
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn experiments_command_is_read_only(command: Option<&ExperimentsCommands>) -> bool {
        matches!(
            command,
            None | Some(ExperimentsCommands::List) | Some(ExperimentsCommands::View(_))
        )
    }

    #[test]
    fn experiments_routes_list_and_view_to_read_only_auth() {
        assert!(experiments_command_is_read_only(None));
        assert!(experiments_command_is_read_only(Some(
            &ExperimentsCommands::List
        )));
        assert!(experiments_command_is_read_only(Some(
            &ExperimentsCommands::View(ViewArgs {
                name_positional: Some("my-experiment".to_string()),
                name_flag: None,
                web: false,
            })
        )));
    }

    #[test]
    fn experiments_routes_delete_to_validated_auth() {
        assert!(!experiments_command_is_read_only(Some(
            &ExperimentsCommands::Delete(DeleteArgs {
                name_positional: Some("my-experiment".to_string()),
                name_flag: None,
                force: true,
            })
        )));
    }
}
