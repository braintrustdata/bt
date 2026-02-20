use std::io::IsTerminal;

use anyhow::{anyhow, bail, Result};
use dialoguer::Confirm;

use crate::{
    http::ApiClient,
    projects::api::Project,
    ui::{self, print_command_status, with_spinner, CommandStatus},
};

use super::api::{self, Experiment};

pub async fn run(
    client: &ApiClient,
    project: &Project,
    name: Option<&str>,
    force: bool,
) -> Result<()> {
    let project_name = &project.name;
    if force && name.is_none() {
        bail!("name required when using --force. Use: bt experiments delete <name> --force");
    }

    let experiment = match name {
        Some(n) => api::get_experiment_by_name(client, project_name, n)
            .await?
            .ok_or_else(|| anyhow!("experiment '{n}' not found"))?,
        None => {
            if !std::io::stdin().is_terminal() {
                bail!("experiment name required. Use: bt experiments delete <name>");
            }
            select_experiment_interactive(client, project_name).await?
        }
    };

    if !force && std::io::stdin().is_terminal() {
        let confirm = Confirm::new()
            .with_prompt(format!(
                "Delete experiment '{}' from {}?",
                &experiment.name, &project_name
            ))
            .default(false)
            .interact()?;
        if !confirm {
            return Ok(());
        }
    }

    match with_spinner(
        "Deleting experiment...",
        api::delete_experiment(client, &experiment.id),
    )
    .await
    {
        Ok(_) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Deleted '{}'", experiment.name),
            );
            eprintln!("Run `bt experiments list` to see remaining experiments.");
            Ok(())
        }
        Err(e) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Failed to delete '{}'", experiment.name),
            );
            Err(e)
        }
    }
}

pub async fn select_experiment_interactive(
    client: &ApiClient,
    project: &str,
) -> Result<Experiment> {
    let mut experiments = with_spinner(
        "Loading experiments...",
        api::list_experiments(client, project),
    )
    .await?;

    if experiments.is_empty() {
        bail!("no experiments found");
    }

    experiments.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = experiments.iter().map(|e| e.name.as_str()).collect();
    let selection = ui::fuzzy_select("Select experiment", &names)?;
    Ok(experiments[selection].clone())
}
