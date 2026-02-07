use std::io::IsTerminal;

use anyhow::{bail, Result};
use dialoguer::Confirm;

use crate::http::ApiClient;
use crate::ui::{print_command_status, with_spinner, CommandStatus};

use super::api;
use super::switch::select_project_interactive;

pub async fn run(client: &ApiClient, name: Option<&str>) -> Result<()> {
    let project = match name {
        Some(n) => with_spinner("Loading project...", api::get_project_by_name(client, n))
            .await?
            .ok_or_else(|| anyhow::anyhow!("project '{n}' not found"))?,
        None => {
            if !std::io::stdin().is_terminal() {
                bail!("project name required. Use: bt projects delete <name>");
            }
            let name = select_project_interactive(client).await?;
            with_spinner(
                "Loading project...",
                api::get_project_by_name(client, &name),
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("project '{name}' not found"))?
        }
    };

    if std::io::stdin().is_terminal() {
        let confirm = Confirm::new()
            .with_prompt(format!("Delete project '{}'?", project.name))
            .default(false)
            .interact()?;

        if !confirm {
            return Ok(());
        }
    }

    match with_spinner(
        "Deleting project...",
        api::delete_project(client, &project.id),
    )
    .await
    {
        Ok(_) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Deleted '{}'", project.name),
            );
            Ok(())
        }
        Err(e) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Failed to delete '{}'", project.name),
            );
            Err(e)
        }
    }
}
