use anyhow::{bail, Result};
use dialoguer::Confirm;

use crate::http::ApiClient;
use crate::ui::{
    is_interactive, print_command_status, select_project_interactive, with_spinner, CommandStatus,
};

use super::api;

pub async fn run(client: &ApiClient, name: Option<&str>, force: bool) -> Result<()> {
    if force && name.is_none() {
        bail!("project name required when using --force. Use: bt projects delete <name> --force");
    }

    let project = match name {
        Some(n) => with_spinner("Loading project...", api::get_project_by_name(client, n))
            .await?
            .ok_or_else(|| anyhow::anyhow!("project '{n}' not found"))?,
        None => {
            if !is_interactive() {
                bail!("project name required. Use: bt projects delete <name>");
            }
            let name = select_project_interactive(client, None, None).await?;
            with_spinner(
                "Loading project...",
                api::get_project_by_name(client, &name),
            )
            .await?
            .ok_or_else(|| anyhow::anyhow!("project '{name}' not found"))?
        }
    };

    if !force && is_interactive() {
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
            eprintln!("Run `bt projects list` to see remaining projects.");
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
