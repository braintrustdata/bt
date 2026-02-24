use anyhow::{anyhow, bail, Result};
use dialoguer::Confirm;

use crate::ui::{is_interactive, print_command_status, with_spinner, CommandStatus};

use super::{api, ResolvedContext};

pub async fn run(ctx: &ResolvedContext, name: Option<&str>, force: bool) -> Result<()> {
    let project_name = &ctx.project.name;
    if force && name.is_none() {
        bail!("name required when using --force. Use: bt experiments delete <name> --force");
    }

    let experiment = match name {
        Some(n) => api::get_experiment_by_name(&ctx.client, project_name, n)
            .await?
            .ok_or_else(|| anyhow!("experiment '{n}' not found"))?,
        None => {
            if !is_interactive() {
                bail!("experiment name required. Use: bt experiments delete <name>");
            }
            super::select_experiment_interactive(&ctx.client, project_name).await?
        }
    };

    if !force && is_interactive() {
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
        api::delete_experiment(&ctx.client, &experiment.id),
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
