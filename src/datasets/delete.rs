use anyhow::{anyhow, bail, Result};
use dialoguer::Confirm;

use crate::ui::{is_interactive, is_quiet, print_command_status, with_spinner, CommandStatus};

use super::{api, ResolvedContext};

pub async fn run(ctx: &ResolvedContext, name: Option<&str>, force: bool) -> Result<()> {
    if force && name.is_none() {
        bail!("name required when using --force. Use: bt datasets delete <name> --force");
    }

    let dataset = match name {
        Some(name) => with_spinner(
            "Loading dataset...",
            api::get_dataset_by_name(&ctx.client, &ctx.project.id, name),
        )
        .await?
        .ok_or_else(|| anyhow!("dataset '{name}' not found"))?,
        None => {
            if !is_interactive() {
                bail!("dataset name required. Use: bt datasets delete <name>");
            }
            let Some(dataset) =
                super::select_dataset_interactive(&ctx.client, &ctx.project.id).await?
            else {
                super::print_no_datasets_found(&ctx.project.name);
                return Ok(());
            };
            dataset
        }
    };

    if !force {
        if !is_interactive() {
            bail!(
                "dataset delete requires --force in non-interactive mode. Use: bt datasets delete <name> --force"
            );
        }

        let confirm = Confirm::new()
            .with_prompt(format!(
                "Delete dataset '{}' from {}?",
                dataset.name, ctx.project.name
            ))
            .default(false)
            .interact()?;
        if !confirm {
            return Ok(());
        }
    }

    match with_spinner(
        "Deleting dataset...",
        api::delete_dataset(&ctx.client, &dataset.id),
    )
    .await
    {
        Ok(_) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Deleted '{}'", dataset.name),
            );
            if !is_quiet() {
                eprintln!("Run `bt datasets list` to see remaining datasets.");
            }
            Ok(())
        }
        Err(error) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Failed to delete '{}'", dataset.name),
            );
            Err(error)
        }
    }
}
