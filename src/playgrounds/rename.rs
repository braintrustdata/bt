use anyhow::Result;
use dialoguer::Confirm;

use crate::ui::{is_interactive, is_quiet, print_command_status, with_spinner, CommandStatus};

use super::delete::resolve_playground;
use super::{api, ResolvedContext};

pub async fn run(
    ctx: &ResolvedContext,
    name: Option<&str>,
    new_name: Option<&str>,
    description: Option<&str>,
    force: bool,
) -> Result<()> {
    let playground = resolve_playground(ctx, name, "rename").await?;

    if !force && is_interactive() {
        let mut summary = String::new();
        if let Some(n) = new_name {
            summary.push_str(&format!("name -> {n} "));
        }
        if let Some(d) = description {
            summary.push_str(&format!("description -> {d} "));
        }
        let confirm = Confirm::new()
            .with_prompt(format!(
                "Update playground '{}' ({})?",
                playground.name,
                summary.trim()
            ))
            .default(false)
            .interact()?;

        if !confirm {
            return Ok(());
        }
    }

    match with_spinner(
        "Updating playground...",
        api::rename_playground(&ctx.client, &playground.id, new_name, description),
    )
    .await
    {
        Ok(_) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Updated '{}'", playground.name),
            );
            if !is_quiet() {
                eprintln!(
                    "Run `bt playgrounds view {}` to see the result.",
                    new_name.unwrap_or(&playground.name)
                );
            }
            Ok(())
        }
        Err(e) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Failed to update '{}'", playground.name),
            );
            Err(e)
        }
    }
}
