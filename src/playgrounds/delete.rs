use anyhow::{anyhow, bail, Result};
use dialoguer::Confirm;

use crate::{
    http::ApiClient,
    playgrounds::api::{self, PlaygroundSummary},
    ui::{self, is_interactive, is_quiet, print_command_status, with_spinner, CommandStatus},
};

use super::ResolvedContext;

pub async fn run(ctx: &ResolvedContext, name: Option<&str>, force: bool) -> Result<()> {
    let project_name = &ctx.project.name;
    if force && name.is_none() {
        bail!("name required when using --force. Use: bt playgrounds delete <name> --force");
    }

    let playground = resolve_playground(ctx, name, "delete").await?;
    // Show a short id alongside the name: playgrounds are resolved by name
    // from a list, so two playgrounds sharing a name would otherwise both
    // match `find` silently. The id disambiguates which one we're deleting.
    let short_id = ui::truncate(&playground.id, 8);

    if !force && is_interactive() {
        let confirm = Confirm::new()
            .with_prompt(format!(
                "Delete playground '{}' ({}) from {}?",
                &playground.name, short_id, project_name
            ))
            .default(false)
            .interact()?;

        if !confirm {
            return Ok(());
        }
    } else if force {
        // No confirmation prompt under --force, so log the resolved target
        // (with its id) before the delete fires.
        print_command_status(
            CommandStatus::Warning,
            &format!("Deleting playground '{}' ({})", &playground.name, short_id),
        );
    }

    match with_spinner(
        "Deleting playground...",
        api::delete_playground(&ctx.client, &playground.id),
    )
    .await
    {
        Ok(_) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Deleted '{}' ({})", playground.name, short_id),
            );
            if !is_quiet() {
                eprintln!("Run `bt playgrounds list` to see remaining playgrounds.");
            }
            Ok(())
        }
        Err(e) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Failed to delete '{}' ({})", playground.name, short_id),
            );
            Err(e)
        }
    }
}

/// Resolve a playground within the current project, by name when given or via
/// an interactive picker otherwise. `command` is the verb used in the
/// non-interactive "name required" hint (e.g. "delete", "rename", "view").
pub async fn resolve_playground(
    ctx: &ResolvedContext,
    name: Option<&str>,
    command: &str,
) -> Result<PlaygroundSummary> {
    let project_name = &ctx.project.name;
    match name {
        Some(n) => Ok(with_spinner(
            "Loading playground...",
            api::list_playground_summaries(&ctx.client, project_name),
        )
        .await?
        .into_iter()
        .find(|p| p.name == n)
        .ok_or_else(|| anyhow!("playground with name '{n}' not found"))?),
        None => {
            if !is_interactive() {
                bail!("playground name required. Use: bt playgrounds {command} <name>");
            }
            select_playground_interactive(&ctx.client, project_name).await
        }
    }
}

async fn select_playground_interactive(
    client: &ApiClient,
    project: &str,
) -> Result<PlaygroundSummary> {
    let mut playgrounds = with_spinner(
        "Loading playgrounds...",
        api::list_playground_summaries(client, project),
    )
    .await?;
    if playgrounds.is_empty() {
        bail!("no playgrounds found");
    }

    playgrounds.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = playgrounds.iter().map(|p| p.name.as_str()).collect();

    let selection = ui::fuzzy_select("Select playground", &names, 0)?;
    Ok(playgrounds[selection].clone())
}
