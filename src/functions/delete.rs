use anyhow::{anyhow, bail, Result};
use dialoguer::Confirm;

use crate::ui::{is_interactive, is_quiet, print_command_status, with_spinner, CommandStatus};

use super::{api, label, label_plural, select_function_interactive};
use super::{FunctionTypeFilter, ResolvedContext};

pub async fn run(
    ctx: &ResolvedContext,
    slug: Option<&str>,
    force: bool,
    ft: Option<FunctionTypeFilter>,
) -> Result<()> {
    if force && slug.is_none() {
        bail!(
            "slug required when using --force. Use: bt {} delete <slug> --force",
            label_plural(ft),
        );
    }

    let project_id = &ctx.project.id;

    let function = match slug {
        Some(s) => api::get_function_by_slug(&ctx.client, project_id, s)
            .await?
            .ok_or_else(|| anyhow!("{} with slug '{s}' not found", label(ft)))?,
        None => {
            if !is_interactive() {
                bail!(
                    "{} slug required. Use: bt {} delete <slug>",
                    label(ft),
                    label_plural(ft),
                );
            }
            select_function_interactive(&ctx.client, project_id, ft).await?
        }
    };

    if !force && is_interactive() {
        let confirm = Confirm::new()
            .with_prompt(format!(
                "Delete {} '{}' from {}?",
                label(ft),
                &function.name,
                &ctx.project.name
            ))
            .default(false)
            .interact()?;
        if !confirm {
            return Ok(());
        }
    }

    match with_spinner(
        &format!("Deleting {}...", label(ft)),
        api::delete_function(&ctx.client, &function.id),
    )
    .await
    {
        Ok(_) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Deleted '{}'", function.name),
            );
            if !is_quiet() {
                eprintln!(
                    "Run `bt {} list` to see remaining {}.",
                    label_plural(ft),
                    label_plural(ft)
                );
            }
            Ok(())
        }
        Err(e) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Failed to delete '{}'", function.name),
            );
            Err(e)
        }
    }
}
