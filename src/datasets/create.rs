use std::time::Duration;

use anyhow::bail;
use anyhow::Result;
use dialoguer::Input;

use crate::ui::{
    is_interactive, print_command_status, with_spinner, with_spinner_visible, CommandStatus,
};

use super::{api, ResolvedContext};

pub async fn run(
    ctx: &ResolvedContext,
    name: Option<&str>,
    description: Option<&str>,
) -> Result<()> {
    let project_name = &ctx.project.name;
    let name = match name {
        Some(n) if !n.is_empty() => n.to_string(),
        _ => {
            if !is_interactive() {
                bail!("dataset name required. Use: bt datasets create <name>");
            }
            Input::new().with_prompt("Dataset name").interact_text()?
        }
    };

    // Check if dataset already exists
    let exists = with_spinner(
        "Checking dataset...",
        api::get_dataset_by_name(&ctx.client, project_name, &name),
    )
    .await?;
    if exists.is_some() {
        bail!("dataset '{name}' already exists in project '{project_name}'");
    }

    match with_spinner_visible(
        "Creating dataset...",
        api::create_dataset(&ctx.client, &ctx.project.id, &name, description),
        Duration::from_millis(300),
    )
    .await
    {
        Ok(_) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Created dataset '{name}' in {project_name}"),
            );
            Ok(())
        }
        Err(e) => {
            print_command_status(CommandStatus::Error, &format!("Failed to create '{name}'"));
            Err(e)
        }
    }
}
