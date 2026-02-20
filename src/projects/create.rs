use std::time::Duration;

use anyhow::{bail, Result};
use dialoguer::Input;

use crate::http::ApiClient;
use crate::ui::{
    is_interactive, print_command_status, with_spinner, with_spinner_visible, CommandStatus,
};

use super::api;

pub async fn run(client: &ApiClient, name: Option<&str>) -> Result<()> {
    let name = match name {
        Some(n) if !n.is_empty() => n.to_string(),
        _ => {
            if !is_interactive() {
                bail!("project name required. Use: bt projects create <name>");
            }
            Input::new().with_prompt("Project name").interact_text()?
        }
    };

    // Check if project already exists
    let exists = with_spinner(
        "Checking project...",
        api::get_project_by_name(client, &name),
    )
    .await?;
    if exists.is_some() {
        bail!("project '{name}' already exists");
    }

    match with_spinner_visible(
        "Creating project...",
        api::create_project(client, &name),
        Duration::from_millis(300),
    )
    .await
    {
        Ok(_) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Successfully created '{name}'"),
            );
            Ok(())
        }
        Err(e) => {
            print_command_status(CommandStatus::Error, &format!("Failed to create '{name}'"));
            Err(e)
        }
    }
}
