use std::io::IsTerminal;

use anyhow::{bail, Result};
use dialoguer::Input;
use reqwest::Client;

use crate::login::LoginContext;
use crate::ui::{print_command_status, with_spinner, CommandStatus};

use super::api;

pub async fn run(http: &Client, ctx: &LoginContext, name: Option<&str>) -> Result<()> {
    let name = match name {
        Some(n) if !n.is_empty() => n.to_string(),
        _ => {
            if !std::io::stdin().is_terminal() {
                bail!("project name required. Use: bt projects create <name>");
            }
            Input::new().with_prompt("Project name").interact_text()?
        }
    };

    // Check if project already exists
    let exists = with_spinner(
        "Checking project...",
        api::get_project_by_name(http, ctx, &name),
    )
    .await?;
    if exists.is_some() {
        bail!("project '{}' already exists", name);
    }

    match with_spinner("Creating project...", api::create_project(http, ctx, &name)).await {
        Ok(_) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Successfully created '{}'", name),
            );
            Ok(())
        }
        Err(e) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Failed to create '{}'", name),
            );
            Err(e)
        }
    }
}
