use std::io::IsTerminal;

use anyhow::{anyhow, bail, Result};
use dialoguer::Confirm;

use crate::{
    http::ApiClient,
    prompts::api::{self, Prompt},
    resource_cmd::select_named_resource_interactive,
    ui::{print_command_status, with_spinner, CommandStatus},
};

pub async fn run(client: &ApiClient, project: &str, slug: Option<&str>, force: bool) -> Result<()> {
    if force && slug.is_none() {
        bail!("slug required when using --force. Use: bt prompts delete <slug> --force");
    }

    let prompt = match slug {
        Some(s) => api::get_prompt_by_slug(client, project, s)
            .await?
            .ok_or_else(|| anyhow!("prompt with slug '{s}' not found"))?,
        None => {
            if !std::io::stdin().is_terminal() {
                bail!("prompt slug required. Use: bt prompts delete <slug>");
            }
            select_prompt_interactive(client, project).await?
        }
    };

    if !force && std::io::stdin().is_terminal() {
        let confirm = Confirm::new()
            .with_prompt(format!(
                "Delete prompt '{}' from {}?",
                &prompt.name, project
            ))
            .default(false)
            .interact()?;

        if !confirm {
            return Ok(());
        }
    }

    match with_spinner("Deleting prompt...", api::delete_prompt(client, &prompt.id)).await {
        Ok(_) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Deleted '{}'", prompt.name),
            );
            eprintln!("Run `bt prompts list` to see remaining prompts.");
            Ok(())
        }
        Err(e) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Failed to delete '{}'", prompt.name),
            );
            Err(e)
        }
    }
}

pub async fn select_prompt_interactive(client: &ApiClient, project: &str) -> Result<Prompt> {
    let prompts = with_spinner("Loading prompts...", api::list_prompts(client, project)).await?;
    select_named_resource_interactive(prompts, "no prompts found", "Select prompt")
}
