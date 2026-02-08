use std::io::IsTerminal;

use anyhow::{bail, Result};
use dialoguer::Confirm;

use crate::{
    http::ApiClient,
    prompts::api::{self, Prompt},
    ui::{self, print_command_status, with_spinner, CommandStatus},
};

pub async fn run(client: &ApiClient, project: &str, name: Option<&str>) -> Result<()> {
    let prompt = match name {
        Some(n) => api::get_prompt_by_name(client, project, n).await?,
        None => {
            if !std::io::stdin().is_terminal() {
                bail!("prompt name required. Use: bt prompts delete <name>");
            }
            select_prompt_interactive(client, project).await?
        }
    };

    if std::io::stdin().is_terminal() {
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
    let mut prompts =
        with_spinner("Loading prompts...", api::list_prompts(client, project)).await?;
    if prompts.is_empty() {
        bail!("no prompts found");
    }

    prompts.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = prompts.iter().map(|p| p.name.as_str()).collect();

    let selection = ui::fuzzy_select("Select prompt", &names)?;
    Ok(prompts[selection].clone())
}
