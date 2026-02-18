use std::io::IsTerminal;

use anyhow::{anyhow, bail, Result};
use dialoguer::Confirm;

use crate::{
    functions::api::{self, Function},
    http::ApiClient,
    resource_cmd::select_named_resource_interactive,
    ui::{print_command_status, with_spinner, CommandStatus},
};

pub async fn run(client: &ApiClient, project: &str, slug: Option<&str>, force: bool) -> Result<()> {
    if force && slug.is_none() {
        bail!("slug required when using --force. Use: bt functions delete <slug> --force");
    }

    let function = match slug {
        Some(s) => api::get_function_by_slug(client, project, s)
            .await?
            .ok_or_else(|| anyhow!("function with slug '{s}' not found"))?,
        None => {
            if !std::io::stdin().is_terminal() {
                bail!("function slug required. Use: bt functions delete <slug>");
            }
            select_function_interactive(client, project).await?
        }
    };

    if !force && std::io::stdin().is_terminal() {
        let confirm = Confirm::new()
            .with_prompt(format!(
                "Delete function '{}' from {}?",
                &function.name, project
            ))
            .default(false)
            .interact()?;

        if !confirm {
            return Ok(());
        }
    }

    match with_spinner(
        "Deleting function...",
        api::delete_function(client, &function.id),
    )
    .await
    {
        Ok(_) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Deleted '{}'", function.name),
            );
            eprintln!("Run `bt functions list` to see remaining functions.");
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

pub async fn select_function_interactive(client: &ApiClient, project: &str) -> Result<Function> {
    let functions =
        with_spinner("Loading functions...", api::list_functions(client, project)).await?;
    select_named_resource_interactive(functions, "no functions found", "Select function")
}
