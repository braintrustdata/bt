use std::io::IsTerminal;

use anyhow::{anyhow, bail, Result};
use dialoguer::Confirm;

use crate::{
    http::ApiClient,
    ui::{self, print_command_status, with_spinner, CommandStatus},
};

use super::{
    api::{self, Function},
    FunctionKind,
};

pub async fn run(
    client: &ApiClient,
    project: &str,
    slug: Option<&str>,
    force: bool,
    kind: &FunctionKind,
) -> Result<()> {
    if force && slug.is_none() {
        bail!(
            "slug required when using --force. Use: bt {} delete <slug> --force",
            kind.plural
        );
    }

    let function = match slug {
        Some(s) => api::get_function_by_slug(client, project, s, Some(kind.function_type))
            .await?
            .ok_or_else(|| anyhow!("{} with slug '{s}' not found", kind.type_name))?,
        None => {
            if !std::io::stdin().is_terminal() {
                bail!(
                    "{} slug required. Use: bt {} delete <slug>",
                    kind.type_name,
                    kind.plural
                );
            }
            select_function_interactive(client, project, kind).await?
        }
    };

    if !force && std::io::stdin().is_terminal() {
        let confirm = Confirm::new()
            .with_prompt(format!(
                "Delete {} '{}' from {}?",
                kind.type_name, &function.name, project
            ))
            .default(false)
            .interact()?;
        if !confirm {
            return Ok(());
        }
    }

    match with_spinner(
        &format!("Deleting {}...", kind.type_name),
        api::delete_function(client, &function.id),
    )
    .await
    {
        Ok(_) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Deleted '{}'", function.name),
            );
            eprintln!(
                "Run `bt {} list` to see remaining {}.",
                kind.plural, kind.plural
            );
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

pub async fn select_function_interactive(
    client: &ApiClient,
    project: &str,
    kind: &FunctionKind,
) -> Result<Function> {
    let mut functions = with_spinner(
        &format!("Loading {}...", kind.plural),
        api::list_functions(client, project, Some(kind.function_type)),
    )
    .await?;

    if functions.is_empty() {
        bail!("no {} found", kind.plural);
    }

    functions.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = functions.iter().map(|f| f.name.as_str()).collect();
    let selection = ui::fuzzy_select(&format!("Select {}", kind.type_name), &names)?;
    Ok(functions.swap_remove(selection))
}
