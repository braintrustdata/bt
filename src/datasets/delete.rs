use anyhow::{anyhow, bail, Result};
use dialoguer::Confirm;

use crate::{
    datasets::api::{self, Dataset},
    http::ApiClient,
    ui::{self, is_interactive, print_command_status, with_spinner, CommandStatus},
};

use super::ResolvedContext;

pub async fn run(ctx: &ResolvedContext, name: Option<&str>, force: bool) -> Result<()> {
    let project_name = &ctx.project.name;
    if force && name.is_none() {
        bail!("name required when using --force. Use: bt datasets delete <name> --force");
    }

    let dataset = match name {
        Some(n) => api::get_dataset_by_name(&ctx.client, project_name, n)
            .await?
            .ok_or_else(|| anyhow!("dataset '{n}' not found"))?,
        None => {
            if !is_interactive() {
                bail!("dataset name required. Use: bt datasets delete <name>");
            }
            select_dataset_interactive(&ctx.client, project_name).await?
        }
    };

    if !force && is_interactive() {
        let confirm = Confirm::new()
            .with_prompt(format!(
                "Delete dataset '{}' from {}?",
                &dataset.name, project_name
            ))
            .default(false)
            .interact()?;

        if !confirm {
            return Ok(());
        }
    }

    match with_spinner(
        "Deleting dataset...",
        api::delete_dataset(&ctx.client, &dataset.id),
    )
    .await
    {
        Ok(_) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Deleted '{}'", dataset.name),
            );
            if !crate::ui::is_quiet() {
                eprintln!("Run `bt datasets list` to see remaining datasets.");
            }
            Ok(())
        }
        Err(e) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Failed to delete '{}'", dataset.name),
            );
            Err(e)
        }
    }
}

pub async fn select_dataset_interactive(client: &ApiClient, project: &str) -> Result<Dataset> {
    let mut datasets =
        with_spinner("Loading datasets...", api::list_datasets(client, project)).await?;
    if datasets.is_empty() {
        bail!("no datasets found");
    }

    datasets.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = datasets.iter().map(|d| d.name.as_str()).collect();

    let selection = ui::fuzzy_select("Select dataset", &names, 0)?;
    Ok(datasets[selection].clone())
}
