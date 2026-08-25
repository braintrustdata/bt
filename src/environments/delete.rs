use anyhow::{bail, Result};
use dialoguer::Confirm;

use crate::{
    http::ApiClient,
    ui::{is_interactive, is_quiet, print_command_status, with_spinner, CommandStatus},
};

use super::{api, resolve_environment};

pub async fn run(client: &ApiClient, slug: Option<&str>, force: bool, json: bool) -> Result<()> {
    if force && slug.is_none() {
        bail!("environment slug required when using --force. Use: bt environments delete <slug> --force");
    }

    let environment = resolve_environment(client, slug, "delete").await?;

    if !force {
        if !is_interactive() {
            bail!("environment delete requires --force in non-interactive mode. Use: bt environments delete <slug> --force");
        }
        let confirmed = Confirm::new()
            .with_prompt(format!(
                "Delete environment '{}' ({})?",
                environment.name, environment.slug
            ))
            .default(false)
            .interact()?;
        if !confirmed {
            return Ok(());
        }
    }

    let deleted = with_spinner(
        "Deleting environment...",
        api::delete_environment(client, &environment.id),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&deleted)?);
    } else {
        print_command_status(
            CommandStatus::Success,
            &format!("Deleted environment '{}' ({})", deleted.name, deleted.slug),
        );
        if !is_quiet() {
            eprintln!("Run `bt environments list` to see remaining environments.");
        }
    }
    Ok(())
}
