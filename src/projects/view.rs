use std::io::IsTerminal;

use anyhow::{bail, Result};
use urlencoding::encode;

use crate::http::ApiClient;
use crate::ui::{print_command_status, select_project_interactive, with_spinner, CommandStatus};

use super::api;

pub async fn run(
    client: &ApiClient,
    app_url: &str,
    org_name: &str,
    name: Option<&str>,
) -> Result<()> {
    let project_name = match name {
        Some(n) => n.to_string(),
        None => {
            if !std::io::stdin().is_terminal() {
                bail!("project name required. Use: bt projects view <name>")
            }
            select_project_interactive(client, None, None).await?
        }
    };

    // Verify project exists
    let exists = with_spinner(
        "Loading project...",
        api::get_project_by_name(client, &project_name),
    )
    .await?;
    if exists.is_none() {
        bail!("project '{project_name}' not found");
    }

    let url = format!(
        "{}/app/{}/p/{}",
        app_url.trim_end_matches('/'),
        encode(org_name),
        encode(&project_name)
    );

    open::that(&url)?;
    print_command_status(CommandStatus::Success, &format!("Opened {url} in browser"));

    Ok(())
}
