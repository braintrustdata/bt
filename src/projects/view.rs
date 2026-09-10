use anyhow::{bail, Result};

use crate::http::ApiClient;
use crate::ui::{
    is_interactive, print_command_status, select_project, with_spinner, CommandStatus,
    ProjectSelectMode,
};
use crate::utils::app_project_url;

use super::api;

pub async fn run(
    client: &ApiClient,
    app_url: &str,
    org_name: &str,
    name: Option<&str>,
    json: bool,
) -> Result<()> {
    let project = match name {
        Some(n) => with_spinner("Loading project...", api::get_project_by_name(client, n))
            .await?
            .ok_or_else(|| anyhow::anyhow!("project '{n}' not found"))?,
        None => {
            if !is_interactive() {
                bail!("project name required. Use: bt projects view <name>")
            }
            select_project(client, None, None, ProjectSelectMode::ExistingOnly).await?
        }
    };

    if json {
        println!("{}", serde_json::to_string(&project)?);
        return Ok(());
    }

    let url = app_project_url(app_url, org_name, &project.name, &[]);

    open::that(&url)?;
    print_command_status(CommandStatus::Success, &format!("Opened {url} in browser"));

    Ok(())
}
