use std::io::IsTerminal;

use anyhow::{anyhow, Result};

use crate::{
    args::BaseArgs,
    http::ApiClient,
    login::{login, LoginContext},
    projects::{api::get_project_by_name, switch::select_project_interactive},
};

pub struct ProjectCommandContext {
    pub login: LoginContext,
    pub client: ApiClient,
    pub project: String,
}

pub async fn resolve_project_command_context(base: &BaseArgs) -> Result<ProjectCommandContext> {
    let login = login(base).await?;
    let client = ApiClient::new(&login)?;
    let project = match &base.project {
        Some(p) => p.clone(),
        None if std::io::stdin().is_terminal() => select_project_interactive(&client).await?,
        None => anyhow::bail!("--project required (or set BRAINTRUST_DEFAULT_PROJECT)"),
    };

    get_project_by_name(&client, &project)
        .await?
        .ok_or_else(|| anyhow!("project '{project}' not found"))?;

    Ok(ProjectCommandContext {
        login,
        client,
        project,
    })
}
