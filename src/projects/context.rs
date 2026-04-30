use anyhow::{anyhow, bail, Result};

use crate::{
    args::BaseArgs,
    auth::{login, login_read_only},
    config,
    http::ApiClient,
    ui::{is_interactive, select_project, ProjectSelectMode},
};

use super::api::{get_project_by_name, Project};

pub(crate) struct ProjectContext {
    pub client: ApiClient,
    pub app_url: String,
    pub project: Project,
}

pub(crate) async fn resolve_project_context(
    base: &BaseArgs,
    read_only: bool,
) -> Result<ProjectContext> {
    let auth = if read_only {
        login_read_only(base).await?
    } else {
        login(base).await?
    };
    let client = ApiClient::new(&auth)?;
    let config_project = config::configured_project_for_context(base, Some(client.org_name()));
    let project_name = match base.project.as_deref().or(config_project.as_deref()) {
        Some(p) => p.to_string(),
        None if is_interactive() => {
            select_project(&client, None, None, ProjectSelectMode::ExistingOnly)
                .await?
                .name
        }
        None => bail!("--project required (or set BRAINTRUST_DEFAULT_PROJECT)"),
    };
    let project = get_project_by_name(&client, &project_name)
        .await?
        .ok_or_else(|| anyhow!("project '{project_name}' not found"))?;

    Ok(ProjectContext {
        client,
        app_url: auth.app_url,
        project,
    })
}
