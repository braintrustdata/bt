use anyhow::{anyhow, Result};

use crate::{
    args::BaseArgs,
    auth::{login, login_read_only},
    config,
    http::ApiClient,
    projects::api::{get_project_by_name, Project},
    ui::{is_interactive, select_project, ProjectSelectMode},
};

pub(crate) struct ProjectContext {
    pub client: ApiClient,
    pub app_url: String,
    pub project: Project,
}

pub(crate) async fn resolve_project_optional(
    base: &BaseArgs,
    client: &ApiClient,
    allow_interactive_selection: bool,
) -> Result<Option<Project>> {
    let config_project = config::load().ok().and_then(|config| config.project);
    let project_name = match base.project.as_deref().or(config_project.as_deref()) {
        Some(project_name) => Some(project_name.to_string()),
        None if allow_interactive_selection && is_interactive() => Some(
            select_project(client, None, None, ProjectSelectMode::ExistingOnly)
                .await?
                .name,
        ),
        None => None,
    };

    match project_name {
        Some(project_name) => get_project_by_name(client, &project_name)
            .await?
            .map(Some)
            .ok_or_else(|| anyhow!("project '{project_name}' not found")),
        None => Ok(None),
    }
}

pub(crate) async fn resolve_required_project(
    base: &BaseArgs,
    client: &ApiClient,
    allow_interactive_selection: bool,
) -> Result<Project> {
    resolve_project_optional(base, client, allow_interactive_selection)
        .await?
        .ok_or_else(|| anyhow!("--project required (or set BRAINTRUST_DEFAULT_PROJECT)"))
}

pub(crate) async fn resolve_project_command_context_with_auth_mode(
    base: &BaseArgs,
    read_only: bool,
) -> Result<ProjectContext> {
    let auth = if read_only {
        login_read_only(base).await?
    } else {
        login(base).await?
    };
    let client = ApiClient::new(&auth)?;
    let project = resolve_required_project(base, &client, true).await?;
    Ok(ProjectContext {
        client,
        app_url: auth.app_url,
        project,
    })
}
