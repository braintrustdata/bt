use std::path::PathBuf;

use anyhow::{anyhow, bail, Result};
use dialoguer::Confirm;

use crate::{
    args::BaseArgs,
    config::{self, Config},
    http::ApiClient,
    ui::{self, with_spinner},
};

use super::api::{get_project_by_name, list_projects, Project};

#[derive(Clone)]
pub(crate) struct ProjectContext {
    pub client: ApiClient,
    pub app_url: String,
    pub project: Project,
}

pub(crate) async fn resolve_project_or_select(
    base: &BaseArgs,
    client: &ApiClient,
) -> Result<Project> {
    resolve_project_or_select_with_id(base, client, None).await
}

pub(crate) async fn resolve_project_or_select_with_id(
    base: &BaseArgs,
    client: &ApiClient,
    explicit_project_id: Option<&str>,
) -> Result<Project> {
    let cfg = config::load().ok().unwrap_or_default();
    if let Some(project_id) = explicit_project_id.and_then(non_empty_trimmed) {
        let project = resolve_project_by_id(client, project_id)
            .await?
            .ok_or_else(|| anyhow!("project '{project_id}' not found"))?;
        maybe_heal_project_config(cfg.project.as_deref(), &project)?;
        return Ok(project);
    }

    let requested_name = base
        .project
        .as_deref()
        .and_then(non_empty_trimmed)
        .or(cfg.project.as_deref());

    match requested_name {
        Some(project_name) => resolve_named_project(base, client, &cfg, project_name).await,
        None if ui::is_interactive() => select_project_record(client, "Select project", None).await,
        None => bail!("--project required (or set BRAINTRUST_DEFAULT_PROJECT)"),
    }
}

async fn resolve_named_project(
    base: &BaseArgs,
    client: &ApiClient,
    cfg: &Config,
    project_name: &str,
) -> Result<Project> {
    let explicit_project_name = base
        .project
        .as_deref()
        .and_then(non_empty_trimmed)
        .is_some();
    let resolved = get_project_by_name(client, project_name).await?;

    if !has_stale_cached_project_id(cfg, project_name, resolved.as_ref()) {
        return resolved.ok_or_else(|| anyhow!("project '{project_name}' not found"));
    }

    recover_stale_project(
        base,
        client,
        cfg,
        project_name,
        resolved,
        explicit_project_name,
    )
    .await
}

async fn recover_stale_project(
    _base: &BaseArgs,
    client: &ApiClient,
    cfg: &Config,
    requested_name: &str,
    resolved_by_name: Option<Project>,
    explicit_project_name: bool,
) -> Result<Project> {
    let interactive = ui::is_interactive();
    if !interactive && !explicit_project_name {
        bail!(
            "cached project_id for '{requested_name}' is stale. In non-interactive mode, pass --project <project-name> or --project-id <project-id>."
        );
    }

    let healed = if interactive {
        choose_healed_project_interactive(client, requested_name, resolved_by_name.as_ref()).await?
    } else {
        resolved_by_name.ok_or_else(|| anyhow!("project '{requested_name}' not found"))?
    };

    maybe_heal_project_config(cfg.project.as_deref(), &healed)?;
    Ok(healed)
}

async fn choose_healed_project_interactive(
    client: &ApiClient,
    requested_name: &str,
    resolved_by_name: Option<&Project>,
) -> Result<Project> {
    if let Some(project) = resolved_by_name {
        let use_resolved = Confirm::new()
            .with_prompt(format!(
                "Cached project context for '{requested_name}' is stale. Use '{}' and heal config?",
                project.name
            ))
            .default(true)
            .interact()?;
        if use_resolved {
            return Ok(project.clone());
        }
    }

    select_project_record(
        client,
        "Select project to heal config",
        Some(requested_name),
    )
    .await
}

async fn resolve_project_by_id(client: &ApiClient, project_id: &str) -> Result<Option<Project>> {
    let projects = list_projects(client).await?;
    Ok(projects
        .into_iter()
        .find(|project| project.id == project_id))
}

async fn select_project_record(
    client: &ApiClient,
    prompt: &str,
    current: Option<&str>,
) -> Result<Project> {
    let mut projects = with_spinner("Loading projects...", list_projects(client)).await?;
    if projects.is_empty() {
        bail!("no projects found in org '{}'", client.org_name());
    }

    projects.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = projects
        .iter()
        .map(|project| project.name.as_str())
        .collect();
    let default = current
        .and_then(|name| names.iter().position(|candidate| *candidate == name))
        .unwrap_or(0);
    let idx = ui::fuzzy_select(prompt, &names, default)?;
    Ok(projects[idx].clone())
}

fn maybe_heal_project_config(config_project_name: Option<&str>, project: &Project) -> Result<()> {
    let Some(config_project_name) = config_project_name else {
        return Ok(());
    };
    let Some(path) = project_config_path(config_project_name)? else {
        return Ok(());
    };

    let mut file_cfg = config::load_file(&path);
    if file_cfg.project.as_deref() == Some(project.name.as_str())
        && file_cfg.project_id.as_deref() == Some(project.id.as_str())
    {
        return Ok(());
    }

    file_cfg.project = Some(project.name.clone());
    file_cfg.project_id = Some(project.id.clone());
    config::save_file(&path, &file_cfg)
}

fn project_config_path(project_name: &str) -> Result<Option<PathBuf>> {
    if let Some(path) = config::local_path() {
        let cfg = config::load_file(&path);
        if cfg.project.as_deref() == Some(project_name) {
            return Ok(Some(path));
        }
    }

    let global_path = config::global_path()?;
    let global_cfg = config::load_file(&global_path);
    if global_cfg.project.as_deref() == Some(project_name) {
        return Ok(Some(global_path));
    }

    Ok(None)
}

fn has_stale_cached_project_id(
    cfg: &Config,
    project_name: &str,
    resolved_project: Option<&Project>,
) -> bool {
    let Some(cfg_project_name) = cfg.project.as_deref() else {
        return false;
    };
    if cfg_project_name != project_name {
        return false;
    }
    let Some(cached_project_id) = cfg.project_id.as_deref() else {
        return false;
    };
    match resolved_project {
        Some(project) => project.id != cached_project_id,
        None => true,
    }
}

fn non_empty_trimmed(value: &str) -> Option<&str> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        None
    } else {
        Some(trimmed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config(project: Option<&str>, project_id: Option<&str>) -> Config {
        Config {
            org: Some("acme".to_string()),
            project: project.map(|value| value.to_string()),
            project_id: project_id.map(|value| value.to_string()),
            ..Default::default()
        }
    }

    fn project(name: &str, id: &str) -> Project {
        Project {
            id: id.to_string(),
            name: name.to_string(),
            org_id: "org".to_string(),
            description: None,
        }
    }

    #[test]
    fn stale_cached_id_when_resolved_project_id_differs() {
        let cfg = config(Some("foo"), Some("proj_old"));
        let resolved = project("foo", "proj_new");
        assert!(has_stale_cached_project_id(&cfg, "foo", Some(&resolved)));
    }

    #[test]
    fn stale_cached_id_when_project_name_missing() {
        let cfg = config(Some("foo"), Some("proj_old"));
        assert!(has_stale_cached_project_id(&cfg, "foo", None));
    }

    #[test]
    fn cached_id_not_stale_when_ids_match() {
        let cfg = config(Some("foo"), Some("proj_ok"));
        let resolved = project("foo", "proj_ok");
        assert!(!has_stale_cached_project_id(&cfg, "foo", Some(&resolved)));
    }

    #[test]
    fn cached_id_not_stale_for_non_config_project_name() {
        let cfg = config(Some("foo"), Some("proj_ok"));
        let resolved = project("bar", "proj_ok");
        assert!(!has_stale_cached_project_id(&cfg, "bar", Some(&resolved)));
    }
}
