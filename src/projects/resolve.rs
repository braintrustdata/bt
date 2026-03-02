use std::path::PathBuf;

use anyhow::{anyhow, bail, Result};
use dialoguer::Confirm;

use crate::{
    args::BaseArgs,
    config::{self, Config},
    http::ApiClient,
    ui::{self, print_command_status, with_spinner, CommandStatus},
};

use super::api::{get_project_by_name, list_projects, Project};

#[derive(Clone)]
pub(crate) struct ProjectContext {
    pub client: ApiClient,
    pub app_url: String,
    pub project: Project,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
enum StaleRecoveryPath {
    ErrorRequiresExplicitSelector,
    NonInteractiveUseResolved,
    InteractiveConfirmOrSelect,
    InteractiveSelectOnly,
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
        heal_project_config_best_effort(base, cfg.project.as_deref(), &project);
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
    let resolved = get_project_by_name(client, project_name).await?;

    if !has_stale_cached_project_id(cfg, project_name, resolved.as_ref()) {
        return resolved.ok_or_else(|| anyhow!("project '{project_name}' not found"));
    }

    recover_stale_project(base, client, cfg, project_name, resolved).await
}

async fn recover_stale_project(
    base: &BaseArgs,
    client: &ApiClient,
    cfg: &Config,
    requested_name: &str,
    resolved_by_name: Option<Project>,
) -> Result<Project> {
    let path = decide_stale_recovery_path(ui::is_interactive(), resolved_by_name.is_some());
    let healed = match path {
        StaleRecoveryPath::ErrorRequiresExplicitSelector => {
            bail!("{}", stale_non_interactive_error_message(requested_name))
        }
        StaleRecoveryPath::NonInteractiveUseResolved => {
            resolved_by_name.ok_or_else(|| anyhow!("project '{requested_name}' not found"))?
        }
        StaleRecoveryPath::InteractiveConfirmOrSelect
        | StaleRecoveryPath::InteractiveSelectOnly => {
            choose_healed_project_interactive(client, requested_name, resolved_by_name.as_ref())
                .await?
        }
    };

    heal_project_config_best_effort(base, cfg.project.as_deref(), &healed);
    Ok(healed)
}

fn decide_stale_recovery_path(
    interactive: bool,
    resolved_by_name_exists: bool,
) -> StaleRecoveryPath {
    if interactive {
        if resolved_by_name_exists {
            StaleRecoveryPath::InteractiveConfirmOrSelect
        } else {
            StaleRecoveryPath::InteractiveSelectOnly
        }
    } else if resolved_by_name_exists {
        StaleRecoveryPath::NonInteractiveUseResolved
    } else {
        StaleRecoveryPath::ErrorRequiresExplicitSelector
    }
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

    let healed_name = project.name.clone();
    let healed_id = project.id.clone();
    config::update_file_with_lock(&path, |file_cfg| {
        if file_cfg.project.as_deref() != Some(config_project_name) {
            return false;
        }
        if file_cfg.project.as_deref() == Some(healed_name.as_str())
            && file_cfg.project_id.as_deref() == Some(healed_id.as_str())
        {
            return false;
        }

        file_cfg.project = Some(healed_name.clone());
        file_cfg.project_id = Some(healed_id.clone());
        true
    })?;
    Ok(())
}

fn heal_project_config_best_effort(
    base: &BaseArgs,
    config_project_name: Option<&str>,
    project: &Project,
) {
    if let Err(err) = maybe_heal_project_config(config_project_name, project) {
        if !base.json {
            print_command_status(
                CommandStatus::Warning,
                &format!(
                    "resolved project '{}' but failed to heal cached config: {err}",
                    project.name
                ),
            );
        }
    }
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

fn stale_non_interactive_error_message(requested_name: &str) -> String {
    format!(
        "cached project_id for '{requested_name}' is stale and project '{requested_name}' was not found. In non-interactive mode, pass --project <project-name> or set BRAINTRUST_DEFAULT_PROJECT."
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        env,
        ffi::OsString,
        sync::{Mutex, OnceLock},
    };
    use tempfile::TempDir;

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

    #[test]
    fn stale_recovery_non_interactive_uses_resolved_when_available() {
        assert_eq!(
            decide_stale_recovery_path(false, true),
            StaleRecoveryPath::NonInteractiveUseResolved
        );
    }

    #[test]
    fn stale_recovery_non_interactive_errors_when_named_project_missing() {
        assert_eq!(
            decide_stale_recovery_path(false, false),
            StaleRecoveryPath::ErrorRequiresExplicitSelector
        );
    }

    #[test]
    fn stale_recovery_interactive_uses_confirm_when_named_match_exists() {
        assert_eq!(
            decide_stale_recovery_path(true, true),
            StaleRecoveryPath::InteractiveConfirmOrSelect
        );
    }

    #[test]
    fn stale_recovery_interactive_falls_back_to_selection_when_named_match_missing() {
        assert_eq!(
            decide_stale_recovery_path(true, false),
            StaleRecoveryPath::InteractiveSelectOnly
        );
    }

    #[test]
    fn stale_non_interactive_message_does_not_suggest_project_id() {
        let message = stale_non_interactive_error_message("foo");
        assert!(message.contains("--project <project-name>"));
        assert!(!message.contains("--project-id"));
    }

    #[test]
    fn maybe_heal_project_config_updates_project_and_project_id_atomically() {
        let _guard = env_test_lock().lock().unwrap();
        let temp = TempDir::new().unwrap();
        let _xdg_guard = XdgConfigHomeGuard::set_to(temp.path().join("xdg"));

        let config_path = config::global_path().unwrap();
        let initial = Config {
            project: Some("foo".to_string()),
            project_id: Some("proj_old".to_string()),
            ..Default::default()
        };
        config::save_file(&config_path, &initial).unwrap();

        let healed = project("foo", "proj_new");
        maybe_heal_project_config(Some("foo"), &healed).unwrap();
        let healed_config = config::load_file(&config_path);
        assert_eq!(healed_config.project.as_deref(), Some("foo"));
        assert_eq!(healed_config.project_id.as_deref(), Some("proj_new"));
    }

    fn env_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    struct XdgConfigHomeGuard {
        old: Option<OsString>,
    }

    impl XdgConfigHomeGuard {
        fn set_to(path: std::path::PathBuf) -> Self {
            let old = env::var_os("XDG_CONFIG_HOME");
            unsafe {
                env::set_var("XDG_CONFIG_HOME", path);
            }
            Self { old }
        }
    }

    impl Drop for XdgConfigHomeGuard {
        fn drop(&mut self) {
            match &self.old {
                Some(value) => unsafe {
                    env::set_var("XDG_CONFIG_HOME", value);
                },
                None => unsafe {
                    env::remove_var("XDG_CONFIG_HOME");
                },
            }
        }
    }
}
