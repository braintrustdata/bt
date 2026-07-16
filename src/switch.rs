use anyhow::{bail, Context, Result};
use clap::Args;
use dialoguer::{console, theme::ColorfulTheme, Select};

use crate::args::BaseArgs;
use crate::auth::{self, login};
use crate::config;
use crate::http::ApiClient;
use crate::projects::api;
use crate::ui::{
    is_interactive, print_command_status, select_project, with_spinner, CommandStatus,
};

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt switch
  bt switch my-project
  bt switch personal-org/my-project
")]
pub struct SwitchArgs {
    /// Force set global config value
    #[arg(long, short = 'g', conflicts_with = "local")]
    global: bool,
    /// Force set local config value
    #[arg(long, short = 'l')]
    local: bool,
    /// Target: project name or org/project
    #[arg(value_name = "TARGET")]
    target: Option<String>,
}

impl SwitchArgs {
    fn resolve_target(&self, base: &BaseArgs) -> (Option<String>, Option<String>) {
        let (pos_org, pos_project) = match &self.target {
            None => (None, None),
            Some(t) if t.contains('/') => {
                let parts: Vec<&str> = t.splitn(2, '/').collect();
                let o = (!parts[0].is_empty()).then(|| parts[0].to_string());
                let p = (!parts[1].is_empty()).then(|| parts[1].to_string());
                (o, p)
            }
            Some(t) => (None, Some(t.clone())),
        };

        let org = base.org_name.clone().or(pos_org);
        let project = base.project.clone().or(pos_project);

        (org, project)
    }
}

pub async fn run(base: BaseArgs, args: SwitchArgs) -> Result<()> {
    let current_cfg = config::load().unwrap_or_default();
    let (resolved_org, resolved_project) = args.resolve_target(&base);
    let mut interactive = false;

    let mut login_base = base.clone();
    if login_base.org_name.is_none() {
        let saved_org = select_saved_auth_org_for_switch()?;
        login_base.org_name = resolved_org
            .clone()
            .or_else(|| current_cfg.org.clone())
            .or(saved_org);
    }

    let ctx = login(&login_base).await?;
    let client = ApiClient::new(&ctx)?;
    let org_name = client.org_name().to_string();

    let project = match resolved_project {
        Some(p) => validate_or_create_project(&client, &p).await?,
        None => {
            if !is_interactive() {
                bail!("target required. Use: bt switch <project> or bt switch <org>/<project>");
            }
            interactive = true;
            select_project(
                &client,
                None,
                None,
                crate::ui::ProjectSelectMode::ExistingOnly,
            )
            .await?
        }
    };

    let (path, scope) = if args.local {
        (
            config::local_path().ok_or_else(|| {
                anyhow::anyhow!(
                    "No local .bt directory found. Use bt init to initialize this directory."
                )
            })?,
            "local",
        )
    } else if args.global {
        (config::global_path()?, "global")
    } else if interactive && config::local_path().is_some() {
        select_scope()?
    } else {
        (config::global_path()?, "global")
    };

    let mut cfg = config::load_file(&path);
    apply_switch_config(&mut cfg, Some(&org_name), Some(&project));
    config::save_file(&path, &cfg)
        .context(format!("Could not save config to {}", path.display()))?;

    if base.json {
        let payload = serde_json::json!({
            "org": org_name,
            "project": project.name,
            "project_id": project.id,
            "scope": scope,
            "path": path.display().to_string(),
        });
        println!("{}", serde_json::to_string(&payload)?);
        return Ok(());
    }

    let display = format!("{org_name}/{}", project.name);
    print_command_status(CommandStatus::Success, &format!("Switched to {display}"));
    if base.verbose {
        eprintln!("Wrote to {}", path.display());
    }

    Ok(())
}

fn select_saved_auth_org_for_switch() -> Result<Option<String>> {
    if !is_interactive() {
        return Ok(None);
    }

    let mut orgs = auth::list_profiles()?
        .into_iter()
        .filter_map(|profile| profile.org_name)
        .filter(|org| !org.trim().is_empty())
        .collect::<Vec<_>>();
    orgs.sort();
    orgs.dedup();

    match orgs.len() {
        0 => Ok(None),
        1 => Ok(orgs.into_iter().next()),
        _ => {
            let labels = orgs.iter().map(String::as_str).collect::<Vec<_>>();
            let idx = crate::ui::fuzzy_select("Select organization", &labels, 0)?;
            Ok(Some(orgs[idx].clone()))
        }
    }
}

pub(crate) fn select_scope() -> Result<(std::path::PathBuf, &'static str)> {
    let global = config::global_path()?;
    let local = config::local_path().unwrap();
    let options = [
        format!(
            "Global ({})",
            console::style(
                dirs::home_dir()
                    .and_then(|home| global
                        .parent()
                        .unwrap()
                        .strip_prefix(&home)
                        .ok()
                        .map(|rel| format!("~/{}", rel.display())))
                    .unwrap_or_else(|| global.parent().unwrap().display().to_string())
            )
            .dim()
        ),
        format!(
            "Local ({})",
            console::style(
                local
                    .parent()
                    .and_then(|bt| {
                        let bt_name = bt.file_name()?;
                        let parent_name = bt.parent()?.file_name()?;
                        Some(format!(
                            "{}/{}",
                            parent_name.to_string_lossy(),
                            bt_name.to_string_lossy()
                        ))
                    })
                    .unwrap_or_else(|| local.parent().unwrap().display().to_string()),
            )
            .dim()
        ),
    ];
    let idx = Select::with_theme(&ColorfulTheme::default())
        .with_prompt("Save to")
        .items(&options)
        .default(1)
        .interact()?;
    if idx == 0 {
        Ok((global, "global"))
    } else {
        Ok((local, "local"))
    }
}

pub(crate) async fn validate_or_create_project(
    client: &ApiClient,
    name: &str,
) -> Result<api::Project> {
    let exists = with_spinner("Loading project...", api::get_project_by_name(client, name)).await?;

    if let Some(project) = exists {
        return Ok(project);
    }

    if !is_interactive() {
        bail!("project '{name}' not found");
    }

    let create = dialoguer::Confirm::new()
        .with_prompt(format!("Project '{name}' not found. Create it?"))
        .default(false)
        .interact()?;

    if create {
        with_spinner("Creating project...", api::create_project(client, name)).await
    } else {
        bail!("project '{name}' not found");
    }
}

pub(crate) fn apply_switch_config(
    cfg: &mut config::Config,
    org_name: Option<&str>,
    project: Option<&api::Project>,
) {
    cfg.org = config::trimmed_option(org_name).map(str::to_string);
    match project {
        Some(project) => {
            cfg.project = Some(project.name.clone());
            cfg.project_id = Some(project.id.clone());
        }
        None => {
            cfg.project = None;
            cfg.project_id = None;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn switch_args(target: Option<&str>) -> SwitchArgs {
        SwitchArgs {
            global: false,
            local: false,
            target: target.map(String::from),
        }
    }

    fn base_args(org: Option<&str>, project: Option<&str>) -> BaseArgs {
        BaseArgs {
            json: false,
            verbose: false,
            verbose_source: None,
            quiet: false,
            quiet_source: None,
            no_color: false,
            no_input: false,
            org_name: org.map(String::from),
            org_name_source: None,
            project: project.map(String::from),
            project_source: None,
            api_key: None,
            api_key_source: None,
            prefer_api_key: false,
            api_url: None,
            app_url: None,
            ca_cert: None,
            env_file: None,
        }
    }

    // --- resolve_target tests (unchanged) ---

    #[test]
    fn no_args_returns_none() {
        let args = switch_args(None);
        let base = base_args(None, None);
        assert_eq!(args.resolve_target(&base), (None, None));
    }

    #[test]
    fn positional_org_project() {
        let args = switch_args(Some("myorg/proj"));
        let base = base_args(None, None);
        assert_eq!(
            args.resolve_target(&base),
            (Some("myorg".into()), Some("proj".into()))
        );
    }

    #[test]
    fn positional_project_only() {
        let args = switch_args(Some("proj"));
        let base = base_args(None, None);
        assert_eq!(args.resolve_target(&base), (None, Some("proj".into())));
    }

    #[test]
    fn slash_with_empty_org() {
        let args = switch_args(Some("/project"));
        let base = base_args(None, None);
        assert_eq!(args.resolve_target(&base), (None, Some("project".into())));
    }

    #[test]
    fn slash_with_empty_project() {
        let args = switch_args(Some("org/"));
        let base = base_args(None, None);
        assert_eq!(args.resolve_target(&base), (Some("org".into()), None));
    }

    #[test]
    fn flag_org_only() {
        let args = switch_args(None);
        let base = base_args(Some("x"), None);
        assert_eq!(args.resolve_target(&base), (Some("x".into()), None));
    }

    #[test]
    fn flag_project_only() {
        let args = switch_args(None);
        let base = base_args(None, Some("y"));
        assert_eq!(args.resolve_target(&base), (None, Some("y".into())));
    }

    #[test]
    fn flags_only() {
        let args = switch_args(None);
        let base = base_args(Some("a"), Some("b"));
        assert_eq!(
            args.resolve_target(&base),
            (Some("a".into()), Some("b".into()))
        );
    }

    #[test]
    fn flag_overrides_positional_project() {
        let args = switch_args(Some("myorg/proj"));
        let base = base_args(None, Some("foo"));
        assert_eq!(
            args.resolve_target(&base),
            (Some("myorg".into()), Some("foo".into()))
        );
    }

    #[test]
    fn flag_org_with_positional_project() {
        let args = switch_args(Some("proj"));
        let base = base_args(Some("bar"), None);
        assert_eq!(
            args.resolve_target(&base),
            (Some("bar".into()), Some("proj".into()))
        );
    }

    #[test]
    fn flag_override_both() {
        let args = switch_args(Some("myorg/proj"));
        let base = base_args(Some("x"), Some("y"));
        assert_eq!(
            args.resolve_target(&base),
            (Some("x".into()), Some("y".into()))
        );
    }

    #[test]
    fn apply_switch_config_sets_project_id_with_project_name_and_org() {
        let mut cfg = config::Config::default();
        let project = api::Project {
            id: "proj_123".to_string(),
            name: "my-project".to_string(),
            org_id: "org_123".to_string(),
            description: None,
        };

        apply_switch_config(&mut cfg, Some("acme-org"), Some(&project));

        assert_eq!(cfg.org.as_deref(), Some("acme-org"));
        assert_eq!(cfg.project.as_deref(), Some("my-project"));
        assert_eq!(cfg.project_id.as_deref(), Some("proj_123"));
    }

    #[test]
    fn apply_switch_config_clears_project_and_org_when_context_is_org_only() {
        let mut cfg = config::Config {
            org: Some("old-org".to_string()),
            project: Some("stale-project".to_string()),
            project_id: Some("proj_stale".to_string()),
            ..Default::default()
        };

        apply_switch_config(&mut cfg, None, None);

        assert_eq!(cfg.org, None);
        assert_eq!(cfg.project, None);
        assert_eq!(cfg.project_id, None);
    }
}
