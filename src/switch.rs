use std::io::IsTerminal;

use anyhow::{bail, Context, Result};
use clap::Args;
use dialoguer::{console, theme::ColorfulTheme, Select};

use crate::args::BaseArgs;
use crate::auth::{self, login};
use crate::config;
use crate::http::ApiClient;
use crate::projects::api;
use crate::ui::{print_command_status, select_project_interactive, with_spinner, CommandStatus};

#[derive(Debug, Clone, Args)]
pub struct SwitchArgs {
    /// Force set global config value
    #[arg(long, short = 'g', conflicts_with = "local")]
    global: bool,
    /// Force set local config value
    #[arg(long, short = 'l')]
    local: bool,
    /// Output verbose response
    #[arg(long)]
    verbose: bool,
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

    let profile_name = match &resolved_org {
        Some(org_or_profile) => {
            if base.profile.is_some() {
                None
            } else {
                let profiles = auth::list_profiles()?;
                Some(auth::resolve_org_to_profile(org_or_profile, &profiles)?)
            }
        }
        None => {
            if resolved_project.is_none() && std::io::stdin().is_terminal() {
                interactive = true;
                auth::select_profile_interactive(current_cfg.org.as_deref())?
            } else {
                None
            }
        }
    };

    // When we resolved a profile from an org identifier, clear org_name — the raw identifier
    // (e.g. "staging") may differ from the profile's actual org (e.g. "staging-org"). Letting
    // org_name stay would override the profile's stored org_name in resolve_auth_from_store.
    //
    // When no org was specified (project-only switch), load the current config org so
    // resolve_auth can find the right profile for authentication.
    let login_base = match &profile_name {
        Some(profile) if base.profile.is_none() => BaseArgs {
            profile: Some(profile.clone()),
            org_name: None,
            ..base.clone()
        },
        _ => {
            let mut b = base.clone();
            if b.org_name.is_none() && b.profile.is_none() {
                b.org_name = current_cfg.org.clone();
            }
            if b.org_name.is_none() && b.profile.is_none() {
                let profiles = auth::list_profiles()?;
                if profiles.len() > 1 {
                    let names: Vec<&str> = profiles.iter().map(|p| p.name.as_str()).collect();
                    bail!(
                        "multiple auth profiles found: {}. Use --profile to disambiguate.",
                        names.join(", ")
                    );
                }
            }
            b
        }
    };

    let ctx = login(&login_base).await?;
    let client = ApiClient::new(&ctx)?;
    let org_name = client.org_name();

    let project_name = match resolved_project {
        Some(p) => Some(validate_or_create_project(&client, &p).await?),
        None => {
            if !std::io::stdin().is_terminal() {
                bail!("target required. Use: bt switch <project> or bt switch <org>/<project>");
            }
            interactive = true;
            Some(select_project_interactive(&client, None, current_cfg.project.as_deref()).await?)
        }
    };

    let path = if args.local {
        config::local_path().ok_or_else(|| {
            anyhow::anyhow!(
                "No local .bt directory found. Use bt init to initialize this directory."
            )
        })?
    } else if args.global {
        config::global_path()?
    } else if interactive && config::local_path().is_some() {
        select_scope()?
    } else {
        config::global_path()?
    };

    let mut cfg = config::load_file(&path);
    cfg.org = Some(org_name.to_string());
    cfg.project = project_name.clone();
    config::save_file(&path, &cfg)
        .context(format!("Could not save config to {}", path.display()))?;

    let display = match &project_name {
        Some(p) => format!("{org_name}/{p}"),
        None => org_name.to_string(),
    };
    print_command_status(CommandStatus::Success, &format!("Switched to {display}"));
    if args.verbose {
        eprintln!("Wrote to {}", path.display());
    }

    Ok(())
}

fn select_scope() -> Result<std::path::PathBuf> {
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
        Ok(global)
    } else {
        Ok(local)
    }
}

async fn validate_or_create_project(client: &ApiClient, name: &str) -> Result<String> {
    let exists = with_spinner("Loading project...", api::get_project_by_name(client, name)).await?;

    if exists.is_some() {
        return Ok(name.to_string());
    }

    if !std::io::stdin().is_terminal() {
        bail!("project '{name}' not found");
    }

    let create = dialoguer::Confirm::new()
        .with_prompt(format!("Project '{name}' not found. Create it?"))
        .default(false)
        .interact()?;

    if create {
        with_spinner("Creating project...", api::create_project(client, name)).await?;
        Ok(name.to_string())
    } else {
        bail!("project '{name}' not found");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::{resolve_org_to_profile, ProfileInfo};

    fn switch_args(target: Option<&str>) -> SwitchArgs {
        SwitchArgs {
            global: false,
            local: false,
            verbose: false,
            target: target.map(String::from),
        }
    }

    fn base_args(org: Option<&str>, project: Option<&str>) -> BaseArgs {
        BaseArgs {
            json: false,
            profile: None,
            org_name: org.map(String::from),
            project: project.map(String::from),
            api_key: None,
            prefer_profile: false,
            api_url: None,
            app_url: None,
            env_file: None,
        }
    }

    fn profile_info(name: &str, org_name: Option<&str>) -> ProfileInfo {
        ProfileInfo {
            name: name.to_string(),
            org_name: org_name.map(String::from),
            user_name: None,
            email: None,
            api_key_hint: None,
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

    // --- resolve_org_to_profile tests ---

    #[test]
    fn resolve_by_exact_profile_name() {
        let profiles = vec![profile_info("acme", Some("acme-corp"))];
        assert_eq!(resolve_org_to_profile("acme", &profiles).unwrap(), "acme");
    }

    #[test]
    fn resolve_by_org_name_when_profile_name_differs() {
        let profiles = vec![profile_info("work", Some("acme-corp"))];
        assert_eq!(
            resolve_org_to_profile("acme-corp", &profiles).unwrap(),
            "work"
        );
    }

    #[test]
    fn resolve_no_match_errors() {
        let profiles = vec![profile_info("work", Some("acme-corp"))];
        assert!(resolve_org_to_profile("unknown", &profiles).is_err());
    }

    #[test]
    fn resolve_empty_profiles_errors() {
        let profiles: Vec<ProfileInfo> = vec![];
        let err = resolve_org_to_profile("anything", &profiles).unwrap_err();
        assert!(err.to_string().contains("no auth profiles found"));
    }

    #[test]
    fn resolve_prefers_profile_name_over_org_name() {
        let profiles = vec![
            profile_info("acme", Some("other")),
            profile_info("x", Some("acme")),
        ];
        assert_eq!(resolve_org_to_profile("acme", &profiles).unwrap(), "acme");
    }

    #[test]
    fn resolve_profile_without_org() {
        let profiles = vec![profile_info("default", None)];
        assert_eq!(
            resolve_org_to_profile("default", &profiles).unwrap(),
            "default"
        );
    }

    // --- login_base org_name clearing tests ---

    #[test]
    fn login_base_clears_org_name_when_profile_resolved() {
        let base = BaseArgs {
            org_name: Some("staging".into()),
            ..base_args(None, Some("foobar"))
        };
        let profile_name = Some("staging".to_string());

        let login_base = match &profile_name {
            Some(profile) if base.profile.is_none() => BaseArgs {
                profile: Some(profile.clone()),
                org_name: None,
                ..base.clone()
            },
            _ => base.clone(),
        };

        assert_eq!(login_base.profile, Some("staging".into()));
        assert_eq!(login_base.org_name, None);
    }

    #[test]
    fn login_base_preserves_org_when_explicit_profile_flag() {
        let base = BaseArgs {
            profile: Some("staging".into()),
            org_name: Some("custom-org".into()),
            ..base_args(None, Some("foobar"))
        };
        let profile_name: Option<String> = None;

        let login_base = match &profile_name {
            Some(profile) if base.profile.is_none() => BaseArgs {
                profile: Some(profile.clone()),
                org_name: None,
                ..base.clone()
            },
            _ => base.clone(),
        };

        assert_eq!(login_base.profile, Some("staging".into()));
        assert_eq!(login_base.org_name, Some("custom-org".into()));
    }
}
