use std::io::IsTerminal;

use anyhow::{bail, Context, Result};
use clap::Args;

use crate::args::BaseArgs;
use crate::config;
use crate::http::ApiClient;
use crate::login::login;
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

        let org = base.org.clone().or(pos_org);
        let project = base.project.clone().or(pos_project);

        (org, project)
    }
}

pub async fn run(base: BaseArgs, args: SwitchArgs) -> Result<()> {
    let path = config::resolve_write_path(args.global, args.local)?;

    let ctx = login(&base).await?;
    let client = ApiClient::new(&ctx)?;
    // For now, always use org from API client
    // We use the org_name from the client to ensure user's can't override it
    // accidentally when switching until we have multi-org oauth setup
    let org_name = &client.org_name();

    // TODO: get `resolved_org` available when multi-org auth is ready
    let (_, resolved_project) = args.resolve_target(&base);

    let project_name = match resolved_project {
        Some(p) => validate_or_create_project(&client, &p).await?,
        None => {
            if !std::io::stdin().is_terminal() {
                bail!("project required. Use: bt switch <target> or bt switch --project <name>");
            }
            select_project_interactive(&client, None).await?
        }
    };

    let mut cfg = config::load_file(&path);
    // TODO: use `resolved_org` in place of `org_name` to support switching when multi-org auth is ready
    cfg.org = Some(org_name.to_string());
    cfg.project = Some(project_name.clone());

    config::save_file(&path, &cfg)
        .context(format!("Could not save config to {}", path.display()))?;

    print_command_status(
        CommandStatus::Success,
        &format!("Switched to {org_name}/{project_name}"),
    );
    // TODO: Only show in --verbose mode
    eprintln!("Wrote to {}", path.display());

    Ok(())
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
            org: org.map(String::from),
            project: project.map(String::from),
            api_key: None,
            api_url: None,
            app_url: None,
            env_file: None,
        }
    }

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
}
