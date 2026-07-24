use anyhow::{bail, Context, Result};
use clap::Args;

use crate::args::BaseArgs;
use crate::auth::{self, login};
use crate::config;
use crate::http::ApiClient;
use crate::ui::{can_prompt, print_command_status, select_or_create_project, CommandStatus};

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt switch
  bt switch test-project
  bt switch test-org/test-project
  bt switch --org cross-org
")]
pub struct SwitchArgs {
    #[command(flatten)]
    scope: config::ScopeArgs,

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
                let o = (!parts[0].is_empty()).then(|| config::normalize_org(parts[0]).to_string());
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
    args.scope.preflight(can_prompt())?;
    let current_cfg = if args.scope.global {
        config::load_global().unwrap_or_default()
    } else {
        config::load().unwrap_or_default()
    };
    let (resolved_org, resolved_project) = args.resolve_target(&base);
    let bare_switch = resolved_org.is_none() && resolved_project.is_none();

    let mut login_base = base.clone();
    login_base.org_name = resolved_org.clone();
    login_base.project = None;
    login_base.project_source = None;

    if login_base.org_name.is_none() && !bare_switch {
        login_base.org_name = current_cfg.org.clone();
    }
    if login_base.org_name.is_none()
        && !auth::select_saved_login(&mut login_base, current_cfg.org.as_deref(), true)?
    {
        bail!("no saved auth logins found; run `bt auth login` to create one");
    }

    if login_base.org_name.as_deref() == Some("") && resolved_project.is_some() {
        bail!(
            "cross-org mode cannot have a default project; rerun with --org <ORG> --project <PROJECT>"
        );
    }

    let ctx = login(&login_base).await?;
    let client = ApiClient::new(&ctx)?;
    let org_name = client.org_name().to_string();

    let project = if org_name.is_empty() {
        None
    } else {
        Some(
            select_or_create_project(
                &client,
                resolved_project.as_deref(),
                current_cfg.project.as_deref(),
                None,
            )
            .await?,
        )
    };

    // Scope is prompted last, after org and project.
    let (path, scope) = args.scope.resolve(can_prompt(), "Save to")?;
    let mut cfg = config::load_file(&path);
    cfg.set_context(
        Some(&org_name),
        project
            .as_ref()
            .map(|project| (project.name.as_str(), project.id.as_str())),
    );
    config::save_file(&path, &cfg)
        .with_context(|| format!("Could not save config to {}", path.display()))?;

    if base.json {
        let payload = serde_json::json!({
            "org": config::display_org(&org_name),
            "project": project.as_ref().map(|p| p.name.clone()),
            "project_id": project.as_ref().map(|p| p.id.clone()),
            "scope": scope,
            "path": path.display().to_string(),
        });
        println!("{}", serde_json::to_string(&payload)?);
        return Ok(());
    }

    let display = project
        .as_ref()
        .map(|project| format!("{org_name}/{}", project.name))
        .unwrap_or_else(|| config::display_org(&org_name).to_string());
    print_command_status(CommandStatus::Success, &format!("Switched to {display}"));
    if base.verbose {
        eprintln!("Wrote to {}", path.display());
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn resolve_target_combines_positionals_and_flags() {
        for (target, org, project, expected) in [
            (None, None, None, (None, None)),
            (
                Some("test-org/test-project"),
                None,
                None,
                (Some("test-org"), Some("test-project")),
            ),
            (
                Some("test-project"),
                None,
                None,
                (None, Some("test-project")),
            ),
            (
                Some("/test-project"),
                None,
                None,
                (None, Some("test-project")),
            ),
            (Some("test-org/"), None, None, (Some("test-org"), None)),
            // Positional "cross-org" folds to the "" marker, matching --org.
            (
                Some("cross-org/test-project"),
                None,
                None,
                (Some(""), Some("test-project")),
            ),
            (Some("cross-org/"), None, None, (Some(""), None)),
            (None, Some("test-org"), None, (Some("test-org"), None)),
            (
                None,
                None,
                Some("test-project"),
                (None, Some("test-project")),
            ),
            (
                None,
                Some("test-org"),
                Some("test-project"),
                (Some("test-org"), Some("test-project")),
            ),
            (
                Some("old-org/old-project"),
                None,
                Some("test-project"),
                (Some("old-org"), Some("test-project")),
            ),
            (
                Some("old-project"),
                Some("test-org"),
                None,
                (Some("test-org"), Some("old-project")),
            ),
            (
                Some("old-org/old-project"),
                Some("test-org"),
                Some("test-project"),
                (Some("test-org"), Some("test-project")),
            ),
        ] {
            let args = SwitchArgs {
                scope: config::ScopeArgs::default(),
                target: target.map(str::to_string),
            };
            let base = BaseArgs {
                org_name: org.map(str::to_string),
                project: project.map(str::to_string),
                ..Default::default()
            };
            let actual = args.resolve_target(&base);
            assert_eq!((actual.0.as_deref(), actual.1.as_deref()), expected);
        }
    }
}
