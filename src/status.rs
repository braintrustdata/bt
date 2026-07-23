use anyhow::Result;
use clap::Args;
use serde::Serialize;

use crate::args::BaseArgs;
use crate::auth;
use crate::config;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt status
  bt status --json
  bt status --verbose
")]
pub struct StatusArgs {}

#[derive(Serialize)]
struct StatusOutput {
    org: Option<String>,
    project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    api_key_hint: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    auth_method: Option<String>,
    source: Option<String>,
}

fn format_identity(p: &auth::ProfileInfo) -> Option<String> {
    if let Some(ref email) = p.email {
        match p.user_name.as_deref() {
            Some(name) => Some(format!("{name} ({email})")),
            None => Some(email.clone()),
        }
    } else {
        p.api_key_hint.clone()
    }
}

fn format_auth(p: &auth::ProfileInfo) -> String {
    match format_identity(p) {
        Some(identity) => format!("{} — {identity}", p.auth_method),
        None => p.auth_method.clone(),
    }
}

pub async fn run(base: BaseArgs, _args: StatusArgs) -> Result<()> {
    let global_path = config::global_path().ok();
    let global_cfg = config::load_global().unwrap_or_default();
    let local_path = config::local_path();
    let local_cfg = local_path
        .as_ref()
        .map(|p| config::load_file(p))
        .unwrap_or_default();

    let overrides = ConfigOverrides::from_base(&base);
    let (org, mut project, source) = resolve_config(
        overrides,
        &global_cfg,
        &local_cfg,
        &local_path,
        &global_path,
    );
    let merged_cfg = global_cfg.merge(&local_cfg);
    let auth_info = auth::active_auth_info(&base, org.as_deref());

    if base
        .project
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
    {
        project = config::project_from_config_for_context(&base, &merged_cfg, org.as_deref());
    }

    if base.json {
        let output = StatusOutput {
            org,
            project,
            user_name: auth_info.as_ref().and_then(|p| p.user_name.clone()),
            user_email: auth_info.as_ref().and_then(|p| p.email.clone()),
            api_key_hint: auth_info.as_ref().and_then(|p| p.api_key_hint.clone()),
            auth_method: auth_info.as_ref().map(|p| p.auth_method.clone()),
            source,
        };
        println!("{}", serde_json::to_string(&output)?);
        return Ok(());
    }

    if base.verbose {
        let org_display = if org.is_none()
            && auth_info
                .as_ref()
                .is_some_and(|p| p.auth_method == "oauth" && p.org_name.is_none())
        {
            "cross-org"
        } else {
            org.as_deref().unwrap_or("(unset)")
        };
        println!("org: {org_display}");
        println!("project: {}", project.as_deref().unwrap_or("(unset)"));
        if let Some(ref p) = auth_info {
            println!("auth: {}", format_auth(p));
        }
        if let Some(src) = source {
            println!("source: {src}");
        }
    } else if org.is_some() {
        let scope = match (&org, &project) {
            (Some(o), Some(p)) => format!("{o}/{p}"),
            (Some(o), None) => o.to_string(),
            _ => unreachable!(),
        };
        println!("{scope}");
        let auth_line = match &auth_info {
            Some(p) => format!("  auth: {}", format_auth(p)),
            None => "  auth: (none)".to_string(),
        };
        println!("{auth_line}");
    } else if auth_info
        .as_ref()
        .is_some_and(|p| p.auth_method == "oauth" && p.org_name.is_none())
    {
        println!("cross-org");
        if let Some(p) = &auth_info {
            println!("  auth: {}", format_auth(p));
        }
    } else {
        println!("No org/project configured. Run `bt switch` to set one.");
    }

    Ok(())
}

#[derive(Default)]
pub(crate) struct ConfigOverrides {
    cli_org: Option<String>,
    env_org: Option<String>,
    cli_project: Option<String>,
    env_project: Option<String>,
}

impl ConfigOverrides {
    fn from_base(base: &BaseArgs) -> Self {
        use crate::args::ArgValueSource;

        let (cli_org, env_org) = match base.org_name_source {
            Some(ArgValueSource::CommandLine) => (base.org_name.clone(), None),
            Some(ArgValueSource::EnvVariable) => (None, base.org_name.clone()),
            None => (None, None),
        };
        let (cli_project, env_project) = match base.project_source {
            Some(ArgValueSource::CommandLine) => (base.project.clone(), None),
            Some(ArgValueSource::EnvVariable) => (None, base.project.clone()),
            None => (None, None),
        };

        Self {
            cli_org,
            env_org,
            cli_project,
            env_project,
        }
    }
}

/// Precedence (clig.dev): CLI flag > env var > local config > global config.
pub(crate) fn resolve_config(
    overrides: ConfigOverrides,
    global: &config::Config,
    local: &config::Config,
    local_path: &Option<std::path::PathBuf>,
    global_path: &Option<std::path::PathBuf>,
) -> (Option<String>, Option<String>, Option<String>) {
    let ConfigOverrides {
        cli_org,
        env_org,
        cli_project,
        env_project,
    } = overrides;
    let env_org = env_org.filter(|s| !s.is_empty());
    let env_project = env_project.filter(|s| !s.is_empty());

    let org = cli_org
        .clone()
        .or_else(|| env_org.clone())
        .or_else(|| local.org.clone())
        .or_else(|| global.org.clone());

    let project = cli_project
        .clone()
        .or_else(|| env_project.clone())
        .or_else(|| local.project.clone())
        .or_else(|| global.project.clone());

    let source = if cli_org.is_some() || cli_project.is_some() {
        Some("cli".to_string())
    } else if env_org.is_some() || env_project.is_some() {
        Some("env".to_string())
    } else if local.org.is_some() || local.project.is_some() {
        local_path.as_ref().map(|p| p.display().to_string())
    } else if global.org.is_some() || global.project.is_some() {
        global_path.as_ref().map(|p| p.display().to_string())
    } else {
        None
    };

    (org, project, source)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn s(v: &str) -> Option<String> {
        Some(v.into())
    }

    fn config(org: Option<&str>, project: Option<&str>) -> config::Config {
        config::Config {
            org: org.map(String::from),
            project: project.map(String::from),
            ..Default::default()
        }
    }

    #[test]
    fn cli_overrides_everything() {
        let global = config(Some("global-org"), Some("global-proj"));
        let local = config(Some("local-org"), Some("local-proj"));
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));

        let (org, project, source) = resolve_config(
            ConfigOverrides {
                cli_org: s("cli-org"),
                cli_project: s("cli-proj"),
                ..Default::default()
            },
            &global,
            &local,
            &local_path,
            &global_path,
        );

        assert_eq!(org, s("cli-org"));
        assert_eq!(project, s("cli-proj"));
        assert_eq!(source, s("cli"));
    }

    #[test]
    fn env_overrides_config_below_cli() {
        let global = config(Some("global-org"), Some("global-proj"));
        let local = config(Some("local-org"), Some("local-proj"));
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));

        let (org, project, source) = resolve_config(
            ConfigOverrides {
                env_org: s("env-org"),
                env_project: s("env-proj"),
                ..Default::default()
            },
            &global,
            &local,
            &local_path,
            &global_path,
        );

        assert_eq!(org, s("env-org"));
        assert_eq!(project, s("env-proj"));
        assert_eq!(source, s("env"));
    }

    #[test]
    fn local_overrides_global() {
        let global = config(Some("global-org"), Some("global-proj"));
        let local = config(Some("local-org"), Some("local-proj"));
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));

        let (org, project, source) = resolve_config(
            ConfigOverrides::default(),
            &global,
            &local,
            &local_path,
            &global_path,
        );

        assert_eq!(org, s("local-org"));
        assert_eq!(project, s("local-proj"));
        assert_eq!(source, s("/project/.bt/config.json"));
    }

    #[test]
    fn global_used_when_local_empty() {
        let global = config(Some("global-org"), Some("global-proj"));
        let local = config(None, None);
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));

        let (org, project, source) = resolve_config(
            ConfigOverrides::default(),
            &global,
            &local,
            &local_path,
            &global_path,
        );

        assert_eq!(org, s("global-org"));
        assert_eq!(project, s("global-proj"));
        assert_eq!(source, s("/home/.bt/config.json"));
    }

    #[test]
    fn no_source_when_all_empty() {
        let global = config(None, None);
        let local = config(None, None);
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));

        let (org, project, source) = resolve_config(
            ConfigOverrides::default(),
            &global,
            &local,
            &local_path,
            &global_path,
        );

        assert_eq!(org, None);
        assert_eq!(project, None);
        assert_eq!(source, None);
    }

    #[test]
    fn mixed_sources_org_cli_project_local() {
        let global = config(Some("global-org"), Some("global-proj"));
        let local = config(None, Some("local-proj"));
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));

        let (org, project, source) = resolve_config(
            ConfigOverrides {
                cli_org: s("cli-org"),
                ..Default::default()
            },
            &global,
            &local,
            &local_path,
            &global_path,
        );

        assert_eq!(org, s("cli-org"));
        assert_eq!(project, s("local-proj"));
        assert_eq!(source, s("cli"));
    }

    #[test]
    fn values_cascade_across_layers() {
        let global = config(Some("global-org"), None);
        let local = config(None, Some("local-proj"));
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));

        let (org, project, source) = resolve_config(
            ConfigOverrides::default(),
            &global,
            &local,
            &local_path,
            &global_path,
        );

        assert_eq!(org, s("global-org"));
        assert_eq!(project, s("local-proj"));
        assert_eq!(source, s("/project/.bt/config.json"));
    }

    fn profile(
        user_name: Option<&str>,
        email: Option<&str>,
        api_key_hint: Option<&str>,
    ) -> auth::ProfileInfo {
        auth::ProfileInfo {
            auth_method: if api_key_hint.is_some() {
                "api_key"
            } else {
                "oauth"
            }
            .into(),
            org_name: None,
            user_name: user_name.map(Into::into),
            email: email.map(Into::into),
            api_key_hint: api_key_hint.map(Into::into),
        }
    }

    #[test]
    fn format_identity_name_and_email() {
        let p = profile(Some("Alice"), Some("alice@example.com"), None);
        assert_eq!(
            format_identity(&p),
            Some("Alice (alice@example.com)".into())
        );
    }

    #[test]
    fn format_identity_email_only() {
        let p = profile(None, Some("alice@example.com"), None);
        assert_eq!(format_identity(&p), Some("alice@example.com".into()));
    }

    #[test]
    fn format_identity_api_key_hint() {
        let p = profile(None, None, Some("sk-****zhJwO"));
        assert_eq!(format_identity(&p), Some("sk-****zhJwO".into()));
    }

    #[test]
    fn format_identity_none() {
        let p = profile(None, None, None);
        assert_eq!(format_identity(&p), None);
    }
}
