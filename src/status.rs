use anyhow::Result;
use clap::Args;
use serde::Serialize;

use crate::args::{BaseArgs, DEFAULT_API_URL, DEFAULT_APP_URL};
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
    org_id: Option<String>,
    project: Option<String>,
    project_id: Option<String>,
    app_url: Option<String>,
    api_url: Option<String>,
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

fn format_auth(p: &auth::ProfileInfo) -> String {
    auth::identity_label(
        p.user_name.as_deref(),
        p.email.as_deref(),
        p.api_key_hint.as_deref(),
    )
    .map(|identity| format!("{} — {identity}", p.auth_method))
    .unwrap_or_else(|| p.auth_method.clone())
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
    let auth_info = auth::active_auth_info(&base, org.as_deref())?;

    if base
        .project
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
    {
        project = config::project_from_config_for_context(&base, &merged_cfg, org.as_deref());
    }

    let org_id = if base.org_name_source.is_some() {
        None
    } else {
        base.org_id.clone()
    };
    let configured_project =
        config::project_from_config_for_context(&base, &merged_cfg, org.as_deref());
    let project_id = (base.project_source.is_none()
        && configured_project.is_some()
        && configured_project == project)
        .then(|| merged_cfg.project_id.clone())
        .flatten();
    let app_url = Some(
        base.app_url
            .clone()
            .or_else(|| merged_cfg.app_url.clone())
            .unwrap_or_else(|| DEFAULT_APP_URL.to_string()),
    );
    let api_url = Some(
        base.api_url
            .clone()
            .or_else(|| merged_cfg.api_url.clone())
            .unwrap_or_else(|| DEFAULT_API_URL.to_string()),
    );
    if base.json {
        let output = StatusOutput {
            org: org.clone(),
            org_id,
            project,
            project_id,
            app_url,
            api_url,
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
        println!("org: {}", org.as_deref().unwrap_or("(unset)"));
        println!("org_id: {}", org_id.as_deref().unwrap_or("(unset)"));
        println!("project: {}", project.as_deref().unwrap_or("(unset)"));
        println!("project_id: {}", project_id.as_deref().unwrap_or("(unset)"));
        println!("app_url: {}", app_url.as_deref().unwrap_or(DEFAULT_APP_URL));
        println!("api_url: {}", api_url.as_deref().unwrap_or(DEFAULT_API_URL));
        if let Some(ref p) = auth_info {
            println!("auth: {}", format_auth(p));
        }
        if let Some(src) = source {
            println!("source: {src}");
        }
    } else {
        let header = match org.as_deref() {
            Some(org) => match project.as_deref() {
                Some(project) => format!("{org}/{project}"),
                None => org.to_string(),
            },
            None if auth_info.is_some() => "No default org".to_string(),
            None => "No org/project configured. Run `bt switch` to set one.".to_string(),
        };
        println!("{header}");
        match &auth_info {
            Some(p) => println!("  auth: {}", format_auth(p)),
            None if org.is_some() => println!("  auth: (none)"),
            None => {}
        }
    }

    Ok(())
}

#[derive(Default)]
pub(crate) struct ConfigOverrides {
    cli_org: Option<String>,
    env_org: Option<String>,
    cli_project: Option<String>,
    env_project: Option<String>,
    cli_app_url: Option<String>,
    env_app_url: Option<String>,
    cli_api_url: Option<String>,
    env_api_url: Option<String>,
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
        let (cli_app_url, env_app_url) = match base.app_url_source {
            Some(ArgValueSource::CommandLine) => (base.app_url.clone(), None),
            Some(ArgValueSource::EnvVariable) => (None, base.app_url.clone()),
            None => (None, None),
        };
        let (cli_api_url, env_api_url) = match base.api_url_source {
            Some(ArgValueSource::CommandLine) => (base.api_url.clone(), None),
            Some(ArgValueSource::EnvVariable) => (None, base.api_url.clone()),
            None => (None, None),
        };

        Self {
            cli_org,
            env_org,
            cli_project,
            env_project,
            cli_app_url,
            env_app_url,
            cli_api_url,
            env_api_url,
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
        cli_app_url,
        env_app_url,
        cli_api_url,
        env_api_url,
    } = overrides;
    let env_project = env_project.filter(|s| !s.is_empty());
    let merged = global.merge(local);
    let app_override = cli_app_url.as_deref().or(env_app_url.as_deref());
    let config_app = merged.app_url.as_deref().unwrap_or(DEFAULT_APP_URL);
    let same_instance = app_override.is_none_or(|app| config::urls_equal(app, config_app));
    let config_org = same_instance.then(|| merged.org.clone()).flatten();
    let config_project = same_instance.then(|| merged.project.clone()).flatten();
    let org = cli_org.clone().or_else(|| env_org.clone()).or(config_org);

    let project = cli_project
        .clone()
        .or_else(|| env_project.clone())
        .or(config_project);

    let source = if cli_org.is_some()
        || cli_project.is_some()
        || cli_app_url.is_some()
        || cli_api_url.is_some()
    {
        Some("cli".to_string())
    } else if env_org.is_some()
        || env_project.is_some()
        || env_app_url.is_some()
        || env_api_url.is_some()
    {
        Some("env".to_string())
    } else if local.org.is_some()
        || local.project.is_some()
        || local.app_url.is_some()
        || local.api_url.is_some()
    {
        local_path.as_ref().map(|p| p.display().to_string())
    } else if global.org.is_some()
        || global.project.is_some()
        || global.app_url.is_some()
        || global.api_url.is_some()
    {
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
    fn config_precedence_and_context_safety() {
        let both = || config(Some("global-org"), Some("global-proj"));
        let cases = [
            (
                "cli",
                ConfigOverrides {
                    cli_org: s("cli-org"),
                    cli_project: s("cli-proj"),
                    ..Default::default()
                },
                both(),
                config(Some("local-org"), Some("local-proj")),
                (Some("cli-org"), Some("cli-proj"), Some("cli")),
            ),
            (
                "env",
                ConfigOverrides {
                    env_org: s("env-org"),
                    env_project: s("env-proj"),
                    ..Default::default()
                },
                both(),
                config(Some("local-org"), Some("local-proj")),
                (Some("env-org"), Some("env-proj"), Some("env")),
            ),
            (
                "local",
                ConfigOverrides::default(),
                both(),
                config(Some("local-org"), Some("local-proj")),
                (
                    Some("local-org"),
                    Some("local-proj"),
                    Some("/project/.bt/config.json"),
                ),
            ),
            (
                "global",
                ConfigOverrides::default(),
                both(),
                config(None, None),
                (
                    Some("global-org"),
                    Some("global-proj"),
                    Some("/home/.bt/config.json"),
                ),
            ),
            (
                "empty",
                ConfigOverrides::default(),
                config(None, None),
                config(None, None),
                (None, None, None),
            ),
            (
                "app override does not inherit another instance's context",
                ConfigOverrides {
                    cli_app_url: s("https://other.example.test"),
                    ..Default::default()
                },
                both(),
                config(None, None),
                (None, None, Some("cli")),
            ),
            (
                "mixed cli/local",
                ConfigOverrides {
                    cli_org: s("cli-org"),
                    ..Default::default()
                },
                both(),
                config(None, Some("local-proj")),
                (Some("cli-org"), Some("local-proj"), Some("cli")),
            ),
            (
                "local project does not inherit global org",
                ConfigOverrides::default(),
                config(Some("global-org"), None),
                config(None, Some("local-proj")),
                (None, Some("local-proj"), Some("/project/.bt/config.json")),
            ),
        ];
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));
        for (name, overrides, global, local, expected) in cases {
            let actual = resolve_config(overrides, &global, &local, &local_path, &global_path);
            assert_eq!(
                actual,
                (
                    expected.0.map(str::to_string),
                    expected.1.map(str::to_string),
                    expected.2.map(str::to_string),
                ),
                "{name}"
            );
        }
    }
}
