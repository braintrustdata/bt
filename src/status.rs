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
pub struct StatusArgs {
    /// Output verbose status
    #[arg(long)]
    pub verbose: bool,
}

#[derive(Serialize)]
struct StatusOutput {
    org: Option<String>,
    project: Option<String>,
    profile: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    user_email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    api_key_hint: Option<String>,
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

pub async fn run(base: BaseArgs, args: StatusArgs) -> Result<()> {
    let global_path = config::global_path().ok();
    let global_cfg = config::load_global().unwrap_or_default();
    let local_path = config::local_path();
    let local_cfg = local_path
        .as_ref()
        .map(|p| config::load_file(p))
        .unwrap_or_default();

    let cli_org = cli_flag_value(&["--org", "-o"]);
    let cli_project = cli_flag_value(&["--project", "-p"]);
    let (org, project, source) = resolve_config(
        cli_org,
        cli_project,
        &global_cfg,
        &local_cfg,
        &local_path,
        &global_path,
    );
    let profile_info = resolve_profile_info(base.profile.as_deref(), org.as_deref());

    if base.json {
        let output = StatusOutput {
            org,
            project,
            profile: profile_info.as_ref().map(|p| p.name.clone()),
            user_name: profile_info.as_ref().and_then(|p| p.user_name.clone()),
            user_email: profile_info.as_ref().and_then(|p| p.email.clone()),
            api_key_hint: profile_info.as_ref().and_then(|p| p.api_key_hint.clone()),
            source,
        };
        println!("{}", serde_json::to_string(&output)?);
        return Ok(());
    }

    if args.verbose {
        println!("org: {}", org.as_deref().unwrap_or("(unset)"));
        println!("project: {}", project.as_deref().unwrap_or("(unset)"));
        if let Some(ref p) = profile_info {
            println!("profile: {}", p.name);
            if let Some(id) = format_identity(p) {
                println!("user: {id}");
            }
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
        let profile_line = match &profile_info {
            Some(p) => match format_identity(p) {
                Some(id) => format!("  profile: {} — {id}", p.name),
                None => format!("  profile: {}", p.name),
            },
            None => "  profile: (none)".to_string(),
        };
        println!("{profile_line}");
    } else {
        println!("No org/project configured. Run `bt switch` to set one.");
    }

    Ok(())
}

/// Precedence (clig.dev): CLI flag > env var > local config > global config.
pub(crate) fn resolve_config(
    cli_org: Option<String>,
    cli_project: Option<String>,
    global: &config::Config,
    local: &config::Config,
    local_path: &Option<std::path::PathBuf>,
    global_path: &Option<std::path::PathBuf>,
) -> (Option<String>, Option<String>, Option<String>) {
    let env_org = std::env::var("BRAINTRUST_ORG_NAME")
        .ok()
        .filter(|s| !s.is_empty());
    let env_project = std::env::var("BRAINTRUST_DEFAULT_PROJECT")
        .ok()
        .filter(|s| !s.is_empty());

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

fn cli_flag_value(flags: &[&str]) -> Option<String> {
    let args: Vec<String> = std::env::args().collect();
    for (i, arg) in args.iter().enumerate() {
        if arg == "--" {
            break;
        }
        for flag in flags {
            if arg == *flag {
                return args.get(i + 1).cloned();
            }
            if let Some(val) = arg.strip_prefix(&format!("{flag}=")) {
                return Some(val.to_string());
            }
            // Handle -oVALUE style (short flags only)
            if flag.len() == 2 && flag.starts_with('-') {
                if let Some(val) = arg.strip_prefix(flag) {
                    if !val.is_empty() {
                        return Some(val.to_string());
                    }
                }
            }
        }
    }
    None
}

fn resolve_profile_info(profile: Option<&str>, org: Option<&str>) -> Option<auth::ProfileInfo> {
    let profiles = auth::list_profiles().ok()?;

    if let Some(p) = profile {
        return profiles.into_iter().find(|pi| pi.name == p);
    }

    if let Some(o) = org {
        if profiles.iter().any(|pi| pi.name == o) {
            return profiles.into_iter().find(|pi| pi.name == o);
        }
        let org_matches: Vec<&auth::ProfileInfo> = profiles
            .iter()
            .filter(|pi| pi.org_name.as_deref() == Some(o))
            .collect();
        if org_matches.len() == 1 {
            let name = org_matches[0].name.clone();
            return profiles.into_iter().find(|pi| pi.name == name);
        }
        return None;
    }

    if profiles.len() == 1 {
        return profiles.into_iter().next();
    }

    None
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
            s("cli-org"),
            s("cli-proj"),
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
    fn local_overrides_global() {
        let global = config(Some("global-org"), Some("global-proj"));
        let local = config(Some("local-org"), Some("local-proj"));
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));

        let (org, project, source) =
            resolve_config(None, None, &global, &local, &local_path, &global_path);

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

        let (org, project, source) =
            resolve_config(None, None, &global, &local, &local_path, &global_path);

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

        let (org, project, source) =
            resolve_config(None, None, &global, &local, &local_path, &global_path);

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
            s("cli-org"),
            None,
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

        let (org, project, source) =
            resolve_config(None, None, &global, &local, &local_path, &global_path);

        assert_eq!(org, s("global-org"));
        assert_eq!(project, s("local-proj"));
        assert_eq!(source, s("/project/.bt/config.json"));
    }

    fn profile(
        name: &str,
        user_name: Option<&str>,
        email: Option<&str>,
        api_key_hint: Option<&str>,
    ) -> auth::ProfileInfo {
        auth::ProfileInfo {
            name: name.into(),
            org_name: None,
            user_name: user_name.map(Into::into),
            email: email.map(Into::into),
            api_key_hint: api_key_hint.map(Into::into),
            is_oauth: false,
        }
    }

    #[test]
    fn format_identity_name_and_email() {
        let p = profile("work", Some("Alice"), Some("alice@example.com"), None);
        assert_eq!(
            format_identity(&p),
            Some("Alice (alice@example.com)".into())
        );
    }

    #[test]
    fn format_identity_email_only() {
        let p = profile("work", None, Some("alice@example.com"), None);
        assert_eq!(format_identity(&p), Some("alice@example.com".into()));
    }

    #[test]
    fn format_identity_api_key_hint() {
        let p = profile("work", None, None, Some("sk-****zhJwO"));
        assert_eq!(format_identity(&p), Some("sk-****zhJwO".into()));
    }

    #[test]
    fn format_identity_none() {
        let p = profile("work", None, None, None);
        assert_eq!(format_identity(&p), None);
    }
}
