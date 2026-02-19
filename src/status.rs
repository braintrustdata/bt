use anyhow::Result;
use clap::Args;
use serde::Serialize;

use crate::args::BaseArgs;
use crate::auth;
use crate::config;

#[derive(Debug, Clone, Args)]
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
    source: Option<String>,
}

pub async fn run(base: BaseArgs, args: StatusArgs) -> Result<()> {
    let global_path = config::global_path().ok();
    let global_cfg = config::load_global().unwrap_or_default();
    let local_path = config::local_path();
    let local_cfg = local_path
        .as_ref()
        .map(|p| config::load_file(p))
        .unwrap_or_default();

    let (org, project, source) =
        resolve_config(&base, &global_cfg, &local_cfg, &local_path, &global_path);
    let profile = resolve_profile(org.as_deref());

    if base.json {
        let output = StatusOutput {
            org,
            project,
            profile,
            source,
        };
        println!("{}", serde_json::to_string(&output)?);
        return Ok(());
    }

    if args.verbose {
        println!("org: {}", org.as_deref().unwrap_or("(unset)"));
        println!("project: {}", project.as_deref().unwrap_or("(unset)"));
        if let Some(ref p) = profile {
            println!("profile: {p}");
        }
        if let Some(src) = source {
            println!("source: {src}");
        }
    } else {
        match (&org, &project) {
            (Some(o), Some(p)) => println!("{o}/{p}"),
            _ => println!("No org/project configured"),
        }
    }

    Ok(())
}

pub(crate) fn resolve_config(
    base: &BaseArgs,
    global: &config::Config,
    local: &config::Config,
    local_path: &Option<std::path::PathBuf>,
    global_path: &Option<std::path::PathBuf>,
) -> (Option<String>, Option<String>, Option<String>) {
    let org = base
        .org_name
        .clone()
        .or_else(|| local.org.clone())
        .or_else(|| global.org.clone());

    let project = base
        .project
        .clone()
        .or_else(|| local.project.clone())
        .or_else(|| global.project.clone());

    let source = if base.org_name.is_some() || base.project.is_some() {
        Some("cli".to_string())
    } else if local.org.is_some() || local.project.is_some() {
        local_path.as_ref().map(|p| p.display().to_string())
    } else if global.org.is_some() || global.project.is_some() {
        global_path.as_ref().map(|p| p.display().to_string())
    } else {
        None
    };

    (org, project, source)
}

fn resolve_profile(org: Option<&str>) -> Option<String> {
    let o = org?;
    let profiles = auth::list_profiles().ok()?;
    profiles
        .iter()
        .find(|p| p.name == o)
        .or_else(|| {
            let by_org: Vec<_> = profiles
                .iter()
                .filter(|p| p.org_name.as_deref() == Some(o))
                .collect();
            (by_org.len() == 1).then(|| by_org[0])
        })
        .map(|p| p.name.clone())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

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

    fn config(org: Option<&str>, project: Option<&str>) -> config::Config {
        config::Config {
            org: org.map(String::from),
            project: project.map(String::from),
            ..Default::default()
        }
    }

    #[test]
    fn cli_overrides_everything() {
        let base = base_args(Some("cli-org"), Some("cli-proj"));
        let global = config(Some("global-org"), Some("global-proj"));
        let local = config(Some("local-org"), Some("local-proj"));
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));

        let (org, project, source) =
            resolve_config(&base, &global, &local, &local_path, &global_path);

        assert_eq!(org, Some("cli-org".into()));
        assert_eq!(project, Some("cli-proj".into()));
        assert_eq!(source, Some("cli".into()));
    }

    #[test]
    fn local_overrides_global() {
        let base = base_args(None, None);
        let global = config(Some("global-org"), Some("global-proj"));
        let local = config(Some("local-org"), Some("local-proj"));
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));

        let (org, project, source) =
            resolve_config(&base, &global, &local, &local_path, &global_path);

        assert_eq!(org, Some("local-org".into()));
        assert_eq!(project, Some("local-proj".into()));
        assert_eq!(source, Some("/project/.bt/config.json".into()));
    }

    #[test]
    fn global_used_when_local_empty() {
        let base = base_args(None, None);
        let global = config(Some("global-org"), Some("global-proj"));
        let local = config(None, None);
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));

        let (org, project, source) =
            resolve_config(&base, &global, &local, &local_path, &global_path);

        assert_eq!(org, Some("global-org".into()));
        assert_eq!(project, Some("global-proj".into()));
        assert_eq!(source, Some("/home/.bt/config.json".into()));
    }

    #[test]
    fn no_source_when_all_empty() {
        let base = base_args(None, None);
        let global = config(None, None);
        let local = config(None, None);
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));

        let (org, project, source) =
            resolve_config(&base, &global, &local, &local_path, &global_path);

        assert_eq!(org, None);
        assert_eq!(project, None);
        assert_eq!(source, None);
    }

    #[test]
    fn mixed_sources_org_cli_project_local() {
        let base = base_args(Some("cli-org"), None);
        let global = config(Some("global-org"), Some("global-proj"));
        let local = config(None, Some("local-proj"));
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));

        let (org, project, source) =
            resolve_config(&base, &global, &local, &local_path, &global_path);

        assert_eq!(org, Some("cli-org".into()));
        assert_eq!(project, Some("local-proj".into()));
        assert_eq!(source, Some("cli".into()));
    }

    #[test]
    fn values_cascade_across_layers() {
        let base = base_args(None, None);
        let global = config(Some("global-org"), None);
        let local = config(None, Some("local-proj"));
        let local_path = Some(PathBuf::from("/project/.bt/config.json"));
        let global_path = Some(PathBuf::from("/home/.bt/config.json"));

        let (org, project, source) =
            resolve_config(&base, &global, &local, &local_path, &global_path);

        assert_eq!(org, Some("global-org".into()));
        assert_eq!(project, Some("local-proj".into()));
        // Source reports local since that's where the highest-priority non-empty value came from
        assert_eq!(source, Some("/project/.bt/config.json".into()));
    }
}
