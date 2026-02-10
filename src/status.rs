use anyhow::Result;
use clap::Args;
use serde::Serialize;

use crate::args::BaseArgs;
use crate::config;

#[derive(Debug, Clone, Args)]
pub struct StatusArgs {}

#[derive(Serialize)]
struct StatusOutput {
    org: Option<String>,
    project: Option<String>,
    source: Option<String>,
}

pub async fn run(base: BaseArgs, _args: StatusArgs) -> Result<()> {
    let global_path = config::global_path().ok();
    let global_cfg = config::load_global().unwrap_or_default();
    let local_path = config::local_path();
    let local_cfg = local_path
        .as_ref()
        .map(|p| config::load_file(p))
        .unwrap_or_default();

    // Resolve values with priority: CLI > local > global
    let (org, project, source) =
        resolve_config(&base, &global_cfg, &local_cfg, &local_path, &global_path);

    if base.json {
        let output = StatusOutput {
            org,
            project,
            source,
        };
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("org: {}", org.as_deref().unwrap_or("(not set)"));
        println!("project: {}", project.as_deref().unwrap_or("(not set)"));
        if let Some(src) = source {
            println!("source: {src}");
        }
    }

    Ok(())
}

fn resolve_config(
    base: &BaseArgs,
    global: &config::Config,
    local: &config::Config,
    local_path: &Option<std::path::PathBuf>,
    global_path: &Option<std::path::PathBuf>,
) -> (Option<String>, Option<String>, Option<String>) {
    // Priority: CLI flags > local config > global config
    let org = base
        .org
        .clone()
        .or_else(|| local.org.clone())
        .or_else(|| global.org.clone());

    let project = base
        .project
        .clone()
        .or_else(|| local.project.clone())
        .or_else(|| global.project.clone());

    // Determine source based on where the values came from
    let source = if base.org.is_some() || base.project.is_some() {
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
