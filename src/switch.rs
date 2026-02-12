use std::io::IsTerminal;
use std::path::PathBuf;

use anyhow::{bail, Result};
use clap::Args;

use crate::args::BaseArgs;
use crate::config::{self, write_target, WriteTarget};
use crate::http::ApiClient;
use crate::login::login;
use crate::projects::api;
use crate::ui::{print_command_status, select_project_interactive, with_spinner, CommandStatus};

#[derive(Debug, Clone, Args)]
pub struct SwitchArgs {
    /// force set global config value
    #[arg(long, short = 'g', conflicts_with = "local")]
    global: bool,
    /// force set local config value
    #[arg(long, short = 'l')]
    local: bool,
    /// Target: project name or org/project
    #[arg(value_name = "TARGET")]
    target: Option<String>,
}

pub async fn run(base: BaseArgs, args: SwitchArgs) -> Result<()> {
    let ctx = login(&base).await?;
    let client = ApiClient::new(&ctx)?;
    // For now, always use org from API client
    // TODO: support org switching when multi-org auth is ready
    let org_name = &client.org_name();

    if args.target.is_none() && base.org.is_none() && base.project.is_none() {
        let cfg = config::load().unwrap_or_default();
        if let (Some(org), Some(project)) = (&cfg.org, &cfg.project) {
            eprintln!("Current: {org}/{project}");
        } else {
            eprintln!("No org/project configured");
        }
    }

    let project_name = if let Some(t) = &args.target {
        if t.contains('/') {
            // parse org/project format
            let parts: Vec<&str> = t.splitn(2, '/').collect();
            if parts.len() != 2 || parts[0].is_empty() || parts[1].is_empty() {
                bail!("invalid target format. expected org/project");
            }
            // ignore org part for now, just use project
            let project = parts[1];
            validate_or_create_project(&client, project).await?
        } else {
            validate_or_create_project(&client, t).await?
        }
    } else {
        select_project_interactive(&client, None).await?
    };

    let path = resolve_target_path(args.global, args.local)?;
    let mut cfg = config::load_file(&path);
    cfg.org = Some(org_name.to_string());
    cfg.project = Some(project_name.clone());

    match config::save_file(&path, &cfg) {
        Ok(_) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Switched to {org_name}/{project_name}"),
            );
            // TODO: Only show in --verbose mode
            eprintln!("Wrote to {}", path.display());
        }
        Err(_) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Could not switch to {org_name}/{project_name}"),
            );
        }
    }

    Ok(())
}

fn resolve_target_path(global: bool, local: bool) -> Result<PathBuf> {
    if global {
        config::global_path()
    } else if local {
        let dir = std::env::current_dir()?.join(".bt");
        std::fs::create_dir_all(&dir)?;

        Ok(dir.join("config.json"))
    } else {
        match write_target()? {
            WriteTarget::Local(p) | WriteTarget::Global(p) => Ok(p),
        }
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
