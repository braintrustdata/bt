use std::io::IsTerminal;

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
            // org/project format - ignore org part for now, just use project
            let project = t.split('/').nth(1).unwrap();
            validate_or_create_project(&client, project).await?
        } else {
            validate_or_create_project(&client, t).await?
        }
    } else {
        select_project_interactive(&client, None).await?
    };

    // Save to config
    let mut cfg = config::load_global().unwrap_or_default();
    cfg.org = Some(org_name.to_string());
    cfg.project = Some(project_name.clone());

    let path = if args.global {
        config::save_global(&cfg)?;
        config::global_path()?
    } else if args.local {
        config::save_local(&cfg, true)?;
        std::env::current_dir()?.join(".bt/config.json")
    } else {
        match write_target()? {
            WriteTarget::Local(p) => {
                config::save_file(&p, &cfg)?;
                p
            }
            WriteTarget::Global(p) => {
                config::save_file(&p, &cfg)?;
                p
            }
        }
    };

    print_command_status(
        CommandStatus::Success,
        &format!("Switched to {org_name}/{project_name}"),
    );
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
