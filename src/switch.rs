use std::io::IsTerminal;

use anyhow::{bail, Result};
use clap::Args;

use crate::args::BaseArgs;
use crate::config;
use crate::http::ApiClient;
use crate::login::login;
use crate::projects::api;
use crate::ui::{self, print_command_status, with_spinner, CommandStatus};

#[derive(Debug, Clone, Args)]
pub struct SwitchArgs {
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

    // Priority: positional target > --project flag/env > interactive
    let project_name = if let Some(t) = &args.target {
        if t.contains('/') {
            // org/project format - ignore org part for now, just use project
            let project = t.split('/').nth(1).unwrap();
            validate_or_create_project(&client, project).await?
        } else {
            validate_or_create_project(&client, t).await?
        }
    } else if let Some(name) = &base.project {
        validate_or_create_project(&client, name).await?
    } else {
        select_project_interactive(&client).await?
    };

    // Save to config
    let mut cfg = config::load_global().unwrap_or_default();
    cfg.org = Some(org_name.to_string());
    cfg.project = Some(project_name.clone());
    config::save_global(&cfg)?;

    let path = config::global_path()?;
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

async fn select_project_interactive(client: &ApiClient) -> Result<String> {
    let mut projects = with_spinner("Loading projects...", api::list_projects(client)).await?;

    if projects.is_empty() {
        bail!("no projects found in org '{}'", &client.org_name());
    }

    projects.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = projects.iter().map(|p| p.name.as_str()).collect();

    let selection = ui::fuzzy_select("Select project", &names)?;
    Ok(projects[selection].name.clone())
}
