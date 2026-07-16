use anyhow::{bail, Result};
use clap::Args;

use crate::{
    args::BaseArgs,
    auth::{self, login},
    config,
    http::ApiClient,
    ui::{is_interactive, print_command_status, select_project, CommandStatus, ProjectSelectMode},
};

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt init
  bt init --org acme --project my-app
")]
pub struct InitArgs {}

fn select_saved_auth_org_for_init() -> Result<Option<String>> {
    let mut orgs = auth::list_profiles()?
        .into_iter()
        .filter_map(|profile| profile.org_name)
        .filter(|org| !org.trim().is_empty())
        .collect::<Vec<_>>();
    orgs.sort();
    orgs.dedup();

    match orgs.len() {
        0 => Ok(None),
        1 => Ok(orgs.into_iter().next()),
        _ => {
            let labels = orgs.iter().map(String::as_str).collect::<Vec<_>>();
            let idx = crate::ui::fuzzy_select("Select organization", &labels, 0)?;
            Ok(Some(orgs[idx].clone()))
        }
    }
}

pub async fn run(base: BaseArgs, _args: InitArgs) -> Result<()> {
    let config_path = config::local_save_path()?;
    if config_path.exists() {
        if base.json {
            let existing = config::load_file(&config_path);
            let payload = serde_json::json!({
                "initialized": false,
                "status": "already-initialized",
                "org": existing.org,
                "project": existing.project,
                "path": config_path.display().to_string(),
            });
            println!("{}", serde_json::to_string(&payload)?);
        } else {
            print_command_status(CommandStatus::Warning, "Already Initialized");
        }
        return Ok(());
    }

    eprintln!("Link to a Braintrust project...");

    let (org, project) = if let (Some(o), Some(p)) = (&base.org_name, &base.project) {
        (o.clone(), p.clone())
    } else if !is_interactive() {
        bail!("--org and --project required in non-interactive mode");
    } else {
        let mut login_base = base.clone();
        if login_base.org_name.is_none() {
            login_base.org_name = config::load().ok().and_then(|cfg| cfg.org);
        }
        if login_base.org_name.is_none() {
            login_base.org_name = select_saved_auth_org_for_init()?;
        }
        let ctx = login(&login_base).await?;
        let client = ApiClient::new(&ctx)?;

        let org = client.org_name().to_string();
        let project = select_project(
            &client,
            None,
            Some("Link to project"),
            ProjectSelectMode::ExistingOnly,
        )
        .await?
        .name;

        (org, project)
    };

    let cfg = config::Config {
        org: Some(org.clone()),
        project: Some(project.clone()),
        ..Default::default()
    };

    let written_path = config::save_local(&cfg, true)?;

    if base.json {
        let payload = serde_json::json!({
            "initialized": true,
            "status": "created",
            "org": org,
            "project": project,
            "path": written_path.display().to_string(),
        });
        println!("{}", serde_json::to_string(&payload)?);
    } else {
        print_command_status(
            CommandStatus::Success,
            &format!("Project linked to {org}/{project}"),
        );
        print_command_status(CommandStatus::Success, "Created .bt/config.json");
    }

    Ok(())
}
