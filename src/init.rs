use anyhow::{bail, Result};
use clap::Args;

use crate::{
    args::BaseArgs,
    auth::{self, login},
    config,
    http::ApiClient,
    ui::{is_interactive, print_command_status, CommandStatus},
};

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt init
  bt init --org acme --project my-app
")]
pub struct InitArgs {}

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
                "project_id": existing.project_id,
                "path": config_path.display().to_string(),
            });
            println!("{}", serde_json::to_string(&payload)?);
        } else {
            print_command_status(CommandStatus::Warning, "Already Initialized");
        }
        return Ok(());
    }

    config::preflight_config_write(&config_path)?;

    eprintln!("Link to a Braintrust project...");

    let (org, project, profile) = if let (Some(_), Some(project_name)) =
        (&base.org_name, &base.project)
    {
        let ctx = login(&base).await?;
        let client = ApiClient::new(&ctx)?;
        let org = client.org_name().to_string();
        let project = crate::switch::resolve_project(&client, Some(project_name), None).await?;
        (org, project, ctx.profile)
    } else if !is_interactive() {
        bail!("--org and --project required in non-interactive mode");
    } else {
        let mut login_base = base.clone();
        if login_base.org_name.is_none() && login_base.profile.is_none() {
            if let Some(profile) = auth::select_profile_interactive(None)? {
                login_base.profile = Some(profile);
            }
        }
        let ctx = if login_base.org_name.is_none() {
            let (options, org) = crate::switch::select_org_for_switch(&login_base, None).await?;
            login_base.org_name = Some(org.name.clone());
            options.login_context(&login_base, &org).await
        } else {
            login(&login_base).await?
        };
        let client = ApiClient::new(&ctx)?;

        let org = client.org_name().to_string();
        let project =
            crate::switch::resolve_project(&client, None, Some("Link to project")).await?;

        (org, project, ctx.profile)
    };

    let mut cfg = config::Config::default();
    crate::switch::apply_switch_config(&mut cfg, profile.as_deref(), Some(&org), Some(&project));

    let written_path = config::save_local(&cfg, true)?;

    if base.json {
        let payload = serde_json::json!({
            "initialized": true,
            "status": "created",
            "org": org,
            "project": project.name,
            "project_id": project.id,
            "path": written_path.display().to_string(),
        });
        println!("{}", serde_json::to_string(&payload)?);
    } else {
        print_command_status(
            CommandStatus::Success,
            &format!("Project linked to {org}/{}", project.name),
        );
        print_command_status(CommandStatus::Success, "Created .bt/config.json");
    }

    Ok(())
}
