use std::io::IsTerminal;

use anyhow::{bail, Result};
use clap::Args;

use crate::{
    args::BaseArgs,
    config,
    http::ApiClient,
    login::login,
    ui::{print_command_status, select_project_interactive, CommandStatus},
};

#[derive(Debug, Clone, Args)]
pub struct InitArgs {}

pub async fn run(base: BaseArgs, _args: InitArgs) -> Result<()> {
    println!("Link to a Braintrust project...");

    let (org, project) = if let (Some(o), Some(p)) = (&base.org, &base.project) {
        (o.clone(), p.clone())
    } else if !std::io::stdin().is_terminal() {
        bail!("--org and --project required in non-interactive mode");
    } else {
        let ctx = login(&base).await?;
        let client = ApiClient::new(&ctx)?;

        let org = client.org_name().to_string();
        let project = select_project_interactive(&client, Some("Link to project")).await?;

        (org, project)
    };

    let bt_dir = std::env::current_dir()?.join(".bt");
    let bt_config = &bt_dir.join("config.json");

    if bt_config.exists() {
        print_command_status(CommandStatus::Warning, "Already Initialized");
        return Ok(());
    }

    std::fs::create_dir_all(&bt_dir)?;

    let cfg = config::Config {
        org: Some(org.clone()),
        project: Some(project.clone()),
        ..Default::default()
    };
    config::save_local(&cfg, false)?;

    print_command_status(
        CommandStatus::Success,
        &format!("Project linked to {org}/{project}"),
    );
    print_command_status(CommandStatus::Success, "Created .bt/config.json");

    Ok(())
}
