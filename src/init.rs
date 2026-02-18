use std::io::IsTerminal;

use anyhow::{bail, Result};
use clap::Args;

use crate::{
    args::BaseArgs,
    auth::login,
    config,
    http::ApiClient,
    ui::{print_command_status, select_project_interactive, CommandStatus},
};

#[derive(Debug, Clone, Args)]
pub struct InitArgs {}

pub async fn run(base: BaseArgs, _args: InitArgs) -> Result<()> {
    let bt_dir = std::env::current_dir()?.join(".bt");
    if bt_dir.join("config.json").exists() {
        print_command_status(CommandStatus::Warning, "Already Initialized");
        return Ok(());
    }

    eprintln!("Link to a Braintrust project...");

    let (org, project) = if let (Some(o), Some(p)) = (&base.org_name, &base.project) {
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

    let cfg = config::Config {
        org: Some(org.clone()),
        project: Some(project.clone()),
        ..Default::default()
    };

    config::save_local(&cfg, true)?;

    print_command_status(
        CommandStatus::Success,
        &format!("Project linked to {org}/{project}"),
    );
    print_command_status(CommandStatus::Success, "Created .bt/config.json");

    Ok(())
}
