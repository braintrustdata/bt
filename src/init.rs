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
    let bt_dir = std::env::current_dir()?.join(".bt");

    if bt_dir.exists() {
        print_command_status(CommandStatus::Warning, "Already Initialized");
        return Ok(());
    }

    let (org, project) = if let (Some(o), Some(p)) = (&base.org, &base.project) {
        (o.clone(), p.clone())
    } else if !std::io::stdin().is_terminal() {
        bail!("--org and --project required in non-interactive mode");
    } else {
        let ctx = login(&base).await?;
        let client = ApiClient::new(&ctx)?;

        let org = client.org_name().to_string();
        let project = select_project_interactive(&client).await?;

        (org, project)
    };

    std::fs::create_dir_all(&bt_dir)?;

    let cfg = config::Config {
        org: Some(org.clone()),
        project: Some(project.clone()),
        ..Default::default()
    };
    config::save_local(&cfg, false)?;

    print_command_status(
        CommandStatus::Success,
        &format!("Initialized with {org}/{project}"),
    );
    eprintln!("Created .bt/config.json");

    Ok(())
}
