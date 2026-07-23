use anyhow::{bail, Context, Result};
use clap::Args;

use crate::{
    args::BaseArgs,
    auth::{self, login},
    config,
    http::ApiClient,
    ui::{print_command_status, select_or_create_project, CommandStatus},
};

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt init
  bt init --org test-org --project test-project
  bt init --here
  bt init --here --force
")]
pub struct InitArgs {
    /// Create .bt/config.json in the current directory without searching upward.
    ///
    /// Bypasses the normal home and filesystem-root search boundaries, so it
    /// also applies when the current directory is ~ or /.
    #[arg(long)]
    here: bool,

    /// Overwrite an existing .bt/config.json. Does not change discovery.
    #[arg(long, short = 'f')]
    force: bool,
}

pub async fn run(base: BaseArgs, args: InitArgs) -> Result<()> {
    let config_path = config::init_target(args.here, args.force)?;
    let current_cfg = config::load().unwrap_or_default();
    let mut login_base = base.clone();
    login_base.project = None;
    login_base.project_source = None;
    if login_base.org_name.is_none()
        && !auth::select_saved_login(&mut login_base, current_cfg.org.as_deref(), false)?
    {
        bail!("no saved concrete-org login is available; run `bt auth login --org <ORG>`");
    }

    let ctx = login(&login_base).await?;
    let client = ApiClient::new(&ctx)?;
    let org = client.org_name().to_string();
    if org.is_empty() {
        bail!(
            "cross-org mode has no project; `bt init` is project-scoped. Rerun with --org <ORG> --project <PROJECT>"
        );
    }

    let project = select_or_create_project(
        &client,
        base.project.as_deref(),
        None,
        Some("Link to project"),
    )
    .await?;
    let mut cfg = config::Config::default();
    cfg.set_context(
        Some(&org),
        Some((project.name.as_str(), project.id.as_str())),
    );

    config::save_file(&config_path, &cfg).with_context(|| {
        format!(
            "authentication succeeded, but initialization failed: could not create or write {}; any credential updates remain saved",
            config_path.display()
        )
    })?;

    if base.json {
        let payload = serde_json::json!({
            "initialized": true,
            "status": "created",
            "org": org,
            "project": project.name,
            "project_id": project.id,
            "path": config_path.display().to_string(),
        });
        println!("{}", serde_json::to_string(&payload)?);
    } else {
        print_command_status(
            CommandStatus::Success,
            &format!("Project linked to {org}/{}", project.name),
        );
        print_command_status(
            CommandStatus::Success,
            &format!("Created {}", config_path.display()),
        );
    }

    Ok(())
}
