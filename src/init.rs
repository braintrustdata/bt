use anyhow::{Context, Result};
use clap::Args;

use crate::{
    args::{ArgValueSource, BaseArgs, DEFAULT_API_URL, DEFAULT_APP_URL},
    config, switch,
    ui::{print_command_status, CommandStatus},
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
    let requested_org = matches!(
        base.org_name_source,
        Some(ArgValueSource::CommandLine | ArgValueSource::EnvVariable)
    )
    .then(|| base.org_name.as_deref())
    .flatten();
    let (instance, org, project) = switch::select_context(
        &base,
        requested_org,
        base.project.as_deref(),
        &current_cfg,
        Some("Link to project"),
    )
    .await?;
    let api_url = if matches!(
        base.api_url_source,
        Some(ArgValueSource::CommandLine | ArgValueSource::EnvVariable)
    ) {
        base.api_url.clone()
    } else {
        org.api_url.clone().or_else(|| {
            config::urls_equal(
                current_cfg.app_url.as_deref().unwrap_or(DEFAULT_APP_URL),
                &instance.app_url,
            )
            .then(|| current_cfg.api_url.clone())
            .flatten()
        })
    }
    .unwrap_or_else(|| DEFAULT_API_URL.to_string());

    // With --force, preserve unknown passthrough keys from the old file.
    let mut cfg = config::load_file(&config_path);
    cfg.set_context(
        (org.name.as_str(), org.id.as_str()),
        Some((project.name.as_str(), project.id.as_str())),
        &instance.app_url,
        &api_url,
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
            "org": org.name,
            "org_id": org.id,
            "project": project.name,
            "project_id": project.id,
            "app_url": instance.app_url,
            "api_url": api_url,
            "path": config_path.display().to_string(),
        });
        println!("{}", serde_json::to_string(&payload)?);
    } else {
        print_command_status(
            CommandStatus::Success,
            &format!("Project linked to {}/{}", org.name, project.name),
        );
        print_command_status(
            CommandStatus::Success,
            &format!("Created {}", config_path.display()),
        );
    }

    Ok(())
}
