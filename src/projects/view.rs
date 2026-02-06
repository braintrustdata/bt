use std::io::IsTerminal;

use anyhow::{bail, Result};
use reqwest::Client;
use urlencoding::encode;

use crate::login::LoginContext;
use crate::ui::{print_command_status, with_spinner, CommandStatus};

use super::api;
use super::switch::select_project_interactive;

pub async fn run(http: &Client, ctx: &LoginContext, name: Option<&str>) -> Result<()> {
    let project_name = match name {
        Some(n) => n.to_string(),
        None => {
            if !std::io::stdin().is_terminal() {
                bail!("Must specify a project in non-TTY mode")
            }
            select_project_interactive(http, ctx).await?
        }
    };

    // Verify project exists
    let exists = with_spinner(
        "Loading project...",
        api::get_project_by_name(http, ctx, &project_name),
    )
    .await?;
    if exists.is_none() {
        bail!("project '{}' not found", project_name);
    }

    let url = format!(
        "{}/app/{}/p/{}",
        ctx.app_url.trim_end_matches('/'),
        encode(&ctx.login.org_name),
        encode(&project_name)
    );

    open::that(&url)?;
    print_command_status(
        CommandStatus::Success,
        &format!("Opened {} in browser", url),
    );

    Ok(())
}
