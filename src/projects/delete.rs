use std::io::IsTerminal;

use anyhow::{bail, Result};
use dialoguer::Confirm;
use reqwest::Client;

use crate::login::LoginContext;
use crate::ui::with_spinner;

use super::api;
use super::switch::select_project_interactive;

pub async fn run(http: &Client, ctx: &LoginContext, name: Option<&str>) -> Result<()> {
    let project = match name {
        Some(n) => with_spinner("Loading project...", api::get_project_by_name(http, ctx, n))
            .await?
            .ok_or_else(|| anyhow::anyhow!("project '{}' not found", n))?,
        None => {
            if !std::io::stdin().is_terminal() {
                bail!("project name required. Use: bt projects delete <name>");
            }
            let name = select_project_interactive(http, ctx).await?;
            with_spinner("Loading project...", api::get_project_by_name(http, ctx, &name))
                .await?
                .ok_or_else(|| anyhow::anyhow!("project '{}' not found", name))?
        }
    };

    if std::io::stdin().is_terminal() {
        let confirm = Confirm::new()
            .with_prompt(format!("Delete project '{}'?", project.name))
            .default(false)
            .interact()?;

        if !confirm {
            return Ok(());
        }
    }

    with_spinner("Deleting project...", api::delete_project(http, ctx, &project.id)).await?;
    eprintln!("Deleted {}", project.name);

    Ok(())
}
