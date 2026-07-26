use std::fmt::Write as _;

use anyhow::{anyhow, bail, Result};
use dialoguer::console;

use crate::prompts::delete::select_prompt_interactive;
use crate::ui::prompt_render::{render_options, render_prompt_block};
use crate::ui::{print_command_status, print_with_pager, with_spinner, CommandStatus};
use crate::utils::app_project_url;

use super::{api, ResolvedContext};

pub async fn run(
    ctx: &ResolvedContext,
    slug: Option<&str>,
    json: bool,
    web: bool,
    verbose: bool,
) -> Result<()> {
    let project_name = &ctx.project.name;
    let prompt = match slug {
        Some(s) => with_spinner(
            "Loading prompt...",
            api::get_prompt_by_slug(&ctx.client, project_name, s),
        )
        .await?
        .ok_or_else(|| anyhow!("prompt with slug '{s}' not found"))?,
        None => {
            if !crate::ui::is_interactive() {
                bail!("prompt slug required. Use: bt prompts view <slug>");
            }
            select_prompt_interactive(&ctx.client, project_name).await?
        }
    };

    if web {
        let url = app_project_url(
            &ctx.app_url,
            ctx.client.org_name(),
            project_name,
            &["prompts", &prompt.id],
        );
        open::that(&url)?;
        print_command_status(CommandStatus::Success, &format!("Opened {url} in browser"));
        return Ok(());
    }

    if json {
        println!("{}", serde_json::to_string(&prompt)?);
        return Ok(());
    }

    let mut output = String::new();

    writeln!(output, "Viewing {}", console::style(&prompt.name).bold())?;

    let options = prompt.prompt_data.as_ref().and_then(|pd| pd.get("options"));

    if let Some(model) = options
        .and_then(|o| o.get("model"))
        .and_then(|m| m.as_str())
    {
        writeln!(output, "{} {}", console::style("Model:").dim(), model)?;
    }

    if verbose {
        if let Some(opts) = options {
            render_options(&mut output, opts)?;
        }
    }

    writeln!(output)?;

    if let Some(prompt_block) = prompt.prompt_data.as_ref().and_then(|pd| pd.get("prompt")) {
        render_prompt_block(&mut output, prompt_block)?;
    }

    print_with_pager(&output)?;
    Ok(())
}
