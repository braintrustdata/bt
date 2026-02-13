use std::fmt::Write as _;
use std::io::IsTerminal;

use anyhow::{anyhow, bail, Result};
use dialoguer::console;
use urlencoding::encode;

use crate::http::ApiClient;
use crate::prompts::delete::select_prompt_interactive;
use crate::ui::prompt_render::{
    render_content_lines, render_message, render_options, render_tools,
};
use crate::ui::{print_command_status, print_with_pager, with_spinner, CommandStatus};

use super::api;

pub async fn run(
    client: &ApiClient,
    app_url: &str,
    project: &str,
    org_name: &str,
    slug: Option<&str>,
    json: bool,
    web: bool,
    verbose: bool,
) -> Result<()> {
    let prompt = match slug {
        Some(s) => with_spinner(
            "Loading prompt...",
            api::get_prompt_by_slug(client, project, s),
        )
        .await?
        .ok_or_else(|| anyhow!("prompt with slug '{s}' not found"))?,
        None => {
            if !std::io::stdin().is_terminal() {
                bail!("prompt slug required. Use: bt prompts view <slug>");
            }
            select_prompt_interactive(client, project).await?
        }
    };

    if web {
        let url = format!(
            "{}/app/{}/p/{}/prompts/{}",
            app_url.trim_end_matches('/'),
            encode(org_name),
            encode(project),
            encode(&prompt.id)
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
        match prompt_block.get("type").and_then(|t| t.as_str()) {
            Some("chat") => {
                if let Some(messages) = prompt_block.get("messages").and_then(|m| m.as_array()) {
                    for msg in messages {
                        render_message(&mut output, msg)?;
                    }
                }
            }
            Some("completion") => {
                if let Some(content) = prompt_block.get("content").and_then(|c| c.as_str()) {
                    render_content_lines(&mut output, content)?;
                    writeln!(output)?;
                }
            }
            _ => {}
        }

        if let Some(tools_val) = prompt_block.get("tools") {
            let tools: Option<Vec<serde_json::Value>> = match tools_val {
                serde_json::Value::Array(arr) => Some(arr.clone()),
                serde_json::Value::String(s) => serde_json::from_str(s).ok(),
                _ => None,
            };
            if let Some(ref tools) = tools {
                render_tools(&mut output, tools)?;
            }
        }
    }

    print_with_pager(&output)?;
    Ok(())
}
