use std::fmt::Write as _;
use std::io::IsTerminal;
use std::sync::LazyLock;

use anyhow::{bail, Result};
use dialoguer::console;
use regex::Regex;
use urlencoding::encode;

static TEMPLATE_VAR_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{\{([^}]+)\}\}").unwrap());

use crate::http::ApiClient;
use crate::prompts::delete::select_prompt_interactive;
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
        Some(s) => {
            with_spinner(
                "Loading prompt...",
                api::get_prompt_by_slug(client, project, s),
            )
            .await?
        }
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

fn render_message(output: &mut String, msg: &serde_json::Value) -> Result<()> {
    let role = msg
        .get("role")
        .and_then(|r| r.as_str())
        .unwrap_or("unknown");
    let styled_role = match role {
        "system" => console::style(role).dim().bold(),
        "user" => console::style(role).green().bold(),
        "assistant" => console::style(role).blue().bold(),
        _ => console::style(role).bold(),
    };
    writeln!(output, "{} {styled_role}", console::style("┃").dim())?;

    if let Some(content) = msg.get("content") {
        match content {
            serde_json::Value::String(s) => render_content_lines(output, s)?,
            serde_json::Value::Array(parts) => {
                for part in parts {
                    match part.get("type").and_then(|t| t.as_str()) {
                        Some("text") => {
                            if let Some(text) = part.get("text").and_then(|t| t.as_str()) {
                                render_content_lines(output, text)?;
                            }
                        }
                        Some("image_url") => {
                            let url = part
                                .get("image_url")
                                .and_then(|iu| iu.get("url"))
                                .and_then(|u| u.as_str())
                                .unwrap_or("?");
                            writeln!(
                                output,
                                "{} {}",
                                console::style("│").dim(),
                                console::style(format!("[image: {url}]")).dim()
                            )?;
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }

    if let Some(tool_calls) = msg.get("tool_calls").and_then(|tc| tc.as_array()) {
        for tc in tool_calls {
            if let Some(func) = tc.get("function") {
                let name = func.get("name").and_then(|n| n.as_str()).unwrap_or("?");
                let args = func.get("arguments").and_then(|a| a.as_str()).unwrap_or("");
                writeln!(
                    output,
                    "{} {}({})",
                    console::style("│").dim(),
                    console::style(name).yellow(),
                    args
                )?;
            }
        }
    }

    writeln!(output)?;
    Ok(())
}

fn render_content_lines(output: &mut String, content: &str) -> Result<()> {
    for line in content.lines() {
        let highlighted = highlight_template_vars(line);
        writeln!(output, "{} {highlighted}", console::style("│").dim())?;
    }
    Ok(())
}

fn highlight_template_vars(line: &str) -> String {
    let re = &*TEMPLATE_VAR_RE;
    let mut result = String::new();
    let mut last_end = 0;
    for cap in re.find_iter(line) {
        result.push_str(&line[last_end..cap.start()]);
        result.push_str(&format!("{}", console::style(cap.as_str()).cyan().bold()));
        last_end = cap.end();
    }
    result.push_str(&line[last_end..]);
    result
}

fn render_options(output: &mut String, options: &serde_json::Value) -> Result<()> {
    let Some(params) = options.get("params").and_then(|p| p.as_object()) else {
        return Ok(());
    };

    for (key, val) in params {
        if !val.is_null() {
            writeln!(
                output,
                "  {:<24}{}",
                console::style(format!("{key}:")).dim(),
                format_param_value(val)
            )?;
        }
    }

    Ok(())
}

fn format_param_value(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::String(s) => s.clone(),
        serde_json::Value::Bool(b) => b.to_string(),
        serde_json::Value::Number(n) => n.to_string(),
        serde_json::Value::Array(arr) => {
            let items: Vec<String> = arr.iter().map(format_param_value).collect();
            format!("[{}]", items.join(", "))
        }
        other => other.to_string(),
    }
}

fn render_tools(output: &mut String, tools: &[serde_json::Value]) -> Result<()> {
    writeln!(
        output,
        "{} {}",
        console::style("┃").dim(),
        console::style("tools").magenta().bold()
    )?;
    for tool in tools {
        let func = tool.get("function").unwrap_or(tool);
        let name = func.get("name").and_then(|n| n.as_str()).unwrap_or("?");
        let desc = func.get("description").and_then(|d| d.as_str());
        match desc {
            Some(d) => writeln!(
                output,
                "{} {} {}",
                console::style("│").dim(),
                console::style(name).yellow(),
                console::style(format!("— {d}")).dim()
            )?,
            None => writeln!(
                output,
                "{} {}",
                console::style("│").dim(),
                console::style(name).yellow()
            )?,
        }
    }
    writeln!(output)?;
    Ok(())
}
