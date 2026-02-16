use std::fmt::Write as _;
use std::io::IsTerminal;

use anyhow::{anyhow, bail, Result};
use dialoguer::console;
use urlencoding::encode;

use crate::http::ApiClient;
use crate::ui::prompt_render::{
    render_content_lines, render_message, render_options, render_tools,
};
use crate::ui::{print_command_status, print_with_pager, with_spinner, CommandStatus};

use super::{api, delete, FunctionKind};

#[allow(clippy::too_many_arguments)]
pub async fn run(
    client: &ApiClient,
    app_url: &str,
    project_id: &str,
    org_name: &str,
    slug: Option<&str>,
    json: bool,
    web: bool,
    verbose: bool,
    kind: &FunctionKind,
) -> Result<()> {
    let function = match slug {
        Some(s) => with_spinner(
            &format!("Loading {}...", kind.type_name),
            api::get_function_by_slug(client, project_id, s),
        )
        .await?
        .ok_or_else(|| anyhow!("{} with slug '{s}' not found", kind.type_name))?,
        None => {
            if !std::io::stdin().is_terminal() {
                bail!(
                    "{} slug required. Use: bt {} view <slug>",
                    kind.type_name,
                    kind.plural
                );
            }
            delete::select_function_interactive(client, project_id, kind).await?
        }
    };

    if web {
        let url = format!(
            "{}/app/{}/p/{}/{}/{}",
            app_url.trim_end_matches('/'),
            encode(org_name),
            encode(project_id),
            kind.url_segment,
            encode(&function.id)
        );
        open::that(&url)?;
        print_command_status(CommandStatus::Success, &format!("Opened {url} in browser"));
        return Ok(());
    }

    if json {
        println!("{}", serde_json::to_string(&function)?);
        return Ok(());
    }

    let mut output = String::new();
    writeln!(output, "Viewing {}", console::style(&function.name).bold())?;

    if let Some(ft) = &function.function_type {
        writeln!(output, "{} {}", console::style("Type:").dim(), ft)?;
    }

    if let Some(pd) = &function.prompt_data {
        let options = pd.get("options");
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

        if let Some(prompt_block) = pd.get("prompt") {
            match prompt_block.get("type").and_then(|t| t.as_str()) {
                Some("chat") => {
                    if let Some(messages) = prompt_block.get("messages").and_then(|m| m.as_array())
                    {
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
    }

    if let Some(fd) = &function.function_data {
        if let Some(fd_type) = fd.get("type").and_then(|t| t.as_str()) {
            match fd_type {
                "code" => {
                    writeln!(output, "{} code", console::style("Function:").dim())?;
                    if let Some(data) = fd.get("data") {
                        if let Some(rt) = data
                            .get("runtime_context")
                            .and_then(|r| r.get("runtime"))
                            .and_then(|r| r.as_str())
                        {
                            writeln!(output, "  {} {}", console::style("Runtime:").dim(), rt)?;
                        }
                        if let Some(loc) = data.get("location") {
                            let lt = loc.get("type").and_then(|t| t.as_str()).unwrap_or("?");
                            writeln!(output, "  {} {}", console::style("Location:").dim(), lt)?;
                        }
                    }
                }
                "global" => {
                    writeln!(
                        output,
                        "{} global (built-in)",
                        console::style("Function:").dim()
                    )?;
                    if let Some(name) = fd.get("name").and_then(|n| n.as_str()) {
                        writeln!(output, "  {} {}", console::style("Name:").dim(), name)?;
                    }
                }
                "prompt" => {}
                other => {
                    writeln!(output, "{} {}", console::style("Function:").dim(), other)?;
                }
            }
        }
    }

    if verbose {
        if let Some(tags) = &function.tags {
            if !tags.is_empty() {
                writeln!(
                    output,
                    "\n{} {}",
                    console::style("Tags:").dim(),
                    tags.join(", ")
                )?;
            }
        }
        if let Some(meta) = &function.metadata {
            if let Some(obj) = meta.as_object() {
                if !obj.is_empty() {
                    writeln!(
                        output,
                        "{} {}",
                        console::style("Metadata:").dim(),
                        serde_json::to_string_pretty(meta).unwrap_or_default()
                    )?;
                }
            }
        }
    }

    print_with_pager(&output)?;
    Ok(())
}
