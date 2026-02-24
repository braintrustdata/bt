use std::fmt::Write as _;

use anyhow::{anyhow, bail, Result};
use dialoguer::console;
use urlencoding::encode;

use crate::ui::prompt_render::{
    render_code_lines, render_content_lines, render_options, render_prompt_block,
};
use crate::ui::{
    is_interactive, print_command_status, print_with_pager, with_spinner, CommandStatus,
};

use super::{api, build_web_path, label, label_plural, select_function_interactive};
use super::{FunctionTypeFilter, ResolvedContext};

pub async fn run(
    ctx: &ResolvedContext,
    slug: Option<&str>,
    json: bool,
    web: bool,
    verbose: bool,
    ft: Option<FunctionTypeFilter>,
) -> Result<()> {
    let project_id = &ctx.project.id;
    let function = match slug {
        Some(s) => with_spinner(
            &format!("Loading {}...", label(ft)),
            api::get_function_by_slug(&ctx.client, project_id, s),
        )
        .await?
        .ok_or_else(|| anyhow!("{} with slug '{s}' not found", label(ft)))?,
        None => {
            if !is_interactive() {
                bail!(
                    "{} slug required. Use: bt {} view <slug>",
                    label(ft),
                    label_plural(ft),
                );
            }
            select_function_interactive(&ctx.client, project_id, ft).await?
        }
    };

    if web {
        let path = build_web_path(&function);
        let url = format!(
            "{}/app/{}/p/{}/{}",
            ctx.app_url.trim_end_matches('/'),
            encode(ctx.client.org_name()),
            encode(&ctx.project.name),
            path
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
            render_prompt_block(&mut output, prompt_block)?;
        }
    }

    if let Some(fd) = &function.function_data {
        if let Some(fd_type) = fd.get("type").and_then(|t| t.as_str()) {
            match fd_type {
                "code" => {
                    if let Some(data) = fd.get("data") {
                        let data_type = data.get("type").and_then(|t| t.as_str());

                        let runtime_name = data
                            .get("runtime_context")
                            .and_then(|rc| rc.get("runtime"))
                            .and_then(|r| r.as_str());

                        if let Some(runtime) = data.get("runtime_context").and_then(|rc| {
                            let rt = rc.get("runtime").and_then(|r| r.as_str())?;
                            let ver = rc.get("version").and_then(|v| v.as_str()).unwrap_or("");
                            Some(if ver.is_empty() {
                                rt.to_string()
                            } else {
                                format!("{rt} {ver}")
                            })
                        }) {
                            writeln!(output, "{} {}", console::style("Runtime:").dim(), runtime)?;
                        }

                        match data_type {
                            Some("inline") => {
                                if let Some(code) = data.get("code").and_then(|c| c.as_str()) {
                                    if !code.is_empty() {
                                        writeln!(output)?;
                                        writeln!(output, "{}", console::style("Code:").dim())?;
                                        render_code_lines(&mut output, code, runtime_name)?;
                                    }
                                }
                            }
                            Some("bundle") => {
                                match data.get("preview").and_then(|p| p.as_str()) {
                                    Some(p) if !p.is_empty() => {
                                        writeln!(output)?;
                                        writeln!(
                                            output,
                                            "{}",
                                            console::style("Code (preview):").dim()
                                        )?;
                                        render_code_lines(&mut output, p, runtime_name)?;
                                    }
                                    _ => {
                                        writeln!(
                                            output,
                                            "  {}",
                                            console::style("Code bundle — preview not available")
                                                .dim()
                                        )?;
                                    }
                                }

                                if verbose {
                                    if let Some(bid) =
                                        data.get("bundle_id").and_then(|b| b.as_str())
                                    {
                                        writeln!(
                                            output,
                                            "  {} {}",
                                            console::style("Bundle ID:").dim(),
                                            bid
                                        )?;
                                    }
                                    if let Some(loc) = data.get("location") {
                                        let loc_str = match loc.get("type").and_then(|t| t.as_str())
                                        {
                                            Some("experiment") => {
                                                let eval_name = loc
                                                    .get("eval_name")
                                                    .and_then(|e| e.as_str())
                                                    .unwrap_or("?");
                                                let pos_type = loc
                                                    .get("position")
                                                    .and_then(|p| p.get("type"))
                                                    .and_then(|t| t.as_str())
                                                    .unwrap_or("?");
                                                format!("experiment/{eval_name}/{pos_type}")
                                            }
                                            Some("function") => {
                                                let index = loc
                                                    .get("index")
                                                    .and_then(|i| i.as_u64())
                                                    .map(|i| i.to_string())
                                                    .unwrap_or_else(|| "?".to_string());
                                                format!("function/{index}")
                                            }
                                            Some(other) => other.to_string(),
                                            None => "?".to_string(),
                                        };
                                        writeln!(
                                            output,
                                            "  {} {}",
                                            console::style("Location:").dim(),
                                            loc_str
                                        )?;
                                    }
                                }
                            }
                            _ => {
                                writeln!(output, "{} code", console::style("Function:").dim())?;
                            }
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
                "facet" => {
                    if let Some(model) = fd.get("model").and_then(|m| m.as_str()) {
                        writeln!(output, "{} {}", console::style("Model:").dim(), model)?;
                    }
                    if let Some(prompt) = fd.get("prompt").and_then(|p| p.as_str()) {
                        writeln!(output)?;
                        writeln!(output, "{}", console::style("Prompt:").dim())?;
                        render_content_lines(&mut output, prompt)?;
                    }
                    if let Some(pp) = fd.get("preprocessor") {
                        let name = pp.get("name").and_then(|n| n.as_str()).unwrap_or("?");
                        let pp_type = pp.get("type").and_then(|t| t.as_str()).unwrap_or("?");
                        writeln!(
                            output,
                            "{} {} ({})",
                            console::style("Preprocessor:").dim(),
                            name,
                            pp_type
                        )?;
                    }
                }
                "topic_map" => {
                    if let Some(facet) = fd.get("source_facet").and_then(|f| f.as_str()) {
                        writeln!(
                            output,
                            "{} {}",
                            console::style("Source facet:").dim(),
                            facet
                        )?;
                    }
                    if let Some(model) = fd.get("embedding_model").and_then(|m| m.as_str()) {
                        writeln!(
                            output,
                            "{} {}",
                            console::style("Embedding model:").dim(),
                            model
                        )?;
                    }
                }
                "parameters" => {
                    render_parameters(&mut output, fd)?;
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

fn render_prompt_value(output: &mut String, val: &serde_json::Value) -> Result<()> {
    if let Some(model) = val
        .get("options")
        .and_then(|o| o.get("model"))
        .and_then(|m| m.as_str())
    {
        writeln!(output, "    {} {}", console::style("Model:").dim(), model)?;
    }

    let mut prompt_buf = String::new();
    if let Some(prompt_block) = val.get("prompt") {
        render_prompt_block(&mut prompt_buf, prompt_block)?;
    }

    for line in prompt_buf.lines() {
        writeln!(output, "    {line}")?;
    }
    Ok(())
}

fn render_parameters(output: &mut String, fd: &serde_json::Value) -> Result<()> {
    let schema = fd.get("__schema");
    let data = fd.get("data");
    let properties = schema
        .and_then(|s| s.get("properties"))
        .and_then(|p| p.as_object());
    let required: Vec<&str> = schema
        .and_then(|s| s.get("required"))
        .and_then(|r| r.as_array())
        .map(|arr| arr.iter().filter_map(|v| v.as_str()).collect())
        .unwrap_or_default();

    let Some(props) = properties else {
        return Ok(());
    };

    writeln!(output)?;
    writeln!(output, "{}", console::style("Fields:").dim())?;

    for (name, prop) in props {
        let type_label = prop
            .get("x-bt-type")
            .or_else(|| prop.get("type"))
            .and_then(|t| t.as_str())
            .unwrap_or("unknown");
        let is_required = required.contains(&name.as_str());
        let tag = if is_required { "required" } else { "optional" };

        writeln!(
            output,
            "  {} {} {}",
            console::style(name).bold(),
            console::style(format!("({type_label})")).dim(),
            console::style(format!("[{tag}]")).dim(),
        )?;

        if let Some(desc) = prop.get("description").and_then(|d| d.as_str()) {
            writeln!(output, "    {desc}")?;
        }

        if let Some(val) = data.and_then(|d| d.get(name)) {
            if type_label == "prompt" {
                render_prompt_value(output, val)?;
            } else {
                let display = match val {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Null => "null".to_string(),
                    other => serde_json::to_string(other).unwrap_or_default(),
                };
                writeln!(output, "    {} {}", console::style("Value:").dim(), display)?;
            }
        }
    }

    Ok(())
}
