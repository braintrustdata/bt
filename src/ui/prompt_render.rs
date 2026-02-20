use std::fmt::Write as _;
use std::sync::LazyLock;

use anyhow::Result;
use dialoguer::console;
use regex::Regex;

static TEMPLATE_VAR_RE: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\{\{([^}]+)\}\}").unwrap());

pub fn render_message(output: &mut String, msg: &serde_json::Value) -> Result<()> {
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

pub fn render_content_lines(output: &mut String, content: &str) -> Result<()> {
    for line in content.lines() {
        let highlighted = highlight_template_vars(line);
        writeln!(output, "{} {highlighted}", console::style("│").dim())?;
    }
    Ok(())
}

pub fn render_code_lines(output: &mut String, code: &str, language: Option<&str>) -> Result<()> {
    let highlighted: Option<Vec<String>> = if console::colors_enabled() {
        language.and_then(|lang| super::highlight::highlight_code(code, lang))
    } else {
        None
    };

    let lines: Vec<&str> = code.lines().collect();
    let width = lines.len().to_string().len();
    for (i, line) in lines.iter().enumerate() {
        let display = match &highlighted {
            Some(hl) => hl.get(i).map(|s| s.as_str()).unwrap_or(line),
            None => line,
        };
        writeln!(
            output,
            "  {} {} {}",
            console::style(format!("{:>width$}", i + 1)).dim(),
            console::style("│").dim(),
            display
        )?;
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

pub fn render_options(output: &mut String, options: &serde_json::Value) -> Result<()> {
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

pub fn render_tools(output: &mut String, tools: &[serde_json::Value]) -> Result<()> {
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
