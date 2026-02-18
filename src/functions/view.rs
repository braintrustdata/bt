use std::fmt::Write as _;
use std::io::IsTerminal;

use anyhow::{anyhow, bail, Result};
use dialoguer::console;

use crate::http::ApiClient;
use crate::ui::{print_with_pager, with_spinner};

use super::api;
use super::delete::select_function_interactive;

pub async fn run(client: &ApiClient, project: &str, slug: Option<&str>, json: bool) -> Result<()> {
    let function = match slug {
        Some(s) => with_spinner(
            "Loading function...",
            api::get_function_by_slug(client, project, s),
        )
        .await?
        .ok_or_else(|| anyhow!("function with slug '{s}' not found"))?,
        None => {
            if !std::io::stdin().is_terminal() {
                bail!("function slug required. Use: bt functions view <slug>");
            }
            select_function_interactive(client, project).await?
        }
    };

    if json {
        println!("{}", serde_json::to_string(&function)?);
        return Ok(());
    }

    let mut output = String::new();
    writeln!(output, "Viewing {}", console::style(&function.name).bold())?;
    writeln!(
        output,
        "{} {}",
        console::style("Slug:").dim(),
        function.slug
    )?;
    writeln!(
        output,
        "{} {}",
        console::style("Type:").dim(),
        function.display_type()
    )?;

    if let Some(desc) = function
        .description
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        writeln!(output, "{} {}", console::style("Description:").dim(), desc)?;
    }

    if let Some(function_data) = &function.function_data {
        writeln!(output)?;
        writeln!(output, "{}", console::style("Function Data").bold())?;
        writeln!(output, "{}", serde_json::to_string_pretty(function_data)?)?;
    }

    print_with_pager(&output)?;
    Ok(())
}
