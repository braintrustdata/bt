use std::fmt::Write as _;

use anyhow::Result;
use dialoguer::console;

use crate::{http::ApiClient, ui::print_with_pager};

use super::resolve_environment;

pub async fn run(client: &ApiClient, slug: Option<&str>, json: bool) -> Result<()> {
    let environment = resolve_environment(client, slug, "view").await?;

    if json {
        println!("{}", serde_json::to_string(&environment)?);
        return Ok(());
    }

    let mut output = String::new();
    writeln!(output, "{}", console::style(&environment.name).bold())?;
    writeln!(
        output,
        "{} {}",
        console::style("Slug:").dim(),
        environment.slug
    )?;
    writeln!(
        output,
        "{} {}",
        console::style("Description:").dim(),
        environment.description.as_deref().unwrap_or("-")
    )?;
    writeln!(
        output,
        "{} {}",
        console::style("Created:").dim(),
        environment.created.as_deref().unwrap_or("-")
    )?;
    writeln!(output, "{} {}", console::style("ID:").dim(), environment.id)?;
    print_with_pager(&output)?;
    Ok(())
}
