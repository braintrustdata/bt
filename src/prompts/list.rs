use std::fmt::Write as _;

use anyhow::Result;
use dialoguer::console;

use crate::{
    ui::{apply_column_padding, header, print_with_pager, styled_table, truncate, with_spinner},
    utils::pluralize,
};

use super::{api, ResolvedContext};

pub async fn run(ctx: &ResolvedContext, environment: Option<&str>, json: bool) -> Result<()> {
    let project_name = &ctx.project.name;
    let prompts = with_spinner(
        "Loading prompts...",
        api::list_prompts(&ctx.client, project_name, environment),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&prompts)?);
        return Ok(());
    }

    let mut output = String::new();

    let count = format!(
        "{} {}",
        prompts.len(),
        pluralize(prompts.len(), "prompt", None)
    );
    writeln!(
        output,
        "{} found in {} {} {}{}\n",
        console::style(count),
        console::style(ctx.client.org_name()).bold(),
        console::style("/").dim().bold(),
        console::style(project_name).bold(),
        environment
            .map(|environment| format!(" for environment {}", console::style(environment).bold()))
            .unwrap_or_default()
    )?;

    let mut table = styled_table();
    let mut headers = vec![header("Name"), header("Description"), header("Slug")];
    if environment.is_some() {
        headers.push(header("Version"));
    }
    table.set_header(headers);
    apply_column_padding(&mut table, (0, 6));

    for prompt in &prompts {
        let desc = prompt
            .description
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|s| truncate(s, 60))
            .unwrap_or_else(|| "-".to_string());
        let version = prompt
            ._xact_id
            .as_deref()
            .map(crate::util_cmd::display_xact_id)
            .unwrap_or_else(|| "-".to_string());
        let mut row = vec![prompt.name.as_str(), desc.as_str(), prompt.slug.as_str()];
        if environment.is_some() {
            row.push(version.as_str());
        }
        table.add_row(row);
    }

    write!(output, "{table}")?;
    print_with_pager(&output)?;
    Ok(())
}
