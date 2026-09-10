use std::fmt::Write as _;

use anyhow::Result;
use dialoguer::console;

use crate::ui::{header, print_with_pager, styled_table, with_spinner};
use crate::utils::pluralize;

use super::{api, resolve_prompt, ResolvedContext};

pub async fn run(ctx: &ResolvedContext, slug: Option<&str>, json: bool) -> Result<()> {
    let prompt = resolve_prompt(ctx, slug, None, None, "bt prompts versions <slug>").await?;

    let versions = with_spinner(
        "Loading prompt versions...",
        api::list_prompt_versions(&ctx.client, &ctx.project.id, &prompt.id),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&versions)?);
        return Ok(());
    }

    let mut output = String::new();
    let count = format!(
        "{} {}",
        versions.len(),
        pluralize(versions.len(), "version", None)
    );
    writeln!(
        output,
        "{} found for {}\n",
        console::style(count),
        console::style(&prompt.slug).bold()
    )?;

    let mut table = styled_table();
    table.set_header(vec![header("Version")]);
    for version in versions {
        table.add_row(vec![version]);
    }

    write!(output, "{table}")?;
    print_with_pager(&output)?;
    Ok(())
}
