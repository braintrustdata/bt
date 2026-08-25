use std::fmt::Write as _;

use anyhow::{anyhow, bail, Result};
use dialoguer::console;

use crate::prompts::delete::select_prompt_interactive;
use crate::ui::{header, print_with_pager, styled_table, with_spinner};
use crate::utils::pluralize;

use super::{api, ResolvedContext};

pub async fn run(ctx: &ResolvedContext, slug: Option<&str>, json: bool) -> Result<()> {
    let project_name = &ctx.project.name;
    let prompt = match slug {
        Some(slug) => with_spinner(
            "Loading prompt...",
            api::get_prompt_by_slug(&ctx.client, project_name, slug, None, None),
        )
        .await?
        .ok_or_else(|| anyhow!("prompt with slug '{slug}' not found"))?,
        None => {
            if !crate::ui::is_interactive() {
                bail!("prompt slug required. Use: bt prompts versions <slug>");
            }
            select_prompt_interactive(&ctx.client, project_name).await?
        }
    };

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
