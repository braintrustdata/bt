use std::fmt::Write as _;

use anyhow::Result;
use dialoguer::console;

use crate::{
    ui::{apply_column_padding, header, print_with_pager, styled_table, truncate, with_spinner},
    utils::pluralize,
};

use super::{api, ResolvedContext};

pub async fn run(ctx: &ResolvedContext, json: bool) -> Result<()> {
    let project_name = &ctx.project.name;
    let playgrounds = with_spinner(
        "Loading playgrounds...",
        api::list_playground_summaries(&ctx.client, project_name),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&playgrounds)?);
        return Ok(());
    }

    let mut output = String::new();

    let count = format!(
        "{} {}",
        playgrounds.len(),
        pluralize(playgrounds.len(), "playground", None)
    );
    writeln!(
        output,
        "{} found in {} {} {}\n",
        console::style(count),
        console::style(ctx.client.org_name()).bold(),
        console::style("/").dim().bold(),
        console::style(project_name).bold()
    )?;

    if playgrounds.is_empty() {
        write!(output, "No playgrounds yet. Create one in the web app.")?;
        print_with_pager(&output)?;
        return Ok(());
    }

    let mut table = styled_table();
    table.set_header(vec![
        header("Name"),
        header("Created"),
        header("Created by"),
    ]);
    apply_column_padding(&mut table, (0, 6));

    for playground in &playgrounds {
        let created = playground
            .created
            .as_deref()
            .filter(|s| !s.is_empty())
            .and_then(|s| s.get(..10))
            .unwrap_or("-");
        let created_by = playground
            .display_name()
            .map(|s| truncate(s, 40))
            .unwrap_or_else(|| "-".to_string());
        table.add_row(vec![&playground.name, created, &created_by]);
    }

    write!(output, "{table}")?;
    print_with_pager(&output)?;
    Ok(())
}
