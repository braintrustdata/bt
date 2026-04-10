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
    let datasets = with_spinner(
        "Loading datasets...",
        api::list_datasets(&ctx.client, project_name),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&datasets)?);
        return Ok(());
    }

    let mut output = String::new();

    let count = format!(
        "{} {}",
        datasets.len(),
        pluralize(datasets.len(), "dataset", None)
    );
    writeln!(
        output,
        "{} found in {} {} {}\n",
        console::style(count),
        console::style(ctx.client.org_name()).bold(),
        console::style("/").dim().bold(),
        console::style(project_name).bold()
    )?;

    let mut table = styled_table();
    table.set_header(vec![
        header("Name"),
        header("Description"),
        header("Created"),
    ]);
    apply_column_padding(&mut table, (0, 6));

    for dataset in &datasets {
        let desc = dataset
            .description
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|s| truncate(s, 60))
            .unwrap_or_else(|| "-".to_string());
        let created = dataset.created.as_deref().unwrap_or("-").to_string();
        table.add_row(vec![&dataset.name, &desc, &created]);
    }

    write!(output, "{table}")?;
    print_with_pager(&output)?;
    Ok(())
}
