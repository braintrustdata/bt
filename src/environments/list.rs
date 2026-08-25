use std::fmt::Write as _;

use anyhow::Result;
use dialoguer::console;

use crate::{
    http::ApiClient,
    ui::{apply_column_padding, header, print_with_pager, styled_table, truncate, with_spinner},
    utils::pluralize,
};

use super::api;

pub async fn run(client: &ApiClient, json: bool) -> Result<()> {
    let environments =
        with_spinner("Loading environments...", api::list_environments(client)).await?;

    if json {
        println!("{}", serde_json::to_string(&environments)?);
        return Ok(());
    }

    let mut output = String::new();
    let count = format!(
        "{} {}",
        environments.len(),
        pluralize(environments.len(), "environment", None)
    );
    writeln!(
        output,
        "{} found in {}\n",
        console::style(count),
        console::style(client.org_name()).bold()
    )?;

    let mut table = styled_table();
    table.set_header(vec![
        header("Name"),
        header("Slug"),
        header("Description"),
        header("Created"),
    ]);
    apply_column_padding(&mut table, (0, 6));

    for environment in &environments {
        let description = environment
            .description
            .as_deref()
            .filter(|value| !value.is_empty())
            .map(|value| truncate(value, 60))
            .unwrap_or_else(|| "-".to_string());
        let created = environment
            .created
            .as_deref()
            .map(|value| truncate(value, 10))
            .unwrap_or_else(|| "-".to_string());
        table.add_row(vec![
            environment.name.as_str(),
            environment.slug.as_str(),
            description.as_str(),
            created.as_str(),
        ]);
    }

    write!(output, "{table}")?;
    print_with_pager(&output)?;
    Ok(())
}
