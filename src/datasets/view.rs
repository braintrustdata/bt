use std::fmt::Write as _;

use anyhow::{anyhow, bail, Result};
use dialoguer::console;
use serde_json::{Map, Value};
use urlencoding::encode;

use crate::datasets::delete::select_dataset_interactive;
use crate::http::BtqlResponse;
use crate::ui::{
    header, is_interactive, print_command_status, print_with_pager, styled_table, truncate,
    with_spinner, CommandStatus,
};

use super::{api, ResolvedContext};

pub async fn run(
    ctx: &ResolvedContext,
    name: Option<&str>,
    json: bool,
    web: bool,
    limit: usize,
) -> Result<()> {
    let project_name = &ctx.project.name;
    let dataset = match name {
        Some(n) => with_spinner(
            "Loading dataset...",
            api::get_dataset_by_name(&ctx.client, project_name, n),
        )
        .await?
        .ok_or_else(|| anyhow!("dataset '{n}' not found"))?,
        None => {
            if !is_interactive() {
                bail!("dataset name required. Use: bt datasets view <name>");
            }
            select_dataset_interactive(&ctx.client, project_name).await?
        }
    };

    if web {
        let url = format!(
            "{}/app/{}/p/{}/datasets/{}",
            ctx.app_url.trim_end_matches('/'),
            encode(ctx.client.org_name()),
            encode(project_name),
            encode(&dataset.id)
        );
        open::that(&url)?;
        print_command_status(CommandStatus::Success, &format!("Opened {url} in browser"));
        return Ok(());
    }

    if json {
        println!("{}", serde_json::to_string(&dataset)?);
        return Ok(());
    }

    let mut output = String::new();

    writeln!(output, "Viewing {}", console::style(&dataset.name).bold())?;

    if let Some(desc) = dataset.description.as_deref().filter(|d| !d.is_empty()) {
        writeln!(output, "{} {}", console::style("Description:").dim(), desc)?;
    }
    if let Some(created) = &dataset.created {
        writeln!(output, "{} {}", console::style("Created:").dim(), created)?;
    }

    let url = format!(
        "{}/app/{}/p/{}/datasets/{}",
        ctx.app_url.trim_end_matches('/'),
        encode(ctx.client.org_name()),
        encode(project_name),
        encode(&dataset.id)
    );

    // Fetch sample rows via BTQL
    if limit > 0 {
        writeln!(output)?;

        let query = format!("SELECT * FROM dataset('{}') LIMIT {}", dataset.id, limit);
        let response: BtqlResponse<Map<String, Value>> =
            with_spinner("Loading rows...", ctx.client.btql(&query)).await?;

        if response.data.is_empty() {
            writeln!(output, "{}", console::style("(no rows)").dim())?;
        } else {
            writeln!(
                output,
                "{}\n",
                console::style(format!("Sample rows ({})", response.data.len())).dim()
            )?;
            write!(output, "{}", render_rows_table(&response.data))?;
        }
    }

    writeln!(
        output,
        "\n{} {}",
        console::style("View in browser:").dim(),
        console::style(&url).underlined()
    )?;

    print_with_pager(&output)?;
    Ok(())
}

fn render_rows_table(rows: &[Map<String, Value>]) -> String {
    // Collect all unique column names from all rows
    let mut columns: Vec<String> = Vec::new();
    for row in rows {
        for key in row.keys() {
            if !columns.contains(key) {
                columns.push(key.clone());
            }
        }
    }

    let mut table = styled_table();
    table.set_header(columns.iter().map(|c| header(c)).collect::<Vec<_>>());

    for row in rows {
        let cells: Vec<String> = columns
            .iter()
            .map(|col| {
                let cell = format_cell(row.get(col));
                truncate(&cell, 50)
            })
            .collect();
        table.add_row(cells);
    }

    table.to_string()
}

fn format_cell(value: Option<&Value>) -> String {
    match value {
        None => String::new(),
        Some(v) => match v {
            Value::String(s) => s.clone(),
            Value::Null => String::new(),
            Value::Array(_) | Value::Object(_) => serde_json::to_string(v).unwrap_or_default(),
            other => other.to_string(),
        },
    }
}
