use std::fmt::Write as _;

use anyhow::{anyhow, bail, Result};
use dialoguer::console;
use serde_json::{Map, Value};
use urlencoding::encode;

use crate::ui::{
    is_interactive, print_command_status, print_with_pager, with_spinner, CommandStatus,
};

use super::{api, ResolvedContext};

pub async fn run(
    ctx: &ResolvedContext,
    name: Option<&str>,
    json: bool,
    web: bool,
    verbose: bool,
) -> Result<()> {
    let dataset = match name {
        Some(name) => with_spinner(
            "Loading dataset...",
            api::get_dataset_by_name(&ctx.client, &ctx.project.id, name),
        )
        .await?
        .ok_or_else(|| anyhow!("dataset '{name}' not found"))?,
        None => {
            if !is_interactive() {
                bail!("dataset name required. Use: bt datasets view <name>");
            }
            super::select_dataset_interactive(&ctx.client, &ctx.project.id).await?
        }
    };

    let url = format!(
        "{}/app/{}/p/{}/datasets/{}",
        ctx.app_url.trim_end_matches('/'),
        encode(ctx.client.org_name()),
        encode(&ctx.project.name),
        encode(&dataset.name)
    );

    if web {
        open::that(&url)?;
        print_command_status(CommandStatus::Success, &format!("Opened {url} in browser"));
        return Ok(());
    }

    let rows = with_spinner(
        "Loading dataset rows...",
        api::list_dataset_rows(&ctx.client, &dataset.id),
    )
    .await?;

    if json {
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "dataset": dataset,
                "rows": rows,
            }))?
        );
        return Ok(());
    }

    let display_rows = if verbose {
        rows.clone()
    } else {
        rows.iter().map(compact_row_for_display).collect()
    };

    let mut output = String::new();
    writeln!(output, "Viewing {}", console::style(&dataset.name).bold())?;

    if let Some(description) = dataset.description_text() {
        writeln!(
            output,
            "{} {}",
            console::style("Description:").dim(),
            description
        )?;
    }
    writeln!(output, "{} {}", console::style("ID:").dim(), dataset.id)?;
    writeln!(
        output,
        "{} {}",
        console::style("Project:").dim(),
        ctx.project.name
    )?;
    if let Some(created) = dataset.created_text() {
        writeln!(output, "{} {}", console::style("Created:").dim(), created)?;
    }
    writeln!(output, "{} {}", console::style("Rows:").dim(), rows.len())?;

    writeln!(
        output,
        "\n{} {}",
        console::style("View dataset:").dim(),
        console::style(&url).underlined()
    )?;

    writeln!(output, "\n{}", console::style("Dataset rows:").dim())?;
    if !verbose {
        writeln!(
            output,
            "{}",
            console::style(
                "Showing row id/input/expected/output/metadata/tags only. Re-run with --verbose to inspect full row payloads."
            )
            .dim()
        )?;
    }
    writeln!(output, "{}", serde_json::to_string_pretty(&display_rows)?)?;

    print_with_pager(&output)?;
    Ok(())
}

fn compact_row_for_display(row: &Map<String, Value>) -> Map<String, Value> {
    let mut compact = Map::new();

    if let Some(value) = row.get("id").cloned() {
        compact.insert("id".to_string(), value);
    } else if let Some(value) = row.get("span_id").cloned() {
        compact.insert("id".to_string(), value);
    }

    for key in ["input", "expected", "output", "metadata", "tags"] {
        if let Some(value) = row.get(key).cloned() {
            compact.insert(key.to_string(), value);
        }
    }

    compact
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compact_row_for_display_keeps_user_facing_fields() {
        let row = serde_json::from_value::<Map<String, Value>>(serde_json::json!({
            "id": "case-1",
            "dataset_id": "dataset_1",
            "created": "2026-01-01T00:00:00Z",
            "input": {"prompt": "hello"},
            "expected": "world",
            "metadata": {"topic": "math"},
            "tags": ["smoke"],
            "span_id": "span-1"
        }))
        .expect("row map");

        let compact = compact_row_for_display(&row);
        assert_eq!(compact.len(), 5);
        assert_eq!(
            compact.get("id"),
            Some(&Value::String("case-1".to_string()))
        );
        assert!(compact.get("dataset_id").is_none());
        assert!(compact.get("created").is_none());
        assert!(compact.get("input").is_some());
        assert!(compact.get("expected").is_some());
        assert!(compact.get("metadata").is_some());
        assert!(compact.get("tags").is_some());
    }

    #[test]
    fn compact_row_for_display_falls_back_to_span_id_and_output() {
        let row = serde_json::from_value::<Map<String, Value>>(serde_json::json!({
            "span_id": "span-1",
            "output": "value",
            "created": "2026-01-01T00:00:00Z"
        }))
        .expect("row map");

        let compact = compact_row_for_display(&row);
        assert_eq!(
            compact.get("id"),
            Some(&Value::String("span-1".to_string()))
        );
        assert_eq!(
            compact.get("output"),
            Some(&Value::String("value".to_string()))
        );
        assert!(compact.get("created").is_none());
    }
}
