use std::fmt::Write as _;

use anyhow::{anyhow, bail, Result};
use dialoguer::console;
use serde_json::{Map, Value};
use urlencoding::encode;

use crate::ui::{
    apply_column_padding, header, is_interactive, print_command_status, print_with_pager,
    styled_table, truncate, with_spinner, CommandStatus,
};

use super::{api, ResolvedContext};

pub async fn run(
    ctx: &ResolvedContext,
    name: Option<&str>,
    json: bool,
    web: bool,
    verbose: bool,
    max_rows: Option<usize>,
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

    let (rows, rows_truncated) = with_spinner(
        "Loading dataset rows...",
        api::list_dataset_rows_limited(&ctx.client, &dataset.id, max_rows),
    )
    .await?;

    if json {
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "dataset": dataset,
                "rows": rows,
                "rows_truncated": rows_truncated,
                "row_limit": max_rows,
            }))?
        );
        return Ok(());
    }

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
    if rows_truncated {
        let label = max_rows
            .map(|max_rows| format!("{} (truncated to {})", rows.len(), max_rows))
            .unwrap_or_else(|| rows.len().to_string());
        writeln!(output, "{} {}", console::style("Rows:").dim(), label)?;
    } else {
        writeln!(output, "{} {}", console::style("Rows:").dim(), rows.len())?;
    }

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
    if rows_truncated {
        writeln!(
            output,
            "{}",
            console::style(
                "Row output was truncated. Re-run with --all-rows or a larger --limit to inspect more rows."
            )
            .dim()
        )?;
    }
    if verbose {
        writeln!(output, "{}", serde_json::to_string_pretty(&rows)?)?;
    } else {
        let mut table = styled_table();
        table.set_header(vec![
            header("ID"),
            header("Input"),
            header("Expected"),
            header("Output"),
            header("Metadata"),
            header("Tags"),
        ]);
        apply_column_padding(&mut table, (0, 2));

        for row in &rows {
            let compact = compact_row_for_display(row);
            table.add_row(vec![
                format_compact_value(compact.get("id"), 30),
                format_compact_value(compact.get("input"), 40),
                format_compact_value(compact.get("expected"), 40),
                format_compact_value(compact.get("output"), 40),
                format_compact_value(compact.get("metadata"), 40),
                format_compact_value(compact.get("tags"), 30),
            ]);
        }

        writeln!(output, "{table}")?;
    }

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

fn format_compact_value(value: Option<&Value>, max_len: usize) -> String {
    let Some(value) = value else {
        return "-".to_string();
    };

    let rendered = match value {
        Value::String(s) => s.clone(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| "<invalid json>".to_string()),
    };
    truncate(&rendered, max_len)
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

    #[test]
    fn format_compact_value_handles_strings_and_missing_values() {
        assert_eq!(
            format_compact_value(Some(&Value::String("abc".into())), 10),
            "abc"
        );
        assert_eq!(format_compact_value(None, 10), "-");
    }
}
