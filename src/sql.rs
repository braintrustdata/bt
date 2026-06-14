use std::collections::HashMap;
use std::io;
use std::io::Read;

use anyhow::{bail, Context, Result};
use clap::{builder::BoolishValueParser, Args};
use dialoguer::console::style;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use unicode_width::UnicodeWidthStr;

use crate::args::BaseArgs;
use crate::auth::login;
use crate::http::{ApiClient, HttpError};
use crate::ui::{with_spinner, LinePrompt};

const QUERY_SOURCE: &str = "bt_sql_9f4b1e6d7c2a4a7b8d4f9a6c2b1e7f3d";

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt sql \"SELECT * FROM project_logs('<PROJECT_ID>') LIMIT 5\"
  cat query.sql | bt sql
  bt sql --non-interactive \"SELECT count(*) FROM project_logs('<PROJECT_ID>')\"
")]
pub struct SqlArgs {
    /// SQL query to execute
    pub query: Option<String>,

    /// Force non-interactive mode
    #[arg(long)]
    pub non_interactive: bool,

    /// Run the query even when the SQL linter reports failures
    #[arg(
        long,
        env = "BRAINTRUST_SQL_FORCE_IGNORE_LINTER",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    pub force_ignore_linter: bool,
}

#[derive(Debug, Serialize, Deserialize)]
struct SqlResponse {
    pub data: Vec<Map<String, Value>>,
    pub schema: Value,
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub freshness_state: Option<FreshnessState>,
    #[serde(default)]
    pub realtime_state: Option<RealtimeState>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub warnings: Vec<QueryLint>,
    #[serde(flatten)]
    pub extra: HashMap<String, Value>,
}

#[derive(Debug, Serialize, Deserialize)]
struct QueryLint {
    code: String,
    message: String,
    stage: String,
}

#[derive(Debug, Serialize, Deserialize)]
struct FreshnessState {
    #[serde(default)]
    pub last_considered_xact_id: Option<String>,
    #[serde(default)]
    pub last_processed_xact_id: Option<String>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RealtimeState {
    #[serde(default)]
    pub actual_xact_id: Option<String>,
    #[serde(default)]
    pub minimum_xact_id: Option<String>,
    #[serde(default)]
    pub read_bytes: Option<u64>,
    #[serde(default)]
    pub timeout_ms: Option<u64>,
    #[serde(rename = "type")]
    pub state_type: String,
}

pub async fn run(base: BaseArgs, args: SqlArgs) -> Result<()> {
    let ctx = login(&base).await?;
    let client = ApiClient::new(&ctx)?;
    let interactive = !base.json && crate::ui::is_interactive() && !args.non_interactive;
    let query = read_non_interactive_query(&args.query, interactive)?;
    let lint_mode = if args.force_ignore_linter {
        "default"
    } else {
        "strict"
    };

    if let Some(query) = query {
        let response = with_spinner(
            "Running query...",
            execute_query(&client, &query, lint_mode),
        )
        .await?;
        print_response(&response, base.json)?;
        return Ok(());
    }

    if !interactive {
        bail!(
            "query is required in non-interactive mode. Pass `bt sql \"SELECT * FROM project_logs('<PROJECT_ID>') LIMIT 1\"` or pipe SQL via stdin."
        );
    }

    run_interactive(base, client, lint_mode.to_string()).await
}

fn read_non_interactive_query(
    query_arg: &Option<String>,
    interactive: bool,
) -> Result<Option<String>> {
    if let Some(query) = query_arg.as_deref() {
        let trimmed = query.trim();
        if !trimmed.is_empty() {
            return Ok(Some(trimmed.to_string()));
        }
    }

    if interactive {
        return Ok(None);
    }

    let mut stdin_query = String::new();
    io::stdin().read_to_string(&mut stdin_query)?;
    let trimmed = stdin_query.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    Ok(Some(trimmed.to_string()))
}

async fn run_interactive(base: BaseArgs, client: ApiClient, lint_mode: String) -> Result<()> {
    let handle = tokio::runtime::Handle::current();
    tokio::task::block_in_place(|| run_interactive_blocking(base.json, client, handle, lint_mode))
}

fn run_interactive_blocking(
    json_output: bool,
    client: ApiClient,
    handle: tokio::runtime::Handle,
    lint_mode: String,
) -> Result<()> {
    let mut editor = LinePrompt::new(Vec::new());
    let prompt = style("SQL> ").bold().to_string();

    loop {
        let Some(input) = editor.read_line(&prompt, "SQL> ".len())? else {
            return Ok(());
        };
        let query = input.trim();
        if query.is_empty() {
            continue;
        }

        match handle.block_on(execute_query(&client, query, lint_mode.as_str())) {
            Ok(response) => print_response(&response, json_output)?,
            Err(err) => eprintln!("{} {err}", style("error").red().bold()),
        }

        editor.add_history(query);
    }
}

fn format_response(response: &SqlResponse, json_output: bool) -> Result<String> {
    if json_output {
        Ok(serde_json::to_string(response)?)
    } else if let Some(table) = render_table(response) {
        Ok(table)
    } else {
        Ok(serde_json::to_string_pretty(response)?)
    }
}

async fn execute_query(client: &ApiClient, query: &str, lint_mode: &str) -> Result<SqlResponse> {
    let body = query_body(query, lint_mode);

    let org_name = client.org_name();
    let headers = if !org_name.is_empty() {
        vec![("x-bt-org-name", org_name)]
    } else {
        vec![]
    };

    match client.post_with_headers("/btql", &body, &headers).await {
        Ok(response) => Ok(response),
        Err(err) => {
            if let Some(message) = format_strict_lint_error(&err) {
                Err(err).context(message)
            } else {
                Err(err)
            }
        }
    }
}

fn query_body(query: &str, lint_mode: &str) -> Value {
    json!({
        "query": query,
        "fmt": "json",
        "lint_mode": lint_mode,
        "strict_lint_mode": lint_mode == "strict",
        "query_source": QUERY_SOURCE,
    })
}

fn print_response(response: &SqlResponse, json_output: bool) -> Result<()> {
    let output = format_response(response, json_output)?;
    if !json_output {
        for warning in &response.warnings {
            eprintln!(
                "warning[{}:{}]: {}",
                warning.stage, warning.code, warning.message
            );
        }
    }
    println!("{output}");
    Ok(())
}

fn format_strict_lint_error(err: &anyhow::Error) -> Option<String> {
    let http_error = err.downcast_ref::<HttpError>()?;
    let body: Value = serde_json::from_str(&http_error.body).ok()?;
    format_strict_lint_message(body.get("Message")?.as_str()?)
}

fn format_strict_lint_message(message: &str) -> Option<String> {
    let marker = "Query blocked by strict lint mode due to";
    let strict_lint_start = message.find(marker)?;
    let strict_lint_message = &message[strict_lint_start..];
    let (_, details) = strict_lint_message.split_once(':')?;
    let mut output = String::from("Query blocked due to lint failures");

    for failure in details
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .map(format_strict_lint_failure)
    {
        output.push_str("\n\n  - ");
        output.push_str(&failure);
    }

    Some(output)
}

fn format_strict_lint_failure(line: &str) -> String {
    let line = strip_api_context_suffix(line);
    if let Some((_, message)) = line.split_once("):") {
        return message.trim().to_string();
    }
    line.to_string()
}

fn strip_api_context_suffix(line: &str) -> &str {
    let mut end = line.len();
    for marker in [" [user_email=", " [timestamp="] {
        if let Some(idx) = line.find(marker) {
            end = end.min(idx);
        }
    }
    line[..end].trim()
}

fn render_table(response: &SqlResponse) -> Option<String> {
    let mut headers = extract_headers(&response.schema);
    if headers.is_empty() {
        if let Some(first_row) = response.data.first() {
            headers = first_row.keys().cloned().collect();
        }
    }

    if headers.is_empty() {
        if response.data.is_empty() {
            return Some("(no rows)".to_string());
        }
        return None;
    }

    let rows: Vec<Vec<String>> = response
        .data
        .iter()
        .map(|row| {
            headers
                .iter()
                .map(|header| format_cell(row.get(header)))
                .collect()
        })
        .collect();

    Some(build_table(&headers, &rows))
}

fn extract_headers(schema: &Value) -> Vec<String> {
    let items = schema.get("items").and_then(|v| v.as_object());
    let properties = items
        .and_then(|i| i.get("properties"))
        .and_then(|v| v.as_object());
    properties
        .map(|props| props.keys().cloned().collect())
        .unwrap_or_default()
}

fn format_cell(value: Option<&Value>) -> String {
    match value {
        None => String::new(),
        Some(v) => match v {
            Value::String(s) => s.clone(),
            Value::Array(_) | Value::Object(_) => serde_json::to_string(v).unwrap_or_default(),
            other => other.to_string(),
        },
    }
}

fn build_table(headers: &[String], rows: &[Vec<String>]) -> String {
    let mut widths: Vec<usize> = headers
        .iter()
        .map(|h| UnicodeWidthStr::width(h.as_str()))
        .collect();

    for row in rows {
        for (idx, cell) in row.iter().enumerate() {
            let width = UnicodeWidthStr::width(cell.as_str());
            if width > widths[idx] {
                widths[idx] = width;
            }
        }
    }

    let separator = build_separator(&widths);
    let mut out = String::new();
    out.push_str(&separator);
    out.push('\n');
    out.push_str(&build_row(headers, &widths));
    out.push('\n');
    out.push_str(&separator);

    for row in rows {
        out.push('\n');
        out.push_str(&build_row(row, &widths));
    }

    out.push('\n');
    out.push_str(&separator);
    out
}

fn build_separator(widths: &[usize]) -> String {
    let mut line = String::new();
    line.push('+');
    for width in widths {
        line.push_str(&"-".repeat(width + 2));
        line.push('+');
    }
    line
}

fn build_row(cells: &[String], widths: &[usize]) -> String {
    let mut line = String::new();
    line.push('|');
    for (cell, width) in cells.iter().zip(widths) {
        line.push(' ');
        line.push_str(&pad_cell(cell, *width));
        line.push(' ');
        line.push('|');
    }
    line
}

fn pad_cell(cell: &str, width: usize) -> String {
    let current = UnicodeWidthStr::width(cell);
    if current >= width {
        return cell.to_string();
    }
    let mut out = String::with_capacity(cell.len() + (width - current));
    out.push_str(cell);
    out.extend(std::iter::repeat_n(' ', width - current));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn query_body_sets_lint_mode() {
        let body = query_body("select 1", "strict");

        assert_eq!(body["query"], "select 1");
        assert_eq!(body["fmt"], "json");
        assert_eq!(body["lint_mode"], "strict");
        assert_eq!(body["strict_lint_mode"], true);
        assert_eq!(body["query_source"], QUERY_SOURCE);
    }

    #[test]
    fn sql_response_decodes_warnings() {
        let response: SqlResponse = serde_json::from_value(json!({
            "data": [],
            "schema": {"type": "array"},
            "warnings": [{
                "code": "filterPushdown",
                "message": "Rewrite this filter.",
                "stage": "optimizer",
                "severity": "warning"
            }]
        }))
        .unwrap();

        assert_eq!(response.warnings.len(), 1);
        assert_eq!(response.warnings[0].code, "filterPushdown");
        assert_eq!(response.warnings[0].message, "Rewrite this filter.");
    }

    #[test]
    fn formats_strict_lint_http_error() {
        let message = "Brainstore btql/query request failed 400 (Bad Request) after 9 ms: Query blocked by strict lint mode due to 1 lint:\nNo filters available for segment elimination. Add a range filter on created, _xact_id, or _pagination_key, or scope to a specific root_span_id or id. [user_email=test@example.com] [timestamp=123]";

        let formatted = format_strict_lint_message(message).unwrap();

        assert_eq!(
            formatted,
            "Query blocked due to lint failures\n\n  - No filters available for segment elimination. Add a range filter on created, _xact_id, or _pagination_key, or scope to a specific root_span_id or id."
        );
    }

    #[test]
    fn formats_rule_prefixed_strict_lint_lines() {
        let message = "Query blocked by strict lint mode due to 1 lint:\nmissingSegmentEliminationSpecs (optimizer/warning): No filters available for segment elimination.";

        let formatted = format_strict_lint_message(message).unwrap();

        assert_eq!(
            formatted,
            "Query blocked due to lint failures\n\n  - No filters available for segment elimination."
        );
    }
}
