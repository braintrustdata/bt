use std::cmp::Ordering;
use std::fmt::Write as _;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, Timelike, Utc};
use clap::{builder::BoolishValueParser, Args};
use dialoguer::console;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::{
    sql::run_btql_rows,
    ui::{apply_column_padding, header, print_with_pager, styled_table, truncate, with_spinner},
    utils::{format_cost, parse_duration_to_seconds, pluralize},
};

use super::{
    pricing::{format_timestamp, parse_timestamp, PriceBook, TokenUsage},
    ResolvedContext,
};

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Pricing file (USD per 1 million tokens):
  version = 1

  [models.\"custom-chat-model\"]
  aliases = [\"custom-deployment-name\"]

  [[models.\"custom-chat-model\".rates]]
  effective_from = \"2025-01-01T00:00:00Z\"
  effective_until = \"2025-06-01T00:00:00Z\"
  input_usd_per_1m_tokens = 3.0
  cached_input_usd_per_1m_tokens = 0.3
  cache_write_usd_per_1m_tokens = 3.75
  cache_write_5m_usd_per_1m_tokens = 3.75
  cache_write_1h_usd_per_1m_tokens = 6.0
  output_usd_per_1m_tokens = 15.0

Repeat [[models.\"...\".rates]] for historical prices. Bounds are [from, until).
When effective_until is omitted, a rate ends at the next effective_from, or never.
Braintrust's effective estimated cost takes precedence; file rates fill unpriced token spans.")]
pub(crate) struct LogsArgs {
    /// Relative time window ending at --until
    #[arg(long, env = "BRAINTRUST_COST_WINDOW", default_value = "7d")]
    window: String,

    /// Absolute inclusive lower bound (RFC 3339 or YYYY-MM-DD); overrides --window
    #[arg(long, env = "BRAINTRUST_COST_SINCE")]
    since: Option<String>,

    /// Absolute exclusive upper bound (RFC 3339 or YYYY-MM-DD); defaults to now
    #[arg(long, env = "BRAINTRUST_COST_UNTIL")]
    until: Option<String>,

    /// TOML file containing historical per-model token prices
    #[arg(long, env = "BRAINTRUST_COST_PRICING_FILE", value_name = "PATH")]
    pricing_file: Option<PathBuf>,

    /// Exclude spans whose purpose is scorer
    #[arg(
        long,
        env = "BRAINTRUST_COST_EXCLUDE_SCORERS",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    exclude_scorers: bool,
}

impl Default for LogsArgs {
    fn default() -> Self {
        Self {
            window: "7d".to_string(),
            since: None,
            until: None,
            pricing_file: None,
            exclude_scorers: false,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimeRange {
    since: DateTime<Utc>,
    until: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimeSegment {
    since: DateTime<Utc>,
    until: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Coverage {
    Full,
    Partial,
    Unknown,
    None,
}

impl Coverage {
    fn classify(priced_spans: u64, unpriced_token_spans: u64) -> Self {
        match (priced_spans, unpriced_token_spans) {
            (0, 0) => Coverage::None,
            (0, _) => Coverage::Unknown,
            (_, 0) => Coverage::Full,
            (_, _) => Coverage::Partial,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct LogsCostRow {
    model: Option<String>,
    purpose: Option<String>,
    candidate_spans: u64,
    cost: Option<f64>,
    braintrust_cost: Option<f64>,
    file_cost: Option<f64>,
    braintrust_priced_spans: u64,
    file_priced_spans: u64,
    unpriced_token_spans: u64,
    no_usage_spans: u64,
    coverage: Coverage,
}

impl LogsCostRow {
    fn priced_spans(&self) -> u64 {
        self.braintrust_priced_spans + self.file_priced_spans
    }
}

#[derive(Debug, Clone, Serialize)]
struct LogsCostTotals {
    candidate_spans: u64,
    cost: Option<f64>,
    braintrust_cost: Option<f64>,
    file_cost: Option<f64>,
    braintrust_priced_spans: u64,
    file_priced_spans: u64,
    unpriced_token_spans: u64,
    no_usage_spans: u64,
    coverage: Coverage,
}

#[derive(Debug, Serialize)]
struct LogsCostOutput<'a> {
    project: &'a str,
    org: &'a str,
    currency: &'static str,
    since: String,
    until: String,
    excludes_scorers: bool,
    pricing_file: Option<String>,
    rows: &'a [LogsCostRow],
    totals: &'a LogsCostTotals,
}

pub(crate) async fn run(ctx: &ResolvedContext, args: LogsArgs, json: bool) -> Result<()> {
    let now = Utc::now()
        .with_nanosecond(0)
        .expect("zero nanoseconds is a valid timestamp");
    let range = resolve_time_range(&args, now)?;
    let price_book = args
        .pricing_file
        .as_deref()
        .map(PriceBook::load)
        .transpose()?;
    let segments = build_time_segments(range, price_book.as_ref());
    let query = build_cost_query(&ctx.project.id, range, &segments, args.exclude_scorers);
    let result_rows = with_spinner(
        "Estimating log cost...",
        run_btql_rows(&ctx.client, &query, "default"),
    )
    .await?;

    let mut rows = build_cost_rows(&result_rows, &segments, price_book.as_ref())?;
    rows.sort_by(compare_cost_rows);
    let totals = calculate_totals(&rows);

    if json {
        let output = LogsCostOutput {
            project: &ctx.project.name,
            org: ctx.client.org_name(),
            currency: "USD",
            since: format_timestamp(range.since),
            until: format_timestamp(range.until),
            excludes_scorers: args.exclude_scorers,
            pricing_file: args
                .pricing_file
                .as_ref()
                .map(|path| path.display().to_string()),
            rows: &rows,
            totals: &totals,
        };
        println!("{}", serde_json::to_string(&output)?);
        return Ok(());
    }

    print_table(ctx, range, args.pricing_file.as_ref(), &rows, &totals)?;
    Ok(())
}

fn resolve_time_range(args: &LogsArgs, now: DateTime<Utc>) -> Result<TimeRange> {
    let until = args
        .until
        .as_deref()
        .map(parse_timestamp)
        .transpose()
        .context("invalid --until")?
        .unwrap_or(now);
    let since = match args.since.as_deref() {
        Some(since) => parse_timestamp(since).context("invalid --since")?,
        None => {
            let seconds = parse_duration_to_seconds(&args.window)
                .with_context(|| format!("invalid --window '{}'", args.window))?;
            if seconds == 0 {
                bail!("--window must be greater than zero");
            }
            let seconds = i64::try_from(seconds).context("--window is too large")?;
            until
                .checked_sub_signed(Duration::seconds(seconds))
                .context("--window produces a timestamp outside the supported range")?
        }
    };
    if since >= until {
        bail!("--since must be earlier than --until");
    }
    Ok(TimeRange { since, until })
}

fn build_time_segments(range: TimeRange, price_book: Option<&PriceBook>) -> Vec<TimeSegment> {
    let mut boundaries = vec![range.since, range.until];
    if let Some(price_book) = price_book {
        boundaries.extend(price_book.boundaries_between(range.since, range.until));
    }
    boundaries.sort_unstable();
    boundaries.dedup();
    boundaries
        .windows(2)
        .map(|bounds| TimeSegment {
            since: bounds[0],
            until: bounds[1],
        })
        .collect()
}

fn build_cost_query(
    project_id: &str,
    range: TimeRange,
    segments: &[TimeSegment],
    exclude_scorers: bool,
) -> String {
    let prompt_tokens = "COALESCE(metrics.prompt_tokens, 0)";
    let completion_tokens = "COALESCE(metrics.completion_tokens, 0)";
    let cached_tokens = "COALESCE(metrics.prompt_cached_tokens, 0)";
    let generic_write_tokens = "COALESCE(metrics.prompt_cache_creation_tokens, 0)";
    let write_5m_tokens = "COALESCE(metrics.prompt_cache_creation_5m_tokens, 0)";
    let write_1h_tokens = "COALESCE(metrics.prompt_cache_creation_1h_tokens, 0)";
    let split_write_tokens = format!("({write_5m_tokens} + {write_1h_tokens})");
    let effective_write_tokens = format!("GREATEST({generic_write_tokens}, {split_write_tokens})");
    let uncached_input_tokens =
        format!("GREATEST(0, {prompt_tokens} - {cached_tokens} - {effective_write_tokens})");
    let token_activity = [
        "metrics.prompt_tokens IS NOT NULL",
        "metrics.completion_tokens IS NOT NULL",
        "metrics.prompt_cached_tokens IS NOT NULL",
        "metrics.prompt_cache_creation_tokens IS NOT NULL",
        "metrics.prompt_cache_creation_5m_tokens IS NOT NULL",
        "metrics.prompt_cache_creation_1h_tokens IS NOT NULL",
    ]
    .join(" OR ");

    let mut select_fields = vec![
        "metadata.model AS model".to_string(),
        "span_attributes.purpose AS purpose".to_string(),
        format!(
            "COUNT(CASE WHEN estimated_cost() IS NOT NULL OR metadata.model IS NOT NULL OR ({token_activity}) THEN 1 END) AS candidate_spans"
        ),
        "COUNT(estimated_cost()) AS braintrust_priced_spans".to_string(),
        "SUM(estimated_cost()) AS braintrust_cost".to_string(),
        format!(
            "COUNT(CASE WHEN estimated_cost() IS NULL AND ({token_activity}) THEN 1 END) AS unpriced_token_spans"
        ),
        format!(
            "COUNT(CASE WHEN estimated_cost() IS NULL AND metadata.model IS NOT NULL AND NOT ({token_activity}) THEN 1 END) AS no_usage_spans"
        ),
    ];

    for (index, segment) in segments.iter().enumerate() {
        let condition = format!(
            "estimated_cost() IS NULL AND created >= {} AND created < {} AND ({token_activity})",
            sql_quote(&format_timestamp(segment.since)),
            sql_quote(&format_timestamp(segment.until)),
        );
        let split_complete = format!("{split_write_tokens} >= {generic_write_tokens}");
        let fallback_write_tokens =
            format!("CASE WHEN {split_complete} THEN 0 ELSE {effective_write_tokens} END");
        let split_5m = format!("CASE WHEN {split_complete} THEN {write_5m_tokens} ELSE 0 END");
        let split_1h = format!("CASE WHEN {split_complete} THEN {write_1h_tokens} ELSE 0 END");

        select_fields.extend([
            format!("COUNT(CASE WHEN {condition} THEN 1 END) AS p{index}_spans"),
            format!(
                "SUM(CASE WHEN {condition} THEN {uncached_input_tokens} ELSE 0 END) AS p{index}_uncached_input_tokens"
            ),
            format!(
                "SUM(CASE WHEN {condition} THEN {cached_tokens} ELSE 0 END) AS p{index}_cached_input_tokens"
            ),
            format!(
                "SUM(CASE WHEN {condition} THEN {effective_write_tokens} ELSE 0 END) AS p{index}_effective_cache_write_tokens"
            ),
            format!(
                "SUM(CASE WHEN {condition} THEN {split_5m} ELSE 0 END) AS p{index}_split_cache_write_5m_tokens"
            ),
            format!(
                "SUM(CASE WHEN {condition} THEN {split_1h} ELSE 0 END) AS p{index}_split_cache_write_1h_tokens"
            ),
            format!(
                "SUM(CASE WHEN {condition} THEN {fallback_write_tokens} ELSE 0 END) AS p{index}_fallback_cache_write_tokens"
            ),
            format!(
                "SUM(CASE WHEN {condition} THEN {completion_tokens} ELSE 0 END) AS p{index}_output_tokens"
            ),
        ]);
    }

    let scorer_filter = if exclude_scorers {
        "\n  AND (span_attributes.purpose IS NULL OR span_attributes.purpose != 'scorer')"
    } else {
        ""
    };
    format!(
        "SELECT\n  {}\nFROM project_logs({}, shape => 'spans')\nWHERE created >= {}\n  AND created < {}{}\nGROUP BY metadata.model, span_attributes.purpose",
        select_fields.join(",\n  "),
        sql_quote(project_id),
        sql_quote(&format_timestamp(range.since)),
        sql_quote(&format_timestamp(range.until)),
        scorer_filter,
    )
}

fn build_cost_rows(
    result_rows: &[Map<String, Value>],
    segments: &[TimeSegment],
    price_book: Option<&PriceBook>,
) -> Result<Vec<LogsCostRow>> {
    result_rows
        .iter()
        .map(|row| build_cost_row(row, segments, price_book))
        .filter_map(|result| match result {
            Ok(row) if row.candidate_spans == 0 => None,
            other => Some(other),
        })
        .collect()
}

fn build_cost_row(
    row: &Map<String, Value>,
    segments: &[TimeSegment],
    price_book: Option<&PriceBook>,
) -> Result<LogsCostRow> {
    let model = row.get("model").and_then(Value::as_str).map(str::to_string);
    let purpose = row
        .get("purpose")
        .and_then(Value::as_str)
        .map(str::to_string);
    let candidate_spans = value_as_u64(row.get("candidate_spans"));
    let braintrust_priced_spans = value_as_u64(row.get("braintrust_priced_spans"));
    let braintrust_cost = value_as_opt_f64(row.get("braintrust_cost"));
    let expected_unpriced_token_spans = value_as_u64(row.get("unpriced_token_spans"));
    let no_usage_spans = value_as_u64(row.get("no_usage_spans"));

    let mut file_cost = 0.0;
    let mut file_priced_spans = 0_u64;
    let mut unpriced_token_spans = 0_u64;
    let mut segmented_token_spans = 0_u64;

    for (index, segment) in segments.iter().enumerate() {
        let usage = token_usage_from_row(row, index);
        segmented_token_spans = segmented_token_spans.saturating_add(usage.spans);
        let rates = model.as_deref().and_then(|model| {
            price_book.and_then(|price_book| price_book.rate_at(model, segment.since))
        });
        if let Some(rates) = rates {
            file_cost += usage.cost(rates);
            file_priced_spans = file_priced_spans.saturating_add(usage.spans);
        } else {
            unpriced_token_spans = unpriced_token_spans.saturating_add(usage.spans);
        }
    }

    if segmented_token_spans != expected_unpriced_token_spans {
        bail!(
            "cost query returned inconsistent token coverage for model {}: expected {}, got {}",
            model.as_deref().unwrap_or("<missing>"),
            expected_unpriced_token_spans,
            segmented_token_spans
        );
    }

    let priced_spans = braintrust_priced_spans.saturating_add(file_priced_spans);
    let cost = (priced_spans > 0).then(|| braintrust_cost.unwrap_or(0.0) + file_cost);
    Ok(LogsCostRow {
        model,
        purpose,
        candidate_spans,
        cost,
        braintrust_cost: (braintrust_priced_spans > 0).then(|| braintrust_cost.unwrap_or(0.0)),
        file_cost: (file_priced_spans > 0).then_some(file_cost),
        braintrust_priced_spans,
        file_priced_spans,
        unpriced_token_spans,
        no_usage_spans,
        coverage: Coverage::classify(priced_spans, unpriced_token_spans),
    })
}

fn token_usage_from_row(row: &Map<String, Value>, index: usize) -> TokenUsage {
    let value = |suffix: &str| value_as_u64(row.get(&format!("p{index}_{suffix}")));
    TokenUsage {
        spans: value("spans"),
        uncached_input_tokens: value("uncached_input_tokens"),
        cached_input_tokens: value("cached_input_tokens"),
        effective_cache_write_tokens: value("effective_cache_write_tokens"),
        split_cache_write_5m_tokens: value("split_cache_write_5m_tokens"),
        split_cache_write_1h_tokens: value("split_cache_write_1h_tokens"),
        fallback_cache_write_tokens: value("fallback_cache_write_tokens"),
        output_tokens: value("output_tokens"),
    }
}

fn calculate_totals(rows: &[LogsCostRow]) -> LogsCostTotals {
    let braintrust_priced_spans = rows.iter().map(|row| row.braintrust_priced_spans).sum();
    let file_priced_spans = rows.iter().map(|row| row.file_priced_spans).sum();
    let unpriced_token_spans = rows.iter().map(|row| row.unpriced_token_spans).sum();
    let priced_spans = braintrust_priced_spans + file_priced_spans;
    let braintrust_cost = (braintrust_priced_spans > 0)
        .then(|| rows.iter().filter_map(|row| row.braintrust_cost).sum());
    let file_cost =
        (file_priced_spans > 0).then(|| rows.iter().filter_map(|row| row.file_cost).sum());
    LogsCostTotals {
        candidate_spans: rows.iter().map(|row| row.candidate_spans).sum(),
        cost: (priced_spans > 0).then(|| braintrust_cost.unwrap_or(0.0) + file_cost.unwrap_or(0.0)),
        braintrust_cost,
        file_cost,
        braintrust_priced_spans,
        file_priced_spans,
        unpriced_token_spans,
        no_usage_spans: rows.iter().map(|row| row.no_usage_spans).sum(),
        coverage: Coverage::classify(priced_spans, unpriced_token_spans),
    }
}

fn compare_cost_rows(left: &LogsCostRow, right: &LogsCostRow) -> Ordering {
    let left_cost = left.cost.unwrap_or(f64::NEG_INFINITY);
    let right_cost = right.cost.unwrap_or(f64::NEG_INFINITY);
    right_cost
        .partial_cmp(&left_cost)
        .unwrap_or(Ordering::Equal)
        .then_with(|| left.model.cmp(&right.model))
        .then_with(|| left.purpose.cmp(&right.purpose))
}

fn print_table(
    ctx: &ResolvedContext,
    range: TimeRange,
    pricing_file: Option<&PathBuf>,
    rows: &[LogsCostRow],
    totals: &LogsCostTotals,
) -> Result<()> {
    let mut output = String::new();
    let count = format!(
        "{} {}",
        rows.len(),
        pluralize(rows.len(), "cost group", None)
    );
    writeln!(
        output,
        "{} in {} {} {}",
        console::style(count),
        console::style(ctx.client.org_name()).bold(),
        console::style("/").dim().bold(),
        console::style(&ctx.project.name).bold()
    )?;
    writeln!(
        output,
        "{} {} {}",
        console::style(format_timestamp(range.since)).dim(),
        console::style("to").dim(),
        console::style(format_timestamp(range.until)).dim()
    )?;
    if let Some(path) = pricing_file {
        writeln!(output, "Pricing: {}", console::style(path.display()).dim())?;
    }
    output.push('\n');

    let mut table = styled_table();
    table.set_header(vec![
        header("Model"),
        header("Purpose"),
        header("Cost"),
        header("Coverage"),
        header("Sources"),
    ]);
    apply_column_padding(&mut table, (0, 4));

    for row in rows {
        table.add_row(vec![
            truncate(row.model.as_deref().unwrap_or("<missing>"), 50),
            truncate(row.purpose.as_deref().unwrap_or("default"), 20),
            cost_display(row.cost, row.coverage),
            coverage_display(row.priced_spans(), row.unpriced_token_spans, row.coverage),
            source_display(row),
        ]);
    }
    write!(output, "{table}")?;

    let total_cost = totals
        .cost
        .map(format_cost)
        .unwrap_or_else(|| "n/a".to_string());
    let total_prefix = if totals.unpriced_token_spans > 0 {
        "~"
    } else {
        ""
    };
    write!(
        output,
        "\n\nTotal: {}",
        console::style(format!("{total_prefix}{total_cost}")).bold()
    )?;
    if let Some(file_cost) = totals.file_cost {
        write!(
            output,
            "  {}",
            console::style(format!("{} from pricing file", format_cost(file_cost))).dim()
        )?;
    }
    if totals.unpriced_token_spans > 0 {
        write!(
            output,
            "  {}",
            console::style(format!(
                "{} unpriced token {}",
                totals.unpriced_token_spans,
                pluralize(totals.unpriced_token_spans as usize, "span", None)
            ))
            .yellow()
        )?;
    }
    if totals.no_usage_spans > 0 {
        write!(
            output,
            "  {}",
            console::style(format!("{} without cost or usage", totals.no_usage_spans)).dim()
        )?;
    }
    output.push('\n');

    print_with_pager(&output)?;
    Ok(())
}

fn cost_display(cost: Option<f64>, coverage: Coverage) -> String {
    match (cost, coverage) {
        (Some(cost), Coverage::Partial) => format!("~{}", format_cost(cost)),
        (Some(cost), _) => format_cost(cost),
        (None, Coverage::Unknown) => "unknown".to_string(),
        (None, _) => "n/a".to_string(),
    }
}

fn coverage_display(priced_spans: u64, unpriced_token_spans: u64, coverage: Coverage) -> String {
    match coverage {
        Coverage::None => "—".to_string(),
        _ => format!("{}/{}", priced_spans, priced_spans + unpriced_token_spans),
    }
}

fn source_display(row: &LogsCostRow) -> String {
    let mut parts = Vec::new();
    if row.braintrust_priced_spans > 0 {
        parts.push(format!("Braintrust {}", row.braintrust_priced_spans));
    }
    if row.file_priced_spans > 0 {
        parts.push(format!("file {}", row.file_priced_spans));
    }
    if row.no_usage_spans > 0 {
        parts.push(format!("no usage {}", row.no_usage_spans));
    }
    if parts.is_empty() {
        "—".to_string()
    } else {
        parts.join(", ")
    }
}

fn value_as_opt_f64(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Number(number)) => number.as_f64(),
        Some(Value::String(value)) => value.parse().ok(),
        _ => None,
    }
    .filter(|value| value.is_finite() && *value >= 0.0)
}

fn value_as_u64(value: Option<&Value>) -> u64 {
    match value {
        Some(Value::Number(number)) => number
            .as_u64()
            .or_else(|| number.as_f64().map(|value| value.max(0.0) as u64))
            .unwrap_or(0),
        Some(Value::String(value)) => value.parse().unwrap_or(0),
        _ => 0,
    }
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    fn range() -> TimeRange {
        TimeRange {
            since: parse_timestamp("2025-01-01").unwrap(),
            until: parse_timestamp("2025-03-01").unwrap(),
        }
    }

    #[test]
    fn query_is_timestamp_bounded_and_uses_spans() {
        let query = build_cost_query(
            "test-project-id",
            range(),
            &[TimeSegment {
                since: range().since,
                until: range().until,
            }],
            false,
        );
        assert!(
            query.contains("FROM project_logs('test-project-id', shape => 'spans')"),
            "{query}"
        );
        assert!(
            query.contains("created >= '2025-01-01T00:00:00Z'"),
            "{query}"
        );
        assert!(
            query.contains("created < '2025-03-01T00:00:00Z'"),
            "{query}"
        );
        assert!(
            query.contains("SUM(estimated_cost()) AS braintrust_cost"),
            "{query}"
        );
        assert!(query.contains("p0_uncached_input_tokens"), "{query}");
        assert!(!query.contains("purpose != 'scorer'"), "{query}");
    }

    #[test]
    fn query_can_exclude_scorers() {
        let query = build_cost_query("test-project-id", range(), &[], true);
        assert!(
            query
                .contains("span_attributes.purpose IS NULL OR span_attributes.purpose != 'scorer'"),
            "{query}"
        );
    }

    #[test]
    fn time_range_uses_until_as_window_anchor() {
        let args = LogsArgs {
            window: "1d".to_string(),
            until: Some("2025-02-02T12:00:00Z".to_string()),
            ..LogsArgs::default()
        };
        let resolved = resolve_time_range(&args, parse_timestamp("2030-01-01").unwrap()).unwrap();
        assert_eq!(
            resolved.since,
            parse_timestamp("2025-02-01T12:00:00Z").unwrap()
        );
        assert_eq!(
            resolved.until,
            parse_timestamp("2025-02-02T12:00:00Z").unwrap()
        );
    }

    #[test]
    fn historical_file_rates_are_applied_to_the_matching_segment() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            r#"
version = 1

[models."test-model"]
[[models."test-model".rates]]
effective_from = "2025-01-01"
input_usd_per_1m_tokens = 1.0
output_usd_per_1m_tokens = 2.0

[[models."test-model".rates]]
effective_from = "2025-02-01"
input_usd_per_1m_tokens = 3.0
output_usd_per_1m_tokens = 4.0
"#,
        )
        .unwrap();
        let price_book = PriceBook::load(file.path()).unwrap();
        let segments = build_time_segments(range(), Some(&price_book));
        assert_eq!(segments.len(), 2);

        let row = json!({
            "model": "test-model",
            "purpose": null,
            "candidate_spans": 3,
            "braintrust_priced_spans": 1,
            "braintrust_cost": 0.5,
            "unpriced_token_spans": 2,
            "no_usage_spans": 0,
            "p0_spans": 1,
            "p0_uncached_input_tokens": 1000000,
            "p0_cached_input_tokens": 0,
            "p0_effective_cache_write_tokens": 0,
            "p0_split_cache_write_5m_tokens": 0,
            "p0_split_cache_write_1h_tokens": 0,
            "p0_fallback_cache_write_tokens": 0,
            "p0_output_tokens": 1000000,
            "p1_spans": 1,
            "p1_uncached_input_tokens": 1000000,
            "p1_cached_input_tokens": 0,
            "p1_effective_cache_write_tokens": 0,
            "p1_split_cache_write_5m_tokens": 0,
            "p1_split_cache_write_1h_tokens": 0,
            "p1_fallback_cache_write_tokens": 0,
            "p1_output_tokens": 1000000
        })
        .as_object()
        .unwrap()
        .clone();
        let cost_row = build_cost_row(&row, &segments, Some(&price_book)).unwrap();
        assert_eq!(cost_row.braintrust_cost, Some(0.5));
        assert_eq!(cost_row.file_cost, Some(10.0));
        assert_eq!(cost_row.cost, Some(10.5));
        assert_eq!(cost_row.file_priced_spans, 2);
        assert_eq!(cost_row.coverage, Coverage::Full);
    }

    #[test]
    fn missing_historical_rate_remains_unpriced() {
        let row = json!({
            "model": null,
            "candidate_spans": 1,
            "braintrust_priced_spans": 0,
            "braintrust_cost": null,
            "unpriced_token_spans": 1,
            "no_usage_spans": 0,
            "p0_spans": 1,
            "p0_uncached_input_tokens": 10,
            "p0_cached_input_tokens": 0,
            "p0_effective_cache_write_tokens": 0,
            "p0_split_cache_write_5m_tokens": 0,
            "p0_split_cache_write_1h_tokens": 0,
            "p0_fallback_cache_write_tokens": 0,
            "p0_output_tokens": 5
        })
        .as_object()
        .unwrap()
        .clone();
        let segments = vec![TimeSegment {
            since: range().since,
            until: range().until,
        }];
        let cost_row = build_cost_row(&row, &segments, None).unwrap();
        assert_eq!(cost_row.cost, None);
        assert_eq!(cost_row.unpriced_token_spans, 1);
        assert_eq!(cost_row.coverage, Coverage::Unknown);
    }

    #[test]
    fn partial_cost_is_marked_approximate() {
        assert_eq!(cost_display(Some(1.25), Coverage::Partial), "~$1.25");
        assert_eq!(cost_display(None, Coverage::Unknown), "unknown");
        assert_eq!(coverage_display(3, 2, Coverage::Partial), "3/5");
    }
}
