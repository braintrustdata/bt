//! `bt cost` — estimate LLM spend across a project's logs, topics, experiments,
//! and playgrounds.
//!
//! Cost is one query: `SUM(cost) GROUP BY <dims> WHERE <filters> AND <time>`.
//! Logs, topics, and experiments share the `project()` amalgam (one query, no
//! experiment enumeration); playgrounds are a separate `playground_logs(...)`
//! table that needs a prompt-session enumeration first.

use std::collections::BTreeMap;
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Duration, FixedOffset, Local, Timelike, Utc};
use clap::Args;

use crate::{
    args::BaseArgs,
    project_context::resolve_project_command_context_with_auth_mode,
    sql::{run_btql_rows_with_limit, RowLimit, TzOffset},
    ui::with_spinner,
    utils::parse_duration_to_seconds,
};

mod pricing;
mod query;
mod render;
mod rows;
mod sessions;

pub(crate) use crate::project_context::ProjectContext as ResolvedContext;

use pricing::{format_timestamp_in_offset, parse_timestamp_in_offset, PriceBook};

#[cfg(test)]
use pricing::parse_timestamp;
use query::{
    build_query, internal_dimensions, playground_from, project_from, Dimension, Filters, Source,
    TableKind, TimeRange, TimeSegment,
};

/// Default backend row cap for grouped queries (mirrors `DEFAULT_LIMIT` in
/// `api-ts/src/btql.ts`). Used to detect truncation.
const BACKEND_DEFAULT_LIMIT: usize = 1000;

#[derive(Debug, Clone, Args)]
#[command(
    about = "Estimate LLM cost for the active project",
    after_help = "\
Examples:
  bt cost                              Total spend over the last 7 days, by model
  bt cost --group-by source,model      Break down by cost pool and model
  bt cost --source experiments         Only experiment (eval) cost
  bt cost --group-by day --window 30d  Daily spend over 30 days
  bt cost --group-by trace --no-limit  Per-trace cost, all groups
  bt cost --pricing-file prices.toml   Price models Braintrust doesn't know

Pricing file (USD per 1 million tokens):
  version = 1

  [models.\"my-model\"]
  aliases = [\"my-model-preview\"]

  [[models.\"my-model\".rates]]
  effective_from = \"2025-01-01T00:00:00Z\"
  input_usd_per_1m_tokens = 0.6
  output_usd_per_1m_tokens = 2.2

Braintrust's logged/registry cost takes precedence; file rates fill unpriced spans.
Run `bt cost --help` for the complete pricing-file schema.",
    after_long_help = "\
Examples:
  bt cost                              Total spend over the last 7 days, by model
  bt cost --group-by source,model      Break down by cost pool and model
  bt cost --source experiments         Only experiment (eval) cost
  bt cost --group-by day --window 30d  Daily spend over 30 days
  bt cost --group-by trace --no-limit  Per-trace cost, all groups
  bt cost --pricing-file prices.toml   Price models Braintrust doesn't know

Pricing file:
  Prices models that Braintrust's logged/registry cost cannot price. Braintrust's
  cost always takes precedence; file rates only fill spans left otherwise unpriced.
  All rates are USD per 1,000,000 tokens. Model keys match metadata.model
  case-insensitively; use `aliases` for alternate logged names.

  version = 1                        # required; only version 1 is supported

  [models.\"<model-as-logged>\"]      # one table per model
  aliases = [\"<other-logged-name>\"] # optional; case-insensitive alternate names

  [[models.\"<model-as-logged>\".rates]]      # one or more, oldest to newest
  effective_from = \"2025-01-01T00:00:00Z\"    # required; RFC 3339 or YYYY-MM-DD, inclusive
  effective_until = \"2025-06-01T00:00:00Z\"   # optional; exclusive upper bound. Omit to run
                                             #   until the next rate's effective_from, or forever
  input_usd_per_1m_tokens = 3.0              # required; uncached input (prompt) tokens
  output_usd_per_1m_tokens = 15.0            # required; completion tokens
  cached_input_usd_per_1m_tokens = 0.3       # optional; cache-read tokens (default: input rate)
  cache_write_usd_per_1m_tokens = 3.75       # optional; cache-write tokens (default: input rate)
  cache_write_5m_usd_per_1m_tokens = 3.75    # optional; 5-minute-TTL cache writes
  cache_write_1h_usd_per_1m_tokens = 6.0     # optional; 1-hour-TTL cache writes

  Add more [[models.\"...\".rates]] blocks for historical prices; each interval is
  [effective_from, effective_until). Unknown fields are rejected."
)]
pub struct CostArgs {
    /// Relative time window ending at --until
    #[arg(long, env = "BRAINTRUST_COST_WINDOW", default_value = "7d")]
    window: String,

    /// Absolute inclusive lower bound (RFC 3339 or YYYY-MM-DD); overrides --window
    #[arg(long)]
    since: Option<String>,

    /// Absolute exclusive upper bound (RFC 3339 or YYYY-MM-DD); defaults to now
    #[arg(long)]
    until: Option<String>,

    /// Cost pools to include (default: all)
    #[arg(long = "source", value_enum, value_delimiter = ',')]
    sources: Vec<Source>,

    /// Break cost down by these dimensions (comma-separated or repeated)
    #[arg(
        long = "group-by",
        value_enum,
        value_delimiter = ',',
        default_value = "model"
    )]
    group_by: Vec<Dimension>,

    /// Only count spans for these models (repeatable)
    #[arg(long = "model")]
    models: Vec<String>,

    /// Only count spans of these span types, e.g. llm (repeatable)
    #[arg(long = "type")]
    types: Vec<String>,

    /// Only count spans with these purposes, e.g. scorer (repeatable)
    #[arg(long = "purpose")]
    purposes: Vec<String>,

    /// Exclude spans whose purpose is scorer
    #[arg(long)]
    exclude_scorers: bool,

    /// Cap the number of breakdown rows each query returns
    #[arg(long, conflicts_with = "no_limit")]
    limit: Option<u64>,

    /// Return every group, bypassing the backend row cap
    #[arg(long = "no-limit")]
    no_limit: bool,

    /// TOML file of per-model token prices for models Braintrust cannot price
    #[arg(long, env = "BRAINTRUST_COST_PRICING_FILE", value_name = "PATH")]
    pricing_file: Option<PathBuf>,

    /// Time zone for day/hour buckets, bare YYYY-MM-DD bounds, and display:
    /// utc, local, or a fixed offset like -07:00
    #[arg(
        long = "timezone",
        env = "BRAINTRUST_COST_TIMEZONE",
        default_value = "utc"
    )]
    timezone: String,
}

pub async fn run(base: BaseArgs, args: CostArgs) -> Result<()> {
    let now = Utc::now()
        .with_nanosecond(0)
        .expect("zero nanoseconds is a valid timestamp");
    let offset = parse_timezone(&args.timezone)?;
    let tz_offset = btql_tz_offset(offset);
    let range = resolve_time_range(&args, now, offset)?;

    let price_book = args
        .pricing_file
        .as_deref()
        .map(PriceBook::load)
        .transpose()?;
    let segments = build_time_segments(range, price_book.as_ref());

    let display_dims = dedup_dimensions(&args.group_by);
    let internal_dims = internal_dimensions(&display_dims, price_book.is_some());
    let sources = resolve_sources(&args.sources);
    let row_limit = resolve_row_limit(&args);

    let filters = Filters {
        sources: sources.clone(),
        models: args.models.clone(),
        types: args.types.clone(),
        purposes: args.purposes.clone(),
        exclude_scorers: args.exclude_scorers,
    };

    let ctx = resolve_project_command_context_with_auth_mode(&base, true).await?;

    let mut accumulator: BTreeMap<Vec<Option<String>>, rows::CostRow> = BTreeMap::new();
    let mut candidate_spans = 0_u64;
    let mut truncated = false;

    // Logs / topics / experiments live in the project() amalgam: one query.
    if sources
        .iter()
        .any(|source| !matches!(source, Source::Playgrounds))
    {
        let query = build_query(
            TableKind::Project,
            &project_from(&ctx.project.id),
            range,
            &segments,
            &internal_dims,
            &filters,
        );
        let result = with_spinner(
            "Estimating cost...",
            run_btql_rows_with_limit(&ctx.client, &query, "default", row_limit, tz_offset),
        )
        .await?;
        truncated |= is_truncated(result.len(), row_limit);
        candidate_spans = candidate_spans.saturating_add(rows::accumulate_rows(
            &mut accumulator,
            &result,
            &internal_dims,
            &display_dims,
            &segments,
            price_book.as_ref(),
        ));
    }

    // Playgrounds are a separate table keyed by prompt-session id.
    if sources.contains(&Source::Playgrounds) {
        let session_ids = with_spinner(
            "Listing playgrounds...",
            sessions::list_prompt_session_ids(&ctx.client, &ctx.project.id),
        )
        .await?;
        if !session_ids.is_empty() {
            let query = build_query(
                TableKind::Playground,
                &playground_from(&session_ids),
                range,
                &segments,
                &internal_dims,
                &filters,
            );
            let result = with_spinner(
                "Estimating playground cost...",
                run_btql_rows_with_limit(&ctx.client, &query, "default", row_limit, tz_offset),
            )
            .await?;
            truncated |= is_truncated(result.len(), row_limit);
            candidate_spans = candidate_spans.saturating_add(rows::accumulate_rows(
                &mut accumulator,
                &result,
                &internal_dims,
                &display_dims,
                &segments,
                price_book.as_ref(),
            ));
        }
    }

    let mut cost_rows: Vec<rows::CostRow> = accumulator.into_values().collect();
    rows::sort_rows(&mut cost_rows);
    let totals = rows::totals_from_rows(&cost_rows, candidate_spans);

    let source_labels: Vec<String> = sources
        .iter()
        .map(|source| source.label().to_string())
        .collect();
    let report = render::Report {
        ctx: &ctx,
        since: format_timestamp_in_offset(range.since, offset),
        until: format_timestamp_in_offset(range.until, offset),
        display_dims: &display_dims,
        sources: &source_labels,
        rows: &cost_rows,
        totals: &totals,
        pricing_file: args.pricing_file.as_deref(),
        truncated,
        verbose: base.verbose,
    };

    if base.json {
        render::print_json(&report)
    } else {
        render::print_table(&report)
    }
}

/// All sources when none are specified; otherwise the deduped request.
fn resolve_sources(requested: &[Source]) -> Vec<Source> {
    if requested.is_empty() {
        return vec![
            Source::Logs,
            Source::Topics,
            Source::Experiments,
            Source::Playgrounds,
        ];
    }
    let mut seen = Vec::new();
    for source in requested {
        if !seen.contains(source) {
            seen.push(*source);
        }
    }
    seen
}

fn dedup_dimensions(requested: &[Dimension]) -> Vec<Dimension> {
    let mut seen = Vec::new();
    for dim in requested {
        if !seen.contains(dim) {
            seen.push(*dim);
        }
    }
    if seen.is_empty() {
        seen.push(Dimension::Model);
    }
    seen
}

/// Parse `--timezone` into a fixed offset. Accepts `utc`, `local`, or a fixed
/// offset like `-07:00`, `-7`, or `+0530`. IANA names are not supported because
/// BTQL buckets by a single numeric offset, so a name would collapse to one
/// offset anyway (losing DST correctness within the window).
fn parse_timezone(value: &str) -> Result<FixedOffset> {
    let trimmed = value.trim();
    match trimmed.to_ascii_lowercase().as_str() {
        "utc" | "z" | "" => Ok(FixedOffset::east_opt(0).expect("UTC is valid")),
        "local" => Ok(*Local::now().offset()),
        _ => parse_fixed_offset(trimmed),
    }
}

fn parse_fixed_offset(value: &str) -> Result<FixedOffset> {
    let (sign, rest) = match value.strip_prefix('-') {
        Some(rest) => (-1, rest),
        None => (1, value.strip_prefix('+').unwrap_or(value)),
    };
    let (hours, minutes) = if let Some((hours, minutes)) = rest.split_once(':') {
        (hours, minutes)
    } else if rest.len() == 4 {
        rest.split_at(2)
    } else {
        (rest, "0")
    };
    let hours: i32 = hours
        .parse()
        .with_context(|| format!("invalid --timezone offset '{value}'"))?;
    let minutes: i32 = minutes
        .parse()
        .with_context(|| format!("invalid --timezone offset '{value}'"))?;
    if !(0..=14).contains(&hours) || !(0..=59).contains(&minutes) {
        bail!("--timezone offset '{value}' is out of range");
    }
    let seconds = sign * (hours * 3600 + minutes * 60);
    FixedOffset::east_opt(seconds).with_context(|| format!("invalid --timezone offset '{value}'"))
}

/// Convert a fixed offset to BTQL's `tz_offset` (minutes to add to local to
/// reach UTC; UTC-7 is `+420`). Returns `None` for UTC so no field is sent.
fn btql_tz_offset(offset: FixedOffset) -> Option<TzOffset> {
    let minutes_east = offset.local_minus_utc() / 60;
    (minutes_east != 0).then_some(TzOffset(-minutes_east))
}

fn resolve_row_limit(args: &CostArgs) -> RowLimit {
    if args.no_limit {
        RowLimit::Disabled
    } else if let Some(limit) = args.limit {
        RowLimit::Explicit(limit)
    } else {
        RowLimit::Default
    }
}

/// A query is truncated when it returned as many rows as its cap allowed.
fn is_truncated(row_count: usize, limit: RowLimit) -> bool {
    match limit {
        RowLimit::Disabled => false,
        RowLimit::Default => row_count >= BACKEND_DEFAULT_LIMIT,
        RowLimit::Explicit(limit) => row_count as u64 >= limit,
    }
}

fn resolve_time_range(
    args: &CostArgs,
    now: DateTime<Utc>,
    offset: FixedOffset,
) -> Result<TimeRange> {
    let until = args
        .until
        .as_deref()
        .map(|value| parse_timestamp_in_offset(value, offset))
        .transpose()
        .context("invalid --until")?
        .unwrap_or(now);
    let since = match args.since.as_deref() {
        Some(since) => parse_timestamp_in_offset(since, offset).context("invalid --since")?,
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

/// Split the window at the pricing file's rate boundaries so each segment maps
/// to one rate. Returns empty when there is no pricing file (no file cost to
/// compute), which suppresses the per-segment token columns entirely.
fn build_time_segments(range: TimeRange, price_book: Option<&PriceBook>) -> Vec<TimeSegment> {
    let Some(price_book) = price_book else {
        return Vec::new();
    };
    let mut boundaries = vec![range.since, range.until];
    boundaries.extend(price_book.boundaries_between(range.since, range.until));
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

#[cfg(test)]
mod tests {
    use super::*;

    fn base_args() -> CostArgs {
        CostArgs {
            window: "7d".to_string(),
            since: None,
            until: None,
            sources: Vec::new(),
            group_by: vec![Dimension::Model],
            models: Vec::new(),
            types: Vec::new(),
            purposes: Vec::new(),
            exclude_scorers: false,
            limit: None,
            no_limit: false,
            pricing_file: None,
            timezone: "utc".to_string(),
        }
    }

    fn utc() -> FixedOffset {
        FixedOffset::east_opt(0).unwrap()
    }

    #[test]
    fn empty_sources_expand_to_all() {
        assert_eq!(
            resolve_sources(&[]),
            vec![
                Source::Logs,
                Source::Topics,
                Source::Experiments,
                Source::Playgrounds
            ]
        );
    }

    #[test]
    fn sources_are_deduped_and_ordered() {
        assert_eq!(
            resolve_sources(&[Source::Logs, Source::Logs, Source::Experiments]),
            vec![Source::Logs, Source::Experiments]
        );
    }

    #[test]
    fn row_limit_flags_map_to_variants() {
        assert_eq!(resolve_row_limit(&base_args()), RowLimit::Default);
        let mut disabled = base_args();
        disabled.no_limit = true;
        assert_eq!(resolve_row_limit(&disabled), RowLimit::Disabled);
        let mut explicit = base_args();
        explicit.limit = Some(50);
        assert_eq!(resolve_row_limit(&explicit), RowLimit::Explicit(50));
    }

    #[test]
    fn truncation_detects_full_pages() {
        assert!(is_truncated(BACKEND_DEFAULT_LIMIT, RowLimit::Default));
        assert!(!is_truncated(BACKEND_DEFAULT_LIMIT - 1, RowLimit::Default));
        assert!(!is_truncated(100_000, RowLimit::Disabled));
        assert!(is_truncated(10, RowLimit::Explicit(10)));
    }

    #[test]
    fn window_is_anchored_at_until() {
        let mut args = base_args();
        args.window = "1d".to_string();
        args.until = Some("2025-02-02T12:00:00Z".to_string());
        let range =
            resolve_time_range(&args, parse_timestamp("2030-01-01").unwrap(), utc()).unwrap();
        assert_eq!(
            range.since,
            parse_timestamp("2025-02-01T12:00:00Z").unwrap()
        );
        assert_eq!(
            range.until,
            parse_timestamp("2025-02-02T12:00:00Z").unwrap()
        );
    }

    #[test]
    fn bare_dates_use_the_requested_timezone() {
        let offset = parse_timezone("-07:00").unwrap();
        let mut args = base_args();
        args.since = Some("2026-07-20".to_string());
        args.until = Some("2026-07-21".to_string());
        let range = resolve_time_range(&args, Utc::now(), offset).unwrap();
        // Midnight in UTC-7 is 07:00 UTC.
        assert_eq!(
            range.since,
            parse_timestamp("2026-07-20T07:00:00Z").unwrap()
        );
        assert_eq!(
            range.until,
            parse_timestamp("2026-07-21T07:00:00Z").unwrap()
        );
    }

    #[test]
    fn timezone_parses_offsets_and_keywords() {
        assert_eq!(parse_timezone("utc").unwrap().local_minus_utc(), 0);
        assert_eq!(
            parse_timezone("-07:00").unwrap().local_minus_utc(),
            -7 * 3600
        );
        assert_eq!(parse_timezone("-7").unwrap().local_minus_utc(), -7 * 3600);
        assert_eq!(
            parse_timezone("+0530").unwrap().local_minus_utc(),
            5 * 3600 + 30 * 60
        );
        assert!(parse_timezone("America/Los_Angeles").is_err());
    }

    #[test]
    fn btql_tz_offset_uses_gettimezoneoffset_convention() {
        // UTC-7 -> +420; UTC is None (no field sent).
        assert_eq!(
            btql_tz_offset(parse_timezone("-07:00").unwrap()),
            Some(TzOffset(420))
        );
        assert_eq!(
            btql_tz_offset(parse_timezone("+05:30").unwrap()),
            Some(TzOffset(-330))
        );
        assert_eq!(btql_tz_offset(utc()), None);
    }

    #[test]
    fn group_by_defaults_to_model() {
        assert_eq!(dedup_dimensions(&[]), vec![Dimension::Model]);
        assert_eq!(
            dedup_dimensions(&[Dimension::Source, Dimension::Source, Dimension::Model]),
            vec![Dimension::Source, Dimension::Model]
        );
    }
}
