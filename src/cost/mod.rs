//! `bt cost` — estimate LLM spend across a project's logs, topics, experiments,
//! and playgrounds.
//!
//! Cost is one query: `SUM(cost) GROUP BY <dims> WHERE <filters> AND <time>`.
//! Logs, topics, and experiments share the `project()` amalgam (one query, no
//! experiment enumeration); playgrounds are a separate `playground_logs(...)`
//! table that needs a prompt-session enumeration first.

use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use chrono::{DateTime, Datelike, Duration, FixedOffset, Local, NaiveDate, Timelike, Utc};
use clap::Args;

use crate::{
    args::BaseArgs,
    project_context::resolve_project_command_context_with_auth_mode,
    sql::{run_btql_rows_with_limit, RowLimit, TzOffset},
    ui::{print_command_status, with_spinner, CommandStatus},
    utils::parse_duration_to_seconds,
};

mod plot;
mod pricing;
mod query;
mod render;
mod rows;
mod sessions;
mod termimage;

use termimage::ImageMode;

pub(crate) use crate::project_context::ProjectContext as ResolvedContext;

use pricing::{format_timestamp, format_timestamp_in_offset, parse_timestamp_in_offset, PriceBook};

#[cfg(test)]
use pricing::parse_timestamp;
use query::{
    build_query, internal_dimensions, playground_from, project_from, Dimension, Filters, Source,
    TableKind, TimeRange, TimeSegment,
};

/// Default backend row cap for grouped queries (mirrors `DEFAULT_LIMIT` in
/// `api-ts/src/btql.ts`). Used to detect truncation.
const BACKEND_DEFAULT_LIMIT: usize = 1000;

/// High-resolution fallback for inline images when the terminal doesn't report
/// its pixel size. Rendering large and letting the terminal downscale stays
/// crisp (unlike upscaling a small image).
const IMAGE_FALLBACK_SIZE: (u32, u32) = (2400, 1200);

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
  bt cost --group-by day --plot        Chart daily spend in the console
  bt cost --group-by day --save-fig cost.svg  Save the chart as SVG (or .png)
  bt cost --group-by model --csv > cost.csv   Export the breakdown as CSV
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
  bt cost --group-by day --plot        Chart daily spend in the console
  bt cost --group-by day --save-fig cost.svg  Save the chart as SVG (or .png)
  bt cost --group-by model --csv > cost.csv   Export the breakdown as CSV
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
    /// Relative time window ending at --until, e.g. 7d, 24h, 90m
    #[arg(
        long,
        env = "BRAINTRUST_COST_WINDOW",
        default_value = "7d",
        value_name = "DURATION"
    )]
    window: String,

    /// Absolute inclusive lower bound (RFC 3339 or YYYY-MM-DD); overrides --window
    #[arg(long, value_name = "DATE")]
    since: Option<String>,

    /// Absolute exclusive upper bound (RFC 3339 or YYYY-MM-DD); defaults to now
    #[arg(long, value_name = "DATE")]
    until: Option<String>,

    /// Cost pools to include (default: all)
    #[arg(
        long = "source",
        value_enum,
        value_delimiter = ',',
        value_name = "SOURCE"
    )]
    sources: Vec<Source>,

    /// Break cost down by these dimensions (comma-separated or repeated)
    #[arg(
        long = "group-by",
        value_enum,
        value_delimiter = ',',
        default_value = "model",
        value_name = "DIMENSION"
    )]
    group_by: Vec<Dimension>,

    /// Only count spans for these models (repeatable)
    #[arg(long = "model", value_name = "MODEL")]
    models: Vec<String>,

    /// Only count spans of these span types, e.g. llm or score (repeatable)
    #[arg(long = "type", value_name = "TYPE")]
    types: Vec<String>,

    /// Cap the number of breakdown rows each query returns
    #[arg(long, conflicts_with = "no_limit", value_name = "N")]
    limit: Option<u64>,

    /// Return every group, bypassing the backend row cap
    #[arg(long = "no-limit")]
    no_limit: bool,

    /// Draw a console chart of cost across the breakdown (best with --group-by day/hour;
    /// mutually exclusive with --json/--csv)
    #[arg(long)]
    plot: bool,

    /// In time plots, collapse empty stretches of 2+ buckets ($0) instead of drawing them
    #[arg(long = "skip-gaps")]
    skip_gaps: bool,

    /// Output the breakdown rows as CSV (mutually exclusive with --json/--plot)
    #[arg(long)]
    csv: bool,

    /// Save the chart to a file instead of the console (.svg or .png; defaults to .svg)
    #[arg(long = "save-fig", value_name = "PATH")]
    save_fig: Option<PathBuf>,

    /// Whether --plot may draw an inline image in terminals that support it like Kitty, Ghostty, iTerm2
    #[arg(
        long = "image",
        env = "BRAINTRUST_COST_IMAGE",
        value_enum,
        default_value = "auto",
        value_name = "MODE"
    )]
    image: ImageMode,

    /// TOML file of per-model token prices for models Braintrust cannot price
    #[arg(long, env = "BRAINTRUST_COST_PRICING_FILE", value_name = "PATH")]
    pricing_file: Option<PathBuf>,

    /// Time zone for day/hour buckets, bare YYYY-MM-DD bounds, and display:
    /// local, utc, or a fixed offset like -07:00
    #[arg(
        long = "timezone",
        env = "BRAINTRUST_COST_TIMEZONE",
        default_value = "local",
        allow_hyphen_values = true,
        value_name = "ZONE"
    )]
    timezone: String,
}

pub async fn run(base: BaseArgs, args: CostArgs) -> Result<()> {
    // --plot / --csv / --json are three ways to render the same result and are
    // mutually exclusive. (`--json` is a global flag, so it can't be a clap
    // `conflicts_with` target for the cost-local flags without breaking arg
    // validation — hence the manual check.)
    let mut modes = Vec::new();
    if base.json {
        modes.push("--json");
    }
    if args.csv {
        modes.push("--csv");
    }
    if args.plot {
        modes.push("--plot");
    }
    if modes.len() > 1 {
        bail!(
            "{} cannot be used together; choose one",
            modes.join(" and ")
        );
    }

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

    let display_dims = dedup_dimensions(&args.group_by)?;
    let internal_dims = internal_dimensions(&display_dims, price_book.is_some());
    let sources = resolve_sources(&args.sources);
    let row_limit = resolve_row_limit(&args);

    let filters = Filters {
        sources: sources.clone(),
        models: args.models.clone(),
        types: args.types.clone(),
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
    rows::sort_rows(&mut cost_rows, &display_dims);
    let totals = rows::totals_from_rows(&cost_rows, candidate_spans);

    let source_labels: Vec<String> = sources
        .iter()
        .map(|source| source.label().to_string())
        .collect();

    // A saved figure is a file action, independent of the console chart/JSON.
    if let Some(path) = args.save_fig.as_deref() {
        let (points, title) =
            chart_points(&cost_rows, &display_dims, range, offset, args.skip_gaps);
        let written = plot::save_cost_chart(&title, &points, path)?;
        print_command_status(
            CommandStatus::Success,
            &format!("Saved cost chart to {}", written.display()),
        );
    }

    // For --plot, prefer an inline image when the terminal supports it and
    // --image=auto; otherwise draw the ASCII chart.
    let mut chart: Option<String> = None;
    let mut image: Option<String> = None;
    if args.plot {
        let (points, title) =
            chart_points(&cost_rows, &display_dims, range, offset, args.skip_gaps);
        if matches!(args.image, ImageMode::Auto) {
            image = inline_chart_image(&title, &points)?;
        }
        if image.is_none() {
            let ascii = plot::render_cost_chart(&title, &points, chart_size())?;
            chart = (!ascii.trim().is_empty()).then_some(ascii);
        }
    }

    let timezone_label = offset_label(offset);
    let report = render::Report {
        ctx: &ctx,
        since: format_timestamp_in_offset(range.since, offset),
        until: format_timestamp_in_offset(range.until, offset),
        offset,
        timezone: &timezone_label,
        display_dims: &display_dims,
        sources: &source_labels,
        rows: &cost_rows,
        totals: &totals,
        pricing_file: args.pricing_file.as_deref(),
        truncated,
        verbose: base.verbose,
        chart: chart.as_deref(),
        image: image.as_deref(),
    };

    // At most one of --json/--csv/--plot is set (rejected together up front);
    // --plot flows through the table path via `chart`/`image`.
    if base.json {
        render::print_json(&report)
    } else if args.csv {
        render::print_csv(&report)
    } else {
        render::print_table(&report)
    }
}

/// Render the chart as an inline terminal image, when supported. Returns `None`
/// on unsupported terminals or when there is nothing to plot.
fn inline_chart_image(title: &str, points: &[(String, f64)]) -> Result<Option<String>> {
    if !termimage::is_supported() {
        return Ok(None);
    }
    let cols = chart_size().0 as u16;
    let size = image_pixel_size(cols);
    let Some(png) = plot::render_png_bytes(title, points, size)? else {
        return Ok(None);
    };
    Ok(termimage::inline_image(&png, cols))
}

/// Pixel size to render an inline image at. Matches the terminal's real pixel
/// grid when it reports one (so the image is 1:1 and crisp), otherwise renders
/// high-res for a crisp downscale.
fn image_pixel_size(cols: u16) -> (u32, u32) {
    if let Ok(window) = crossterm::terminal::window_size() {
        if window.width > 0 && window.height > 0 && window.columns > 0 {
            let cell_width = f64::from(window.width) / f64::from(window.columns);
            let width = (cell_width * f64::from(cols)).round().clamp(320.0, 4000.0) as u32;
            let height = ((f64::from(width) * 0.5).round() as u32)
                .min(u32::from(window.height))
                .max(200);
            return (width, height);
        }
    }
    IMAGE_FALLBACK_SIZE
}

/// Build the `(label, cost)` points and chart title. Time breakdowns
/// (`day`/`hour`) are ordered chronologically (left-to-right); everything else
/// keeps the cost-ranked order of the table.
fn chart_points(
    rows: &[rows::CostRow],
    display_dims: &[Dimension],
    range: TimeRange,
    offset: FixedOffset,
    skip_gaps: bool,
) -> (Vec<(String, f64)>, String) {
    let points = match time_granularity(display_dims) {
        // Time breakdowns get a true, evenly-spaced time axis: every bucket in
        // the window is present (missing ones are $0), so real gaps show as the
        // line dropping to zero rather than being compressed away. Labels are
        // compact and in the display time zone.
        Some(granularity) => {
            let mut points = densify_time_points(rows, granularity, range, offset);
            // Optionally collapse long empty stretches so the plot isn't
            // dominated by flat zero when data is bursty.
            if skip_gaps {
                points = drop_long_zero_runs(points);
            }
            let raw: Vec<String> = points.iter().map(|(label, _)| label.clone()).collect();
            let span = range.until - range.since;
            for (point, label) in
                points
                    .iter_mut()
                    .zip(axis_labels(&raw, granularity, offset, span))
            {
                point.0 = label;
            }
            points
        }
        // Categorical breakdowns keep the table's cost-ranked order.
        None => rows
            .iter()
            .map(|row| (chart_label(&row.keys), row.cost().unwrap_or(0.0)))
            .collect(),
    };

    let title = format!(
        "Cost by {}",
        display_dims
            .iter()
            .map(|dim| dim.header())
            .collect::<Vec<_>>()
            .join(" / ")
    );
    (points, title)
}

fn chart_label(keys: &[Option<String>]) -> String {
    keys.iter()
        .map(|value| match value {
            Some(value) if !value.is_empty() => value.as_str(),
            _ => "—",
        })
        .collect::<Vec<_>>()
        .join(" / ")
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TimeGranularity {
    Day,
    Hour,
}

/// Expand time-bucketed rows into a complete, evenly-spaced series over the
/// window: every hour/day bucket is present, with $0 for buckets that had no
/// data, so gaps are represented instead of compressed. Points are chronological
/// and keyed by their UTC RFC 3339 bucket start.
fn densify_time_points(
    rows: &[rows::CostRow],
    granularity: TimeGranularity,
    range: TimeRange,
    offset: FixedOffset,
) -> Vec<(String, f64)> {
    let step = match granularity {
        TimeGranularity::Hour => Duration::hours(1),
        TimeGranularity::Day => Duration::days(1),
    };

    // Sum cost into bucket-start instants (keyed by epoch seconds).
    let mut by_bucket: HashMap<i64, f64> = HashMap::new();
    for row in rows {
        let Some(Some(key)) = row.keys.first() else {
            continue;
        };
        let Ok(instant) = DateTime::parse_from_rfc3339(key) else {
            continue;
        };
        let start = floor_to_bucket(instant.with_timezone(&Utc), granularity, offset);
        *by_bucket.entry(start.timestamp()).or_insert(0.0) += row.cost().unwrap_or(0.0);
    }

    let mut points = Vec::new();
    let mut cursor = floor_to_bucket(range.since, granularity, offset);
    // Cap the number of buckets so a huge window can't blow up (e.g. hourly
    // over years); such ranges aren't meaningfully plottable anyway.
    let mut remaining = 100_000;
    while cursor < range.until && remaining > 0 {
        let cost = by_bucket.get(&cursor.timestamp()).copied().unwrap_or(0.0);
        points.push((format_timestamp(cursor), cost));
        cursor += step;
        remaining -= 1;
    }
    points
}

/// Drop stretches of `$0` that span two or more consecutive buckets, so bursty
/// data isn't buried under long flat-zero runs. Isolated single-bucket zeros
/// are kept (a brief dip). The remaining points keep their real time labels, so
/// a collapsed stretch shows up as a jump in the axis labels.
fn drop_long_zero_runs(points: Vec<(String, f64)>) -> Vec<(String, f64)> {
    let count = points.len();
    let is_zero: Vec<bool> = points.iter().map(|(_, cost)| *cost <= 0.0).collect();
    let mut keep = vec![true; count];
    let mut index = 0;
    while index < count {
        if is_zero[index] {
            let start = index;
            while index < count && is_zero[index] {
                index += 1;
            }
            if index - start >= 2 {
                keep[start..index].fill(false);
            }
        } else {
            index += 1;
        }
    }
    points
        .into_iter()
        .zip(keep)
        .filter_map(|(point, keep)| keep.then_some(point))
        .collect()
}

/// Truncate an instant to the start of its hour/day bucket in `offset`.
fn floor_to_bucket(
    instant: DateTime<Utc>,
    granularity: TimeGranularity,
    offset: FixedOffset,
) -> DateTime<Utc> {
    let local = instant.with_timezone(&offset);
    let floored = match granularity {
        TimeGranularity::Hour => local
            .with_minute(0)
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0)),
        TimeGranularity::Day => local
            .with_hour(0)
            .and_then(|value| value.with_minute(0))
            .and_then(|value| value.with_second(0))
            .and_then(|value| value.with_nanosecond(0)),
    };
    floored.unwrap_or(local).with_timezone(&Utc)
}

fn time_granularity(display_dims: &[Dimension]) -> Option<TimeGranularity> {
    match display_dims {
        [Dimension::Day] => Some(TimeGranularity::Day),
        [Dimension::Hour] => Some(TimeGranularity::Hour),
        _ => None,
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct TimeParts {
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
}

const MONTH_NAMES: [&str; 12] = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
];

/// Parse a bucket key (e.g. `2026-07-21T05:00:00Z` or `2026-07-21`) and express
/// it in `offset`, so labels read in the chosen time zone. BTQL returns bucket
/// keys as UTC instants; converting is what makes `--timezone` visibly shift
/// hour labels and keep day labels correct across the date line.
fn parse_time_parts(label: &str, offset: FixedOffset) -> Option<TimeParts> {
    if let Ok(instant) = DateTime::parse_from_rfc3339(label) {
        let local = instant.with_timezone(&offset);
        return Some(TimeParts {
            year: local.year(),
            month: local.month(),
            day: local.day(),
            hour: local.hour(),
        });
    }
    // Date-only keys carry no time or zone; take them at face value.
    if let Ok(date) = NaiveDate::parse_from_str(label, "%Y-%m-%d") {
        return Some(TimeParts {
            year: date.year(),
            month: date.month(),
            day: date.day(),
            hour: 0,
        });
    }
    None
}

/// Build self-contained axis labels in `offset`. Each tick carries its own
/// context because plotters shows only a sparse subset of ticks — a "show the
/// coarser unit only when it changes" scheme would leave most ticks without a
/// day. The detail is chosen from the span: beyond a few days, hourly ticks are
/// meaningless without their day, so every label gets the day (and month).
fn axis_labels(
    labels: &[String],
    granularity: TimeGranularity,
    offset: FixedOffset,
    span: Duration,
) -> Vec<String> {
    let long = span.num_days() > 3;
    labels
        .iter()
        .map(|label| match parse_time_parts(label, offset) {
            Some(parts) => format_axis_label(parts, granularity, long),
            None => label.clone(),
        })
        .collect()
}

fn format_axis_label(parts: TimeParts, granularity: TimeGranularity, long: bool) -> String {
    let month = MONTH_NAMES
        .get((parts.month as usize).saturating_sub(1))
        .copied()
        .unwrap_or("?");
    // Buckets start on the hour, so minutes are always :00 — use a compact
    // `HHh` form to leave room for more ticks.
    match granularity {
        // Over a few days, hourly ticks show the day (and month) plus the hour.
        TimeGranularity::Hour if long => format!("{month} {} {:02}h", parts.day, parts.hour),
        // Short spans: day number + hour is enough context.
        TimeGranularity::Hour => format!("{} {:02}h", parts.day, parts.hour),
        // Daily buckets always show month + day.
        TimeGranularity::Day => format!("{month} {}", parts.day),
    }
}

/// Chart dimensions in characters, based on the terminal when available.
fn chart_size() -> (u32, u32) {
    let (cols, rows) = dialoguer::console::Term::stdout()
        .size_checked()
        .map(|(rows, cols)| (u32::from(cols), u32::from(rows)))
        .unwrap_or((100, 24));
    (cols.clamp(60, 160), (rows.saturating_sub(4)).clamp(14, 30))
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

fn dedup_dimensions(requested: &[Dimension]) -> Result<Vec<Dimension>> {
    let mut seen = Vec::new();
    for dim in requested {
        if seen.contains(dim) {
            bail!(
                "--group-by '{}' is repeated; each dimension may be grouped at most once",
                dim.alias()
            );
        }
        seen.push(*dim);
    }
    if seen.is_empty() {
        seen.push(Dimension::Model);
    }
    Ok(seen)
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

/// Human label for a fixed offset: `UTC` or `±HH:MM`.
fn offset_label(offset: FixedOffset) -> String {
    let seconds = offset.local_minus_utc();
    if seconds == 0 {
        return "UTC".to_string();
    }
    let sign = if seconds < 0 { '-' } else { '+' };
    let seconds = seconds.abs();
    format!("{sign}{:02}:{:02}", seconds / 3600, (seconds % 3600) / 60)
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
    use clap::Parser;

    #[derive(Debug, Parser)]
    struct Harness {
        #[command(flatten)]
        args: CostArgs,
    }

    #[test]
    fn negative_timezone_offset_parses_as_a_value() {
        // `-7` looks like a flag; allow_hyphen_values lets it be the value.
        let parsed = Harness::try_parse_from(["bt-cost", "--timezone", "-7"]).unwrap();
        assert_eq!(parsed.args.timezone, "-7");
        assert_eq!(
            parse_timezone(&parsed.args.timezone)
                .unwrap()
                .local_minus_utc(),
            -7 * 3600
        );

        let colon = Harness::try_parse_from(["bt-cost", "--timezone", "-07:00"]).unwrap();
        assert_eq!(colon.args.timezone, "-07:00");
    }

    fn base_args() -> CostArgs {
        CostArgs {
            window: "7d".to_string(),
            since: None,
            until: None,
            sources: Vec::new(),
            group_by: vec![Dimension::Model],
            models: Vec::new(),
            types: Vec::new(),
            limit: None,
            no_limit: false,
            plot: false,
            skip_gaps: false,
            csv: false,
            save_fig: None,
            image: ImageMode::Auto,
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
    fn axis_labels_day_are_self_contained() {
        let labels = [
            "2026-07-30T00:00:00Z".to_string(),
            "2026-07-31T00:00:00Z".to_string(),
            "2026-08-01T00:00:00Z".to_string(),
        ];
        // Every daily tick carries month + day, regardless of span.
        let out = axis_labels(&labels, TimeGranularity::Day, utc(), Duration::days(3));
        assert_eq!(out, vec!["Jul 30", "Jul 31", "Aug 1"]);
    }

    #[test]
    fn axis_labels_hour_add_the_day_over_long_spans() {
        let labels = [
            "2026-07-21T15:00:00Z".to_string(),
            "2026-07-24T09:00:00Z".to_string(),
        ];
        // Span > 3 days: each hourly tick shows month, day and hour.
        let long = axis_labels(&labels, TimeGranularity::Hour, utc(), Duration::days(30));
        assert_eq!(long, vec!["Jul 21 15h", "Jul 24 09h"]);
        // Span <= 3 days: day number + hour.
        let short = axis_labels(&labels, TimeGranularity::Hour, utc(), Duration::days(1));
        assert_eq!(short, vec!["21 15h", "24 09h"]);
    }

    fn time_row(key: &str, cost: f64) -> rows::CostRow {
        rows::CostRow {
            keys: vec![Some(key.to_string())],
            logged_cost: cost,
            file_cost: 0.0,
            logged_priced_spans: 1,
            file_priced_spans: 0,
            unpriced_spans: 0,
            no_usage_spans: 0,
        }
    }

    #[test]
    fn skip_gaps_drops_runs_of_two_or_more_zeros() {
        let points = vec![
            ("a".to_string(), 5.0),
            ("b".to_string(), 0.0), // isolated zero: kept
            ("c".to_string(), 3.0),
            ("d".to_string(), 0.0), // run of 3 zeros: dropped
            ("e".to_string(), 0.0),
            ("f".to_string(), 0.0),
            ("g".to_string(), 4.0),
        ];
        let kept: Vec<String> = drop_long_zero_runs(points)
            .into_iter()
            .map(|(label, _)| label)
            .collect();
        assert_eq!(kept, vec!["a", "b", "c", "g"]);
    }

    #[test]
    fn densify_fills_missing_buckets_with_zero() {
        let rows = [
            time_row("2026-07-20T00:00:00Z", 5.0),
            time_row("2026-07-22T00:00:00Z", 3.0),
        ];
        let range = TimeRange {
            since: parse_timestamp("2026-07-20T00:00:00Z").unwrap(),
            until: parse_timestamp("2026-07-23T00:00:00Z").unwrap(),
        };
        let points = densify_time_points(&rows, TimeGranularity::Day, range, utc());
        // Three consecutive days, with the missing middle day represented as $0.
        let costs: Vec<f64> = points.iter().map(|(_, cost)| *cost).collect();
        assert_eq!(costs, vec![5.0, 0.0, 3.0]);
    }

    #[test]
    fn hour_labels_shift_into_the_display_timezone() {
        let labels = [
            "2026-07-21T05:00:00Z".to_string(),
            "2026-07-21T06:00:00Z".to_string(),
        ];
        let span = Duration::days(1);
        // UTC: hours 05, 06 on the 21st.
        assert_eq!(
            axis_labels(&labels, TimeGranularity::Hour, utc(), span),
            vec!["21 05h", "21 06h"]
        );
        // UTC-7: the same instants are 22:00 and 23:00 the previous day.
        let minus7 = parse_timezone("-7").unwrap();
        assert_eq!(
            axis_labels(&labels, TimeGranularity::Hour, minus7, span),
            vec!["20 22h", "20 23h"]
        );
    }

    #[test]
    fn group_by_defaults_to_model() {
        assert_eq!(dedup_dimensions(&[]).unwrap(), vec![Dimension::Model]);
        assert_eq!(
            dedup_dimensions(&[Dimension::Source, Dimension::Model]).unwrap(),
            vec![Dimension::Source, Dimension::Model]
        );
    }

    #[test]
    fn group_by_rejects_repeated_dimensions() {
        assert!(dedup_dimensions(&[Dimension::Source, Dimension::Source]).is_err());
        assert!(dedup_dimensions(&[Dimension::Model, Dimension::User, Dimension::Model]).is_err());
    }
}
