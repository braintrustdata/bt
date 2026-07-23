//! BTQL query construction for `bt cost`.
//!
//! Logs, topics, and experiments share the `project(<project_id>)` amalgam
//! (`project_logs` + `experiment`), so the common case is a single query with
//! no experiment enumeration. Playgrounds are a separate `playground_logs(...)`
//! table keyed by prompt-session id.

use chrono::{DateTime, Utc};
use clap::ValueEnum;

use super::pricing::format_timestamp;

/// A cost breakdown dimension. Each maps to a BTQL expression; new dimensions
/// are new enum values rather than new flags.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub(super) enum Dimension {
    Model,
    Type,
    Source,
    Experiment,
    Trace,
    User,
    Day,
    Hour,
}

impl Dimension {
    /// Column alias used in the SELECT and read back from result rows.
    pub(super) fn alias(self) -> &'static str {
        match self {
            Dimension::Model => "model",
            Dimension::Type => "type",
            Dimension::Source => "source",
            Dimension::Experiment => "experiment",
            Dimension::Trace => "trace",
            Dimension::User => "user",
            Dimension::Day => "day",
            Dimension::Hour => "hour",
        }
    }

    /// Human-facing column header.
    pub(super) fn header(self) -> &'static str {
        match self {
            Dimension::Model => "Model",
            Dimension::Type => "Type",
            Dimension::Source => "Source",
            Dimension::Experiment => "Experiment",
            Dimension::Trace => "Trace",
            Dimension::User => "User",
            Dimension::Day => "Day",
            Dimension::Hour => "Hour",
        }
    }

    /// BTQL expression for this dimension in the given table context.
    fn expr(self, table: TableKind) -> String {
        match self {
            Dimension::Model => "metadata.model".to_string(),
            Dimension::Type => "span_attributes.type".to_string(),
            Dimension::Source => source_case_expr(table),
            Dimension::Experiment => match table {
                TableKind::Project => "experiment_id".to_string(),
                TableKind::Playground => "NULL".to_string(),
            },
            Dimension::Trace => "root_span_id".to_string(),
            Dimension::User => "span_attributes.created_by_user_id".to_string(),
            Dimension::Day => "day(created)".to_string(),
            Dimension::Hour => "hour(created)".to_string(),
        }
    }
}

/// Which BTQL table the query runs against.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum TableKind {
    Project,
    Playground,
}

/// A cost pool the user can include or exclude.
#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
#[value(rename_all = "kebab-case")]
pub(super) enum Source {
    Logs,
    Topics,
    Experiments,
    Playgrounds,
}

impl Source {
    pub(super) fn label(self) -> &'static str {
        match self {
            Source::Logs => "logs",
            Source::Topics => "topics",
            Source::Experiments => "experiments",
            Source::Playgrounds => "playgrounds",
        }
    }
}

/// Span filters applied before grouping.
#[derive(Debug, Clone, Default)]
pub(super) struct Filters {
    pub sources: Vec<Source>,
    pub models: Vec<String>,
    pub types: Vec<String>,
}

/// A half-open `[since, until)` window for a price segment. With no pricing
/// file there is a single segment covering the whole range.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TimeSegment {
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct TimeRange {
    pub since: DateTime<Utc>,
    pub until: DateTime<Utc>,
}

/// `CASE` expression that classifies a `project()` row into a source label.
fn source_case_expr(table: TableKind) -> String {
    match table {
        TableKind::Playground => "'playgrounds'".to_string(),
        TableKind::Project => "CASE \
                WHEN experiment_id IS NOT NULL THEN 'experiments' \
                WHEN span_attributes.purpose = 'scorer' THEN 'topics' \
                ELSE 'logs' END"
            .to_string(),
    }
}

/// Predicate selecting the `project()` rows for a single source.
fn project_source_predicate(source: Source) -> Option<&'static str> {
    match source {
        Source::Experiments => Some("experiment_id IS NOT NULL"),
        Source::Topics => {
            Some("experiment_id IS NULL AND span_attributes.purpose = 'scorer'")
        }
        Source::Logs => Some(
            "experiment_id IS NULL AND (span_attributes.purpose IS NULL OR span_attributes.purpose != 'scorer')",
        ),
        // Playgrounds are a different table, never a `project()` predicate.
        Source::Playgrounds => None,
    }
}

pub(super) fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

/// Which project sources (logs/topics/experiments) are in scope. Returns `None`
/// when all three are included (so no source filter is needed).
fn project_source_filter(filters: &Filters) -> Option<String> {
    let wanted: Vec<Source> = filters
        .sources
        .iter()
        .copied()
        .filter(|source| project_source_predicate(*source).is_some())
        .collect();
    let all = [Source::Logs, Source::Topics, Source::Experiments];
    if all.iter().all(|source| wanted.contains(source)) {
        return None;
    }
    let clauses: Vec<String> = wanted
        .iter()
        .filter_map(|source| project_source_predicate(*source))
        .map(|predicate| format!("({predicate})"))
        .collect();
    (!clauses.is_empty()).then(|| clauses.join(" OR "))
}

/// Token-usage expressions shared across both tables.
struct TokenExprs {
    activity: String,
    uncached_input: String,
    cached: String,
    effective_write: String,
    split_5m: String,
    split_1h: String,
    fallback_write: String,
    completion: String,
}

fn token_exprs() -> TokenExprs {
    let prompt = "COALESCE(metrics.prompt_tokens, 0)";
    let completion = "COALESCE(metrics.completion_tokens, 0)";
    let cached = "COALESCE(metrics.prompt_cached_tokens, 0)";
    let generic_write = "COALESCE(metrics.prompt_cache_creation_tokens, 0)";
    let write_5m = "COALESCE(metrics.prompt_cache_creation_5m_tokens, 0)";
    let write_1h = "COALESCE(metrics.prompt_cache_creation_1h_tokens, 0)";
    let split_write = format!("({write_5m} + {write_1h})");
    let effective_write = format!("GREATEST({generic_write}, {split_write})");
    let uncached_input = format!("GREATEST(0, {prompt} - {cached} - {effective_write})");
    let split_complete = format!("{split_write} >= {generic_write}");
    let fallback_write = format!("CASE WHEN {split_complete} THEN 0 ELSE {effective_write} END");
    let split_5m = format!("CASE WHEN {split_complete} THEN {write_5m} ELSE 0 END");
    let split_1h = format!("CASE WHEN {split_complete} THEN {write_1h} ELSE 0 END");
    let activity = [
        "metrics.prompt_tokens IS NOT NULL",
        "metrics.completion_tokens IS NOT NULL",
        "metrics.prompt_cached_tokens IS NOT NULL",
        "metrics.prompt_cache_creation_tokens IS NOT NULL",
        "metrics.prompt_cache_creation_5m_tokens IS NOT NULL",
        "metrics.prompt_cache_creation_1h_tokens IS NOT NULL",
    ]
    .join(" OR ");
    TokenExprs {
        activity,
        uncached_input,
        cached: cached.to_string(),
        effective_write,
        split_5m,
        split_1h,
        fallback_write,
        completion: completion.to_string(),
    }
}

/// Internal grouping dimensions: the requested display dimensions plus `model`
/// when a pricing file is set (needed for per-model rate lookup) and not
/// already requested.
pub(super) fn internal_dimensions(display: &[Dimension], has_pricing_file: bool) -> Vec<Dimension> {
    let mut dims = display.to_vec();
    if has_pricing_file && !dims.contains(&Dimension::Model) {
        dims.push(Dimension::Model);
    }
    dims
}

/// Build the aggregate cost query for a table (project or playground).
pub(super) fn build_query(
    table: TableKind,
    from_clause: &str,
    range: TimeRange,
    segments: &[TimeSegment],
    dims: &[Dimension],
    filters: &Filters,
) -> String {
    let tokens = token_exprs();
    let activity = &tokens.activity;

    // `dims` is the internal grouping (display dimensions, plus `model` when a
    // pricing file forced it for per-model rate lookup). Select and group by
    // exactly these — nothing else, to keep cardinality minimal.
    let mut select_fields: Vec<String> = Vec::new();
    for dim in dims {
        select_fields.push(format!("{} AS {}", dim.expr(table), dim.alias()));
    }

    select_fields.push(format!(
        "COUNT(CASE WHEN estimated_cost() IS NOT NULL OR metadata.model IS NOT NULL OR ({activity}) THEN 1 END) AS candidate_spans"
    ));
    select_fields.push("COUNT(estimated_cost()) AS logged_priced_spans".to_string());
    select_fields.push("SUM(estimated_cost()) AS logged_cost".to_string());
    select_fields.push(format!(
        "COUNT(CASE WHEN estimated_cost() IS NULL AND ({activity}) THEN 1 END) AS unpriced_token_spans"
    ));
    select_fields.push(format!(
        "COUNT(CASE WHEN estimated_cost() IS NULL AND metadata.model IS NOT NULL AND NOT ({activity}) THEN 1 END) AS no_usage_spans"
    ));

    // Per-segment conditional token sums, used only when a pricing file fills
    // spans that `estimated_cost()` left unpriced. `segments` is empty unless a
    // pricing file is set. Populations are disjoint: logged spans
    // (`estimated_cost() IS NOT NULL`) are excluded here.
    for (index, segment) in segments.iter().enumerate() {
        let condition = format!(
            "estimated_cost() IS NULL AND created >= {} AND created < {} AND ({activity})",
            sql_quote(&format_timestamp(segment.since)),
            sql_quote(&format_timestamp(segment.until)),
        );
        select_fields.extend([
            format!("COUNT(CASE WHEN {condition} THEN 1 END) AS p{index}_spans"),
            format!(
                "SUM(CASE WHEN {condition} THEN {} ELSE 0 END) AS p{index}_uncached_input_tokens",
                tokens.uncached_input
            ),
            format!(
                "SUM(CASE WHEN {condition} THEN {} ELSE 0 END) AS p{index}_cached_input_tokens",
                tokens.cached
            ),
            format!(
                "SUM(CASE WHEN {condition} THEN {} ELSE 0 END) AS p{index}_effective_cache_write_tokens",
                tokens.effective_write
            ),
            format!(
                "SUM(CASE WHEN {condition} THEN {} ELSE 0 END) AS p{index}_split_cache_write_5m_tokens",
                tokens.split_5m
            ),
            format!(
                "SUM(CASE WHEN {condition} THEN {} ELSE 0 END) AS p{index}_split_cache_write_1h_tokens",
                tokens.split_1h
            ),
            format!(
                "SUM(CASE WHEN {condition} THEN {} ELSE 0 END) AS p{index}_fallback_cache_write_tokens",
                tokens.fallback_write
            ),
            format!(
                "SUM(CASE WHEN {condition} THEN {} ELSE 0 END) AS p{index}_output_tokens",
                tokens.completion
            ),
        ]);
    }

    // WHERE clauses. Timestamp bounds satisfy the BTQL safety rule.
    let mut where_clauses = vec![
        format!("created >= {}", sql_quote(&format_timestamp(range.since))),
        format!("created < {}", sql_quote(&format_timestamp(range.until))),
    ];
    if table == TableKind::Project {
        if let Some(source_filter) = project_source_filter(filters) {
            where_clauses.push(format!("({source_filter})"));
        }
    }
    if !filters.models.is_empty() {
        where_clauses.push(format!(
            "metadata.model IN ({})",
            filters
                .models
                .iter()
                .map(|value| sql_quote(value))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }
    if !filters.types.is_empty() {
        where_clauses.push(format!(
            "span_attributes.type IN ({})",
            filters
                .types
                .iter()
                .map(|value| sql_quote(value))
                .collect::<Vec<_>>()
                .join(", ")
        ));
    }

    // GROUP BY exactly the selected dimension expressions.
    let group_exprs: Vec<String> = dims.iter().map(|dim| dim.expr(table)).collect();

    format!(
        "SELECT\n  {}\nFROM {}\nWHERE {}\nGROUP BY {}",
        select_fields.join(",\n  "),
        from_clause,
        where_clauses.join("\n  AND "),
        group_exprs.join(", "),
    )
}

/// `FROM` clause for the project amalgam.
pub(super) fn project_from(project_id: &str) -> String {
    format!("project({}, shape => 'spans')", sql_quote(project_id))
}

/// `FROM` clause for playground logs across the given prompt sessions.
pub(super) fn playground_from(session_ids: &[String]) -> String {
    let ids = session_ids
        .iter()
        .map(|id| sql_quote(id))
        .collect::<Vec<_>>()
        .join(", ");
    format!("playground_logs({ids}, shape => 'spans')")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost::pricing::parse_timestamp;

    fn range() -> TimeRange {
        TimeRange {
            since: parse_timestamp("2025-01-01").unwrap(),
            until: parse_timestamp("2025-03-01").unwrap(),
        }
    }

    #[test]
    fn project_query_is_timestamp_bounded_and_groups_by_dims() {
        let query = build_query(
            TableKind::Project,
            &project_from("proj-1"),
            range(),
            &[],
            &[Dimension::Model],
            &Filters::default(),
        );
        assert!(
            query.contains("FROM project('proj-1', shape => 'spans')"),
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
            query.contains("SUM(estimated_cost()) AS logged_cost"),
            "{query}"
        );
        assert!(query.contains("metadata.model AS model"), "{query}");
        // No pricing file -> no per-segment token columns.
        assert!(!query.contains("p0_uncached_input_tokens"), "{query}");
    }

    #[test]
    fn source_filter_limits_project_rows() {
        let filters = Filters {
            sources: vec![Source::Logs],
            ..Filters::default()
        };
        let query = build_query(
            TableKind::Project,
            &project_from("proj-1"),
            range(),
            &[],
            &[Dimension::Model],
            &filters,
        );
        assert!(query.contains("experiment_id IS NULL"), "{query}");
        assert!(query.contains("!= 'scorer'"), "{query}");
    }

    #[test]
    fn all_project_sources_need_no_source_filter() {
        let filters = Filters {
            sources: vec![Source::Logs, Source::Topics, Source::Experiments],
            ..Filters::default()
        };
        assert!(project_source_filter(&filters).is_none());
    }

    #[test]
    fn source_dimension_uses_case_expression() {
        let query = build_query(
            TableKind::Project,
            &project_from("proj-1"),
            range(),
            &[],
            &[Dimension::Source, Dimension::Model],
            &Filters::default(),
        );
        assert!(query.contains("AS source"), "{query}");
        assert!(query.contains("THEN 'experiments'"), "{query}");
        assert!(query.contains("THEN 'topics'"), "{query}");
    }

    #[test]
    fn pricing_file_emits_segment_columns() {
        let segments = vec![
            TimeSegment {
                since: parse_timestamp("2025-01-01").unwrap(),
                until: parse_timestamp("2025-02-01").unwrap(),
            },
            TimeSegment {
                since: parse_timestamp("2025-02-01").unwrap(),
                until: parse_timestamp("2025-03-01").unwrap(),
            },
        ];
        // Callers pass the internal dimensions (display + model for rate lookup).
        let query = build_query(
            TableKind::Project,
            &project_from("proj-1"),
            range(),
            &segments,
            &internal_dimensions(&[Dimension::Trace], true),
            &Filters::default(),
        );
        assert!(query.contains("p0_uncached_input_tokens"), "{query}");
        assert!(query.contains("p1_output_tokens"), "{query}");
        // Model is grouped for rate lookup even though it is not a display dim.
        assert!(query.contains("metadata.model AS model"), "{query}");
        assert!(query.contains("root_span_id AS trace"), "{query}");
    }

    #[test]
    fn playground_query_uses_playground_table_and_literal_source() {
        let query = build_query(
            TableKind::Playground,
            &playground_from(&["sess-1".to_string(), "sess-2".to_string()]),
            range(),
            &[],
            &[Dimension::Source, Dimension::Model],
            &Filters::default(),
        );
        assert!(
            query.contains("playground_logs('sess-1', 'sess-2'"),
            "{query}"
        );
        assert!(query.contains("'playgrounds' AS source"), "{query}");
    }

    #[test]
    fn internal_dimensions_add_model_only_with_pricing_file() {
        assert_eq!(
            internal_dimensions(&[Dimension::Trace], false),
            vec![Dimension::Trace]
        );
        assert_eq!(
            internal_dimensions(&[Dimension::Trace], true),
            vec![Dimension::Trace, Dimension::Model]
        );
        assert_eq!(
            internal_dimensions(&[Dimension::Model], true),
            vec![Dimension::Model]
        );
    }
}
