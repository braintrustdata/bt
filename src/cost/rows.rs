//! Turn raw BTQL result rows into display cost rows.
//!
//! `estimated_cost()` supplies logged/registry cost (the preferred, accurate
//! source). A pricing file fills spans it leaves unpriced, using per-segment
//! token sums. The two populations are disjoint, so costs and counts add
//! cleanly. Rows are then aggregated from the internal grouping (display
//! dimensions plus `model`) up to the display dimensions.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use chrono::{DateTime, NaiveDate};
use serde_json::{Map, Value};

use super::pricing::{PriceBook, TokenUsage};
use super::query::{Dimension, TimeSegment};

/// One rendered breakdown row, keyed by the display dimension values.
#[derive(Debug, Clone)]
pub(super) struct CostRow {
    /// Display dimension values, aligned with the requested `--group-by` order.
    pub keys: Vec<Option<String>>,
    pub logged_cost: f64,
    pub file_cost: f64,
    pub logged_priced_spans: u64,
    pub file_priced_spans: u64,
    pub unpriced_spans: u64,
    pub no_usage_spans: u64,
}

impl CostRow {
    pub(super) fn priced_spans(&self) -> u64 {
        self.logged_priced_spans + self.file_priced_spans
    }

    pub(super) fn cost(&self) -> Option<f64> {
        (self.priced_spans() > 0).then_some(self.logged_cost + self.file_cost)
    }

    fn merge(&mut self, other: &CostRow) {
        self.logged_cost += other.logged_cost;
        self.file_cost += other.file_cost;
        self.logged_priced_spans += other.logged_priced_spans;
        self.file_priced_spans += other.file_priced_spans;
        self.unpriced_spans += other.unpriced_spans;
        self.no_usage_spans += other.no_usage_spans;
    }
}

/// Aggregate totals across all rows.
#[derive(Debug, Clone, Default)]
pub(super) struct Totals {
    pub logged_cost: f64,
    pub file_cost: f64,
    pub logged_priced_spans: u64,
    pub file_priced_spans: u64,
    pub unpriced_spans: u64,
    pub no_usage_spans: u64,
    pub candidate_spans: u64,
}

impl Totals {
    pub(super) fn priced_spans(&self) -> u64 {
        self.logged_priced_spans + self.file_priced_spans
    }

    pub(super) fn cost(&self) -> Option<f64> {
        (self.priced_spans() > 0).then_some(self.logged_cost + self.file_cost)
    }
}

/// Accumulate result rows from one query into `accumulator`, keyed by the
/// display dimension values.
///
/// `internal_dims` is the order the dimensions were selected in (display
/// dimensions, then `model` when a pricing file forced it). `display_dims` is
/// the subset to key the output on.
pub(super) fn accumulate_rows(
    accumulator: &mut BTreeMap<Vec<Option<String>>, CostRow>,
    result_rows: &[Map<String, Value>],
    internal_dims: &[Dimension],
    display_dims: &[Dimension],
    segments: &[TimeSegment],
    price_book: Option<&PriceBook>,
) -> u64 {
    let mut candidate_total = 0_u64;
    for row in result_rows {
        let candidate_spans = value_as_u64(row.get("candidate_spans"));
        if candidate_spans == 0 {
            continue;
        }
        candidate_total = candidate_total.saturating_add(candidate_spans);

        let model = row.get("model").and_then(Value::as_str).map(str::to_string);
        let logged_priced_spans = value_as_u64(row.get("logged_priced_spans"));
        let logged_cost = value_as_opt_f64(row.get("logged_cost")).unwrap_or(0.0);
        let query_unpriced = value_as_u64(row.get("unpriced_token_spans"));
        let no_usage_spans = value_as_u64(row.get("no_usage_spans"));

        // Fill file cost from per-segment token sums, when a pricing file is
        // set. `segments` is non-empty exactly then, and the query emitted the
        // matching `p{i}_*` columns.
        let mut file_cost = 0.0;
        let mut file_priced_spans = 0_u64;
        let mut unpriced_spans = query_unpriced;
        if price_book.is_some() && !segments.is_empty() {
            unpriced_spans = 0;
            for (index, segment) in segments.iter().enumerate() {
                let usage = token_usage_from_row(row, index);
                let rate = model.as_deref().and_then(|model| {
                    price_book.and_then(|book| book.rate_at(model, segment.since))
                });
                if let Some(rate) = rate {
                    file_cost += usage.cost(rate);
                    file_priced_spans = file_priced_spans.saturating_add(usage.spans);
                } else {
                    unpriced_spans = unpriced_spans.saturating_add(usage.spans);
                }
            }
        }

        let keys = display_key(row, internal_dims, display_dims);
        let entry = CostRow {
            keys: keys.clone(),
            logged_cost,
            file_cost,
            logged_priced_spans,
            file_priced_spans,
            unpriced_spans,
            no_usage_spans,
        };
        accumulator
            .entry(keys)
            .and_modify(|existing| existing.merge(&entry))
            .or_insert(entry);
    }
    candidate_total
}

/// Extract the display-dimension values from a result row.
fn display_key(
    row: &Map<String, Value>,
    _internal_dims: &[Dimension],
    display_dims: &[Dimension],
) -> Vec<Option<String>> {
    display_dims
        .iter()
        .map(|dim| dimension_value(row, *dim))
        .collect()
}

fn dimension_value(row: &Map<String, Value>, dim: Dimension) -> Option<String> {
    match row.get(dim.alias()) {
        Some(Value::String(value)) => Some(value.clone()),
        Some(Value::Number(number)) => Some(number.to_string()),
        Some(Value::Bool(value)) => Some(value.to_string()),
        _ => None,
    }
}

/// Sort rows according to the `--group-by` order. The order defines the
/// nesting hierarchy: the first dimension is the outer group, the next nests
/// within it, and so on.
///
/// Time dimensions (`day`/`hour`) and everything nested below the first time
/// dimension are ordered chronologically (oldest first, most recent last).
/// Categorical levels are ordered by their group total cost descending — so a
/// categorical level above a time dimension groups its values together (each
/// by its subtotal), and a categorical level below a time dimension orders
/// siblings within the same time bucket by cost.
pub(super) fn sort_rows(rows: &mut [CostRow], display_dims: &[Dimension]) {
    let is_time: Vec<bool> = display_dims
        .iter()
        .map(|dim| matches!(dim, Dimension::Day | Dimension::Hour))
        .collect();

    // Total cost per distinct key prefix, so a categorical level is ordered by
    // its group subtotal (summing every row that shares that prefix) rather than
    // by any single row's cost. The full-key prefix is the row itself, so a leaf
    // categorical level falls back to per-row cost.
    let mut prefix_costs: HashMap<Vec<Option<String>>, f64> = HashMap::new();
    for row in rows.iter() {
        let cost = row.cost().unwrap_or(0.0);
        for end in 1..=row.keys.len() {
            *prefix_costs.entry(row.keys[..end].to_vec()).or_insert(0.0) += cost;
        }
    }

    rows.sort_by(|left, right| {
        for (index, &time_dim) in is_time.iter().enumerate() {
            let left_key = left.keys.get(index).and_then(|value| value.as_deref());
            let right_key = right.keys.get(index).and_then(|value| value.as_deref());
            let ordering = if time_dim {
                time_ordering(left_key, right_key)
            } else {
                let left_cost = prefix_costs
                    .get(&left.keys[..=index])
                    .copied()
                    .unwrap_or(0.0);
                let right_cost = prefix_costs
                    .get(&right.keys[..=index])
                    .copied()
                    .unwrap_or(0.0);
                right_cost
                    .partial_cmp(&left_cost)
                    .unwrap_or(Ordering::Equal)
            };
            if ordering != Ordering::Equal {
                return ordering;
            }
        }
        left.keys.cmp(&right.keys)
    });
}

/// Chronological ordering (ascending) for time-bucket keys. Both `day()` and
/// `hour()` return RFC 3339 bucket starts; date-only keys are accepted as a
/// fallback. Unparseable keys sort last so real chronology stays clean.
fn time_ordering(left: Option<&str>, right: Option<&str>) -> Ordering {
    match (time_epoch(left), time_epoch(right)) {
        (Some(left), Some(right)) => left.cmp(&right),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

fn time_epoch(value: Option<&str>) -> Option<i64> {
    let value = value?;
    if let Ok(instant) = DateTime::parse_from_rfc3339(value) {
        return Some(instant.timestamp());
    }
    let date = NaiveDate::parse_from_str(value, "%Y-%m-%d").ok()?;
    Some(date.and_hms_opt(0, 0, 0)?.and_utc().timestamp())
}

/// Compute totals from the final rows and the summed candidate span count.
pub(super) fn totals_from_rows(rows: &[CostRow], candidate_spans: u64) -> Totals {
    let mut totals = Totals {
        candidate_spans,
        ..Totals::default()
    };
    for row in rows {
        totals.logged_cost += row.logged_cost;
        totals.file_cost += row.file_cost;
        totals.logged_priced_spans += row.logged_priced_spans;
        totals.file_priced_spans += row.file_priced_spans;
        totals.unpriced_spans += row.unpriced_spans;
        totals.no_usage_spans += row.no_usage_spans;
    }
    totals
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cost::pricing::parse_timestamp;
    use serde_json::json;

    fn map(value: Value) -> Map<String, Value> {
        value.as_object().unwrap().clone()
    }

    #[test]
    fn accumulates_logged_cost_without_pricing_file() {
        let rows = vec![map(json!({
            "model": "gpt-5.6-sol",
            "candidate_spans": 3,
            "logged_priced_spans": 3,
            "logged_cost": 0.12,
            "unpriced_token_spans": 0,
            "no_usage_spans": 0,
        }))];
        let mut acc = BTreeMap::new();
        let candidates = accumulate_rows(
            &mut acc,
            &rows,
            &[Dimension::Model],
            &[Dimension::Model],
            &[],
            None,
        );
        assert_eq!(candidates, 3);
        let row = acc.values().next().unwrap();
        assert_eq!(row.cost(), Some(0.12));
        assert_eq!(row.priced_spans(), 3);
        assert_eq!(row.keys, vec![Some("gpt-5.6-sol".to_string())]);
    }

    #[test]
    fn merges_rows_sharing_a_display_key() {
        // group by model only; internal groups by (model) too, but two rows
        // with the same model (e.g. from project + playground queries) merge.
        let mut acc = BTreeMap::new();
        let dims = [Dimension::Model];
        accumulate_rows(
            &mut acc,
            &[map(json!({
                "model": "m", "candidate_spans": 1, "logged_priced_spans": 1,
                "logged_cost": 1.0, "unpriced_token_spans": 0, "no_usage_spans": 0,
            }))],
            &dims,
            &dims,
            &[],
            None,
        );
        accumulate_rows(
            &mut acc,
            &[map(json!({
                "model": "m", "candidate_spans": 1, "logged_priced_spans": 1,
                "logged_cost": 2.0, "unpriced_token_spans": 0, "no_usage_spans": 0,
            }))],
            &dims,
            &dims,
            &[],
            None,
        );
        assert_eq!(acc.len(), 1);
        assert_eq!(acc.values().next().unwrap().cost(), Some(3.0));
    }

    #[test]
    fn pricing_file_fills_unpriced_token_spans() {
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            file.path(),
            r#"
version = 1
[models."glm-5.2"]
[[models."glm-5.2".rates]]
effective_from = "2025-01-01"
input_usd_per_1m_tokens = 1.0
output_usd_per_1m_tokens = 2.0
"#,
        )
        .unwrap();
        let price_book = PriceBook::load(file.path()).unwrap();
        let segments = vec![TimeSegment {
            since: parse_timestamp("2025-01-01").unwrap(),
            until: parse_timestamp("2025-02-01").unwrap(),
        }];
        let rows = vec![map(json!({
            "model": "glm-5.2",
            "candidate_spans": 1,
            "logged_priced_spans": 0,
            "logged_cost": null,
            "unpriced_token_spans": 1,
            "no_usage_spans": 0,
            "p0_spans": 1,
            "p0_uncached_input_tokens": 1_000_000,
            "p0_cached_input_tokens": 0,
            "p0_effective_cache_write_tokens": 0,
            "p0_split_cache_write_5m_tokens": 0,
            "p0_split_cache_write_1h_tokens": 0,
            "p0_fallback_cache_write_tokens": 0,
            "p0_output_tokens": 1_000_000,
        }))];
        let mut acc = BTreeMap::new();
        accumulate_rows(
            &mut acc,
            &rows,
            &[Dimension::Trace, Dimension::Model],
            &[Dimension::Trace],
            &segments,
            Some(&price_book),
        );
        let row = acc.values().next().unwrap();
        assert_eq!(row.file_cost, 3.0);
        assert_eq!(row.file_priced_spans, 1);
        assert_eq!(row.unpriced_spans, 0);
        assert_eq!(row.cost(), Some(3.0));
    }

    #[test]
    fn sorts_by_cost_descending() {
        let mut rows = vec![
            CostRow {
                keys: vec![Some("a".to_string())],
                logged_cost: 1.0,
                file_cost: 0.0,
                logged_priced_spans: 1,
                file_priced_spans: 0,
                unpriced_spans: 0,
                no_usage_spans: 0,
            },
            CostRow {
                keys: vec![Some("b".to_string())],
                logged_cost: 5.0,
                file_cost: 0.0,
                logged_priced_spans: 1,
                file_priced_spans: 0,
                unpriced_spans: 0,
                no_usage_spans: 0,
            },
        ];
        sort_rows(&mut rows, &[Dimension::Model]);
        assert_eq!(rows[0].keys, vec![Some("b".to_string())]);
    }

    fn row(keys: &[&str], cost: f64) -> CostRow {
        CostRow {
            keys: keys
                .iter()
                .map(|value| Some((*value).to_string()))
                .collect(),
            logged_cost: cost,
            file_cost: 0.0,
            logged_priced_spans: 1,
            file_priced_spans: 0,
            unpriced_spans: 0,
            no_usage_spans: 0,
        }
    }

    fn key_str(row: &CostRow) -> String {
        row.keys
            .iter()
            .map(|value| value.clone().unwrap_or_else(|| "—".to_string()))
            .collect::<Vec<_>>()
            .join("/")
    }

    #[test]
    fn categorical_levels_order_by_group_subtotal_descending() {
        // --group-by user --group-by model: users ordered by their total cost
        // (desc), models ordered within each user by their subtotal (desc).
        let mut rows = vec![
            row(&["alice", "gpt"], 1.0),
            row(&["alice", "claude"], 3.0),
            row(&["bob", "gpt"], 5.0),
            row(&["bob", "claude"], 1.0),
        ];
        sort_rows(&mut rows, &[Dimension::User, Dimension::Model]);
        let keys: Vec<String> = rows.iter().map(key_str).collect();
        // bob total (6.0) > alice total (4.0); within bob, gpt (5.0) > claude
        // (1.0); within alice, claude (3.0) > gpt (1.0).
        assert_eq!(
            keys,
            vec!["bob/gpt", "bob/claude", "alice/claude", "alice/gpt"]
        );
    }

    #[test]
    fn time_dimension_orders_chronologically_oldest_first() {
        // --group-by hour: rows ordered by time ascending (oldest first).
        let mut rows = vec![
            row(&["2026-07-21T05:00:00Z"], 5.0),
            row(&["2026-07-21T03:00:00Z"], 1.0),
            row(&["2026-07-21T04:00:00Z"], 3.0),
        ];
        sort_rows(&mut rows, &[Dimension::Hour]);
        let keys: Vec<String> = rows.iter().map(key_str).collect();
        assert_eq!(
            keys,
            vec![
                "2026-07-21T03:00:00Z",
                "2026-07-21T04:00:00Z",
                "2026-07-21T05:00:00Z"
            ]
        );
    }

    #[test]
    fn time_dimension_orders_descending_levels_chronologically() {
        // --group-by user --group-by hour --group-by model: users by cost desc,
        // then hours chronological within each user, then models by cost desc
        // within each (user, hour).
        let mut rows = vec![
            row(&["alice", "2026-07-21T05:00:00Z", "gpt"], 1.0),
            row(&["alice", "2026-07-21T03:00:00Z", "claude"], 1.0),
            row(&["alice", "2026-07-21T04:00:00Z", "gpt"], 3.0),
            row(&["bob", "2026-07-21T03:00:00Z", "gpt"], 1.0),
            row(&["bob", "2026-07-21T05:00:00Z", "claude"], 5.0),
        ];
        sort_rows(
            &mut rows,
            &[Dimension::User, Dimension::Hour, Dimension::Model],
        );
        let keys: Vec<String> = rows.iter().map(key_str).collect();
        // bob (6.0) before alice (5.0); within each, hours ascending; within
        // (alice, 04:00), only gpt; within (bob, 05:00), only claude.
        assert_eq!(
            keys,
            vec![
                "bob/2026-07-21T03:00:00Z/gpt",
                "bob/2026-07-21T05:00:00Z/claude",
                "alice/2026-07-21T03:00:00Z/claude",
                "alice/2026-07-21T04:00:00Z/gpt",
                "alice/2026-07-21T05:00:00Z/gpt"
            ]
        );
    }
}
