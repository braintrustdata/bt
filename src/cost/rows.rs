//! Turn raw BTQL result rows into display cost rows.
//!
//! `estimated_cost()` supplies logged/registry cost (the preferred, accurate
//! source). A pricing file fills spans it leaves unpriced, using per-segment
//! token sums. The two populations are disjoint, so costs and counts add
//! cleanly. Rows are then aggregated from the internal grouping (display
//! dimensions plus `model`) up to the display dimensions.

use std::cmp::Ordering;
use std::collections::BTreeMap;

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

/// Sort rows by cost descending, then by key for stable ordering.
pub(super) fn sort_rows(rows: &mut [CostRow]) {
    rows.sort_by(|left, right| {
        let left_cost = left.cost().unwrap_or(f64::NEG_INFINITY);
        let right_cost = right.cost().unwrap_or(f64::NEG_INFINITY);
        right_cost
            .partial_cmp(&left_cost)
            .unwrap_or(Ordering::Equal)
            .then_with(|| left.keys.cmp(&right.keys))
    });
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
        sort_rows(&mut rows);
        assert_eq!(rows[0].keys, vec![Some("b".to_string())]);
    }
}
