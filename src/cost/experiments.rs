use std::collections::HashMap;
use std::fmt::Write as _;

use anyhow::Result;
use dialoguer::console;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::{
    experiments::api::list_experiments,
    sql::run_btql_rows,
    ui::{apply_column_padding, header, print_with_pager, styled_table, truncate, with_spinner},
    utils::{format_cost, pluralize},
};

use super::ResolvedContext;

/// Per-experiment cost stats returned by the aggregate query.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
struct CostStats {
    /// `SUM(estimated_cost())` — null when nothing could be priced.
    cost: Option<f64>,
    /// Spans with a non-null `estimated_cost()` (the ones feeding the sum).
    priced_spans: u64,
    /// Non-scorer spans that clearly involved an LLM (llm span or token
    /// metrics) but could not be priced — the "we don't know" signal.
    unpriced_llm_spans: u64,
}

/// `estimated_cost()` returns NULL for spans it cannot price (no logged cost and
/// no model-registry match) and for scorer spans. Comparing priced vs. unpriced
/// LLM spans lets us report cost coverage instead of a silently-wrong `$0`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
enum Coverage {
    /// Every LLM span was priced.
    Full,
    /// Some, but not all, LLM spans were priced.
    Partial,
    /// LLM spans exist but none could be priced (e.g. missing model pricing).
    Unknown,
    /// No priced spans and no detectable unpriced LLM activity — no cost data.
    None,
}

impl Coverage {
    fn classify(priced_spans: u64, unpriced_llm_spans: u64) -> Self {
        match (priced_spans, unpriced_llm_spans) {
            (0, 0) => Coverage::None,
            (0, _) => Coverage::Unknown,
            (_, 0) => Coverage::Full,
            (_, _) => Coverage::Partial,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
struct ExperimentCostRow {
    id: String,
    name: String,
    created: Option<String>,
    /// Estimated cost in USD. `null` when we could not price anything.
    cost: Option<f64>,
    priced_spans: u64,
    unpriced_llm_spans: u64,
    coverage: Coverage,
}

pub(crate) async fn run(ctx: &ResolvedContext, json: bool) -> Result<()> {
    let project_name = &ctx.project.name;
    let experiments = with_spinner(
        "Loading experiments...",
        list_experiments(&ctx.client, project_name),
    )
    .await?;

    if experiments.is_empty() {
        if json {
            println!("[]");
        } else {
            println!("No experiments found in {project_name}");
        }
        return Ok(());
    }

    let experiment_ids: Vec<String> = experiments.iter().map(|e| e.id.clone()).collect();
    let query = build_cost_query(&experiment_ids);
    let result_rows = with_spinner(
        "Estimating cost...",
        run_btql_rows(&ctx.client, &query, "strict"),
    )
    .await?;

    let stats_by_id = index_cost_rows(&result_rows);

    let mut rows: Vec<ExperimentCostRow> = experiments
        .iter()
        .map(|exp| {
            let stats = stats_by_id.get(&exp.id).copied().unwrap_or_default();
            let coverage = Coverage::classify(stats.priced_spans, stats.unpriced_llm_spans);
            ExperimentCostRow {
                id: exp.id.clone(),
                name: exp.name.clone(),
                created: exp.created.clone(),
                cost: stats.cost,
                priced_spans: stats.priced_spans,
                unpriced_llm_spans: stats.unpriced_llm_spans,
                coverage,
            }
        })
        .collect();

    // Highest cost first; experiments with no known cost sort last, tie-broken by name.
    rows.sort_by(|a, b| {
        let a_key = a.cost.unwrap_or(f64::NEG_INFINITY);
        let b_key = b.cost.unwrap_or(f64::NEG_INFINITY);
        b_key
            .partial_cmp(&a_key)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.name.cmp(&b.name))
    });

    if json {
        println!("{}", serde_json::to_string(&rows)?);
        return Ok(());
    }

    print_table(ctx, &rows)?;
    Ok(())
}

fn build_cost_query(experiment_ids: &[String]) -> String {
    let id_list = experiment_ids
        .iter()
        .map(|id| format!("'{}'", id.replace('\'', "''")))
        .collect::<Vec<_>>()
        .join(", ");

    // `SUM(estimated_cost())` matches the web UI's experiment cost (no span-type
    // filter; scorer spans are already unpriced by `estimated_cost()`).
    // `unpriced_llm_spans` counts non-scorer spans that had LLM activity (llm
    // span type or token metrics) yet could not be priced.
    format!(
        "SELECT \
           experiment_id, \
           SUM(estimated_cost()) AS cost, \
           COUNT(estimated_cost()) AS priced_spans, \
           COUNT(CASE \
             WHEN (span_attributes.purpose IS NULL OR span_attributes.purpose != 'scorer') \
              AND estimated_cost() IS NULL \
              AND (span_attributes.type = 'llm' \
                   OR metrics.prompt_tokens IS NOT NULL \
                   OR metrics.completion_tokens IS NOT NULL) \
             THEN 1 END) AS unpriced_llm_spans \
         FROM experiment({id_list}) \
         GROUP BY experiment_id"
    )
}

fn index_cost_rows(result_rows: &[Map<String, Value>]) -> HashMap<String, CostStats> {
    let mut stats = HashMap::new();
    for row in result_rows {
        let Some(id) = row.get("experiment_id").and_then(Value::as_str) else {
            continue;
        };
        stats.insert(
            id.to_string(),
            CostStats {
                cost: value_as_opt_f64(row.get("cost")),
                priced_spans: value_as_u64(row.get("priced_spans")),
                unpriced_llm_spans: value_as_u64(row.get("unpriced_llm_spans")),
            },
        );
    }
    stats
}

fn print_table(ctx: &ResolvedContext, rows: &[ExperimentCostRow]) -> Result<()> {
    let mut output = String::new();
    let count = format!(
        "{} {}",
        rows.len(),
        pluralize(rows.len(), "experiment", None)
    );
    writeln!(
        output,
        "{} in {} {} {}\n",
        console::style(count),
        console::style(ctx.client.org_name()).bold(),
        console::style("/").dim().bold(),
        console::style(&ctx.project.name).bold()
    )?;

    let mut table = styled_table();
    table.set_header(vec![
        header("Name"),
        header("Created"),
        header("Cost"),
        header("Coverage"),
    ]);
    apply_column_padding(&mut table, (0, 4));

    for row in rows {
        let created = row
            .created
            .as_deref()
            .map(|c| truncate(c, 10))
            .unwrap_or_else(|| "-".to_string());
        table.add_row(vec![
            truncate(&row.name, 60),
            created,
            cost_display(row.cost, row.coverage),
            coverage_display(row.priced_spans, row.unpriced_llm_spans, row.coverage),
        ]);
    }

    write!(output, "{table}")?;

    let priced_total: f64 = rows.iter().filter_map(|r| r.cost).sum();
    let partial = rows
        .iter()
        .filter(|r| r.coverage == Coverage::Partial)
        .count();
    let unknown = rows
        .iter()
        .filter(|r| r.coverage == Coverage::Unknown)
        .count();
    let no_data = rows.iter().filter(|r| r.coverage == Coverage::None).count();

    write!(
        output,
        "\n\nTotal (priced): {}",
        console::style(format_cost(priced_total)).bold()
    )?;
    if partial > 0 {
        write!(
            output,
            "  {}",
            console::style(format!("{partial} partial")).yellow()
        )?;
    }
    if unknown > 0 {
        write!(
            output,
            "  {}",
            console::style(format!("{unknown} unknown")).yellow()
        )?;
    }
    if no_data > 0 {
        write!(
            output,
            "  {}",
            console::style(format!("{no_data} without LLM cost data")).dim()
        )?;
    }
    output.push('\n');

    print_with_pager(&output)?;
    Ok(())
}

fn cost_display(cost: Option<f64>, coverage: Coverage) -> String {
    match coverage {
        Coverage::Full => format_cost(cost.unwrap_or(0.0)),
        Coverage::Partial => format!("~{}", format_cost(cost.unwrap_or(0.0))),
        Coverage::Unknown => "unknown".to_string(),
        Coverage::None => "n/a".to_string(),
    }
}

fn coverage_display(priced_spans: u64, unpriced_llm_spans: u64, coverage: Coverage) -> String {
    match coverage {
        Coverage::None => "—".to_string(),
        _ => format!("{}/{}", priced_spans, priced_spans + unpriced_llm_spans),
    }
}

fn value_as_opt_f64(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.parse().ok(),
        _ => None,
    }
}

fn value_as_u64(value: Option<&Value>) -> u64 {
    match value {
        Some(Value::Number(n)) => n
            .as_u64()
            .or_else(|| n.as_f64().map(|f| f.max(0.0) as u64))
            .unwrap_or(0),
        Some(Value::String(s)) => s.parse().unwrap_or(0),
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn build_cost_query_lists_ids_and_matches_ui_sum() {
        let query = build_cost_query(&["exp-a".to_string(), "exp-b".to_string()]);
        assert!(
            query.contains("FROM experiment('exp-a', 'exp-b')"),
            "{query}"
        );
        assert!(query.contains("SUM(estimated_cost()) AS cost"), "{query}");
        assert!(
            query.contains("COUNT(estimated_cost()) AS priced_spans"),
            "{query}"
        );
        assert!(query.contains("AS unpriced_llm_spans"), "{query}");
        assert!(
            query.contains("span_attributes.purpose != 'scorer'"),
            "{query}"
        );
        assert!(query.contains("span_attributes.type = 'llm'"), "{query}");
        assert!(
            query.contains("metrics.prompt_tokens IS NOT NULL"),
            "{query}"
        );
        assert!(query.contains("GROUP BY experiment_id"), "{query}");
        // The cost sum must not be restricted by span type, to match the web UI.
        assert!(
            !query.contains("WHERE"),
            "cost sum should not be filtered: {query}"
        );
    }

    #[test]
    fn build_cost_query_escapes_single_quotes() {
        let query = build_cost_query(&["ex'p".to_string()]);
        assert!(query.contains("experiment('ex''p')"), "{query}");
    }

    #[test]
    fn classify_covers_all_cases() {
        assert_eq!(Coverage::classify(0, 0), Coverage::None);
        assert_eq!(Coverage::classify(0, 5), Coverage::Unknown);
        assert_eq!(Coverage::classify(3, 2), Coverage::Partial);
        assert_eq!(Coverage::classify(5, 0), Coverage::Full);
    }

    #[test]
    fn cost_and_coverage_display_reflect_classification() {
        assert_eq!(cost_display(Some(1.5), Coverage::Full), "$1.50");
        assert_eq!(cost_display(Some(1.5), Coverage::Partial), "~$1.50");
        assert_eq!(cost_display(None, Coverage::Unknown), "unknown");
        assert_eq!(cost_display(None, Coverage::None), "n/a");

        assert_eq!(coverage_display(5, 0, Coverage::Full), "5/5");
        assert_eq!(coverage_display(3, 2, Coverage::Partial), "3/5");
        assert_eq!(coverage_display(0, 4, Coverage::Unknown), "0/4");
        assert_eq!(coverage_display(0, 0, Coverage::None), "—");
    }

    #[test]
    fn index_cost_rows_parses_null_cost_and_counts() {
        let rows = vec![
            json!({
                "experiment_id": "exp-a",
                "cost": 1.25,
                "priced_spans": 8,
                "unpriced_llm_spans": 2
            })
            .as_object()
            .unwrap()
            .clone(),
            json!({
                "experiment_id": "exp-b",
                "cost": null,
                "priced_spans": 0,
                "unpriced_llm_spans": 0
            })
            .as_object()
            .unwrap()
            .clone(),
        ];
        let stats = index_cost_rows(&rows);
        assert_eq!(
            stats.get("exp-a"),
            Some(&CostStats {
                cost: Some(1.25),
                priced_spans: 8,
                unpriced_llm_spans: 2
            })
        );
        assert_eq!(
            stats.get("exp-b"),
            Some(&CostStats {
                cost: None,
                priced_spans: 0,
                unpriced_llm_spans: 0
            })
        );
    }

    #[test]
    fn experiment_cost_row_json_shape() {
        let row = ExperimentCostRow {
            id: "exp-a".to_string(),
            name: "baseline".to_string(),
            created: Some("2024-01-02T03:04:05Z".to_string()),
            cost: Some(1.25),
            priced_spans: 8,
            unpriced_llm_spans: 2,
            coverage: Coverage::Partial,
        };
        let value = serde_json::to_value(&row).unwrap();
        assert_eq!(value["id"], "exp-a");
        assert_eq!(value["name"], "baseline");
        assert_eq!(value["cost"], 1.25);
        assert_eq!(value["priced_spans"], 8);
        assert_eq!(value["unpriced_llm_spans"], 2);
        assert_eq!(value["coverage"], "partial");
    }

    #[test]
    fn experiment_cost_row_json_null_cost_for_no_data() {
        let row = ExperimentCostRow {
            id: "exp-z".to_string(),
            name: "empty".to_string(),
            created: None,
            cost: None,
            priced_spans: 0,
            unpriced_llm_spans: 0,
            coverage: Coverage::None,
        };
        let value = serde_json::to_value(&row).unwrap();
        assert!(value["cost"].is_null());
        assert_eq!(value["coverage"], "none");
    }
}
