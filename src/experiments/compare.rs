use std::collections::{BTreeMap, BTreeSet};

use anyhow::{anyhow, bail, Result};
use crossterm::style::Stylize;
use serde::Serialize;

use crate::ui::{
    box_with_title, render_experiment_summary_table, summary_metric_unit, with_spinner,
    SummaryExperimentColumn, SummaryMetricCell, SummaryMetricKind, SummaryMetricRow,
    SummaryTableOptions,
};

use super::{api, CompareArgs, ParsedExperimentCompareUrl, ResolvedContext};

const MAX_EXPERIMENTS: usize = 8;

#[derive(Debug, Clone, Serialize)]
struct ExperimentComparisonReport {
    project: ExperimentProjectRef,
    base: ExperimentSummaryRef,
    comparisons: Vec<ExperimentSummaryRef>,
    rows: Vec<MetricComparisonRow>,
}

#[derive(Debug, Clone, Serialize)]
struct ExperimentProjectRef {
    id: String,
    name: String,
}

#[derive(Debug, Clone, Serialize)]
struct ExperimentSummaryRef {
    id: String,
    name: String,
    url: String,
}

#[derive(Debug, Clone, Serialize)]
struct MetricComparisonRow {
    metric: String,
    kind: String,
    aggregation: String,
    unit: Option<String>,
    experiment: String,
    role: String,
    value: Option<f64>,
    delta_vs_base: Option<f64>,
    improvements: usize,
    regressions: usize,
}

#[derive(Debug)]
struct CompareTarget {
    base_experiment: String,
    comparison_experiments: Vec<String>,
}

pub(super) async fn run(
    ctx: &ResolvedContext,
    args: CompareArgs,
    parsed_url: Option<&ParsedExperimentCompareUrl>,
    json: bool,
) -> Result<()> {
    let target = resolve_compare_target(&args, parsed_url)?;
    let project_name = &ctx.project.name;

    let base_experiment = with_spinner(
        "Loading base experiment...",
        api::get_experiment_by_name(&ctx.client, project_name, &target.base_experiment),
    )
    .await?
    .ok_or_else(|| anyhow!("experiment '{}' not found", target.base_experiment))?;

    let mut comparison_experiments = Vec::new();
    for comparison_name in &target.comparison_experiments {
        let message = format!("Loading comparison experiment {comparison_name}...");
        let experiment = with_spinner(
            &message,
            api::get_experiment_by_name(&ctx.client, project_name, comparison_name),
        )
        .await?
        .ok_or_else(|| anyhow!("experiment '{comparison_name}' not found"))?;
        comparison_experiments.push(experiment);
    }

    let base_summary = with_spinner(
        "Loading base experiment summary...",
        api::summarize_experiment(&ctx.client, &base_experiment.id, Some(&base_experiment.id)),
    )
    .await?;

    let mut comparison_summaries = Vec::new();
    for experiment in &comparison_experiments {
        let message = format!("Loading summary for {}...", experiment.name);
        let summary = with_spinner(
            &message,
            api::summarize_experiment(&ctx.client, &experiment.id, Some(&base_experiment.id)),
        )
        .await?;
        comparison_summaries.push(summary);
    }

    let report = build_report(
        ctx,
        &base_experiment,
        &comparison_experiments,
        &base_summary,
        &comparison_summaries,
    );

    if json {
        println!("{}", serde_json::to_string_pretty(&report)?);
    } else {
        print_report_text(&report, args.all)?;
    }
    Ok(())
}

fn resolve_compare_target(
    args: &CompareArgs,
    parsed_url: Option<&ParsedExperimentCompareUrl>,
) -> Result<CompareTarget> {
    let flag_or_url_base = args
        .base
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .or_else(|| {
            parsed_url
                .and_then(|url| url.base_experiment.as_deref())
                .map(str::trim)
                .filter(|value| !value.is_empty())
        });
    let positional_base = args
        .base_or_url
        .as_deref()
        .filter(|value| !super::looks_like_url(value))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .filter(|_| flag_or_url_base.is_none());

    let base_experiment = flag_or_url_base.or(positional_base).ok_or_else(|| {
        anyhow!("base experiment required. Use: bt experiments compare <base> <comparison>")
    })?;

    let mut comparison_experiments = Vec::new();
    if let Some(comparison) = parsed_url
        .and_then(|url| url.comparison_experiment.as_deref())
        .and_then(clean_name)
    {
        comparison_experiments.push(comparison);
    }
    if flag_or_url_base.is_some() {
        if let Some(comparison) = args
            .base_or_url
            .as_deref()
            .filter(|value| !super::looks_like_url(value))
            .and_then(clean_name)
        {
            comparison_experiments.push(comparison);
        }
    }
    comparison_experiments.extend(
        args.comparison_positional
            .iter()
            .filter_map(|value| clean_name(value)),
    );
    comparison_experiments.extend(args.comparison.iter().filter_map(|value| clean_name(value)));

    dedupe_preserving_order(&mut comparison_experiments);

    if comparison_experiments.is_empty() {
        bail!("comparison experiment required. Use: bt experiments compare <base> <comparison>");
    }
    if comparison_experiments.len() + 1 > MAX_EXPERIMENTS {
        bail!(
            "at most {MAX_EXPERIMENTS} experiments supported (base plus up to {} comparisons)",
            MAX_EXPERIMENTS - 1
        );
    }

    Ok(CompareTarget {
        base_experiment: base_experiment.to_string(),
        comparison_experiments,
    })
}

fn clean_name(value: &str) -> Option<String> {
    let value = value.trim();
    (!value.is_empty()).then(|| value.to_string())
}

fn dedupe_preserving_order(values: &mut Vec<String>) {
    let mut seen = BTreeSet::new();
    values.retain(|value| seen.insert(value.clone()));
}

fn build_report(
    ctx: &ResolvedContext,
    base_experiment: &api::Experiment,
    comparison_experiments: &[api::Experiment],
    base_summary: &api::ExperimentSummary,
    comparison_summaries: &[api::ExperimentSummary],
) -> ExperimentComparisonReport {
    let comparisons = comparison_experiments
        .iter()
        .zip(comparison_summaries)
        .map(|(experiment, summary)| ExperimentSummaryRef {
            id: experiment.id.clone(),
            name: experiment.name.clone(),
            url: summary.experiment_url.clone(),
        })
        .collect::<Vec<_>>();

    ExperimentComparisonReport {
        project: ExperimentProjectRef {
            id: ctx.project.id.clone(),
            name: ctx.project.name.clone(),
        },
        base: ExperimentSummaryRef {
            id: base_experiment.id.clone(),
            name: base_experiment.name.clone(),
            url: base_summary.experiment_url.clone(),
        },
        comparisons,
        rows: build_rows(
            &base_experiment.name,
            comparison_experiments,
            base_summary,
            comparison_summaries,
        ),
    }
}

fn build_rows(
    base_experiment_name: &str,
    comparison_experiments: &[api::Experiment],
    base_summary: &api::ExperimentSummary,
    comparison_summaries: &[api::ExperimentSummary],
) -> Vec<MetricComparisonRow> {
    let mut rows = Vec::new();

    let base_scores = map_scores(base_summary);
    let comparison_scores = comparison_summaries
        .iter()
        .map(map_scores)
        .collect::<Vec<_>>();
    for key in sorted_keys(&base_scores, &comparison_scores) {
        let base = base_scores.get(&key);
        let display_name = display_name(&key, base, &comparison_scores, |score| &score.name);
        rows.push(score_row(
            &display_name,
            base_experiment_name,
            "base",
            base,
            base,
        ));
        for (experiment, scores) in comparison_experiments.iter().zip(&comparison_scores) {
            rows.push(score_row(
                &display_name,
                &experiment.name,
                "comparison",
                scores.get(&key),
                base,
            ));
        }
    }

    let base_metrics = map_metrics(base_summary);
    let comparison_metrics = comparison_summaries
        .iter()
        .map(map_metrics)
        .collect::<Vec<_>>();
    for key in sorted_keys(&base_metrics, &comparison_metrics) {
        let base = base_metrics.get(&key);
        let display_name = display_name(&key, base, &comparison_metrics, |metric| &metric.name);
        rows.push(metric_row(
            &display_name,
            base_experiment_name,
            "base",
            base,
            base,
        ));
        for (experiment, metrics) in comparison_experiments.iter().zip(&comparison_metrics) {
            rows.push(metric_row(
                &display_name,
                &experiment.name,
                "comparison",
                metrics.get(&key),
                base,
            ));
        }
    }

    rows
}

fn score_row(
    display_name: &str,
    experiment_name: &str,
    role: &str,
    score: Option<&api::ScoreSummary>,
    base: Option<&api::ScoreSummary>,
) -> MetricComparisonRow {
    let value = score.map(|value| value.score);
    let delta_vs_base = if role == "base" {
        None
    } else {
        score
            .and_then(|value| value.diff)
            .or_else(|| diff(value, base.map(|value| value.score)))
    };

    MetricComparisonRow {
        metric: display_name.to_string(),
        kind: "score".to_string(),
        aggregation: "avg".to_string(),
        unit: Some("%".to_string()),
        experiment: experiment_name.to_string(),
        role: role.to_string(),
        value,
        delta_vs_base,
        improvements: if role == "base" {
            0
        } else {
            score.map(|value| value.improvements).unwrap_or_default()
        },
        regressions: if role == "base" {
            0
        } else {
            score.map(|value| value.regressions).unwrap_or_default()
        },
    }
}

fn metric_row(
    display_name: &str,
    experiment_name: &str,
    role: &str,
    metric: Option<&api::MetricSummary>,
    base: Option<&api::MetricSummary>,
) -> MetricComparisonRow {
    let value = metric.map(|value| value.metric);
    let delta_vs_base = if role == "base" {
        None
    } else {
        metric
            .and_then(|value| value.diff)
            .or_else(|| diff(value, base.map(|value| value.metric)))
    };
    let unit = metric.or(base).map(|value| value.unit.as_str());

    MetricComparisonRow {
        metric: display_name.to_string(),
        kind: "metric".to_string(),
        aggregation: "avg".to_string(),
        unit: summary_metric_unit(display_name, unit),
        experiment: experiment_name.to_string(),
        role: role.to_string(),
        value,
        delta_vs_base,
        improvements: if role == "base" {
            0
        } else {
            metric.map(|value| value.improvements).unwrap_or_default()
        },
        regressions: if role == "base" {
            0
        } else {
            metric.map(|value| value.regressions).unwrap_or_default()
        },
    }
}

fn map_scores(summary: &api::ExperimentSummary) -> BTreeMap<String, api::ScoreSummary> {
    summary
        .scores
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect()
}

fn map_metrics(summary: &api::ExperimentSummary) -> BTreeMap<String, api::MetricSummary> {
    summary
        .metrics
        .clone()
        .unwrap_or_default()
        .into_iter()
        .collect()
}

fn sorted_keys<T>(base: &BTreeMap<String, T>, comparisons: &[BTreeMap<String, T>]) -> Vec<String> {
    let mut keys = base.keys().cloned().collect::<BTreeSet<_>>();
    for comparison in comparisons {
        keys.extend(comparison.keys().cloned());
    }
    keys.into_iter().collect()
}

fn display_name<T>(
    key: &str,
    base: Option<&T>,
    comparisons: &[BTreeMap<String, T>],
    name: impl Fn(&T) -> &str,
) -> String {
    base.or_else(|| comparisons.iter().find_map(|values| values.get(key)))
        .map(|value| name(value).to_string())
        .unwrap_or_else(|| key.to_string())
}

fn diff(comparison: Option<f64>, base: Option<f64>) -> Option<f64> {
    match (comparison, base) {
        (Some(comparison), Some(base)) => Some(comparison - base),
        _ => None,
    }
}

fn print_report_text(report: &ExperimentComparisonReport, show_all_rows: bool) -> Result<()> {
    let columns = report_summary_columns(report);
    let rows = report_summary_rows(report);
    let mut parts = vec![
        format!("project: {} ({})", report.project.id, report.project.name),
        comparison_summary_line(report),
    ];
    if !rows.is_empty() {
        parts.push(render_experiment_summary_table(
            &columns,
            &rows,
            &SummaryTableOptions {
                show_all_rows,
                hidden_rows_message: Some(
                    "zero/no-change rows omitted; use --all to include them".to_string(),
                ),
            },
        ));
    }

    println!(
        "{}",
        box_with_title("Experiment comparison", &parts.join("\n\n"))
    );
    Ok(())
}

fn report_summary_columns(report: &ExperimentComparisonReport) -> Vec<SummaryExperimentColumn> {
    let mut columns = Vec::with_capacity(report.comparisons.len() + 1);
    columns.push(SummaryExperimentColumn {
        name: report.base.name.clone(),
        role: Some("baseline".to_string()),
    });
    columns.extend(
        report
            .comparisons
            .iter()
            .map(|comparison| SummaryExperimentColumn {
                name: comparison.name.clone(),
                role: Some("comparison".to_string()),
            }),
    );
    columns
}

fn comparison_summary_line(report: &ExperimentComparisonReport) -> String {
    if report.comparisons.len() == 1 {
        format!(
            "{} {} ← {} {}",
            report.base.name,
            "(baseline)".dark_grey(),
            report.comparisons[0].name,
            "(comparison)".dark_grey(),
        )
    } else {
        format!(
            "{} {} ← {} comparisons",
            report.base.name,
            "(baseline)".dark_grey(),
            report.comparisons.len(),
        )
    }
}

fn report_summary_rows(report: &ExperimentComparisonReport) -> Vec<SummaryMetricRow> {
    let columns = report.comparisons.len() + 1;
    report
        .rows
        .chunks(columns)
        .filter(|chunk| !chunk.is_empty())
        .map(|chunk| {
            let first = &chunk[0];
            SummaryMetricRow {
                name: first.metric.clone(),
                kind: summary_metric_kind(&first.kind),
                unit: first.unit.clone(),
                cells: chunk.iter().map(report_summary_cell).collect(),
            }
        })
        .collect()
}

fn summary_metric_kind(kind: &str) -> SummaryMetricKind {
    if kind == "score" {
        SummaryMetricKind::Score
    } else {
        SummaryMetricKind::Metric
    }
}

fn report_summary_cell(row: &MetricComparisonRow) -> SummaryMetricCell {
    SummaryMetricCell {
        value: row.value,
        delta: row.delta_vs_base,
        improvements: row.improvements as i64,
        regressions: row.regressions as i64,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn test_experiment(name: &str) -> api::Experiment {
        api::Experiment {
            id: format!("{name}-id"),
            name: name.to_string(),
            project_id: "test-project-id".to_string(),
            public: false,
            description: None,
            created: None,
            dataset_id: None,
            dataset_version: None,
            base_exp_id: None,
            commit: None,
            user_id: None,
            tags: None,
            metadata: None,
        }
    }

    #[test]
    fn score_rows_use_endpoint_diff_for_comparison_delta() {
        let base = api::ScoreSummary {
            name: "quality".to_string(),
            score: 0.6,
            diff: Some(0.0),
            improvements: 0,
            regressions: 0,
        };
        let comparison = api::ScoreSummary {
            name: "quality".to_string(),
            score: 0.75,
            diff: Some(0.15),
            improvements: 3,
            regressions: 1,
        };

        let row = score_row(
            "quality",
            "challenger",
            "comparison",
            Some(&comparison),
            Some(&base),
        );

        assert_eq!(row.experiment, "challenger");
        assert_eq!(row.role, "comparison");
        assert_eq!(row.value, Some(0.75));
        assert_eq!(row.delta_vs_base, Some(0.15));
        assert_eq!(row.improvements, 3);
        assert_eq!(row.regressions, 1);
    }

    #[test]
    fn metric_rows_treat_time_to_first_token_as_seconds() {
        let metric = api::MetricSummary {
            name: "time_to_first_token".to_string(),
            metric: 1.23,
            unit: "tok".to_string(),
            diff: Some(0.1),
            improvements: 2,
            regressions: 1,
        };

        let row = metric_row(
            "time_to_first_token",
            "challenger",
            "comparison",
            Some(&metric),
            None,
        );

        assert_eq!(row.unit.as_deref(), Some("s"));
    }

    #[test]
    fn build_rows_includes_base_and_each_comparison() {
        let base = api::ExperimentSummary {
            project_name: "test-project".to_string(),
            experiment_name: "baseline".to_string(),
            project_url: "https://www.example.test/app/test-org/p/test-project".to_string(),
            experiment_url:
                "https://www.example.test/app/test-org/p/test-project/experiments/baseline"
                    .to_string(),
            comparison_experiment_name: Some("baseline".to_string()),
            scores: Some(HashMap::from([(
                "score_a".to_string(),
                api::ScoreSummary {
                    name: "score_a".to_string(),
                    score: 0.5,
                    diff: Some(0.0),
                    improvements: 0,
                    regressions: 0,
                },
            )])),
            metrics: Some(HashMap::from([(
                "duration".to_string(),
                api::MetricSummary {
                    name: "duration".to_string(),
                    metric: 2.0,
                    unit: "s".to_string(),
                    diff: Some(0.0),
                    improvements: 0,
                    regressions: 0,
                },
            )])),
        };
        let challenger_a = api::ExperimentSummary {
            project_name: "test-project".to_string(),
            experiment_name: "challenger-a".to_string(),
            project_url: "https://www.example.test/app/test-org/p/test-project".to_string(),
            experiment_url:
                "https://www.example.test/app/test-org/p/test-project/experiments/challenger-a"
                    .to_string(),
            comparison_experiment_name: Some("baseline".to_string()),
            scores: Some(HashMap::from([(
                "score_a".to_string(),
                api::ScoreSummary {
                    name: "score_a".to_string(),
                    score: 0.75,
                    diff: Some(0.25),
                    improvements: 1,
                    regressions: 0,
                },
            )])),
            metrics: Some(HashMap::from([(
                "duration".to_string(),
                api::MetricSummary {
                    name: "duration".to_string(),
                    metric: 3.0,
                    unit: "s".to_string(),
                    diff: Some(1.0),
                    improvements: 0,
                    regressions: 1,
                },
            )])),
        };
        let challenger_b = api::ExperimentSummary {
            project_name: "test-project".to_string(),
            experiment_name: "challenger-b".to_string(),
            project_url: "https://www.example.test/app/test-org/p/test-project".to_string(),
            experiment_url:
                "https://www.example.test/app/test-org/p/test-project/experiments/challenger-b"
                    .to_string(),
            comparison_experiment_name: Some("baseline".to_string()),
            scores: Some(HashMap::from([(
                "score_b".to_string(),
                api::ScoreSummary {
                    name: "score_b".to_string(),
                    score: 0.8,
                    diff: Some(0.8),
                    improvements: 2,
                    regressions: 0,
                },
            )])),
            metrics: None,
        };

        let rows = build_rows(
            "baseline",
            &[
                test_experiment("challenger-a"),
                test_experiment("challenger-b"),
            ],
            &base,
            &[challenger_a, challenger_b],
        );

        assert_eq!(rows.len(), 9);
        assert!(rows.iter().any(|row| row.metric == "score_a"
            && row.experiment == "baseline"
            && row.role == "base"
            && row.delta_vs_base.is_none()));
        assert!(rows.iter().any(|row| row.metric == "score_b"
            && row.experiment == "challenger-b"
            && row.delta_vs_base == Some(0.8)));
        assert!(rows.iter().any(|row| row.metric == "duration"
            && row.experiment == "challenger-a"
            && row.delta_vs_base == Some(1.0)));
    }

    #[test]
    fn resolve_compare_target_accepts_up_to_seven_comparisons() {
        let args = CompareArgs {
            base_or_url: Some("baseline".to_string()),
            comparison_positional: (1..=7).map(|idx| format!("challenger-{idx}")).collect(),
            url: None,
            base: None,
            comparison: Vec::new(),
            all: false,
        };

        let target = resolve_compare_target(&args, None).expect("resolve target");

        assert_eq!(target.base_experiment, "baseline");
        assert_eq!(target.comparison_experiments.len(), 7);
    }

    #[test]
    fn resolve_compare_target_rejects_more_than_eight_total_experiments() {
        let args = CompareArgs {
            base_or_url: Some("baseline".to_string()),
            comparison_positional: (1..=8).map(|idx| format!("challenger-{idx}")).collect(),
            url: None,
            base: None,
            comparison: Vec::new(),
            all: false,
        };

        let error = resolve_compare_target(&args, None).expect_err("too many experiments");

        assert!(error.to_string().contains("at most 8 experiments"));
    }

    #[test]
    fn resolve_compare_target_treats_first_positional_as_comparison_when_base_is_flag() {
        let args = CompareArgs {
            base_or_url: Some("challenger-a".to_string()),
            comparison_positional: vec!["challenger-b".to_string()],
            url: None,
            base: Some("baseline".to_string()),
            comparison: Vec::new(),
            all: false,
        };

        let target = resolve_compare_target(&args, None).expect("resolve target");

        assert_eq!(target.base_experiment, "baseline");
        assert_eq!(
            target.comparison_experiments,
            vec!["challenger-a".to_string(), "challenger-b".to_string()]
        );
    }
}
