use std::fmt::Write as _;
use std::io::{self, IsTerminal, Write};
use std::time::Duration;

use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use dialoguer::console;

use crate::ui::{print_with_pager, with_spinner};

use super::{
    api::{self, TopicAutomationStatus, TopicRuntimeSnapshot, TopicsStatusReport},
    formatting::{
        format_count, format_datetime_with_relative, format_duration_compact,
        format_relative_duration_seconds, format_timestamp_with_relative,
    },
    ResolvedContext, StatusArgs,
};

pub async fn run(ctx: &ResolvedContext, args: StatusArgs, json: bool) -> Result<()> {
    if json && args.watch {
        bail!("--watch is not supported with --json");
    }

    if args.watch {
        return watch(ctx, args.full).await;
    }

    let report = with_spinner("Loading Topics status...", api::fetch_topics_status(ctx)).await?;
    if json {
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }

    let output = render_report(&report, args.full);
    print_with_pager(&output)?;
    Ok(())
}

async fn watch(ctx: &ResolvedContext, full: bool) -> Result<()> {
    let is_tty = io::stdout().is_terminal();
    let mut first_frame = true;
    let mut ctrl_c = std::pin::pin!(tokio::signal::ctrl_c());

    loop {
        let report = tokio::select! {
            _ = &mut ctrl_c => {
                if is_tty {
                    println!();
                }
                break;
            }
            report = api::fetch_topics_status(ctx) => report?,
        };
        let output = render_report(&report, full);

        if is_tty {
            print!("\x1b[2J\x1b[H");
        } else if !first_frame {
            println!("\n---");
        }

        print!("{output}");
        if !output.ends_with('\n') {
            println!();
        }
        io::stdout().flush()?;
        first_frame = false;

        tokio::select! {
            _ = &mut ctrl_c => {
                if is_tty {
                    println!();
                }
                break;
            }
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
        }
    }

    Ok(())
}

fn render_report(report: &TopicsStatusReport, full: bool) -> String {
    let mut output = String::new();
    writeln!(
        output,
        "Project: {} {} {}{}{}{}",
        console::style(report.project.org_name.as_str()).bold(),
        console::style("/").dim().bold(),
        console::style(report.project.name.as_str()).bold(),
        console::style(" (").dim(),
        console::style(report.project.id.as_str()).dim(),
        console::style(")").dim()
    )
    .expect("write to string");

    if report.automations.is_empty() {
        writeln!(output, "No topic automations found.").expect("write to string");
        return output;
    }

    if report.automations.len() > 1 {
        writeln!(output, "Topic automations: {}", report.automations.len())
            .expect("write to string");
    }

    for (index, automation) in report.automations.iter().enumerate() {
        if index > 0 {
            writeln!(output, "\n---").expect("write to string");
        }
        write_automation_summary(&mut output, automation, full).expect("write to string");
    }

    output
}

fn write_automation_summary(
    output: &mut String,
    automation: &TopicAutomationStatus,
    full: bool,
) -> std::fmt::Result {
    let progress_window = format_progress_window_label(automation);

    writeln!(output, "\n{} ({})", automation.name, automation.id)?;
    writeln!(output, "status: {}", format_overall_status(automation))?;
    if let Some(next_event) = format_next_event_summary(automation) {
        writeln!(output, "{next_event}")?;
    }
    writeln!(
        output,
        "coverage: {}",
        format_coverage_summary(automation, &progress_window)
    )?;
    writeln!(output, "labels: {}", format_label_summary(automation))?;
    writeln!(
        output,
        "facets: {}",
        format_facet_processing_status(automation)
    )?;

    if !automation.facets.is_empty() && automation.total_traces > 0 {
        writeln!(output)?;
        writeln!(output, "facet progress:")?;
        for line in render_progress_lines(
            &automation.facets,
            &["considered", "matched", "running", "errors"],
            |item| {
                vec![
                    item.checked_count.to_string(),
                    item.matched_count.to_string(),
                    item.processing_count.to_string(),
                    item.error_count.to_string(),
                ]
            },
        ) {
            writeln!(output, "{line}")?;
        }
    }
    if !automation.topics.is_empty() && automation.total_traces > 0 {
        writeln!(output)?;
        writeln!(output, "topic progress:")?;
        for line in render_progress_lines(
            &automation.topics,
            &["considered", "labeled", "running", "errors"],
            |item| {
                vec![
                    item.checked_count.to_string(),
                    item.completed_count.to_string(),
                    item.processing_count.to_string(),
                    item.error_count.to_string(),
                ]
            },
        ) {
            writeln!(output, "{line}")?;
        }
    }
    if full {
        writeln!(output)?;
        write_automation_diagnostics(output, automation)?;
    }

    Ok(())
}

fn write_automation_diagnostics(
    output: &mut String,
    automation: &TopicAutomationStatus,
) -> std::fmt::Result {
    let Some(runtime) = automation.object_cursor.topic_runtime.as_ref() else {
        writeln!(output, "details:")?;
        writeln!(output, "  runtime: unavailable")?;
        return Ok(());
    };

    writeln!(output, "details:")?;
    writeln!(
        output,
        "  scope: {}",
        automation.scope_type.as_deref().unwrap_or("n/a")
    )?;
    writeln!(
        output,
        "  schedule: window {} | cadence {} | overlap {} | idle {}",
        format_duration_compact(automation.window_seconds),
        format_duration_compact(automation.rerun_seconds),
        format_duration_compact(automation.relabel_overlap_seconds),
        format_duration_compact(automation.idle_seconds)
    )?;
    writeln!(
        output,
        "  filter: {}",
        automation.btql_filter.as_deref().unwrap_or("none")
    )?;
    writeln!(
        output,
        "  configured facets: {}",
        summarize_function_names(&automation.facet_functions)
    )?;
    writeln!(
        output,
        "  configured topic maps: {}",
        summarize_function_names(&automation.topic_map_functions)
    )?;
    if let Some(entered_at) = runtime.entered_at.as_deref() {
        writeln!(
            output,
            "  state since: {}",
            format_timestamp_with_relative(entered_at)
        )?;
    }
    if let Some(reason) = runtime.reason.as_deref() {
        writeln!(output, "  reason: {}", format_reason(reason))?;
    }

    writeln!(output)?;
    writeln!(output, "next steps:")?;
    for line in render_transition_requirements(automation, runtime) {
        writeln!(output, "{line}")?;
    }

    writeln!(output)?;
    writeln!(output, "flow: {}", format_flow(runtime.state.as_str()))?;

    Ok(())
}

fn render_transition_requirements(
    automation: &TopicAutomationStatus,
    runtime: &TopicRuntimeSnapshot,
) -> Vec<String> {
    match runtime.state.as_str() {
        "waiting_for_facets" => {
            let mut lines = vec![
                "  - at least one topic map must have enough facet-ready traces for its source facet"
                    .to_string(),
                "  - this state only exists before the first topic map has been generated".to_string(),
            ];
            if let Some(readiness) = format_window_candidate_readiness(&runtime.window_candidates) {
                lines.push(format!("  - current readiness: {readiness}"));
            }
            if let Some(next_run_at) = automation.object_cursor.next_run_at.as_deref() {
                lines.push(format!(
                    "  - next check: {}",
                    format_timestamp_with_relative(next_run_at)
                ));
            }
            lines
        }
        "recomputing_topic_maps" => {
            let mut lines = vec![
                "  - clustering queries are currently running for the selected ready topic maps"
                    .to_string(),
            ];
            if let Some(window_seconds) = runtime.selected_window_seconds {
                lines.push(format!(
                    "  - generation window: {}",
                    format_duration_compact(Some(window_seconds))
                ));
            }
            lines.push(
                "  - if one or more topic maps are generated, label backfill starts next"
                    .to_string(),
            );
            lines.push(
                "  - if none are generated, it goes back to waiting for facets before first initialization, otherwise idle"
                    .to_string(),
            );
            lines
        }
        "pending_topic_classification_backfill" => {
            let mut lines = vec![
                "  - segment cursors have been rewound to the generation window start".to_string(),
            ];
            if let Some(start_xact) = runtime
                .topic_classification_backfill_start_xact_id
                .as_deref()
            {
                lines.push(format!("  - rewind start xact: {start_xact}"));
            }
            lines.push(
                "  - the next check starts label backfill if replay still has rows before the recompute snapshot"
                    .to_string(),
            );
            lines.push(
                "  - if replay has already caught up through the recompute snapshot, it can move directly to idle"
                    .to_string(),
            );
            if let Some(next_run_at) = automation.object_cursor.next_run_at.as_deref() {
                lines.push(format!(
                    "  - next check: {}",
                    format_timestamp_with_relative(next_run_at)
                ));
            }
            lines
        }
        "backfilling_topic_classifications" => {
            let eligible = automation
                .topics
                .iter()
                .map(|item| item.matched_count)
                .sum::<usize>();
            let checked = automation
                .topics
                .iter()
                .map(|item| item.checked_count)
                .sum::<usize>();
            let labeled = automation
                .topics
                .iter()
                .map(|item| item.completed_count)
                .sum::<usize>();
            let mut lines = vec![
                "  - segment replay must catch up through the recompute snapshot".to_string(),
                format!(
                    "  - current replay progress: {}/{} checked ({} labeled)",
                    format_count(checked),
                    format_count(eligible),
                    format_count(labeled)
                ),
                format!(
                    "  - pending segments: {}",
                    automation.cursor.pending_segments
                ),
            ];
            if let Some(idle_wait) = format_trace_idle_wait_summary(automation) {
                lines.push(format!("  - {idle_wait}"));
            }
            if let Some(end_xact) = runtime.generation_window_end_xact_id.as_deref() {
                lines.push(format!(
                    "  - recompute snapshot ends at: {}",
                    format_snapshot_end_xact(end_xact)
                ));
            }
            if let Some(next_run_at) = automation.object_cursor.next_run_at.as_deref() {
                lines.push(format!(
                    "  - next check: {}",
                    format_timestamp_with_relative(next_run_at)
                ));
            }
            lines
        }
        "idle" => {
            let mut lines = vec![
                "  - on the next rerun, readiness is checked again using the candidate windows"
                    .to_string(),
            ];
            if let Some(readiness) = format_window_candidate_readiness(&runtime.window_candidates) {
                lines.push(format!("  - last observed readiness: {readiness}"));
            }
            if let Some(next_run_at) = automation.object_cursor.next_run_at.as_deref() {
                lines.push(format!(
                    "  - next rerun: {}",
                    format_timestamp_with_relative(next_run_at)
                ));
            }
            lines.push(
                "  - if one or more topic maps are ready at rerun time, Topics starts recomputing them; otherwise it stays idle"
                    .to_string(),
            );
            lines
        }
        _ => vec!["  - no transition details available".to_string()],
    }
}

fn format_window_candidate_readiness(
    candidates: &[api::TopicWindowCandidateSnapshot],
) -> Option<String> {
    if candidates.is_empty() {
        return None;
    }

    Some(
        candidates
            .iter()
            .map(|candidate| {
                format!(
                    "{} window: {}/{} topic maps have enough facet-ready traces",
                    format_duration_compact(Some(candidate.window_seconds)),
                    candidate.ready_topic_maps,
                    candidate.total_topic_maps
                )
            })
            .collect::<Vec<_>>()
            .join(" | "),
    )
}

fn format_overall_status(automation: &TopicAutomationStatus) -> String {
    if automation.object_cursor.error_objects > 0 {
        if automation.object_cursor.retry_after.is_some() {
            return "waiting to retry after an error".to_string();
        }
        return "blocked by an error".to_string();
    }

    let Some(runtime) = automation.object_cursor.topic_runtime.as_ref() else {
        if automation.object_cursor.due_objects > 0 {
            return "ready to run".to_string();
        }
        return "idle".to_string();
    };

    match runtime.state.as_str() {
        "waiting_for_facets" => "waiting for facets".to_string(),
        "recomputing_topic_maps" => "recomputing topic maps".to_string(),
        "pending_topic_classification_backfill" => "starting label backfill".to_string(),
        "backfilling_topic_classifications" => format_trace_idle_wait_summary(automation)
            .unwrap_or_else(|| "backfilling labels".to_string()),
        "idle" => {
            if automation.object_cursor.due_objects > 0 {
                "ready to run".to_string()
            } else {
                "idle".to_string()
            }
        }
        other => other.replace('_', " "),
    }
}

fn format_next_event_summary(automation: &TopicAutomationStatus) -> Option<String> {
    if automation.object_cursor.error_objects > 0 {
        if let Some(retry_after) = automation.object_cursor.retry_after.as_deref() {
            return Some(format!(
                "retry: {}",
                format_timestamp_with_relative(retry_after)
            ));
        }
    }

    let runtime = automation.object_cursor.topic_runtime.as_ref();
    let next_run_at = automation.object_cursor.next_run_at.as_deref()?;
    let label =
        if runtime.map(|runtime| runtime.state.as_str()) == Some("idle") || runtime.is_none() {
            "next rerun"
        } else {
            "next check"
        };
    Some(format!(
        "{label}: {}",
        format_timestamp_with_relative(next_run_at)
    ))
}

fn format_coverage_summary(automation: &TopicAutomationStatus, progress_window: &str) -> String {
    if automation.total_traces == 0 {
        return format!("no traces in the {progress_window}");
    }

    format!(
        "{}/{} traces have facet labels in the {}",
        format_count(automation.facet_current_count),
        format_count(automation.total_traces),
        progress_window
    )
}

fn format_label_summary(automation: &TopicAutomationStatus) -> String {
    if automation.total_traces == 0 {
        return "no traces currently fall within the topic window".to_string();
    }

    let eligible = automation
        .topics
        .iter()
        .map(|item| item.matched_count)
        .sum::<usize>();
    let checked = automation
        .topics
        .iter()
        .map(|item| item.checked_count)
        .sum::<usize>();
    let labeled = automation
        .topics
        .iter()
        .map(|item| item.completed_count)
        .sum::<usize>();
    let errors = automation
        .topics
        .iter()
        .map(|item| item.error_count)
        .sum::<usize>();

    if eligible == 0 {
        return "no eligible topic labels yet".to_string();
    }

    let mut parts = vec![format!(
        "{}/{} considered | {} labeled",
        format_count(checked),
        format_count(eligible),
        format_count(labeled),
    )];
    if errors > 0 {
        parts.push(format!("{} errors", format_count(errors)));
    }
    parts.join(" | ")
}

fn format_flow(current_state: &str) -> String {
    [
        ("waiting_for_facets", "waiting for facets"),
        ("recomputing_topic_maps", "recomputing topic maps"),
        ("pending_topic_classification_backfill", "pending backfill"),
        ("backfilling_topic_classifications", "backfilling labels"),
        ("idle", "idle"),
    ]
    .iter()
    .map(|(state, label)| {
        if *state == current_state {
            format!("[{label}]")
        } else {
            (*label).to_string()
        }
    })
    .collect::<Vec<_>>()
    .join(" -> ")
}

fn format_reason(reason: &str) -> String {
    match reason {
        "ready_for_next_rerun" => "waiting for the next scheduled rerun".to_string(),
        "segment_replay_pending" => "segment replay is still behind".to_string(),
        "topic_classification_backfill_complete" => "label backfill completed".to_string(),
        "generated_topic_maps" => "new topic maps were generated".to_string(),
        "generation_produced_no_topic_maps" => {
            "generation did not produce any topic maps".to_string()
        }
        "no_topic_maps_configured" => "no topic maps are configured".to_string(),
        "no_topic_maps_ready_for_generation" => "no topic maps were ready to generate".to_string(),
        "recompute_in_progress" => "topic maps are being recomputed".to_string(),
        "missing_backfill_snapshot" => "the backfill snapshot is missing".to_string(),
        other => other.replace('_', " "),
    }
}

fn format_progress_window_label(automation: &TopicAutomationStatus) -> String {
    automation
        .window_seconds
        .or_else(|| {
            automation
                .object_cursor
                .topic_runtime
                .as_ref()
                .and_then(|runtime| runtime.selected_window_seconds)
        })
        .map(|seconds| format!("{} topic window", format_duration_compact(Some(seconds))))
        .unwrap_or_else(|| "current topic window".to_string())
}

fn format_facet_processing_status(automation: &TopicAutomationStatus) -> String {
    if is_trace_idle_buffering(automation) {
        let wait_remaining_seconds = automation
            .idle_seconds
            .unwrap_or_default()
            .saturating_sub(automation.processing_lag_seconds.unwrap_or_default());
        let segment_label = if automation.cursor.pending_segments == 1 {
            "1 active segment".to_string()
        } else {
            format!("{} active segments", automation.cursor.pending_segments)
        };
        let lag = format_relative_duration_seconds(
            automation.processing_lag_seconds.unwrap_or_default(),
            false,
        );
        let remaining = format_relative_duration_seconds(wait_remaining_seconds, false);
        return format!(
            "buffering recent traces ({lag} behind newest compacted data; waits for {} idle; about {remaining} remaining) · {segment_label}",
            format_duration_compact(automation.idle_seconds)
        );
    }

    let mut parts = Vec::new();
    if let Some(label) = automation.processing_lag_label.as_deref() {
        parts.push(label.to_string());
    }
    if automation.cursor.pending_segments > 0 {
        parts.push(format!(
            "{} pending segment(s)",
            automation.cursor.pending_segments
        ));
    }
    if automation.cursor.error_segments > 0 {
        parts.push(format!(
            "{} error segment(s)",
            automation.cursor.error_segments
        ));
    }
    if parts.is_empty() {
        "up to date".to_string()
    } else {
        parts.join(" · ")
    }
}

fn render_progress_lines<F>(
    items: &[api::TopicAutomationProgressItem],
    column_labels: &[&str],
    row_values: F,
) -> Vec<String>
where
    F: Fn(&api::TopicAutomationProgressItem) -> Vec<String>,
{
    if items.is_empty() {
        return vec!["  none".to_string()];
    }

    let rows = items.iter().map(row_values).collect::<Vec<_>>();

    let name_width = items
        .iter()
        .map(|item| item.name.len())
        .max()
        .unwrap_or(0)
        .max("name".len());
    let column_widths = column_labels
        .iter()
        .enumerate()
        .map(|(index, label)| {
            rows.iter()
                .filter_map(|row| row.get(index))
                .map(|value| value.len())
                .max()
                .unwrap_or(0)
                .max(label.len())
        })
        .collect::<Vec<_>>();

    let format_row = |name: &str, values: &[String]| {
        let formatted_values = values
            .iter()
            .zip(column_widths.iter())
            .map(|(value, width)| format!("{value:>width$}", width = width))
            .collect::<Vec<_>>()
            .join(" | ");
        format!(
            "  {name:<name_width$} | {formatted_values}",
            name_width = name_width
        )
    };

    let header_values = column_labels
        .iter()
        .map(|label| (*label).to_string())
        .collect::<Vec<_>>();
    let mut lines = vec![format_row("name", &header_values)];
    lines.extend(
        items
            .iter()
            .zip(rows.iter())
            .map(|(item, values)| format_row(item.name.as_str(), values)),
    );
    lines
}

fn summarize_function_names(functions: &[api::FunctionSummary]) -> String {
    if functions.is_empty() {
        return "none".to_string();
    }

    functions
        .iter()
        .map(|function| function.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn format_trace_idle_wait_summary(automation: &TopicAutomationStatus) -> Option<String> {
    if !is_trace_idle_buffering(automation) {
        return None;
    }
    let wait_remaining_seconds = automation
        .idle_seconds
        .unwrap_or_default()
        .saturating_sub(automation.processing_lag_seconds.unwrap_or_default());
    Some(format!(
        "waiting for trace idle window ({} remaining)",
        format_relative_duration_seconds(wait_remaining_seconds, false)
    ))
}

fn is_trace_idle_buffering(automation: &TopicAutomationStatus) -> bool {
    automation.scope_type.as_deref() == Some("trace")
        && automation.idle_seconds.is_some()
        && automation.cursor.pending_segments > 0
        && automation.processing_lag_seconds.is_some()
        && automation.processing_lag_seconds.unwrap_or_default()
            < automation.idle_seconds.unwrap_or_default()
}

fn format_snapshot_end_xact(xact_id: &str) -> String {
    let Some(epoch_ms) = api::epoch_ms_from_xact_id(xact_id) else {
        return xact_id.to_string();
    };
    let Some(timestamp) = DateTime::<Utc>::from_timestamp_millis(epoch_ms) else {
        return xact_id.to_string();
    };
    format!(
        "{} (xact_id {})",
        format_datetime_with_relative(timestamp),
        xact_id
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::topics::api::{
        AutomationCursorSnapshot, ObjectAutomationCursorSnapshot, TopicWindowCandidateSnapshot,
        TopicsProjectSummary,
    };

    fn sample_report() -> TopicsStatusReport {
        TopicsStatusReport {
            project: TopicsProjectSummary {
                id: "proj_123".to_string(),
                name: "demo-project".to_string(),
                org_name: "demo-org".to_string(),
                topics_url: "https://app.example.com/app/demo-org/p/demo-project/topics"
                    .to_string(),
            },
            automations: vec![TopicAutomationStatus {
                id: "auto_123".to_string(),
                name: "Topics".to_string(),
                description: String::new(),
                scope_type: Some("trace".to_string()),
                btql_filter: None,
                window_seconds: Some(86400),
                rerun_seconds: Some(86400),
                relabel_overlap_seconds: Some(3600),
                idle_seconds: Some(600),
                configured_facets: 3,
                configured_topic_maps: 2,
                processing_lag_label: Some("4m behind".to_string()),
                processing_lag_seconds: Some(240),
                total_traces: 1010,
                facet_current_count: 1004,
                facets: vec![
                    api::TopicAutomationProgressItem {
                        name: "Task".to_string(),
                        matched_count: 996,
                        completed_count: 1004,
                        checked_count: 1004,
                        processing_count: 6,
                        error_count: 0,
                    },
                    api::TopicAutomationProgressItem {
                        name: "Sentiment".to_string(),
                        matched_count: 80,
                        completed_count: 1004,
                        checked_count: 1004,
                        processing_count: 6,
                        error_count: 0,
                    },
                ],
                topics: vec![
                    api::TopicAutomationProgressItem {
                        name: "Task".to_string(),
                        matched_count: 996,
                        completed_count: 698,
                        checked_count: 900,
                        processing_count: 6,
                        error_count: 0,
                    },
                    api::TopicAutomationProgressItem {
                        name: "Sentiment".to_string(),
                        matched_count: 80,
                        completed_count: 0,
                        checked_count: 20,
                        processing_count: 6,
                        error_count: 0,
                    },
                ],
                facet_functions: vec![
                    api::FunctionSummary {
                        name: "Task".to_string(),
                        ref_type: "global".to_string(),
                        function_type: Some("facet".to_string()),
                        id: None,
                        description: None,
                        version: None,
                        btql_filter: None,
                        source_facet: None,
                        embedding_model: None,
                        distance_threshold: None,
                        generation_settings: None,
                    },
                    api::FunctionSummary {
                        name: "Sentiment".to_string(),
                        ref_type: "global".to_string(),
                        function_type: Some("facet".to_string()),
                        id: None,
                        description: None,
                        version: None,
                        btql_filter: None,
                        source_facet: None,
                        embedding_model: None,
                        distance_threshold: None,
                        generation_settings: None,
                    },
                ],
                topic_map_functions: vec![api::FunctionSummary {
                    name: "Task".to_string(),
                    ref_type: "function".to_string(),
                    function_type: Some("classifier".to_string()),
                    id: Some("func_1".to_string()),
                    description: None,
                    version: None,
                    btql_filter: None,
                    source_facet: None,
                    embedding_model: None,
                    distance_threshold: None,
                    generation_settings: None,
                }],
                cursor: AutomationCursorSnapshot {
                    total_segments: 12,
                    pending_segments: 3,
                    error_segments: 1,
                    pending_min_compacted_xact_id: Some("9990001112223334".to_string()),
                    pending_max_compacted_xact_id: Some("9990001112223399".to_string()),
                    pending_min_executed_xact_id: Some("9990001112223300".to_string()),
                },
                object_cursor: ObjectAutomationCursorSnapshot {
                    total_objects: 5,
                    due_objects: 0,
                    error_objects: 0,
                    last_compacted_xact_id: Some("9990001112223334".to_string()),
                    next_run_at: Some("2026-04-15T12:00:00Z".to_string()),
                    last_run_at: Some("2026-04-14T11:00:00Z".to_string()),
                    retry_after: None,
                    last_error: None,
                    last_error_at: None,
                    topic_runtime: Some(TopicRuntimeSnapshot {
                        state: "idle".to_string(),
                        reason: Some("ready_for_next_rerun".to_string()),
                        entered_at: Some("2026-04-14T11:00:00Z".to_string()),
                        selected_window_seconds: Some(3600),
                        generation_window_start_xact_id: Some("9990001112220000".to_string()),
                        generation_window_end_xact_id: Some("9990001112223334".to_string()),
                        topic_classification_backfill_start_xact_id: Some(
                            "9990001112220000".to_string(),
                        ),
                        active_topic_map_versions: BTreeMap::from([(
                            "func_1".to_string(),
                            "v3".to_string(),
                        )]),
                        window_candidates: vec![TopicWindowCandidateSnapshot {
                            window_seconds: 3600,
                            ready_topic_maps: 1,
                            total_topic_maps: 2,
                        }],
                    }),
                },
            }],
        }
    }

    #[test]
    fn compact_report_includes_summary_details() {
        let output = render_report(&sample_report(), false);
        assert!(output.contains("Project:"));
        assert!(!output.contains("Topic automations: 1"));
        assert!(output.contains("demo-project (proj_123)"));
        assert!(output.contains("status: idle"));
        assert!(output
            .contains("coverage: 1,004/1,010 traces have facet labels in the 1d topic window"));
        assert!(output.contains("labels: 920/1,076 considered | 698 labeled"));
        assert!(output.contains("facets: "));
        assert!(output.contains("facet progress:"));
        assert!(output.contains("topic progress:"));
        assert!(output.contains("considered | matched | running | errors"));
        assert!(output.contains("considered | labeled | running | errors"));
        assert!(!output.contains("processing:"));
        assert!(!output.contains("state machine:"));
    }

    #[test]
    fn full_report_includes_diagnostics_and_flow() {
        let output = render_report(&sample_report(), true);
        assert!(output.contains("details:"));
        assert!(output.contains("  scope: trace"));
        assert!(output.contains("  state since: "));
        assert!(output.contains("  reason: waiting for the next scheduled rerun"));
        assert!(output.contains("next steps:"));
        assert!(output.contains(
            "flow: waiting for facets -> recomputing topic maps -> pending backfill -> backfilling labels -> [idle]"
        ));
        assert!(output.contains("configured topic maps: Task"));
        assert!(!output.contains("state machine:"));
    }

    #[test]
    fn backfilling_transition_includes_label_progress_and_idle_wait() {
        let mut report = sample_report();
        let automation = report.automations.first_mut().expect("automation");
        let runtime = automation
            .object_cursor
            .topic_runtime
            .as_mut()
            .expect("runtime");
        runtime.state = "backfilling_topic_classifications".to_string();
        let runtime = runtime.clone();

        let lines = render_transition_requirements(automation, &runtime);
        assert!(lines
            .contains(&"  - current replay progress: 920/1,076 checked (698 labeled)".to_string()));
        assert!(lines
            .iter()
            .any(|line| line.contains("waiting for trace idle window")));
        assert!(lines.iter().any(|line| {
            line.contains("recompute snapshot ends at: ")
                && line.contains("(xact_id 9990001112223334)")
                && line.contains("T")
        }));
    }

    #[test]
    fn window_candidate_readiness_uses_facet_ready_wording() {
        let readiness = format_window_candidate_readiness(&[TopicWindowCandidateSnapshot {
            window_seconds: 3600,
            ready_topic_maps: 2,
            total_topic_maps: 3,
        }]);

        assert_eq!(
            readiness.as_deref(),
            Some("1h window: 2/3 topic maps have enough facet-ready traces")
        );
    }

    #[test]
    fn progress_window_label_uses_configured_window() {
        let report = sample_report();
        let label = format_progress_window_label(&report.automations[0]);
        assert_eq!(label, "1d topic window");
    }

    #[test]
    fn multi_automation_report_uses_divider() {
        let mut report = sample_report();
        let mut second = report.automations[0].clone();
        second.id = "auto_456".to_string();
        second.name = "Topics copy".to_string();
        report.automations.push(second);

        let output = render_report(&report, false);
        assert!(output.contains("Topic automations: 2"));
        assert!(output.contains("\n---\n\nTopics copy (auto_456)"));
    }
}
