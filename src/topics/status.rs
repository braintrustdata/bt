use std::fmt::Write as _;
use std::io::{self, IsTerminal, Write};
use std::time::Duration;

use anyhow::{bail, Result};
use chrono::{DateTime, SecondsFormat, Utc};
use dialoguer::console;

use crate::ui::{print_with_pager, with_spinner};

use super::{
    api::{self, TopicAutomationStatus, TopicRuntimeSnapshot, TopicsStatusReport},
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

    loop {
        let report = api::fetch_topics_status(ctx).await?;
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
            _ = tokio::signal::ctrl_c() => {
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
        "Project: {} {} {}",
        console::style(report.project.org_name.as_str()).bold(),
        console::style("/").dim().bold(),
        console::style(report.project.name.as_str()).bold()
    )
    .expect("write to string");

    if report.automations.is_empty() {
        writeln!(output, "No topic automations found.").expect("write to string");
        return output;
    }

    writeln!(output, "Topic automations: {}", report.automations.len()).expect("write to string");

    for automation in &report.automations {
        write_automation_summary(&mut output, automation, full).expect("write to string");
    }

    if full {
        writeln!(output, "\nTopic automation state machines:").expect("write to string");
        for automation in &report.automations {
            write_automation_diagnostics(&mut output, automation).expect("write to string");
        }
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
    writeln!(
        output,
        "  scope: {} | filter: {}",
        automation.scope_type.as_deref().unwrap_or("n/a"),
        automation.btql_filter.as_deref().unwrap_or("none")
    )?;
    writeln!(
        output,
        "  schedule: window {} | cadence {} | overlap {} | idle {}",
        format_duration_compact(automation.window_seconds),
        format_duration_compact(automation.rerun_seconds),
        format_duration_compact(automation.relabel_overlap_seconds),
        format_duration_compact(automation.idle_seconds)
    )?;
    writeln!(output, "  processing:")?;
    for line in render_processing_status_lines(automation) {
        writeln!(output, "{line}")?;
    }
    writeln!(
        output,
        "  coverage ({progress_window}): {} traces | {} with facet labels",
        automation.total_traces, automation.facet_current_count
    )?;
    if automation.total_traces == 0 {
        writeln!(
            output,
            "    no traces currently fall within this topic window; older labeled traces are not shown here"
        )?;
    }
    if !automation.facets.is_empty() && automation.total_traces > 0 {
        writeln!(
            output,
            "  facet progress in that window (matched / done / running / errors):"
        )?;
        for line in render_progress_lines(&automation.facets, "matched", "done") {
            writeln!(output, "{line}")?;
        }
    }
    if !automation.topics.is_empty() && automation.total_traces > 0 {
        writeln!(
            output,
            "  topic progress in that window (eligible / labeled / running / errors):"
        )?;
        for line in render_progress_lines(&automation.topics, "eligible", "labeled") {
            writeln!(output, "{line}")?;
        }
    }
    if full {
        writeln!(
            output,
            "  facets: {}",
            summarize_function_names(&automation.facet_functions)
        )?;
        writeln!(
            output,
            "  topic maps: {}",
            summarize_function_names(&automation.topic_map_functions)
        )?;
    }

    Ok(())
}

fn write_automation_diagnostics(
    output: &mut String,
    automation: &TopicAutomationStatus,
) -> std::fmt::Result {
    writeln!(output, "\n{} ({})", automation.name, automation.id)?;

    let Some(runtime) = automation.object_cursor.topic_runtime.as_ref() else {
        writeln!(output, "  state machine: unavailable")?;
        return Ok(());
    };

    writeln!(output, "  current state: {}", runtime.state)?;
    if let Some(entered_at) = runtime.entered_at.as_deref() {
        writeln!(
            output,
            "  entered at: {}",
            format_timestamp_with_relative(entered_at)
        )?;
    }
    if let Some(reason) = runtime.reason.as_deref() {
        writeln!(output, "  reason: {reason}")?;
    }

    writeln!(output, "  to transition:")?;
    for line in render_transition_requirements(automation, runtime) {
        writeln!(output, "{line}")?;
    }

    writeln!(output, "\n  state machine:")?;
    for line in render_state_machine(runtime.state.as_str()) {
        writeln!(output, "{line}")?;
    }

    Ok(())
}

fn render_transition_requirements(
    automation: &TopicAutomationStatus,
    runtime: &TopicRuntimeSnapshot,
) -> Vec<String> {
    match runtime.state.as_str() {
        "waiting_for_facets" => {
            let mut lines = vec![
                "    - at least one topic map must have enough facet-ready traces for its source facet"
                    .to_string(),
                "    - this state only exists before the first topic map has been generated".to_string(),
            ];
            if let Some(readiness) = format_window_candidate_readiness(&runtime.window_candidates) {
                lines.push(format!("    - current readiness: {readiness}"));
            }
            if let Some(next_run_at) = automation.object_cursor.next_run_at.as_deref() {
                lines.push(format!(
                    "    - next check: {}",
                    format_timestamp_with_relative(next_run_at)
                ));
            }
            lines
        }
        "recomputing_topic_maps" => {
            let mut lines = vec![
                "    - clustering queries are currently running for the selected ready topic maps"
                    .to_string(),
            ];
            if let Some(window_seconds) = runtime.selected_window_seconds {
                lines.push(format!(
                    "    - generation window: {}",
                    format_duration_compact(Some(window_seconds))
                ));
            }
            lines.push(
                "    - if one or more topic maps are generated, it transitions to pending_topic_classification_backfill"
                    .to_string(),
            );
            lines.push(
                "    - if none are generated, it falls back to waiting_for_facets before first initialization, otherwise idle"
                    .to_string(),
            );
            lines
        }
        "pending_topic_classification_backfill" => {
            let mut lines = vec![
                "    - segment cursors have been rewound to the generation window start"
                    .to_string(),
            ];
            if let Some(start_xact) = runtime
                .topic_classification_backfill_start_xact_id
                .as_deref()
            {
                lines.push(format!("    - rewind start xact: {start_xact}"));
            }
            lines.push(
                "    - the next object check moves to backfilling_topic_classifications if replay still has rows before the recompute snapshot"
                    .to_string(),
            );
            lines.push(
                "    - if replay has already caught up through the recompute snapshot, it can move directly to idle"
                    .to_string(),
            );
            if let Some(next_run_at) = automation.object_cursor.next_run_at.as_deref() {
                lines.push(format!(
                    "    - next check: {}",
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
            let labeled = automation
                .topics
                .iter()
                .map(|item| item.completed_count)
                .sum::<usize>();
            let mut lines = vec![
                "    - segment replay must catch up through the recompute snapshot".to_string(),
                format!("    - current label progress: {labeled}/{eligible} labeled"),
                format!(
                    "    - pending segments: {}",
                    automation.cursor.pending_segments
                ),
            ];
            if let Some(idle_wait) = format_trace_idle_wait_summary(automation) {
                lines.push(format!("    - {idle_wait}"));
            }
            if let Some(end_xact) = runtime.generation_window_end_xact_id.as_deref() {
                lines.push(format!("    - recompute snapshot end xact: {end_xact}"));
            }
            if let Some(next_run_at) = automation.object_cursor.next_run_at.as_deref() {
                lines.push(format!(
                    "    - next check: {}",
                    format_timestamp_with_relative(next_run_at)
                ));
            }
            lines
        }
        "idle" => {
            let mut lines = vec![
                "    - on the next rerun, readiness is checked again using the candidate windows"
                    .to_string(),
            ];
            if let Some(readiness) = format_window_candidate_readiness(&runtime.window_candidates) {
                lines.push(format!("    - last observed readiness: {readiness}"));
            }
            if let Some(next_run_at) = automation.object_cursor.next_run_at.as_deref() {
                lines.push(format!(
                    "    - next rerun: {}",
                    format_timestamp_with_relative(next_run_at)
                ));
            }
            lines.push(
                "    - if one or more topic maps are ready at rerun time, it transitions to recomputing_topic_maps; otherwise it stays idle"
                    .to_string(),
            );
            lines
        }
        _ => vec!["    - no transition details available".to_string()],
    }
}

fn render_state_machine(current_state: &str) -> Vec<String> {
    vec![
        format!(
            "    {}",
            format_state_label("waiting_for_facets", current_state)
        ),
        "      -> recomputing_topic_maps".to_string(),
        format!(
            "    {}",
            format_state_label("recomputing_topic_maps", current_state)
        ),
        "      -> pending_topic_classification_backfill".to_string(),
        format!(
            "    {}",
            format_state_label("pending_topic_classification_backfill", current_state)
        ),
        "      -> backfilling_topic_classifications  if pending segments".to_string(),
        "      -> idle                              otherwise".to_string(),
        format!(
            "    {}",
            format_state_label("backfilling_topic_classifications", current_state)
        ),
        "      -> idle  when replay is newer than recompute snapshot".to_string(),
        format!("    {}", format_state_label("idle", current_state)),
        "      -> recomputing_topic_maps  if rerun finds ready topic maps".to_string(),
        "      -> idle                    otherwise".to_string(),
    ]
}

fn format_state_label(state: &str, current_state: &str) -> String {
    if state == current_state {
        format!("* {state}")
    } else {
        state.to_string()
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

fn render_processing_status_lines(automation: &TopicAutomationStatus) -> Vec<String> {
    let runtime = automation.object_cursor.topic_runtime.as_ref();
    let mut lines = vec![format!(
        "    facets: {}",
        format_facet_processing_status(automation)
    )];

    let mut topic_maps_status = format_recompute_status(automation);
    if let Some(runtime) = runtime {
        topic_maps_status = format!("{} ({topic_maps_status})", runtime.state);
    }
    lines.push(format!("    topic maps: {topic_maps_status}"));

    if let Some(runtime) = runtime {
        if runtime.state == "waiting_for_facets" {
            if let Some(readiness) = format_topic_generation_readiness(runtime) {
                lines.push(format!("      readiness: {readiness}"));
            }
        } else if let Some(window_seconds) = runtime.selected_window_seconds {
            lines.push(format!(
                "      window: {}",
                format_duration_compact(Some(window_seconds))
            ));
        }
    }

    if automation.object_cursor.error_objects > 0 {
        if let Some(retry_after) = automation.object_cursor.retry_after.as_deref() {
            lines.push(format!(
                "      retry: {}",
                format_timestamp_with_relative(retry_after)
            ));
        }
    } else if runtime.map(|runtime| runtime.state.as_str()) == Some("recomputing_topic_maps") {
        if let Some(started_at) = runtime.and_then(|runtime| runtime.entered_at.as_deref()) {
            lines.push(format!(
                "      started: {}",
                format_timestamp_with_relative(started_at)
            ));
        }
    } else if let Some(next_run_at) = automation.object_cursor.next_run_at.as_deref() {
        let next_label =
            if runtime.map(|runtime| runtime.state.as_str()) == Some("idle") || runtime.is_none() {
                "next"
            } else {
                "check"
            };
        lines.push(format!(
            "      {next_label}: {}",
            format_timestamp_with_relative(next_run_at)
        ));
    }

    if let Some(last_run_at) = automation.object_cursor.last_run_at.as_deref() {
        lines.push(format!(
            "      last: {}",
            format_timestamp_with_relative(last_run_at)
        ));
    }
    if let Some(last_error) = automation.object_cursor.last_error.as_deref() {
        let mut line = format!("      error: {last_error}");
        if let Some(last_error_at) = automation.object_cursor.last_error_at.as_deref() {
            line.push_str(&format!(
                " ({})",
                format_timestamp_with_relative(last_error_at)
            ));
        }
        lines.push(line);
    }

    let mut topic_labels_status = format_topic_label_processing_status(automation);
    if let Some(runtime) = runtime {
        topic_labels_status = format!("{} ({topic_labels_status})", runtime.state);
    }
    lines.push(format!("    labels: {topic_labels_status}"));

    lines
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

fn format_topic_label_processing_status(automation: &TopicAutomationStatus) -> String {
    if let Some(runtime) = automation.object_cursor.topic_runtime.as_ref() {
        match runtime.state.as_str() {
            "waiting_for_facets" => {
                if automation
                    .topic_map_functions
                    .iter()
                    .any(|function| function.version.is_some())
                {
                    return "waiting for more topic maps to initialize".to_string();
                }
                return "waiting for initial topic maps".to_string();
            }
            "recomputing_topic_maps" => {
                return "waiting for topic map recompute to finish".to_string();
            }
            "pending_topic_classification_backfill" => {
                return "waiting to start topic classification backfill".to_string();
            }
            "backfilling_topic_classifications" => {
                let eligible = automation
                    .topics
                    .iter()
                    .map(|item| item.matched_count)
                    .sum::<usize>();
                let labeled = automation
                    .topics
                    .iter()
                    .map(|item| item.completed_count)
                    .sum::<usize>();
                if let Some(idle_wait) = format_trace_idle_wait_summary(automation) {
                    return format!("{idle_wait}; {labeled}/{eligible} labeled");
                }
                return format!("backfilling topic classifications ({labeled}/{eligible} labeled)");
            }
            _ => {}
        }
    }

    if automation.total_traces == 0 {
        return "no traces currently fall within the topic window".to_string();
    }

    let eligible = automation
        .topics
        .iter()
        .map(|item| item.matched_count)
        .sum::<usize>();
    let labeled = automation
        .topics
        .iter()
        .map(|item| item.completed_count)
        .sum::<usize>();
    let running = automation
        .topics
        .iter()
        .map(|item| item.processing_count)
        .sum::<usize>();
    let errors = automation
        .topics
        .iter()
        .map(|item| item.error_count)
        .sum::<usize>();

    if eligible == 0 {
        if is_trace_idle_buffering(automation) {
            return "waiting for idle time before replaying labels".to_string();
        }
        if automation.cursor.pending_segments > 0 {
            return "waiting for facet replay before labels become eligible".to_string();
        }
        return "no eligible rows in the topic window".to_string();
    }

    let progress = format!("{labeled}/{eligible} labeled");
    let status_text = if running > 0 {
        "replaying labels over the topic window"
    } else if is_trace_idle_buffering(automation) && labeled < eligible {
        "waiting for idle time before replaying labels"
    } else if automation.cursor.pending_segments > 0 && labeled < eligible {
        "replaying labels over the topic window"
    } else if labeled < eligible {
        "partially labeled in the topic window"
    } else {
        "up to date in the topic window"
    };

    if errors > 0 {
        format!("{status_text} ({progress}; {errors} errors)")
    } else {
        format!("{status_text} ({progress})")
    }
}

fn format_recompute_status(automation: &TopicAutomationStatus) -> String {
    if automation.object_cursor.error_objects > 0 {
        if automation.object_cursor.retry_after.is_some() {
            if automation.object_cursor.error_objects == 1 {
                return "backing off after last failure".to_string();
            }
            return format!(
                "{} object cursor(s) backing off after failure",
                automation.object_cursor.error_objects
            );
        }
        return format!(
            "{} object cursor(s) in error",
            automation.object_cursor.error_objects
        );
    }

    if let Some(runtime) = automation.object_cursor.topic_runtime.as_ref() {
        match runtime.state.as_str() {
            "waiting_for_facets" => return "waiting for more data".to_string(),
            "recomputing_topic_maps" => return "recomputing topic maps".to_string(),
            "pending_topic_classification_backfill" => {
                return "topic maps are ready; starting classification backfill".to_string();
            }
            "backfilling_topic_classifications" => {
                if let Some(idle_wait) = format_trace_idle_wait_summary(automation) {
                    return idle_wait;
                }
                return "backfilling topic classifications".to_string();
            }
            "idle" => {
                if automation.object_cursor.due_objects > 0 {
                    if automation.object_cursor.due_objects == 1 {
                        return "ready to run now".to_string();
                    }
                    return format!(
                        "{} object cursor(s) ready to run now",
                        automation.object_cursor.due_objects
                    );
                }
                if automation.object_cursor.next_run_at.is_some() {
                    return "scheduled".to_string();
                }
                if automation.object_cursor.last_run_at.is_some() {
                    return "idle".to_string();
                }
            }
            _ => {}
        }
    }

    if automation.object_cursor.due_objects > 0 {
        if automation.object_cursor.due_objects == 1 {
            return "ready to run now".to_string();
        }
        return format!(
            "{} object cursor(s) ready to run now",
            automation.object_cursor.due_objects
        );
    }
    if automation.object_cursor.next_run_at.is_some() {
        return "scheduled".to_string();
    }
    if automation.object_cursor.last_run_at.is_some() {
        return "idle".to_string();
    }
    "not scheduled".to_string()
}

fn format_topic_generation_readiness(runtime: &TopicRuntimeSnapshot) -> Option<String> {
    let best_candidate = runtime.window_candidates.iter().max_by(|left, right| {
        left.ready_topic_maps
            .cmp(&right.ready_topic_maps)
            .then(left.window_seconds.cmp(&right.window_seconds))
    })?;
    if best_candidate.total_topic_maps == 0 {
        return None;
    }
    Some(format!(
        "{}/{} topic maps ready in {}",
        best_candidate.ready_topic_maps,
        best_candidate.total_topic_maps,
        format_duration_compact(Some(best_candidate.window_seconds))
    ))
}

fn render_progress_lines(
    items: &[api::TopicAutomationProgressItem],
    first_label: &str,
    second_label: &str,
) -> Vec<String> {
    if items.is_empty() {
        return vec!["    none".to_string()];
    }

    let name_width = items
        .iter()
        .map(|item| item.name.len())
        .max()
        .unwrap_or(0)
        .max("name".len());
    let first_width = items
        .iter()
        .map(|item| item.matched_count.to_string().len())
        .max()
        .unwrap_or(0)
        .max(first_label.len());
    let second_width = items
        .iter()
        .map(|item| item.completed_count.to_string().len())
        .max()
        .unwrap_or(0)
        .max(second_label.len());
    let running_width = items
        .iter()
        .map(|item| item.processing_count.to_string().len())
        .max()
        .unwrap_or(0)
        .max("running".len());
    let error_width = items
        .iter()
        .map(|item| item.error_count.to_string().len())
        .max()
        .unwrap_or(0)
        .max("errors".len());

    let format_row = |name: &str,
                      first: String,
                      second: String,
                      running: String,
                      errors: String| {
        format!(
            "    {name:<name_width$} | {first:>first_width$} | {second:>second_width$} | {running:>running_width$} | {errors:>error_width$}",
            name_width = name_width,
            first_width = first_width,
            second_width = second_width,
            running_width = running_width,
            error_width = error_width,
        )
    };

    let mut lines = vec![format_row(
        "name",
        first_label.to_string(),
        second_label.to_string(),
        "running".to_string(),
        "errors".to_string(),
    )];
    lines.extend(items.iter().map(|item| {
        format_row(
            item.name.as_str(),
            item.matched_count.to_string(),
            item.completed_count.to_string(),
            item.processing_count.to_string(),
            item.error_count.to_string(),
        )
    }));
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

fn format_duration_compact(seconds: Option<i64>) -> String {
    let Some(seconds) = seconds else {
        return "n/a".to_string();
    };

    let units = [
        ("w", 7 * 24 * 60 * 60),
        ("d", 24 * 60 * 60),
        ("h", 60 * 60),
        ("m", 60),
        ("s", 1),
    ];
    for (suffix, scale) in units {
        if seconds >= scale && seconds % scale == 0 {
            return format!("{}{}", seconds / scale, suffix);
        }
    }
    format!("{seconds}s")
}

fn format_timestamp_with_relative(value: &str) -> String {
    let Ok(parsed) = DateTime::parse_from_rfc3339(value) else {
        return value.to_string();
    };
    let utc = parsed.with_timezone(&Utc);
    format!(
        "{} ({})",
        utc.to_rfc3339_opts(SecondsFormat::Secs, true),
        relative_time(utc)
    )
}

fn relative_time(timestamp: DateTime<Utc>) -> String {
    let delta = timestamp.signed_duration_since(Utc::now());
    let seconds = delta.num_seconds();

    if seconds.abs() < 5 {
        return "now".to_string();
    }

    let text = human_interval(seconds.abs());
    if seconds >= 0 {
        format!("in {text}")
    } else {
        format!("{text} ago")
    }
}

fn human_interval(seconds: i64) -> String {
    let units = [("d", 24 * 60 * 60), ("h", 60 * 60), ("m", 60), ("s", 1)];
    let mut remaining = seconds;
    let mut parts = Vec::new();

    for (suffix, scale) in units {
        if remaining < scale {
            continue;
        }
        let value = remaining / scale;
        remaining %= scale;
        parts.push(format!("{value}{suffix}"));
        if parts.len() == 2 {
            break;
        }
    }

    if parts.is_empty() {
        "0s".to_string()
    } else {
        parts.join(" ")
    }
}

fn format_relative_duration_seconds(delta_seconds: i64, include_direction: bool) -> String {
    let absolute_seconds = delta_seconds.abs();
    if absolute_seconds < 5 {
        return if include_direction {
            "now".to_string()
        } else {
            "0s".to_string()
        };
    }

    let units = [
        ("w", 7 * 24 * 60 * 60),
        ("d", 24 * 60 * 60),
        ("h", 60 * 60),
        ("m", 60),
        ("s", 1),
    ];
    for (suffix, scale) in units {
        if absolute_seconds < scale {
            continue;
        }
        let rounded = absolute_seconds / scale;
        if !include_direction {
            return format!("{rounded}{suffix}");
        }
        if delta_seconds > 0 {
            return format!("in {rounded}{suffix}");
        }
        return format!("{rounded}{suffix} ago");
    }
    "0s".to_string()
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
                        processing_count: 6,
                        error_count: 0,
                    },
                    api::TopicAutomationProgressItem {
                        name: "Sentiment".to_string(),
                        matched_count: 80,
                        completed_count: 1004,
                        processing_count: 6,
                        error_count: 0,
                    },
                ],
                topics: vec![
                    api::TopicAutomationProgressItem {
                        name: "Task".to_string(),
                        matched_count: 996,
                        completed_count: 698,
                        processing_count: 6,
                        error_count: 0,
                    },
                    api::TopicAutomationProgressItem {
                        name: "Sentiment".to_string(),
                        matched_count: 80,
                        completed_count: 0,
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
                        version: None,
                        btql_filter: None,
                    },
                    api::FunctionSummary {
                        name: "Sentiment".to_string(),
                        ref_type: "global".to_string(),
                        function_type: Some("facet".to_string()),
                        id: None,
                        version: None,
                        btql_filter: None,
                    },
                ],
                topic_map_functions: vec![api::FunctionSummary {
                    name: "Task".to_string(),
                    ref_type: "function".to_string(),
                    function_type: Some("classifier".to_string()),
                    id: Some("func_1".to_string()),
                    version: None,
                    btql_filter: None,
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
                    due_objects: 2,
                    error_objects: 1,
                    last_compacted_xact_id: Some("9990001112223334".to_string()),
                    next_run_at: Some("2026-03-09T12:00:00Z".to_string()),
                    last_run_at: Some("2026-03-09T11:00:00Z".to_string()),
                    retry_after: Some("2026-03-09T11:15:00Z".to_string()),
                    last_error: Some("Example failure".to_string()),
                    last_error_at: Some("2026-03-09T11:05:00Z".to_string()),
                    topic_runtime: Some(TopicRuntimeSnapshot {
                        state: "idle".to_string(),
                        reason: Some("ready_for_next_rerun".to_string()),
                        entered_at: Some("2026-03-09T11:00:00Z".to_string()),
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
        assert!(output.contains("Topic automations: 1"));
        assert!(output.contains("scope: trace | filter: none"));
        assert!(output.contains("processing:"));
        assert!(output.contains("coverage (1d topic window): 1010 traces | 1004 with facet labels"));
        assert!(
            output.contains("facet progress in that window (matched / done / running / errors):")
        );
        assert!(output
            .contains("topic progress in that window (eligible / labeled / running / errors):"));
        assert!(!output.contains("topic maps: Task"));
    }

    #[test]
    fn full_report_includes_state_machine() {
        let output = render_report(&sample_report(), true);
        assert!(output.contains("Topic automation state machines:"));
        assert!(output.contains("current state: idle"));
        assert!(output.contains("state machine:"));
        assert!(output.contains("    * idle"));
        assert!(output.contains("-> recomputing_topic_maps  if rerun finds ready topic maps"));
        assert!(output.contains("topic maps: Task"));
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
        assert!(lines.contains(&"    - current label progress: 698/1076 labeled".to_string()));
        assert!(lines
            .iter()
            .any(|line| line.contains("waiting for trace idle window")));
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
}
