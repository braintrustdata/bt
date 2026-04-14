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
        write_automation_summary(&mut output, automation).expect("write to string");
    }

    if full {
        writeln!(output, "\nTopic automation diagnostics:").expect("write to string");
        for automation in &report.automations {
            write_automation_diagnostics(&mut output, automation).expect("write to string");
        }
    }

    output
}

fn write_automation_summary(
    output: &mut String,
    automation: &TopicAutomationStatus,
) -> std::fmt::Result {
    writeln!(output, "\n{} ({})", automation.name, automation.id)?;
    writeln!(
        output,
        "  execution scope: {}",
        automation.scope_type.as_deref().unwrap_or("n/a")
    )?;
    writeln!(
        output,
        "  filter: {}",
        automation.btql_filter.as_deref().unwrap_or("none")
    )?;
    writeln!(output, "  schedule:")?;
    writeln!(
        output,
        "    - topic window: {}",
        format_duration_compact(automation.window_seconds)
    )?;
    writeln!(
        output,
        "    - generation cadence: {}",
        format_duration_compact(automation.rerun_seconds)
    )?;
    writeln!(
        output,
        "    - relabel overlap: {}",
        format_duration_compact(automation.relabel_overlap_seconds)
    )?;
    writeln!(
        output,
        "    - idle time: {}",
        format_duration_compact(automation.idle_seconds)
    )?;
    writeln!(output, "  status:")?;
    writeln!(
        output,
        "    - runtime: {}",
        automation
            .object_cursor
            .topic_runtime
            .as_ref()
            .map(|runtime| runtime.state.as_str())
            .unwrap_or("unavailable")
    )?;
    writeln!(
        output,
        "    - pending segments: {}",
        automation.cursor.pending_segments
    )?;
    writeln!(
        output,
        "    - error segments: {}",
        automation.cursor.error_segments
    )?;
    writeln!(
        output,
        "    - due objects: {}",
        automation.object_cursor.due_objects
    )?;
    writeln!(
        output,
        "    - error objects: {}",
        automation.object_cursor.error_objects
    )?;
    if let Some(next_run_at) = automation.object_cursor.next_run_at.as_deref() {
        writeln!(
            output,
            "    - next run: {}",
            format_timestamp_with_relative(next_run_at)
        )?;
    }
    if let Some(last_error) = automation.object_cursor.last_error.as_deref() {
        writeln!(output, "    - last error: {last_error}")?;
    }
    writeln!(output, "  configured:")?;
    writeln!(output, "    - facets: {}", automation.configured_facets)?;
    writeln!(
        output,
        "    - topic maps: {}",
        automation.configured_topic_maps
    )?;

    if let Some(runtime) = automation.object_cursor.topic_runtime.as_ref() {
        if !runtime.active_topic_map_versions.is_empty() {
            writeln!(output, "  active topic map versions:")?;
            for (topic_map_id, version) in &runtime.active_topic_map_versions {
                writeln!(output, "    - {topic_map_id}: {version}")?;
            }
        }
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
                "    - the next object check moves to backfilling_topic_classifications if segment replay is still pending"
                    .to_string(),
            );
            lines.push(
                "    - if replay already caught up, it can move directly to idle".to_string(),
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
            let mut lines = vec![
                "    - segment replay must catch up for the current generation window".to_string(),
                format!(
                    "    - pending segments: {}",
                    automation.cursor.pending_segments
                ),
            ];
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
    let states = [
        "waiting_for_facets",
        "recomputing_topic_maps",
        "pending_topic_classification_backfill",
        "backfilling_topic_classifications",
        "idle",
    ];
    let width = states.iter().map(|state| state.len()).max().unwrap_or(0) + 2;
    let connector = format!("    {}", " ".repeat((width + 4) / 2));

    let mut lines = Vec::new();
    lines.extend(render_state_machine_box(
        "waiting_for_facets",
        current_state == "waiting_for_facets",
        width,
    ));
    lines.push(format!("{connector}|"));
    lines.push(format!("{connector}| first topic map is ready"));
    lines.push(format!("{connector}v"));
    lines.extend(render_state_machine_box(
        "recomputing_topic_maps",
        current_state == "recomputing_topic_maps",
        width,
    ));
    lines.push(format!("{connector}|"));
    lines.push(format!("{connector}| generated topic maps"));
    lines.push(format!("{connector}v"));
    lines.extend(render_state_machine_box(
        "pending_topic_classification_backfill",
        current_state == "pending_topic_classification_backfill",
        width,
    ));
    lines.push(format!("{connector}|"));
    lines.push(format!("{connector}| next object check"));
    lines.push(format!("{connector}v"));
    lines.push(format!("{connector}pending segments?"));
    lines.push(format!(
        "{connector}+- yes -> backfilling_topic_classifications"
    ));
    lines.push(format!("{connector}'- no  -> idle"));
    lines.extend(render_state_machine_box(
        "backfilling_topic_classifications",
        current_state == "backfilling_topic_classifications",
        width,
    ));
    lines.push(format!("{connector}|"));
    lines.push(format!("{connector}| pending_segments == 0"));
    lines.push(format!("{connector}v"));
    lines.extend(render_state_machine_box(
        "idle",
        current_state == "idle",
        width,
    ));
    lines.push(format!("{connector}|"));
    lines.push(format!("{connector}| rerun due"));
    lines.push(format!("{connector}v"));
    lines.push(format!("{connector}ready topic maps?"));
    lines.push(format!("{connector}+- yes -> recomputing_topic_maps"));
    lines.push(format!("{connector}'- no  -> idle"));
    lines
}

fn render_state_machine_box(state_name: &str, is_current: bool, width: usize) -> Vec<String> {
    let marker = if is_current { "* " } else { "  " };
    let content = format!("{marker}{state_name}");
    let padded = format!("{content:<width$}", width = width);
    vec![
        format!("    +{}+", "-".repeat(width + 2)),
        format!("    | {padded} |"),
        format!("    +{}+", "-".repeat(width + 2)),
    ]
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
                    "{} {}/{} ready",
                    format_duration_compact(Some(candidate.window_seconds)),
                    candidate.ready_topic_maps,
                    candidate.total_topic_maps
                )
            })
            .collect::<Vec<_>>()
            .join(" | "),
    )
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
        assert!(output.contains("execution scope: trace"));
        assert!(output.contains("runtime: idle"));
        assert!(output.contains("topic maps: 2"));
    }

    #[test]
    fn full_report_includes_state_machine() {
        let output = render_report(&sample_report(), true);
        assert!(output.contains("Topic automation diagnostics:"));
        assert!(output.contains("current state: idle"));
        assert!(output.contains("state machine:"));
        assert!(output.contains("+- yes -> recomputing_topic_maps"));
    }
}
