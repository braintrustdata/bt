use anyhow::Result;
use chrono::{DateTime, SecondsFormat, Utc};

use crate::ui::{print_with_pager, with_spinner};

use super::{api, ResolvedContext};

pub async fn run(ctx: &ResolvedContext, json: bool) -> Result<()> {
    let report = with_spinner("Queueing Topics run...", api::poke_topic_automations(ctx)).await?;

    if json {
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }

    print_with_pager(&render_report(&report))?;
    Ok(())
}

fn render_report(report: &api::TopicsPokeReport) -> String {
    let mut lines = vec![format!(
        "Project: {} / {}",
        report.project.org_name, report.project.name
    )];

    if report.queued.is_empty() {
        lines.push("No topic automations found.".to_string());
        return lines.join("\n");
    }

    lines.push(format!(
        "Queued Topics run for {} topic automation(s)",
        report.queued.len()
    ));

    for item in &report.queued {
        lines.push(String::new());
        lines.push(format!("{} ({})", item.name, item.id));
        lines.push(format!("  object: {}", item.object_id));
        if item.runtime_state.as_deref() == Some("idle") {
            lines.push("  next rerun: now".to_string());
            if let Some(previous_next_run_at) = item.previous_next_run_at.as_deref() {
                lines.push(format!(
                    "  previous schedule: {}",
                    format_timestamp_with_relative(previous_next_run_at)
                ));
            }
            lines.push(
                "  effect: Topics will re-check readiness on the next executor pass; if topic maps are ready, recompute can start then"
                    .to_string(),
            );
        } else {
            lines.push("  next check: now".to_string());
            if let Some(previous_next_run_at) = item.previous_next_run_at.as_deref() {
                lines.push(format!(
                    "  previous schedule: {}",
                    format_timestamp_with_relative(previous_next_run_at)
                ));
            }
            match item.runtime_state.as_deref() {
                Some(state) => lines.push(format!(
                    "  effect: Topics is in {state}; the next executor pass will continue that state-machine work immediately"
                )),
                None => lines.push(
                    "  effect: the next executor pass will pick up Topics immediately"
                        .to_string(),
                ),
            }
        }
    }

    lines.join("\n")
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
    use super::*;

    #[test]
    fn render_report_handles_empty_results() {
        let report = api::TopicsPokeReport {
            project: api::TopicsProjectSummary {
                id: "proj_123".to_string(),
                name: "demo-project".to_string(),
                org_name: "demo-org".to_string(),
                topics_url: "https://app.example.com/app/demo-org/p/demo-project/topics"
                    .to_string(),
            },
            queued: Vec::new(),
        };

        let output = render_report(&report);
        assert!(output.contains("No topic automations found."));
    }

    #[test]
    fn render_report_lists_queued_automations() {
        let report = api::TopicsPokeReport {
            project: api::TopicsProjectSummary {
                id: "proj_123".to_string(),
                name: "demo-project".to_string(),
                org_name: "demo-org".to_string(),
                topics_url: "https://app.example.com/app/demo-org/p/demo-project/topics"
                    .to_string(),
            },
            queued: vec![api::TopicAutomationPokeResult {
                id: "auto_123".to_string(),
                name: "Topics".to_string(),
                object_id: "project_logs:proj_123".to_string(),
                previous_next_run_at: Some("2026-03-09T12:00:00Z".to_string()),
                runtime_state: Some("idle".to_string()),
            }],
        };

        let output = render_report(&report);
        assert!(output.contains("Queued Topics run for 1 topic automation(s)"));
        assert!(output.contains("Topics (auto_123)"));
        assert!(output.contains("next rerun: now"));
        assert!(output.contains("previous schedule: 2026-03-09T12:00:00Z"));
        assert!(output.contains("re-check readiness on the next executor pass"));
    }

    #[test]
    fn render_report_explains_non_idle_poke() {
        let report = api::TopicsPokeReport {
            project: api::TopicsProjectSummary {
                id: "proj_123".to_string(),
                name: "demo-project".to_string(),
                org_name: "demo-org".to_string(),
                topics_url: "https://app.example.com/app/demo-org/p/demo-project/topics"
                    .to_string(),
            },
            queued: vec![api::TopicAutomationPokeResult {
                id: "auto_123".to_string(),
                name: "Topics".to_string(),
                object_id: "project_logs:proj_123".to_string(),
                previous_next_run_at: Some("2026-03-09T12:00:00Z".to_string()),
                runtime_state: Some("backfilling_topic_classifications".to_string()),
            }],
        };

        let output = render_report(&report);
        assert!(output.contains("next check: now"));
        assert!(output.contains("Topics is in backfilling_topic_classifications"));
    }
}
