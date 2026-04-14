use anyhow::{bail, Result};

use crate::ui::{print_with_pager, with_spinner};

use super::{api, ResolvedContext, RewindArgs};

pub async fn run(ctx: &ResolvedContext, args: &RewindArgs, json: bool) -> Result<()> {
    let window_seconds = parse_topic_window(&args.topic_window)?;
    let report = with_spinner(
        "Rewinding Topics history...",
        api::rewind_topic_automations(ctx, args.automation_id.as_deref(), window_seconds),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }

    print_with_pager(&render_report(&report))?;
    Ok(())
}

fn render_report(report: &api::TopicsRewindReport) -> String {
    let mut lines = vec![format!(
        "Project: {} / {} ({})",
        report.project.org_name, report.project.name, report.project.id
    )];

    if report.rewound.is_empty() {
        lines.push("No topic automations found.".to_string());
        return lines.join("\n");
    }

    lines.push(format!(
        "Rewound Topics for {} topic automation(s)",
        report.rewound.len()
    ));

    for item in &report.rewound {
        lines.push(String::new());
        lines.push(format!("{} ({})", item.name, item.id));
        lines.push(format!("  object: {}", item.object_id));
        lines.push(format!(
            "  rewind window: {}",
            format_duration_compact(item.window_seconds)
        ));
        lines.push(format!("  rewind start xact: {}", item.start_xact_id));
        lines.push("  next check: now".to_string());
        lines.push(
            "  effect: traces in this window will be reprocessed for facets and topic labels on the next executor pass"
                .to_string(),
        );
    }

    lines.join("\n")
}

fn parse_topic_window(value: &str) -> Result<i64> {
    let value = value.trim();
    if value.is_empty() {
        bail!("topic window cannot be empty");
    }

    let suffix = value.chars().last().filter(|ch| ch.is_ascii_alphabetic());
    let (number, unit) = match suffix {
        Some(unit) => (&value[..value.len() - unit.len_utf8()], unit),
        None => (value, 's'),
    };
    let amount = number.trim().parse::<i64>()?;
    if amount <= 0 {
        bail!("topic window must be greater than zero");
    }
    let multiplier = match unit.to_ascii_lowercase() {
        's' => 1,
        'm' => 60,
        'h' => 60 * 60,
        'd' => 24 * 60 * 60,
        'w' => 7 * 24 * 60 * 60,
        _ => bail!("unsupported duration unit '{unit}'"),
    };
    Ok(amount * multiplier)
}

fn format_duration_compact(seconds: i64) -> String {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_report_handles_empty_results() {
        let report = api::TopicsRewindReport {
            project: api::TopicsProjectSummary {
                id: "proj_123".to_string(),
                name: "demo-project".to_string(),
                org_name: "demo-org".to_string(),
                topics_url: "https://app.example.com/app/demo-org/p/demo-project/topics"
                    .to_string(),
            },
            rewound: Vec::new(),
        };

        let output = render_report(&report);
        assert!(output.contains("No topic automations found."));
    }

    #[test]
    fn render_report_lists_rewound_automations() {
        let report = api::TopicsRewindReport {
            project: api::TopicsProjectSummary {
                id: "proj_123".to_string(),
                name: "demo-project".to_string(),
                org_name: "demo-org".to_string(),
                topics_url: "https://app.example.com/app/demo-org/p/demo-project/topics"
                    .to_string(),
            },
            rewound: vec![api::TopicAutomationRewindResult {
                id: "auto_123".to_string(),
                name: "Topics".to_string(),
                object_id: "project_logs:proj_123".to_string(),
                window_seconds: 7 * 24 * 60 * 60,
                start_xact_id: "1000706252802625535".to_string(),
            }],
        };

        let output = render_report(&report);
        assert!(output.contains("Rewound Topics for 1 topic automation(s)"));
        assert!(output.contains("rewind window: 1w"));
        assert!(output.contains("rewind start xact: 1000706252802625535"));
        assert!(output.contains("traces in this window will be reprocessed"));
    }

    #[test]
    fn parse_topic_window_accepts_duration_suffixes() {
        assert_eq!(parse_topic_window("7d").expect("window"), 7 * 24 * 60 * 60);
    }

    #[test]
    fn parse_topic_window_rejects_empty_values() {
        let error = parse_topic_window("  ").expect_err("expected error");
        assert!(error.to_string().contains("topic window cannot be empty"));
    }

    #[test]
    fn render_report_handles_shorter_windows() {
        let report = api::TopicsRewindReport {
            project: api::TopicsProjectSummary {
                id: "proj_123".to_string(),
                name: "demo-project".to_string(),
                org_name: "demo-org".to_string(),
                topics_url: "https://app.example.com/app/demo-org/p/demo-project/topics"
                    .to_string(),
            },
            rewound: vec![api::TopicAutomationRewindResult {
                id: "auto_123".to_string(),
                name: "Topics".to_string(),
                object_id: "project_logs:proj_123".to_string(),
                window_seconds: 24 * 60 * 60,
                start_xact_id: "1000706252802625535".to_string(),
            }],
        };

        let output = render_report(&report);
        assert!(output.contains("rewind window: 1d"));
    }
}
