use anyhow::{bail, Result};

use crate::ui::{print_command_status, print_with_pager, with_spinner, CommandStatus};

use super::{
    api::{self, TopicAutomationConfig, TopicAutomationConfigPatch, TopicsConfigReport},
    ConfigArgs, ConfigSetArgs, ResolvedContext,
};

pub async fn run_view(ctx: &ResolvedContext, args: &ConfigArgs, json: bool) -> Result<()> {
    let report = with_spinner(
        "Loading Topics config...",
        api::fetch_topics_config(ctx, args.automation_id.as_deref()),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&report)?);
        return Ok(());
    }

    print_with_pager(&render_report(&report))?;
    Ok(())
}

pub async fn run_set(ctx: &ResolvedContext, args: &ConfigSetArgs, json: bool) -> Result<()> {
    let patch = args.to_patch()?;
    let updated = with_spinner(
        "Updating Topics config...",
        api::update_topics_config(ctx, patch),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&updated)?);
        return Ok(());
    }

    print_command_status(CommandStatus::Success, "Updated Topics automation config");
    print_with_pager(&render_single_config(
        &report_header(&ctx.project.name, ctx.client.org_name()),
        &updated,
    ))?;
    Ok(())
}

fn render_report(report: &TopicsConfigReport) -> String {
    let mut output = report_header(&report.project.name, &report.project.org_name);

    if report.automations.is_empty() {
        output.push_str("\nNo topic automations found.\n");
        return output;
    }

    output.push_str(&format!(
        "\nTopic automations: {}\n",
        report.automations.len()
    ));
    for automation in &report.automations {
        output.push('\n');
        output.push_str(&render_config_block(automation));
        output.push('\n');
    }
    output
}

fn render_single_config(header: &str, automation: &TopicAutomationConfig) -> String {
    let mut output = String::new();
    output.push_str(header);
    output.push('\n');
    output.push('\n');
    output.push_str(&render_config_block(automation));
    output.push('\n');
    output
}

fn report_header(project_name: &str, org_name: &str) -> String {
    format!("Project: {org_name} / {project_name}")
}

fn render_config_block(automation: &TopicAutomationConfig) -> String {
    let mut lines = vec![format!("{} ({})", automation.name, automation.id)];
    lines.push(format!(
        "  description: {}",
        if automation.description.is_empty() {
            "none".to_string()
        } else {
            automation.description.clone()
        }
    ));
    push_section(
        &mut lines,
        "identity",
        &[ConfigField {
            label: "scope",
            value: automation
                .scope_type
                .as_deref()
                .unwrap_or("n/a")
                .to_string(),
            hint: Some(match automation.scope_type.as_deref() {
                Some("trace") => "facets and topic labels are computed per-trace",
                _ => "record type Topics processes",
            }),
        }],
    );
    push_section(
        &mut lines,
        "timing",
        &[
            ConfigField {
                label: "topic window",
                value: format_duration_compact(automation.window_seconds),
                hint: Some("when computing topics, we look at this much recent facet history"),
            },
            ConfigField {
                label: "generation cadence",
                value: format_duration_compact(automation.rerun_seconds),
                hint: Some("how often Topics tries to generate fresh topic maps"),
            },
            ConfigField {
                label: "relabel overlap",
                value: format_duration_compact(automation.relabel_overlap_seconds),
                hint: Some(
                    "after recompute, this much recent history is relabeled with the new topics",
                ),
            },
            ConfigField {
                label: "idle time",
                value: format_duration_compact(automation.idle_seconds),
                hint: Some(
                    "how long trace activity must stay quiet before facets and topic labels run",
                ),
            },
        ],
    );
    push_section(
        &mut lines,
        "selection",
        &[
            ConfigField {
                label: "sampling rate",
                value: automation
                    .sampling_rate
                    .map(format_sampling_percent)
                    .unwrap_or_else(|| "n/a".to_string()),
                hint: Some("percent of matching traces used"),
            },
            ConfigField {
                label: "filter",
                value: automation
                    .btql_filter
                    .as_deref()
                    .unwrap_or("none")
                    .to_string(),
                hint: Some("BTQL filter used to select which traces get facets and topics"),
            },
        ],
    );
    lines.push(String::new());
    lines.push("  functions:".to_string());
    push_function_group(
        &mut lines,
        "facets",
        "source facet labels",
        &automation.facet_functions,
    );
    push_function_group(
        &mut lines,
        "topic maps",
        "classifiers built from those facets",
        &automation.topic_map_functions,
    );

    lines.join("\n")
}

struct ConfigField<'a> {
    label: &'a str,
    value: String,
    hint: Option<&'a str>,
}

fn push_section(lines: &mut Vec<String>, title: &str, fields: &[ConfigField<'_>]) {
    lines.push(String::new());
    lines.push(format!("  {title}:"));
    for field in fields {
        let mut line = format!("    {}: {}", field.label, field.value);
        if let Some(hint) = field.hint {
            line.push_str(&format!(" ({hint})"));
        }
        lines.push(line);
    }
}

fn push_function_group(
    lines: &mut Vec<String>,
    label: &str,
    hint: &str,
    functions: &[api::FunctionSummary],
) {
    lines.push(format!("    {label}: ({hint})"));
    if functions.is_empty() {
        lines.push("      - none".to_string());
        return;
    }

    for function in functions {
        match function.id.as_deref() {
            Some(id) => lines.push(format!("      - {} (id: {id})", function.name)),
            None => lines.push(format!("      - {}", function.name)),
        }
    }
}

fn format_sampling_percent(value: f64) -> String {
    let percent = value * 100.0;
    if (percent - percent.round()).abs() < 0.000_001 {
        format!("{percent:.0}%")
    } else if (percent * 10.0 - (percent * 10.0).round()).abs() < 0.000_001 {
        format!("{percent:.1}%")
    } else {
        format!("{percent:.2}%")
    }
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

impl ConfigSetArgs {
    fn to_patch(&self) -> Result<TopicAutomationConfigPatch> {
        let patch = TopicAutomationConfigPatch {
            automation_id: self.automation_id.clone(),
            name: self.name.clone(),
            description: self.description.clone(),
            btql_filter: if self.clear_filter {
                Some(None)
            } else {
                self.filter.clone().map(Some)
            },
            sampling_rate: parse_sampling_rate(self.sampling_rate.as_deref())?,
            window_seconds: parse_duration_to_seconds(self.window.as_deref())?,
            rerun_seconds: parse_duration_to_seconds(self.cadence.as_deref())?,
            relabel_overlap_seconds: parse_duration_to_seconds(self.relabel_overlap.as_deref())?,
            idle_seconds: parse_duration_to_seconds(self.idle.as_deref())?,
        };

        if patch.name.is_none()
            && patch.description.is_none()
            && patch.btql_filter.is_none()
            && patch.sampling_rate.is_none()
            && patch.window_seconds.is_none()
            && patch.rerun_seconds.is_none()
            && patch.relabel_overlap_seconds.is_none()
            && patch.idle_seconds.is_none()
        {
            bail!("no topic automation updates were requested");
        }

        Ok(patch)
    }
}

fn parse_duration_to_seconds(value: Option<&str>) -> Result<Option<i64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let value = value.trim();
    if value.is_empty() {
        bail!("duration cannot be empty");
    }

    let suffix = value.chars().last().filter(|ch| ch.is_ascii_alphabetic());
    let (number, unit) = match suffix {
        Some(unit) => (&value[..value.len() - unit.len_utf8()], unit),
        None => (value, 's'),
    };
    let amount = number.trim().parse::<i64>()?;
    let multiplier = match unit.to_ascii_lowercase() {
        's' => 1,
        'm' => 60,
        'h' => 60 * 60,
        'd' => 24 * 60 * 60,
        'w' => 7 * 24 * 60 * 60,
        _ => bail!("unsupported duration unit '{unit}'"),
    };
    Ok(Some(amount * multiplier))
}

fn parse_sampling_rate(value: Option<&str>) -> Result<Option<f64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let trimmed = value.trim();
    if trimmed.is_empty() {
        bail!("sampling rate cannot be empty");
    }

    let percent_value = if let Some(number) = trimmed.strip_suffix('%') {
        number.trim().parse::<f64>()?
    } else {
        let parsed = trimmed.parse::<f64>()?;
        if trimmed.contains('.') && parsed <= 1.0 {
            parsed * 100.0
        } else {
            parsed
        }
    };

    if !(0.0..=100.0).contains(&percent_value) {
        bail!("sampling rate must be between 0% and 100%");
    }

    Ok(Some(percent_value / 100.0))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_config() -> TopicAutomationConfig {
        TopicAutomationConfig {
            id: "auto_123".to_string(),
            name: "Topics".to_string(),
            description: "Automatically extract facets and classify logs using topic maps"
                .to_string(),
            scope_type: Some("trace".to_string()),
            btql_filter: None,
            sampling_rate: Some(1.0),
            window_seconds: Some(3600),
            rerun_seconds: Some(86400),
            relabel_overlap_seconds: Some(3600),
            idle_seconds: Some(30),
            facet_functions: vec![api::FunctionSummary {
                name: "Task".to_string(),
                ref_type: "global".to_string(),
                function_type: Some("facet".to_string()),
                id: None,
                version: None,
                btql_filter: None,
            }],
            topic_map_functions: vec![api::FunctionSummary {
                name: "Task".to_string(),
                ref_type: "function".to_string(),
                function_type: Some("classifier".to_string()),
                id: Some("func_1".to_string()),
                version: None,
                btql_filter: None,
            }],
        }
    }

    #[test]
    fn render_config_block_includes_meanings() {
        let output = render_config_block(&sample_config());
        assert!(output.contains("  timing:"));
        assert!(output.contains("topic window: 1h"));
        assert!(
            output.contains("(when computing topics, we look at this much recent facet history)")
        );
        assert!(output.contains("  functions:"));
        assert!(output.contains("sampling rate: 100%"));
        assert!(output.contains("    facets: (source facet labels)"));
        assert!(output.contains("      - Task"));
        assert!(output.contains("    topic maps: (classifiers built from those facets)"));
        assert!(output.contains("      - Task (id: func_1)"));
    }

    #[test]
    fn config_set_args_build_patch() {
        let args = ConfigSetArgs {
            automation_id: Some("auto_123".to_string()),
            name: None,
            description: None,
            window: Some("1h".to_string()),
            cadence: Some("1d".to_string()),
            relabel_overlap: None,
            idle: Some("30s".to_string()),
            sampling_rate: Some("50%".to_string()),
            filter: Some("root_span_id = 'abc'".to_string()),
            clear_filter: false,
        };

        let patch = args.to_patch().expect("patch");
        assert_eq!(patch.window_seconds, Some(3600));
        assert_eq!(patch.rerun_seconds, Some(86400));
        assert_eq!(patch.idle_seconds, Some(30));
        assert_eq!(patch.sampling_rate, Some(0.5));
        assert_eq!(
            patch.btql_filter,
            Some(Some("root_span_id = 'abc'".to_string()))
        );
    }

    #[test]
    fn config_set_args_accepts_fractional_sampling_rate_for_compatibility() {
        let args = ConfigSetArgs {
            automation_id: None,
            name: None,
            description: None,
            window: None,
            cadence: None,
            relabel_overlap: None,
            idle: None,
            sampling_rate: Some("0.25".to_string()),
            filter: None,
            clear_filter: false,
        };

        let patch = args.to_patch().expect("patch");
        assert_eq!(patch.sampling_rate, Some(0.25));
    }

    #[test]
    fn config_set_args_treats_integer_sampling_rate_as_percent() {
        let args = ConfigSetArgs {
            automation_id: None,
            name: None,
            description: None,
            window: None,
            cadence: None,
            relabel_overlap: None,
            idle: None,
            sampling_rate: Some("25".to_string()),
            filter: None,
            clear_filter: false,
        };

        let patch = args.to_patch().expect("patch");
        assert_eq!(patch.sampling_rate, Some(0.25));
    }
}
