use anyhow::{bail, Result};

use crate::ui::{print_command_status, print_with_pager, with_spinner, CommandStatus};

use super::{
    api::{
        self, FunctionSummary, TopicAutomationConfig, TopicAutomationConfigPatch,
        TopicMapConfigPatch, TopicMapConfigUpdate, TopicMapGenerationSettings, TopicsConfigReport,
    },
    ConfigArgs, ConfigSetArgs, ResolvedContext, TopicMapSetArgs,
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
        &report_header(&ctx.project.name, &ctx.project.id, ctx.client.org_name()),
        &updated,
    ))?;
    Ok(())
}

pub async fn run_topic_map_set(
    ctx: &ResolvedContext,
    args: &TopicMapSetArgs,
    json: bool,
) -> Result<()> {
    let patch = args.to_patch()?;
    let updated = with_spinner(
        "Updating Topics topic map...",
        api::update_topic_map_config(ctx, patch),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&updated)?);
        return Ok(());
    }

    print_command_status(CommandStatus::Success, "Updated Topics topic map config");
    print_with_pager(&render_single_topic_map_update(
        &report_header(&ctx.project.name, &ctx.project.id, ctx.client.org_name()),
        &updated,
    ))?;
    Ok(())
}

fn render_report(report: &TopicsConfigReport) -> String {
    let mut output = report_header(
        &report.project.name,
        &report.project.id,
        &report.project.org_name,
    );

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

fn render_single_topic_map_update(header: &str, updated: &TopicMapConfigUpdate) -> String {
    let mut output = String::new();
    output.push_str(header);
    output.push('\n');
    output.push('\n');
    output.push_str(&render_topic_map_update_block(
        &updated.automation,
        &updated.topic_map_id,
    ));
    output.push('\n');
    output
}

fn report_header(project_name: &str, project_id: &str, org_name: &str) -> String {
    format!("Project: {org_name} / {project_name} ({project_id})")
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
    push_topic_map_group(&mut lines, &automation.topic_map_functions);

    lines.join("\n")
}

fn render_topic_map_update_block(automation: &TopicAutomationConfig, topic_map_id: &str) -> String {
    let mut lines = vec![format!("{} ({})", automation.name, automation.id)];
    let Some(topic_map) = automation
        .topic_map_functions
        .iter()
        .find(|function| function.id.as_deref() == Some(topic_map_id))
    else {
        lines.push(format!("  topic map id: {topic_map_id}"));
        lines.push("  details: unavailable after update".to_string());
        return lines.join("\n");
    };

    lines.push(format!(
        "  topic map: {}",
        format_function_identity(topic_map)
    ));
    push_topic_map_details(&mut lines, topic_map, "    ");
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
        lines.push(format!("      - {}", format_function_identity(function)));
    }
}

fn push_topic_map_group(lines: &mut Vec<String>, functions: &[api::FunctionSummary]) {
    lines.push("    topic maps: (classifiers built from those facets)".to_string());
    if functions.is_empty() {
        lines.push("      - none".to_string());
        return;
    }

    for function in functions {
        lines.push(format!("      - {}", format_function_identity(function)));
        push_topic_map_details(lines, function, "        ");
    }
}

fn push_topic_map_details(lines: &mut Vec<String>, function: &FunctionSummary, indent: &str) {
    if let Some(description) = function
        .description
        .as_deref()
        .filter(|value| !value.is_empty())
    {
        lines.push(format!("{indent}description: {description}"));
    }
    if let Some(source_facet) = function.source_facet.as_deref() {
        lines.push(format!(
            "{indent}source facet: {source_facet} (facet values this topic map clusters)"
        ));
    }
    if let Some(embedding_model) = function.embedding_model.as_deref() {
        lines.push(format!(
            "{indent}embedding model: {embedding_model} (vector field used for clustering)"
        ));
    }
    if let Some(naming_model) = function
        .generation_settings
        .as_ref()
        .and_then(|settings| settings.naming_model.as_deref())
    {
        lines.push(format!(
            "{indent}naming model: {naming_model} (LLM used to name generated topics)"
        ));
    }
    if let Some(generation_summary) =
        format_generation_settings(function.generation_settings.as_ref())
    {
        lines.push(format!(
            "{indent}generation: {generation_summary} (clustering settings used to build this topic map)"
        ));
    }
    if let Some(distance_threshold) = function.distance_threshold {
        lines.push(format!(
            "{indent}distance threshold: {} (farther matches become no_match)",
            format_float_compact(distance_threshold)
        ));
    }
    if let Some(btql_filter) = function.btql_filter.as_deref() {
        lines.push(format!(
            "{indent}filter: {btql_filter} (extra BTQL filter for this topic map)"
        ));
    }
    if let Some(version) = function.version.as_deref() {
        lines.push(format!("{indent}version: {version}"));
    }
}

fn format_function_identity(function: &FunctionSummary) -> String {
    match function.id.as_deref() {
        Some(id) => format!("{} (id: {id})", function.name),
        None => function.name.clone(),
    }
}

fn format_generation_settings(settings: Option<&TopicMapGenerationSettings>) -> Option<String> {
    let settings = settings?;
    let mut parts = Vec::new();
    if let Some(algorithm) = settings.algorithm.as_deref() {
        parts.push(format!("algorithm {algorithm}"));
    }
    if let Some(dimension_reduction) = settings.dimension_reduction.as_deref() {
        parts.push(format!("reduction {dimension_reduction}"));
    }
    if let Some(sample_size) = settings.sample_size {
        parts.push(format!("sample size {sample_size}"));
    }
    if let Some(n_clusters) = settings.n_clusters {
        parts.push(format!("n clusters {n_clusters}"));
    }
    if let Some(min_cluster_size) = settings.min_cluster_size {
        parts.push(format!("min cluster size {min_cluster_size}"));
    }
    if let Some(min_samples) = settings.min_samples {
        parts.push(format!("min samples {min_samples}"));
    }
    if let Some(hierarchy_threshold) = settings.hierarchy_threshold {
        parts.push(format!("hierarchy threshold {hierarchy_threshold}"));
    }
    if parts.is_empty() {
        None
    } else {
        Some(parts.join(" | "))
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

fn format_float_compact(value: f64) -> String {
    let rounded = value.round();
    if (value - rounded).abs() < 0.000_001 {
        format!("{rounded:.0}")
    } else {
        let text = format!("{value:.4}");
        text.trim_end_matches('0').trim_end_matches('.').to_string()
    }
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

impl TopicMapSetArgs {
    fn to_patch(&self) -> Result<TopicMapConfigPatch> {
        let patch = TopicMapConfigPatch {
            automation_id: self.automation_id.clone(),
            topic_map_target: self.topic_map.trim().to_string(),
            name: self.name.clone(),
            description: self.description.clone(),
            source_facet: trim_to_option(self.source_facet.as_deref()),
            embedding_model: trim_to_option(self.embedding_model.as_deref()),
            distance_threshold: self.distance_threshold,
            algorithm: self.algorithm.clone(),
            dimension_reduction: self.dimension_reduction.clone(),
            sample_size: self.sample_size,
            n_clusters: self.n_clusters,
            min_cluster_size: self.min_cluster_size,
            min_samples: self.min_samples,
            hierarchy_threshold: self.hierarchy_threshold,
            naming_model: trim_to_option(self.naming_model.as_deref()),
        };

        if patch.topic_map_target.is_empty() {
            bail!("topic map target cannot be empty");
        }
        if patch.name.is_none()
            && patch.description.is_none()
            && patch.source_facet.is_none()
            && patch.embedding_model.is_none()
            && patch.distance_threshold.is_none()
            && patch.algorithm.is_none()
            && patch.dimension_reduction.is_none()
            && patch.sample_size.is_none()
            && patch.n_clusters.is_none()
            && patch.min_cluster_size.is_none()
            && patch.min_samples.is_none()
            && patch.hierarchy_threshold.is_none()
            && patch.naming_model.is_none()
        {
            bail!("no topic map updates were requested");
        }

        Ok(patch)
    }
}

fn trim_to_option(value: Option<&str>) -> Option<String> {
    value
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
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
                description: None,
                version: None,
                btql_filter: None,
                source_facet: None,
                embedding_model: None,
                distance_threshold: None,
                generation_settings: None,
            }],
            topic_map_functions: vec![api::FunctionSummary {
                name: "Task".to_string(),
                ref_type: "function".to_string(),
                function_type: Some("classifier".to_string()),
                id: Some("func_1".to_string()),
                description: Some("Groups task-like traces into reusable topics".to_string()),
                version: None,
                btql_filter: None,
                source_facet: Some("Task".to_string()),
                embedding_model: Some("brain-embedding-1".to_string()),
                distance_threshold: Some(0.35),
                generation_settings: Some(api::TopicMapGenerationSettings {
                    algorithm: Some("hdbscan".to_string()),
                    dimension_reduction: Some("umap".to_string()),
                    sample_size: None,
                    n_clusters: None,
                    min_cluster_size: Some(25),
                    min_samples: Some(10),
                    hierarchy_threshold: None,
                    naming_model: Some("brain-agent-1".to_string()),
                }),
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
        assert!(output.contains("source facet: Task"));
        assert!(output.contains("embedding model: brain-embedding-1"));
        assert!(output.contains("naming model: brain-agent-1"));
        assert!(output.contains(
            "generation: algorithm hdbscan | reduction umap | min cluster size 25 | min samples 10"
        ));
        assert!(output.contains("distance threshold: 0.35"));
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

    #[test]
    fn topic_map_set_args_build_patch() {
        let args = TopicMapSetArgs {
            automation_id: Some("auto_123".to_string()),
            topic_map: "Task".to_string(),
            name: None,
            description: None,
            source_facet: Some("Task".to_string()),
            embedding_model: Some("brain-embedding-1".to_string()),
            distance_threshold: Some(0.4),
            algorithm: Some("hdbscan".to_string()),
            dimension_reduction: Some("umap".to_string()),
            sample_size: None,
            n_clusters: None,
            min_cluster_size: Some(20),
            min_samples: Some(10),
            hierarchy_threshold: None,
            naming_model: Some("brain-agent-1".to_string()),
        };

        let patch = args.to_patch().expect("patch");
        assert_eq!(patch.automation_id.as_deref(), Some("auto_123"));
        assert_eq!(patch.topic_map_target, "Task");
        assert_eq!(patch.embedding_model.as_deref(), Some("brain-embedding-1"));
        assert_eq!(patch.distance_threshold, Some(0.4));
        assert_eq!(patch.algorithm.as_deref(), Some("hdbscan"));
        assert_eq!(patch.dimension_reduction.as_deref(), Some("umap"));
        assert_eq!(patch.min_cluster_size, Some(20));
        assert_eq!(patch.min_samples, Some(10));
        assert_eq!(patch.naming_model.as_deref(), Some("brain-agent-1"));
    }
}
