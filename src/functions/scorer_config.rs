use anyhow::{bail, Context, Result};
use serde_json::{json, Map, Value};

use crate::utils::{merge_json_objects, read_text_source, read_yaml_object_source};

use super::prompt_config::{
    parse_choice_scores_source, parse_classifications_source, validate_unit_interval,
    PromptConfigArgs,
};

/// Inputs shared by scorer creation and partial scorer updates.
///
/// Creation supplies all required values, while update leaves unchanged values
/// as `None`. Keeping this independent of the clap structs lets both commands
/// share schema construction without weakening create-time CLI requirements.
pub(crate) struct ScorerConfig<'a> {
    pub(crate) messages: Option<&'a str>,
    pub(crate) model: Option<&'a str>,
    pub(crate) prompt_config: &'a PromptConfigArgs,
    pub(crate) choice_scores: Option<&'a str>,
    pub(crate) classifications: Option<&'a str>,
    pub(crate) use_cot: Option<bool>,
    pub(crate) allow_no_match: Option<bool>,
    pub(crate) pass_threshold: Option<f64>,
    pub(crate) metadata: Option<&'a str>,
    pub(crate) metadata_label: &'a str,
}

/// Build the top-level fields containing a scorer's prompt configuration.
///
/// The result is a complete set of fields for create when `require_output` is
/// true and a partial patch for update otherwise.
pub(crate) fn build_scorer_config(
    config: &ScorerConfig<'_>,
    require_output: bool,
) -> Result<Map<String, Value>> {
    validate_output_selection(config, require_output)?;

    let mut result = Map::new();
    let mut prompt_data = Map::new();

    if let Some(source) = config.messages {
        prompt_data.insert(
            "prompt".to_string(),
            json!({
                "type": "chat",
                "messages": parse_messages_source(source)?,
            }),
        );
    }

    if let Some(output) = build_output_parser(config)? {
        if let Some(function_type) = output.function_type {
            result.insert(
                "function_type".to_string(),
                Value::String(function_type.to_string()),
            );
        }
        prompt_data.insert("parser".to_string(), Value::Object(output.parser));
    }

    let prompt_config = config.prompt_config.build_prompt_data_patch(config.model)?;
    merge_json_objects(&mut prompt_data, &prompt_config);
    if !prompt_data.is_empty() {
        result.insert("prompt_data".to_string(), Value::Object(prompt_data));
    }

    let metadata = build_metadata(config)?;
    if !metadata.is_empty() {
        result.insert("metadata".to_string(), Value::Object(metadata));
    }

    Ok(result)
}

fn validate_output_selection(config: &ScorerConfig<'_>, require_output: bool) -> Result<()> {
    match (config.choice_scores, config.classifications) {
        (Some(_), Some(_)) => bail!(
            "use either --choice-scores for score output or --classifications for classification output, not both"
        ),
        (None, None) if require_output => bail!(
            "output choices required. Pass --choice-scores <SOURCE> or --classifications <SOURCE>"
        ),
        _ => {}
    }

    if config.choice_scores.is_some() && config.allow_no_match.is_some() {
        bail!("--allow-no-match applies to classification output, not --choice-scores");
    }
    if config.classifications.is_some() && config.pass_threshold.is_some() {
        bail!("--pass-threshold applies to score output and cannot be used with --classifications");
    }
    Ok(())
}

struct OutputParser {
    function_type: Option<&'static str>,
    parser: Map<String, Value>,
}

fn build_output_parser(config: &ScorerConfig<'_>) -> Result<Option<OutputParser>> {
    let mut parser = Map::new();
    let mut function_type = None;

    if let Some(source) = config.choice_scores {
        parser.insert("type".to_string(), json!("llm_classifier"));
        parser.insert(
            "choice_scores".to_string(),
            Value::Object(parse_choice_scores_source(source)?),
        );
        function_type = Some("scorer");
    }
    if let Some(source) = config.classifications {
        parser.insert("type".to_string(), json!("llm_classifier"));
        parser.insert(
            "choice".to_string(),
            Value::Array(parse_classifications_source(source)?),
        );
        function_type = Some("classifier");
    }
    if let Some(use_cot) = config.use_cot {
        parser.insert("use_cot".to_string(), Value::Bool(use_cot));
    }
    if let Some(allow_no_match) = config.allow_no_match {
        parser.insert("allow_no_match".to_string(), Value::Bool(allow_no_match));
    }

    Ok((!parser.is_empty()).then_some(OutputParser {
        function_type,
        parser,
    }))
}

fn parse_messages_source(source: &str) -> Result<Value> {
    let raw = read_text_source(source, "messages")?;
    let messages: Value = serde_json::from_str(&raw).context("invalid JSON in messages")?;
    match messages {
        Value::Array(_) => Ok(messages),
        _ => bail!("messages must be a JSON array of chat messages"),
    }
}

fn build_metadata(config: &ScorerConfig<'_>) -> Result<Map<String, Value>> {
    let mut metadata = match config.metadata {
        Some(source) => read_yaml_object_source(source, config.metadata_label)?,
        None => Map::new(),
    };
    if let Some(pass_threshold) = config.pass_threshold {
        validate_unit_interval(pass_threshold, "--pass-threshold")?;
        metadata.insert("__pass_threshold".to_string(), json!(pass_threshold));
    }
    Ok(metadata)
}
