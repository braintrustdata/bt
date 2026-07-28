use std::collections::HashSet;

use anyhow::{bail, Context, Result};
use clap::{Args, ValueEnum};
use serde_json::{json, Map, Number, Value};

use crate::utils::{merge_json_objects, read_text_source};

#[derive(Debug, Clone, Default, Args)]
pub(crate) struct PromptConfigArgs {
    /// Sampling temperature, between 0 and 2. Some models support a smaller
    /// range or do not support custom temperatures.
    #[arg(long, value_name = "NUMBER")]
    temperature: Option<f64>,

    /// Maximum number of generated tokens. Must be greater than 0.
    #[arg(long, value_name = "N")]
    max_tokens: Option<u64>,

    /// Nucleus sampling probability, between 0 and 1.
    #[arg(long, value_name = "NUMBER")]
    top_p: Option<f64>,

    /// Frequency penalty, between -2 and 2.
    #[arg(long, value_name = "NUMBER", allow_hyphen_values = true)]
    frequency_penalty: Option<f64>,

    /// Presence penalty, between -2 and 2.
    #[arg(long, value_name = "NUMBER", allow_hyphen_values = true)]
    presence_penalty: Option<f64>,

    /// Stop sequence. Repeat this flag to specify multiple sequences.
    #[arg(long, value_name = "TEXT", action = clap::ArgAction::Append)]
    stop_sequence: Vec<String>,

    /// Tool choice: auto, none, required, or a specific function name.
    #[arg(long, value_name = "CHOICE")]
    tool_choice: Option<String>,

    /// Reasoning effort for supported models.
    #[arg(long, value_enum)]
    reasoning_effort: Option<ReasoningEffort>,

    /// Response verbosity for supported models.
    #[arg(long, value_enum)]
    verbosity: Option<Verbosity>,

    /// Prompt template syntax. Jinja is stored using Braintrust's `nunjucks`
    /// format; `nunjucks` and `jinja2` are accepted aliases.
    #[arg(long, value_enum, value_name = "FORMAT")]
    template_format: Option<TemplateFormat>,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum ReasoningEffort {
    None,
    Minimal,
    Low,
    Medium,
    High,
}

impl ReasoningEffort {
    fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Minimal => "minimal",
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Verbosity {
    Low,
    Medium,
    High,
}

impl Verbosity {
    fn as_str(self) -> &'static str {
        match self {
            Self::Low => "low",
            Self::Medium => "medium",
            Self::High => "high",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum TemplateFormat {
    Mustache,
    #[value(name = "jinja", alias = "nunjucks", alias = "jinja2")]
    Nunjucks,
    None,
}

impl TemplateFormat {
    fn as_str(self) -> &'static str {
        match self {
            Self::Mustache => "mustache",
            Self::Nunjucks => "nunjucks",
            Self::None => "none",
        }
    }
}

impl PromptConfigArgs {
    /// Build a partial `prompt_data` object matching the app's prompt schema.
    pub(crate) fn build_prompt_data_patch(
        &self,
        model: Option<&str>,
    ) -> Result<Map<String, Value>> {
        let mut prompt_data = Map::new();
        let mut options = Map::new();
        let mut params = Map::new();

        if let Some(model) = model {
            let model = model.trim();
            if model.is_empty() {
                bail!("--model cannot be empty");
            }
            options.insert("model".to_string(), Value::String(model.to_string()));
        }

        insert_optional_number(&mut params, "temperature", self.temperature)?;
        if let Some(max_tokens) = self.max_tokens {
            params.insert("max_tokens".to_string(), Value::Number(max_tokens.into()));
        }
        if let Some(top_p) = self.top_p {
            validate_unit_interval(top_p, "--top-p")?;
            insert_number(&mut params, "top_p", top_p, "--top-p")?;
        }
        insert_optional_number(&mut params, "frequency_penalty", self.frequency_penalty)?;
        insert_optional_number(&mut params, "presence_penalty", self.presence_penalty)?;

        if !self.stop_sequence.is_empty() {
            params.insert(
                "stop".to_string(),
                Value::Array(
                    self.stop_sequence
                        .iter()
                        .map(|value| Value::String(value.clone()))
                        .collect(),
                ),
            );
        }

        if let Some(tool_choice) = self.tool_choice.as_deref() {
            let tool_choice = tool_choice.trim();
            if tool_choice.is_empty() {
                bail!("--tool-choice cannot be empty");
            }
            let value = match tool_choice {
                "auto" | "none" | "required" => Value::String(tool_choice.to_string()),
                function_name => json!({
                    "type": "function",
                    "function": { "name": function_name },
                }),
            };
            params.insert("tool_choice".to_string(), value);
        }

        if let Some(reasoning_effort) = self.reasoning_effort {
            params.insert(
                "reasoning_effort".to_string(),
                Value::String(reasoning_effort.as_str().to_string()),
            );
        }
        if let Some(verbosity) = self.verbosity {
            params.insert(
                "verbosity".to_string(),
                Value::String(verbosity.as_str().to_string()),
            );
        }

        let effective_model = options.get("model").and_then(Value::as_str);
        validate_model_params(effective_model, &params)?;

        if !params.is_empty() {
            options.insert("params".to_string(), Value::Object(params));
        }
        if !options.is_empty() {
            prompt_data.insert("options".to_string(), Value::Object(options));
        }
        if let Some(template_format) = self.template_format {
            prompt_data.insert(
                "template_format".to_string(),
                Value::String(template_format.as_str().to_string()),
            );
        }

        Ok(prompt_data)
    }
}

/// Validate the effective model configuration produced by deep-merging a patch
/// into an existing prompt definition.
///
/// The API accepts partial prompt updates without checking provider-specific
/// model constraints. Validate whenever an update touches the model or its
/// parameters so a successful PATCH cannot leave a scorer or prompt unusable.
pub(crate) fn validate_prompt_data_patch(
    existing_prompt_data: Option<&Value>,
    patch: &Value,
) -> Result<()> {
    let Some(patch_prompt_data) = patch.get("prompt_data").and_then(Value::as_object) else {
        return Ok(());
    };
    if !patch_prompt_data.contains_key("options") {
        return Ok(());
    }

    let mut effective_prompt_data = existing_prompt_data
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    merge_json_objects(&mut effective_prompt_data, patch_prompt_data);

    let Some(options) = effective_prompt_data.get("options") else {
        return Ok(());
    };
    if options.is_null() {
        return Ok(());
    }
    let options = options
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("prompt_data.options must be a JSON object"))?;

    let model = match options.get("model") {
        Some(Value::String(model)) if !model.trim().is_empty() => Some(model.trim()),
        Some(Value::String(_)) => bail!("model cannot be empty"),
        Some(Value::Null) | None => None,
        Some(_) => bail!("prompt_data.options.model must be a string"),
    };
    let params = match options.get("params") {
        Some(Value::Object(params)) => params,
        Some(Value::Null) | None => return Ok(()),
        Some(_) => bail!("prompt_data.options.params must be a JSON object"),
    };

    validate_model_params(model, params)
}

fn validate_model_params(model: Option<&str>, params: &Map<String, Value>) -> Result<()> {
    let temperature = optional_number(params, "temperature", "--temperature")?;
    if let Some(temperature) = temperature {
        let max = if model.is_some_and(uses_anthropic_temperature_range) {
            1.0
        } else {
            2.0
        };
        validate_number_range(temperature, 0.0, max, "--temperature")?;

        if let Some(model) = model {
            validate_temperature_support(model, params)?;
        }
    }

    if let Some(top_p) = optional_number(params, "top_p", "--top-p")? {
        validate_number_range(top_p, 0.0, 1.0, "--top-p")?;
        if let Some(model) = model.filter(|model| has_unsupported_opus_sampling_params(model)) {
            bail!("--top-p is not supported by model '{model}'");
        }
    }

    for (key, label) in [
        ("frequency_penalty", "--frequency-penalty"),
        ("presence_penalty", "--presence-penalty"),
    ] {
        if let Some(value) = optional_number(params, key, label)? {
            validate_number_range(value, -2.0, 2.0, label)?;
        }
    }

    if let Some(max_tokens) = params.get("max_tokens") {
        match max_tokens {
            Value::Null => {}
            Value::Number(number) if number.as_u64().is_some_and(|value| value > 0) => {}
            _ => bail!("--max-tokens must be a positive integer"),
        }
    }

    Ok(())
}

fn optional_number(params: &Map<String, Value>, key: &str, label: &str) -> Result<Option<f64>> {
    match params.get(key) {
        Some(Value::Null) | None => Ok(None),
        Some(Value::Number(number)) => number
            .as_f64()
            .filter(|value| value.is_finite())
            .map(Some)
            .ok_or_else(|| anyhow::anyhow!("{label} must be a finite number")),
        Some(_) => bail!("{label} must be a number"),
    }
}

fn validate_number_range(value: f64, min: f64, max: f64, label: &str) -> Result<()> {
    if !value.is_finite() || !(min..=max).contains(&value) {
        bail!("{label} must be between {min} and {max}");
    }
    Ok(())
}

/// Keep this in sync with `modelSupportsCustomTemperature` in the backend's
/// `typespecs/src/model-capabilities.ts`.
fn validate_temperature_support(model: &str, params: &Map<String, Value>) -> Result<()> {
    let lower = model.to_ascii_lowercase();

    if lower.contains("claude-opus-4-7") {
        bail!("--temperature is not supported by model '{model}'");
    }

    if lower.contains("gpt-5") {
        let has_no_reasoning_effort = params
            .get("reasoning_effort")
            .and_then(Value::as_str)
            .is_some_and(|effort| effort == "none");
        if !has_no_reasoning_effort {
            bail!(
                "--temperature is not supported by model '{model}' unless reasoning effort is 'none'; pass `--reasoning-effort none` or omit `--temperature`"
            );
        }
    } else if ["o1", "o2", "o3", "o4"]
        .iter()
        .any(|prefix| lower.starts_with(prefix))
    {
        bail!("--temperature is not supported by model '{model}'");
    }

    Ok(())
}

fn has_unsupported_opus_sampling_params(model: &str) -> bool {
    model.to_ascii_lowercase().contains("claude-opus-4-7")
}

fn uses_anthropic_temperature_range(model: &str) -> bool {
    let lower = model.to_ascii_lowercase();
    lower.contains("claude")
        || lower.starts_with("anthropic.")
        || lower.contains(".anthropic.")
        || lower.contains("/anthropic/")
}

fn insert_optional_number(
    target: &mut Map<String, Value>,
    key: &str,
    value: Option<f64>,
) -> Result<()> {
    if let Some(value) = value {
        insert_number(target, key, value, &format!("--{}", key.replace('_', "-")))?;
    }
    Ok(())
}

fn insert_number(
    target: &mut Map<String, Value>,
    key: &str,
    value: f64,
    label: &str,
) -> Result<()> {
    let number =
        Number::from_f64(value).ok_or_else(|| anyhow::anyhow!("{label} must be finite"))?;
    target.insert(key.to_string(), Value::Number(number));
    Ok(())
}

pub(crate) fn validate_unit_interval(value: f64, label: &str) -> Result<()> {
    if !value.is_finite() || !(0.0..=1.0).contains(&value) {
        bail!("{label} must be between 0 and 1");
    }
    Ok(())
}

pub(crate) fn parse_choice_scores_source(source: &str) -> Result<Map<String, Value>> {
    let raw = read_text_source(source, "choice scores")?;
    let value: Value = serde_json::from_str(&raw).context("invalid JSON in choice scores")?;
    let scores = match value {
        Value::Object(scores) => scores,
        _ => bail!("choice scores must be a JSON object mapping choices to numeric scores"),
    };
    if scores.is_empty() {
        bail!("choice scores cannot be empty");
    }
    for (choice, score) in &scores {
        if choice.trim().is_empty() {
            bail!("choice score labels cannot be empty");
        }
        let Some(score) = score.as_f64() else {
            bail!("score for choice '{choice}' must be a number");
        };
        validate_unit_interval(score, &format!("score for choice '{choice}'"))?;
    }
    Ok(scores)
}

pub(crate) fn parse_classifications_source(source: &str) -> Result<Vec<Value>> {
    let raw = read_text_source(source, "classifications")?;
    let value: Value = serde_json::from_str(&raw).context("invalid JSON in classifications")?;
    let choices = match value {
        Value::Array(choices) => choices,
        _ => bail!("classifications must be a JSON array of strings"),
    };
    if choices.is_empty() {
        bail!("classifications cannot be empty");
    }

    let mut seen = HashSet::new();
    choices
        .into_iter()
        .map(|choice| {
            let Value::String(choice) = choice else {
                bail!("every classification must be a string");
            };
            let choice = choice.trim();
            if choice.is_empty() {
                bail!("classifications cannot contain an empty label");
            }
            if !seen.insert(choice.to_string()) {
                bail!("classification labels must be unique; found '{choice}' more than once");
            }
            Ok(Value::String(choice.to_string()))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    struct Harness {
        #[command(flatten)]
        config: PromptConfigArgs,
    }

    #[test]
    fn builds_web_ui_compatible_prompt_configuration() {
        let args = Harness::try_parse_from([
            "test",
            "--temperature",
            "0.2",
            "--max-tokens",
            "512",
            "--top-p",
            "0.9",
            "--frequency-penalty",
            "-0.5",
            "--presence-penalty",
            "0.25",
            "--stop-sequence",
            "END",
            "--stop-sequence",
            "DONE",
            "--tool-choice",
            "test_tool",
            "--reasoning-effort",
            "high",
            "--verbosity",
            "low",
            "--template-format",
            "jinja",
        ])
        .expect("parse arguments");

        let patch = args
            .config
            .build_prompt_data_patch(Some("gpt-test"))
            .expect("prompt data");
        assert_eq!(patch["options"]["model"], "gpt-test");
        assert_eq!(patch["options"]["params"]["temperature"], 0.2);
        assert_eq!(patch["options"]["params"]["max_tokens"], 512);
        assert_eq!(patch["options"]["params"]["top_p"], 0.9);
        assert_eq!(patch["options"]["params"]["frequency_penalty"], -0.5);
        assert_eq!(patch["options"]["params"]["presence_penalty"], 0.25);
        assert_eq!(patch["options"]["params"]["stop"], json!(["END", "DONE"]));
        assert_eq!(
            patch["options"]["params"]["tool_choice"],
            json!({"type": "function", "function": {"name": "test_tool"}})
        );
        assert_eq!(patch["options"]["params"]["reasoning_effort"], "high");
        assert_eq!(patch["options"]["params"]["verbosity"], "low");
        assert_eq!(patch["template_format"], "nunjucks");
    }

    #[test]
    fn rejects_model_parameters_outside_provider_ranges() {
        for (arguments, expected) in [
            (
                vec!["--temperature", "99"],
                "--temperature must be between 0 and 2",
            ),
            (
                vec!["--max-tokens", "0"],
                "--max-tokens must be a positive integer",
            ),
            (
                vec!["--frequency-penalty", "-2.1"],
                "--frequency-penalty must be between -2 and 2",
            ),
            (
                vec!["--presence-penalty", "2.1"],
                "--presence-penalty must be between -2 and 2",
            ),
        ] {
            let parsed =
                Harness::try_parse_from(std::iter::once("test").chain(arguments.iter().copied()))
                    .expect("parse arguments");
            let error = parsed
                .config
                .build_prompt_data_patch(Some("gpt-4.1-mini"))
                .expect_err("out-of-range parameter should fail");
            assert_eq!(error.to_string(), expected);
        }
    }

    #[test]
    fn enforces_model_specific_temperature_support() {
        let parsed =
            Harness::try_parse_from(["test", "--temperature", "0.2"]).expect("parse arguments");

        for model in ["gpt-5.4-nano", "o3", "claude-opus-4-7"] {
            let error = parsed
                .config
                .build_prompt_data_patch(Some(model))
                .expect_err("unsupported temperature should fail");
            assert!(error.to_string().contains("not supported by model"));
        }
    }

    #[test]
    fn allows_gpt5_temperature_when_reasoning_effort_is_none() {
        let parsed =
            Harness::try_parse_from(["test", "--temperature", "0.2", "--reasoning-effort", "none"])
                .expect("parse arguments");

        let patch = parsed
            .config
            .build_prompt_data_patch(Some("gpt-5.4-nano"))
            .expect("compatible parameters");
        assert_eq!(patch["options"]["params"]["temperature"], 0.2);
        assert_eq!(patch["options"]["params"]["reasoning_effort"], "none");
    }

    #[test]
    fn validates_update_against_the_existing_model_and_params() {
        let existing = json!({
            "options": {
                "model": "gpt-5.4-nano",
                "params": { "reasoning_effort": "medium" }
            }
        });
        let patch = json!({
            "prompt_data": {
                "options": { "params": { "temperature": 0.2 } }
            }
        });
        let error = validate_prompt_data_patch(Some(&existing), &patch)
            .expect_err("effective model does not support temperature");
        assert!(error.to_string().contains("--reasoning-effort none"));

        let existing = json!({
            "options": {
                "model": "gpt-5.4-nano",
                "params": { "reasoning_effort": "none" }
            }
        });
        validate_prompt_data_patch(Some(&existing), &patch)
            .expect("existing reasoning effort makes temperature valid");
    }

    #[test]
    fn rejects_anthropic_temperature_above_one_in_arbitrary_patch() {
        let patch = json!({
            "prompt_data": {
                "options": {
                    "model": "claude-sonnet-4-5",
                    "params": { "temperature": 1.5 }
                }
            }
        });
        let error = validate_prompt_data_patch(None, &patch)
            .expect_err("Anthropic temperature should use the smaller range");
        assert_eq!(error.to_string(), "--temperature must be between 0 and 1");
    }

    #[test]
    fn ignores_unrelated_updates_to_existing_model_configuration() {
        let existing = json!({
            "options": {
                "model": "gpt-4.1-mini",
                "params": { "temperature": 99 }
            }
        });
        validate_prompt_data_patch(Some(&existing), &json!({"description": "Updated"}))
            .expect("an unrelated metadata update should remain possible");
    }

    #[test]
    fn validates_scores_against_api_range() {
        let error = parse_choice_scores_source(r#"{"bad":1.5}"#)
            .expect_err("out-of-range score should fail");
        assert!(error.to_string().contains("between 0 and 1"));
    }

    #[test]
    fn parses_unique_classification_labels() {
        let choices =
            parse_classifications_source(r#"["safe","unsafe"]"#).expect("classifications");
        assert_eq!(
            choices,
            json!(["safe", "unsafe"]).as_array().unwrap().clone()
        );
    }
}
