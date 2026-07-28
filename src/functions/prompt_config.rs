use std::collections::HashSet;

use anyhow::{bail, Context, Result};
use clap::{Args, ValueEnum};
use serde_json::{json, Map, Number, Value};

use crate::{
    project_context::ProjectContext,
    utils::{merge_json_objects, read_text_source},
};

use super::model_capabilities::{resolve_model_spec, ModelSpec};

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

    /// Frequency penalty. Availability and range depend on the model.
    #[arg(long, value_name = "NUMBER", allow_hyphen_values = true)]
    frequency_penalty: Option<f64>,

    /// Presence penalty. Availability and range depend on the model.
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
        let changed_params = params.keys().cloned().collect();
        validate_model_params(effective_model, &params, &changed_params, None)?;

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
/// into an existing prompt definition. Model metadata comes from the same
/// catalog and custom-model configuration used by the web UI. If metadata is
/// unavailable for an arbitrary model name, validation deliberately falls back
/// to provider-independent type and range checks.
pub(crate) async fn validate_prompt_data_patch(
    ctx: &ProjectContext,
    existing_prompt_data: Option<&Value>,
    patch: &Value,
) -> Result<()> {
    let Some(update) = prepare_model_params_update(existing_prompt_data, patch)? else {
        return Ok(());
    };
    let spec = match update.model.as_deref() {
        Some(model) => resolve_model_spec(ctx, model).await,
        None => None,
    };
    validate_model_params(
        update.model.as_deref(),
        &update.params,
        &update.changed_params,
        spec.as_ref(),
    )
}

#[derive(Debug)]
struct ModelParamsUpdate {
    model: Option<String>,
    params: Map<String, Value>,
    changed_params: HashSet<String>,
}

fn prepare_model_params_update(
    existing_prompt_data: Option<&Value>,
    patch: &Value,
) -> Result<Option<ModelParamsUpdate>> {
    let Some(patch_prompt_data) = patch.get("prompt_data").and_then(Value::as_object) else {
        return Ok(None);
    };
    let Some(patch_options_value) = patch_prompt_data.get("options") else {
        return Ok(None);
    };
    if patch_options_value.is_null() {
        return Ok(None);
    }
    let patch_options = patch_options_value
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("prompt_data.options must be a JSON object"))?;

    let mut effective_prompt_data = existing_prompt_data
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    merge_json_objects(&mut effective_prompt_data, patch_prompt_data);

    let options = effective_prompt_data
        .get("options")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("prompt_data.options must be a JSON object"))?;
    let model = match options.get("model") {
        Some(Value::String(model)) if !model.trim().is_empty() => Some(model.trim().to_string()),
        Some(Value::String(_)) => bail!("model cannot be empty"),
        Some(Value::Null) | None => None,
        Some(_) => bail!("prompt_data.options.model must be a string"),
    };
    let params = match options.get("params") {
        Some(Value::Object(params)) => params.clone(),
        Some(Value::Null) | None => Map::new(),
        Some(_) => bail!("prompt_data.options.params must be a JSON object"),
    };

    let model_changed = patch_options.contains_key("model");
    let mut changed_params = if model_changed {
        params.keys().cloned().collect()
    } else {
        match patch_options.get("params") {
            Some(Value::Object(params)) => params.keys().cloned().collect(),
            Some(Value::Null) | None => HashSet::new(),
            Some(_) => bail!("prompt_data.options.params must be a JSON object"),
        }
    };

    // Temperature support can depend on reasoning effort, so changing either
    // side of that relationship must validate the effective temperature.
    if changed_params.contains("reasoning_effort") && params.contains_key("temperature") {
        changed_params.insert("temperature".to_string());
    }

    if changed_params.is_empty() {
        return Ok(None);
    }

    Ok(Some(ModelParamsUpdate {
        model,
        params,
        changed_params,
    }))
}

fn validate_model_params(
    model: Option<&str>,
    params: &Map<String, Value>,
    changed_params: &HashSet<String>,
    spec: Option<&ModelSpec>,
) -> Result<()> {
    if changed_params.contains("temperature") {
        ensure_parameter_supported(spec, model, "--temperature", TEMPERATURE_FORMATS)?;
        if let Some(temperature) = optional_number(params, "temperature", "--temperature")? {
            let max = match spec.map(|spec| spec.format.as_str()) {
                Some("anthropic" | "converse") => 1.0,
                _ => 2.0,
            };
            validate_number_range(temperature, 0.0, max, "--temperature")?;
            if let Some(model) = model {
                validate_temperature_support(model, params)?;
            }
        }
    }

    if changed_params.contains("top_p") {
        ensure_parameter_supported(spec, model, "--top-p", TOP_P_FORMATS)?;
        if let Some(top_p) = optional_number(params, "top_p", "--top-p")? {
            validate_number_range(top_p, 0.0, 1.0, "--top-p")?;
            if let Some(model) = model.filter(|model| has_unsupported_opus_sampling_params(model)) {
                bail!("--top-p is not supported by model '{model}'");
            }
        }
    }

    for (key, label) in [
        ("frequency_penalty", "--frequency-penalty"),
        ("presence_penalty", "--presence-penalty"),
    ] {
        if changed_params.contains(key) {
            ensure_parameter_supported(spec, model, label, PENALTY_FORMATS)?;
            if let Some(value) = optional_number(params, key, label)? {
                // The web UI exposes 0..=1. For an unknown/custom model whose
                // metadata could not be loaded, retain the provider API's
                // broader OpenAI-compatible range rather than guessing.
                let (min, max) = if spec.is_some_and(|spec| spec.format == "openai") {
                    (0.0, 1.0)
                } else {
                    (-2.0, 2.0)
                };
                validate_number_range(value, min, max, label)?;
            }
        }
    }

    if changed_params.contains("max_tokens") {
        ensure_parameter_supported(spec, model, "--max-tokens", MAX_TOKENS_FORMATS)?;
        if let Some(max_tokens) = params.get("max_tokens") {
            match max_tokens {
                Value::Null => {}
                Value::Number(number) if number.as_u64().is_some_and(|value| value > 0) => {
                    if let (Some(spec), Some(value)) = (spec, number.as_u64()) {
                        let max = spec
                            .max_output_tokens
                            .filter(|max| *max > 0)
                            .unwrap_or(32_768);
                        if value > max {
                            bail!(
                                "--max-tokens must be between 1 and {max} for model '{}'",
                                model.unwrap_or("<unknown>")
                            );
                        }
                    }
                }
                _ => bail!("--max-tokens must be a positive integer"),
            }
        }
    }

    if changed_params.contains("stop") {
        match params.get("stop") {
            Some(Value::Null) | None => {}
            Some(Value::Array(values)) if values.iter().all(Value::is_string) => {}
            _ => bail!("--stop-sequence values must be strings"),
        }
    }

    if changed_params.contains("tool_choice") {
        if let Some(value) = params.get("tool_choice").filter(|value| !value.is_null()) {
            ensure_parameter_supported(spec, model, "--tool-choice", TOOL_FORMATS)?;
            validate_tool_choice(value)?;
        }
    }

    if changed_params.contains("reasoning_effort") {
        validate_reasoning_effort(model, params, spec)?;
    }

    if changed_params.contains("verbosity") {
        if let Some(value) = params.get("verbosity").filter(|value| !value.is_null()) {
            let verbosity = value
                .as_str()
                .ok_or_else(|| anyhow::anyhow!("--verbosity must be a string"))?;
            if !["low", "medium", "high"].contains(&verbosity) {
                bail!("--verbosity must be one of low, medium, high");
            }
            if let (Some(model), Some(spec)) = (model, spec) {
                let display_name = spec.display_name.as_deref().unwrap_or(model);
                if !display_name.to_ascii_lowercase().contains("gpt-5") {
                    bail!("--verbosity is not supported by model '{model}'");
                }
            }
        }
    }

    Ok(())
}

// Keep these in sync with `defaultModelParamSettings`, `getSliderSpecs`, and
// `modelProviderHasTools` in `proxy/packages/proxy/schema/index.ts`.
const KNOWN_FORMATS: &[&str] = &["openai", "anthropic", "google", "js", "window", "converse"];
const TEMPERATURE_FORMATS: &[&str] = &["openai", "anthropic", "google", "window", "converse"];
const MAX_TOKENS_FORMATS: &[&str] = &["openai", "anthropic", "google", "converse"];
const TOP_P_FORMATS: &[&str] = &["openai", "anthropic", "google", "converse"];
const PENALTY_FORMATS: &[&str] = &["openai"];
const TOOL_FORMATS: &[&str] = &["openai", "anthropic", "google", "converse"];

fn ensure_parameter_supported(
    spec: Option<&ModelSpec>,
    model: Option<&str>,
    label: &str,
    supported_formats: &[&str],
) -> Result<()> {
    let Some(spec) = spec else {
        return Ok(());
    };
    if KNOWN_FORMATS.contains(&spec.format.as_str())
        && !supported_formats.contains(&spec.format.as_str())
    {
        bail!(
            "{label} is not supported by model '{}' (format: {})",
            model.unwrap_or("<unknown>"),
            spec.format,
        );
    }
    Ok(())
}

fn validate_tool_choice(value: &Value) -> Result<()> {
    match value {
        Value::String(choice) if ["auto", "none", "required"].contains(&choice.as_str()) => Ok(()),
        Value::Object(choice)
            if choice.get("type").and_then(Value::as_str) == Some("function")
                && choice
                    .get("function")
                    .and_then(Value::as_object)
                    .and_then(|function| function.get("name"))
                    .and_then(Value::as_str)
                    .is_some_and(|name| !name.trim().is_empty()) =>
        {
            Ok(())
        }
        _ => bail!("--tool-choice must be auto, none, required, or a non-empty function name"),
    }
}

fn validate_reasoning_effort(
    model: Option<&str>,
    params: &Map<String, Value>,
    spec: Option<&ModelSpec>,
) -> Result<()> {
    let Some(effort) = params.get("reasoning_effort") else {
        return Ok(());
    };
    if effort.is_null() {
        return Ok(());
    }
    let effort = effort
        .as_str()
        .ok_or_else(|| anyhow::anyhow!("--reasoning-effort must be a string"))?;
    let (Some(model), Some(spec)) = (model, spec) else {
        return Ok(());
    };
    if !KNOWN_FORMATS.contains(&spec.format.as_str()) {
        return Ok(());
    }
    if !spec.supports_reasoning(model) {
        bail!("--reasoning-effort is not supported by model '{model}'");
    }

    let gemini_thinking_level = spec.format == "google" && is_gemini_3_model(model);
    if spec.format != "openai" && spec.reasoning_budget.unwrap_or(false) && !gemini_thinking_level {
        bail!(
            "--reasoning-effort is not supported by model '{model}'; this model uses a reasoning budget"
        );
    }

    let options: &[&str] = if is_gpt_5_pro_model(model) {
        &["high"]
    } else if is_gpt_5_1_or_later(model) {
        &["none", "low", "medium", "high"]
    } else if is_gpt_5_model(model) || gemini_thinking_level {
        &["minimal", "low", "medium", "high"]
    } else {
        &["low", "medium", "high"]
    };
    if !options.contains(&effort) {
        bail!(
            "--reasoning-effort must be one of {} for model '{model}'",
            options.join(", ")
        );
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

fn is_gpt_5_pro_model(model: &str) -> bool {
    model.to_ascii_lowercase().contains("gpt-5-pro")
}

fn is_gpt_5_1_or_later(model: &str) -> bool {
    let lower = model.to_ascii_lowercase();
    let Some(start) = lower.find("gpt-5.") else {
        return false;
    };
    let version = lower[start + "gpt-5.".len()..]
        .chars()
        .take_while(char::is_ascii_digit)
        .collect::<String>();
    version.parse::<u64>().is_ok_and(|version| version >= 1)
}

fn is_gpt_5_model(model: &str) -> bool {
    model.to_ascii_lowercase().contains("gpt-5")
        && !is_gpt_5_1_or_later(model)
        && !is_gpt_5_pro_model(model)
}

fn is_gemini_3_model(model: &str) -> bool {
    let lower = model.to_ascii_lowercase();
    lower.starts_with("gemini-3") || lower.contains("/gemini-3")
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

    fn model_spec(
        format: &str,
        reasoning: bool,
        reasoning_budget: bool,
        max_output_tokens: Option<u64>,
    ) -> ModelSpec {
        ModelSpec {
            format: format.to_string(),
            _flavor: "chat".to_string(),
            display_name: None,
            o1_like: None,
            reasoning: Some(reasoning),
            reasoning_budget: Some(reasoning_budget),
            max_output_tokens,
        }
    }

    fn validate_patch(
        existing_prompt_data: Option<&Value>,
        patch: &Value,
        spec: Option<&ModelSpec>,
    ) -> Result<()> {
        let Some(update) = prepare_model_params_update(existing_prompt_data, patch)? else {
            return Ok(());
        };
        validate_model_params(
            update.model.as_deref(),
            &update.params,
            &update.changed_params,
            spec,
        )
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
        let error = validate_patch(Some(&existing), &patch, None)
            .expect_err("effective model does not support temperature");
        assert!(error.to_string().contains("--reasoning-effort none"));

        let existing = json!({
            "options": {
                "model": "gpt-5.4-nano",
                "params": { "reasoning_effort": "none" }
            }
        });
        validate_patch(Some(&existing), &patch, None)
            .expect("existing reasoning effort makes temperature valid");
    }

    #[test]
    fn applies_format_ranges_without_model_name_heuristics() {
        let patch = json!({
            "prompt_data": {
                "options": {
                    "model": "test-custom-model",
                    "params": { "temperature": 1.5 }
                }
            }
        });
        let anthropic = model_spec("anthropic", false, false, None);
        let error = validate_patch(None, &patch, Some(&anthropic))
            .expect_err("Anthropic temperature should use the smaller range");
        assert_eq!(error.to_string(), "--temperature must be between 0 and 1");

        validate_patch(None, &patch, None)
            .expect("unknown custom models should not be assigned a format by name");
    }

    #[test]
    fn applies_web_ui_parameter_availability_and_model_token_limit() {
        let window = model_spec("window", false, false, None);
        let tool_patch = json!({
            "prompt_data": {
                "options": {
                    "model": "test-window-model",
                    "params": { "tool_choice": "auto" }
                }
            }
        });
        let error = validate_patch(None, &tool_patch, Some(&window))
            .expect_err("Window models do not expose tool choice in the UI");
        assert!(error.to_string().contains("--tool-choice is not supported"));

        let openai = model_spec("openai", false, false, Some(4096));
        let token_patch = json!({
            "prompt_data": {
                "options": {
                    "model": "test-limited-model",
                    "params": { "max_tokens": 4097 }
                }
            }
        });
        let error = validate_patch(None, &token_patch, Some(&openai))
            .expect_err("model output token limit should be enforced");
        assert!(error.to_string().contains("between 1 and 4096"));
    }

    #[test]
    fn applies_web_ui_reasoning_options() {
        let reasoning = model_spec("openai", true, false, None);
        let invalid = json!({
            "prompt_data": {
                "options": {
                    "model": "o3",
                    "params": { "reasoning_effort": "minimal" }
                }
            }
        });
        let error = validate_patch(None, &invalid, Some(&reasoning))
            .expect_err("generic reasoning models accept low, medium, or high");
        assert!(error.to_string().contains("low, medium, high"));

        let non_reasoning = model_spec("openai", false, false, None);
        let non_reasoning_patch = json!({
            "prompt_data": {
                "options": {
                    "model": "gpt-4.1",
                    "params": { "reasoning_effort": "low" }
                }
            }
        });
        let error = validate_patch(None, &non_reasoning_patch, Some(&non_reasoning))
            .expect_err("non-reasoning models should reject reasoning effort");
        assert!(error.to_string().contains("not supported"));
    }

    #[test]
    fn validates_existing_parameters_when_switching_models() {
        let existing = json!({
            "options": {
                "model": "gpt-4.1",
                "params": { "temperature": 0.5 }
            }
        });
        let patch = json!({
            "prompt_data": {
                "options": { "model": "o3" }
            }
        });
        let error = validate_patch(
            Some(&existing),
            &patch,
            Some(&model_spec("openai", true, false, None)),
        )
        .expect_err("switching models must validate retained parameters");
        assert!(error.to_string().contains("--temperature is not supported"));
    }

    #[test]
    fn validates_only_parameters_touched_by_an_update() {
        let existing = json!({
            "options": {
                "model": "test-model",
                "params": { "temperature": 99 }
            }
        });
        validate_patch(
            Some(&existing),
            &json!({"prompt_data": {"options": {"params": {"top_p": 0.5}}}}),
            Some(&model_spec("openai", false, false, None)),
        )
        .expect("an unrelated stale parameter should not block an update");

        validate_patch(Some(&existing), &json!({"description": "Updated"}), None)
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
