use std::collections::HashSet;

use anyhow::{bail, Context, Result};
use clap::{builder::BoolishValueParser, Args, ValueEnum};
use serde_json::{json, Map, Number, Value};

use crate::utils::read_text_source;

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

    /// Top-k sampling value, between 1 and 100. Availability depends on the
    /// model provider.
    #[arg(long, value_name = "N")]
    top_k: Option<u64>,

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

    /// Enable reasoning for models that configure reasoning with a token
    /// budget rather than an effort level.
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = BoolishValueParser::new()
    )]
    reasoning_enabled: Option<bool>,

    /// Reasoning token budget, between 0 and 32768, for supported models.
    #[arg(long, value_name = "N")]
    reasoning_budget: Option<u64>,

    /// Response verbosity for supported models.
    #[arg(long, value_enum)]
    verbosity: Option<Verbosity>,

    /// Whether to use Braintrust's completion cache. Pass --use-cache=false to
    /// bypass it.
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = BoolishValueParser::new()
    )]
    use_cache: Option<bool>,

    /// Model response format: text, json-object, or a response_format JSON
    /// object supplied inline, from @PATH, or from stdin with -.
    #[arg(long, value_name = "FORMAT|SOURCE")]
    response_format: Option<String>,

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
        insert_optional_number(&mut params, "top_p", self.top_p)?;
        if let Some(top_k) = self.top_k {
            params.insert("top_k".to_string(), Value::Number(top_k.into()));
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
        if let Some(reasoning_enabled) = self.reasoning_enabled {
            params.insert("reasoning_enabled".to_string(), reasoning_enabled.into());
        }
        if let Some(reasoning_budget) = self.reasoning_budget {
            params.insert(
                "reasoning_budget".to_string(),
                Value::Number(reasoning_budget.into()),
            );
        }
        if let Some(verbosity) = self.verbosity {
            params.insert(
                "verbosity".to_string(),
                Value::String(verbosity.as_str().to_string()),
            );
        }
        if let Some(use_cache) = self.use_cache {
            params.insert("use_cache".to_string(), Value::Bool(use_cache));
        }
        if let Some(source) = self.response_format.as_deref() {
            params.insert(
                "response_format".to_string(),
                parse_response_format_source(source)?,
            );
        }

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

fn parse_response_format_source(source: &str) -> Result<Value> {
    let value = match source {
        "text" => json!({ "type": "text" }),
        "json-object" | "json_object" => json!({ "type": "json_object" }),
        _ => {
            let raw = read_text_source(source, "response format")?;
            serde_json::from_str(&raw)
                .context("invalid response format; use text, json-object, or a JSON object")?
        }
    };

    let Some(format) = value.as_object() else {
        bail!("response format must be a JSON object");
    };
    match format.get("type").and_then(Value::as_str) {
        Some("text" | "json_object") => {}
        Some("json_schema") => validate_json_schema_response_format(format)?,
        Some(other) => bail!(
            "unsupported response format type '{other}'; expected text, json_object, or json_schema"
        ),
        None => bail!("response format must contain a string 'type' field"),
    }

    Ok(value)
}

fn validate_json_schema_response_format(format: &Map<String, Value>) -> Result<()> {
    let Some(schema) = format.get("json_schema").and_then(Value::as_object) else {
        bail!("json_schema response format must contain a 'json_schema' object");
    };
    match schema.get("name") {
        Some(Value::String(_)) => {}
        _ => bail!("json_schema response format must contain a string 'json_schema.name' field"),
    }
    if let Some(value) = schema.get("description") {
        if !value.is_string() {
            bail!("response format 'json_schema.description' must be a string");
        }
    }
    if let Some(value) = schema.get("schema") {
        if !value.is_object() && !value.is_string() {
            bail!("response format 'json_schema.schema' must be an object or template string");
        }
    }
    if let Some(value) = schema.get("strict") {
        if !value.is_boolean() && !value.is_null() {
            bail!("response format 'json_schema.strict' must be a boolean or null");
        }
    }
    Ok(())
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
            "0.5",
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
            "--use-cache=false",
            "--response-format",
            r#"{"type":"json_schema","json_schema":{"name":"test_result","schema":{"type":"object"},"strict":true}}"#,
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
        assert_eq!(patch["options"]["params"]["frequency_penalty"], 0.5);
        assert_eq!(patch["options"]["params"]["presence_penalty"], 0.25);
        assert_eq!(patch["options"]["params"]["stop"], json!(["END", "DONE"]));
        assert_eq!(
            patch["options"]["params"]["tool_choice"],
            json!({"type": "function", "function": {"name": "test_tool"}})
        );
        assert_eq!(patch["options"]["params"]["reasoning_effort"], "high");
        assert_eq!(patch["options"]["params"]["verbosity"], "low");
        assert_eq!(patch["options"]["params"]["use_cache"], false);
        assert_eq!(
            patch["options"]["params"]["response_format"],
            json!({
                "type": "json_schema",
                "json_schema": {
                    "name": "test_result",
                    "schema": { "type": "object" },
                    "strict": true,
                }
            })
        );
        assert_eq!(patch["template_format"], "nunjucks");
    }

    #[test]
    fn sends_cache_and_temperature_values_without_client_normalization() {
        let args = Harness::try_parse_from(["test", "--temperature", "0.5", "--use-cache=true"])
            .expect("parse arguments");

        let patch = args
            .config
            .build_prompt_data_patch(Some("test-model"))
            .expect("prompt data");
        assert_eq!(patch["options"]["params"]["temperature"], 0.5);
        assert_eq!(patch["options"]["params"]["use_cache"], true);
    }

    #[test]
    fn supports_response_format_shorthands() {
        assert_eq!(
            parse_response_format_source("text").expect("text format"),
            json!({ "type": "text" })
        );
        assert_eq!(
            parse_response_format_source("json-object").expect("JSON object format"),
            json!({ "type": "json_object" })
        );
    }

    #[test]
    fn rejects_invalid_json_schema_response_format() {
        let error = parse_response_format_source(r#"{"type":"json_schema"}"#)
            .expect_err("missing json_schema should fail");
        assert!(error.to_string().contains("'json_schema' object"));
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
