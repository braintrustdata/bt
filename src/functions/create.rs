use anyhow::{bail, Context, Result};
use clap::{builder::BoolishValueParser, ArgGroup, Args};
use dialoguer::Input;
use serde_json::{json, Map, Value};

use crate::{
    error::UserError,
    ui::{is_interactive, print_command_status, with_spinner, CommandStatus},
    utils::{merge_json_objects, read_text_source, read_yaml_object_source},
};

use super::{
    api,
    prompt_config::{
        parse_choice_scores_source, parse_classifications_source, validate_unit_interval,
        PromptConfigArgs,
    },
    IfExistsMode, ResolvedContext,
};

/// Create an LLM scorer or classifier.
///
/// The generated definition matches Braintrust's prompt-function schema with
/// an `llm_classifier` parser. `--choice-scores` produces numeric scores;
/// `--classifications` produces labels.
#[derive(Debug, Clone, Args)]
#[command(group(
    ArgGroup::new("output")
        .required(true)
        .multiple(false)
        .args(["choice_scores", "classifications"])
))]
#[command(after_help = "\
Examples:
  bt scorers create \"Helpfulness\" --model gpt-5.4-nano --messages @messages.json \\
    --choice-scores '{\"A\":1,\"B\":0}'
  bt scorers create \"Correctness\" --slug correctness --model gpt-5.4-nano \\
    --messages @messages.json \\
    --choice-scores '{\"correct\":1,\"incorrect\":0}' --use-cot=false
  bt scorers create \"Tone\" --model gpt-5.4-nano \\
    --messages @messages.json --choice-scores @scores.json
  bt scorers create \"Safety label\" --model gpt-5.4-nano --messages @messages.json \\
    --classifications '[\"safe\",\"unsafe\"]' --template-format jinja

TypeScript and Python code scorers:
  TypeScript: projects.create({ name: \"test-project\" }).scorers.create({...})
              bt functions push scorer.ts
  Python:     projects.create(\"test-project\").scorers.create(...)
              bt functions push scorer.py
")]
pub(crate) struct CreateArgs {
    /// Scorer name.
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Scorer name (named form).
    #[arg(long, value_name = "NAME")]
    name: Option<String>,

    /// Unique scorer slug. Defaults to a slug generated from the name.
    #[arg(long, short = 's')]
    slug: Option<String>,

    /// Scorer description.
    #[arg(long, short = 'd')]
    description: Option<String>,

    /// Chat messages source: inline JSON, @PATH to read from a file, or - for
    /// stdin.
    #[arg(long, value_name = "SOURCE")]
    messages: String,

    /// Model used by the LLM judge.
    #[arg(long, short = 'm', value_name = "MODEL")]
    model: String,

    #[command(flatten)]
    prompt_config: PromptConfigArgs,

    /// Choice-to-score mapping for score output: inline JSON, @PATH to read
    /// from a file, or - for stdin. Scores must be between 0 and 1.
    #[arg(long, value_name = "SOURCE")]
    choice_scores: Option<String>,

    /// Labels for classification output: an inline JSON array, @PATH to read
    /// from a file, or - for stdin. This creates an LLM classifier, which is
    /// shown alongside scorers in the Braintrust UI.
    #[arg(long, value_name = "SOURCE")]
    classifications: Option<String>,

    /// Allow a classifier to return no matching classification.
    #[arg(long, requires = "classifications")]
    allow_no_match: bool,

    /// Whether the scorer should use chain-of-thought reasoning. Defaults to
    /// true; pass --use-cot=false to disable it.
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "true",
        default_value_t = true,
        value_parser = BoolishValueParser::new()
    )]
    use_cot: bool,

    /// Score threshold for passing, between 0 and 1.
    #[arg(long, value_name = "NUMBER", conflicts_with = "classifications")]
    pass_threshold: Option<f64>,

    /// Metadata as inline YAML, @PATH to a YAML file, or - for stdin.
    #[arg(long, value_name = "SOURCE")]
    metadata: Option<String>,

    /// Behavior when a scorer with the same slug already exists.
    #[arg(long, value_enum, default_value = "error")]
    if_exists: IfExistsMode,
}

pub(crate) async fn run(ctx: &ResolvedContext, args: &CreateArgs, json_output: bool) -> Result<()> {
    let name = resolve_name(args).map_err(UserError::from)?;
    let slug = resolve_slug(args, &name).map_err(UserError::from)?;
    let definition =
        build_scorer_definition(args, &ctx.project.id, &name, &slug).map_err(UserError::from)?;
    let validation = with_spinner(
        "Validating scorer...",
        api::validate_functions(&ctx.client, std::slice::from_ref(&definition)),
    )
    .await?;
    report_validation_issues(&validation).map_err(UserError::from)?;

    let result = match with_spinner(
        "Creating scorer...",
        api::insert_functions(&ctx.client, std::slice::from_ref(&definition)),
    )
    .await
    {
        Ok(result) => result,
        Err(error) => {
            print_command_status(CommandStatus::Error, &format!("Failed to create '{name}'"));
            return Err(error);
        }
    };

    let ignored = result.ignored_entries.is_some_and(|count| count > 0);

    if json_output {
        let function = result
            .functions
            .first()
            .context("insert-functions response did not include the scorer identity")?;
        println!(
            "{}",
            serde_json::to_string(&json!({
                "id": function.id,
                "project_id": function.project_id,
                "slug": function.slug,
                "version": result.xact_id,
                "found_existing": function.found_existing,
                "ignored": ignored,
            }))?
        );
        return Ok(());
    }

    if ignored {
        print_command_status(
            CommandStatus::Warning,
            &format!("Scorer '{name}' already exists; left it unchanged"),
        );
    } else if args.if_exists == IfExistsMode::Replace {
        print_command_status(CommandStatus::Success, &format!("Saved '{name}'"));
    } else {
        print_command_status(CommandStatus::Success, &format!("Created '{name}'"));
    }

    Ok(())
}

fn report_validation_issues(report: &api::FunctionValidationReport) -> Result<()> {
    let mut blocking = Vec::new();
    for result in &report.results {
        for issue in &result.issues {
            let path = issue
                .path
                .iter()
                .map(|part| {
                    part.as_str()
                        .map(ToOwned::to_owned)
                        .unwrap_or_else(|| part.to_string())
                })
                .collect::<Vec<_>>()
                .join(".");
            let location = if path.is_empty() {
                issue.code.clone()
            } else {
                path
            };
            let suggestion = issue
                .suggestion
                .as_ref()
                .map(
                    |suggestion| match (suggestion.action.as_str(), &suggestion.value) {
                        ("remove", _) => "; suggestion: remove this parameter".to_string(),
                        ("set", Some(value)) => format!("; suggestion: set it to {value}"),
                        _ => String::new(),
                    },
                )
                .unwrap_or_default();
            let message = format!("{location}: {}{suggestion}", issue.message);
            if issue.blocking {
                blocking.push(message);
            } else {
                print_command_status(CommandStatus::Warning, &message);
            }
        }
    }
    if blocking.is_empty() && report.valid {
        Ok(())
    } else if blocking.is_empty() {
        bail!("the backend rejected the scorer definition")
    } else {
        bail!(blocking.join("; "))
    }
}

fn resolve_name(args: &CreateArgs) -> Result<String> {
    let name = match args.name_positional.as_deref().or(args.name.as_deref()) {
        Some(name) => name.trim().to_string(),
        None if is_interactive() => Input::<String>::new()
            .with_prompt("Scorer name")
            .interact_text()?
            .trim()
            .to_string(),
        None => bail!("scorer name required. Use: bt scorers create <name> ..."),
    };

    if name.is_empty() {
        bail!("scorer name cannot be empty");
    }
    Ok(name)
}

fn resolve_slug(args: &CreateArgs, name: &str) -> Result<String> {
    let slug = args
        .slug
        .as_deref()
        .map(str::trim)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| slugify(name));
    if slug.is_empty() {
        bail!("could not generate a slug from the scorer name; pass --slug explicitly");
    }
    Ok(slug)
}

fn slugify(value: &str) -> String {
    let mut slug = String::new();
    let mut pending_separator = false;

    for character in value.trim().chars() {
        if character.is_alphanumeric() {
            if pending_separator && !slug.is_empty() {
                slug.push('-');
            }
            slug.extend(character.to_lowercase());
            pending_separator = false;
        } else if !slug.is_empty() {
            pending_separator = true;
        }
    }

    slug
}

fn build_scorer_definition(
    args: &CreateArgs,
    project_id: &str,
    name: &str,
    slug: &str,
) -> Result<Value> {
    let prompt = resolve_prompt_block(args)?;
    let (function_type, parser) = resolve_output_parser(args)?;

    let mut prompt_data = json!({
        "prompt": prompt,
        "parser": parser,
    })
    .as_object()
    .expect("prompt data is an object")
    .clone();
    let prompt_config = args
        .prompt_config
        .build_prompt_data_patch(Some(&args.model))?;
    merge_json_objects(&mut prompt_data, &prompt_config);

    let mut definition = json!({
        "project_id": project_id,
        "name": name,
        "slug": slug,
        "function_data": {
            "type": "prompt",
        },
        "prompt_data": prompt_data,
        "if_exists": args.if_exists.as_str(),
        "function_type": function_type,
    });

    if let Some(description) = args.description.as_deref() {
        definition["description"] = Value::String(description.to_string());
    }

    let metadata = resolve_metadata(args)?;
    if !metadata.is_empty() {
        definition["metadata"] = Value::Object(metadata);
    }

    Ok(definition)
}

fn resolve_output_parser(args: &CreateArgs) -> Result<(&'static str, Value)> {
    match (
        args.choice_scores.as_deref(),
        args.classifications.as_deref(),
    ) {
        (Some(source), None) => Ok((
            "scorer",
            json!({
                "type": "llm_classifier",
                "use_cot": args.use_cot,
                "choice_scores": parse_choice_scores_source(source)?,
            }),
        )),
        (None, Some(source)) => Ok((
            "classifier",
            json!({
                "type": "llm_classifier",
                "use_cot": args.use_cot,
                "choice": parse_classifications_source(source)?,
                "allow_no_match": args.allow_no_match,
            }),
        )),
        (Some(_), Some(_)) => bail!(
            "use either --choice-scores for score output or --classifications for classification output, not both"
        ),
        (None, None) => bail!(
            "output choices required. Pass --choice-scores <SOURCE> or --classifications <SOURCE>"
        ),
    }
}

fn resolve_metadata(args: &CreateArgs) -> Result<Map<String, Value>> {
    let mut metadata = match args.metadata.as_deref() {
        Some(source) => read_yaml_object_source(source, "scorer metadata")?,
        None => Map::new(),
    };
    if let Some(pass_threshold) = args.pass_threshold {
        validate_unit_interval(pass_threshold, "--pass-threshold")?;
        metadata.insert("__pass_threshold".to_string(), json!(pass_threshold));
    }
    Ok(metadata)
}

fn resolve_prompt_block(args: &CreateArgs) -> Result<Value> {
    let raw = read_text_source(&args.messages, "messages")?;
    parse_messages(&raw)
}

fn parse_messages(raw: &str) -> Result<Value> {
    let messages: Value = serde_json::from_str(raw).context("invalid JSON in scorer messages")?;
    match messages {
        Value::Array(_) => Ok(json!({ "type": "chat", "messages": messages })),
        _ => bail!("scorer messages must be a JSON array"),
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    struct CreateArgsHarness {
        #[command(flatten)]
        args: CreateArgs,
    }

    fn args() -> CreateArgs {
        CreateArgs {
            name_positional: Some("Test Helpfulness".to_string()),
            name: None,
            slug: None,
            description: Some("Synthetic test scorer".to_string()),
            messages: r#"[{"role":"user","content":"Judge {{output}}."}]"#.to_string(),
            model: "gpt-test".to_string(),
            prompt_config: PromptConfigArgs::default(),
            choice_scores: Some(r#"{"A":1,"B":0}"#.to_string()),
            classifications: None,
            allow_no_match: false,
            use_cot: true,
            pass_threshold: None,
            metadata: None,
            if_exists: IfExistsMode::Error,
        }
    }

    #[test]
    fn use_cot_defaults_to_true() {
        let parsed = CreateArgsHarness::try_parse_from([
            "bt-scorers-create",
            "Test scorer",
            "--model",
            "gpt-test",
            "--messages",
            r#"[{"role":"user","content":"Judge {{output}}"}]"#,
            "--choice-scores",
            r#"{"yes":1,"no":0}"#,
        ])
        .expect("parse create args");

        assert!(parsed.args.use_cot);
    }

    #[test]
    fn builds_sdk_compatible_llm_scorer_definition() {
        let args = args();
        let body = build_scorer_definition(
            &args,
            "00000000-0000-0000-0000-000000000001",
            "Test Helpfulness",
            "test-helpfulness",
        )
        .expect("definition");

        assert_eq!(body["function_data"], json!({ "type": "prompt" }));
        assert_eq!(body["function_type"], "scorer");
        assert_eq!(body["prompt_data"]["prompt"]["type"], "chat");
        assert_eq!(body["prompt_data"]["options"]["model"], "gpt-test");
        assert_eq!(
            body["prompt_data"]["parser"],
            json!({
                "type": "llm_classifier",
                "use_cot": true,
                "choice_scores": { "A": 1, "B": 0 },
            })
        );
        assert_eq!(body["if_exists"], "error");
        assert_eq!(body["description"], "Synthetic test scorer");
    }

    #[test]
    fn rejects_non_array_messages() {
        let mut args = args();
        args.messages = r#"{"role":"user","content":"Judge {{output}}"}"#.to_string();

        let error = build_scorer_definition(&args, "test-project", "Test", "test")
            .expect_err("messages should be an array");
        assert!(error.to_string().contains("messages must be a JSON array"));
    }

    #[test]
    fn rejects_non_numeric_choice_score() {
        let mut args = args();
        args.choice_scores = Some(r#"{"A":"one"}"#.to_string());

        let error = build_scorer_definition(&args, "test-project", "Test", "test")
            .expect_err("string score should fail");
        assert!(error.to_string().contains("must be a number"));
    }

    #[test]
    fn supports_disabling_use_cot() {
        let mut args = args();
        args.use_cot = false;

        let body =
            build_scorer_definition(&args, "test-project", "Test", "test").expect("definition");
        assert_eq!(body["prompt_data"]["parser"]["use_cot"], false);
    }

    #[test]
    fn builds_classification_output() {
        let mut args = args();
        args.choice_scores = None;
        args.classifications = Some(r#"["safe","unsafe"]"#.to_string());
        args.allow_no_match = true;

        let body =
            build_scorer_definition(&args, "test-project", "Test", "test").expect("definition");

        assert_eq!(body["function_type"], "classifier");
        assert_eq!(
            body["prompt_data"]["parser"]["choice"],
            json!(["safe", "unsafe"])
        );
        assert_eq!(body["prompt_data"]["parser"]["allow_no_match"], true);
        assert!(body["prompt_data"]["parser"].get("choice_scores").is_none());
    }

    #[test]
    fn builds_metadata_and_pass_threshold() {
        let mut args = args();
        args.metadata = Some("owner: test-team".to_string());
        args.pass_threshold = Some(0.7);

        let body = build_scorer_definition(&args, "test-project", "Test scorer", "test-scorer")
            .expect("definition");

        assert_eq!(
            body["metadata"],
            json!({ "owner": "test-team", "__pass_threshold": 0.7 })
        );
    }

    #[test]
    fn builds_model_params_template_metadata_and_pass_threshold() {
        let parsed = CreateArgsHarness::try_parse_from([
            "bt-scorers-create",
            "Test scorer",
            "--model",
            "gpt-test",
            "--messages",
            r#"[{"role":"user","content":"Judge {{output}}"}]"#,
            "--choice-scores",
            r#"{"yes":1,"no":0}"#,
            "--temperature",
            "0.1",
            "--max-tokens",
            "256",
            "--top-p",
            "0.8",
            "--frequency-penalty",
            "0.25",
            "--presence-penalty",
            "0.5",
            "--stop-sequence",
            "END",
            "--tool-choice",
            "required",
            "--reasoning-effort",
            "medium",
            "--verbosity",
            "high",
            "--template-format",
            "jinja",
            "--pass-threshold",
            "0.7",
            "--metadata",
            "owner: test-team",
        ])
        .expect("parse create args");

        let body =
            build_scorer_definition(&parsed.args, "test-project", "Test scorer", "test-scorer")
                .expect("definition");
        let params = &body["prompt_data"]["options"]["params"];
        assert_eq!(params["temperature"], 0.1);
        assert_eq!(params["max_tokens"], 256);
        assert_eq!(params["top_p"], 0.8);
        assert_eq!(params["frequency_penalty"], 0.25);
        assert_eq!(params["presence_penalty"], 0.5);
        assert_eq!(params["stop"], json!(["END"]));
        assert_eq!(params["tool_choice"], "required");
        assert_eq!(params["reasoning_effort"], "medium");
        assert_eq!(params["verbosity"], "high");
        assert_eq!(body["prompt_data"]["template_format"], "nunjucks");
        assert_eq!(body["metadata"]["owner"], "test-team");
        assert_eq!(body["metadata"]["__pass_threshold"], 0.7);
    }

    #[test]
    fn positional_name_takes_precedence_over_named_form() {
        let parsed = CreateArgsHarness::try_parse_from([
            "bt-scorers-create",
            "Positional name",
            "--name",
            "Named form",
            "--model",
            "gpt-test",
            "--messages",
            r#"[{"role":"user","content":"Judge {{output}}"}]"#,
            "--choice-scores",
            r#"{"yes":1,"no":0}"#,
        ])
        .expect("parse both name forms");

        assert_eq!(
            resolve_name(&parsed.args).expect("resolve name"),
            "Positional name"
        );
    }

    #[test]
    fn slugify_normalizes_name() {
        assert_eq!(
            slugify("  Test Helpfulness / Judge  "),
            "test-helpfulness-judge"
        );
        assert_eq!(slugify("Already--Separated"), "already-separated");
    }
}
