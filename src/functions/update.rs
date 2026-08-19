use anyhow::{anyhow, bail, Context, Result};
use clap::{builder::BoolishValueParser, Args};
use dialoguer::Confirm;
use serde_json::{json, Map, Value};

use crate::{
    error::user_error,
    ui::{is_interactive, print_command_status, with_spinner, CommandStatus},
    utils::{merge_json_objects, read_text_source},
};

use super::{
    api, create::report_validation_issues, label, label_plural, select_function_interactive,
};
use super::{
    prompt_config::PromptConfigArgs,
    prompt_patch::materialize_prompt_data_patch,
    scorer_config::{build_scorer_config, ScorerConfig},
    FunctionTypeFilter, ResolvedContext,
};

/// Update a function's prompt configuration or metadata in place.
///
/// This wraps `PATCH /v1/function/{id}`. The endpoint replaces `prompt_data`
/// wholesale, so the command reads the current definition and materializes a
/// complete replacement while changing only the fields requested by the user.
#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt scorers update my-scorer --messages @messages.json
  bt scorers update my-scorer --model gpt-5.4-nano --reasoning-effort none --temperature 0.1
  bt scorers update my-scorer --template-format jinja --pass-threshold 0.7
  bt scorers update my-scorer --classifications '[\"safe\",\"unsafe\"]'
  bt scorers update my-scorer --metadata @metadata.yaml
  bt scorers update my-scorer --description \"Helpfulness judge\"
  bt scorers update my-scorer --patch '{\"prompt_data\":{\"options\":{\"model\":\"gpt-5.4-nano\"}}}'
  bt scorers update --id fn_123 --patch @scorer-patch.json
  bt tools update my-tool --patch @tool-patch.json
")]
pub struct UpdateArgs {
    #[command(flatten)]
    slug: super::SlugArgs,

    /// Function id (alternative to slug). Auto-detected for `fn_`/`func_` prefixes.
    #[arg(long = "id")]
    id: Option<String>,

    /// Replacement chat messages source: inline JSON, @PATH to read from a
    /// file, or - for stdin.
    #[arg(long, value_name = "SOURCE")]
    messages: Option<String>,

    /// Update the model used by an LLM scorer/prompt.
    #[arg(long, short = 'm', value_name = "MODEL")]
    model: Option<String>,

    #[command(flatten)]
    prompt_config: PromptConfigArgs,

    /// Replace choice-to-score mappings for score output. Accepts inline JSON,
    /// @PATH to read from a file, or - for stdin.
    #[arg(long, value_name = "SOURCE", conflicts_with = "classifications")]
    choice_scores: Option<String>,

    /// Replace labels for classification output. Accepts an inline JSON array,
    /// @PATH to read from a file, or - for stdin.
    #[arg(long, value_name = "SOURCE", conflicts_with = "choice_scores")]
    classifications: Option<String>,

    /// Update chain-of-thought reasoning. Pass --use-cot=false to disable it.
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = BoolishValueParser::new()
    )]
    use_cot: Option<bool>,

    /// Update whether a classifier may return no matching classification.
    #[arg(
        long,
        num_args = 0..=1,
        default_missing_value = "true",
        value_parser = BoolishValueParser::new()
    )]
    allow_no_match: Option<bool>,

    /// Update the score threshold for passing, between 0 and 1.
    #[arg(long, value_name = "NUMBER", conflicts_with = "classifications")]
    pass_threshold: Option<f64>,

    /// Deep-merge metadata from inline YAML, @PATH, or stdin (-).
    #[arg(long, value_name = "SOURCE")]
    metadata: Option<String>,

    /// Update the function description.
    #[arg(long, short = 'd', value_name = "TEXT")]
    description: Option<String>,

    /// Arbitrary JSON object deep-merged into the function. Accepts inline
    /// JSON, @PATH to read JSON from a file, or - for stdin.
    #[arg(long, value_name = "SOURCE")]
    patch: Option<String>,

    /// Skip the confirmation prompt.
    #[arg(long, short = 'y')]
    yes: bool,
}

impl UpdateArgs {
    /// Flags that only make sense for LLM scorers and classifiers.
    ///
    /// Returns the flag names that were set so callers can reject them on other
    /// function kinds (for example tools) with an actionable message.
    fn scorer_output_flags(&self) -> Vec<&'static str> {
        let mut flags = Vec::new();
        if self.choice_scores.is_some() {
            flags.push("--choice-scores");
        }
        if self.classifications.is_some() {
            flags.push("--classifications");
        }
        if self.allow_no_match.is_some() {
            flags.push("--allow-no-match");
        }
        if self.use_cot.is_some() {
            flags.push("--use-cot");
        }
        if self.pass_threshold.is_some() {
            flags.push("--pass-threshold");
        }
        flags
    }

    fn selector(&self) -> Result<UpdateSelector<'_>> {
        match (
            self.id.as_deref(),
            self.slug.slug_positional(),
            self.slug.slug_flag(),
        ) {
            (Some(_), Some(_), _) | (Some(_), _, Some(_)) => {
                bail!("use either --id or a slug, not both")
            }
            (Some(id), None, None) => Ok(UpdateSelector::Id(id)),
            (None, Some(positional), None) if super::is_likely_function_id(positional) => {
                Ok(UpdateSelector::Id(positional))
            }
            (None, positional, flag) => Ok(UpdateSelector::Slug(positional.or(flag))),
        }
    }
}

#[derive(Debug)]
enum UpdateSelector<'a> {
    Id(&'a str),
    Slug(Option<&'a str>),
}

fn validation_candidate(function: &api::Function, patch: &Value) -> Value {
    let mut candidate = json!({
        "project_id": function.project_id,
        "name": function.name,
        "slug": function.slug,
    });
    let object = candidate
        .as_object_mut()
        .expect("validation candidate is an object");

    for (key, value) in [
        (
            "description",
            function.description.as_ref().map(|v| json!(v)),
        ),
        (
            "function_type",
            function.function_type.as_ref().map(|v| json!(v)),
        ),
        ("prompt_data", function.prompt_data.clone()),
        ("function_data", function.function_data.clone()),
        ("tags", function.tags.as_ref().map(|v| json!(v))),
        ("metadata", function.metadata.clone()),
    ] {
        if let Some(value) = value {
            object.insert(key.to_string(), value);
        }
    }

    // PATCH replaces each top-level value. `prompt_data` has already been
    // materialized into a complete value before this helper is called.
    for (key, value) in patch
        .as_object()
        .expect("function update patch is an object")
    {
        object.insert(key.clone(), value.clone());
    }
    candidate
}

pub async fn run(
    ctx: &ResolvedContext,
    args: &UpdateArgs,
    json_output: bool,
    ft: Option<FunctionTypeFilter>,
) -> Result<()> {
    let mut body = build_patch_body(args)?;

    let function = resolve_target_function(ctx, args, ft).await?;

    // LLM scorer/classifier output flags only apply to prompt-based scorers and
    // classifiers. Reject them on other function kinds (for example tools) so an
    // unrelated function is not silently patched with a parser it cannot use.
    let is_scorer_like = matches!(
        function.function_type.as_deref(),
        Some("scorer") | Some("classifier")
    );
    let scorer_flags = args.scorer_output_flags();
    if !scorer_flags.is_empty() && !is_scorer_like {
        bail!(
            "{} apply to LLM scorers and classifiers, not {} '{}'. \
             Run `bt scorers update` on a scorer instead.",
            scorer_flags.join(", "),
            label(ft),
            function.name,
        );
    }

    // Mirrors `create`, where --allow-no-match requires --classifications: a
    // score parser would never consult it.
    let produces_classifications =
        args.classifications.is_some() || function.function_type.as_deref() == Some("classifier");
    if args.allow_no_match.is_some() && !produces_classifications {
        bail!(
            "--allow-no-match applies to classification output, but '{}' produces scores. \
             Pass --classifications to switch it to labels.",
            function.name,
        );
    }

    materialize_prompt_data_patch(&mut body, function.prompt_data.as_ref());

    // Last of the up-front checks because it hits the network. Validate the
    // complete candidate rather than the partial PATCH body so the backend can
    // apply the same model-parameter checks as it does for scorer creation.
    let candidate = validation_candidate(&function, &body);
    let validation = with_spinner(
        "Validating function...",
        api::validate_functions(&ctx.client, std::slice::from_ref(&candidate)),
    )
    .await?;
    report_validation_issues(&validation).map_err(user_error)?;

    // Switching output mode updates function_type, but materialization merges
    // the parser and does not drop the previous mode's keys. Warn so the
    // user can review or recreate for a clean switch.
    if !crate::ui::is_quiet() {
        match function.function_type.as_deref() {
            Some("classifier") if args.choice_scores.is_some() => print_command_status(
                CommandStatus::Warning,
                "Switching to score output; previous classification labels may remain in the definition. Review with `bt scorers view`.",
            ),
            Some("scorer") if args.classifications.is_some() => print_command_status(
                CommandStatus::Warning,
                "Switching to classification output; previous choice scores may remain in the definition. Review with `bt scorers view`.",
            ),
            _ => {}
        }
    }

    if !args.yes && is_interactive() {
        let confirm = Confirm::new()
            .with_prompt(format!(
                "Update {} '{}' in {}?",
                label(ft),
                function.name,
                ctx.project.name
            ))
            .default(false)
            .interact()?;
        if !confirm {
            return Ok(());
        }
    }

    let updated = match with_spinner(
        &format!("Updating {}...", label(ft)),
        api::patch_function(&ctx.client, &function.id, &body),
    )
    .await
    {
        Ok(value) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Updated '{}'", function.name),
            );
            value
        }
        Err(error) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Failed to update '{}'", function.name),
            );
            return Err(error);
        }
    };

    if json_output {
        println!("{}", serde_json::to_string(&updated)?);
    } else if !crate::ui::is_quiet() {
        eprintln!(
            "Run `bt {} view {}` to inspect the updated definition.",
            label_plural(ft),
            function.slug
        );
    }

    Ok(())
}

async fn resolve_target_function(
    ctx: &ResolvedContext,
    args: &UpdateArgs,
    ft: Option<FunctionTypeFilter>,
) -> Result<api::Function> {
    let project_id = &ctx.project.id;
    match args.selector()? {
        UpdateSelector::Id(id) => api::get_function_by_id(&ctx.client, id, None)
            .await?
            .ok_or_else(|| anyhow!("{} with id '{id}' not found", label(ft))),
        UpdateSelector::Slug(Some(slug)) => {
            api::get_function_by_slug(&ctx.client, project_id, slug, None)
                .await?
                .ok_or_else(|| anyhow!("{} with slug '{slug}' not found", label(ft)))
        }
        UpdateSelector::Slug(None) => {
            if !is_interactive() {
                bail!(
                    "{} slug or --id required. Use: bt {} update <slug> [--patch ...]",
                    label(ft),
                    label_plural(ft),
                );
            }
            Ok(select_function_interactive(&ctx.client, project_id, ft).await?)
        }
    }
}

fn build_patch_body(args: &UpdateArgs) -> Result<Value> {
    let mut patch = build_scorer_config(
        &ScorerConfig {
            messages: args.messages.as_deref(),
            model: args.model.as_deref(),
            prompt_config: &args.prompt_config,
            choice_scores: args.choice_scores.as_deref(),
            classifications: args.classifications.as_deref(),
            use_cot: args.use_cot,
            allow_no_match: args.allow_no_match,
            pass_threshold: args.pass_threshold,
            metadata: args.metadata.as_deref(),
            metadata_label: "function metadata",
        },
        false,
    )?;

    if let Some(description) = args.description.as_deref() {
        patch.insert(
            "description".to_string(),
            Value::String(description.to_string()),
        );
    }

    if let Some(extra_obj) = resolve_extra_patch(args)? {
        merge_json_objects(&mut patch, &extra_obj);
    }

    if patch.is_empty() {
        bail!("no updates requested. Pass an update flag; see `bt scorers update --help`");
    }

    Ok(Value::Object(patch))
}

fn resolve_extra_patch(args: &UpdateArgs) -> Result<Option<Map<String, Value>>> {
    let Some(source) = args.patch.as_deref() else {
        return Ok(None);
    };
    let raw = read_text_source(source, "patch")?;
    parse_patch_object(&raw).map(Some)
}

fn parse_patch_object(raw: &str) -> Result<Map<String, Value>> {
    let value: Value = serde_json::from_str(raw).context("invalid JSON in --patch")?;
    match value {
        Value::Object(map) => Ok(map),
        _ => bail!("--patch must be a JSON object"),
    }
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    struct UpdateArgsHarness {
        #[command(flatten)]
        args: UpdateArgs,
    }

    fn args(model: Option<&str>, description: Option<&str>) -> UpdateArgs {
        UpdateArgs {
            slug: super::super::SlugArgs {
                slug_positional: Some("test-slug".to_string()),
                slug_flag: None,
            },
            id: None,
            messages: None,
            model: model.map(ToOwned::to_owned),
            prompt_config: PromptConfigArgs::default(),
            choice_scores: None,
            classifications: None,
            use_cot: None,
            allow_no_match: None,
            pass_threshold: None,
            metadata: None,
            description: description.map(ToOwned::to_owned),
            patch: None,
            yes: true,
        }
    }

    #[test]
    fn validation_candidate_applies_top_level_patch_to_existing_function() {
        let function = api::Function {
            id: "fn_test_update".to_string(),
            name: "Test scorer".to_string(),
            slug: "test-scorer".to_string(),
            project_id: "test-project".to_string(),
            description: Some("Old description".to_string()),
            function_type: Some("scorer".to_string()),
            prompt_data: Some(json!({"options": {"model": "test-model"}})),
            function_data: Some(json!({"type": "prompt"})),
            tags: Some(vec!["test-tag".to_string()]),
            metadata: Some(json!({"owner": "test-user"})),
            created: None,
            _xact_id: None,
        };
        let patch = json!({
            "description": "New description",
            "prompt_data": {"options": {"model": "test-model-2"}}
        });

        let candidate = validation_candidate(&function, &patch);

        assert_eq!(candidate["project_id"], "test-project");
        assert_eq!(candidate["description"], "New description");
        assert_eq!(candidate["prompt_data"], patch["prompt_data"]);
        assert_eq!(candidate["function_data"], function.function_data.unwrap());
        assert!(candidate.get("id").is_none());
    }

    #[test]
    fn scorer_output_flags_reported_only_when_set() {
        let base = args(None, None);
        assert!(base.scorer_output_flags().is_empty());

        let mut scored = args(None, None);
        scored.choice_scores = Some(r#"{"pass":1}"#.to_string());
        scored.pass_threshold = Some(0.5);
        assert_eq!(
            scored.scorer_output_flags(),
            vec!["--choice-scores", "--pass-threshold"]
        );

        let mut labeled = args(None, None);
        labeled.classifications = Some(r#"["a"]"#.to_string());
        labeled.allow_no_match = Some(true);
        labeled.use_cot = Some(false);
        assert_eq!(
            labeled.scorer_output_flags(),
            vec!["--classifications", "--allow-no-match", "--use-cot"]
        );
    }

    #[test]
    fn build_patch_body_messages_writes_chat_block() {
        let mut args = args(None, None);
        args.messages = Some(r#"[{"role":"user","content":"hi"}]"#.to_string());
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(
            body["prompt_data"]["prompt"]["type"],
            serde_json::json!("chat")
        );
        assert_eq!(
            body["prompt_data"]["prompt"]["messages"],
            serde_json::json!([{"role":"user","content":"hi"}])
        );
    }

    #[test]
    fn build_patch_body_model_merges_into_prompt_data() {
        let args = args(Some("gpt-4o-mini"), None);
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(
            body["prompt_data"]["options"]["model"],
            serde_json::json!("gpt-4o-mini")
        );
    }

    #[test]
    fn build_patch_body_messages_and_model_combine() {
        let mut args = args(Some("gpt-4o-mini"), None);
        args.messages = Some(r#"[{"role":"user","content":"Grade it."}]"#.to_string());
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(
            body["prompt_data"]["prompt"]["messages"],
            json!([{"role": "user", "content": "Grade it."}])
        );
        assert_eq!(
            body["prompt_data"]["options"]["model"],
            serde_json::json!("gpt-4o-mini")
        );
    }

    #[test]
    fn build_patch_body_updates_all_llm_configuration() {
        let parsed = UpdateArgsHarness::try_parse_from([
            "test",
            "test-scorer",
            "--model",
            "gpt-test",
            "--temperature",
            "0.2",
            "--max-tokens",
            "128",
            "--top-p",
            "0.9",
            "--frequency-penalty",
            "0.5",
            "--presence-penalty",
            "0.25",
            "--stop-sequence",
            "END",
            "--tool-choice",
            "test_tool",
            "--reasoning-effort",
            "low",
            "--verbosity",
            "high",
            "--template-format",
            "none",
            "--use-cot=false",
        ])
        .expect("parse update");

        let body = build_patch_body(&parsed.args).expect("patch body");
        let params = &body["prompt_data"]["options"]["params"];
        assert_eq!(body["prompt_data"]["options"]["model"], "gpt-test");
        assert_eq!(params["temperature"], 0.2);
        assert_eq!(params["max_tokens"], 128);
        assert_eq!(params["top_p"], 0.9);
        assert_eq!(params["frequency_penalty"], 0.5);
        assert_eq!(params["presence_penalty"], 0.25);
        assert_eq!(params["stop"], json!(["END"]));
        assert_eq!(
            params["tool_choice"],
            json!({"type": "function", "function": {"name": "test_tool"}})
        );
        assert_eq!(params["reasoning_effort"], "low");
        assert_eq!(params["verbosity"], "high");
        assert_eq!(body["prompt_data"]["template_format"], "none");
        assert_eq!(body["prompt_data"]["parser"]["use_cot"], false);
    }

    #[test]
    fn build_patch_body_switches_to_classification_output() {
        let mut args = args(None, None);
        args.classifications = Some(r#"["safe","unsafe"]"#.to_string());
        args.allow_no_match = Some(true);
        args.metadata = Some("owner: test-team".to_string());

        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(body["function_type"], "classifier");
        assert_eq!(
            body["prompt_data"]["parser"]["choice"],
            json!(["safe", "unsafe"])
        );
        assert_eq!(body["prompt_data"]["parser"]["allow_no_match"], true);
        assert_eq!(body["metadata"]["owner"], "test-team");
    }

    #[test]
    fn build_patch_body_updates_scores_and_pass_threshold() {
        let mut args = args(None, None);
        args.choice_scores = Some(r#"{"pass":1,"fail":0}"#.to_string());
        args.pass_threshold = Some(0.8);

        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(body["function_type"], "scorer");
        assert_eq!(
            body["prompt_data"]["parser"]["choice_scores"],
            json!({"pass": 1, "fail": 0})
        );
        assert_eq!(body["metadata"]["__pass_threshold"], 0.8);
    }

    #[test]
    fn build_patch_body_description_is_top_level() {
        let args = args(None, Some("Helpfulness judge"));
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(body["description"], serde_json::json!("Helpfulness judge"));
    }

    #[test]
    fn build_patch_body_reads_at_prefixed_messages_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("messages.json");
        std::fs::write(&path, r#"[{"role":"user","content":"Grade from a file."}]"#)
            .expect("write messages");
        let source = format!("@{}", path.display());

        let mut args = args(None, None);
        args.messages = Some(source);
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(
            body["prompt_data"]["prompt"]["messages"],
            json!([{"role": "user", "content": "Grade from a file."}])
        );
    }

    #[test]
    fn build_patch_body_reads_at_prefixed_patch_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("patch.json");
        std::fs::write(&path, r#"{"description":"From a file"}"#).expect("write patch");
        let source = format!("@{}", path.display());

        let mut args = args(None, None);
        args.patch = Some(source);
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(body["description"], "From a file");
    }

    #[test]
    fn build_patch_body_rejects_empty_update() {
        let args = args(None, None);
        let err = build_patch_body(&args).expect_err("should reject empty");
        assert!(err.to_string().contains("no updates requested"));
    }

    #[test]
    fn build_patch_body_extra_patch_merges_into_prompt_data() {
        let mut args = args(None, None);
        args.patch = Some(r#"{"prompt_data":{"parser":{"type":"llm_classifier","use_cot":true,"choice_scores":{"A":1.0,"B":0.0}}}}"#.to_string());
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(
            body["prompt_data"]["parser"]["choice_scores"],
            serde_json::json!({"A": 1.0, "B": 0.0})
        );
    }

    #[test]
    fn parse_patch_object_rejects_non_object() {
        let err = parse_patch_object("[1,2,3]").expect_err("should reject");
        assert!(err.to_string().contains("JSON object"));
    }

    #[test]
    fn merge_objects_deep_merges_nested_maps() {
        let mut target = serde_json::json!({
            "prompt_data": { "options": { "model": "gpt-4o" } }
        })
        .as_object()
        .expect("object")
        .clone();
        let source = serde_json::json!({
            "prompt_data": { "options": { "temperature": 0 } }
        })
        .as_object()
        .expect("object")
        .clone();

        merge_json_objects(&mut target, &source);

        assert_eq!(
            target["prompt_data"]["options"]["model"],
            serde_json::json!("gpt-4o")
        );
        assert_eq!(
            target["prompt_data"]["options"]["temperature"],
            serde_json::json!(0)
        );
    }
}
