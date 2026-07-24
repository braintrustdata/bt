use anyhow::{anyhow, bail, Context, Result};
use clap::Args;
use dialoguer::Confirm;
use serde_json::{json, Map, Value};

use crate::{
    functions::prompt_config::PromptConfigArgs,
    ui::{is_interactive, print_command_status, with_spinner, CommandStatus},
    utils::{merge_json_objects, read_text_source, read_yaml_object_source},
};

use super::{api, ResolvedContext};

/// Update a prompt's configuration or metadata in place.
///
/// The Braintrust API deep-merges object fields, so you can send just the
/// nested fields you want to change (for example `prompt_data.prompt`) without
/// re-authoring the whole prompt.
#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt prompts update my-prompt --messages @messages.json
  bt prompts update my-prompt --model gpt-5.4-nano
  bt prompts update my-prompt --description \"Customer support prompt\"
  bt prompts update my-prompt --patch '{\"prompt_data\":{\"options\":{\"model\":\"gpt-5.4-nano\"}}}'
  bt prompts update my-prompt --patch @prompt-patch.json
")]
pub struct UpdateArgs {
    /// Prompt slug (positional)
    #[arg(value_name = "SLUG", conflicts_with = "slug_flag")]
    slug_positional: Option<String>,

    /// Prompt slug (flag)
    #[arg(long = "slug", short = 's')]
    slug_flag: Option<String>,

    /// Replacement chat messages source: inline JSON, @PATH to read from a
    /// file, or - for stdin.
    #[arg(long, value_name = "SOURCE")]
    messages: Option<String>,

    /// Update the model used by the prompt.
    #[arg(long, short = 'm', value_name = "MODEL")]
    model: Option<String>,

    #[command(flatten)]
    prompt_config: PromptConfigArgs,

    /// Deep-merge metadata from inline YAML, @PATH, or stdin (-).
    #[arg(long, value_name = "SOURCE")]
    metadata: Option<String>,

    /// Update the prompt description.
    #[arg(long, short = 'd', value_name = "TEXT")]
    description: Option<String>,

    /// Arbitrary JSON object deep-merged into the prompt. Accepts inline JSON,
    /// @PATH to read JSON from a file, or - for stdin.
    #[arg(long, value_name = "SOURCE")]
    patch: Option<String>,

    /// Skip the confirmation prompt.
    #[arg(long, short = 'y')]
    yes: bool,
}

impl UpdateArgs {
    fn slug(&self) -> Option<&str> {
        self.slug_positional
            .as_deref()
            .or(self.slug_flag.as_deref())
    }
}

pub async fn run(ctx: &ResolvedContext, args: &UpdateArgs, json_output: bool) -> Result<()> {
    let project_name = &ctx.project.name;
    let body = build_patch_body(args)?;

    let prompt = match args.slug() {
        Some(slug) => with_spinner(
            "Loading prompt...",
            api::get_prompt_by_slug(&ctx.client, project_name, slug),
        )
        .await?
        .ok_or_else(|| anyhow!("prompt with slug '{slug}' not found"))?,
        None => {
            if !is_interactive() {
                bail!("prompt slug required. Use: bt prompts update <slug> [--patch ...]");
            }
            super::delete::select_prompt_interactive(&ctx.client, project_name).await?
        }
    };

    if !args.yes && is_interactive() {
        let confirm = Confirm::new()
            .with_prompt(format!(
                "Update prompt '{}' in {}?",
                prompt.name, project_name
            ))
            .default(false)
            .interact()?;
        if !confirm {
            return Ok(());
        }
    }

    let updated = match with_spinner(
        "Updating prompt...",
        api::patch_prompt(&ctx.client, &prompt.id, &body),
    )
    .await
    {
        Ok(value) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Updated '{}'", prompt.name),
            );
            value
        }
        Err(error) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Failed to update '{}'", prompt.name),
            );
            return Err(error);
        }
    };

    if json_output {
        println!("{}", serde_json::to_string(&updated)?);
    } else if !crate::ui::is_quiet() {
        eprintln!(
            "Run `bt prompts view {}` to inspect the updated prompt.",
            prompt.slug
        );
    }

    Ok(())
}

fn build_patch_body(args: &UpdateArgs) -> Result<Value> {
    let mut patch: Map<String, Value> = Map::new();

    if let Some(description) = args.description.as_deref() {
        patch.insert(
            "description".to_string(),
            Value::String(description.to_string()),
        );
    }

    if let Some(source) = args.metadata.as_deref() {
        patch.insert(
            "metadata".to_string(),
            Value::Object(read_yaml_object_source(source, "prompt metadata")?),
        );
    }

    let messages_json = resolve_messages(args)?;

    if let Some(messages) = messages_json {
        let prompt_block = json!({
            "type": "chat",
            "messages": messages,
        });

        let prompt_data = match patch.get("prompt_data") {
            Some(Value::Object(existing)) => {
                let mut merged = existing.clone();
                merged.insert("prompt".to_string(), prompt_block);
                Value::Object(merged)
            }
            _ => json!({ "prompt": prompt_block }),
        };
        patch.insert("prompt_data".to_string(), prompt_data);
    }

    let prompt_config = args
        .prompt_config
        .build_prompt_data_patch(args.model.as_deref())?;
    if !prompt_config.is_empty() {
        let prompt_data_patch = json!({ "prompt_data": prompt_config });
        merge_json_objects(
            &mut patch,
            prompt_data_patch
                .as_object()
                .expect("prompt data patch is an object"),
        );
    }

    let extra = resolve_extra_patch(args)?;
    if let Some(extra_obj) = extra {
        merge_json_objects(&mut patch, &extra_obj);
    }

    if patch.is_empty() {
        bail!("no updates requested. Pass an update flag; see `bt prompts update --help`");
    }

    Ok(Value::Object(patch))
}

fn resolve_messages(args: &UpdateArgs) -> Result<Option<Value>> {
    match args.messages.as_deref() {
        Some(source) => {
            let raw = read_text_source(source, "messages")?;
            let parsed: Value = serde_json::from_str(&raw).context("invalid JSON in --messages")?;
            match parsed {
                Value::Array(_) => Ok(Some(parsed)),
                _ => bail!("--messages must be a JSON array of chat messages"),
            }
        }
        None => Ok(None),
    }
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
            slug_positional: Some("test-prompt".to_string()),
            slug_flag: None,
            messages: None,
            model: model.map(ToOwned::to_owned),
            prompt_config: PromptConfigArgs::default(),
            metadata: None,
            description: description.map(ToOwned::to_owned),
            patch: None,
            yes: true,
        }
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
        args.messages = Some(r#"[{"role":"user","content":"Answer it."}]"#.to_string());
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(
            body["prompt_data"]["prompt"]["messages"],
            json!([{"role": "user", "content": "Answer it."}])
        );
        assert_eq!(
            body["prompt_data"]["options"]["model"],
            serde_json::json!("gpt-4o-mini")
        );
    }

    #[test]
    fn build_patch_body_updates_prompt_configuration_and_metadata() {
        let parsed = UpdateArgsHarness::try_parse_from([
            "test",
            "test-prompt",
            "--temperature",
            "0.3",
            "--max-tokens",
            "100",
            "--template-format",
            "mustache",
            "--metadata",
            "owner: test-team",
        ])
        .expect("parse update");

        let body = build_patch_body(&parsed.args).expect("patch body");
        assert_eq!(body["prompt_data"]["options"]["params"]["temperature"], 0.3);
        assert_eq!(body["prompt_data"]["options"]["params"]["max_tokens"], 100);
        assert_eq!(body["prompt_data"]["template_format"], "mustache");
        assert_eq!(body["metadata"]["owner"], "test-team");
    }

    #[test]
    fn build_patch_body_description_is_top_level() {
        let args = args(None, Some("Customer support prompt"));
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(
            body["description"],
            serde_json::json!("Customer support prompt")
        );
    }

    #[test]
    fn build_patch_body_reads_at_prefixed_messages_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("messages.json");
        std::fs::write(
            &path,
            r#"[{"role":"user","content":"Answer from a file."}]"#,
        )
        .expect("write messages");
        let source = format!("@{}", path.display());

        let mut args = args(None, None);
        args.messages = Some(source);
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(
            body["prompt_data"]["prompt"]["messages"],
            json!([{"role": "user", "content": "Answer from a file."}])
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
        args.patch =
            Some(r#"{"prompt_data":{"options":{"params":{"temperature":0}}}}"#.to_string());
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(
            body["prompt_data"]["options"]["params"]["temperature"],
            serde_json::json!(0)
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
