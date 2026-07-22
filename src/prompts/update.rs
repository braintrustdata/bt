use std::path::PathBuf;

use anyhow::{anyhow, bail, Context, Result};
use clap::Args;
use dialoguer::Confirm;
use serde_json::{json, Map, Value};

use crate::ui::{is_interactive, print_command_status, with_spinner, CommandStatus};

use super::{api, ResolvedContext};

/// Update a prompt in place via `PATCH /v1/prompt/{id}`.
///
/// The Braintrust API deep-merges object fields, so you can send just the
/// nested fields you want to change (for example `prompt_data.prompt`) without
/// re-authoring the whole prompt.
#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt prompts update my-prompt --prompt-file prompt.md
  bt prompts update my-prompt --model gpt-4o-mini
  bt prompts update my-prompt --description \"Customer support prompt\"
  bt prompts update my-prompt --patch '{\"prompt_data\":{\"options\":{\"model\":\"gpt-4o-mini\"}}}'
  bt prompts update my-prompt --patch-file prompt-patch.json
")]
pub struct UpdateArgs {
    /// Prompt slug (positional)
    #[arg(value_name = "SLUG", conflicts_with = "slug_flag")]
    slug_positional: Option<String>,

    /// Prompt slug (flag)
    #[arg(long = "slug", short = 's', env = "BT_PROMPTS_UPDATE_SLUG")]
    slug_flag: Option<String>,

    /// Replace the completion prompt text. Writes `prompt_data.prompt` as
    /// `{"type":"completion","content":<text>}`. Read from a file with
    /// --prompt-file, or stdin when no value is given in a non-interactive shell.
    #[arg(
        long,
        env = "BT_PROMPTS_UPDATE_PROMPT",
        value_name = "TEXT",
        conflicts_with_all = ["prompt_file", "messages"]
    )]
    prompt: Option<String>,

    /// Read the completion prompt text from a file. Mutually exclusive with --prompt.
    #[arg(
        long,
        env = "BT_PROMPTS_UPDATE_PROMPT_FILE",
        value_name = "PATH",
        conflicts_with_all = ["prompt", "messages"]
    )]
    prompt_file: Option<PathBuf>,

    /// Replace the chat prompt messages (JSON array). Writes
    /// `prompt_data.prompt` as `{"type":"chat","messages":<json>}`.
    #[arg(
        long,
        env = "BT_PROMPTS_UPDATE_MESSAGES",
        value_name = "JSON",
        conflicts_with_all = ["prompt", "prompt_file"]
    )]
    messages: Option<String>,

    /// Update the model used by the prompt. Writes `prompt_data.options.model`.
    #[arg(
        long,
        short = 'm',
        env = "BT_PROMPTS_UPDATE_MODEL",
        value_name = "MODEL"
    )]
    model: Option<String>,

    /// Update the prompt description.
    #[arg(
        long,
        short = 'd',
        env = "BT_PROMPTS_UPDATE_DESCRIPTION",
        value_name = "TEXT"
    )]
    description: Option<String>,

    /// Arbitrary JSON object deep-merged into the prompt on patch. Use this
    /// for fields without a dedicated flag (for example `tags`, `metadata`,
    /// or nested `prompt_data.options.params`).
    #[arg(
        long,
        env = "BT_PROMPTS_UPDATE_PATCH",
        value_name = "JSON",
        conflicts_with = "patch_file"
    )]
    patch: Option<String>,

    /// Read the arbitrary patch JSON from a file. Mutually exclusive with --patch.
    #[arg(
        long,
        env = "BT_PROMPTS_UPDATE_PATCH_FILE",
        value_name = "PATH",
        conflicts_with = "patch"
    )]
    patch_file: Option<PathBuf>,

    /// Skip the confirmation prompt.
    #[arg(long, short = 'y', env = "BT_PROMPTS_UPDATE_YES", default_value_t = false, value_parser = clap::builder::BoolishValueParser::new())]
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

    let prompt_text = resolve_prompt_text(args)?;
    let messages_json = resolve_messages(args)?;

    if prompt_text.is_some() || messages_json.is_some() {
        let prompt_block = match (prompt_text, messages_json) {
            (Some(text), None) => json!({
                "type": "completion",
                "content": text,
            }),
            (None, Some(messages)) => json!({
                "type": "chat",
                "messages": messages,
            }),
            (Some(_), Some(_)) => {
                bail!("use either --prompt/--prompt-file or --messages, not both")
            }
            (None, None) => unreachable!("guarded above"),
        };

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

    if let Some(model) = args.model.as_deref() {
        let prompt_data = match patch.get("prompt_data") {
            Some(Value::Object(existing)) => {
                let mut merged = existing.clone();
                let options = match merged.get("options") {
                    Some(Value::Object(opts)) => opts.clone(),
                    _ => Map::new(),
                };
                let mut options = options;
                options.insert("model".to_string(), Value::String(model.to_string()));
                merged.insert("options".to_string(), Value::Object(options));
                Value::Object(merged)
            }
            _ => json!({ "options": { "model": model } }),
        };
        patch.insert("prompt_data".to_string(), prompt_data);
    }

    let extra = resolve_extra_patch(args)?;
    if let Some(extra_obj) = extra {
        merge_objects(&mut patch, &extra_obj);
    }

    if patch.is_empty() {
        bail!(
            "no updates requested. Pass one of --prompt/--prompt-file, --messages, --model, --description, or --patch/--patch-file"
        );
    }

    Ok(Value::Object(patch))
}

fn resolve_prompt_text(args: &UpdateArgs) -> Result<Option<String>> {
    match (&args.prompt, &args.prompt_file) {
        (Some(_), Some(_)) => bail!("use either --prompt or --prompt-file, not both"),
        (Some(text), None) => Ok(Some(text.clone())),
        (None, Some(path)) => {
            let content = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read prompt file {}", path.display()))?;
            Ok(Some(content))
        }
        (None, None) => {
            if args.messages.is_some()
                || args.model.is_some()
                || args.description.is_some()
                || args.patch.is_some()
                || args.patch_file.is_some()
            {
                return Ok(None);
            }
            if !is_interactive() {
                use std::io::Read;
                let mut buf = String::new();
                std::io::stdin()
                    .read_to_string(&mut buf)
                    .context("failed to read prompt from stdin")?;
                let trimmed = buf.trim();
                if trimmed.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(trimmed.to_string()));
            }
            Ok(None)
        }
    }
}

fn resolve_messages(args: &UpdateArgs) -> Result<Option<Value>> {
    match &args.messages {
        Some(raw) => {
            let parsed: Value = serde_json::from_str(raw).context("invalid JSON in --messages")?;
            match parsed {
                Value::Array(_) => Ok(Some(parsed)),
                _ => bail!("--messages must be a JSON array of chat messages"),
            }
        }
        None => Ok(None),
    }
}

fn resolve_extra_patch(args: &UpdateArgs) -> Result<Option<Map<String, Value>>> {
    match (&args.patch, &args.patch_file) {
        (Some(_), Some(_)) => bail!("use either --patch or --patch-file, not both"),
        (Some(raw), None) => parse_patch_object(raw).map(Some),
        (None, Some(path)) => {
            let content = std::fs::read_to_string(path)
                .with_context(|| format!("failed to read patch file {}", path.display()))?;
            parse_patch_object(&content).map(Some)
        }
        (None, None) => Ok(None),
    }
}

fn parse_patch_object(raw: &str) -> Result<Map<String, Value>> {
    let value: Value = serde_json::from_str(raw).context("invalid JSON in --patch/--patch-file")?;
    match value {
        Value::Object(map) => Ok(map),
        _ => bail!("--patch/--patch-file must be a JSON object"),
    }
}

fn merge_objects(target: &mut Map<String, Value>, source: &Map<String, Value>) {
    for (key, value) in source {
        match (target.get_mut(key), value) {
            (Some(Value::Object(target_inner)), Value::Object(source_inner)) => {
                merge_objects(target_inner, source_inner);
            }
            _ => {
                target.insert(key.clone(), value.clone());
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(prompt: Option<&str>, model: Option<&str>, description: Option<&str>) -> UpdateArgs {
        UpdateArgs {
            slug_positional: Some("test-prompt".to_string()),
            slug_flag: None,
            prompt: prompt.map(ToOwned::to_owned),
            prompt_file: None,
            messages: None,
            model: model.map(ToOwned::to_owned),
            description: description.map(ToOwned::to_owned),
            patch: None,
            patch_file: None,
            yes: true,
        }
    }

    #[test]
    fn build_patch_body_prompt_writes_completion_block() {
        let args = args(Some("Answer the question."), None, None);
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(
            body["prompt_data"]["prompt"]["type"],
            serde_json::json!("completion")
        );
        assert_eq!(
            body["prompt_data"]["prompt"]["content"],
            serde_json::json!("Answer the question.")
        );
    }

    #[test]
    fn build_patch_body_messages_writes_chat_block() {
        let mut args = args(None, None, None);
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
        let args = args(None, Some("gpt-4o-mini"), None);
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(
            body["prompt_data"]["options"]["model"],
            serde_json::json!("gpt-4o-mini")
        );
    }

    #[test]
    fn build_patch_body_prompt_and_model_combine() {
        let args = args(Some("Answer it."), Some("gpt-4o-mini"), None);
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(body["prompt_data"]["prompt"]["content"], "Answer it.");
        assert_eq!(
            body["prompt_data"]["options"]["model"],
            serde_json::json!("gpt-4o-mini")
        );
    }

    #[test]
    fn build_patch_body_description_is_top_level() {
        let args = args(None, None, Some("Customer support prompt"));
        let body = build_patch_body(&args).expect("patch body");
        assert_eq!(
            body["description"],
            serde_json::json!("Customer support prompt")
        );
    }

    #[test]
    fn build_patch_body_rejects_prompt_and_messages_together() {
        let mut args = args(Some("Answer it."), None, None);
        args.messages = Some(r#"[{"role":"user","content":"hi"}]"#.to_string());
        let err = build_patch_body(&args).expect_err("should reject");
        assert!(err.to_string().contains("not both"));
    }

    #[test]
    fn build_patch_body_rejects_prompt_and_prompt_file_together() {
        let mut args = args(Some("Answer it."), None, None);
        args.prompt_file = Some(PathBuf::from("/tmp/ignore.md"));
        let err = build_patch_body(&args).expect_err("should reject");
        assert!(err.to_string().contains("not both"));
    }

    #[test]
    fn build_patch_body_rejects_patch_and_patch_file_together() {
        let mut args = args(None, None, None);
        args.patch = Some("{}".to_string());
        args.patch_file = Some(PathBuf::from("/tmp/ignore.json"));
        let err = build_patch_body(&args).expect_err("should reject");
        assert!(err.to_string().contains("not both"));
    }

    #[test]
    fn build_patch_body_rejects_empty_update() {
        let args = args(None, None, None);
        let err = build_patch_body(&args).expect_err("should reject empty");
        assert!(err.to_string().contains("no updates requested"));
    }

    #[test]
    fn build_patch_body_extra_patch_merges_into_prompt_data() {
        let mut args = args(None, None, None);
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

        merge_objects(&mut target, &source);

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
