use std::{io::Read, path::PathBuf};

use anyhow::{bail, Context, Result};
use clap::{builder::BoolishValueParser, Args};
use dialoguer::Input;
use serde_json::{json, Map, Value};

use crate::ui::{is_interactive, print_command_status, with_spinner, CommandStatus};

use super::{api, IfExistsMode, ResolvedContext};

/// Create an LLM scorer.
///
/// The generated definition matches `project.scorers.create(...)`: a prompt
/// function with an `llm_classifier` parser, model, chain-of-thought setting,
/// and numeric score for each possible choice.
#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt scorers create \"Helpfulness\" --model gpt-4o-mini --prompt-file judge.md \\
    --choice-scores '{\"A\":1,\"B\":0}' --use-cot
  bt scorers create \"Correctness\" --slug correctness --model gpt-4o-mini \\
    --prompt \"Score {{output}} against {{expected}}\" \\
    --choice-scores '{\"correct\":1,\"incorrect\":0}' --use-cot=false
  bt scorers create \"Tone\" --model gpt-4o-mini \\
    --messages '[{\"role\":\"user\",\"content\":\"Judge {{output}}\"}]' \\
    --choice-scores-file scores.json --use-cot
")]
pub(crate) struct CreateArgs {
    /// Scorer name.
    #[arg(value_name = "NAME", conflicts_with = "name")]
    name_positional: Option<String>,

    /// Scorer name (alternative to the positional name).
    #[arg(long, env = "BT_SCORERS_CREATE_NAME", value_name = "NAME")]
    name: Option<String>,

    /// Unique scorer slug. Defaults to a slug generated from the name.
    #[arg(long, short = 's', env = "BT_SCORERS_CREATE_SLUG")]
    slug: Option<String>,

    /// Scorer description.
    #[arg(long, short = 'd', env = "BT_SCORERS_CREATE_DESCRIPTION")]
    description: Option<String>,

    /// Completion prompt text. Use --prompt-file for a file, or pipe the
    /// prompt through stdin when no prompt option is supplied.
    #[arg(
        long,
        env = "BT_SCORERS_CREATE_PROMPT",
        value_name = "TEXT",
        conflicts_with_all = ["prompt_file", "messages", "messages_file"]
    )]
    prompt: Option<String>,

    /// Read the completion prompt text from a file.
    #[arg(
        long,
        env = "BT_SCORERS_CREATE_PROMPT_FILE",
        value_name = "PATH",
        conflicts_with_all = ["prompt", "messages", "messages_file"]
    )]
    prompt_file: Option<PathBuf>,

    /// Chat prompt messages as a JSON array.
    #[arg(
        long,
        env = "BT_SCORERS_CREATE_MESSAGES",
        value_name = "JSON",
        conflicts_with_all = ["prompt", "prompt_file", "messages_file"]
    )]
    messages: Option<String>,

    /// Read chat prompt messages as a JSON array from a file.
    #[arg(
        long,
        env = "BT_SCORERS_CREATE_MESSAGES_FILE",
        value_name = "PATH",
        conflicts_with_all = ["prompt", "prompt_file", "messages"]
    )]
    messages_file: Option<PathBuf>,

    /// Model used by the LLM judge.
    #[arg(
        long,
        short = 'm',
        env = "BT_SCORERS_CREATE_MODEL",
        value_name = "MODEL"
    )]
    model: String,

    /// JSON object mapping each classifier choice to a numeric score.
    #[arg(
        long,
        env = "BT_SCORERS_CREATE_CHOICE_SCORES",
        value_name = "JSON",
        required_unless_present = "choice_scores_file",
        conflicts_with = "choice_scores_file"
    )]
    choice_scores: Option<String>,

    /// Read the choice-to-score JSON object from a file.
    #[arg(
        long,
        env = "BT_SCORERS_CREATE_CHOICE_SCORES_FILE",
        value_name = "PATH",
        conflicts_with = "choice_scores"
    )]
    choice_scores_file: Option<PathBuf>,

    /// Whether the scorer should use chain-of-thought reasoning. This option
    /// is required; pass --use-cot or --use-cot=false.
    #[arg(
        long,
        env = "BT_SCORERS_CREATE_USE_COT",
        num_args = 0..=1,
        default_missing_value = "true",
        required = true,
        value_parser = BoolishValueParser::new()
    )]
    use_cot: Option<bool>,

    /// Behavior when a scorer with the same slug already exists.
    #[arg(
        long,
        env = "BT_SCORERS_CREATE_IF_EXISTS",
        value_enum,
        default_value = "error"
    )]
    if_exists: IfExistsMode,
}

pub(crate) async fn run(ctx: &ResolvedContext, args: &CreateArgs, json_output: bool) -> Result<()> {
    let name = resolve_name(args)?;
    let slug = resolve_slug(args, &name)?;
    let definition = build_scorer_definition(args, &ctx.project.id, &name, &slug)?;

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
        println!(
            "{}",
            serde_json::to_string(&json!({
                "scorer": definition,
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

fn resolve_name(args: &CreateArgs) -> Result<String> {
    let name = match (&args.name_positional, &args.name) {
        (Some(_), Some(_)) => bail!("use either a positional name or --name, not both"),
        (Some(name), None) | (None, Some(name)) => name.trim().to_string(),
        (None, None) if is_interactive() => Input::<String>::new()
            .with_prompt("Scorer name")
            .interact_text()?
            .trim()
            .to_string(),
        (None, None) => bail!("scorer name required. Use: bt scorers create <name> ..."),
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
    if args.model.trim().is_empty() {
        bail!("--model cannot be empty");
    }
    let use_cot = args.use_cot.ok_or_else(|| {
        anyhow::anyhow!("--use-cot is required; pass --use-cot or --use-cot=false")
    })?;
    let prompt = resolve_prompt_block(args)?;
    let choice_scores = resolve_choice_scores(args)?;

    let mut definition = json!({
        "project_id": project_id,
        "name": name,
        "slug": slug,
        "function_data": {
            "type": "prompt",
        },
        "prompt_data": {
            "prompt": prompt,
            "options": {
                "model": args.model,
            },
            "parser": {
                "type": "llm_classifier",
                "use_cot": use_cot,
                "choice_scores": choice_scores,
            },
        },
        "if_exists": args.if_exists.as_str(),
        "function_type": "scorer",
    });

    if let Some(description) = args.description.as_deref() {
        definition["description"] = Value::String(description.to_string());
    }

    Ok(definition)
}

fn resolve_prompt_block(args: &CreateArgs) -> Result<Value> {
    let selected = usize::from(args.prompt.is_some())
        + usize::from(args.prompt_file.is_some())
        + usize::from(args.messages.is_some())
        + usize::from(args.messages_file.is_some());
    if selected > 1 {
        bail!("use only one of --prompt, --prompt-file, --messages, or --messages-file");
    }

    if let Some(prompt) = args.prompt.as_deref() {
        return Ok(json!({ "type": "completion", "content": prompt }));
    }
    if let Some(path) = args.prompt_file.as_deref() {
        let prompt = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read prompt file {}", path.display()))?;
        return Ok(json!({ "type": "completion", "content": prompt }));
    }
    if let Some(raw) = args.messages.as_deref() {
        return parse_messages(raw);
    }
    if let Some(path) = args.messages_file.as_deref() {
        let raw = std::fs::read_to_string(path)
            .with_context(|| format!("failed to read messages file {}", path.display()))?;
        return parse_messages(&raw);
    }

    if is_interactive() {
        bail!("scorer prompt required. Pass --prompt, --prompt-file, or --messages");
    }

    let mut prompt = String::new();
    std::io::stdin()
        .read_to_string(&mut prompt)
        .context("failed to read prompt from stdin")?;
    if prompt.is_empty() {
        bail!(
            "scorer prompt required. Pass --prompt, --prompt-file, or --messages, or pipe prompt text through stdin"
        );
    }
    Ok(json!({ "type": "completion", "content": prompt }))
}

fn parse_messages(raw: &str) -> Result<Value> {
    let messages: Value = serde_json::from_str(raw).context("invalid JSON in scorer messages")?;
    match messages {
        Value::Array(_) => Ok(json!({ "type": "chat", "messages": messages })),
        _ => bail!("scorer messages must be a JSON array"),
    }
}

fn resolve_choice_scores(args: &CreateArgs) -> Result<Value> {
    let raw = match (&args.choice_scores, &args.choice_scores_file) {
        (Some(_), Some(_)) => {
            bail!("use either --choice-scores or --choice-scores-file, not both")
        }
        (Some(raw), None) => raw.clone(),
        (None, Some(path)) => std::fs::read_to_string(path)
            .with_context(|| format!("failed to read choice scores file {}", path.display()))?,
        (None, None) => bail!(
            "--choice-scores is required; pass a JSON object such as '{{\"yes\":1,\"no\":0}}'"
        ),
    };

    let value: Value = serde_json::from_str(&raw).context("invalid JSON in choice scores")?;
    let scores = match value {
        Value::Object(scores) => scores,
        _ => bail!("choice scores must be a JSON object mapping choices to numeric scores"),
    };
    validate_choice_scores(&scores)?;
    Ok(Value::Object(scores))
}

fn validate_choice_scores(scores: &Map<String, Value>) -> Result<()> {
    if scores.is_empty() {
        bail!("choice scores cannot be empty");
    }
    for (choice, score) in scores {
        if !score.is_number() {
            bail!("score for choice '{choice}' must be a number");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args() -> CreateArgs {
        CreateArgs {
            name_positional: Some("Test Helpfulness".to_string()),
            name: None,
            slug: None,
            description: Some("Synthetic test scorer".to_string()),
            prompt: Some("Judge {{output}}.".to_string()),
            prompt_file: None,
            messages: None,
            messages_file: None,
            model: "gpt-test".to_string(),
            choice_scores: Some(r#"{"A":1,"B":0}"#.to_string()),
            choice_scores_file: None,
            use_cot: Some(true),
            if_exists: IfExistsMode::Error,
        }
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
        assert_eq!(body["prompt_data"]["prompt"]["type"], "completion");
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
    fn builds_chat_prompt_definition() {
        let mut args = args();
        args.prompt = None;
        args.messages = Some(r#"[{"role":"user","content":"Judge {{output}}"}]"#.to_string());

        let body =
            build_scorer_definition(&args, "test-project", "Test", "test").expect("definition");

        assert_eq!(body["prompt_data"]["prompt"]["type"], "chat");
        assert_eq!(
            body["prompt_data"]["prompt"]["messages"],
            json!([{ "role": "user", "content": "Judge {{output}}" }])
        );
    }

    #[test]
    fn rejects_multiple_prompt_sources() {
        let mut args = args();
        args.messages = Some("[]".to_string());

        let error = build_scorer_definition(&args, "test-project", "Test", "test")
            .expect_err("prompt sources should conflict");
        assert!(error.to_string().contains("use only one"));
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
    fn requires_explicit_use_cot() {
        let mut args = args();
        args.use_cot = None;

        let error = build_scorer_definition(&args, "test-project", "Test", "test")
            .expect_err("missing use-cot should fail");
        assert!(error.to_string().contains("--use-cot is required"));
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
