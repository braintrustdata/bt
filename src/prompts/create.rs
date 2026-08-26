use anyhow::{bail, Result};
use clap::Args;
use dialoguer::Input;
use serde_json::{json, Value};

use crate::{
    error::user_error,
    functions::{
        create::slugify,
        prompt_config::PromptConfigArgs,
        scorer_config::{build_scorer_config, ScorerConfig},
    },
    ui::{is_interactive, print_command_status, with_spinner, CommandStatus},
};

use super::{api, ResolvedContext};

/// Create a prompt in the current project.
#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt prompts create \"Support reply\" --model gpt-5.4-nano --messages @messages.json
  bt prompts create --name \"Summarize\" --slug summarize --model gpt-5.4-nano \\
    --messages '[{\"role\":\"user\",\"content\":\"Summarize {{input}}\"}]'
")]
pub(crate) struct CreateArgs {
    /// Prompt name.
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Prompt name (named form).
    #[arg(long, value_name = "NAME")]
    name: Option<String>,

    /// Unique prompt slug. Defaults to a slug generated from the name.
    #[arg(long, short = 's')]
    slug: Option<String>,

    /// Prompt description.
    #[arg(long, short = 'd')]
    description: Option<String>,

    /// Chat messages source: inline JSON, @PATH, or - for stdin.
    #[arg(long, value_name = "SOURCE")]
    messages: String,

    /// Model used by the prompt.
    #[arg(long, short = 'm', value_name = "MODEL")]
    model: String,

    #[command(flatten)]
    prompt_config: PromptConfigArgs,
}

pub(crate) async fn run(ctx: &ResolvedContext, args: &CreateArgs, json_output: bool) -> Result<()> {
    let name = resolve_name(args).map_err(user_error)?;
    let slug = args
        .slug
        .as_deref()
        .map(str::trim)
        .filter(|slug| !slug.is_empty())
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| slugify(&name));
    if slug.is_empty() {
        bail!("could not generate a slug from the prompt name; pass --slug explicitly");
    }

    let definition = build_definition(args, &ctx.project.id, &name, &slug).map_err(user_error)?;
    let prompt = match with_spinner(
        "Creating prompt...",
        api::create_prompt(&ctx.client, &definition),
    )
    .await
    {
        Ok(prompt) => prompt,
        Err(error) => {
            print_command_status(CommandStatus::Error, &format!("Failed to create '{name}'"));
            return Err(error);
        }
    };

    if json_output {
        println!("{}", serde_json::to_string(&prompt)?);
    } else {
        print_command_status(CommandStatus::Success, &format!("Created '{name}'"));
    }
    Ok(())
}

fn resolve_name(args: &CreateArgs) -> Result<String> {
    let name = match args.name_positional.as_deref().or(args.name.as_deref()) {
        Some(name) => name.trim().to_string(),
        None if is_interactive() => Input::<String>::new()
            .with_prompt("Prompt name")
            .interact_text()?
            .trim()
            .to_string(),
        None => bail!("prompt name required. Use: bt prompts create <name> ..."),
    };
    if name.is_empty() {
        bail!("prompt name cannot be empty");
    }
    Ok(name)
}

fn build_definition(args: &CreateArgs, project_id: &str, name: &str, slug: &str) -> Result<Value> {
    let config = build_scorer_config(
        &ScorerConfig {
            messages: Some(&args.messages),
            model: Some(&args.model),
            prompt_config: &args.prompt_config,
            choice_scores: None,
            classifications: None,
            use_cot: None,
            allow_no_match: None,
            pass_threshold: None,
            metadata: None,
            metadata_label: "prompt metadata",
        },
        false,
    )?;
    let mut definition = json!({
        "project_id": project_id,
        "name": name,
        "slug": slug,
    });
    definition
        .as_object_mut()
        .expect("prompt definition is an object")
        .extend(config);
    if let Some(description) = args.description.as_deref() {
        definition["description"] = Value::String(description.to_string());
    }
    Ok(definition)
}
