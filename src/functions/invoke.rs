use std::io::{self, IsTerminal, Read};

use anyhow::{bail, Context, Result};
use clap::Args;
use serde_json::{json, Value};

use super::{select_function_interactive, FunctionTypeFilter, ResolvedContext, SlugArgs};
use crate::ui::is_interactive;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt functions invoke my-fn --input '{\"key\": \"value\"}'
  bt functions invoke my-fn --message \"What is 2+2?\"
  bt functions invoke my-fn -i '{\"my-var\": \"A very long text...\"}' -m \"Summarize this\"
  bt functions invoke my-fn --mode json --version abc123
  ")]
pub(crate) struct InvokeArgs {
    #[command(flatten)]
    slug: SlugArgs,

    /// JSON input to the function
    #[arg(long, short = 'i')]
    input: Option<String>,

    /// User message (repeatable, for LLM functions)
    #[arg(long, short = 'm')]
    message: Vec<String>,

    /// Response format: auto, json, text, parallel
    #[arg(long)]
    mode: Option<String>,

    /// Pin to a specific function version
    #[arg(long)]
    version: Option<String>,
}

impl InvokeArgs {
    pub fn slug(&self) -> Option<&str> {
        self.slug.slug()
    }
}

fn resolve_input(input_arg: &Option<String>) -> Result<Option<Value>> {
    if let Some(raw) = input_arg {
        let parsed: Value = serde_json::from_str(raw).context("invalid JSON in --input")?;
        return Ok(Some(parsed));
    }

    if !io::stdin().is_terminal() {
        let mut buf = String::new();
        io::stdin()
            .read_to_string(&mut buf)
            .context("failed to read from stdin")?;
        let trimmed = buf.trim();
        if trimmed.is_empty() {
            return Ok(None);
        }
        let parsed: Value = serde_json::from_str(trimmed).context("invalid JSON from stdin")?;
        return Ok(Some(parsed));
    }

    Ok(None)
}

pub async fn run(
    ctx: &ResolvedContext,
    args: &InvokeArgs,
    json_output: bool,
    ft: Option<FunctionTypeFilter>,
) -> Result<()> {
    let slug = match args.slug() {
        Some(s) => s.to_string(),
        None if is_interactive() => {
            let f = select_function_interactive(&ctx.client, &ctx.project.id, ft).await?;
            f.slug
        }
        None => bail!("<SLUG> required"),
    };

    let resolved_input = resolve_input(&args.input)?;

    let mut body = json!({
        "project_name": ctx.project.name,
        "slug": slug,
    });

    if let Some(input) = resolved_input {
        body["input"] = input;
    }
    if !args.message.is_empty() {
        let messages: Vec<Value> = args
            .message
            .iter()
            .map(|m| json!({"role": "user", "content": m}))
            .collect();
        body["messages"] = json!(messages);
    }
    if let Some(mode) = &args.mode {
        body["mode"] = json!(mode);
    }
    if let Some(version) = &args.version {
        body["version"] = json!(version);
    }

    let result = super::api::invoke_function(&ctx.client, &body).await?;

    if json_output {
        println!("{}", serde_json::to_string(&result)?);
    } else {
        match &result {
            Value::String(s) => println!("{s}"),
            Value::Null => {}
            _ => println!("{}", serde_json::to_string_pretty(&result)?),
        }
    }

    Ok(())
}
