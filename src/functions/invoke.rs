use std::io::{self, Read};

use anyhow::{bail, Context, Result};
use clap::Args;
use serde_json::Value;

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

fn resolve_mode(mode: Option<&str>, json_output: bool) -> Option<&str> {
    mode.or(json_output.then_some("json"))
}

fn resolve_input(input_arg: &Option<String>) -> Result<Option<Value>> {
    if let Some(raw) = input_arg {
        let parsed: Value = serde_json::from_str(raw).context("invalid JSON in --input")?;
        return Ok(Some(parsed));
    }

    if !crate::ui::is_interactive() {
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
            let f =
                select_function_interactive(&ctx.client, &ctx.project.id.to_string(), ft).await?;
            f.slug
        }
        None => bail!("<SLUG> required"),
    };

    let resolved_input = resolve_input(&args.input)?;

    let body = build_invocation(
        &ctx.project.name,
        slug,
        resolved_input,
        &args.message,
        resolve_mode(args.mode.as_deref(), json_output),
        args.version.as_deref(),
    )?;

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

fn build_invocation(
    project_name: &str,
    slug: String,
    input: Option<Value>,
    messages: &[String],
    mode: Option<&str>,
    version: Option<&str>,
) -> Result<braintrust_api_client::backend::extensions::Invocation> {
    use braintrust_api_client::{
        backend::extensions::Invocation,
        types::{
            ChatCompletionMessageParam, ContentBlockInline, ContentBlockInlineContent,
            ContentBlockInlineRole, FunctionId, InvokeApi, ProjectNameContent, StreamingMode,
        },
    };
    let messages = (!messages.is_empty()).then(|| {
        messages
            .iter()
            .map(|message| {
                ChatCompletionMessageParam::User(ContentBlockInline {
                    content: Some(ContentBlockInlineContent::String(message.clone())),
                    name: None,
                    role: ContentBlockInlineRole::User,
                })
            })
            .collect()
    });
    let mode = mode
        .map(|mode| match mode {
            "auto" => Ok(StreamingMode::Auto),
            "json" => Ok(StreamingMode::Json),
            "text" => Ok(StreamingMode::Text),
            "parallel" => Ok(StreamingMode::Parallel),
            other => bail!("unsupported invocation mode '{other}'"),
        })
        .transpose()?;
    Ok(Invocation::new(
        FunctionId::ProjectNameContent(ProjectNameContent {
            project_name: project_name.to_owned(),
            slug,
            version: version.map(ToOwned::to_owned),
        }),
        InvokeApi {
            input,
            messages,
            mode: mode.map(Some),
            ..Default::default()
        },
    ))
}

#[cfg(test)]
mod tests {
    use super::{build_invocation, resolve_mode};
    use serde_json::json;

    #[test]
    fn typed_invocation_preserves_cli_request_shape() {
        let request = build_invocation(
            "test project",
            "test-slug".into(),
            Some(json!({"key":"value"})),
            &["hello".into()],
            Some("json"),
            Some("version-1"),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(request).unwrap(),
            json!({"project_name":"test project", "slug":"test-slug", "version":"version-1", "input":{"key":"value"}, "messages":[{"role":"user", "content":"hello"}], "mode":"json"})
        );
    }

    #[test]
    fn resolves_invoke_mode() {
        assert_eq!(resolve_mode(None, true), Some("json"));
        assert_eq!(resolve_mode(Some("text"), true), Some("text"));
        assert_eq!(resolve_mode(None, false), None);
    }
}
