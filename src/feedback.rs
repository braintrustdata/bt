use std::io::{self, IsTerminal, Read};

use anyhow::{bail, Context, Result};
use braintrust_api_client::backend::mcp::McpSession;
use clap::Args;
use serde_json::{json, Value};

use crate::{args::BaseArgs, auth, mcp};

#[derive(Debug, Clone, Args)]
#[command(
    after_help = "Feedback is sent to the Braintrust team. Describe what you tried, what happened, and what you expected. Do not include trace contents, prompts, model inputs or outputs, query results, or other project data.\n\nExamples:\n  bt feedback \"The error did not explain how to select a project\"\n  bt feedback \"The option was rejected\" --context '{\"command\":\"bt projects list\"}'\n  cat report.txt | bt feedback"
)]
pub struct FeedbackArgs {
    /// Feedback message (or provide it on stdin)
    message: Option<String>,
    /// Optional structured context as a JSON object
    #[arg(long, value_parser = parse_context)]
    context: Option<Value>,
}

fn parse_context(raw: &str) -> Result<Value, String> {
    let value: Value = serde_json::from_str(raw).map_err(|e| e.to_string())?;
    if !value.is_object() {
        return Err("context must be a JSON object".into());
    }
    Ok(value)
}

pub async fn run(base: BaseArgs, args: FeedbackArgs) -> Result<()> {
    let message = match args.message {
        Some(message) => message,
        None if io::stdin().is_terminal() && base.login.no_input => {
            bail!("provide a message to `bt feedback` when --no-input is set")
        }
        None if io::stdin().is_terminal() => dialoguer::Input::<String>::new()
            .with_prompt("Feedback for the Braintrust team")
            .interact_text()?,
        None => {
            let mut message = String::new();
            io::stdin()
                .read_to_string(&mut message)
                .context("failed to read feedback from stdin")?;
            message
        }
    };
    if message.trim().is_empty() {
        bail!("feedback cannot be empty; pass a message or pipe one to `bt feedback`");
    }
    let mut arguments = json!({"feedback": message});
    if let Some(context) = args.context {
        arguments["context"] = context;
    }
    let auth = auth::fast_login(&base).await?;
    let api_key = auth
        .login
        .api_key()
        .context("login state missing API key")?;
    let mut client = McpSession::new(&auth.api_url, &api_key, "bt", env!("CARGO_PKG_VERSION"))?;
    client.initialize().await?;
    let result = client
        .call_tool("send_feedback", arguments)
        .await
        .context("failed to send feedback")?;
    let output = mcp::format_tool_result(&result, base.json)?;
    if !output.is_empty() {
        println!("{output}");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn context_must_be_an_object() {
        assert!(parse_context(r#"{"command":"bt status"}"#).is_ok());
        assert!(parse_context("[]").is_err());
        assert!(parse_context("invalid").is_err());
    }

    #[tokio::test]
    async fn sends_feedback_to_mcp_and_propagates_tool_errors() {
        use actix_web::{web, App, HttpResponse, HttpServer};
        use std::sync::{Arc, Mutex};

        let calls = Arc::new(Mutex::new(Vec::new()));
        let captured = calls.clone();
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = HttpServer::new(move || {
            let captured = captured.clone();
            App::new().route("/mcp", web::post().to(move |body: web::Json<Value>| {
                let captured = captured.clone();
                async move {
                    let result = match body["method"].as_str().unwrap() {
                        "initialize" => json!({"protocolVersion":"2025-03-26"}),
                        "notifications/initialized" => return HttpResponse::Accepted().finish(),
                        "tools/call" => {
                            let mut calls = captured.lock().unwrap();
                            calls.push(body["params"].clone());
                            if calls.len() == 1 {
                                json!({"content":[{"type":"text","text":"Feedback recorded."}]})
                            } else {
                                json!({"isError":true,"content":[{"type":"text","text":"unavailable"}]})
                            }
                        }
                        other => panic!("unexpected method {other}"),
                    };
                    HttpResponse::Ok().json(json!({"jsonrpc":"2.0","id":body["id"],"result":result}))
                }
            }))
        }).workers(1).listen(listener).unwrap().run();
        let handle = server.handle();
        tokio::spawn(server);
        let mut base = BaseArgs::default();
        base.login.api_url = Some(url);
        base.login.api_key = Some("test-key".into());
        let args = FeedbackArgs {
            message: Some("test report".into()),
            context: Some(json!({"command":"bt status"})),
        };
        run(base.clone(), args.clone()).await.unwrap();
        assert!(run(base, args).await.is_err());
        assert_eq!(
            calls.lock().unwrap()[0],
            json!({"name":"send_feedback","arguments":{"feedback":"test report","context":{"command":"bt status"}}})
        );
        handle.stop(false).await;
    }
}
