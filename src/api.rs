use std::fs;
use std::io::{self, Read, Write};
use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use reqwest::Method;
use serde::Serialize;
use serde_json::Value;

use crate::args::BaseArgs;
use crate::auth::login;
use crate::http::{ApiClient, HttpError, RawRequestBody, ServiceBase};

const OPENAPI_SPEC_URL: &str =
    "https://raw.githubusercontent.com/braintrustdata/braintrust-openapi/main/openapi/spec.json";

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt api get /v1/project
  bt api post /v1/project_score --body '{\"name\":\"example\"}'
  bt api post /api/project_score/register --base app --body-file payload.json
  bt api spec --filter project_score
")]
pub struct ApiArgs {
    #[command(subcommand)]
    command: ApiCommand,
}

#[derive(Debug, Clone, Subcommand)]
enum ApiCommand {
    /// Send an authenticated GET request
    Get(ReadRequestArgs),
    /// Send an authenticated POST request
    Post(WriteRequestArgs),
    /// Send an authenticated PUT request
    Put(WriteRequestArgs),
    /// Send an authenticated PATCH request
    Patch(WriteRequestArgs),
    /// Send an authenticated DELETE request
    Delete(ReadRequestArgs),
    /// Fetch and inspect the Braintrust OpenAPI spec
    Spec(SpecArgs),
}

#[derive(Debug, Clone, Args)]
struct ReadRequestArgs {
    #[command(flatten)]
    target: RequestTargetArgs,
}

#[derive(Debug, Clone, Args)]
struct WriteRequestArgs {
    #[command(flatten)]
    target: RequestTargetArgs,

    #[command(flatten)]
    body: RequestBodyArgs,
}

#[derive(Debug, Clone, Args)]
struct RequestTargetArgs {
    /// Relative path to request (for example: /v1/project or /api/project_score/register)
    #[arg(value_name = "PATH")]
    path: String,

    /// Which Braintrust base URL to target
    #[arg(long, value_enum, default_value_t = ApiBase::Auto)]
    base: ApiBase,

    /// Extra request header in Name: Value form
    #[arg(long = "header", value_parser = parse_header_arg, value_name = "NAME:VALUE")]
    headers: Vec<HeaderArg>,
}

#[derive(Debug, Clone, Args)]
struct RequestBodyArgs {
    /// Inline request body
    #[arg(long, conflicts_with = "body_file", value_name = "BODY")]
    body: Option<String>,

    /// Read request body from a file, or '-' for stdin
    #[arg(long, conflicts_with = "body", value_name = "FILE")]
    body_file: Option<PathBuf>,

    /// Content-Type to send when a body is present
    #[arg(long, value_name = "MIME_TYPE")]
    content_type: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct SpecArgs {
    /// Print the raw JSON OpenAPI document
    #[arg(long)]
    raw: bool,

    /// Filter operations by path, method, summary, or operation id
    #[arg(long, value_name = "TEXT")]
    filter: Option<String>,

    /// Override the source URL for the spec
    #[arg(long, hide = true, value_name = "URL")]
    source_url: Option<String>,
}

#[derive(Debug, Clone, Copy, ValueEnum, Eq, PartialEq)]
enum ApiBase {
    Auto,
    Api,
    App,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct HeaderArg {
    name: String,
    value: String,
}

pub async fn run(base: BaseArgs, args: ApiArgs) -> Result<()> {
    match args.command {
        ApiCommand::Spec(args) => run_spec(base.json, args).await,
        command => {
            let ctx = login(&base).await?;
            let client = ApiClient::new(&ctx)?;
            run_authenticated_command(&client, command).await
        }
    }
}

async fn run_authenticated_command(client: &ApiClient, command: ApiCommand) -> Result<()> {
    match command {
        ApiCommand::Get(args) => run_request(client, Method::GET, args.target, None).await,
        ApiCommand::Post(args) => {
            run_request(
                client,
                Method::POST,
                args.target,
                load_request_body(args.body)?,
            )
            .await
        }
        ApiCommand::Put(args) => {
            run_request(
                client,
                Method::PUT,
                args.target,
                load_request_body(args.body)?,
            )
            .await
        }
        ApiCommand::Patch(args) => {
            run_request(
                client,
                Method::PATCH,
                args.target,
                load_request_body(args.body)?,
            )
            .await
        }
        ApiCommand::Delete(args) => run_request(client, Method::DELETE, args.target, None).await,
        ApiCommand::Spec(_) => unreachable!("spec commands are handled before auth is loaded"),
    }
}

async fn run_request(
    client: &ApiClient,
    method: Method,
    args: RequestTargetArgs,
    body: Option<RawRequestBody>,
) -> Result<()> {
    if is_absolute_url(&args.path) {
        bail!("absolute URLs are not supported; pass a relative path such as /v1/project");
    }

    let service = resolve_service_base(&args.path, args.base);
    let headers = args
        .headers
        .into_iter()
        .map(|header| (header.name, header.value))
        .collect::<Vec<_>>();

    let response = client
        .request_raw(method, service, &args.path, &headers, body)
        .await?;
    let status = response.status();
    let bytes = response
        .bytes()
        .await
        .context("failed to read response body")?;
    if !status.is_success() {
        let body = String::from_utf8_lossy(&bytes).into_owned();
        return Err(HttpError { status, body }.into());
    }

    if !bytes.is_empty() {
        io::stdout()
            .write_all(&bytes)
            .context("failed to write response body")?;
    }
    Ok(())
}

async fn run_spec(json: bool, args: SpecArgs) -> Result<()> {
    let source_url = args
        .source_url
        .unwrap_or_else(|| OPENAPI_SPEC_URL.to_string());
    let spec = fetch_openapi_spec(&source_url).await?;
    if args.raw {
        print!("{spec}");
        return Ok(());
    }

    let operations = filter_operations(&extract_operations(&spec)?, args.filter.as_deref());
    if json {
        println!(
            "{}",
            serde_json::to_string(&SpecOutput {
                source_url,
                operations,
            })?
        );
        return Ok(());
    }

    for operation in operations {
        println!("{}", format_operation_line(&operation));
    }
    Ok(())
}

fn load_request_body(args: RequestBodyArgs) -> Result<Option<RawRequestBody>> {
    let RequestBodyArgs {
        body,
        body_file,
        content_type,
    } = args;

    let body_bytes = if let Some(body) = body {
        Some(body.into_bytes())
    } else if let Some(path) = body_file {
        Some(read_body_source(&path)?)
    } else {
        None
    };

    if body_bytes.is_none() {
        if content_type.is_some() {
            bail!("--content-type requires --body or --body-file");
        }
        return Ok(None);
    }

    Ok(body_bytes.map(|bytes| RawRequestBody {
        bytes,
        content_type: Some(content_type.unwrap_or_else(|| "application/json".to_string())),
    }))
}

fn read_body_source(path: &PathBuf) -> Result<Vec<u8>> {
    if path.as_os_str() == "-" {
        let mut bytes = Vec::new();
        io::stdin()
            .read_to_end(&mut bytes)
            .context("failed to read request body from stdin")?;
        return Ok(bytes);
    }

    fs::read(path).with_context(|| format!("failed to read request body from {}", path.display()))
}

fn parse_header_arg(value: &str) -> Result<HeaderArg, String> {
    let Some((name, raw_value)) = value.split_once(':') else {
        return Err("header must be in Name: Value form".to_string());
    };

    let name = name.trim();
    if name.is_empty() {
        return Err("header name cannot be empty".to_string());
    }

    Ok(HeaderArg {
        name: name.to_string(),
        value: raw_value.trim().to_string(),
    })
}

fn resolve_service_base(path: &str, base: ApiBase) -> ServiceBase {
    match base {
        ApiBase::Api => ServiceBase::Api,
        ApiBase::App => ServiceBase::App,
        ApiBase::Auto => {
            if is_app_path(path) {
                ServiceBase::App
            } else {
                ServiceBase::Api
            }
        }
    }
}

fn is_app_path(path: &str) -> bool {
    let trimmed = path.trim_start_matches('/');
    trimmed == "api" || trimmed.starts_with("api/")
}

fn is_absolute_url(path: &str) -> bool {
    path.starts_with("http://") || path.starts_with("https://")
}

#[derive(Debug, Clone, Serialize, Eq, PartialEq)]
struct SpecOperation {
    method: String,
    path: String,
    operation_id: Option<String>,
    summary: Option<String>,
}

#[derive(Debug, Serialize)]
struct SpecOutput {
    source_url: String,
    operations: Vec<SpecOperation>,
}

async fn fetch_openapi_spec(source_url: &str) -> Result<String> {
    let response = reqwest::Client::new()
        .get(source_url)
        .send()
        .await
        .with_context(|| format!("failed to fetch OpenAPI spec from {source_url}"))?;
    let status = response.status();
    let body = response
        .text()
        .await
        .context("failed to read OpenAPI spec response body")?;
    if !status.is_success() {
        bail!("failed to fetch OpenAPI spec ({status}): {body}");
    }
    Ok(body)
}

fn extract_operations(spec_json: &str) -> Result<Vec<SpecOperation>> {
    let spec: Value = serde_json::from_str(spec_json).context("failed to parse OpenAPI spec")?;
    let paths = spec
        .get("paths")
        .and_then(Value::as_object)
        .ok_or_else(|| anyhow::anyhow!("OpenAPI spec is missing the top-level `paths` object"))?;

    let mut operations = Vec::new();
    for (path, path_item) in paths {
        let Some(path_item) = path_item.as_object() else {
            continue;
        };

        for method in ["get", "post", "put", "patch", "delete", "options", "head"] {
            let Some(operation) = path_item.get(method).and_then(Value::as_object) else {
                continue;
            };
            operations.push(SpecOperation {
                method: method.to_uppercase(),
                path: path.clone(),
                operation_id: operation
                    .get("operationId")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
                summary: operation
                    .get("summary")
                    .and_then(Value::as_str)
                    .map(ToOwned::to_owned),
            });
        }
    }

    operations.sort_by(|left, right| {
        left.path
            .cmp(&right.path)
            .then_with(|| left.method.cmp(&right.method))
    });
    Ok(operations)
}

fn filter_operations(operations: &[SpecOperation], filter: Option<&str>) -> Vec<SpecOperation> {
    let Some(filter) = filter.map(str::trim).filter(|value| !value.is_empty()) else {
        return operations.to_vec();
    };
    let needle = filter.to_ascii_lowercase();

    operations
        .iter()
        .filter(|operation| operation_matches_filter(operation, &needle))
        .cloned()
        .collect()
}

fn operation_matches_filter(operation: &SpecOperation, needle: &str) -> bool {
    [
        operation.method.as_str(),
        operation.path.as_str(),
        operation.operation_id.as_deref().unwrap_or(""),
        operation.summary.as_deref().unwrap_or(""),
    ]
    .into_iter()
    .any(|value| value.to_ascii_lowercase().contains(needle))
}

fn format_operation_line(operation: &SpecOperation) -> String {
    let mut line = format!("{} {}", operation.method, operation.path);
    let detail = operation
        .summary
        .as_deref()
        .or(operation.operation_id.as_deref())
        .unwrap_or("");
    if !detail.is_empty() {
        line.push_str("  ");
        line.push_str(detail);
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE_SPEC: &str = r#"{
      "paths": {
        "/v1/project_score": {
          "get": {
            "summary": "List project scores",
            "operationId": "listProjectScores"
          },
          "post": {
            "summary": "Create a project score",
            "operationId": "createProjectScore"
          }
        },
        "/brainstore/automation/reset-cursors": {
          "post": {
            "summary": "Reset automation cursors",
            "operationId": "resetAutomationCursors"
          }
        }
      }
    }"#;

    #[test]
    fn parse_header_arg_splits_name_and_value() {
        let header = parse_header_arg("X-Test: hello").expect("parse header");
        assert_eq!(
            header,
            HeaderArg {
                name: "X-Test".to_string(),
                value: "hello".to_string(),
            }
        );
    }

    #[test]
    fn parse_header_arg_rejects_invalid_input() {
        let err = parse_header_arg("invalid").expect_err("expected parse failure");
        assert!(err.contains("Name: Value"));
    }

    #[test]
    fn resolve_service_base_uses_app_for_next_api_routes() {
        assert_eq!(
            resolve_service_base("/api/project_score/register", ApiBase::Auto),
            ServiceBase::App
        );
        assert_eq!(
            resolve_service_base("api/foo", ApiBase::Auto),
            ServiceBase::App
        );
    }

    #[test]
    fn resolve_service_base_uses_api_for_non_app_routes() {
        assert_eq!(
            resolve_service_base("/v1/project_score", ApiBase::Auto),
            ServiceBase::Api
        );
        assert_eq!(
            resolve_service_base("/brainstore/automation/reset-cursors", ApiBase::Auto),
            ServiceBase::Api
        );
    }

    #[test]
    fn load_request_body_defaults_to_json_content_type() {
        let body = load_request_body(RequestBodyArgs {
            body: Some("{\"ok\":true}".to_string()),
            body_file: None,
            content_type: None,
        })
        .expect("load request body")
        .expect("expected request body");

        assert_eq!(body.content_type.as_deref(), Some("application/json"));
        assert_eq!(body.bytes, br#"{"ok":true}"#);
    }

    #[test]
    fn load_request_body_rejects_content_type_without_body() {
        let err = load_request_body(RequestBodyArgs {
            body: None,
            body_file: None,
            content_type: Some("application/json".to_string()),
        })
        .expect_err("expected failure");

        assert!(err.to_string().contains("--content-type requires"));
    }

    #[test]
    fn extract_operations_reads_methods_and_metadata() {
        let operations = extract_operations(SAMPLE_SPEC).expect("extract operations");
        assert_eq!(
            operations,
            vec![
                SpecOperation {
                    method: "POST".to_string(),
                    path: "/brainstore/automation/reset-cursors".to_string(),
                    operation_id: Some("resetAutomationCursors".to_string()),
                    summary: Some("Reset automation cursors".to_string()),
                },
                SpecOperation {
                    method: "GET".to_string(),
                    path: "/v1/project_score".to_string(),
                    operation_id: Some("listProjectScores".to_string()),
                    summary: Some("List project scores".to_string()),
                },
                SpecOperation {
                    method: "POST".to_string(),
                    path: "/v1/project_score".to_string(),
                    operation_id: Some("createProjectScore".to_string()),
                    summary: Some("Create a project score".to_string()),
                },
            ]
        );
    }

    #[test]
    fn filter_operations_matches_path_method_summary_and_operation_id() {
        let operations = extract_operations(SAMPLE_SPEC).expect("extract operations");

        let by_path = filter_operations(&operations, Some("project_score"));
        assert_eq!(by_path.len(), 2);

        let by_method = filter_operations(&operations, Some("post"));
        assert_eq!(by_method.len(), 2);

        let by_summary = filter_operations(&operations, Some("reset automation"));
        assert_eq!(by_summary.len(), 1);

        let by_operation_id = filter_operations(&operations, Some("createProjectScore"));
        assert_eq!(by_operation_id.len(), 1);
        assert_eq!(by_operation_id[0].path, "/v1/project_score");
    }

    #[test]
    fn format_operation_line_prefers_summary() {
        let line = format_operation_line(&SpecOperation {
            method: "POST".to_string(),
            path: "/v1/project_score".to_string(),
            operation_id: Some("createProjectScore".to_string()),
            summary: Some("Create a project score".to_string()),
        });

        assert_eq!(line, "POST /v1/project_score  Create a project score");
    }
}
