use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Arg, ArgAction, ArgMatches, Command, CommandFactory, FromArgMatches};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::{args::BaseArgs, auth, http::build_http_client, Cli};

const PROTOCOL_VERSION: &str = "2025-03-26";

#[derive(Debug, Clone, Deserialize)]
struct Tool {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default, rename = "inputSchema")]
    input_schema: Value,
}

#[derive(Debug, Deserialize)]
struct ToolsResult {
    tools: Vec<Tool>,
    #[serde(default, rename = "nextCursor")]
    next_cursor: Option<String>,
}

#[derive(Debug, Deserialize)]
struct RpcResponse {
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<RpcError>,
}

#[derive(Debug, Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

#[derive(Debug, Serialize)]
struct RpcRequest<'a> {
    jsonrpc: &'static str,
    id: u64,
    method: &'a str,
    params: Value,
}

struct McpClient {
    http: reqwest::Client,
    endpoint: String,
    api_key: String,
    session_id: Option<String>,
    protocol_version: Option<String>,
    next_id: u64,
}

pub fn try_main(argv: &[std::ffi::OsString]) -> Result<()> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build async runtime")?;
    runtime.block_on(run(argv))
}

async fn run(argv: &[std::ffi::OsString]) -> Result<()> {
    let command = Cli::command();
    let global_args: Vec<Arg> = command
        .find_subcommand("status")
        .context("status command is unavailable")?
        .get_arguments()
        .filter(|arg| arg.is_global_set())
        .cloned()
        .collect();
    // The MCP namespace is intentionally discovered only on this path. Resolve
    // the same profile/API-key configuration used by the rest of the CLI.
    let mut preflight = Command::new("bt")
        .disable_help_flag(true)
        .disable_version_flag(true);
    for arg in &global_args {
        preflight = preflight.arg(arg.clone());
    }
    let preflight = preflight.subcommand(Command::new("mcp").allow_external_subcommands(true));
    let preflight_argv: Vec<OsString> = argv
        .iter()
        .filter(|arg| !matches!(arg.to_str(), Some("--help" | "-h" | "--version" | "-V")))
        .cloned()
        .collect();
    let matches = preflight
        .try_get_matches_from(preflight_argv)
        .map_err(|err| anyhow!(err.to_string()))?;
    let mut base =
        BaseArgs::from_arg_matches(&matches).context("failed to parse global options")?;
    crate::apply_base_arg_sources(&matches, &mut base.login);
    if !base.login.quiet && !base.login.verbose {
        base.login.quiet = true;
    } else if base.login.quiet {
        base.login.verbose = false;
    }
    crate::configure_output(&base.login);
    crate::apply_runtime_env_overrides(&base.login);
    let auth = auth::fast_login(&base).await?;
    let endpoint = format!("{}/mcp", auth.api_url.trim_end_matches('/'));
    let api_key = auth
        .login
        .api_key()
        .context("login state missing API key")?;
    let mut client = McpClient::new(&endpoint, &api_key)?;
    client.initialize().await?;
    let tools = client.list_tools().await?;

    let mut mcp = Command::new("mcp")
        .about("Run tools exposed by the Braintrust MCP server")
        .subcommand_required(true)
        .arg_required_else_help(true);
    let mut input_maps = BTreeMap::new();
    for (index, tool) in tools.iter().enumerate() {
        let (tool_command, arg_map) = tool_command(tool, index)?;
        input_maps.insert(tool.name.clone(), arg_map);
        mcp = mcp.subcommand(tool_command);
    }
    let mut command = Command::new("bt")
        .version(crate::CLI_VERSION)
        .subcommand(mcp);
    for arg in global_args {
        command = command.arg(arg);
    }
    let matches = command
        .try_get_matches_from(argv)
        .map_err(|err| anyhow!(err.to_string()))?;
    let base = BaseArgs::from_arg_matches(&matches).context("failed to parse global options")?;
    let Some((tool_name, tool_matches)) = matches
        .subcommand_matches("mcp")
        .and_then(ArgMatches::subcommand)
    else {
        return Ok(());
    };
    let arg_map = input_maps
        .get(tool_name)
        .context("tool arguments unavailable")?;
    let arguments = arguments_from_matches(tool_matches, arg_map)?;
    let result = client.call_tool(tool_name, arguments).await?;
    if base.json {
        println!("{}", serde_json::to_string(&result)?);
    } else if let Some(content) = result.get("content") {
        for item in content.as_array().into_iter().flatten() {
            if let Some(text) = item.get("text").and_then(Value::as_str) {
                println!("{text}");
            } else {
                println!("{}", serde_json::to_string(item)?);
            }
        }
        if let Some(structured) = result.get("structuredContent") {
            println!("{}", serde_json::to_string_pretty(structured)?);
        }
    } else {
        println!("{}", serde_json::to_string_pretty(&result)?);
    }
    Ok(())
}

fn tool_command(
    tool: &Tool,
    tool_index: usize,
) -> Result<(Command, BTreeMap<String, (String, Value)>)> {
    if !is_cli_name(&tool.name) || tool.name == "help" {
        bail!(
            "MCP tool name `{}` cannot be represented as a CLI command",
            tool.name
        );
    }
    let mut command = Command::new(tool.name.clone());
    if let Some(description) = &tool.description {
        command = command.about(description.clone());
    }
    let properties = tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let required: Vec<String> = tool
        .input_schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .map(str::to_owned)
        .collect();
    let mut arg_map = BTreeMap::new();
    let mut flags = BTreeSet::new();
    for (field_index, (name, schema)) in properties.iter().enumerate() {
        let id = format!("mcp_{tool_index}_{field_index}");
        let flag = name.replace('_', "-");
        if !is_cli_name(&flag) || flag == "help" {
            bail!(
                "MCP argument `{name}` on tool `{}` cannot be represented as a CLI flag",
                tool.name
            );
        }
        if !flags.insert(flag.clone()) {
            bail!(
                "MCP tool `{}` has colliding CLI flags after underscore normalization",
                tool.name
            );
        }
        let description = schema
            .get("description")
            .and_then(Value::as_str)
            .unwrap_or("");
        let is_boolean = schema.get("type").and_then(Value::as_str) == Some("boolean");
        let mut arg = Arg::new(id.clone())
            .long(flag)
            .help(description.to_string());
        if required.iter().any(|required| required == name) {
            arg = arg.required(true);
        }
        if is_boolean {
            arg = arg
                .action(ArgAction::Set)
                .value_parser(clap::value_parser!(bool))
                .value_name("BOOL");
        } else {
            arg = arg.value_name(name.to_ascii_uppercase());
        }
        command = command.arg(arg);
        arg_map.insert(id, (name.clone(), schema.clone()));
    }
    Ok((command, arg_map))
}

fn is_cli_name(name: &str) -> bool {
    !name.is_empty()
        && name.as_bytes()[0].is_ascii_alphanumeric()
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_'))
}

fn arguments_from_matches(
    matches: &ArgMatches,
    arg_map: &BTreeMap<String, (String, Value)>,
) -> Result<Value> {
    let mut args = Map::new();
    for (id, (name, schema)) in arg_map {
        let is_boolean = schema.get("type").and_then(Value::as_str) == Some("boolean");
        if is_boolean {
            if let Some(value) = matches.get_one::<bool>(id) {
                args.insert(name.clone(), Value::Bool(*value));
            }
            continue;
        }
        if let Some(raw) = matches.get_one::<String>(id) {
            let value = parse_argument_value(raw, schema)
                .with_context(|| format!("invalid value for --{}", name.replace('_', "-")))?;
            args.insert(name.clone(), value);
        }
    }
    Ok(Value::Object(args))
}

fn parse_argument_value(raw: &str, schema: &Value) -> Result<Value> {
    match schema.get("type").and_then(Value::as_str) {
        Some("integer") => Ok(Value::Number(raw.parse::<i64>()?.into())),
        Some("number") => serde_json::Number::from_f64(raw.parse::<f64>()?)
            .map(Value::Number)
            .context("number must be finite"),
        Some("boolean") => match raw {
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            _ => bail!("expected true or false"),
        },
        Some("object" | "array") | None => {
            serde_json::from_str(raw).context("expected a JSON object or array")
        }
        _ => Ok(Value::String(raw.to_owned())),
    }
}

impl McpClient {
    fn new(endpoint: &str, api_key: &str) -> Result<Self> {
        Ok(Self {
            http: build_http_client(std::time::Duration::from_secs(60))?,
            endpoint: endpoint.to_owned(),
            api_key: api_key.to_owned(),
            session_id: None,
            protocol_version: None,
            next_id: 1,
        })
    }

    async fn initialize(&mut self) -> Result<()> {
        let response = self
            .request(
                "initialize",
                json!({
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": { "name": "bt", "version": env!("CARGO_PKG_VERSION") }
                }),
            )
            .await?;
        self.protocol_version = response
            .get("protocolVersion")
            .and_then(Value::as_str)
            .map(str::to_owned);
        if self.protocol_version.is_none() {
            bail!("MCP server returned an invalid initialize response");
        }
        self.notify("notifications/initialized", json!({})).await?;
        Ok(())
    }

    async fn list_tools(&mut self) -> Result<Vec<Tool>> {
        let mut tools = Vec::new();
        let mut cursor: Option<String> = None;
        loop {
            let mut params = Map::new();
            if let Some(value) = cursor.take() {
                params.insert("cursor".to_string(), Value::String(value));
            }
            let value = self.request("tools/list", Value::Object(params)).await?;
            let page: ToolsResult =
                serde_json::from_value(value).context("invalid tools/list response")?;
            tools.extend(page.tools);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        Ok(tools)
    }

    async fn call_tool(&mut self, name: &str, arguments: Value) -> Result<Value> {
        let result = self
            .request(
                "tools/call",
                json!({ "name": name, "arguments": arguments }),
            )
            .await?;
        if result.get("isError").and_then(Value::as_bool) == Some(true) {
            bail!(
                "MCP tool `{name}` failed: {}",
                result
                    .get("content")
                    .map(|v| v.to_string())
                    .unwrap_or_default()
            );
        }
        Ok(result)
    }

    async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        let request = RpcRequest {
            jsonrpc: "2.0",
            id,
            method,
            params,
        };
        let response = self.post(serde_json::to_value(request)?).await?;
        let rpc: RpcResponse =
            serde_json::from_value(response).context("invalid MCP JSON-RPC response")?;
        if let Some(error) = rpc.error {
            bail!("MCP server error {}: {}", error.code, error.message);
        }
        rpc.result.context("MCP server response had no result")
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<()> {
        let value = json!({ "jsonrpc": "2.0", "method": method, "params": params });
        let request = self.request_builder(value).await?;
        let response = request.send().await.context("MCP notification failed")?;
        if !response.status().is_success() && response.status() != reqwest::StatusCode::ACCEPTED {
            bail!("MCP notification failed ({})", response.status());
        }
        self.capture_session(&response);
        Ok(())
    }

    async fn post(&mut self, value: Value) -> Result<Value> {
        let response = self
            .request_builder(value)
            .await?
            .send()
            .await
            .context("MCP request failed")?;
        self.capture_session(&response);
        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            bail!("MCP request failed ({status}): {body}");
        }
        let body = response
            .text()
            .await
            .context("failed reading MCP response")?;
        parse_rpc_body(&body)
    }

    async fn request_builder(&self, value: Value) -> Result<reqwest::RequestBuilder> {
        let mut request = self
            .http
            .post(&self.endpoint)
            .bearer_auth(&self.api_key)
            .header(
                reqwest::header::ACCEPT,
                "application/json, text/event-stream",
            )
            .json(&value);
        if let Some(session_id) = &self.session_id {
            request = request.header("Mcp-Session-Id", session_id);
        }
        if let Some(protocol_version) = &self.protocol_version {
            request = request.header("MCP-Protocol-Version", protocol_version);
        }
        Ok(request)
    }

    fn capture_session(&mut self, response: &reqwest::Response) {
        if let Some(value) = response
            .headers()
            .get("Mcp-Session-Id")
            .and_then(|v| v.to_str().ok())
        {
            self.session_id = Some(value.to_owned());
        }
    }
}

fn parse_rpc_body(body: &str) -> Result<Value> {
    if let Ok(value) = serde_json::from_str(body) {
        return Ok(value);
    }
    for line in body.lines() {
        if let Some(data) = line.strip_prefix("data:").map(str::trim) {
            if !data.is_empty() && data != "[DONE]" {
                return serde_json::from_str(data).context("invalid MCP event stream response");
            }
        }
    }
    Err(anyhow!(
        "MCP server returned an empty or unsupported response"
    ))
}
