use std::{
    collections::{BTreeMap, BTreeSet},
    ffi::OsString,
    io::Read,
};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Arg, ArgAction, ArgMatches, Command, CommandFactory, FromArgMatches};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::{args::BaseArgs, auth, http::build_http_client, Cli};

const PROTOCOL_VERSION: &str = "2025-03-26";

fn global_args() -> Result<Vec<Arg>> {
    Ok(Cli::command()
        .find_subcommand("status")
        .context("status command is unavailable")?
        .get_arguments()
        .filter(|arg| arg.is_global_set())
        .cloned()
        .collect())
}

fn global_option_specs(args: &[Arg]) -> Vec<(Option<String>, Option<char>, bool)> {
    args.iter()
        .map(|arg| {
            (
                arg.get_long().map(str::to_owned),
                arg.get_short(),
                matches!(arg.get_action(), ArgAction::Set | ArgAction::Append),
            )
        })
        .collect()
}

/// Detect the MCP namespace even when global options appear before it.
pub fn is_mcp_invocation(argv: &[OsString]) -> Result<bool> {
    let specs = global_option_specs(&global_args()?);
    let mut index = 1;
    while index < argv.len() {
        let Some(token) = argv[index].to_str() else {
            index += 1;
            continue;
        };
        if token == "--" {
            return Ok(false);
        }
        if token == "mcp" {
            return Ok(true);
        }
        if let Some((name, _)) = token.strip_prefix("--").and_then(|s| s.split_once('=')) {
            if specs
                .iter()
                .any(|(long, _, _)| long.as_deref() == Some(name))
            {
                index += 1;
                continue;
            }
        } else if let Some(name) = token.strip_prefix("--") {
            if let Some((_, _, takes_value)) = specs
                .iter()
                .find(|(long, _, _)| long.as_deref() == Some(name))
            {
                index += 1 + usize::from(*takes_value);
                continue;
            }
        } else if token.starts_with('-') && token.len() >= 2 {
            let short = token.chars().nth(1);
            if let Some((_, _, takes_value)) =
                specs.iter().find(|(_, candidate, _)| *candidate == short)
            {
                index += 1 + usize::from(*takes_value && token.len() == 2);
                continue;
            }
        }
        // The first non-global token is the root command. Do not mistake a
        // later positional or tool argument whose value happens to be `mcp`.
        return Ok(false);
    }
    Ok(false)
}

fn normalize_global_args(argv: &[OsString], args: &[Arg]) -> Vec<OsString> {
    let specs = global_option_specs(args);
    let mut normalized = Vec::with_capacity(argv.len());
    let mut global = Vec::new();
    if let Some(program) = argv.first() {
        normalized.push(program.clone());
    }
    let mut index = 1;
    while index < argv.len() {
        let token = argv[index].to_string_lossy();
        if token == "--" {
            normalized.extend(argv[index..].iter().cloned());
            break;
        }
        let matched = if let Some(name) = token.strip_prefix("--") {
            let name = name.split_once('=').map_or(name, |(name, _)| name);
            specs
                .iter()
                .find(|(long, _, _)| long.as_deref() == Some(name))
                .map(|(_, _, takes_value)| *takes_value)
                .map(|takes_value| (takes_value, token.contains('=')))
        } else if token.starts_with('-') && token.len() >= 2 {
            let short = token.chars().nth(1);
            specs
                .iter()
                .find(|(_, candidate, _)| *candidate == short)
                .map(|(_, _, takes_value)| *takes_value)
                .map(|takes_value| (takes_value, token.len() > 2))
        } else {
            None
        };
        if let Some((takes_value, attached_value)) = matched {
            global.push(argv[index].clone());
            index += 1;
            if takes_value && !attached_value && index < argv.len() {
                global.push(argv[index].clone());
                index += 1;
            }
        } else {
            normalized.push(argv[index].clone());
            index += 1;
        }
    }
    normalized.splice(1..1, global);
    normalized
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct Tool {
    name: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default, rename = "inputSchema")]
    input_schema: Value,
}

type ToolArgumentMap = BTreeMap<String, (String, Value)>;

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
    let global_args = global_args()?;
    let normalized_argv = normalize_global_args(argv, &global_args);
    // The MCP namespace is intentionally discovered only on this path. Resolve
    // the same profile/API-key configuration used by the rest of the CLI.
    let mut preflight = Command::new("bt")
        .disable_help_flag(true)
        .disable_version_flag(true);
    for arg in &global_args {
        preflight = preflight.arg(arg.clone());
    }
    let preflight = preflight.subcommand(Command::new("mcp").allow_external_subcommands(true));
    let preflight_argv: Vec<OsString> = normalized_argv
        .iter()
        .filter(|arg| !matches!(arg.to_str(), Some("--help" | "-h" | "--version" | "-V")))
        .cloned()
        .collect();
    let Some(matches) = parse_matches(preflight, preflight_argv)? else {
        return Ok(());
    };
    let mut base =
        BaseArgs::from_arg_matches(&matches).context("failed to parse global options")?;
    crate::apply_base_arg_sources(&matches, &mut base.login);
    base.login.profile_explicit = crate::args::has_explicit_profile_arg(argv);
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

    let mut run = Command::new("run")
        .about("Run a tool exposed by the Braintrust MCP server")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .arg(
            Arg::new("mcp_json_args")
                .long("args")
                .global(true)
                .value_name("JSON|@FILE|-")
                .help("Pass tool arguments as JSON, read JSON from @FILE, or read it from stdin with -"),
        );
    let mut input_maps = BTreeMap::new();
    for (index, tool) in tools.iter().enumerate() {
        let (tool_command, arg_map) = tool_command(tool, index)?;
        input_maps.insert(tool.name.clone(), arg_map);
        run = run.subcommand(tool_command);
    }
    let list = Command::new("list").about("List tools exposed by the Braintrust MCP server");
    let mcp = Command::new("mcp")
        .about("Discover and run tools exposed by the Braintrust MCP server")
        .subcommand_required(true)
        .arg_required_else_help(true)
        .subcommand(list)
        .subcommand(run);
    let mut command = Command::new("bt")
        .version(crate::CLI_VERSION)
        .subcommand(mcp);
    for arg in global_args {
        command = command.arg(arg);
    }
    let Some(matches) = parse_matches(command, normalized_argv.iter().cloned())? else {
        return Ok(());
    };
    let base = BaseArgs::from_arg_matches(&matches).context("failed to parse global options")?;
    let Some((mcp_subcommand, mcp_matches)) = matches
        .subcommand_matches("mcp")
        .and_then(ArgMatches::subcommand)
    else {
        return Ok(());
    };
    if mcp_subcommand == "list" {
        print_tool_list(&tools, base.json)?;
        return Ok(());
    }
    if mcp_subcommand != "run" {
        bail!("unknown MCP command `{mcp_subcommand}`");
    }
    let run_matches = mcp_matches;
    let Some((tool_name, tool_matches)) = run_matches.subcommand() else {
        return Ok(());
    };
    let arg_map = input_maps
        .get(tool_name)
        .context("tool arguments unavailable")?;
    let tool = tools
        .iter()
        .find(|tool| tool.name == tool_name)
        .context("tool schema unavailable")?;
    let arguments = if let Some(raw) = run_matches.get_one::<String>("mcp_json_args") {
        let value = read_json_arguments(raw)?;
        validate_json_arguments(&value, tool)?;
        value
    } else {
        let value = arguments_from_matches(tool_matches, arg_map)?;
        validate_json_arguments(&value, tool)?;
        value
    };
    let result = client.call_tool(tool_name, arguments).await?;
    let output = format_tool_result(&result, base.json)?;
    if !output.is_empty() {
        println!("{output}");
    }
    Ok(())
}

fn parse_matches(
    command: Command,
    argv: impl IntoIterator<Item = OsString>,
) -> Result<Option<ArgMatches>> {
    match command.try_get_matches_from(argv) {
        Ok(matches) => Ok(Some(matches)),
        Err(error)
            if matches!(
                error.kind(),
                clap::error::ErrorKind::DisplayHelp
                    | clap::error::ErrorKind::DisplayVersion
                    | clap::error::ErrorKind::DisplayHelpOnMissingArgumentOrSubcommand
            ) =>
        {
            error.print().context("failed to print command help")?;
            Ok(None)
        }
        Err(error) => Err(anyhow!(error.to_string())),
    }
}

fn tool_command(tool: &Tool, tool_index: usize) -> Result<(Command, ToolArgumentMap)> {
    if !is_cli_name(&tool.name) || tool.name == "help" {
        bail!(
            "MCP tool name `{}` cannot be represented as a CLI command",
            tool.name
        );
    }
    let mut command = Command::new(tool.name.clone());
    if let Some(description) = &tool.description {
        command = command
            .about(short_description(description))
            .long_about(description.clone());
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
        // Keep the stable JSON-input flag available when the server exposes a
        // field named `args`; that one field uses --tool-args instead.
        let flag = if name == "args" {
            "tool-args".to_owned()
        } else {
            name.replace('_', "-")
        };
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
            .conflicts_with("mcp_json_args")
            .help(description.to_string());
        if required.iter().any(|required| required == name) {
            arg = arg.required_unless_present("mcp_json_args");
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
    if !required.is_empty() {
        command = command.after_help(format!(
            "Required fields: {}. Provide them with generated flags or --args.",
            required.join(", ")
        ));
    }
    Ok((command, arg_map))
}

fn print_tool_list(tools: &[Tool], json_output: bool) -> Result<()> {
    if json_output {
        let value = json!({ "tools": tools });
        println!("{}", serde_json::to_string(&value)?);
    } else {
        for tool in tools {
            let description = tool
                .description
                .as_deref()
                .map(short_description)
                .unwrap_or_default();
            if description.is_empty() {
                println!("{}", tool.name);
            } else {
                println!("{}\t{}", tool.name, description);
            }
        }
    }
    Ok(())
}

fn read_json_arguments(raw: &str) -> Result<Value> {
    let input = if raw == "-" {
        let mut input = String::new();
        std::io::stdin()
            .read_to_string(&mut input)
            .context("failed to read MCP arguments from stdin")?;
        input
    } else if let Some(path) = raw.strip_prefix('@') {
        std::fs::read_to_string(path)
            .with_context(|| format!("failed to read MCP arguments from `{path}`"))?
    } else {
        raw.to_owned()
    };
    let value: Value = serde_json::from_str(&input).context("MCP arguments must be valid JSON")?;
    if !value.is_object() {
        bail!("MCP arguments must be a JSON object");
    }
    Ok(value)
}

fn validate_json_arguments(arguments: &Value, tool: &Tool) -> Result<()> {
    let arguments = arguments
        .as_object()
        .context("MCP arguments must be a JSON object")?;
    let properties = tool
        .input_schema
        .get("properties")
        .and_then(Value::as_object);
    let required = tool
        .input_schema
        .get("required")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str);
    for name in required {
        if !arguments.contains_key(name) {
            bail!(
                "missing required MCP argument `{name}` for tool `{}`",
                tool.name
            );
        }
    }
    if tool.input_schema.get("additionalProperties") == Some(&Value::Bool(false)) {
        if let Some(name) = arguments
            .keys()
            .find(|name| !properties.is_some_and(|properties| properties.contains_key(*name)))
        {
            bail!("unknown MCP argument `{name}` for tool `{}`", tool.name);
        }
    }
    if let Some(properties) = properties {
        for (name, value) in arguments {
            let Some(schema) = properties.get(name) else {
                continue;
            };
            let types = schema_types(schema);
            if !types.is_empty() && !value_matches_schema_type(value, &types) {
                bail!(
                    "MCP argument `{name}` has the wrong JSON type for tool `{}`",
                    tool.name
                );
            }
        }
    }
    Ok(())
}

fn short_description(description: &str) -> String {
    const MAX_LENGTH: usize = 120;

    let first_line = description
        .lines()
        .map(str::trim)
        .find(|line| !line.is_empty())
        .unwrap_or_default()
        .trim_start_matches('#')
        .trim_start_matches(['-', '*'])
        .trim();
    let sentence_end = first_line.char_indices().find_map(|(index, character)| {
        if matches!(character, '.' | '!' | '?')
            && first_line[index + character.len_utf8()..]
                .chars()
                .next()
                .is_none_or(char::is_whitespace)
        {
            Some(index + character.len_utf8())
        } else {
            None
        }
    });
    let summary = sentence_end
        .map(|end| &first_line[..end])
        .unwrap_or(first_line)
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ");

    let mut chars = summary.chars();
    let shortened: String = chars.by_ref().take(MAX_LENGTH).collect();
    if chars.next().is_some() {
        format!("{}…", shortened.trim_end())
    } else {
        shortened
    }
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
    let types = schema_types(schema);

    if raw == "null" && types.contains("null") {
        return Ok(Value::Null);
    }

    if types.len() == 1 {
        return parse_typed_argument(raw, types.iter().next().expect("one schema type"));
    }

    // For unions that include strings, accept normal CLI text while still
    // allowing JSON objects/arrays and explicitly quoted JSON strings.
    if types.contains("string") {
        if let Ok(value) = serde_json::from_str::<Value>(raw) {
            if value.is_string()
                || (value.is_object() && types.contains("object"))
                || (value.is_array() && types.contains("array"))
            {
                return Ok(value);
            }
        }
        return Ok(Value::String(raw.to_owned()));
    }

    let value: Value = serde_json::from_str(raw)
        .context("expected a JSON value matching one of the schema alternatives")?;
    if value_matches_schema_type(&value, &types) {
        Ok(value)
    } else {
        bail!("value does not match any supported schema alternative")
    }
}

fn parse_typed_argument(raw: &str, schema_type: &str) -> Result<Value> {
    match schema_type {
        "integer" => Ok(Value::Number(raw.parse::<i64>()?.into())),
        "number" => serde_json::Number::from_f64(raw.parse::<f64>()?)
            .map(Value::Number)
            .context("number must be finite"),
        "boolean" => match raw {
            "true" => Ok(Value::Bool(true)),
            "false" => Ok(Value::Bool(false)),
            _ => bail!("expected true or false"),
        },
        "object" | "array" => {
            let value: Value =
                serde_json::from_str(raw).context("expected a JSON object or array")?;
            if value_matches_schema_type(&value, &BTreeSet::from([schema_type.to_owned()])) {
                Ok(value)
            } else {
                bail!("expected a JSON {schema_type}")
            }
        }
        "null" => {
            if raw == "null" {
                Ok(Value::Null)
            } else {
                bail!("expected null")
            }
        }
        _ => Ok(Value::String(raw.to_owned())),
    }
}

fn schema_types(schema: &Value) -> BTreeSet<String> {
    let mut types = BTreeSet::new();
    collect_schema_types(schema, &mut types);
    types
}

fn collect_schema_types(schema: &Value, types: &mut BTreeSet<String>) {
    if let Some(schema_type) = schema.get("type") {
        match schema_type {
            Value::String(value) => {
                types.insert(value.clone());
            }
            Value::Array(values) => {
                types.extend(values.iter().filter_map(Value::as_str).map(str::to_owned));
            }
            _ => {}
        }
    }
    for key in ["anyOf", "oneOf"] {
        if let Some(alternatives) = schema.get(key).and_then(Value::as_array) {
            for alternative in alternatives {
                collect_schema_types(alternative, types);
            }
        }
    }
    if let Some(constant) = schema.get("const") {
        types.insert(json_value_type(constant).to_owned());
    }
    if let Some(values) = schema.get("enum").and_then(Value::as_array) {
        types.extend(values.iter().map(|value| json_value_type(value).to_owned()));
    }
}

fn json_value_type(value: &Value) -> &str {
    match value {
        Value::Null => "null",
        Value::Bool(_) => "boolean",
        Value::Number(number) if number.is_i64() || number.is_u64() => "integer",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

fn value_matches_schema_type(value: &Value, types: &BTreeSet<String>) -> bool {
    types.contains(json_value_type(value)) || (value.is_number() && types.contains("number"))
}

fn format_tool_result(result: &Value, json_output: bool) -> Result<String> {
    if json_output {
        return serde_json::to_string(result).context("failed to serialize MCP result");
    }

    // Structured content is the authoritative result when the server returns
    // both it and a text rendering in content; printing both duplicates output.
    if let Some(structured) = result
        .get("structuredContent")
        .filter(|value| !value.is_null())
    {
        return serde_json::to_string_pretty(structured)
            .context("failed to serialize structured MCP result");
    }

    if let Some(content) = result.get("content").and_then(Value::as_array) {
        return content
            .iter()
            .map(|item| {
                if let Some(text) = item.get("text").and_then(Value::as_str) {
                    Ok(text.to_owned())
                } else {
                    Ok(serde_json::to_string(item)?)
                }
            })
            .collect::<Result<Vec<_>>>()
            .map(|items| items.join("\n"))
            .context("failed to serialize MCP content");
    }

    serde_json::to_string_pretty(result).context("failed to serialize MCP result")
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_mcp_after_global_options() {
        for argv in [
            vec!["bt", "--profile", "work", "mcp", "search_docs"],
            vec!["bt", "--api-url=https://api.example.test", "mcp"],
            vec!["bt", "-pwork", "mcp", "search_docs"],
        ] {
            let argv = argv.into_iter().map(OsString::from).collect::<Vec<_>>();
            assert!(is_mcp_invocation(&argv).unwrap());
        }
    }

    #[test]
    fn does_not_treat_a_global_option_value_as_the_namespace() {
        let argv = ["bt", "--profile", "mcp", "status"]
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>();
        assert!(!is_mcp_invocation(&argv).unwrap());

        let positional = ["bt", "status", "mcp"]
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>();
        assert!(!is_mcp_invocation(&positional).unwrap());
    }

    #[test]
    fn moves_global_options_after_tool_before_namespace() {
        let args = global_args().unwrap();
        let argv = [
            "bt",
            "mcp",
            "search_docs",
            "--profile",
            "work",
            "--api-url",
            "https://api.example.test",
            "--query",
            "synthetic",
        ]
        .into_iter()
        .map(OsString::from)
        .collect::<Vec<_>>();
        let normalized = normalize_global_args(&argv, &args);
        assert_eq!(
            normalized,
            [
                "bt",
                "--profile",
                "work",
                "--api-url",
                "https://api.example.test",
                "mcp",
                "search_docs",
                "--query",
                "synthetic"
            ]
            .map(OsString::from)
        );

        let mut preflight = Command::new("bt");
        for arg in &args {
            preflight = preflight.arg(arg.clone());
        }
        preflight = preflight.subcommand(Command::new("mcp").allow_external_subcommands(true));
        assert!(preflight.try_get_matches_from(normalized).is_ok());
    }

    #[test]
    fn explicit_profile_after_dynamic_tool_is_preserved() {
        let argv = ["bt", "mcp", "search_docs", "--profile", "work"]
            .into_iter()
            .map(OsString::from)
            .collect::<Vec<_>>();
        assert!(crate::args::has_explicit_profile_arg(&argv));
    }

    #[test]
    fn generated_tool_flags_and_json_args_are_alternative_inputs() {
        let tool = Tool {
            name: "test_tool".to_owned(),
            description: Some("Synthetic test tool".to_owned()),
            input_schema: json!({
                "type": "object",
                "properties": {
                    "args": { "type": "string" },
                    "count": { "type": "integer" }
                },
                "required": ["args"]
            }),
        };
        let (tool_command, _) = tool_command(&tool, 0).unwrap();
        let run = Command::new("run")
            .subcommand_required(true)
            .arg_required_else_help(true)
            .arg(
                Arg::new("mcp_json_args")
                    .long("args")
                    .global(true)
                    .value_name("JSON|@FILE|-"),
            )
            .subcommand(tool_command);
        let parsed = run
            .clone()
            .try_get_matches_from(["run", "test_tool", "--args", r#"{"args":"x"}"#])
            .expect("JSON arguments parse");
        assert_eq!(
            parsed
                .get_one::<String>("mcp_json_args")
                .map(String::as_str),
            Some(r#"{"args":"x"}"#)
        );
        assert!(run
            .clone()
            .try_get_matches_from(["run", "test_tool", "--tool-args", "x"])
            .is_ok());
        assert!(run
            .try_get_matches_from([
                "run",
                "test_tool",
                "--args",
                r#"{"args":"x"}"#,
                "--tool-args",
                "y"
            ])
            .is_err());
    }

    #[test]
    fn json_arguments_require_object_and_validate_schema() {
        let tool = Tool {
            name: "test_tool".to_owned(),
            description: None,
            input_schema: json!({
                "type": "object",
                "properties": { "count": { "type": "integer" } },
                "required": ["count"],
                "additionalProperties": false
            }),
        };
        let arguments = read_json_arguments(r#"{"count":2}"#).unwrap();
        validate_json_arguments(&arguments, &tool).unwrap();
        assert!(read_json_arguments("[]").is_err());
        assert!(validate_json_arguments(&json!({}), &tool).is_err());
        assert!(validate_json_arguments(&json!({ "count": "2" }), &tool).is_err());
        assert!(validate_json_arguments(&json!({ "count": 2, "other": true }), &tool).is_err());
    }

    #[test]
    fn accepts_unquoted_strings_for_string_unions() {
        let uuid_union = json!({
            "anyOf": [
                { "type": "string", "format": "uuid" },
                { "type": "string", "const": "overview" }
            ]
        });
        assert_eq!(
            parse_argument_value("68191735-06de-4056-afe5-11f97dcc5988", &uuid_union).unwrap(),
            json!("68191735-06de-4056-afe5-11f97dcc5988")
        );
        assert_eq!(
            parse_argument_value("overview", &uuid_union).unwrap(),
            json!("overview")
        );
    }

    #[test]
    fn accepts_plain_strings_and_json_objects_for_string_object_unions() {
        let time_frame = json!({
            "anyOf": [
                { "type": "string", "enum": ["1h", "1d", "3d"] },
                { "type": "object", "required": ["from", "to"] }
            ]
        });
        assert_eq!(
            parse_argument_value("3d", &time_frame).unwrap(),
            json!("3d")
        );
        assert_eq!(
            parse_argument_value(r#"{"from":"2026-01-01","to":"2026-01-02"}"#, &time_frame)
                .unwrap(),
            json!({"from":"2026-01-01","to":"2026-01-02"})
        );
    }

    #[test]
    fn accepts_json_null_for_nullable_string_and_boolean() {
        let nullable_string = json!({ "type": ["string", "null"] });
        let nullable_boolean = json!({ "type": ["boolean", "null"] });
        assert_eq!(
            parse_argument_value("filter text", &nullable_string).unwrap(),
            json!("filter text")
        );
        assert_eq!(
            parse_argument_value("null", &nullable_string).unwrap(),
            Value::Null
        );
        assert_eq!(
            parse_argument_value("false", &nullable_boolean).unwrap(),
            json!(false)
        );
        assert_eq!(
            parse_argument_value("null", &nullable_boolean).unwrap(),
            Value::Null
        );
    }

    #[test]
    fn structured_result_is_not_repeated_as_text_content() {
        let result = json!({
            "content": [{ "type": "text", "text": "{\"views\":[]}" }],
            "structuredContent": { "views": [] }
        });
        assert_eq!(
            format_tool_result(&result, false).unwrap(),
            "{\n  \"views\": []\n}"
        );
        assert_eq!(
            format_tool_result(&result, true).unwrap(),
            result.to_string()
        );
    }

    #[test]
    fn content_only_results_keep_text_and_non_text_items() {
        let result = json!({
            "content": [
                { "type": "text", "text": "first" },
                { "type": "image", "data": "ZmFrZQ==", "mimeType": "image/png" }
            ]
        });
        let rendered = format_tool_result(&result, false).unwrap();
        assert!(rendered.starts_with("first\n"));
        assert!(rendered.contains("\"type\":\"image\""));
    }
}
