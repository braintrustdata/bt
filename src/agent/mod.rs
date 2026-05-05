use std::collections::BTreeSet;
use std::ffi::OsString;
use std::io::Write;
#[cfg(test)]
use std::sync::{Mutex, OnceLock};

use anyhow::Result;
use clap::{builder::ValueRange, Arg, ArgAction, Args, CommandFactory, Subcommand};
use serde_json::{Map, Value};

use crate::args::BaseArgs;

const DEFAULT_CANARY_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "-canary.dev");
const CLI_VERSION: &str = match option_env!("BT_VERSION_STRING") {
    Some(version) => version,
    None => DEFAULT_CANARY_VERSION,
};
const DOCS_URL: &str = "https://braintrust.dev/docs/reference/cli";
const GUIDE_BODY: &str = include_str!("guide.txt");

const AGENT_ENV_VARS: &[&str] = &[
    "CLAUDECODE",
    "CLAUDE_CODE",
    "CURSOR_AGENT",
    "CODEX",
    "OPENAI_CODEX",
    "OPENCODE",
    "AIDER",
    "CLINE",
    "WINDSURF_AGENT",
    "GITHUB_COPILOT",
    "AMAZON_Q",
    "AWS_Q_DEVELOPER",
    "GEMINI_CODE_ASSIST",
    "SRC_CODY",
    "AGENT",
];

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt agent schema
  bt agent schema --compact
  bt agent guide
  CLAUDE_CODE=true bt --help
")]
pub struct AgentArgs {
    #[command(subcommand)]
    command: Option<AgentCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum AgentCommands {
    /// Output command schema as JSON
    Schema(AgentSchemaArgs),
    /// Display coding-agent operational guidance for bt
    Guide,
}

#[derive(Debug, Clone, Args)]
struct AgentSchemaArgs {
    /// Output minimal schema and minified JSON
    #[arg(long, default_value_t = false)]
    compact: bool,
}

pub async fn run(_base: BaseArgs, args: AgentArgs) -> Result<()> {
    match args.command {
        Some(AgentCommands::Schema(schema_args)) => {
            let cmd = crate::Cli::command();
            let schema = if schema_args.compact {
                build_compact_agent_schema(&cmd)
            } else {
                build_agent_schema(&cmd)
            };
            if schema_args.compact {
                println!("{}", serde_json::to_string(&schema)?);
            } else {
                println!("{}", serde_json::to_string_pretty(&schema)?);
            }
            Ok(())
        }
        Some(AgentCommands::Guide) => {
            print_guide();
            Ok(())
        }
        None => print_agent_help(),
    }
}

pub(crate) fn maybe_intercept_help(argv: &[OsString]) -> Result<bool> {
    if !should_intercept_help(argv) {
        return Ok(false);
    }

    let cmd = crate::Cli::command();
    let path = deepest_subcommand_path(&cmd, argv);
    let schema = if path.is_empty() {
        build_agent_schema(&cmd)
    } else {
        build_agent_schema_scoped(&cmd, &path)
    };
    println!("{}", serde_json::to_string_pretty(&schema)?);
    Ok(true)
}

pub(crate) fn should_intercept_help(argv: &[OsString]) -> bool {
    if !has_help_flag(argv) {
        return false;
    }
    if has_flag(argv, "--no-agent") || is_env_truthy("BRAINTRUST_NO_AGENT") {
        return false;
    }
    has_flag(argv, "--agent") || is_env_truthy("BRAINTRUST_AGENT") || is_agent_mode_from_env()
}

fn has_help_flag(argv: &[OsString]) -> bool {
    for arg in argv.iter().skip(1) {
        let Some(value) = arg.to_str() else {
            continue;
        };
        if value == "--" {
            break;
        }
        if value == "-h" || value == "--help" {
            return true;
        }
    }
    false
}

fn has_flag(argv: &[OsString], name: &str) -> bool {
    for arg in argv.iter().skip(1) {
        let Some(value) = arg.to_str() else {
            continue;
        };
        if value == "--" {
            break;
        }
        if value == name || value.starts_with(&format!("{name}=")) {
            return true;
        }
    }
    false
}

fn is_agent_mode_from_env() -> bool {
    is_env_truthy("FORCE_AGENT_MODE") || AGENT_ENV_VARS.iter().any(|key| is_env_truthy(key))
}

fn is_env_truthy(key: &str) -> bool {
    match std::env::var(key) {
        Ok(value) => value == "1" || value.eq_ignore_ascii_case("true"),
        Err(_) => false,
    }
}

fn deepest_subcommand_path(root: &clap::Command, argv: &[OsString]) -> Vec<String> {
    let mut tokens = Vec::new();
    for arg in argv.iter().skip(1) {
        let Some(value) = arg.to_str() else {
            continue;
        };
        if value == "--" {
            break;
        }
        if value == "-h" || value == "--help" || value.starts_with('-') {
            continue;
        }
        tokens.push(value.to_string());
    }

    let mut path = Vec::new();
    let mut current = root;
    for token in tokens {
        let next = current
            .get_subcommands()
            .find(|sub| sub.get_name() == token && is_visible_subcommand(sub));
        let Some(next) = next else {
            break;
        };
        path.push(token);
        current = next;
    }

    path
}

pub(crate) fn build_agent_schema(root: &clap::Command) -> Value {
    let mut schema = schema_root(root);
    let mut commands: Vec<Value> = visible_subcommands_sorted(root)
        .into_iter()
        .map(|sub| build_command_schema(sub, ""))
        .collect();
    commands.sort_by(|a, b| command_name(a).cmp(command_name(b)));
    schema.insert("commands".to_string(), Value::Array(commands));
    Value::Object(schema)
}

pub(crate) fn build_agent_schema_scoped(root: &clap::Command, path: &[String]) -> Value {
    let mut schema = schema_root(root);
    let chain = resolve_command_chain(root, path);
    if chain.is_empty() {
        return build_agent_schema(root);
    }

    let scoped = build_command_chain(&chain, "");
    schema.insert("commands".to_string(), Value::Array(vec![scoped]));
    Value::Object(schema)
}

pub(crate) fn build_compact_agent_schema(root: &clap::Command) -> Value {
    let mut schema = Map::new();
    schema.insert("schema_version".to_string(), Value::Number(1.into()));
    schema.insert(
        "bt_version".to_string(),
        Value::String(CLI_VERSION.to_string()),
    );
    schema.insert(
        "global_flags".to_string(),
        Value::Array(compact_global_flags(root)),
    );

    let mut commands: Vec<Value> = visible_subcommands_sorted(root)
        .into_iter()
        .map(|sub| build_compact_command_schema(sub, ""))
        .collect();
    commands.sort_by(|a, b| command_name(a).cmp(command_name(b)));

    schema.insert("commands".to_string(), Value::Array(commands));
    Value::Object(schema)
}

fn schema_root(root: &clap::Command) -> Map<String, Value> {
    let mut schema = Map::new();
    schema.insert("schema_version".to_string(), Value::Number(1.into()));
    schema.insert(
        "bt_version".to_string(),
        Value::String(CLI_VERSION.to_string()),
    );
    schema.insert(
        "description".to_string(),
        Value::String(
            root.get_about()
                .map(|value| value.to_string())
                .unwrap_or_default(),
        ),
    );
    schema.insert("docs_url".to_string(), Value::String(DOCS_URL.to_string()));

    let mut auth = Map::new();
    auth.insert(
        "oauth".to_string(),
        Value::String("bt auth login --oauth".to_string()),
    );
    auth.insert(
        "api_key".to_string(),
        Value::String("--api-key or BRAINTRUST_API_KEY".to_string()),
    );
    auth.insert(
        "status".to_string(),
        Value::String("bt auth status".to_string()),
    );
    schema.insert("auth".to_string(), Value::Object(auth));

    schema.insert("global_flags".to_string(), Value::Array(global_flags(root)));

    let mut context = Map::new();
    context.insert(
        "best_practices".to_string(),
        Value::Array(vec![
            Value::String(
                "Start with `bt agent schema` to discover commands and flags before executing anything.".to_string(),
            ),
            Value::String(
                "Prefer `--json` for machine-readable command output.".to_string(),
            ),
            Value::String(
                "Use `bt <command> <subcommand> --help` for command-specific details."
                    .to_string(),
            ),
            Value::String(
                "Use `bt agent schema --compact` when token budget is tight.".to_string(),
            ),
        ]),
    );
    context.insert(
        "safety_rules".to_string(),
        Value::Array(vec![
            Value::String("Every BTQL query must include either a timestamp filter or a root_span_id filter.".to_string()),
            Value::String("Do not run BTQL queries that lack both constraints.".to_string()),
            Value::String("Do not infer/transform app URLs from API URLs. Treat --app-url / BRAINTRUST_APP_URL as the source of truth for app URLs.".to_string()),
        ]),
    );
    context.insert(
        "usage_patterns".to_string(),
        Value::Array(vec![
            Value::String(
                "Run `bt agent schema` for full machine-readable command schema.".to_string(),
            ),
            Value::String(
                "Run `bt agent schema --compact` for compact minified schema.".to_string(),
            ),
            Value::String(
                "Run `bt agent guide` for quick operational usage guidance.".to_string(),
            ),
            Value::String("In agent mode, `bt --help` returns verbose JSON schema.".to_string()),
        ]),
    );
    context.insert(
        "anti_patterns".to_string(),
        Value::Array(vec![
            Value::String("Do not run BTQL queries without a timestamp filter or root_span_id filter.".to_string()),
            Value::String("Do not infer app URLs from API URLs.".to_string()),
            Value::String("Do not assume text help in agent mode; `--help` may return JSON schema.".to_string()),
        ]),
    );
    schema.insert("context".to_string(), Value::Object(context));

    schema.insert(
        "context_sources".to_string(),
        Value::Array(vec![
            Value::String("README.md".to_string()),
            Value::String("src/main.rs".to_string()),
            Value::String("src/agent/guide.txt".to_string()),
        ]),
    );

    schema
}

fn resolve_command_chain<'a>(root: &'a clap::Command, path: &[String]) -> Vec<&'a clap::Command> {
    let mut chain = Vec::new();
    let mut current = root;
    for part in path {
        let next = current
            .get_subcommands()
            .find(|sub| sub.get_name() == part && is_visible_subcommand(sub));
        let Some(next) = next else {
            break;
        };
        chain.push(next);
        current = next;
    }
    chain
}

fn build_command_chain(chain: &[&clap::Command], parent_path: &str) -> Value {
    let current = chain[0];
    if chain.len() == 1 {
        return build_command_schema(current, parent_path);
    }

    let mut node = build_command_schema(current, parent_path);
    let full_path = node
        .get("full_path")
        .and_then(Value::as_str)
        .unwrap_or_default()
        .to_string();
    let child = build_command_chain(&chain[1..], &full_path);

    if let Value::Object(ref mut map) = node {
        map.remove("read_only");
        map.insert("subcommands".to_string(), Value::Array(vec![child]));
    }

    node
}

fn build_command_schema(cmd: &clap::Command, parent_path: &str) -> Value {
    let mut node = build_command_base(cmd, parent_path);

    let mut subs: Vec<Value> = visible_subcommands_sorted(cmd)
        .into_iter()
        .map(|sub| build_command_schema(sub, node_full_path(&node)))
        .collect();
    subs.sort_by(|a, b| command_name(a).cmp(command_name(b)));

    if let Value::Object(ref mut map) = node {
        if subs.is_empty() {
            let full_path = map
                .get("full_path")
                .and_then(Value::as_str)
                .unwrap_or_default();
            let name = map.get("name").and_then(Value::as_str).unwrap_or_default();
            map.insert(
                "read_only".to_string(),
                Value::Bool(classify_read_only(full_path, name)),
            );
        } else {
            map.insert("subcommands".to_string(), Value::Array(subs));
        }
    }

    node
}

fn build_command_base(cmd: &clap::Command, parent_path: &str) -> Value {
    let mut map = Map::new();
    let name = cmd.get_name().to_string();
    let full_path = if parent_path.is_empty() {
        name.clone()
    } else {
        format!("{parent_path} {name}")
    };

    map.insert("name".to_string(), Value::String(name));
    map.insert("full_path".to_string(), Value::String(full_path));

    if let Some(description) = cmd.get_long_about().or_else(|| cmd.get_about()) {
        map.insert(
            "description".to_string(),
            Value::String(description.to_string()),
        );
    }

    let args = positional_args(cmd);
    if !args.is_empty() {
        map.insert("args".to_string(), Value::Array(args));
    }

    let flags = command_flags(cmd);
    if !flags.is_empty() {
        map.insert("flags".to_string(), Value::Array(flags));
    }

    Value::Object(map)
}

fn build_compact_command_schema(cmd: &clap::Command, parent_path: &str) -> Value {
    let name = cmd.get_name().to_string();
    let full_path = if parent_path.is_empty() {
        name.clone()
    } else {
        format!("{parent_path} {name}")
    };

    let mut map = Map::new();
    map.insert("name".to_string(), Value::String(name));
    map.insert("full_path".to_string(), Value::String(full_path.clone()));

    let compact_flags = compact_command_flags(cmd);
    if !compact_flags.is_empty() {
        map.insert("flags".to_string(), Value::Array(compact_flags));
    }

    let mut subcommands: Vec<Value> = visible_subcommands_sorted(cmd)
        .into_iter()
        .map(|sub| build_compact_command_schema(sub, &full_path))
        .collect();
    subcommands.sort_by(|a, b| command_name(a).cmp(command_name(b)));

    if !subcommands.is_empty() {
        map.insert("subcommands".to_string(), Value::Array(subcommands));
    }

    Value::Object(map)
}

fn positional_args(cmd: &clap::Command) -> Vec<Value> {
    cmd.get_arguments()
        .filter(|arg| is_visible_argument(arg))
        .filter(|arg| arg.get_long().is_none())
        .enumerate()
        .map(|(index, arg)| positional_arg_schema(arg, index + 1))
        .collect()
}

fn positional_arg_schema(arg: &Arg, index: usize) -> Value {
    let mut map = Map::new();
    map.insert(
        "name".to_string(),
        Value::String(arg.get_id().as_str().to_string()),
    );
    map.insert("index".to_string(), Value::Number((index as u64).into()));
    map.insert("required".to_string(), Value::Bool(arg.is_required_set()));
    map.insert(
        "type".to_string(),
        Value::String(argument_type(arg).to_string()),
    );

    if let Some(help) = arg.get_help() {
        map.insert("description".to_string(), Value::String(help.to_string()));
    }

    maybe_insert_default(&mut map, arg);
    maybe_insert_env(&mut map, arg);
    maybe_insert_value_names(&mut map, arg);
    maybe_insert_possible_values(&mut map, arg);

    if is_multiple_argument(arg) {
        map.insert("multiple".to_string(), Value::Bool(true));
    }

    Value::Object(map)
}

fn command_flags(cmd: &clap::Command) -> Vec<Value> {
    let mut flags: Vec<Value> = cmd
        .get_arguments()
        .filter(|arg| is_visible_argument(arg))
        .filter(|arg| arg.get_long().is_some() || arg.get_short().is_some())
        .filter(|arg| !arg.is_global_set())
        .map(flag_schema)
        .collect();

    flags.sort_by(|a, b| command_name(a).cmp(command_name(b)));
    flags
}

fn compact_command_flags(cmd: &clap::Command) -> Vec<Value> {
    let mut flags: Vec<Value> = command_flag_names(cmd)
        .into_iter()
        .map(Value::String)
        .collect();
    flags.sort_by(|a, b| command_name(a).cmp(command_name(b)));
    flags
}

fn global_flags(root: &clap::Command) -> Vec<Value> {
    let mut seen = BTreeSet::new();
    let mut flags = Vec::new();
    for arg in root.get_arguments() {
        if !is_visible_argument(arg) || !is_schema_global_flag(arg) {
            continue;
        }
        let key = flag_name(arg);
        if !seen.insert(key) {
            continue;
        }
        flags.push(flag_schema(arg));
    }
    flags.sort_by(|a, b| command_name(a).cmp(command_name(b)));
    flags
}

fn compact_global_flags(root: &clap::Command) -> Vec<Value> {
    let mut seen = BTreeSet::new();
    let mut flags = Vec::new();
    for arg in root.get_arguments() {
        if !is_visible_argument(arg) || !is_schema_global_flag(arg) {
            continue;
        }
        let name = flag_name(arg);
        if seen.insert(name.clone()) {
            flags.push(Value::String(name));
        }
    }
    flags.sort_by(|a, b| command_name(a).cmp(command_name(b)));
    flags
}

fn flag_schema(arg: &Arg) -> Value {
    let mut map = Map::new();
    map.insert("name".to_string(), Value::String(flag_name(arg)));
    map.insert(
        "type".to_string(),
        Value::String(argument_type(arg).to_string()),
    );
    map.insert("required".to_string(), Value::Bool(arg.is_required_set()));

    if let Some(short) = arg.get_short() {
        map.insert("short".to_string(), Value::String(format!("-{short}")));
    }
    if let Some(long) = arg.get_long() {
        map.insert("long".to_string(), Value::String(format!("--{long}")));
    }
    if let Some(help) = arg.get_help() {
        map.insert("description".to_string(), Value::String(help.to_string()));
    }

    maybe_insert_default(&mut map, arg);
    maybe_insert_env(&mut map, arg);
    maybe_insert_value_names(&mut map, arg);
    maybe_insert_possible_values(&mut map, arg);

    if is_multiple_argument(arg) {
        map.insert("multiple".to_string(), Value::Bool(true));
    }

    Value::Object(map)
}

fn maybe_insert_default(map: &mut Map<String, Value>, arg: &Arg) {
    if let Some(default) = arg.get_default_values().first().and_then(|v| v.to_str()) {
        map.insert("default".to_string(), Value::String(default.to_string()));
    }
}

fn maybe_insert_env(map: &mut Map<String, Value>, arg: &Arg) {
    #[allow(clippy::redundant_else)]
    if arg.is_hide_env_set() {
    } else if let Some(env) = arg.get_env().and_then(|value| value.to_str()) {
        map.insert("env".to_string(), Value::String(env.to_string()));
    }
}

fn maybe_insert_value_names(map: &mut Map<String, Value>, arg: &Arg) {
    if let Some(value_names) = arg.get_value_names() {
        let values = value_names
            .iter()
            .map(|value| Value::String(value.to_string()))
            .collect::<Vec<_>>();
        if !values.is_empty() {
            map.insert("value_names".to_string(), Value::Array(values));
        }
    }
}

fn maybe_insert_possible_values(map: &mut Map<String, Value>, arg: &Arg) {
    if arg.is_hide_possible_values_set() {
        return;
    }

    let values = arg
        .get_possible_values()
        .into_iter()
        .filter(|value| !value.is_hide_set())
        .map(|value| Value::String(value.get_name().to_string()))
        .collect::<Vec<_>>();
    if !values.is_empty() {
        map.insert("possible_values".to_string(), Value::Array(values));
    }
}

fn argument_type(arg: &Arg) -> &'static str {
    if !arg.get_action().takes_values() {
        "bool"
    } else {
        "string"
    }
}

fn is_multiple_argument(arg: &Arg) -> bool {
    matches!(arg.get_action(), ArgAction::Append | ArgAction::Count)
        || arg
            .get_num_args()
            .map(value_range_is_multiple)
            .unwrap_or(false)
}

fn value_range_is_multiple(range: ValueRange) -> bool {
    range.min_values() > 1 || range.max_values() > 1 || range.max_values() == usize::MAX
}

fn flag_name(arg: &Arg) -> String {
    if let Some(long) = arg.get_long() {
        format!("--{long}")
    } else if let Some(short) = arg.get_short() {
        format!("-{short}")
    } else {
        arg.get_id().as_str().to_string()
    }
}

fn command_flag_names(cmd: &clap::Command) -> Vec<String> {
    cmd.get_arguments()
        .filter(|arg| is_visible_argument(arg))
        .filter(|arg| !arg.is_global_set())
        .filter(|arg| arg.get_long().is_some() || arg.get_short().is_some())
        .map(flag_name)
        .collect()
}

fn is_visible_argument(arg: &Arg) -> bool {
    let id = arg.get_id().as_str();
    !arg.is_hide_set() && id != "help" && id != "version"
}

fn is_schema_global_flag(arg: &Arg) -> bool {
    arg.is_global_set() || matches!(arg.get_id().as_str(), "agent_mode" | "no_agent_mode")
}

fn is_visible_subcommand(cmd: &clap::Command) -> bool {
    !cmd.is_hide_set() && cmd.get_name() != "help"
}

fn visible_subcommands_sorted(cmd: &clap::Command) -> Vec<&clap::Command> {
    let mut subcommands: Vec<&clap::Command> = cmd
        .get_subcommands()
        .filter(|sub| is_visible_subcommand(sub))
        .collect();
    subcommands.sort_by(|a, b| a.get_name().cmp(b.get_name()));
    subcommands
}

fn node_full_path(node: &Value) -> &str {
    node.get("full_path")
        .and_then(Value::as_str)
        .unwrap_or_default()
}

fn command_name(value: &Value) -> &str {
    value
        .get("name")
        .and_then(Value::as_str)
        .or_else(|| value.as_str())
        .unwrap_or_default()
}

fn classify_read_only(full_path: &str, leaf_name: &str) -> bool {
    if let Some(override_value) = read_only_override(full_path) {
        return override_value;
    }

    if is_write_command_name(leaf_name) {
        return false;
    }

    true
}

fn read_only_override(full_path: &str) -> Option<bool> {
    match full_path {
        "sync pull" => Some(false),
        "functions pull" => Some(false),
        "status" => Some(true),
        "view" => Some(true),
        "sql" => Some(true),
        "projects view" => Some(true),
        "agent schema" => Some(true),
        "agent guide" => Some(true),
        "docs fetch" => Some(false),
        "auth login" => Some(false),
        "auth logout" => Some(false),
        "auth refresh" => Some(false),
        "eval" => Some(false),
        "setup skills" => Some(false),
        "setup instrument" => Some(false),
        "setup mcp" => Some(false),
        _ => None,
    }
}

fn is_write_command_name(name: &str) -> bool {
    name == "delete"
        || name == "create"
        || name == "update"
        || name == "cancel"
        || name == "trigger"
        || name == "set"
        || name == "add"
        || name == "remove"
        || name == "install"
        || name == "assign"
        || name == "archive"
        || name == "unarchive"
        || name == "activate"
        || name == "deactivate"
        || name == "move"
        || name == "link"
        || name == "unlink"
        || name == "configure"
        || name == "upgrade"
        || name.starts_with("update-")
        || name.starts_with("create-")
        || name == "submit"
        || name == "send"
        || name == "import"
        || name == "register"
        || name == "unregister"
        || name.contains("delete")
        || name == "patch"
        || name.starts_with("patch-")
        || name == "push"
        || name == "pull"
        || name == "login"
        || name == "logout"
        || name == "refresh"
        || name == "init"
        || name == "switch"
        || name == "rewind"
        || name == "poke"
        || name == "enable"
        || name == "disable"
}

fn print_guide() {
    println!("bt agent guide");
    println!("bt version: {CLI_VERSION}");
    println!();
    println!("{}", GUIDE_BODY.trim_end());
}

fn print_agent_help() -> Result<()> {
    let mut root = crate::Cli::command();
    if let Some(agent_cmd) = root.find_subcommand_mut("agent") {
        agent_cmd.print_long_help()?;
        let mut out = std::io::stdout().lock();
        writeln!(out)?;
        return Ok(());
    }

    root.print_long_help()?;
    let mut out = std::io::stdout().lock();
    writeln!(out)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn clear_agent_env() {
        std::env::remove_var("FORCE_AGENT_MODE");
        std::env::remove_var("BRAINTRUST_AGENT");
        std::env::remove_var("BRAINTRUST_NO_AGENT");
        for key in AGENT_ENV_VARS {
            std::env::remove_var(key);
        }
    }

    #[test]
    fn schema_has_expected_top_level_fields() {
        let schema = build_agent_schema(&crate::Cli::command());
        for key in [
            "schema_version",
            "bt_version",
            "description",
            "docs_url",
            "auth",
            "global_flags",
            "context",
            "context_sources",
            "commands",
        ] {
            assert!(schema.get(key).is_some(), "missing {key}");
        }
    }

    #[test]
    fn schema_commands_are_alphabetical() {
        let schema = build_agent_schema(&crate::Cli::command());
        let commands = schema
            .get("commands")
            .and_then(Value::as_array)
            .expect("commands array");
        let names: Vec<String> = commands
            .iter()
            .filter_map(|command| {
                command
                    .get("name")
                    .and_then(Value::as_str)
                    .map(|name| name.to_string())
            })
            .collect();
        let mut sorted = names.clone();
        sorted.sort();
        assert_eq!(names, sorted);
    }

    #[test]
    fn compact_schema_has_global_flags() {
        let schema = build_compact_agent_schema(&crate::Cli::command());
        assert!(schema
            .get("global_flags")
            .and_then(Value::as_array)
            .is_some());
    }

    #[test]
    fn scoped_schema_reconstructs_chain() {
        let schema = build_agent_schema_scoped(
            &crate::Cli::command(),
            &["projects".to_string(), "create".to_string()],
        );
        let commands = schema
            .get("commands")
            .and_then(Value::as_array)
            .expect("commands");
        assert_eq!(commands.len(), 1);

        let top = &commands[0];
        assert_eq!(top.get("name").and_then(Value::as_str), Some("projects"));
        let subs = top
            .get("subcommands")
            .and_then(Value::as_array)
            .expect("subcommands");
        assert_eq!(subs.len(), 1);
        assert_eq!(subs[0].get("name").and_then(Value::as_str), Some("create"));
    }

    #[test]
    fn help_interception_precedence_matches_expectations() {
        let _guard = env_lock().lock().expect("env lock");
        clear_agent_env();

        std::env::set_var("CLAUDE_CODE", "1");
        assert!(should_intercept_help(&[
            OsString::from("bt"),
            OsString::from("--help")
        ]));

        assert!(!should_intercept_help(&[
            OsString::from("bt"),
            OsString::from("--help"),
            OsString::from("--no-agent")
        ]));

        clear_agent_env();
    }

    #[test]
    fn truthy_env_detection_matches_pup() {
        let _guard = env_lock().lock().expect("env lock");
        clear_agent_env();

        std::env::set_var("CLAUDE_CODE", "true");
        assert!(is_agent_mode_from_env());

        std::env::set_var("CLAUDE_CODE", "1");
        assert!(is_agent_mode_from_env());

        std::env::set_var("CLAUDE_CODE", "yes");
        assert!(!is_agent_mode_from_env());

        clear_agent_env();
    }

    #[test]
    fn read_only_overrides_and_heuristics_work() {
        assert!(!classify_read_only("sync pull", "pull"));
        assert!(!classify_read_only("functions pull", "pull"));
        assert!(classify_read_only("status", "status"));
        assert!(classify_read_only("projects view", "view"));
        assert!(classify_read_only("agent guide", "guide"));
        assert!(!classify_read_only("projects create", "create"));
    }

    #[test]
    fn global_flags_include_agent_toggles() {
        let schema = build_agent_schema(&crate::Cli::command());
        let flags = schema
            .get("global_flags")
            .and_then(Value::as_array)
            .expect("global_flags");
        let names: Vec<&str> = flags
            .iter()
            .filter_map(|flag| flag.get("name").and_then(Value::as_str))
            .collect();
        assert!(names.contains(&"--agent"));
        assert!(names.contains(&"--no-agent"));
    }
}
