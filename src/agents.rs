//! `bt trace` — manages coding-agent tracing integrations.
//!
//! Hooks forward only profile, organization, and destination selection. The
//! long-lived daemon host resolves those selections through `bt`'s auth store,
//! including OAuth refresh and keychain access.

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::{bail, Context};
use async_trait::async_trait;
use clap::{Args, Subcommand};
use serde_json::{json, Map, Value};

use bt_daemon::wire::{
    AuthSelection, BackendAuth, FlushMode, SessionConfig, SessionRoute, TraceDestination,
};
use bt_daemon::{
    braintrust_serve_options, paths, run_hook, run_import, run_serve, run_status, run_traced,
    shutdown_daemon, AuthLease, AuthProvider, AuthResolveReason, BraintrustSinkConfig, HookArgs,
    HostInfo, ImportArgs, Registry, RunArgs, RunHookCommand, ServeArgs, ServeOptions, StatusArgs,
};

use crate::args::BaseArgs;

#[derive(Debug, Clone, Args)]
pub struct TraceArgs {
    #[command(subcommand)]
    command: TraceCommand,
}

#[derive(Debug, Clone, Subcommand)]
// Clap argument structs are parsed once; keeping their natural shapes is
// clearer than boxing individual command variants for stack-size savings.
#[allow(clippy::large_enum_variant)]
enum TraceCommand {
    /// Install the published Braintrust tracing plugin for a coding agent.
    Setup(SetupArgs),
    /// Run the tracing daemon (foreground).
    #[command(hide = true)]
    Daemon(ServeArgs),
    /// Forward one coding-agent hook event (read from stdin) to the daemon.
    #[command(hide = true)]
    Hook(HookArgs),
    /// Print daemon/session status.
    #[command(hide = true)]
    Status(StatusArgs),
    /// Gracefully stop the tracing daemon.
    #[command(hide = true)]
    Stop(StopArgs),
    /// Import a past Codex or Claude Code session by its resume id.
    Import(ImportArgs),
    /// Launch a coding agent with tracing enabled for this invocation.
    Run(RunArgs),
}

#[derive(Debug, Clone, Args)]
struct StopArgs {
    /// Socket path override (default: see the daemon protocol documentation).
    #[arg(long)]
    socket: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
struct SetupArgs {
    #[command(subcommand)]
    agent: SetupAgent,
}

#[derive(Debug, Clone, Copy, Subcommand)]
enum SetupAgent {
    /// Install the published Codex tracing plugin.
    Codex,
    /// Install the published Claude Code tracing plugin.
    Claude,
}

const GENERATED_MARKETPLACE: &str = "braintrust-bt-trace";
const GENERATED_CODEX_PLUGIN: &str = "trace-codex@braintrust-bt-trace";
const GENERATED_CLAUDE_PLUGIN: &str = "trace-claude-code@braintrust-bt-trace";
const LEGACY_CODEX_PLUGIN: &str = "trace-codex@braintrust-codex-plugins";
const LEGACY_CLAUDE_PLUGIN: &str = "trace-claude-code@braintrust-claude-plugin";

const CODEX_HOOK_EVENTS: &[&str] = &[
    "SessionStart",
    "UserPromptSubmit",
    "PreToolUse",
    "PermissionRequest",
    "PostToolUse",
    "PreCompact",
    "PostCompact",
    "SubagentStart",
    "SubagentStop",
    "Stop",
];

const CLAUDE_HOOK_EVENTS: &[&str] = &[
    "ConfigChange",
    "CwdChanged",
    "Elicitation",
    "ElicitationResult",
    "FileChanged",
    "InstructionsLoaded",
    "MessageDisplay",
    "Notification",
    "PermissionDenied",
    "PermissionRequest",
    "PostCompact",
    "PostToolBatch",
    "PostToolUse",
    "PostToolUseFailure",
    "PreCompact",
    "PreToolUse",
    "SessionEnd",
    "SessionStart",
    "Setup",
    "Stop",
    "StopFailure",
    "SubagentStart",
    "SubagentStop",
    "TaskCompleted",
    "TaskCreated",
    "TeammateIdle",
    "UserPromptExpansion",
    "UserPromptSubmit",
    "WorktreeCreate",
    "WorktreeRemove",
];

/// How the shim (re)launches the daemon: `bt trace daemon` from this same
/// binary.
fn host_info() -> HostInfo {
    let exe = std::env::current_exe()
        .map(OsString::from)
        .unwrap_or_else(|_| OsString::from("bt"));
    HostInfo {
        serve_argv: vec![exe, OsString::from("trace"), OsString::from("daemon")],
        version: crate::CLI_VERSION.to_string(),
    }
}

/// Production serve options: real agent translators, the Braintrust sink, and
/// `bt`'s profile-aware auth resolver.
fn serve_options(base: BaseArgs) -> ServeOptions {
    let cfg = BraintrustSinkConfig {
        api_url: None,
        app_url: None,
        version: crate::CLI_VERSION.to_string(),
    };
    let mut options = braintrust_serve_options(
        crate::CLI_VERSION,
        cfg,
        Arc::new(Registry::default_agents()),
    );
    options.auth_provider = Some(Arc::new(BtAuthProvider { base }));
    options
}

#[derive(Clone)]
struct BtAuthProvider {
    base: BaseArgs,
}

#[async_trait]
impl AuthProvider for BtAuthProvider {
    async fn resolve(
        &self,
        selection: &AuthSelection,
        _reason: AuthResolveReason,
    ) -> anyhow::Result<AuthLease> {
        let mut base = self.base.clone();
        base.no_input = true;
        if let Some(profile) = &selection.profile {
            base.profile = Some(profile.clone());
            base.profile_explicit = true;
            base.prefer_profile = true;
        }
        if let Some(org_name) = &selection.org_name {
            base.org_name = Some(org_name.clone());
        }
        let resolved = crate::auth::resolve_auth(&base)
            .await
            .map_err(|error| anyhow::anyhow!("resolve auth: {error}"))?;
        let token = resolved
            .api_key
            .ok_or_else(|| anyhow::anyhow!("selected Braintrust profile has no credential"))?;
        let profile = resolved
            .profile
            .or_else(|| selection.profile.clone())
            .unwrap_or_else(|| "environment".into());
        let expires_at_ms = resolved.is_oauth.then(|| {
            chrono::Utc::now()
                .timestamp_millis()
                .saturating_add(5 * 60 * 1000)
        });
        Ok(AuthLease {
            profile,
            auth: BackendAuth {
                token,
                api_url: resolved.api_url,
                app_url: resolved.app_url,
                org_name: resolved.org_name,
                org_id: None,
            },
            expires_at_ms,
        })
    }
}

fn session_route(base: &BaseArgs) -> SessionRoute {
    SessionRoute {
        auth: AuthSelection {
            profile: base.profile.clone(),
            org_name: base.org_name.clone(),
        },
        destination: base
            .project
            .clone()
            .map(|project_name| TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some(project_name),
            }),
        flush_mode: FlushMode::FireAndForget,
        additional_metadata: None,
    }
}

async fn resolve_trace_project(mut base: BaseArgs) -> anyhow::Result<BaseArgs> {
    if base
        .project
        .as_deref()
        .is_some_and(|project| !project.trim().is_empty())
    {
        return Ok(base);
    }

    if let Some(project) =
        crate::config::configured_project_for_context(&base, base.org_name.as_deref())
    {
        base.project = Some(project);
        return Ok(base);
    }

    if !crate::ui::is_interactive() {
        bail!(
            "project choice required in non-interactive mode; pass --project <NAME> or set BRAINTRUST_DEFAULT_PROJECT"
        );
    }

    let auth = crate::auth::resolve_auth(&base)
        .await
        .map_err(|error| anyhow::anyhow!("resolve auth: {error}"))?;
    if let Some(profile) = auth.profile.clone() {
        base.profile = Some(profile);
        base.profile_explicit = true;
        base.prefer_profile = true;
    }
    if base.org_name.is_none() {
        base.org_name = auth.org_name.clone();
    }
    if let Some(project) =
        crate::config::configured_project_for_context(&base, auth.org_name.as_deref())
    {
        base.project = Some(project);
        return Ok(base);
    }

    let login = crate::auth::login_read_only(&base).await?;
    let client = crate::http::ApiClient::new(&login)?;
    let project = crate::ui::select_project(
        &client,
        None,
        Some("Select a project for coding-agent traces"),
        crate::ui::ProjectSelectMode::ExistingOnly,
    )
    .await?;
    base.project = Some(project.name);
    Ok(base)
}

fn init_daemon_logging(verbose: bool) {
    let fallback = if verbose { "debug" } else { "info" };
    let filter = tracing_subscriber::EnvFilter::new(fallback);
    if let Err(error) = tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .try_init()
    {
        eprintln!("bt trace daemon logging unavailable: {error}");
    }
}

fn command_json(program: &str, args: &[&str]) -> anyhow::Result<Value> {
    let output = Command::new(program).args(args).output().with_context(|| {
        format!("failed to run `{program}`; install {program} and ensure it is on PATH")
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        bail!("`{program} {}` failed: {}", args.join(" "), stderr.trim());
    }
    serde_json::from_slice(&output.stdout)
        .with_context(|| format!("`{program} {}` returned invalid JSON", args.join(" ")))
}

fn run_command(program: &str, args: &[&str]) -> anyhow::Result<()> {
    let status = Command::new(program).args(args).status().with_context(|| {
        format!("failed to run `{program}`; install {program} and ensure it is on PATH")
    })?;
    if !status.success() {
        bail!("`{program} {}` failed with {status}", args.join(" "));
    }
    Ok(())
}

fn codex_marketplace_installed(value: &Value, name: &str) -> bool {
    value
        .get("marketplaces")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.get("name").and_then(Value::as_str) == Some(name))
        })
}

fn codex_plugin_installed(value: &Value, id: &str) -> bool {
    value
        .get("installed")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.get("pluginId").and_then(Value::as_str) == Some(id))
        })
}

fn claude_marketplace_installed(value: &Value, name: &str) -> bool {
    value.as_array().is_some_and(|items| {
        items
            .iter()
            .any(|item| item.get("name").and_then(Value::as_str) == Some(name))
    })
}

fn claude_plugin<'a>(value: &'a Value, id: &str) -> Option<&'a Value> {
    value
        .as_array()?
        .iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(id))
}

fn write_json(path: &Path, value: &Value) -> anyhow::Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("create plugin directory {}", parent.display()))?;
    }
    let mut encoded = serde_json::to_string_pretty(value)?;
    encoded.push('\n');
    crate::utils::write_text_atomic(path, &encoded)
}

fn hook_config(source: &str, events: &[&str], codex: bool) -> Value {
    let command = format!(
        "bt trace hook --source {source} --source-version {}",
        crate::CLI_VERSION
    );
    let hooks = events
        .iter()
        .map(|event| {
            let hook = if codex {
                json!({
                    "type": "command",
                    "command": command,
                    "commandWindows": command,
                    "statusMessage": "Braintrust tracing"
                })
            } else {
                json!({
                    "type": "command",
                    "command": command,
                    "async": false
                })
            };
            ((*event).to_owned(), json!([{ "hooks": [hook] }]))
        })
        .collect::<Map<_, _>>();
    json!({ "hooks": hooks })
}

fn generate_codex_plugin() -> anyhow::Result<PathBuf> {
    let root = crate::config::global_config_dir()?.join("trace-plugins/codex");
    write_json(
        &root.join(".agents/plugins/marketplace.json"),
        &json!({
            "name": GENERATED_MARKETPLACE,
            "plugins": [{
                "name": "trace-codex",
                "source": {
                    "source": "local",
                    "path": "./plugins/trace-codex"
                }
            }]
        }),
    )?;
    let plugin = root.join("plugins/trace-codex");
    write_json(
        &plugin.join(".codex-plugin/plugin.json"),
        &json!({
            "name": "trace-codex",
            "version": crate::CLI_VERSION,
            "description": "Trace Codex sessions through the Braintrust bt daemon",
            "hooks": "./hooks/hooks.json"
        }),
    )?;
    write_json(
        &plugin.join("hooks/hooks.json"),
        &hook_config("codex", CODEX_HOOK_EVENTS, true),
    )?;
    Ok(root)
}

fn generate_claude_plugin() -> anyhow::Result<PathBuf> {
    let root = crate::config::global_config_dir()?.join("trace-plugins/claude");
    write_json(
        &root.join(".claude-plugin/marketplace.json"),
        &json!({
            "$schema": "https://anthropic.com/claude-code/marketplace.schema.json",
            "name": GENERATED_MARKETPLACE,
            "version": crate::CLI_VERSION,
            "description": "Braintrust tracing through the bt daemon",
            "owner": { "name": "Braintrust" },
            "plugins": [{
                "name": "trace-claude-code",
                "description": "Trace Claude Code sessions through the Braintrust bt daemon",
                "source": "./plugins/trace-claude-code",
                "category": "observability"
            }]
        }),
    )?;
    let plugin = root.join("plugins/trace-claude-code");
    write_json(
        &plugin.join(".claude-plugin/plugin.json"),
        &json!({
            "name": "trace-claude-code",
            "version": crate::CLI_VERSION,
            "description": "Trace Claude Code sessions through the Braintrust bt daemon"
        }),
    )?;
    write_json(
        &plugin.join("hooks/hooks.json"),
        &hook_config("claude-code", CLAUDE_HOOK_EVENTS, false),
    )?;
    Ok(root)
}

fn setup_codex() -> anyhow::Result<()> {
    let marketplace = generate_codex_plugin()?;
    let marketplace = marketplace
        .to_str()
        .context("generated Codex plugin path is not UTF-8")?;
    let marketplaces = command_json("codex", &["plugin", "marketplace", "list", "--json"])?;

    let plugins = command_json("codex", &["plugin", "list", "--json"])?;
    for plugin in [LEGACY_CODEX_PLUGIN, GENERATED_CODEX_PLUGIN] {
        if codex_plugin_installed(&plugins, plugin) {
            run_command("codex", &["plugin", "remove", plugin])?;
        }
    }
    if codex_marketplace_installed(&marketplaces, GENERATED_MARKETPLACE) {
        run_command(
            "codex",
            &["plugin", "marketplace", "remove", GENERATED_MARKETPLACE],
        )?;
    }
    run_command("codex", &["plugin", "marketplace", "add", marketplace])?;
    run_command("codex", &["plugin", "add", GENERATED_CODEX_PLUGIN])?;
    Ok(())
}

fn setup_claude() -> anyhow::Result<()> {
    let marketplace = generate_claude_plugin()?;
    let marketplace = marketplace
        .to_str()
        .context("generated Claude plugin path is not UTF-8")?;
    let marketplaces = command_json("claude", &["plugin", "marketplace", "list", "--json"])?;
    let plugins = command_json("claude", &["plugin", "list", "--json"])?;
    if claude_plugin(&plugins, LEGACY_CLAUDE_PLUGIN)
        .is_some_and(|plugin| plugin.get("enabled").and_then(Value::as_bool) != Some(false))
    {
        run_command("claude", &["plugin", "disable", LEGACY_CLAUDE_PLUGIN])?;
    }
    if claude_plugin(&plugins, GENERATED_CLAUDE_PLUGIN).is_some() {
        run_command("claude", &["plugin", "uninstall", GENERATED_CLAUDE_PLUGIN])?;
    }
    if claude_marketplace_installed(&marketplaces, GENERATED_MARKETPLACE) {
        run_command(
            "claude",
            &["plugin", "marketplace", "remove", GENERATED_MARKETPLACE],
        )?;
    }
    run_command("claude", &["plugin", "marketplace", "add", marketplace])?;
    run_command("claude", &["plugin", "install", GENERATED_CLAUDE_PLUGIN])?;
    Ok(())
}

fn load_settings(path: &Path) -> anyhow::Result<Map<String, Value>> {
    match std::fs::read(path) {
        Ok(raw) => {
            let value: Value = serde_json::from_slice(&raw)
                .with_context(|| format!("invalid shared agent settings: {}", path.display()))?;
            value.as_object().cloned().ok_or_else(|| {
                anyhow::anyhow!(
                    "shared agent settings must be a JSON object: {}",
                    path.display()
                )
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Map::new()),
        Err(error) => Err(error)
            .with_context(|| format!("failed to read shared agent settings: {}", path.display())),
    }
}

fn enable_tracing(route: SessionRoute) -> anyhow::Result<PathBuf> {
    let path = paths::settings_path(None);
    let mut settings = load_settings(&path)?;
    settings.insert("traceToBraintrust".into(), Value::Bool(true));
    settings.insert("route".into(), serde_json::to_value(route)?);
    settings.remove("project");

    let mut encoded = serde_json::to_string_pretty(&Value::Object(settings))?;
    encoded.push('\n');
    crate::utils::write_text_atomic(&path, &encoded)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to protect shared settings: {}", path.display()))?;
    }
    Ok(path)
}

async fn run_setup(base: BaseArgs, args: SetupArgs) -> anyhow::Result<()> {
    let base = resolve_trace_project(base).await?;

    match args.agent {
        SetupAgent::Codex => setup_codex()?,
        SetupAgent::Claude => setup_claude()?,
    }
    let settings_path = enable_tracing(session_route(&base))?;
    println!(
        "The Braintrust tracing plugin is installed for {} and configured in {}.",
        match args.agent {
            SetupAgent::Codex => "Codex",
            SetupAgent::Claude => "Claude Code",
        },
        settings_path.display()
    );
    println!("Restart the coding agent to load the tracing plugin.");
    Ok(())
}

/// Resolve `bt`'s auth for one-shot imports.
async fn session_config(base: &BaseArgs) -> anyhow::Result<SessionConfig> {
    let auth = crate::auth::resolve_auth(base)
        .await
        .map_err(|e| anyhow::anyhow!("resolve auth: {e}"))?;
    Ok(SessionConfig {
        auth: BackendAuth {
            token: auth.api_key.unwrap_or_default(),
            api_url: auth.api_url,
            app_url: auth.app_url,
            org_name: auth.org_name,
            org_id: None,
        },
        destination: session_route(base).destination,
        flush_mode: FlushMode::FireAndForget,
        additional_metadata: None,
    })
}

pub async fn run(base: BaseArgs, args: TraceArgs) -> anyhow::Result<()> {
    match args.command {
        TraceCommand::Setup(setup_args) => run_setup(base, setup_args).await,
        TraceCommand::Daemon(serve_args) => {
            init_daemon_logging(base.verbose);
            run_serve(serve_args, serve_options(base)).await
        }
        TraceCommand::Hook(hook_args) => {
            // A hook must NEVER fail the agent's turn. It forwards only
            // non-secret routing selection; the daemon resolves credentials.
            if let Err(e) = run_hook(hook_args, session_route(&base), host_info()).await {
                eprintln!("bt trace hook (non-fatal): {e}");
            }
            Ok(())
        }
        TraceCommand::Status(status_args) => match run_status(status_args).await? {
            Some(status) => {
                println!("{}", serde_json::to_string_pretty(&status)?);
                Ok(())
            }
            None => {
                println!("bt-daemon is not running");
                Ok(())
            }
        },
        TraceCommand::Stop(stop_args) => {
            let socket = paths::socket_path(stop_args.socket.as_deref());
            let status_args = StatusArgs {
                socket: Some(socket.clone()),
                session_id: None,
            };
            if run_status(status_args).await?.is_none() {
                println!("No tracing daemon is running.");
                return Ok(());
            }
            shutdown_daemon(&socket).await?;
            println!("Tracing daemon stopped.");
            Ok(())
        }
        TraceCommand::Import(import_args) => {
            let base = if import_args.destination.is_none() && import_args.parent.is_none() {
                resolve_trace_project(base).await?
            } else {
                base
            };
            let config = session_config(&base).await?;
            run_import(import_args, serve_options(base), Some(config)).await
        }
        TraceCommand::Run(run_args) => {
            let base = resolve_trace_project(base).await?;
            let exe = std::env::current_exe()
                .map(OsString::from)
                .unwrap_or_else(|_| OsString::from("bt"));
            let hook_command = RunHookCommand {
                program: exe,
                args: vec![OsString::from("trace"), OsString::from("hook")],
            };
            let status = run_traced(run_args, hook_command, session_route(&base)).await?;
            if status.success() {
                Ok(())
            } else {
                bail!("coding agent exited with {status}")
            }
        }
    }
}
