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
use serde_json::{Map, Value};

use bt_daemon::wire::{
    AuthSelection, BackendAuth, FlushMode, SessionConfig, SessionRoute, TraceDestination,
};
use bt_daemon::{
    braintrust_serve_options, paths, run_hook, run_import, run_serve, run_status, run_traced,
    shutdown_daemon, AuthLease, AuthProvider, AuthResolveReason, BraintrustSinkConfig, HookArgs,
    HostInfo, ImportArgs, OutputFormat, Registry, RunArgs, RunHookCommand, ServeArgs, ServeOptions,
    StatusArgs, TraceCommandOutput,
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
    /// Configure the published OpenCode tracing plugin.
    #[command(name = "opencode", alias = "open-code")]
    OpenCode,
    /// Install the published Pi tracing extension.
    Pi,
}

const CODEX_MARKETPLACE: &str = "braintrust-codex-plugins";
const CODEX_MARKETPLACE_SOURCE: &str = "braintrustdata/braintrust-codex-plugin";
const CODEX_PLUGIN: &str = "trace-codex@braintrust-codex-plugins";
const CLAUDE_MARKETPLACE: &str = "braintrust-claude-plugin";
const CLAUDE_MARKETPLACE_SOURCE: &str = "braintrustdata/braintrust-claude-plugin";
const CLAUDE_PLUGIN: &str = "trace-claude-code@braintrust-claude-plugin";
const OPENCODE_PLUGIN: &str = "@braintrust/trace-opencode@^1";
const PI_PLUGIN: &str = "npm:@braintrust/pi-extension@^1";

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

fn codex_marketplace_installed(value: &Value) -> bool {
    value
        .get("marketplaces")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.get("name").and_then(Value::as_str) == Some(CODEX_MARKETPLACE))
        })
}

fn codex_plugin_installed(value: &Value) -> bool {
    value
        .get("installed")
        .and_then(Value::as_array)
        .is_some_and(|items| {
            items
                .iter()
                .any(|item| item.get("pluginId").and_then(Value::as_str) == Some(CODEX_PLUGIN))
        })
}

fn claude_marketplace_installed(value: &Value) -> bool {
    value.as_array().is_some_and(|items| {
        items
            .iter()
            .any(|item| item.get("name").and_then(Value::as_str) == Some(CLAUDE_MARKETPLACE))
    })
}

fn claude_plugin(value: &Value) -> Option<&Value> {
    value
        .as_array()?
        .iter()
        .find(|item| item.get("id").and_then(Value::as_str) == Some(CLAUDE_PLUGIN))
}

fn setup_codex() -> anyhow::Result<()> {
    let marketplaces = command_json("codex", &["plugin", "marketplace", "list", "--json"])?;
    if !codex_marketplace_installed(&marketplaces) {
        run_command(
            "codex",
            &["plugin", "marketplace", "add", CODEX_MARKETPLACE_SOURCE],
        )?;
    }

    let plugins = command_json("codex", &["plugin", "list", "--json"])?;
    if !codex_plugin_installed(&plugins) {
        run_command("codex", &["plugin", "add", CODEX_PLUGIN])?;
    }
    Ok(())
}

fn setup_claude() -> anyhow::Result<()> {
    let marketplaces = command_json("claude", &["plugin", "marketplace", "list", "--json"])?;
    if !claude_marketplace_installed(&marketplaces) {
        run_command(
            "claude",
            &["plugin", "marketplace", "add", CLAUDE_MARKETPLACE_SOURCE],
        )?;
    }

    let plugins = command_json("claude", &["plugin", "list", "--json"])?;
    match claude_plugin(&plugins) {
        None => run_command("claude", &["plugin", "install", CLAUDE_PLUGIN])?,
        Some(plugin) if plugin.get("enabled").and_then(Value::as_bool) == Some(false) => {
            run_command("claude", &["plugin", "enable", CLAUDE_PLUGIN])?;
        }
        Some(_) => {}
    }
    Ok(())
}

fn opencode_config_path() -> PathBuf {
    let config_home = std::env::var_os("XDG_CONFIG_HOME")
        .filter(|path| !path.is_empty())
        .map(PathBuf::from)
        .or_else(|| dirs::home_dir().map(|home| home.join(".config")))
        .unwrap_or_else(|| PathBuf::from(".config"));
    config_home.join("opencode").join("opencode.json")
}

fn setup_opencode() -> anyhow::Result<()> {
    let path = opencode_config_path();
    let mut config = load_settings(&path)?;
    let plugins = config
        .entry("plugin")
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or_else(|| {
            anyhow::anyhow!(
                "OpenCode `plugin` config must be an array: {}",
                path.display()
            )
        })?;
    plugins.retain(|plugin| {
        plugin.as_str().is_none_or(|plugin| {
            plugin != "@braintrust/trace-opencode"
                && !plugin.starts_with("@braintrust/trace-opencode@")
        })
    });
    plugins.push(Value::String(OPENCODE_PLUGIN.into()));

    let mut encoded = serde_json::to_string_pretty(&Value::Object(config))?;
    encoded.push('\n');
    crate::utils::write_text_atomic(&path, &encoded)?;
    Ok(())
}

fn setup_pi() -> anyhow::Result<()> {
    run_command("pi", &["install", PI_PLUGIN])
}

fn load_settings(path: &Path) -> anyhow::Result<Map<String, Value>> {
    match std::fs::read(path) {
        Ok(raw) => {
            let value: Value = serde_json::from_slice(&raw)
                .with_context(|| format!("invalid JSON configuration: {}", path.display()))?;
            value.as_object().cloned().ok_or_else(|| {
                anyhow::anyhow!("configuration must be a JSON object: {}", path.display())
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(Map::new()),
        Err(error) => {
            Err(error).with_context(|| format!("failed to read configuration: {}", path.display()))
        }
    }
}

fn enable_tracing(source: &str, route: SessionRoute) -> anyhow::Result<PathBuf> {
    let path = paths::agent_settings_path(source, None);
    let mut settings = load_settings(&path)?;
    settings.insert("trace_to_braintrust".into(), Value::Bool(true));
    settings.insert("route".into(), serde_json::to_value(route)?);
    settings.remove("traceToBraintrust");
    settings.remove("project");

    let mut encoded = serde_json::to_string_pretty(&Value::Object(settings))?;
    encoded.push('\n');
    crate::utils::write_text_atomic(&path, &encoded)?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))
            .with_context(|| format!("failed to protect agent settings: {}", path.display()))?;
    }
    Ok(path)
}

async fn run_setup(base: BaseArgs, args: SetupArgs, format: OutputFormat) -> anyhow::Result<()> {
    let base = resolve_trace_project(base).await?;

    let (source, label) = match args.agent {
        SetupAgent::Codex => {
            setup_codex()?;
            ("codex", "Codex")
        }
        SetupAgent::Claude => {
            setup_claude()?;
            ("claude", "Claude Code")
        }
        SetupAgent::OpenCode => {
            setup_opencode()?;
            ("opencode", "OpenCode")
        }
        SetupAgent::Pi => {
            setup_pi()?;
            ("pi", "Pi")
        }
    };
    let settings_path = enable_tracing(source, session_route(&base))?;
    println!(
        "{}",
        TraceCommandOutput::setup(source, label, settings_path).render(format)?
    );
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
    let format = OutputFormat::from(base.login.json);
    match args.command {
        TraceCommand::Setup(setup_args) => run_setup(base, setup_args, format).await,
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
        TraceCommand::Status(status_args) => {
            let output = TraceCommandOutput::status(run_status(status_args).await?);
            println!("{}", output.render(format)?);
            Ok(())
        }
        TraceCommand::Stop(stop_args) => {
            let socket = paths::socket_path(stop_args.socket.as_deref());
            let status_args = StatusArgs {
                socket: Some(socket.clone()),
                session_id: None,
            };
            if run_status(status_args).await?.is_none() {
                println!("{}", TraceCommandOutput::stop(false, false).render(format)?);
                return Ok(());
            }
            shutdown_daemon(&socket).await?;
            println!("{}", TraceCommandOutput::stop(true, true).render(format)?);
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
