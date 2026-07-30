//! `bt agents` — manages coding-agent tracing integrations.
//!
//! The daemon library is credential-passive: it receives a resolved
//! `BackendAuth` with each session's config. Here `bt` fills that from its own
//! `resolve_auth` (profiles / OAuth refresh / keychain), so a `bt agents hook`
//! invocation traces to whatever profile the user is on. See
//! `../plugin-monorepo/bt-daemon/DESIGN.md` ("Dual consumption", auth handoff).

use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::Arc;

use anyhow::{bail, Context};
use clap::{Args, Subcommand};
use serde_json::{Map, Value};

use bt_daemon::wire::{BackendAuth, FlushMode, SessionConfig};
use bt_daemon::{
    braintrust_serve_options, paths, run_hook, run_replay, run_serve, run_status,
    BraintrustSinkConfig, DebugSinkFactory, HookArgs, HostInfo, Registry, ReplayArgs, ServeArgs,
    ServeOptions, StatusArgs,
};

use crate::args::BaseArgs;

#[derive(Debug, Clone, Args)]
pub struct AgentsArgs {
    #[command(subcommand)]
    command: AgentsCommand,
}

#[derive(Debug, Clone, Subcommand)]
enum AgentsCommand {
    /// Install the published Braintrust tracing plugin for a coding agent.
    Setup(SetupArgs),
    /// Run the tracing daemon (foreground).
    Daemon(ServeArgs),
    /// Forward one coding-agent hook event (read from stdin) to the daemon.
    Hook(HookArgs),
    /// Print daemon/session status.
    Status(StatusArgs),
    /// Replay a journal file through the translators + sink.
    Replay(ReplayArgs),
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

const CODEX_MARKETPLACE: &str = "braintrust-codex-plugins";
const CODEX_MARKETPLACE_SOURCE: &str = "braintrustdata/braintrust-codex-plugin";
const CODEX_PLUGIN: &str = "trace-codex@braintrust-codex-plugins";
const CLAUDE_MARKETPLACE: &str = "braintrust-claude-plugin";
const CLAUDE_MARKETPLACE_SOURCE: &str = "braintrustdata/braintrust-claude-plugin";
const CLAUDE_PLUGIN: &str = "trace-claude-code@braintrust-claude-plugin";

/// How the shim (re)launches the daemon: `bt agents daemon` from this same
/// binary.
fn host_info() -> HostInfo {
    let exe = std::env::current_exe()
        .map(OsString::from)
        .unwrap_or_else(|_| OsString::from("bt"));
    HostInfo {
        serve_argv: vec![exe, OsString::from("agents"), OsString::from("daemon")],
        version: crate::CLI_VERSION.to_string(),
    }
}

/// Production serve options: real agent translators + the Braintrust sink.
/// Per-session backend URLs arrive with each event's config (bt resolves them
/// per profile), so no daemon-level defaults are set here.
fn serve_options() -> ServeOptions {
    let cfg = BraintrustSinkConfig {
        api_url: None,
        app_url: None,
        version: crate::CLI_VERSION.to_string(),
    };
    braintrust_serve_options(
        crate::CLI_VERSION,
        cfg,
        Arc::new(Registry::default_agents()),
    )
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

fn enable_tracing(project: Option<&str>) -> anyhow::Result<PathBuf> {
    let path = paths::settings_path(None);
    let mut settings = load_settings(&path)?;
    settings.insert("traceToBraintrust".into(), Value::Bool(true));

    let existing_project = settings
        .get("project")
        .and_then(Value::as_str)
        .filter(|project| !project.is_empty());
    let project = project
        .filter(|project| !project.is_empty())
        .or(existing_project)
        .unwrap_or("coding-agents");
    settings.insert("project".into(), Value::String(project.to_string()));

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

fn run_setup(base: &BaseArgs, args: SetupArgs) -> anyhow::Result<()> {
    match args.agent {
        SetupAgent::Codex => setup_codex()?,
        SetupAgent::Claude => setup_claude()?,
    }
    let settings_path = enable_tracing(base.project.as_deref())?;
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

/// Resolve `bt`'s auth into the daemon's per-session config.
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
        project: base.project.clone(),
        parent_span_id: None,
        root_span_id: None,
        flush_mode: FlushMode::FireAndForget,
        additional_metadata: None,
    })
}

pub async fn run(base: BaseArgs, args: AgentsArgs) -> anyhow::Result<()> {
    match args.command {
        AgentsCommand::Setup(setup_args) => run_setup(&base, setup_args),
        AgentsCommand::Daemon(serve_args) => run_serve(serve_args, serve_options()).await,
        AgentsCommand::Hook(hook_args) => {
            // A hook must NEVER fail the agent's turn. Resolve auth and forward;
            // log and swallow any error, exit 0.
            match session_config(&base).await {
                Ok(config) => {
                    if let Err(e) = run_hook(hook_args, config, host_info()).await {
                        eprintln!("bt agents hook (non-fatal): {e}");
                    }
                }
                Err(e) => eprintln!("bt agents hook (non-fatal): {e}"),
            }
            Ok(())
        }
        AgentsCommand::Status(status_args) => match run_status(status_args).await? {
            Some(status) => {
                println!("{}", serde_json::to_string_pretty(&status)?);
                Ok(())
            }
            None => {
                println!("bt-daemon is not running");
                Ok(())
            }
        },
        AgentsCommand::Replay(replay_args) => {
            // Replay through the real translators into the debug sink (no
            // network): useful for inspecting what a journal produces.
            let data_dir = paths::data_dir(None);
            let opts = ServeOptions {
                version: crate::CLI_VERSION.to_string(),
                translators: Arc::new(Registry::default_agents()),
                sink_factory: Arc::new(DebugSinkFactory {
                    dir: data_dir.join("spans"),
                }),
            };
            run_replay(replay_args, opts).await
        }
    }
}
