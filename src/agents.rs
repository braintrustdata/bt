//! `bt agents` — manages coding-agent tracing integrations.
//!
//! The daemon library is credential-passive: it receives a resolved
//! `BackendAuth` with each session's config. Here `bt` fills that from its own
//! `resolve_auth` (profiles / OAuth refresh / keychain), so a `bt agents hook`
//! invocation traces to whatever profile the user is on. See
//! `../plugin-monorepo/bt-daemon/DESIGN.md` ("Dual consumption", auth handoff).

use std::ffi::OsString;
use std::sync::Arc;

use clap::{Args, Subcommand};

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
    /// Run the tracing daemon (foreground).
    Daemon(ServeArgs),
    /// Forward one coding-agent hook event (read from stdin) to the daemon.
    Hook(HookArgs),
    /// Print daemon/session status.
    Status(StatusArgs),
    /// Replay a journal file through the translators + sink.
    Replay(ReplayArgs),
}

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
