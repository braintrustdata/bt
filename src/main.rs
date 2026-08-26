use anyhow::{Context, Result};
use clap::{parser::ValueSource, ArgMatches, CommandFactory, FromArgMatches, Parser, Subcommand};
use std::ffi::{OsStr, OsString};

mod args;
mod auth;
#[allow(dead_code)]
mod config;
mod datasets;
mod env;
mod error;
#[cfg(unix)]
mod eval;
mod experiments;
mod functions;
mod http;
mod init;
mod js_runner;
mod profiles;
mod project_context;
mod projects;
mod prompts;
mod python_runner;
mod runner_sse;
mod scorers;
mod self_update;
mod setup;
mod source_language;
mod sql;
mod status;
mod switch;
mod sync;
mod tools;
mod topics;
mod trace_host;
mod traces;
mod ui;
mod util_cmd;
mod utils;

use crate::args::{has_explicit_profile_arg, ArgValueSource, CLIArgs, LoginBaseArgs};

const DEFAULT_CANARY_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "-canary.dev");
pub(crate) const CLI_VERSION: &str = match option_env!("BT_VERSION_STRING") {
    Some(version) => version,
    None => DEFAULT_CANARY_VERSION,
};

pub(crate) const BANNER: &str = r#"

  ███  ███
███      ███
  ███  ███
███      ███
  ███  ███
"#;

const HELP_TEMPLATE: &str = "\
{before-help}{about} - {usage}

Core
  init         Initialize .bt config directory and files
  login        Log in to Braintrust
  logout       Remove a saved Braintrust login
  profiles     Manage saved Braintrust login profiles
  switch       Switch org and project context
  view         View logs, traces, and spans

Projects & resources
  projects     Manage projects
  topics       Inspect and control Topics automation
  prompts      Manage prompts
  functions    Manage functions (tools, scorers, and more)
  tools        Manage tools
  scorers      Manage scorers
  experiments  Manage experiments

Data & evaluation
  datasets     Manage datasets
  eval         Run eval files
  sql          Run SQL queries against Braintrust
  sync         Synchronize project logs between Braintrust and local NDJSON files

Additional
  docs         Manage workflow docs for coding agents
  trace        Manage coding-agent tracing
  setup        Configure Braintrust setup flows (deprecated: use curl -fsSL https://braintrust.dev/wizard/setup.sh | sh)
  status       Show current identity, org, and project context
  update       Update bt in-place

Flags
      --profile <PROFILE>    Use a saved login profile [env: BRAINTRUST_PROFILE]
  -o, --org <ORG>            Override active org [env: BRAINTRUST_ORG_NAME]
      -p, --project <PROJECT>    Override active project [env: BRAINTRUST_DEFAULT_PROJECT]
      --json                 Output as JSON
  -v, --verbose              Increase output verbosity [env: BRAINTRUST_VERBOSE=]
  -q, --quiet                Reduce interactive UI output [env: BRAINTRUST_QUIET=]
      --no-color             Disable ANSI color output
      --no-input             Disable all interactive prompts
      --api-url <URL>        Override API URL [env: BRAINTRUST_API_URL]
      --app-url <URL>        Override app URL [env: BRAINTRUST_APP_URL]
      --ca-cert <PATH>       Path to PEM CA bundle [env: BRAINTRUST_CA_CERT; overrides SSL_CERT_FILE]
      --env-file <PATH>      Path to a .env file to load
  -h, --help                 Print help
  -V, --version              Print version

LEARN MORE
Use `bt <command> <subcommand> --help` for more information about a command.
Read the manual at https://braintrust.dev/docs/reference/cli

";

#[derive(Debug, Parser)]
#[command(
    name = "bt",
    about = "bt is the CLI for interacting with your Braintrust projects",
    version = CLI_VERSION,
    before_help = BANNER,
    help_template = HELP_TEMPLATE,
    after_help = "Docs: https://braintrust.dev/docs",
)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Initialize .bt config directory and files
    Init(CLIArgs<init::InitArgs>),
    /// Configure Braintrust setup flows
    Setup(CLIArgs<setup::SetupArgs>),
    /// Manage workflow docs for coding agents
    Docs(CLIArgs<setup::DocsArgs>),
    /// Run SQL queries against Braintrust
    Sql(CLIArgs<sql::SqlArgs>),
    /// Log in to Braintrust
    Login(CLIArgs<auth::LoginArgs, LoginBaseArgs>),
    /// Remove a saved Braintrust login
    Logout(CLIArgs<auth::LogoutArgs>),
    /// Manage saved Braintrust login profiles
    Profiles(CLIArgs<profiles::ProfilesArgs, LoginBaseArgs>),
    /// View logs, traces, and spans
    View(CLIArgs<traces::ViewArgs>),
    #[cfg(unix)]
    /// Run eval files
    Eval(CLIArgs<eval::EvalArgs>),
    /// Manage projects
    Projects(CLIArgs<projects::ProjectsArgs>),
    /// Inspect and control Topics automation
    Topics(CLIArgs<topics::TopicsArgs>),
    /// Manage datasets
    Datasets(CLIArgs<datasets::DatasetsArgs>),
    /// Manage prompts
    Prompts(CLIArgs<prompts::PromptsArgs>),
    /// Update bt in-place
    Update(CLIArgs<self_update::UpdateArgs>),
    #[command(name = "self", hide = true)]
    /// Self-management commands
    SelfCommand(CLIArgs<self_update::SelfArgs>),
    /// Manage tools
    Tools(CLIArgs<tools::ToolsArgs>),
    /// Manage scorers
    Scorers(CLIArgs<scorers::ScorersArgs>),
    /// Manage functions (tools, scorers, and more)
    Functions(CLIArgs<functions::FunctionsArgs>),
    /// Manage experiments
    Experiments(CLIArgs<experiments::ExperimentsArgs>),
    /// Synchronize project logs between Braintrust and local NDJSON files
    Sync(CLIArgs<sync::SyncArgs>),
    /// Local utility commands
    Util(CLIArgs<util_cmd::UtilArgs>),
    /// Switch org and project context
    Switch(CLIArgs<switch::SwitchArgs>),
    /// Show current identity, org, and project context
    Status(CLIArgs<status::StatusArgs>),
    /// Manage coding-agent tracing
    Trace(CLIArgs<bt_daemon::TraceArgs>),
    // /// View and modify config
    // Config(CLIArgs<config::ConfigArgs>),
}

impl Commands {
    fn base(&self) -> &LoginBaseArgs {
        match self {
            Commands::Init(cmd) => &cmd.base,
            Commands::Setup(cmd) => &cmd.base,
            Commands::Docs(cmd) => &cmd.base,
            Commands::Sql(cmd) => &cmd.base,
            Commands::Login(cmd) => &cmd.base,
            Commands::Logout(cmd) => &cmd.base,
            Commands::Profiles(cmd) => &cmd.base,
            Commands::View(cmd) => &cmd.base,
            #[cfg(unix)]
            Commands::Eval(cmd) => &cmd.base,
            Commands::Projects(cmd) => &cmd.base,
            Commands::Topics(cmd) => &cmd.base,
            Commands::Datasets(cmd) => &cmd.base,
            Commands::Prompts(cmd) => &cmd.base,
            Commands::Update(cmd) => &cmd.base,
            Commands::SelfCommand(cmd) => &cmd.base,
            Commands::Tools(cmd) => &cmd.base,
            Commands::Scorers(cmd) => &cmd.base,
            Commands::Functions(cmd) => &cmd.base,
            Commands::Experiments(cmd) => &cmd.base,
            Commands::Sync(cmd) => &cmd.base,
            Commands::Util(cmd) => &cmd.base,
            Commands::Switch(cmd) => &cmd.base,
            Commands::Status(cmd) => &cmd.base,
            Commands::Trace(cmd) => &cmd.base,
        }
    }

    fn base_mut(&mut self) -> &mut LoginBaseArgs {
        match self {
            Commands::Init(cmd) => &mut cmd.base,
            Commands::Setup(cmd) => &mut cmd.base,
            Commands::Docs(cmd) => &mut cmd.base,
            Commands::Sql(cmd) => &mut cmd.base,
            Commands::Login(cmd) => &mut cmd.base,
            Commands::Logout(cmd) => &mut cmd.base,
            Commands::Profiles(cmd) => &mut cmd.base,
            Commands::View(cmd) => &mut cmd.base,
            #[cfg(unix)]
            Commands::Eval(cmd) => &mut cmd.base,
            Commands::Projects(cmd) => &mut cmd.base,
            Commands::Datasets(cmd) => &mut cmd.base,
            Commands::Topics(cmd) => &mut cmd.base,
            Commands::Prompts(cmd) => &mut cmd.base,
            Commands::Update(cmd) => &mut cmd.base,
            Commands::SelfCommand(cmd) => &mut cmd.base,
            Commands::Tools(cmd) => &mut cmd.base,
            Commands::Scorers(cmd) => &mut cmd.base,
            Commands::Functions(cmd) => &mut cmd.base,
            Commands::Experiments(cmd) => &mut cmd.base,
            Commands::Sync(cmd) => &mut cmd.base,
            Commands::Util(cmd) => &mut cmd.base,
            Commands::Switch(cmd) => &mut cmd.base,
            Commands::Status(cmd) => &mut cmd.base,
            Commands::Trace(cmd) => &mut cmd.base,
        }
    }

    fn verbose_by_default(&self) -> bool {
        !matches!(self, Commands::Setup(_))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(i32)]
enum ExitCode {
    Success = 0,
    Error = 1,
    Auth = 2,
    Network = 3,
    User = 4,
}

impl ExitCode {
    #[cfg(windows)]
    fn from_process_code(code: Option<i32>) -> Self {
        [Self::Error, Self::Auth, Self::Network, Self::User]
            .into_iter()
            .find(|value| Some(*value as i32) == code)
            .unwrap_or(Self::Error)
    }
}

static JSON_OUTPUT_REQUESTED: std::sync::atomic::AtomicBool =
    std::sync::atomic::AtomicBool::new(false);

fn main() {
    let exit_code = match try_main() {
        Ok(()) => ExitCode::Success,
        Err(err) => {
            let missing_credential = crate::auth::is_missing_credential_error(&err);
            let code = classify_error(&err, missing_credential);
            print_error(
                &err,
                code,
                missing_credential,
                JSON_OUTPUT_REQUESTED.load(std::sync::atomic::Ordering::Relaxed),
            );
            code
        }
    };
    std::process::exit(exit_code as i32);
}

fn handle_version_json(argv: &[OsString]) -> Result<bool> {
    use clap::{Arg, ArgAction, Command};
    let preflight = Command::new("bt-version-preflight")
        .ignore_errors(true)
        .disable_help_flag(true)
        .disable_version_flag(true)
        .arg(
            Arg::new("version")
                .long("version")
                .short('V')
                .action(ArgAction::Count),
        )
        .arg(Arg::new("json").long("json").action(ArgAction::Count));
    let Ok(matches) = preflight.try_get_matches_from(argv) else {
        return Ok(false);
    };
    if matches.get_count("version") == 0 || matches.get_count("json") == 0 {
        return Ok(false);
    }
    let payload = serde_json::json!({ "version": CLI_VERSION });
    println!("{}", serde_json::to_string(&payload)?);
    Ok(true)
}

fn apply_runtime_env_overrides(base: &LoginBaseArgs) {
    // Apply the CLI-owned override once so reqwest and inherited child
    // commands consistently observe BRAINTRUST_CA_CERT/--ca-cert precedence
    // over any ambient SSL_CERT_FILE.
    if let Some(ca_cert) = base.ca_cert() {
        std::env::set_var("SSL_CERT_FILE", ca_cert);
    }
}

fn try_main() -> Result<()> {
    let argv: Vec<OsString> = std::env::args_os().collect();
    env::bootstrap_from_args(&argv)?;

    if handle_version_json(&argv)? {
        return Ok(());
    }

    let matches = Cli::command().get_matches_from(&argv);
    let mut cli = Cli::from_arg_matches(&matches).expect("clap matches should parse");
    apply_base_arg_sources(&matches, cli.command.base_mut());
    cli.command.base_mut().profile_explicit = has_explicit_profile_arg(&argv);
    apply_base_output_defaults(&mut cli.command);
    JSON_OUTPUT_REQUESTED.store(
        cli.command.base().json,
        std::sync::atomic::Ordering::Relaxed,
    );
    configure_output(cli.command.base());
    apply_runtime_env_overrides(cli.command.base());
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("failed to build async runtime")?;

    let command_result: Result<()> = runtime.block_on(async move {
        match cli.command {
            Commands::Login(cmd) => auth::run_login_command(cmd.base.into(), cmd.args).await?,
            Commands::Logout(cmd) => auth::run_logout_command(cmd.base, cmd.args)?,
            Commands::Profiles(cmd) => profiles::run(cmd.base, cmd.args)?,
            Commands::View(cmd) => traces::run(cmd.base, cmd.args).await?,
            Commands::Init(cmd) => init::run(cmd.base, cmd.args).await?,
            Commands::Sql(cmd) => sql::run(cmd.base, cmd.args).await?,
            Commands::Setup(cmd) => setup::run_setup_top(cmd.base, cmd.args).await?,
            Commands::Docs(cmd) => setup::run_docs_top(cmd.base, cmd.args).await?,
            #[cfg(unix)]
            Commands::Eval(cmd) => eval::run(cmd.base, cmd.args).await?,
            Commands::Projects(cmd) => projects::run(cmd.base, cmd.args).await?,
            Commands::Datasets(cmd) => datasets::run(cmd.base, cmd.args).await?,
            Commands::Topics(cmd) => topics::run(cmd.base, cmd.args).await?,
            Commands::Prompts(cmd) => prompts::run(cmd.base, cmd.args).await?,
            Commands::Update(cmd) => {
                self_update::run(
                    cmd.base,
                    self_update::SelfArgs {
                        command: self_update::SelfSubcommand::Update(cmd.args),
                    },
                )
                .await?
            }
            Commands::Tools(cmd) => tools::run(cmd.base, cmd.args).await?,
            Commands::Scorers(cmd) => scorers::run(cmd.base, cmd.args).await?,
            Commands::Functions(cmd) => functions::run(cmd.base, cmd.args).await?,
            Commands::Experiments(cmd) => experiments::run(cmd.base, cmd.args).await?,
            Commands::Sync(cmd) => sync::run(cmd.base, cmd.args).await?,
            Commands::Util(cmd) => util_cmd::run(cmd.base, cmd.args).await?,
            Commands::SelfCommand(cmd) => self_update::run(cmd.base, cmd.args).await?,
            Commands::Switch(cmd) => switch::run(cmd.base, cmd.args).await?,
            Commands::Status(cmd) => status::run(cmd.base, cmd.args).await?,
            Commands::Trace(cmd) => {
                bt_daemon::run_trace(cmd.args, trace_host::context(cmd.base)).await?
            }
        }
        Ok(())
    });

    command_result
}

fn apply_base_arg_sources(matches: &ArgMatches, base: &mut LoginBaseArgs) {
    base.verbose_source = find_value_source(matches, "verbose").and_then(map_value_source);
    base.quiet_source = find_value_source(matches, "quiet").and_then(map_value_source);
    base.api_key_source = find_value_source(matches, "api_key").and_then(map_value_source);
}

fn apply_base_output_defaults(command: &mut Commands) {
    let verbose_by_default = command.verbose_by_default();
    let base = command.base_mut();

    if base.quiet {
        base.verbose = false;
        return;
    }

    base.verbose = base.verbose || verbose_by_default;
    base.quiet = !base.verbose;
}

fn find_value_source(matches: &ArgMatches, id: &str) -> Option<ValueSource> {
    match matches.try_contains_id(id) {
        Ok(_) => matches.value_source(id),
        Err(_) => matches
            .subcommand()
            .and_then(|(_, sub_matches)| find_value_source(sub_matches, id)),
    }
}

fn map_value_source(source: ValueSource) -> Option<ArgValueSource> {
    match source {
        ValueSource::CommandLine => Some(ArgValueSource::CommandLine),
        ValueSource::EnvVariable => Some(ArgValueSource::EnvVariable),
        _ => None,
    }
}

fn configure_output(base: &LoginBaseArgs) {
    let mut disable_color = base.no_color || std::env::var_os("NO_COLOR").is_some();

    // TERM is a terminal capability signal; it isn't a user-facing config knob.
    let term_is_dumb = match std::env::var_os("TERM") {
        Some(term) => term == OsStr::new("dumb"),
        None => false,
    };

    if term_is_dumb {
        disable_color = true;
        ui::set_animations_enabled(false);
    }

    ui::set_quiet(base.quiet);
    ui::set_no_input(base.no_input);
    ui::set_animations_enabled(!term_is_dumb && !base.quiet);

    if disable_color {
        dialoguer::console::set_colors_enabled(false);
        dialoguer::console::set_colors_enabled_stderr(false);
    }
}

fn classify_error(err: &anyhow::Error, missing_credential: bool) -> ExitCode {
    if missing_credential {
        return ExitCode::Auth;
    }

    #[cfg(windows)]
    if let Some(err) = err
        .chain()
        .find_map(|e| e.downcast_ref::<self_update::UpdateWorkerError>())
    {
        return ExitCode::from_process_code(err.1);
    }

    if has_user_error(err) {
        return ExitCode::User;
    }

    if let Some(http_error) = find_http_error(err) {
        let status = http_error.status.as_u16();
        if status == 401 || status == 403 {
            return ExitCode::Auth;
        }
        if (400..=499).contains(&status) {
            return ExitCode::User;
        }
        if (500..=599).contains(&status) {
            return ExitCode::Network;
        }
    }

    if let Some(code) = classify_sdk_error(err) {
        return code;
    }

    if has_reqwest_error(err) {
        return ExitCode::Network;
    }

    if has_response_parse_error(err) {
        return ExitCode::Error;
    }

    if has_io_error(err) || looks_like_user_error(err) {
        return ExitCode::User;
    }

    ExitCode::Error
}

fn find_http_error(err: &anyhow::Error) -> Option<&crate::http::HttpError> {
    err.chain()
        .find_map(|source| source.downcast_ref::<crate::http::HttpError>())
}

fn classify_sdk_error(err: &anyhow::Error) -> Option<ExitCode> {
    let sdk_err = err
        .chain()
        .find_map(|source| source.downcast_ref::<braintrust_sdk_rust::BraintrustError>())?;
    match sdk_err {
        braintrust_sdk_rust::BraintrustError::Api { status, .. } => {
            if *status == 401 || *status == 403 {
                Some(ExitCode::Auth)
            } else if (400..=499).contains(status) {
                Some(ExitCode::User)
            } else if (500..=599).contains(status) {
                Some(ExitCode::Network)
            } else {
                None
            }
        }
        braintrust_sdk_rust::BraintrustError::Http(_)
        | braintrust_sdk_rust::BraintrustError::Network(_) => Some(ExitCode::Network),
        braintrust_sdk_rust::BraintrustError::InvalidConfig(_)
        | braintrust_sdk_rust::BraintrustError::ChannelClosed
        | braintrust_sdk_rust::BraintrustError::Background(_)
        | braintrust_sdk_rust::BraintrustError::StreamAggregation(_) => None,
    }
}

fn has_reqwest_error(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|source| source.downcast_ref::<reqwest::Error>().is_some())
}

fn has_response_parse_error(err: &anyhow::Error) -> bool {
    err.chain().any(|source| {
        source
            .downcast_ref::<crate::http::ResponseParseError>()
            .is_some()
    })
}

fn has_io_error(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|source| source.downcast_ref::<std::io::Error>().is_some())
}

fn has_user_error(err: &anyhow::Error) -> bool {
    err.downcast_ref::<crate::error::UserError>().is_some()
}

fn looks_like_user_error(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_lowercase();
    message.contains("required")
        || message.contains("use:")
        || message.contains("not found")
        || message.contains("invalid")
}

fn json_error_payload(err: &anyhow::Error) -> serde_json::Value {
    let details = find_http_error(err)
        .and_then(|error| serde_json::from_str::<serde_json::Value>(&error.body).ok());
    let message = details
        .as_ref()
        .and_then(json_error_message)
        .unwrap_or_else(|| err.to_string());

    match details {
        Some(details) => serde_json::json!({
            "error": {
                "message": message,
                "details": details,
            }
        }),
        None => serde_json::json!({ "error": { "message": message } }),
    }
}

fn json_error_message(details: &serde_json::Value) -> Option<String> {
    details
        .pointer("/error/message")
        .and_then(serde_json::Value::as_str)
        .or_else(|| details.get("message").and_then(serde_json::Value::as_str))
        .or_else(|| details.get("error").and_then(serde_json::Value::as_str))
        .map(ToOwned::to_owned)
}

fn print_error(err: &anyhow::Error, code: ExitCode, missing_credential: bool, json_output: bool) {
    if json_output {
        println!("{}", json_error_payload(err));
        return;
    }

    eprintln!("error: {err}");
    if code == ExitCode::Auth && !missing_credential {
        eprintln!("Your credentials may be expired or invalid. For OAuth profiles, try `bt login --refresh --profile <NAME>`; if refresh fails, re-run `bt login --oauth --profile <NAME>`. Run `bt status --all` to inspect profile status.");
    }
    if code == ExitCode::Error {
        eprintln!("If this seems like a bug, file an issue at https://github.com/braintrustdata/bt/issues/new and include `bt --version`, `bt status --json`, and the command you ran.");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};

    fn env_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn restore_env_var(key: &str, previous: Option<OsString>) {
        match previous {
            Some(value) => env::set_var(key, value),
            None => env::remove_var(key),
        }
    }

    #[test]
    fn apply_base_arg_sources_tracks_cli_api_key() {
        let _guard = env_test_lock().lock().expect("env test lock");
        let previous_api_key = env::var_os("BRAINTRUST_API_KEY");
        env::remove_var("BRAINTRUST_API_KEY");

        let matches = Cli::command()
            .try_get_matches_from(["bt", "status", "--api-key", "secret"])
            .expect("matches");
        let mut cli = Cli::from_arg_matches(&matches).expect("cli");

        apply_base_arg_sources(&matches, cli.command.base_mut());

        restore_env_var("BRAINTRUST_API_KEY", previous_api_key);

        assert_eq!(
            cli.command.base().api_key_source,
            Some(ArgValueSource::CommandLine)
        );
    }

    #[test]
    fn apply_base_arg_sources_leaves_api_key_source_empty_when_unset() {
        let _guard = env_test_lock().lock().expect("env test lock");
        let previous_api_key = env::var_os("BRAINTRUST_API_KEY");
        env::remove_var("BRAINTRUST_API_KEY");

        let matches = Cli::command()
            .try_get_matches_from(["bt", "status"])
            .expect("matches");
        let mut cli = Cli::from_arg_matches(&matches).expect("cli");

        apply_base_arg_sources(&matches, cli.command.base_mut());

        restore_env_var("BRAINTRUST_API_KEY", previous_api_key);

        assert_eq!(cli.command.base().api_key_source, None);
    }

    #[test]
    fn apply_base_arg_sources_tracks_cli_verbose() {
        let matches = Cli::command()
            .try_get_matches_from(["bt", "sync", "pull", "--verbose"])
            .expect("matches");
        let mut cli = Cli::from_arg_matches(&matches).expect("cli");

        apply_base_arg_sources(&matches, cli.command.base_mut());

        assert_eq!(
            cli.command.base().verbose_source,
            Some(ArgValueSource::CommandLine)
        );
        assert!(cli.command.base().verbose_explicit());
    }

    #[test]
    fn login_rejects_context_selection_flags() {
        for args in [
            ["bt", "login", "--org", "test-org"],
            ["bt", "login", "--project", "test-project"],
        ] {
            let err = Cli::try_parse_from(args).expect_err("context flag should be rejected");
            assert_eq!(err.kind(), clap::error::ErrorKind::UnknownArgument);
        }
    }

    #[test]
    fn default_verbose_output_is_not_explicit_verbose() {
        let matches = Cli::command()
            .try_get_matches_from(["bt", "sync", "pull"])
            .expect("matches");
        let mut cli = Cli::from_arg_matches(&matches).expect("cli");

        apply_base_arg_sources(&matches, cli.command.base_mut());
        apply_base_output_defaults(&mut cli.command);

        assert!(cli.command.base().verbose);
        assert_eq!(cli.command.base().verbose_source, None);
        assert!(!cli.command.base().verbose_explicit());
    }

    #[test]
    fn apply_base_output_defaults_keeps_setup_quiet_by_default() {
        let matches = Cli::command()
            .try_get_matches_from([
                "bt",
                "setup",
                "--no-instrument",
                "--global",
                "--agent",
                "codex",
            ])
            .expect("matches");
        let mut cli = Cli::from_arg_matches(&matches).expect("cli");

        apply_base_output_defaults(&mut cli.command);

        assert!(cli.command.base().quiet);
        assert!(!cli.command.base().verbose);
    }

    #[test]
    fn apply_base_output_defaults_keeps_status_verbose_by_default() {
        let matches = Cli::command()
            .try_get_matches_from(["bt", "status"])
            .expect("matches");
        let mut cli = Cli::from_arg_matches(&matches).expect("cli");

        apply_base_output_defaults(&mut cli.command);

        assert!(!cli.command.base().quiet);
        assert!(cli.command.base().verbose);
    }

    #[test]
    fn apply_base_output_defaults_honors_explicit_verbose_for_setup() {
        let matches = Cli::command()
            .try_get_matches_from([
                "bt",
                "setup",
                "--verbose",
                "--no-instrument",
                "--global",
                "--agent",
                "codex",
            ])
            .expect("matches");
        let mut cli = Cli::from_arg_matches(&matches).expect("cli");

        apply_base_output_defaults(&mut cli.command);

        assert!(!cli.command.base().quiet);
        assert!(cli.command.base().verbose);
    }

    fn argv(parts: &[&str]) -> Vec<OsString> {
        parts.iter().map(OsString::from).collect()
    }

    #[test]
    fn typed_user_errors_use_the_user_exit_code() {
        let err =
            crate::error::user_error(anyhow::anyhow!("--temperature must be between 0 and 2"));

        assert_eq!(classify_error(&err, false), ExitCode::User);
    }

    #[test]
    fn provider_credential_errors_are_not_classified_as_bt_auth_errors() {
        let err = crate::error::user_error(anyhow::Error::new(crate::http::HttpError {
            status: reqwest::StatusCode::UNAUTHORIZED,
            body: serde_json::json!({
                "error": {
                    "message": "Incorrect API key provided: synthetic-key",
                    "type": "invalid_request_error",
                    "code": "invalid_api_key"
                },
                "status": 401
            })
            .to_string(),
        }));

        assert_eq!(classify_error(&err, false), ExitCode::User);
        let payload = json_error_payload(&err);
        assert_eq!(
            payload["error"]["message"],
            "Incorrect API key provided: synthetic-key"
        );
        assert_eq!(
            payload["error"]["details"]["error"]["code"],
            "invalid_api_key"
        );
    }

    #[test]
    fn json_http_errors_use_a_stable_envelope() {
        let err = anyhow::Error::new(crate::http::HttpError {
            status: reqwest::StatusCode::BAD_REQUEST,
            body: serde_json::json!(["synthetic", "details"]).to_string(),
        });

        let payload = json_error_payload(&err);
        assert!(payload["error"]["message"].is_string());
        assert_eq!(
            payload["error"]["details"],
            serde_json::json!(["synthetic", "details"])
        );
    }

    #[test]
    fn bt_unauthorized_errors_remain_auth_errors() {
        for body in [
            serde_json::json!({ "error": "Unauthorized" }),
            serde_json::json!({
                "error": {
                    "message": "Invalid Braintrust API key",
                    "code": "invalid_api_key"
                }
            }),
        ] {
            let err = anyhow::Error::new(crate::http::HttpError {
                status: reqwest::StatusCode::UNAUTHORIZED,
                body: body.to_string(),
            });

            assert_eq!(classify_error(&err, false), ExitCode::Auth);
        }
    }

    #[test]
    fn handle_version_json_detects_long_form() {
        assert!(handle_version_json(&argv(&["bt", "--version", "--json"])).unwrap());
        assert!(handle_version_json(&argv(&["bt", "--json", "--version"])).unwrap());
    }

    #[test]
    fn handle_version_json_detects_short_form() {
        assert!(handle_version_json(&argv(&["bt", "-V", "--json"])).unwrap());
    }

    #[test]
    fn handle_version_json_requires_both_flags() {
        assert!(!handle_version_json(&argv(&["bt", "--version"])).unwrap());
        assert!(!handle_version_json(&argv(&["bt", "--json", "status"])).unwrap());
    }

    #[test]
    fn handle_version_json_ignores_args_after_double_dash() {
        assert!(
            !handle_version_json(&argv(&["bt", "eval", "--", "--version", "--json",])).unwrap()
        );
    }
}
