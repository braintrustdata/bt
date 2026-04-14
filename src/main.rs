use anyhow::Result;
use clap::{Parser, Subcommand};
use std::ffi::{OsStr, OsString};

mod args;
mod auth;
#[allow(dead_code)]
mod config;
mod env;
#[cfg(unix)]
mod eval;
mod experiments;
mod functions;
mod http;
mod init;
mod js_runner;
mod projects;
mod prompts;
mod python_runner;
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
mod traces;
mod ui;
mod util_cmd;
mod utils;

use crate::args::{BaseArgs, CLIArgs};

const DEFAULT_CANARY_VERSION: &str = concat!(env!("CARGO_PKG_VERSION"), "-canary.dev");
const CLI_VERSION: &str = match option_env!("BT_VERSION_STRING") {
    Some(version) => version,
    None => DEFAULT_CANARY_VERSION,
};

const BANNER: &str = r#"

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
  auth         Authenticate bt with Braintrust
  switch       Switch org and project context
  view         View logs, traces, and spans

Projects & resources
  projects     Manage projects
  topics       Inspect Topics automation status
  prompts      Manage prompts
  functions    Manage functions (tools, scorers, and more)
  tools        Manage tools
  scorers      Manage scorers
  experiments  Manage experiments

Data & evaluation
  eval         Run eval files
  sql          Run SQL queries against Braintrust
  sync         Synchronize project logs between Braintrust and local NDJSON files

Additional
  docs         Manage workflow docs for coding agents
  self         Self-management commands
  setup        Configure Braintrust setup flows
  status       Show current org and project context

Flags
      --profile <PROFILE>    Use a saved login profile [env: BRAINTRUST_PROFILE]
  -o, --org <ORG>            Override active org [env: BRAINTRUST_ORG_NAME]
  -p, --project <PROJECT>    Override active project [env: BRAINTRUST_DEFAULT_PROJECT]
  -q, --quiet                Suppress non-essential output
      --json                 Output as JSON
      --no-color             Disable ANSI color output
      --no-input             Disable all interactive prompts
      --api-url <URL>        Override API URL [env: BRAINTRUST_API_URL]
      --app-url <URL>        Override app URL [env: BRAINTRUST_APP_URL]
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
    /// Authenticate bt with Braintrust
    Auth(CLIArgs<auth::AuthArgs>),
    /// View logs, traces, and spans
    View(CLIArgs<traces::ViewArgs>),
    #[cfg(unix)]
    /// Run eval files
    Eval(CLIArgs<eval::EvalArgs>),
    /// Manage projects
    Projects(CLIArgs<projects::ProjectsArgs>),
    /// Inspect Topics automation status
    Topics(CLIArgs<topics::TopicsArgs>),
    /// Manage prompts
    Prompts(CLIArgs<prompts::PromptsArgs>),
    #[command(name = "self")]
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
    /// Show current org and project context
    Status(CLIArgs<status::StatusArgs>),
    // /// View and modify config
    // Config(CLIArgs<config::ConfigArgs>),
}

impl Commands {
    fn base(&self) -> &BaseArgs {
        match self {
            Commands::Init(cmd) => &cmd.base,
            Commands::Setup(cmd) => &cmd.base,
            Commands::Docs(cmd) => &cmd.base,
            Commands::Sql(cmd) => &cmd.base,
            Commands::Auth(cmd) => &cmd.base,
            Commands::View(cmd) => &cmd.base,
            #[cfg(unix)]
            Commands::Eval(cmd) => &cmd.base,
            Commands::Projects(cmd) => &cmd.base,
            Commands::Topics(cmd) => &cmd.base,
            Commands::Prompts(cmd) => &cmd.base,
            Commands::SelfCommand(cmd) => &cmd.base,
            Commands::Tools(cmd) => &cmd.base,
            Commands::Scorers(cmd) => &cmd.base,
            Commands::Functions(cmd) => &cmd.base,
            Commands::Experiments(cmd) => &cmd.base,
            Commands::Sync(cmd) => &cmd.base,
            Commands::Util(cmd) => &cmd.base,
            Commands::Switch(cmd) => &cmd.base,
            Commands::Status(cmd) => &cmd.base,
        }
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

#[tokio::main]
async fn main() {
    let exit_code = match try_main().await {
        Ok(()) => ExitCode::Success,
        Err(err) => {
            let code = classify_error(&err);
            print_error(&err, code);
            code
        }
    };
    std::process::exit(exit_code as i32);
}

async fn try_main() -> Result<()> {
    let argv: Vec<OsString> = std::env::args_os().collect();
    env::bootstrap_from_args(&argv)?;

    let cli = Cli::parse_from(argv);
    configure_output(cli.command.base());

    match cli.command {
        Commands::Auth(cmd) => auth::run(cmd.base, cmd.args).await?,
        Commands::View(cmd) => traces::run(cmd.base, cmd.args).await?,
        Commands::Init(cmd) => init::run(cmd.base, cmd.args).await?,
        Commands::Sql(cmd) => sql::run(cmd.base, cmd.args).await?,
        Commands::Setup(cmd) => setup::run_setup_top(cmd.base, cmd.args).await?,
        Commands::Docs(cmd) => setup::run_docs_top(cmd.base, cmd.args).await?,
        #[cfg(unix)]
        Commands::Eval(cmd) => eval::run(cmd.base, cmd.args).await?,
        Commands::Projects(cmd) => projects::run(cmd.base, cmd.args).await?,
        Commands::Topics(cmd) => topics::run(cmd.base, cmd.args).await?,
        Commands::Prompts(cmd) => prompts::run(cmd.base, cmd.args).await?,
        Commands::Tools(cmd) => tools::run(cmd.base, cmd.args).await?,
        Commands::Scorers(cmd) => scorers::run(cmd.base, cmd.args).await?,
        Commands::Functions(cmd) => functions::run(cmd.base, cmd.args).await?,
        Commands::Experiments(cmd) => experiments::run(cmd.base, cmd.args).await?,
        Commands::Sync(cmd) => sync::run(cmd.base, cmd.args).await?,
        Commands::Util(cmd) => util_cmd::run(cmd.base, cmd.args).await?,
        Commands::SelfCommand(cmd) => self_update::run(cmd.args).await?,
        Commands::Switch(cmd) => switch::run(cmd.base, cmd.args).await?,
        Commands::Status(cmd) => status::run(cmd.base, cmd.args).await?,
    }

    Ok(())
}

fn configure_output(base: &BaseArgs) {
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

    if base.quiet {
        ui::set_quiet(true);
        ui::set_animations_enabled(false);
    }

    if base.no_input {
        ui::set_no_input(true);
    }

    if disable_color {
        dialoguer::console::set_colors_enabled(false);
        dialoguer::console::set_colors_enabled_stderr(false);
    }
}

fn classify_error(err: &anyhow::Error) -> ExitCode {
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

fn has_io_error(err: &anyhow::Error) -> bool {
    err.chain()
        .any(|source| source.downcast_ref::<std::io::Error>().is_some())
}

fn looks_like_user_error(err: &anyhow::Error) -> bool {
    let message = err.to_string().to_lowercase();
    message.contains("required")
        || message.contains("use:")
        || message.contains("not found")
        || message.contains("invalid")
}

fn print_error(err: &anyhow::Error, code: ExitCode) {
    eprintln!("error: {err}");
    if code == ExitCode::Error {
        eprintln!("If this seems like a bug, file an issue at https://github.com/braintrustdata/bt/issues/new and include `bt --version`, `bt status --json`, and the command you ran.");
    }
}
