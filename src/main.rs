use anyhow::Result;
use clap::{Parser, Subcommand};
use std::ffi::OsString;

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
mod projects;
mod prompts;
mod scorers;
mod self_update;
mod setup;
mod sql;
mod status;
mod switch;
mod sync;
mod tools;
mod traces;
mod ui;
mod utils;

use crate::args::CLIArgs;

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
  init      Initialize .bt config directory and files
  auth      Authenticate bt with Braintrust
  switch    Switch org and project context
  view      View logs, traces, and spans

Projects & resources
  projects  Manage projects
  prompts   Manage prompts

Data & evaluation
  eval      Run eval files
  sql       Run SQL queries against Braintrust
  sync      Synchronize project logs between Braintrust and local NDJSON files

Additional
  docs      Manage workflow docs for coding agents
  self      Self-management commands
  setup     Configure Braintrust setup flows
  status    Show current org and project context

Flags
      --profile <PROFILE>    Use a saved login profile [env: BRAINTRUST_PROFILE]
  -o, --org <ORG>            Override active org [env: BRAINTRUST_ORG_NAME]
  -p, --project <PROJECT>    Override active project [env: BRAINTRUST_DEFAULT_PROJECT]
      --json                 Output as JSON
      --api-url <URL>        Override API URL [env: BRAINTRUST_API_URL]
      --app-url <URL>        Override app URL [env: BRAINTRUST_APP_URL]
      --env-file <PATH>      Path to a .env file to load
  -h, --help                 Print help
  -V, --version              Print version

LEARN MORE
Use `bt <command> <subcommand> --help` for more information about a command.
Read the manual at https://braintrust.dev/docs/cli

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
    /// Manage prompts
    Prompts(CLIArgs<prompts::PromptsArgs>),
    #[command(name = "self")]
    /// Self-management commands
    SelfCommand(self_update::SelfArgs),
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
    /// Switch org and project context
    Switch(CLIArgs<switch::SwitchArgs>),
    /// Show current org and project context
    Status(CLIArgs<status::StatusArgs>),
    // /// View and modify config
    // Config(CLIArgs<config::ConfigArgs>),
}

#[tokio::main]
async fn main() -> Result<()> {
    let argv: Vec<OsString> = std::env::args_os().collect();
    env::bootstrap_from_args(&argv)?;

    if std::env::var_os("NO_COLOR").is_some() {
        dialoguer::console::set_colors_enabled(false);
        dialoguer::console::set_colors_enabled_stderr(false);
    }
    if argv.iter().any(|a| a == "--no-input") {
        ui::set_no_input(true);
    }

    let cli = Cli::parse_from(argv);

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
        Commands::Prompts(cmd) => prompts::run(cmd.base, cmd.args).await?,
        Commands::Tools(cmd) => tools::run(cmd.base, cmd.args).await?,
        Commands::Scorers(cmd) => scorers::run(cmd.base, cmd.args).await?,
        Commands::Functions(cmd) => functions::run_functions(cmd.base, cmd.args).await?,
        Commands::Experiments(cmd) => experiments::run(cmd.base, cmd.args).await?,
        Commands::Sync(cmd) => sync::run(cmd.base, cmd.args).await?,
        Commands::SelfCommand(args) => self_update::run(args).await?,
        Commands::Switch(cmd) => switch::run(cmd.base, cmd.args).await?,
        Commands::Status(cmd) => status::run(cmd.base, cmd.args).await?,
    }

    Ok(())
}
