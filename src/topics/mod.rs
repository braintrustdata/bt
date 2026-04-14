use anyhow::Result;
use clap::{Args, Subcommand};

use crate::{args::BaseArgs, projects::context::resolve_project_context};

pub(crate) mod api;
mod config;
mod open;
mod poke;
mod rewind;
mod status;

pub(crate) type ResolvedContext = crate::projects::context::ProjectContext;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt topics
  bt topics status
  bt topics status --full
  bt topics status --watch
  bt topics config
  bt topics config set --topic-window 1h --generation-cadence 1d
  bt topics poke
  bt topics rewind 7d
  bt topics open
")]
pub struct TopicsArgs {
    #[command(subcommand)]
    command: Option<TopicsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum TopicsCommands {
    /// Show Topics automation status for the active project
    Status(StatusArgs),
    /// View or edit Topics automation config
    Config(ConfigArgs),
    /// Queue Topics to run on the next executor pass
    Poke,
    /// Rewind recent Topics history and queue it to reprocess
    Rewind(RewindArgs),
    /// Open the Topics page in the browser
    Open,
}

#[derive(Debug, Clone, Args)]
struct StatusArgs {
    /// Show expanded diagnostics, including the state machine
    #[arg(long)]
    full: bool,

    /// Refresh every 2 seconds until interrupted
    #[arg(long)]
    watch: bool,
}

#[derive(Debug, Clone, Args)]
struct ConfigArgs {
    /// Specific automation ID to show
    #[arg(long = "automation-id")]
    automation_id: Option<String>,

    #[command(subcommand)]
    command: Option<ConfigCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum ConfigCommands {
    /// Update editable Topics config fields
    Set(ConfigSetArgs),
}

#[derive(Debug, Clone, Args)]
struct ConfigSetArgs {
    /// Specific automation ID to update
    #[arg(long = "automation-id")]
    automation_id: Option<String>,

    /// Human-friendly automation name
    #[arg(long)]
    name: Option<String>,

    /// Human-friendly automation description
    #[arg(long)]
    description: Option<String>,

    /// Topic window duration, for example 1h or 1d
    #[arg(long = "topic-window", alias = "window")]
    window: Option<String>,

    /// How often Topics should try to generate fresh topic maps, for example 1h or 1d
    #[arg(long = "generation-cadence", alias = "cadence")]
    cadence: Option<String>,

    /// Relabel overlap duration, for example 1h
    #[arg(long = "relabel-overlap")]
    relabel_overlap: Option<String>,

    /// Trace idle wait duration, for example 30s
    #[arg(long = "idle-time", alias = "idle")]
    idle: Option<String>,

    /// Percent of matching traces to sample, for example 25 or 25%
    #[arg(long = "sampling-rate")]
    sampling_rate: Option<String>,

    /// BTQL filter used to select which traces get facets and topics
    #[arg(long, conflicts_with = "clear_filter")]
    filter: Option<String>,

    /// Clear the top-level BTQL filter
    #[arg(long, conflicts_with = "filter")]
    clear_filter: bool,
}

#[derive(Debug, Clone, Args)]
struct RewindArgs {
    /// Specific automation ID to rewind
    #[arg(long = "automation-id")]
    automation_id: Option<String>,

    /// Topic window to reprocess, for example 1h or 7d
    topic_window: String,
}

pub async fn run(base: BaseArgs, args: TopicsArgs) -> Result<()> {
    let read_only = matches!(
        args.command.as_ref(),
        None | Some(TopicsCommands::Status(_))
            | Some(TopicsCommands::Open)
            | Some(TopicsCommands::Config(ConfigArgs { command: None, .. }))
    );
    let ctx = resolve_project_context(&base, read_only).await?;

    match args.command {
        None => {
            status::run(
                &ctx,
                StatusArgs {
                    full: false,
                    watch: false,
                },
                base.json,
            )
            .await
        }
        Some(TopicsCommands::Status(status_args)) => {
            status::run(&ctx, status_args, base.json).await
        }
        Some(TopicsCommands::Config(config_args)) => match config_args.command {
            None => config::run_view(&ctx, &config_args, base.json).await,
            Some(ConfigCommands::Set(set_args)) => {
                config::run_set(&ctx, &set_args, base.json).await
            }
        },
        Some(TopicsCommands::Poke) => poke::run(&ctx, base.json).await,
        Some(TopicsCommands::Rewind(rewind_args)) => {
            rewind::run(&ctx, &rewind_args, base.json).await
        }
        Some(TopicsCommands::Open) => open::run(&ctx).await,
    }
}

#[cfg(test)]
mod tests {
    use clap::{Parser, Subcommand};

    use super::*;

    #[derive(Debug, Parser)]
    struct CliHarness {
        #[command(subcommand)]
        command: Commands,
    }

    #[derive(Debug, Subcommand)]
    enum Commands {
        Topics(TopicsArgs),
    }

    fn parse(args: &[&str]) -> anyhow::Result<TopicsArgs> {
        let mut argv = vec!["bt"];
        argv.extend_from_slice(args);
        let parsed = CliHarness::try_parse_from(argv)?;
        match parsed.command {
            Commands::Topics(args) => Ok(args),
        }
    }

    fn topics_command_is_read_only(command: Option<&TopicsCommands>) -> bool {
        matches!(
            command,
            None | Some(TopicsCommands::Status(_))
                | Some(TopicsCommands::Open)
                | Some(TopicsCommands::Config(ConfigArgs { command: None, .. }))
        )
    }

    #[test]
    fn topics_commands_use_read_only_auth() {
        let parsed = parse(&["topics"]).expect("parse");
        assert!(topics_command_is_read_only(parsed.command.as_ref()));

        let parsed = parse(&["topics", "status"]).expect("parse");
        assert!(topics_command_is_read_only(parsed.command.as_ref()));

        let parsed = parse(&["topics", "status", "--full", "--watch"]).expect("parse");
        let Some(TopicsCommands::Status(status)) = parsed.command.as_ref() else {
            panic!("expected status command");
        };
        assert!(status.full);
        assert!(status.watch);

        let parsed = parse(&["topics", "open"]).expect("parse");
        assert!(topics_command_is_read_only(parsed.command.as_ref()));
    }

    #[test]
    fn topics_poke_uses_validated_auth() {
        let parsed = parse(&["topics", "poke"]).expect("parse");
        assert!(!topics_command_is_read_only(parsed.command.as_ref()));
    }

    #[test]
    fn topics_rewind_uses_validated_auth() {
        let parsed = parse(&["topics", "rewind", "7d"]).expect("parse");
        assert!(!topics_command_is_read_only(parsed.command.as_ref()));
    }

    #[test]
    fn topics_config_view_uses_read_only_auth() {
        let parsed = parse(&["topics", "config"]).expect("parse");
        assert!(topics_command_is_read_only(parsed.command.as_ref()));
    }

    #[test]
    fn topics_config_set_uses_validated_auth() {
        let parsed = parse(&["topics", "config", "set", "--topic-window", "1h"]).expect("parse");
        assert!(!topics_command_is_read_only(parsed.command.as_ref()));
    }

    #[test]
    fn topics_config_set_accepts_legacy_flag_aliases() {
        let parsed = parse(&[
            "topics",
            "config",
            "set",
            "--window",
            "1h",
            "--cadence",
            "1d",
            "--idle",
            "30s",
        ])
        .expect("parse");

        let Some(TopicsCommands::Config(ConfigArgs {
            command: Some(ConfigCommands::Set(set_args)),
            ..
        })) = parsed.command.as_ref()
        else {
            panic!("expected config set command");
        };

        assert_eq!(set_args.window.as_deref(), Some("1h"));
        assert_eq!(set_args.cadence.as_deref(), Some("1d"));
        assert_eq!(set_args.idle.as_deref(), Some("30s"));
    }

    #[test]
    fn topics_rewind_uses_positional_window() {
        let parsed = parse(&["topics", "rewind", "7d"]).expect("parse");
        let Some(TopicsCommands::Rewind(rewind_args)) = parsed.command.as_ref() else {
            panic!("expected rewind command");
        };
        assert_eq!(rewind_args.topic_window.as_str(), "7d");
    }
}
