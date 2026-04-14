use anyhow::Result;
use clap::{Args, Subcommand};

use crate::{args::BaseArgs, projects::context::resolve_project_context};

pub(crate) mod api;
mod open;
mod poke;
mod status;

pub(crate) type ResolvedContext = crate::projects::context::ProjectContext;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt topics
  bt topics status
  bt topics status --full
  bt topics status --watch
  bt topics poke
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
    /// Queue Topics to run on the next executor pass
    Poke,
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

pub async fn run(base: BaseArgs, args: TopicsArgs) -> Result<()> {
    let read_only = matches!(
        args.command.as_ref(),
        None | Some(TopicsCommands::Status(_)) | Some(TopicsCommands::Open)
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
        Some(TopicsCommands::Poke) => poke::run(&ctx, base.json).await,
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
            None | Some(TopicsCommands::Status(_)) | Some(TopicsCommands::Open)
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
}
