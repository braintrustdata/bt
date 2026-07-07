use anyhow::{bail, Result};
use clap::{Args, Subcommand};

use crate::{args::BaseArgs, project_context::resolve_project_command_context_with_auth_mode};

pub(crate) use crate::project_context::ProjectContext as ResolvedContext;

pub mod api;
mod delete;
mod list;
mod rename;
mod view;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Playgrounds are interactive no-code workspaces backed by the prompt_session
resource. Listing, viewing, deleting, and renaming operate on the current
project context (see `bt switch` / `bt status`).

Examples:
  bt playgrounds list
  bt playgrounds view my-playground
  bt playgrounds view my-playground --web
  bt playgrounds rename my-playground --name new-name
  bt playgrounds delete my-playground --force
")]
pub struct PlaygroundsArgs {
    #[command(subcommand)]
    command: Option<PlaygroundsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum PlaygroundsCommands {
    /// List all playgrounds in the current project
    List,
    /// View a playground's metadata, tasks, and recent runs
    View(ViewArgs),
    /// Delete a playground (soft delete)
    Delete(DeleteArgs),
    /// Rename a playground and/or update its description
    Rename(RenameArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ViewArgs {
    /// Playground name (positional)
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Playground name (flag)
    #[arg(long = "name", short = 'n')]
    name_flag: Option<String>,

    /// Open in the browser instead of showing in the terminal
    #[arg(long)]
    web: bool,
}

impl ViewArgs {
    fn name(&self) -> Option<&str> {
        self.name_positional
            .as_deref()
            .or(self.name_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
pub struct DeleteArgs {
    /// Playground name (positional) of the playground to delete
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Playground name (flag) of the playground to delete
    #[arg(long = "name", short = 'n')]
    name_flag: Option<String>,

    /// Skip confirmation prompt (requires name)
    #[arg(long, short = 'f')]
    force: bool,
}

impl DeleteArgs {
    fn name(&self) -> Option<&str> {
        self.name_positional
            .as_deref()
            .or(self.name_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
pub struct RenameArgs {
    /// Playground name (positional) of the playground to rename
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Playground name (flag) of the playground to rename
    #[arg(long = "name", short = 'n')]
    name_flag: Option<String>,

    /// New playground name
    #[arg(long = "new-name")]
    new_name: Option<String>,

    /// New description
    #[arg(long = "description")]
    description: Option<String>,

    /// Skip confirmation prompt
    #[arg(long, short = 'f')]
    force: bool,
}

impl RenameArgs {
    fn name(&self) -> Option<&str> {
        self.name_positional
            .as_deref()
            .or(self.name_flag.as_deref())
    }
}

pub async fn run(base: BaseArgs, args: PlaygroundsArgs) -> Result<()> {
    let read_only = playgrounds_command_is_read_only(args.command.as_ref());
    let ctx = resolve_project_command_context_with_auth_mode(&base, read_only).await?;

    match args.command {
        None | Some(PlaygroundsCommands::List) => list::run(&ctx, base.json).await,
        Some(PlaygroundsCommands::View(v)) => {
            view::run(&ctx, v.name(), base.json, v.web, base.verbose).await
        }
        Some(PlaygroundsCommands::Delete(d)) => delete::run(&ctx, d.name(), d.force).await,
        Some(PlaygroundsCommands::Rename(r)) => {
            if r.new_name.is_none() && r.description.is_none() {
                bail!(
                    "nothing to change. Use --new-name and/or --description. \
                     Run `bt playgrounds rename --help` for usage."
                );
            }
            rename::run(
                &ctx,
                r.name(),
                r.new_name.as_deref(),
                r.description.as_deref(),
                r.force,
            )
            .await
        }
    }
}

fn playgrounds_command_is_read_only(command: Option<&PlaygroundsCommands>) -> bool {
    matches!(
        command,
        None | Some(PlaygroundsCommands::List) | Some(PlaygroundsCommands::View(_))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn list_and_view_are_read_only() {
        assert!(playgrounds_command_is_read_only(None));
        assert!(playgrounds_command_is_read_only(Some(
            &PlaygroundsCommands::List
        )));
        assert!(playgrounds_command_is_read_only(Some(
            &PlaygroundsCommands::View(ViewArgs {
                name_positional: Some("my-playground".to_string()),
                name_flag: None,
                web: false,
            })
        )));
    }

    #[test]
    fn delete_and_rename_require_full_auth() {
        assert!(!playgrounds_command_is_read_only(Some(
            &PlaygroundsCommands::Delete(DeleteArgs {
                name_positional: Some("my-playground".to_string()),
                name_flag: None,
                force: true,
            })
        )));
        assert!(!playgrounds_command_is_read_only(Some(
            &PlaygroundsCommands::Rename(RenameArgs {
                name_positional: Some("my-playground".to_string()),
                name_flag: None,
                new_name: Some("new-name".to_string()),
                description: None,
                force: false,
            })
        )));
    }
}
