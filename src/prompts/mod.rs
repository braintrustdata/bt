use anyhow::Result;
use clap::{Args, Subcommand};

use crate::{args::BaseArgs, project_context::resolve_project_command_context_with_auth_mode};

mod api;
mod delete;
mod list;
mod view;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt prompts list
  bt prompts view my-prompt
  bt prompts delete my-prompt
")]
pub struct PromptsArgs {
    #[command(subcommand)]
    command: Option<PromptsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum PromptsCommands {
    /// List all prompts
    List,
    /// View a prompt's content
    View(ViewArgs),
    /// Delete a prompt
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ViewArgs {
    /// Prompt slug (positional)
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,

    /// Prompt slug (flag)
    #[arg(long = "slug", short = 's')]
    slug_flag: Option<String>,

    /// Open in browser instead of showing in terminal
    #[arg(long)]
    web: bool,

    /// Show all model parameters and configuration
    #[arg(long)]
    verbose: bool,
}

impl ViewArgs {
    fn slug(&self) -> Option<&str> {
        self.slug_positional
            .as_deref()
            .or(self.slug_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
pub struct DeleteArgs {
    /// Prompt slug (positional) of the prompt to delete
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,

    /// Prompt slug (flag) of the prompt to delete
    #[arg(long = "slug", short = 's')]
    slug_flag: Option<String>,

    /// Skip confirmation prompt (requires slug)
    #[arg(long, short = 'f')]
    force: bool,
}

impl DeleteArgs {
    fn slug(&self) -> Option<&str> {
        self.slug_positional
            .as_deref()
            .or(self.slug_flag.as_deref())
    }
}

pub async fn run(base: BaseArgs, args: PromptsArgs) -> Result<()> {
    let read_only = prompts_command_is_read_only(args.command.as_ref());
    let ctx = resolve_project_command_context_with_auth_mode(&base, read_only).await?;

    match args.command {
        None | Some(PromptsCommands::List) => list::run(&ctx, base.json).await,
        Some(PromptsCommands::View(p)) => {
            view::run(&ctx, p.slug(), base.json, p.web, p.verbose).await
        }
        Some(PromptsCommands::Delete(p)) => delete::run(&ctx, p.slug(), p.force).await,
    }
}

fn prompts_command_is_read_only(command: Option<&PromptsCommands>) -> bool {
    matches!(
        command,
        None | Some(PromptsCommands::List) | Some(PromptsCommands::View(_))
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompts_routes_list_and_view_to_read_only_auth() {
        assert!(prompts_command_is_read_only(None));
        assert!(prompts_command_is_read_only(Some(&PromptsCommands::List)));
        assert!(prompts_command_is_read_only(Some(&PromptsCommands::View(
            ViewArgs {
                slug_positional: Some("my-prompt".to_string()),
                slug_flag: None,
                web: false,
                verbose: false,
            }
        ))));
    }

    #[test]
    fn prompts_routes_delete_to_validated_auth() {
        assert!(!prompts_command_is_read_only(Some(
            &PromptsCommands::Delete(DeleteArgs {
                slug_positional: Some("my-prompt".to_string()),
                slug_flag: None,
                force: true,
            })
        )));
    }
}
