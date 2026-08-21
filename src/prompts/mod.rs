use anyhow::{anyhow, bail, Result};
use clap::{Args, Subcommand};

use crate::{args::BaseArgs, project_context::resolve_project_command_context_with_auth_mode};

pub(crate) use crate::project_context::ProjectContext as ResolvedContext;

mod api;
mod delete;
mod list;
mod promote;
mod view;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt prompts list
  bt prompts list --environment production
  bt prompts view my-prompt --environment production
  bt prompts promote my-prompt --environment production --version 1234
  bt prompts delete my-prompt
")]
pub struct PromptsArgs {
    #[command(subcommand)]
    command: Option<PromptsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum PromptsCommands {
    /// List all prompts
    List(ListArgs),
    /// View a prompt's content
    View(ViewArgs),
    /// Promote a prompt version to an environment
    Promote(PromoteArgs),
    /// Delete a prompt
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
struct PromptEnvironmentArgs {
    /// Environment slug (for example, production)
    #[arg(long, env = "BT_PROMPTS_ENVIRONMENT")]
    environment: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct PromptVersionArgs {
    /// Prompt version identifier (for example, a transaction ID)
    #[arg(long, env = "BT_PROMPTS_VERSION")]
    version: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct PromptSelectorArgs {
    #[command(flatten)]
    version: PromptVersionArgs,

    #[command(flatten)]
    environment: PromptEnvironmentArgs,
}

impl PromptSelectorArgs {
    fn version(&self) -> Option<&str> {
        self.version.version.as_deref()
    }

    fn environment(&self) -> Option<&str> {
        self.environment.environment.as_deref()
    }
}

#[derive(Debug, Clone, Args)]
pub struct ListArgs {
    #[command(flatten)]
    environment: PromptEnvironmentArgs,
}

#[derive(Debug, Clone, Args)]
pub struct ViewArgs {
    /// Prompt slug (positional)
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,

    /// Prompt slug (flag)
    #[arg(long = "slug", short = 's')]
    slug_flag: Option<String>,

    #[command(flatten)]
    selector: PromptSelectorArgs,

    /// Open in browser instead of showing in terminal
    #[arg(long)]
    web: bool,
}

impl ViewArgs {
    fn slug(&self) -> Option<&str> {
        self.slug_positional
            .as_deref()
            .or(self.slug_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
pub struct PromoteArgs {
    /// Prompt slug (positional)
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,

    /// Prompt slug (flag)
    #[arg(long = "slug", short = 's')]
    slug_flag: Option<String>,

    #[command(flatten)]
    selector: PromptSelectorArgs,
}

impl PromoteArgs {
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
        None => list::run(&ctx, None, base.json).await,
        Some(PromptsCommands::List(args)) => {
            list::run(&ctx, args.environment.environment.as_deref(), base.json).await
        }
        Some(PromptsCommands::View(args)) => {
            if args.selector.version().is_some() && args.selector.environment().is_some() {
                bail!("--version and --environment cannot be used together");
            }
            view::run(
                &ctx,
                args.slug(),
                args.selector.version(),
                args.selector.environment(),
                base.json,
                args.web,
                base.verbose,
            )
            .await
        }
        Some(PromptsCommands::Promote(args)) => {
            let hint =
                "Use: bt prompts promote <slug> --environment <environment> --version <version>";
            let version = args
                .selector
                .version()
                .ok_or_else(|| anyhow!("--version is required. {hint}"))?;
            let environment = args
                .selector
                .environment()
                .ok_or_else(|| anyhow!("--environment is required. {hint}"))?;
            promote::run(&ctx, args.slug(), environment, version, base.json).await
        }
        Some(PromptsCommands::Delete(args)) => delete::run(&ctx, args.slug(), args.force).await,
    }
}

fn prompts_command_is_read_only(command: Option<&PromptsCommands>) -> bool {
    matches!(
        command,
        None | Some(PromptsCommands::List(_)) | Some(PromptsCommands::View(_))
    )
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    struct CliHarness {
        #[command(flatten)]
        prompts: PromptsArgs,
    }

    fn selectors(version: Option<&str>, environment: Option<&str>) -> PromptSelectorArgs {
        PromptSelectorArgs {
            version: PromptVersionArgs {
                version: version.map(ToOwned::to_owned),
            },
            environment: PromptEnvironmentArgs {
                environment: environment.map(ToOwned::to_owned),
            },
        }
    }

    #[test]
    fn subcommands_only_expose_supported_selectors() {
        let list =
            CliHarness::try_parse_from(["bt-prompts", "list", "--environment", "production"])
                .expect("parse list");
        let Some(PromptsCommands::List(list)) = list.prompts.command else {
            panic!("expected list command");
        };
        assert_eq!(list.environment.environment.as_deref(), Some("production"));

        let error = CliHarness::try_parse_from(["bt-prompts", "list", "--version", "1234"])
            .expect_err("list should reject version");
        assert!(error
            .to_string()
            .contains("unexpected argument '--version'"));

        let promote = CliHarness::try_parse_from([
            "bt-prompts",
            "promote",
            "test-prompt",
            "--environment",
            "production",
            "--version",
            "1234",
        ])
        .expect("parse promote");
        let Some(PromptsCommands::Promote(promote)) = promote.prompts.command else {
            panic!("expected promote command");
        };
        assert_eq!(promote.selector.version(), Some("1234"));
        assert_eq!(promote.selector.environment(), Some("production"));
    }

    #[test]
    fn prompts_routes_list_and_view_to_read_only_auth() {
        assert!(prompts_command_is_read_only(None));
        assert!(prompts_command_is_read_only(Some(&PromptsCommands::List(
            ListArgs {
                environment: PromptEnvironmentArgs { environment: None },
            }
        ))));
        assert!(prompts_command_is_read_only(Some(&PromptsCommands::View(
            ViewArgs {
                slug_positional: Some("test-prompt".to_string()),
                slug_flag: None,
                selector: selectors(None, None),
                web: false,
            }
        ))));
    }

    #[test]
    fn prompts_routes_mutations_to_validated_auth() {
        assert!(!prompts_command_is_read_only(Some(
            &PromptsCommands::Promote(PromoteArgs {
                slug_positional: Some("test-prompt".to_string()),
                slug_flag: None,
                selector: selectors(Some("1234"), Some("production")),
            })
        )));
        assert!(!prompts_command_is_read_only(Some(
            &PromptsCommands::Delete(DeleteArgs {
                slug_positional: Some("test-prompt".to_string()),
                slug_flag: None,
                force: true,
            })
        )));
    }
}
