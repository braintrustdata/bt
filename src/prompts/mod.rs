use anyhow::{anyhow, bail, Result};
use clap::{Args, Subcommand};

use crate::ui::{is_interactive, with_spinner};
use crate::{args::BaseArgs, project_context::resolve_project_command_context_with_auth_mode};

pub(crate) use crate::project_context::ProjectContext as ResolvedContext;

mod api;
mod assign;
mod delete;
mod list;
mod versions;
mod view;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt prompts list
  bt prompts list --environment production
  bt prompts versions my-prompt
  bt prompts view my-prompt --environment production
  bt prompts assign my-prompt --environment production --version 1234
  bt prompts unassign my-prompt --environment production
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
    /// List all versions of a prompt
    Versions(PromptSlugArgs),
    /// Assign a prompt version to an environment
    Assign(AssignArgs),
    /// Unassign a prompt from an environment
    Unassign(UnassignArgs),
    /// Delete a prompt
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
struct PromptSelectorArgs {
    /// Prompt version ID (short or decimal transaction ID)
    #[arg(long)]
    version: Option<String>,

    /// Environment slug (for example, production)
    #[arg(long)]
    environment: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct ListArgs {
    /// Environment slug (for example, production)
    #[arg(long)]
    environment: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct PromptSlugArgs {
    /// Prompt slug (positional)
    #[arg(value_name = "SLUG", conflicts_with = "slug_flag")]
    slug_positional: Option<String>,

    /// Prompt slug (flag)
    #[arg(long = "slug", short = 's')]
    slug_flag: Option<String>,
}

impl PromptSlugArgs {
    fn slug(&self) -> Option<&str> {
        self.slug_positional
            .as_deref()
            .or(self.slug_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
pub struct ViewArgs {
    #[command(flatten)]
    slug: PromptSlugArgs,

    #[command(flatten)]
    selector: PromptSelectorArgs,

    /// Open in browser instead of showing in terminal
    #[arg(long)]
    web: bool,
}

#[derive(Debug, Clone, Args)]
pub struct AssignArgs {
    #[command(flatten)]
    slug: PromptSlugArgs,

    #[command(flatten)]
    selector: PromptSelectorArgs,
}

#[derive(Debug, Clone, Args)]
pub struct UnassignArgs {
    #[command(flatten)]
    slug: PromptSlugArgs,

    /// Environment slug (for example, production)
    #[arg(long)]
    environment: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct DeleteArgs {
    #[command(flatten)]
    slug: PromptSlugArgs,

    /// Skip confirmation prompt (requires slug)
    #[arg(long, short = 'f')]
    force: bool,
}

async fn resolve_prompt(
    ctx: &ResolvedContext,
    slug: Option<&str>,
    version: Option<&str>,
    environment: Option<&str>,
    usage: &str,
) -> Result<api::Prompt> {
    let interactive_selection = slug.is_none();
    let selected = if interactive_selection {
        if !is_interactive() {
            bail!("prompt slug required. Use: {usage}");
        }
        Some(delete::select_prompt_interactive(&ctx.client, &ctx.project.name).await?)
    } else {
        None
    };
    if version.is_none() && environment.is_none() {
        if let Some(prompt) = selected {
            return Ok(prompt);
        }
    }

    let slug = slug.unwrap_or_else(|| &selected.as_ref().unwrap().slug);
    with_spinner(
        "Loading prompt...",
        api::get_prompt_by_slug(&ctx.client, &ctx.project.name, slug, version, environment),
    )
    .await?
    .ok_or_else(|| {
        if interactive_selection {
            let selector = version
                .map(|value| format!("version {}", crate::util_cmd::display_xact_id(value)))
                .or_else(|| environment.map(|value| format!("environment {value}")))
                .unwrap_or_default();
            anyhow!("prompt with slug '{slug}' not found at {selector}")
        } else {
            anyhow!("prompt with slug '{slug}' not found")
        }
    })
}

pub async fn run(base: BaseArgs, args: PromptsArgs) -> Result<()> {
    let read_only = prompts_command_is_read_only(args.command.as_ref());
    let ctx = resolve_project_command_context_with_auth_mode(&base, read_only).await?;

    match args.command {
        None => list::run(&ctx, None, base.json).await,
        Some(PromptsCommands::List(args)) => {
            list::run(&ctx, args.environment.as_deref(), base.json).await
        }
        Some(PromptsCommands::Versions(args)) => versions::run(&ctx, args.slug(), base.json).await,
        Some(PromptsCommands::View(args)) => {
            if args.selector.version.is_some() && args.selector.environment.is_some() {
                bail!("--version and --environment cannot be used together");
            }
            view::run(
                &ctx,
                args.slug.slug(),
                args.selector.version.as_deref(),
                args.selector.environment.as_deref(),
                base.json,
                args.web,
                base.verbose,
            )
            .await
        }
        Some(PromptsCommands::Assign(args)) => {
            let hint =
                "Use: bt prompts assign <slug> --environment <environment> --version <version>";
            let version = args
                .selector
                .version
                .as_deref()
                .ok_or_else(|| anyhow!("--version is required. {hint}"))?;
            let environment = args
                .selector
                .environment
                .as_deref()
                .ok_or_else(|| anyhow!("--environment is required. {hint}"))?;
            assign::run(
                &ctx,
                args.slug.slug(),
                environment,
                assign::Action::Assign { version },
                base.json,
            )
            .await
        }
        Some(PromptsCommands::Unassign(args)) => {
            let hint = "Use: bt prompts unassign <slug> --environment <environment>";
            let environment = args
                .environment
                .as_deref()
                .ok_or_else(|| anyhow!("--environment is required. {hint}"))?;
            assign::run(
                &ctx,
                args.slug.slug(),
                environment,
                assign::Action::Unassign,
                base.json,
            )
            .await
        }
        Some(PromptsCommands::Delete(args)) => {
            delete::run(&ctx, args.slug.slug(), args.force).await
        }
    }
}

fn prompts_command_is_read_only(command: Option<&PromptsCommands>) -> bool {
    matches!(
        command,
        None | Some(PromptsCommands::List(_))
            | Some(PromptsCommands::View(_))
            | Some(PromptsCommands::Versions(_))
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
            version: version.map(ToOwned::to_owned),
            environment: environment.map(ToOwned::to_owned),
        }
    }

    fn slug(slug: &str) -> PromptSlugArgs {
        PromptSlugArgs {
            slug_positional: Some(slug.to_string()),
            slug_flag: None,
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
        assert_eq!(list.environment.as_deref(), Some("production"));

        let error = CliHarness::try_parse_from(["bt-prompts", "list", "--version", "1234"])
            .expect_err("list should reject version");
        assert!(error
            .to_string()
            .contains("unexpected argument '--version'"));

        let versions = CliHarness::try_parse_from(["bt-prompts", "versions", "test-prompt"])
            .expect("parse versions");
        let Some(PromptsCommands::Versions(versions)) = versions.prompts.command else {
            panic!("expected versions command");
        };
        assert_eq!(versions.slug(), Some("test-prompt"));

        let assign = CliHarness::try_parse_from([
            "bt-prompts",
            "assign",
            "test-prompt",
            "--environment",
            "production",
            "--version",
            "1234",
        ])
        .expect("parse assign");
        let Some(PromptsCommands::Assign(assign)) = assign.prompts.command else {
            panic!("expected assign command");
        };
        assert_eq!(assign.selector.version.as_deref(), Some("1234"));
        assert_eq!(assign.selector.environment.as_deref(), Some("production"));
    }

    #[test]
    fn prompts_routes_list_and_view_to_read_only_auth() {
        assert!(prompts_command_is_read_only(None));
        assert!(prompts_command_is_read_only(Some(&PromptsCommands::List(
            ListArgs { environment: None }
        ))));
        assert!(prompts_command_is_read_only(Some(&PromptsCommands::View(
            ViewArgs {
                slug: slug("test-prompt"),
                selector: selectors(None, None),
                web: false,
            }
        ))));
        assert!(prompts_command_is_read_only(Some(
            &PromptsCommands::Versions(slug("test-prompt"))
        )));
    }

    #[test]
    fn prompts_routes_mutations_to_validated_auth() {
        assert!(!prompts_command_is_read_only(Some(
            &PromptsCommands::Assign(AssignArgs {
                slug: slug("test-prompt"),
                selector: selectors(Some("1234"), Some("production")),
            })
        )));
        assert!(!prompts_command_is_read_only(Some(
            &PromptsCommands::Delete(DeleteArgs {
                slug: slug("test-prompt"),
                force: true,
            })
        )));
    }
}
