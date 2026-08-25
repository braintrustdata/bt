use anyhow::{anyhow, bail, Result};
use clap::{Args, Subcommand};

use crate::{
    args::BaseArgs,
    auth::{login, login_read_only},
    http::ApiClient,
    ui::{fuzzy_select, is_interactive, with_spinner},
};

mod api;
mod create;
mod delete;
mod list;
mod update;
mod view;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt environments list
  bt environments view production
  bt environments create Production --slug production
  bt environments update production --description \"Production deployment\"
  bt environments delete production --force
")]
pub struct EnvironmentsArgs {
    #[command(subcommand)]
    command: Option<EnvironmentsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum EnvironmentsCommands {
    /// List all environments
    List,
    /// View an environment
    View(ViewArgs),
    /// Create an environment
    Create(CreateArgs),
    /// Update an environment
    Update(UpdateArgs),
    /// Delete an environment
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
struct ViewArgs {
    /// Environment slug
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,

    /// Environment slug
    #[arg(long = "slug", env = "BT_ENVIRONMENTS_SLUG")]
    slug_flag: Option<String>,
}

impl ViewArgs {
    fn slug(&self) -> Option<&str> {
        self.slug_positional
            .as_deref()
            .or(self.slug_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
struct CreateArgs {
    /// Display name for the environment
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Display name for the environment
    #[arg(long = "name", env = "BT_ENVIRONMENTS_NAME")]
    name_flag: Option<String>,

    /// URL-friendly slug, unique within the organization
    #[arg(long, env = "BT_ENVIRONMENTS_SLUG")]
    slug: Option<String>,

    /// Environment description
    #[arg(long, env = "BT_ENVIRONMENTS_DESCRIPTION")]
    description: Option<String>,
}

impl CreateArgs {
    fn name(&self) -> Option<&str> {
        self.name_positional
            .as_deref()
            .or(self.name_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
struct UpdateArgs {
    /// Current environment slug
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,

    /// Current environment slug
    #[arg(long = "slug", env = "BT_ENVIRONMENTS_SLUG")]
    slug_flag: Option<String>,

    /// New display name
    #[arg(long, env = "BT_ENVIRONMENTS_NAME")]
    name: Option<String>,

    /// New environment slug
    #[arg(long, env = "BT_ENVIRONMENTS_NEW_SLUG")]
    new_slug: Option<String>,

    /// New environment description
    #[arg(
        long,
        env = "BT_ENVIRONMENTS_DESCRIPTION",
        conflicts_with = "clear_description"
    )]
    description: Option<String>,

    /// Remove the environment description
    #[arg(
        long,
        env = "BT_ENVIRONMENTS_CLEAR_DESCRIPTION",
        value_parser = clap::builder::BoolishValueParser::new(),
        default_value_t = false,
        conflicts_with = "description"
    )]
    clear_description: bool,
}

impl UpdateArgs {
    fn slug(&self) -> Option<&str> {
        self.slug_positional
            .as_deref()
            .or(self.slug_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
struct DeleteArgs {
    /// Environment slug
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,

    /// Environment slug
    #[arg(long = "slug", env = "BT_ENVIRONMENTS_SLUG")]
    slug_flag: Option<String>,

    /// Skip the confirmation prompt
    #[arg(
        long,
        short = 'f',
        env = "BT_ENVIRONMENTS_FORCE",
        value_parser = clap::builder::BoolishValueParser::new(),
        default_value_t = false
    )]
    force: bool,
}

impl DeleteArgs {
    fn slug(&self) -> Option<&str> {
        self.slug_positional
            .as_deref()
            .or(self.slug_flag.as_deref())
    }
}

pub async fn run(base: BaseArgs, args: EnvironmentsArgs) -> Result<()> {
    let read_only = environments_command_is_read_only(args.command.as_ref());
    let auth = if read_only {
        login_read_only(&base).await?
    } else {
        login(&base).await?
    };
    let client = ApiClient::new(&auth)?;

    match args.command {
        None | Some(EnvironmentsCommands::List) => list::run(&client, base.json).await,
        Some(EnvironmentsCommands::View(args)) => view::run(&client, args.slug(), base.json).await,
        Some(EnvironmentsCommands::Create(args)) => {
            create::run(
                &client,
                args.name(),
                args.slug.as_deref(),
                args.description.as_deref(),
                base.json,
            )
            .await
        }
        Some(EnvironmentsCommands::Update(args)) => {
            let options = update::UpdateOptions {
                name: args.name.as_deref(),
                new_slug: args.new_slug.as_deref(),
                description: args.description.as_deref(),
                clear_description: args.clear_description,
                json: base.json,
            };
            update::run(&client, args.slug(), options).await
        }
        Some(EnvironmentsCommands::Delete(args)) => {
            delete::run(&client, args.slug(), args.force, base.json).await
        }
    }
}

fn environments_command_is_read_only(command: Option<&EnvironmentsCommands>) -> bool {
    matches!(
        command,
        None | Some(EnvironmentsCommands::List) | Some(EnvironmentsCommands::View(_))
    )
}

async fn resolve_environment(
    client: &ApiClient,
    slug: Option<&str>,
    command: &str,
) -> Result<api::Environment> {
    if let Some(slug) = slug {
        return with_spinner(
            "Loading environment...",
            api::get_environment_by_slug(client, slug),
        )
        .await?
        .ok_or_else(|| anyhow!("environment '{slug}' not found"));
    }

    if !is_interactive() {
        bail!("environment slug required. Use: bt environments {command} <slug>");
    }

    let environments =
        with_spinner("Loading environments...", api::list_environments(client)).await?;
    if environments.is_empty() {
        bail!(
            "no environments found. Create one with: bt environments create <name> --slug <slug>"
        );
    }
    let labels = environments
        .iter()
        .map(|environment| format!("{} ({})", environment.name, environment.slug))
        .collect::<Vec<_>>();
    let selection = fuzzy_select("Select environment", &labels, 0)?;
    Ok(environments[selection].clone())
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[derive(Debug, Parser)]
    struct CliHarness {
        #[command(flatten)]
        environments: EnvironmentsArgs,
    }

    #[test]
    fn parses_environment_crud_commands() {
        let create = CliHarness::try_parse_from([
            "bt-environments",
            "create",
            "Production",
            "--slug",
            "production",
            "--description",
            "Production deployment",
        ])
        .expect("parse create");
        let Some(EnvironmentsCommands::Create(create)) = create.environments.command else {
            panic!("expected create command");
        };
        assert_eq!(create.name(), Some("Production"));
        assert_eq!(create.slug.as_deref(), Some("production"));

        let update = CliHarness::try_parse_from([
            "bt-environments",
            "update",
            "production",
            "--new-slug",
            "prod",
            "--clear-description",
        ])
        .expect("parse update");
        let Some(EnvironmentsCommands::Update(update)) = update.environments.command else {
            panic!("expected update command");
        };
        assert_eq!(update.slug(), Some("production"));
        assert_eq!(update.new_slug.as_deref(), Some("prod"));
        assert!(update.clear_description);
    }

    #[test]
    fn rejects_conflicting_description_updates() {
        let error = CliHarness::try_parse_from([
            "bt-environments",
            "update",
            "production",
            "--description",
            "Production",
            "--clear-description",
        ])
        .expect_err("description flags should conflict");
        assert!(error.to_string().contains("cannot be used with"));
    }

    #[test]
    fn routes_only_list_and_view_to_read_only_auth() {
        assert!(environments_command_is_read_only(None));
        assert!(environments_command_is_read_only(Some(
            &EnvironmentsCommands::List
        )));
        assert!(environments_command_is_read_only(Some(
            &EnvironmentsCommands::View(ViewArgs {
                slug_positional: Some("production".to_string()),
                slug_flag: None,
            })
        )));
        assert!(!environments_command_is_read_only(Some(
            &EnvironmentsCommands::Delete(DeleteArgs {
                slug_positional: Some("production".to_string()),
                slug_flag: None,
                force: true,
            })
        )));
    }
}
