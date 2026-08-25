use std::fmt::Write as _;

use anyhow::{anyhow, bail, Result};
use clap::{Args, Subcommand};
use dialoguer::{console, Confirm, Input};
use serde::Serialize;

use crate::{
    args::BaseArgs,
    auth::{login, login_read_only},
    http::ApiClient,
    ui::{
        apply_column_padding, fuzzy_select, header, is_interactive, is_quiet, print_command_status,
        print_with_pager, styled_table, truncate, with_spinner, CommandStatus,
    },
};

mod api;

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
    command: Option<Command>,
}

#[derive(Debug, Clone, Subcommand)]
enum Command {
    /// List all environments
    List,
    /// View an environment
    View(Selector),
    /// Create an environment
    Create(CreateArgs),
    /// Update an environment
    Update(UpdateArgs),
    /// Delete an environment
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
struct Selector {
    /// Environment slug
    #[arg(value_name = "SLUG")]
    positional: Option<String>,
    /// Environment slug
    #[arg(long = "slug")]
    flag: Option<String>,
}

impl Selector {
    fn value(&self) -> Option<&str> {
        self.positional.as_deref().or(self.flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
struct CreateArgs {
    /// Display name for the environment
    #[arg(value_name = "NAME")]
    name: Option<String>,
    /// URL-friendly slug, unique within the organization
    #[arg(long)]
    slug: Option<String>,
    /// Environment description
    #[arg(long)]
    description: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct UpdateArgs {
    #[command(flatten)]
    selector: Selector,
    /// New display name
    #[arg(long)]
    name: Option<String>,
    /// New environment slug
    #[arg(long)]
    new_slug: Option<String>,
    /// New environment description
    #[arg(long, conflicts_with = "clear_description")]
    description: Option<String>,
    /// Remove the environment description
    #[arg(long, default_value_t = false, conflicts_with = "description")]
    clear_description: bool,
}

#[derive(Debug, Clone, Args)]
struct DeleteArgs {
    #[command(flatten)]
    selector: Selector,
    /// Skip the confirmation prompt
    #[arg(long, short = 'f', default_value_t = false)]
    force: bool,
}

pub async fn run(base: BaseArgs, args: EnvironmentsArgs) -> Result<()> {
    let read_only = matches!(args.command, None | Some(Command::List | Command::View(_)));
    let auth = if read_only {
        login_read_only(&base).await?
    } else {
        login(&base).await?
    };
    let client = ApiClient::new(&auth)?;

    match args.command {
        None | Some(Command::List) => list(&client, base.json).await,
        Some(Command::View(args)) => view(&client, args.value(), base.json).await,
        Some(Command::Create(args)) => create(&client, args, base.json).await,
        Some(Command::Update(args)) => update(&client, args, base.json).await,
        Some(Command::Delete(args)) => delete(&client, args, base.json).await,
    }
}

async fn list(client: &ApiClient, json: bool) -> Result<()> {
    let environments = with_spinner("Loading environments...", api::list(client)).await?;
    if json {
        return print_json(&environments);
    }

    let mut output = String::new();
    writeln!(
        output,
        "{} environments found in {}\n",
        console::style(environments.len()),
        console::style(client.org_name()).bold()
    )?;
    let mut table = styled_table();
    table.set_header([
        header("Name"),
        header("Slug"),
        header("Description"),
        header("Created"),
    ]);
    apply_column_padding(&mut table, (0, 6));
    for environment in environments {
        let description = display(environment.description.as_deref(), 60);
        let created = display(environment.created.as_deref(), 10);
        table.add_row([environment.name, environment.slug, description, created]);
    }
    write!(output, "{table}")?;
    print_with_pager(&output)?;
    Ok(())
}

async fn view(client: &ApiClient, slug: Option<&str>, json: bool) -> Result<()> {
    let environment = resolve(client, slug, "view").await?;
    if json {
        return print_json(&environment);
    }

    let mut output = String::new();
    writeln!(output, "{}", console::style(&environment.name).bold())?;
    for (label, value) in [
        ("Slug:", Some(environment.slug.as_str())),
        ("Description:", environment.description.as_deref()),
        ("Created:", environment.created.as_deref()),
        ("ID:", Some(environment.id.as_str())),
    ] {
        writeln!(
            output,
            "{} {}",
            console::style(label).dim(),
            value.unwrap_or("-")
        )?;
    }
    print_with_pager(&output)?;
    Ok(())
}

async fn create(client: &ApiClient, args: CreateArgs, json: bool) -> Result<()> {
    let name = required(
        args.name.as_deref(),
        "Environment name",
        "environment name required",
    )?;
    let slug = required(
        args.slug.as_deref(),
        "Environment slug",
        "--slug required. Use: bt environments create <name> --slug <slug>",
    )?;
    let body = api::CreateEnvironment {
        name: &name,
        slug: &slug,
        description: args.description.as_deref(),
        org_name: client.org_name(),
    };
    let environment = with_spinner("Creating environment...", api::create(client, &body)).await?;
    output_result(&environment, json, "Created")
}

async fn update(client: &ApiClient, args: UpdateArgs, json: bool) -> Result<()> {
    if args.name.is_none()
        && args.new_slug.is_none()
        && args.description.is_none()
        && !args.clear_description
    {
        bail!("at least one update is required. Use --name, --new-slug, --description, or --clear-description");
    }
    let environment = resolve(client, args.selector.value(), "update").await?;
    let body = api::UpdateEnvironment {
        name: args.name.as_deref(),
        slug: args.new_slug.as_deref(),
        description: if args.clear_description {
            Some(None)
        } else {
            args.description.as_deref().map(Some)
        },
    };
    let environment = with_spinner(
        "Updating environment...",
        api::update(client, &environment.id, &body),
    )
    .await?;
    output_result(&environment, json, "Updated")
}

async fn delete(client: &ApiClient, args: DeleteArgs, json: bool) -> Result<()> {
    if args.force && args.selector.value().is_none() {
        bail!("environment slug required when using --force. Use: bt environments delete <slug> --force");
    }
    let environment = resolve(client, args.selector.value(), "delete").await?;
    if !args.force {
        if !is_interactive() {
            bail!("environment delete requires --force in non-interactive mode. Use: bt environments delete <slug> --force");
        }
        if !Confirm::new()
            .with_prompt(format!(
                "Delete environment '{}' ({})?",
                environment.name, environment.slug
            ))
            .default(false)
            .interact()?
        {
            return Ok(());
        }
    }
    with_spinner(
        "Deleting environment...",
        api::delete(client, &environment.id),
    )
    .await?;
    output_result(&environment, json, "Deleted")?;
    if !json && !is_quiet() {
        eprintln!("Run `bt environments list` to see remaining environments.");
    }
    Ok(())
}

async fn resolve(
    client: &ApiClient,
    slug: Option<&str>,
    command: &str,
) -> Result<api::Environment> {
    if let Some(slug) = slug {
        return with_spinner("Loading environment...", api::get_by_slug(client, slug))
            .await?
            .ok_or_else(|| anyhow!("environment '{slug}' not found"));
    }
    if !is_interactive() {
        bail!("environment slug required. Use: bt environments {command} <slug>");
    }
    let environments = with_spinner("Loading environments...", api::list(client)).await?;
    if environments.is_empty() {
        bail!(
            "no environments found. Create one with: bt environments create <name> --slug <slug>"
        );
    }
    let labels: Vec<_> = environments
        .iter()
        .map(|e| format!("{} ({})", e.name, e.slug))
        .collect();
    Ok(environments[fuzzy_select("Select environment", &labels, 0)?].clone())
}

fn required(value: Option<&str>, prompt: &str, error: &str) -> Result<String> {
    match value.filter(|value| !value.is_empty()) {
        Some(value) => Ok(value.to_string()),
        None if is_interactive() => Ok(Input::new().with_prompt(prompt).interact_text()?),
        None => bail!("{error}"),
    }
}

fn display(value: Option<&str>, limit: usize) -> String {
    value
        .filter(|value| !value.is_empty())
        .map(|value| truncate(value, limit))
        .unwrap_or_else(|| "-".into())
}

fn print_json(value: &impl Serialize) -> Result<()> {
    println!("{}", serde_json::to_string(value)?);
    Ok(())
}

fn output_result(environment: &api::Environment, json: bool, verb: &str) -> Result<()> {
    if json {
        print_json(environment)
    } else {
        print_command_status(
            CommandStatus::Success,
            &format!(
                "{verb} environment '{}' ({})",
                environment.name, environment.slug
            ),
        );
        Ok(())
    }
}
