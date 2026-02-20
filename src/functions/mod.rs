use std::io::IsTerminal;

use anyhow::{anyhow, Result};
use clap::{Args, Subcommand};

use crate::{
    args::BaseArgs,
    http::ApiClient,
    login::login,
    projects::{api::get_project_by_name, switch::select_project_interactive},
};

pub mod api;
mod delete;
mod list;
mod view;

pub struct FunctionKind {
    pub type_name: &'static str,
    pub plural: &'static str,
    pub function_type: &'static str,
    pub url_segment: &'static str,
}

pub const TOOL: FunctionKind = FunctionKind {
    type_name: "tool",
    plural: "tools",
    function_type: "tool",
    // include query params `pr=` prefix since tools use a query param to open in a modal/dialog window
    url_segment: "tools?pr=",
};

pub const SCORER: FunctionKind = FunctionKind {
    type_name: "scorer",
    plural: "scorers",
    function_type: "scorer",
    // includes `/` since scorers use a route to open in a in a full-page
    url_segment: "scorers/",
};

#[derive(Debug, Clone, Args)]
pub struct FunctionArgs {
    #[command(subcommand)]
    pub command: Option<FunctionCommands>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum FunctionCommands {
    /// List all in the current project
    List,
    /// View details
    View(ViewArgs),
    /// Delete by slug
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ViewArgs {
    /// Slug (positional)
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,

    /// Slug (flag)
    #[arg(long = "slug", short = 's')]
    slug_flag: Option<String>,

    /// Open in browser
    #[arg(long)]
    web: bool,

    /// Show all configuration details
    #[arg(long)]
    verbose: bool,
}

impl ViewArgs {
    pub fn slug(&self) -> Option<&str> {
        self.slug_positional
            .as_deref()
            .or(self.slug_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
pub struct DeleteArgs {
    /// Slug (positional)
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,

    /// Slug (flag)
    #[arg(long = "slug", short = 's')]
    slug_flag: Option<String>,

    /// Skip confirmation
    #[arg(long, short = 'f')]
    force: bool,
}

impl DeleteArgs {
    pub fn slug(&self) -> Option<&str> {
        self.slug_positional
            .as_deref()
            .or(self.slug_flag.as_deref())
    }
}

pub async fn run(base: BaseArgs, args: FunctionArgs, kind: &FunctionKind) -> Result<()> {
    let ctx = login(&base).await?;
    let org_name = base.org.unwrap_or_else(|| ctx.login.org_name.clone());
    let client = ApiClient::new(&ctx)?.with_org_name(org_name.clone());
    let project = match base.project {
        Some(p) => p,
        None if std::io::stdin().is_terminal() => select_project_interactive(&client).await?,
        None => anyhow::bail!("--project required (or set BRAINTRUST_DEFAULT_PROJECT)"),
    };

    let resolved_project = get_project_by_name(&client, &project)
        .await?
        .ok_or_else(|| anyhow!("project '{project}' not found"))?;

    match args.command {
        None | Some(FunctionCommands::List) => {
            list::run(&client, &resolved_project, &org_name, base.json, kind).await
        }
        Some(FunctionCommands::View(v)) => {
            view::run(
                &client,
                &ctx.app_url,
                &resolved_project,
                &org_name,
                v.slug(),
                base.json,
                v.web,
                v.verbose,
                kind,
            )
            .await
        }
        Some(FunctionCommands::Delete(d)) => {
            delete::run(&client, &resolved_project, d.slug(), d.force, kind).await
        }
    }
}
