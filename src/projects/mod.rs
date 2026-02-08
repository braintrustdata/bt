use anyhow::Result;
use clap::{Args, Subcommand};

use crate::args::BaseArgs;
use crate::http::ApiClient;
use crate::login::login;

pub mod api;
mod create;
mod delete;
mod list;
mod switch;
mod view;

#[derive(Debug, Clone, Args)]
pub struct ProjectsArgs {
    #[command(subcommand)]
    command: Option<ProjectsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum ProjectsCommands {
    /// List all projects
    List,
    /// Create a new project
    Create(CreateArgs),
    /// Open a project in the browser
    View(ViewArgs),
    /// Delete a project
    Delete(DeleteArgs),
    /// Switch to a project
    Switch(SwitchArgs),
}

#[derive(Debug, Clone, Args)]
struct CreateArgs {
    /// Name of the project to create
    name: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct ViewArgs {
    /// Project name (positional)
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Project name (flag)
    #[arg(long = "name", short = 'n')]
    name_flag: Option<String>,
}

impl ViewArgs {
    fn name(&self) -> Option<&str> {
        self.name_positional
            .as_deref()
            .or(self.name_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
struct DeleteArgs {
    /// Name of the project to delete
    name: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct SwitchArgs {
    /// Project name
    #[arg(long = "name", short = 'n')]
    name: Option<String>,
}

pub async fn run(base: BaseArgs, args: ProjectsArgs) -> Result<()> {
    let ctx = login(&base).await?;
    let client = ApiClient::new(&ctx)?;

    match args.command {
        None | Some(ProjectsCommands::List) => {
            list::run(&client, &ctx.login.org_name, base.json).await
        }
        Some(ProjectsCommands::Create(a)) => create::run(&client, a.name.as_deref()).await,
        Some(ProjectsCommands::View(a)) => {
            view::run(&client, &ctx.app_url, &ctx.login.org_name, a.name()).await
        }
        Some(ProjectsCommands::Delete(a)) => delete::run(&client, a.name.as_deref()).await,
        Some(ProjectsCommands::Switch(a)) => switch::run(&client, a.name.as_deref()).await,
    }
}
