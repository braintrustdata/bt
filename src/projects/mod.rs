use anyhow::Result;
use clap::{Args, Subcommand};

use crate::args::BaseArgs;
use crate::auth::{login_with_policy, LoginPolicy};
use crate::http::ApiClient;

pub(crate) mod api;
mod create;
mod delete;
mod list;
pub(crate) mod resolve;
mod view;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt projects list
  bt projects create my-project
  bt projects view my-project --web
")]
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

    /// Skip confirmation prompt (requires name)
    #[arg(long, short = 'f')]
    force: bool,
}

fn policy_for_command(command: Option<&ProjectsCommands>) -> LoginPolicy {
    match command {
        Some(ProjectsCommands::Create(_)) | Some(ProjectsCommands::Delete(_)) => {
            LoginPolicy::Validated
        }
        None | Some(ProjectsCommands::List) | Some(ProjectsCommands::View(_)) => LoginPolicy::Fast,
    }
}

pub async fn run(base: BaseArgs, args: ProjectsArgs) -> Result<()> {
    let policy = policy_for_command(args.command.as_ref());
    let ctx = login_with_policy(&base, policy, true).await?;
    let client = ApiClient::new(&ctx)?;

    match args.command {
        None | Some(ProjectsCommands::List) => {
            list::run(&client, &ctx.login.org_name, base.json).await
        }
        Some(ProjectsCommands::Create(a)) => create::run(&client, a.name.as_deref()).await,
        Some(ProjectsCommands::View(a)) => {
            view::run(&client, &ctx.app_url, &ctx.login.org_name, a.name()).await
        }
        Some(ProjectsCommands::Delete(a)) => delete::run(&client, a.name.as_deref(), a.force).await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn create_and_delete_use_validated_login() {
        let create_policy =
            policy_for_command(Some(&ProjectsCommands::Create(CreateArgs { name: None })));
        let delete_policy = policy_for_command(Some(&ProjectsCommands::Delete(DeleteArgs {
            name: None,
            force: false,
        })));
        assert_eq!(create_policy, LoginPolicy::Validated);
        assert_eq!(delete_policy, LoginPolicy::Validated);
    }

    #[test]
    fn list_and_view_use_fast_login() {
        let none_policy = policy_for_command(None);
        let list_policy = policy_for_command(Some(&ProjectsCommands::List));
        let view_policy = policy_for_command(Some(&ProjectsCommands::View(ViewArgs {
            name_positional: None,
            name_flag: None,
        })));
        assert_eq!(none_policy, LoginPolicy::Fast);
        assert_eq!(list_policy, LoginPolicy::Fast);
        assert_eq!(view_policy, LoginPolicy::Fast);
    }
}
