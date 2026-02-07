use anyhow::Result;
use clap::{Args, Subcommand};

use crate::{args::BaseArgs, http::ApiClient, login::login};

mod api;
mod delete;
mod list;
mod view;

#[derive(Debug, Clone, Args)]
pub struct PromptsArgs {
    #[command(subcommand)]
    command: Option<PromptsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum PromptsCommands {
    List,
    View(ViewArgs),
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
pub struct ViewArgs {
    /// Prompt name (positional)
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Prompt name (flag)
    #[arg(long = "name", short = 'n')]
    name_flag: Option<String>,
}

// impl ViewArgs {
//     fn name(&self) -> Option<&str> {
//         self.name_positional
//             .as_deref()
//             .or(self.name_flag.as_deref())
//     }
// }

#[derive(Debug, Clone, Args)]
pub struct DeleteArgs {
    /// Name of the project to delete
    name: Option<String>,
}

pub async fn run(base: BaseArgs, args: PromptsArgs) -> Result<()> {
    let ctx = login(&base).await?;
    let client = ApiClient::new(&ctx)?;
    let project = &base
        .project
        .as_deref()
        .ok_or_else(|| anyhow::anyhow!("--project required (or set BRAINTRUST_DEFAULT_PROJECT"))?;

    match args.command {
        None | Some(PromptsCommands::List) => {
            list::run(&client, project, &ctx.login.org_name, base.json).await
        }
        Some(PromptsCommands::View(_p)) => view::run().await,
        Some(PromptsCommands::Delete(_p)) => delete::run().await,
    }
}
