use anyhow::Result;

use crate::ui::{print_command_status, CommandStatus};

use super::{api, ResolvedContext};

pub async fn run(ctx: &ResolvedContext) -> Result<()> {
    let url = api::topics_url(&ctx.app_url, ctx.client.org_name(), &ctx.project.name);
    open::that(&url)?;
    print_command_status(CommandStatus::Success, &format!("Opened {url} in browser"));
    Ok(())
}
