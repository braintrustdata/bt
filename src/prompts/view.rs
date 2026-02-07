use std::io::IsTerminal;

use anyhow::{anyhow, bail, Result};
use urlencoding::encode;

use crate::http::ApiClient;
use crate::prompts::delete::select_prompt_interactive;
use crate::ui::{print_command_status, CommandStatus};

use super::api;

pub async fn run(
    client: &ApiClient,
    app_url: &str,
    project: &str,
    org_name: &str,
    name: Option<&str>,
) -> Result<()> {
    let prompt = match name {
        Some(n) => {
            let prompts = api::list_prompts(client, project).await?;

            prompts
                .into_iter()
                .find(|p| p.name == n)
                .ok_or_else(|| anyhow!("prompt '{n}' not found"))?
        }
        None => {
            if !std::io::stdin().is_terminal() {
                bail!("prompt name required. Use: bt prompts view <name>")
            }
            select_prompt_interactive(client, project).await?
        }
    };

    let url = format!(
        "{}/app/{}/p/{}/prompts/{}",
        app_url.trim_end_matches('/'),
        encode(org_name),
        encode(project),
        encode(&prompt.id)
    );

    open::that(&url)?;
    print_command_status(CommandStatus::Success, &format!("Opened {url} in browser"));

    Ok(())
}
