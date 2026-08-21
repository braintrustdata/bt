use anyhow::{anyhow, bail, Result};

use crate::ui::{print_command_status, with_spinner, CommandStatus};

use super::{api, ResolvedContext};

pub async fn run(
    ctx: &ResolvedContext,
    slug: Option<&str>,
    environment: &str,
    version: &str,
    json: bool,
) -> Result<()> {
    let Some(slug) = slug else {
        bail!("prompt slug required. Use: bt prompts promote <slug> --environment <environment> --version <version>");
    };

    let prompt = with_spinner(
        "Loading prompt version...",
        api::get_prompt_by_slug(&ctx.client, &ctx.project.name, slug, Some(version), None),
    )
    .await?
    .ok_or_else(|| anyhow!("prompt with slug '{slug}' not found at version {version}"))?;

    let object_version = prompt._xact_id.as_deref().ok_or_else(|| {
        anyhow!("prompt version response did not include a transaction version; cannot promote")
    })?;

    let association = with_spinner(
        "Promoting prompt...",
        api::promote_prompt(&ctx.client, &prompt.id, environment, object_version),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&association)?);
    } else {
        print_command_status(
            CommandStatus::Success,
            &format!("Promoted prompt '{slug}' version {version} to environment '{environment}'"),
        );
    }

    Ok(())
}
