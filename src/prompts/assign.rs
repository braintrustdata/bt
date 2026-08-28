use anyhow::{anyhow, bail, Result};

use crate::ui::{print_command_status, with_spinner, CommandStatus};

use super::{api, ResolvedContext};

#[derive(Clone, Copy)]
pub enum Action<'a> {
    Assign { version: &'a str },
    Unassign,
}

pub async fn run(
    ctx: &ResolvedContext,
    slug: Option<&str>,
    environment: &str,
    action: Action<'_>,
    json: bool,
) -> Result<()> {
    let Some(slug) = slug else {
        match action {
            Action::Assign { .. } => bail!("prompt slug required. Use: bt prompts assign <slug> --environment <environment> --version <version>"),
            Action::Unassign => bail!("prompt slug required. Use: bt prompts unassign <slug> --environment <environment>"),
        }
    };

    let version = match action {
        Action::Assign { version } => Some(version),
        Action::Unassign => None,
    };
    let loading_message = if version.is_some() {
        "Loading prompt version..."
    } else {
        "Loading prompt..."
    };
    let prompt = with_spinner(
        loading_message,
        api::get_prompt_by_slug(&ctx.client, &ctx.project.name, slug, version, None),
    )
    .await?
    .ok_or_else(|| match version {
        Some(version) => anyhow!(
            "prompt with slug '{slug}' not found at version {}",
            crate::util_cmd::display_xact_id(version)
        ),
        None => anyhow!("prompt with slug '{slug}' not found"),
    })?;

    let association = match action {
        Action::Assign { .. } => {
            let object_version = prompt._xact_id.as_deref().ok_or_else(|| {
                anyhow!(
                    "prompt version response did not include a transaction version; cannot assign"
                )
            })?;
            with_spinner(
                "Assigning prompt...",
                api::assign_prompt(&ctx.client, &prompt.id, environment, object_version),
            )
            .await?
        }
        Action::Unassign => {
            with_spinner(
                "Unassigning prompt...",
                api::unassign_prompt(&ctx.client, &prompt.id, environment),
            )
            .await?
        }
    };

    if json {
        println!("{}", serde_json::to_string(&association)?);
    } else {
        let message = match action {
            Action::Assign { version } => {
                format!(
                    "Assigned prompt '{slug}' version {} to environment '{environment}'",
                    crate::util_cmd::display_xact_id(version)
                )
            }
            Action::Unassign => {
                format!("Unassigned prompt '{slug}' from environment '{environment}'")
            }
        };
        print_command_status(CommandStatus::Success, &message);
    }

    Ok(())
}
