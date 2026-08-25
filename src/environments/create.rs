use anyhow::{bail, Result};
use dialoguer::Input;

use crate::{
    http::ApiClient,
    ui::{is_interactive, print_command_status, with_spinner, CommandStatus},
};

use super::api::{self, CreateEnvironment};

pub async fn run(
    client: &ApiClient,
    name: Option<&str>,
    slug: Option<&str>,
    description: Option<&str>,
    json: bool,
) -> Result<()> {
    let (name, slug) = match (name, slug) {
        (Some(name), Some(slug)) if !name.is_empty() && !slug.is_empty() => {
            (name.to_string(), slug.to_string())
        }
        (name, slug) if is_interactive() => {
            let name = match name {
                Some(name) if !name.is_empty() => name.to_string(),
                _ => Input::new()
                    .with_prompt("Environment name")
                    .interact_text()?,
            };
            let slug = match slug {
                Some(slug) if !slug.is_empty() => slug.to_string(),
                _ => Input::new()
                    .with_prompt("Environment slug")
                    .interact_text()?,
            };
            (name, slug)
        }
        _ => bail!(
            "environment name and --slug required. Use: bt environments create <name> --slug <slug>"
        ),
    };

    let input = CreateEnvironment {
        name: &name,
        slug: &slug,
        description,
        org_name: client.org_name(),
    };
    let environment = with_spinner(
        "Creating environment...",
        api::create_environment(client, &input),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&environment)?);
    } else {
        print_command_status(
            CommandStatus::Success,
            &format!(
                "Created environment '{}' ({})",
                environment.name, environment.slug
            ),
        );
    }
    Ok(())
}
