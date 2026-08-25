use anyhow::{bail, Result};

use crate::{
    http::ApiClient,
    ui::{print_command_status, with_spinner, CommandStatus},
};

use super::{
    api::{self, UpdateEnvironment},
    resolve_environment,
};

pub struct UpdateOptions<'a> {
    pub name: Option<&'a str>,
    pub new_slug: Option<&'a str>,
    pub description: Option<&'a str>,
    pub clear_description: bool,
    pub json: bool,
}

pub async fn run(client: &ApiClient, slug: Option<&str>, options: UpdateOptions<'_>) -> Result<()> {
    if options.name.is_none()
        && options.new_slug.is_none()
        && options.description.is_none()
        && !options.clear_description
    {
        bail!("at least one update is required. Use --name, --new-slug, --description, or --clear-description");
    }

    let environment = resolve_environment(client, slug, "update").await?;
    let description = if options.clear_description {
        Some(None)
    } else {
        options.description.map(Some)
    };
    let input = UpdateEnvironment {
        name: options.name,
        slug: options.new_slug,
        description,
    };
    let updated = with_spinner(
        "Updating environment...",
        api::update_environment(client, &environment.id, &input),
    )
    .await?;

    if options.json {
        println!("{}", serde_json::to_string(&updated)?);
    } else {
        print_command_status(
            CommandStatus::Success,
            &format!("Updated environment '{}' ({})", updated.name, updated.slug),
        );
    }
    Ok(())
}
