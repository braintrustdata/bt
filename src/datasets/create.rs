use std::{path::Path, time::Duration};

use anyhow::{bail, Result};
use serde_json::json;

use crate::ui::{print_command_status, with_spinner, with_spinner_visible, CommandStatus};

use super::{api, records::load_optional_upload_records, utils, ResolvedContext};

pub async fn run(
    ctx: &ResolvedContext,
    name: Option<&str>,
    description: Option<&str>,
    input_path: Option<&Path>,
    inline_rows: Option<&str>,
    id_field: &str,
    json_output: bool,
) -> Result<()> {
    let name = super::resolve_dataset_name(name, "create")?;

    let exists = with_spinner(
        "Checking dataset...",
        api::get_dataset_by_name(&ctx.client, &ctx.project.id, &name),
    )
    .await?;
    if exists.is_some() {
        bail!(
            "dataset '{name}' already exists in project '{}'; use `bt datasets update {name}` to add rows",
            ctx.project.name
        );
    }

    let records = load_optional_upload_records(input_path, inline_rows, id_field)?;
    let uploaded = records.as_ref().map_or(0, |records| records.len());

    let dataset = match with_spinner_visible(
        "Creating dataset...",
        api::create_dataset(&ctx.client, &ctx.project.id, &name, description),
        Duration::from_millis(300),
    )
    .await
    {
        Ok(dataset) => dataset,
        Err(error) => {
            print_command_status(CommandStatus::Error, &format!("Failed to create '{name}'"));
            return Err(error);
        }
    };

    if let Some(records) = records.as_ref() {
        if let Err(error) = utils::submit_prepared_records(
            ctx,
            &dataset.id,
            records,
            "Uploading dataset rows...",
            "dataset upload failed",
        )
        .await
        {
            print_command_status(
                CommandStatus::Error,
                &format!("Created '{name}' but failed to upload initial rows"),
            );
            return Err(error);
        }
    }

    if json_output {
        println!(
            "{}",
            serde_json::to_string(&json!({
                "dataset": dataset,
                "created_dataset": true,
                "uploaded": uploaded,
                "mode": "create",
            }))?
        );
        return Ok(());
    }

    let detail = if uploaded == 0 {
        format!("Created '{name}'")
    } else {
        format!("Created '{name}' with {uploaded} records")
    };
    print_command_status(CommandStatus::Success, &detail);
    Ok(())
}
