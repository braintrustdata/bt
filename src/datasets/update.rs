use std::path::Path;

use anyhow::{bail, Result};
use serde_json::json;

use crate::ui::{print_command_status, with_spinner, CommandStatus};

use super::{api, records::load_refresh_records, utils, ResolvedContext};

pub async fn run(
    ctx: &ResolvedContext,
    name: Option<&str>,
    input_path: Option<&Path>,
    inline_rows: Option<&str>,
    id_field: &str,
    json_output: bool,
) -> Result<()> {
    let dataset_name = super::resolve_dataset_name(name, "refresh")?;
    let local_records = load_refresh_records(input_path, inline_rows, id_field)?;

    let existing_dataset = with_spinner(
        "Resolving remote dataset...",
        api::get_dataset_by_name(&ctx.client, &ctx.project.id, &dataset_name),
    )
    .await?;
    let dataset = match existing_dataset {
        Some(dataset) => dataset,
        None => bail!(
            "dataset '{}' not found in project '{}'",
            dataset_name,
            ctx.project.name
        ),
    };

    if let Err(error) = utils::submit_prepared_records(
        ctx,
        &dataset.id,
        &local_records,
        "Updating remote dataset...",
        "dataset update failed",
    )
    .await
    {
        print_command_status(
            CommandStatus::Error,
            &format!("Failed to update '{}'", dataset.name),
        );
        return Err(error);
    }

    if json_output {
        println!(
            "{}",
            serde_json::to_string(&json!({
                "dataset": dataset,
                "mode": "update",
            }))?
        );
        return Ok(());
    }

    print_command_status(
        CommandStatus::Success,
        &format!("Updated '{}'", dataset.name),
    );
    Ok(())
}
