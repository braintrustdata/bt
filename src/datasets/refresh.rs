use std::path::Path;
use std::time::Duration;

use anyhow::{bail, Result};
use serde_json::json;

use crate::ui::{print_command_status, with_spinner, with_spinner_visible, CommandStatus};

use super::{
    api,
    records::{load_refresh_records, remote_records_by_id},
    upload, ResolvedContext,
};

pub async fn run(
    ctx: &ResolvedContext,
    name: Option<&str>,
    input_path: Option<&Path>,
    inline_rows: Option<&str>,
    id_field: &str,
    json_output: bool,
) -> Result<()> {
    let dataset_name = upload::resolve_dataset_name(name, "refresh")?;
    let local_records = load_refresh_records(input_path, inline_rows, id_field)?;

    let existing_dataset = with_spinner_visible(
        "Resolving remote dataset...",
        api::get_dataset_by_name(&ctx.client, &ctx.project.id, &dataset_name),
        Duration::from_millis(300),
    )
    .await?;
    let (dataset, created_dataset) = match existing_dataset {
        Some(dataset) => (dataset, false),
        None => bail!(
            "dataset '{}' not found in project '{}'",
            dataset_name,
            ctx.project.name
        ),
    };

    let remote_records = if created_dataset {
        Default::default()
    } else {
        let remote_rows = with_spinner(
            "Loading remote dataset rows...",
            api::list_dataset_rows(&ctx.client, &dataset.id),
        )
        .await?;
        remote_records_by_id(remote_rows)?
    };

    let mut upload_rows = Vec::new();
    let mut created = 0usize;
    let mut updated = 0usize;
    let mut unchanged = 0usize;

    for record in &local_records {
        match remote_records.get(&record.id) {
            None => {
                created += 1;
                upload_rows.push(record.to_upload_row(&dataset.id));
            }
            Some(existing) if existing == record => {
                unchanged += 1;
            }
            Some(_) => {
                updated += 1;
                upload_rows.push(record.to_upload_row(&dataset.id));
            }
        }
    }

    if !upload_rows.is_empty() {
        upload::submit_rows(
            ctx,
            &upload_rows,
            "Refreshing remote dataset...",
            "dataset refresh failed",
        )
        .await?;
    }

    if json_output {
        println!(
            "{}",
            serde_json::to_string(&json!({
                "dataset": dataset,
                "created_dataset": created_dataset,
                "created": created,
                "updated": updated,
                "unchanged": unchanged,
                "mode": "update",
            }))?
        );
        return Ok(());
    }

    let detail = if upload_rows.is_empty() {
        format!("'{}' is already up to date.", dataset.name)
    } else {
        format!(
            "Updated '{}' (created {}, updated {}, unchanged {}).",
            dataset.name, created, updated, unchanged
        )
    };
    print_command_status(CommandStatus::Success, &detail);
    Ok(())
}
