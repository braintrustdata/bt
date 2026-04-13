use std::time::Duration;

use anyhow::{anyhow, bail, Result};
use braintrust_sdk_rust::Logs3BatchUploader;
use dialoguer::Input;
use serde_json::{Map, Value};

use crate::ui::{is_interactive, with_spinner_visible};

use super::{
    records::{PreparedDatasetRecord, DATASET_UPLOAD_BATCH_SIZE},
    ResolvedContext,
};

pub(crate) fn resolve_dataset_name(name: Option<&str>, command: &str) -> Result<String> {
    match name {
        Some(name) if !name.trim().is_empty() => Ok(name.trim().to_string()),
        _ => {
            if !is_interactive() {
                bail!("dataset name required. Use: bt datasets {command} <name>");
            }
            Ok(Input::new().with_prompt("Dataset name").interact_text()?)
        }
    }
}

pub(crate) async fn submit_prepared_records(
    ctx: &ResolvedContext,
    dataset_id: &str,
    records: &[PreparedDatasetRecord],
    spinner_label: &str,
    error_context: &str,
) -> Result<()> {
    let rows = records
        .iter()
        .map(|record| record.to_upload_row(dataset_id))
        .collect::<Vec<_>>();
    submit_rows(ctx, &rows, spinner_label, error_context).await
}

pub(crate) async fn submit_rows(
    ctx: &ResolvedContext,
    rows: &[Map<String, Value>],
    spinner_label: &str,
    error_context: &str,
) -> Result<()> {
    let mut uploader = dataset_uploader(ctx)?;
    with_spinner_visible(
        spinner_label,
        async {
            uploader
                .upload_rows(rows, DATASET_UPLOAD_BATCH_SIZE)
                .await
                .map_err(|err| anyhow!("{error_context}: {err}"))
        },
        Duration::from_millis(300),
    )
    .await?;
    Ok(())
}

fn dataset_uploader(ctx: &ResolvedContext) -> Result<Logs3BatchUploader> {
    Logs3BatchUploader::new(
        ctx.client.base_url(),
        ctx.client.api_key().to_string(),
        (!ctx.client.org_name().trim().is_empty()).then_some(ctx.client.org_name().to_string()),
    )
    .map_err(|err| anyhow!("failed to initialize dataset uploader: {err}"))
}
