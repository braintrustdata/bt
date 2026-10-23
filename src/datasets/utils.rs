use std::time::Duration;

use anyhow::{anyhow, Result};
use braintrust_sdk_rust::{api::Config, ingest::BatchUploader};
use serde_json::{Map, Value};

use crate::ui::with_spinner_visible;

use super::{
    records::{PreparedDatasetRecord, DATASET_UPLOAD_BATCH_SIZE},
    ResolvedContext,
};

pub(crate) async fn submit_prepared_records(
    ctx: &ResolvedContext,
    dataset_id: &str,
    records: &[PreparedDatasetRecord],
    is_merge: bool,
    spinner_label: &str,
    error_context: &str,
) -> Result<()> {
    let rows = records
        .iter()
        .map(|record| record.to_upload_row(dataset_id, is_merge))
        .collect::<Vec<_>>();
    submit_rows(ctx, &rows, spinner_label, error_context).await
}

pub(crate) async fn submit_rows(
    ctx: &ResolvedContext,
    rows: &[Map<String, Value>],
    spinner_label: &str,
    error_context: &str,
) -> Result<()> {
    let uploader = dataset_uploader(ctx)?;
    with_spinner_visible(
        spinner_label,
        async {
            uploader
                .upload_rows(rows.to_vec(), DATASET_UPLOAD_BATCH_SIZE)
                .await
                .map_err(|err| anyhow!("{error_context}: {err}"))
        },
        Duration::from_millis(300),
    )
    .await?;
    Ok(())
}

fn dataset_uploader(ctx: &ResolvedContext) -> Result<BatchUploader> {
    let mut config = Config::new(ctx.client.api_key().to_string());
    config.api_url = ctx.client.base_url().to_string();
    config.org_name =
        (!ctx.client.org_name().trim().is_empty()).then_some(ctx.client.org_name().to_string());
    BatchUploader::new(config)
        .map_err(|err| anyhow!("failed to initialize dataset uploader: {err}"))
}
