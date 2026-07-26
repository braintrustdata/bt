use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Serialize;

use crate::{
    args::BaseArgs,
    auth::login_read_only,
    http::{self, ApiClient},
    ui::{print_command_status, with_spinner, CommandStatus},
    utils::write_text_atomic,
};

use super::{api, ReportArgs};

#[derive(Debug, Serialize)]
struct TopicMapReportDownload {
    function_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    output: String,
    bytes: usize,
}

pub async fn run(base: &BaseArgs, args: &ReportArgs, json: bool) -> Result<()> {
    let auth = login_read_only(base).await?;
    let client = ApiClient::new(&auth)?;
    let function_id = args.function_id()?;

    let report_url = with_spinner(
        "Requesting topic map report URL...",
        api::fetch_topic_map_report_url(&client, function_id, args.version.as_deref()),
    )
    .await?;

    let report = with_spinner(
        "Downloading topic map report...",
        download_topic_map_report(&report_url.url),
    )
    .await?;

    let Some(output) = args.output.as_deref() else {
        print!("{report}");
        return Ok(());
    };

    let output = resolve_output_path(output)?;
    write_text_atomic(&output, &report)
        .with_context(|| format!("failed to write report to {}", output.display()))?;

    let downloaded = TopicMapReportDownload {
        function_id: function_id.to_string(),
        version: args.version.clone(),
        output: output.display().to_string(),
        bytes: report.len(),
    };

    if json {
        println!("{}", serde_json::to_string(&downloaded)?);
        return Ok(());
    }

    print_command_status(
        CommandStatus::Success,
        &format!(
            "Downloaded topic map report to {} ({} bytes)",
            downloaded.output, downloaded.bytes
        ),
    );
    Ok(())
}

async fn download_topic_map_report(url: &str) -> Result<String> {
    let client = http::build_http_client(http::DEFAULT_HTTP_TIMEOUT)
        .context("failed to build report download HTTP client")?;
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to download topic map report from {url}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("topic map report download failed ({status}): {body}");
    }
    response
        .text()
        .await
        .context("failed to read topic map report body")
}

fn resolve_output_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()
        .context("failed to resolve current directory")?
        .join(path))
}
