use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use serde::Serialize;

use crate::{
    args::BaseArgs,
    auth::login_read_only,
    http::{self, ApiClient},
    ui::{print_command_status, with_spinner, CommandStatus},
    utils::write_bytes_atomic,
};

use super::{api, BtmapArgs};

#[derive(Debug, Serialize)]
struct TopicMapBtmapDownload {
    function_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<String>,
    output: String,
    bytes: usize,
}

pub async fn run(base: &BaseArgs, args: &BtmapArgs, json: bool) -> Result<()> {
    let auth = login_read_only(base).await?;
    let client = ApiClient::new(&auth)?;
    let function_id = args.function_id()?;

    let btmap_url = with_spinner(
        "Requesting topic map URL...",
        api::fetch_topic_map_btmap_url(&client, function_id, args.version.as_deref()),
    )
    .await?;

    let btmap = with_spinner(
        "Downloading topic map...",
        download_topic_map_btmap(&btmap_url.url),
    )
    .await?;

    let Some(output) = args.output.as_deref() else {
        std::io::stdout()
            .write_all(&btmap)
            .context("failed to write topic map to stdout")?;
        return Ok(());
    };

    let output = resolve_output_path(output)?;
    write_bytes_atomic(&output, &btmap)
        .with_context(|| format!("failed to write topic map to {}", output.display()))?;

    let downloaded = TopicMapBtmapDownload {
        function_id: function_id.to_string(),
        version: args.version.clone(),
        output: output.display().to_string(),
        bytes: btmap.len(),
    };

    if json {
        println!("{}", serde_json::to_string(&downloaded)?);
        return Ok(());
    }

    print_command_status(
        CommandStatus::Success,
        &format!(
            "Downloaded topic map to {} ({} bytes)",
            downloaded.output, downloaded.bytes
        ),
    );
    Ok(())
}

async fn download_topic_map_btmap(url: &str) -> Result<Vec<u8>> {
    let client = http::build_http_client(http::DEFAULT_HTTP_TIMEOUT)
        .context("failed to build topic map download HTTP client")?;
    let response = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("failed to download topic map from {url}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("topic map download failed ({status}): {body}");
    }
    Ok(response
        .bytes()
        .await
        .context("failed to read topic map body")?
        .to_vec())
}

fn resolve_output_path(path: &Path) -> Result<PathBuf> {
    if path.is_absolute() {
        return Ok(path.to_path_buf());
    }
    Ok(std::env::current_dir()
        .context("failed to resolve current directory")?
        .join(path))
}
