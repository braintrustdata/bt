use std::collections::HashMap;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoffBuilder;
use reqwest::header::{HeaderMap, HeaderName, HeaderValue};
use reqwest::{multipart, Client, Url};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use tokio::time::sleep;

use crate::args::BaseArgs;

const DEFAULT_MAX_REQUEST_SIZE: usize = 6 * 1024 * 1024;
const LOGS_API_VERSION: u8 = 2;
const LOGS3_OVERFLOW_REFERENCE_TYPE: &str = "logs3_overflow";
const MAX_ATTEMPTS: usize = 5;
const BASE_BACKOFF_MS: u64 = 300;
const MAX_BACKOFF_MS: u64 = 8_000;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const OBJECT_ID_KEYS: [&str; 6] = [
    "experiment_id",
    "dataset_id",
    "prompt_session_id",
    "project_id",
    "log_id",
    "function_data",
];

#[derive(Debug, Clone, Default)]
pub struct Logs3UploadResult {
    pub rows_uploaded: usize,
    pub bytes_processed: usize,
    pub requests_sent: usize,
}

#[derive(Debug, Clone)]
pub struct Logs3BatchUploader {
    client: Client,
    api_url: Url,
    api_key: String,
    org_name: Option<String>,
    payload_limits: Option<PayloadLimits>,
}

#[derive(Debug, Clone)]
struct PayloadLimits {
    max_request_size: usize,
    can_use_overflow: bool,
}

#[derive(Debug, Clone)]
struct RowPayload {
    row_json: String,
    row_bytes: usize,
    overflow_meta: Logs3OverflowInputRow,
}

#[derive(Debug, Clone, Serialize)]
struct Logs3OverflowInputRow {
    object_ids: Map<String, Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    is_delete: Option<bool>,
    input_row: OverflowInputRowInfo,
}

#[derive(Debug, Clone, Serialize)]
struct OverflowInputRowInfo {
    byte_size: usize,
}

#[derive(Debug, Deserialize)]
struct VersionInfo {
    #[serde(default)]
    logs3_payload_max_bytes: Option<usize>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "UPPERCASE")]
enum OverflowMethod {
    Put,
    Post,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Logs3OverflowUpload {
    method: OverflowMethod,
    signed_url: String,
    headers: Option<HashMap<String, String>>,
    fields: Option<HashMap<String, String>>,
    key: String,
}

#[derive(Debug)]
struct UploadApiError {
    status: u16,
    body: String,
}

impl std::fmt::Display for UploadApiError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "request failed ({}): {}", self.status, self.body)
    }
}

impl std::error::Error for UploadApiError {}

impl Logs3BatchUploader {
    pub fn new(
        base: &BaseArgs,
        api_url: impl AsRef<str>,
        api_key: impl Into<String>,
        org_name: Option<String>,
    ) -> Result<Self> {
        let api_url =
            Url::parse(api_url.as_ref()).map_err(|err| anyhow!("invalid api_url: {err}"))?;
        let client = crate::http::client_builder(base, REQUEST_TIMEOUT)?
            .build()
            .context("failed to build logs3 HTTP client")?;
        Ok(Self {
            client,
            api_url,
            api_key: api_key.into(),
            org_name: org_name.filter(|name| !name.trim().is_empty()),
            payload_limits: None,
        })
    }

    pub async fn upload_rows(
        &mut self,
        rows: &[Map<String, Value>],
        batch_max_num_items: usize,
    ) -> Result<Logs3UploadResult> {
        if rows.is_empty() {
            return Ok(Logs3UploadResult::default());
        }

        let limits = self.get_payload_limits().await;
        let mut row_payloads = Vec::with_capacity(rows.len());
        for row in rows {
            row_payloads.push(RowPayload::from_row(row)?);
        }

        let batch_limit_rows = batch_max_num_items.max(1);
        let batch_limit_bytes = (limits.max_request_size / 2).max(1);
        let batches = batch_items(row_payloads, batch_limit_rows, batch_limit_bytes, |item| {
            item.row_bytes
        });

        let mut result = Logs3UploadResult::default();
        for batch in batches {
            let batch_result = self.submit_row_batch(&batch, &limits).await?;
            result.bytes_processed += batch_result.bytes_processed;
            result.requests_sent += batch_result.requests_sent;
            result.rows_uploaded += batch_result.rows_uploaded;
        }
        Ok(result)
    }

    async fn get_payload_limits(&mut self) -> PayloadLimits {
        if let Some(existing) = &self.payload_limits {
            return existing.clone();
        }

        let fetched = self.fetch_payload_limits().await;
        self.payload_limits = Some(fetched.clone());
        fetched
    }

    async fn fetch_payload_limits(&self) -> PayloadLimits {
        let mut server_limit = None;
        if let Ok(url) = self.api_url.join("version") {
            let mut request = self.client.get(url).bearer_auth(&self.api_key);
            if let Some(org_name) = &self.org_name {
                request = request.header("x-bt-org-name", org_name);
            }
            if let Ok(response) = request.send().await {
                if response.status().is_success() {
                    if let Ok(version) = response.json::<VersionInfo>().await {
                        server_limit = version.logs3_payload_max_bytes.filter(|limit| *limit > 0);
                    }
                }
            }
        }

        PayloadLimits {
            max_request_size: server_limit.unwrap_or(DEFAULT_MAX_REQUEST_SIZE).max(1),
            can_use_overflow: server_limit.is_some(),
        }
    }

    async fn submit_row_batch(
        &self,
        items: &[RowPayload],
        limits: &PayloadLimits,
    ) -> Result<Logs3UploadResult> {
        if items.is_empty() {
            return Ok(Logs3UploadResult::default());
        }

        let mut pending = vec![items.to_vec()];
        let mut result = Logs3UploadResult::default();

        while let Some(chunk) = pending.pop() {
            if chunk.is_empty() {
                continue;
            }
            let payload = construct_logs3_payload(&chunk);
            let payload_bytes = payload.len();

            if limits.can_use_overflow && payload_bytes > limits.max_request_size {
                let overflow_result = self.submit_overflow(&chunk, payload, payload_bytes).await?;
                result.rows_uploaded += overflow_result.rows_uploaded;
                result.bytes_processed += overflow_result.bytes_processed;
                result.requests_sent += overflow_result.requests_sent;
                continue;
            }

            let (post_result, post_attempts) =
                run_with_backoff(|| self.post_logs3_raw(&payload)).await;
            result.requests_sent += post_attempts;
            match post_result {
                Ok(()) => {
                    result.rows_uploaded += chunk.len();
                    result.bytes_processed += payload_bytes;
                }
                Err(err) if upload_api_status(&err) == Some(413) => {
                    if limits.can_use_overflow {
                        let overflow_result = self
                            .submit_overflow(&chunk, payload.clone(), payload_bytes)
                            .await?;
                        result.rows_uploaded += overflow_result.rows_uploaded;
                        result.bytes_processed += overflow_result.bytes_processed;
                        result.requests_sent += overflow_result.requests_sent;
                    } else if chunk.len() > 1 {
                        let mid = chunk.len() / 2;
                        pending.push(chunk[mid..].to_vec());
                        pending.push(chunk[..mid].to_vec());
                    } else {
                        return Err(anyhow!(
                            "single row exceeds server payload limit and overflow is unavailable"
                        ));
                    }
                }
                Err(err) => return Err(err),
            }
        }

        Ok(result)
    }

    async fn submit_overflow(
        &self,
        items: &[RowPayload],
        payload: String,
        payload_bytes: usize,
    ) -> Result<Logs3UploadResult> {
        let overflow_rows = items
            .iter()
            .map(|item| item.overflow_meta.clone())
            .collect::<Vec<_>>();
        let mut requests_sent = 0usize;

        let (upload_result, upload_attempts) =
            run_with_backoff(|| self.request_overflow_upload(&overflow_rows, payload_bytes)).await;
        requests_sent += upload_attempts;
        let upload = upload_result?;

        let (payload_result, payload_attempts) =
            run_with_backoff(|| self.upload_overflow_payload(&upload, &payload)).await;
        requests_sent += payload_attempts;
        payload_result?;

        let overflow_reference = json!({
            "rows": {
                "type": LOGS3_OVERFLOW_REFERENCE_TYPE,
                "key": upload.key,
            },
            "api_version": LOGS_API_VERSION,
        })
        .to_string();
        let (post_result, post_attempts) =
            run_with_backoff(|| self.post_logs3_raw(&overflow_reference)).await;
        requests_sent += post_attempts;
        post_result?;

        Ok(Logs3UploadResult {
            rows_uploaded: items.len(),
            bytes_processed: payload_bytes,
            requests_sent,
        })
    }

    async fn request_overflow_upload(
        &self,
        rows: &[Logs3OverflowInputRow],
        payload_bytes: usize,
    ) -> Result<Logs3OverflowUpload> {
        let url = self
            .api_url
            .join("logs3/overflow")
            .map_err(|err| anyhow!("invalid overflow URL: {err}"))?;
        let request_body = json!({
            "content_type": "application/json",
            "size_bytes": payload_bytes,
            "rows": rows,
        });
        let mut request = self
            .client
            .post(url)
            .bearer_auth(&self.api_key)
            .header("content-type", "application/json")
            .json(&request_body);
        if let Some(org_name) = &self.org_name {
            request = request.header("x-bt-org-name", org_name);
        }
        let response = request
            .send()
            .await
            .context("failed to request logs3 overflow upload")?;
        ensure_success(response)
            .await?
            .json::<Logs3OverflowUpload>()
            .await
            .context("failed to parse logs3 overflow response")
    }

    async fn upload_overflow_payload(
        &self,
        upload: &Logs3OverflowUpload,
        payload: &str,
    ) -> Result<()> {
        let signed_url = Url::parse(&upload.signed_url)
            .map_err(|err| anyhow!("invalid logs3 overflow signed URL: {err}"))?;

        match upload.method {
            OverflowMethod::Post => {
                let fields = upload
                    .fields
                    .clone()
                    .ok_or_else(|| anyhow!("logs3 overflow POST upload missing form fields"))?;
                let content_type = fields
                    .get("Content-Type")
                    .cloned()
                    .unwrap_or_else(|| "application/json".to_string());
                let mut form = multipart::Form::new();
                for (key, value) in &fields {
                    form = form.text(key.clone(), value.clone());
                }
                let file_part = multipart::Part::text(payload.to_string())
                    .mime_str(&content_type)
                    .map_err(|err| {
                        anyhow!("invalid overflow content-type '{content_type}': {err}")
                    })?;
                form = form.part("file", file_part);

                let mut request = self.client.post(signed_url).multipart(form);
                let mut headers = header_map_from_pairs(upload.headers.as_ref())?;
                headers.remove("content-type");
                if !headers.is_empty() {
                    request = request.headers(headers);
                }
                let response = request
                    .send()
                    .await
                    .context("failed to upload overflow payload")?;
                ensure_success(response).await?;
            }
            OverflowMethod::Put => {
                let mut headers = header_map_from_pairs(upload.headers.as_ref())?;
                if upload.signed_url.contains("blob.core.windows.net")
                    && !headers.contains_key("x-ms-blob-type")
                {
                    headers.insert("x-ms-blob-type", HeaderValue::from_static("BlockBlob"));
                }

                let mut request = self.client.put(signed_url).body(payload.to_string());
                if !headers.is_empty() {
                    request = request.headers(headers);
                }
                let response = request
                    .send()
                    .await
                    .context("failed to upload overflow payload")?;
                ensure_success(response).await?;
            }
        }

        Ok(())
    }

    async fn post_logs3_raw(&self, payload: &str) -> Result<()> {
        let url = self
            .api_url
            .join("logs3")
            .map_err(|err| anyhow!("invalid logs3 URL: {err}"))?;
        let mut request = self
            .client
            .post(url)
            .bearer_auth(&self.api_key)
            .header("content-type", "application/json")
            .body(payload.to_string());
        if let Some(org_name) = &self.org_name {
            request = request.header("x-bt-org-name", org_name);
        }
        let response = request.send().await.context("failed to post logs3 batch")?;
        ensure_success(response).await?;
        Ok(())
    }
}

impl RowPayload {
    fn from_row(row: &Map<String, Value>) -> Result<Self> {
        let row_json = serde_json::to_string(row).context("failed to serialize logs3 row")?;
        let row_bytes = row_json.len();
        let mut object_ids = Map::new();
        for key in OBJECT_ID_KEYS {
            if let Some(value) = row.get(key) {
                object_ids.insert(key.to_string(), value.clone());
            }
        }
        let is_delete = row.get("_object_delete").and_then(Value::as_bool);
        Ok(Self {
            row_json,
            row_bytes,
            overflow_meta: Logs3OverflowInputRow {
                object_ids,
                is_delete,
                input_row: OverflowInputRowInfo {
                    byte_size: row_bytes,
                },
            },
        })
    }
}

fn ensure_success(
    response: reqwest::Response,
) -> std::pin::Pin<Box<dyn std::future::Future<Output = Result<reqwest::Response>> + Send>> {
    Box::pin(async move {
        if response.status().is_success() {
            return Ok(response);
        }
        let status = response.status().as_u16();
        let body = response.text().await.unwrap_or_default();
        Err(UploadApiError { status, body }.into())
    })
}

fn upload_api_status(err: &anyhow::Error) -> Option<u16> {
    err.chain().find_map(|source| {
        source
            .downcast_ref::<UploadApiError>()
            .map(|err| err.status)
    })
}

fn construct_logs3_payload(items: &[RowPayload]) -> String {
    let rows = items
        .iter()
        .map(|item| item.row_json.as_str())
        .collect::<Vec<_>>()
        .join(",");
    format!("{{\"rows\":[{rows}],\"api_version\":{LOGS_API_VERSION}}}")
}

fn header_map_from_pairs(headers: Option<&HashMap<String, String>>) -> Result<HeaderMap> {
    let mut out = HeaderMap::new();
    if let Some(headers) = headers {
        for (key, value) in headers {
            let name = HeaderName::from_bytes(key.as_bytes())
                .map_err(|err| anyhow!("invalid HTTP header name '{key}': {err}"))?;
            let header_value = HeaderValue::from_str(value)
                .map_err(|err| anyhow!("invalid HTTP header value for '{key}': {err}"))?;
            out.insert(name, header_value);
        }
    }
    Ok(out)
}

fn should_retry(err: &anyhow::Error) -> bool {
    if let Some(reqwest_err) = err
        .chain()
        .find_map(|source| source.downcast_ref::<reqwest::Error>())
    {
        return reqwest_err.is_timeout() || reqwest_err.is_connect();
    }

    matches!(upload_api_status(err), Some(429 | 500..=599))
}

async fn run_with_backoff<T, F, Fut>(mut operation: F) -> (Result<T>, usize)
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempts = 0usize;
    let mut backoff = ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(BASE_BACKOFF_MS))
        .with_multiplier(2.0)
        .with_randomization_factor(0.2)
        .with_max_interval(Duration::from_millis(MAX_BACKOFF_MS))
        .with_max_elapsed_time(None)
        .build();

    loop {
        attempts += 1;
        match operation().await {
            Ok(value) => return (Ok(value), attempts),
            Err(err) => {
                if !should_retry(&err) || attempts >= MAX_ATTEMPTS {
                    return (Err(err), attempts);
                }
                match backoff.next_backoff() {
                    Some(delay) => sleep(delay).await,
                    None => return (Err(err), attempts),
                }
            }
        }
    }
}

fn batch_items<T, F>(items: Vec<T>, max_items: usize, max_bytes: usize, size_of: F) -> Vec<Vec<T>>
where
    F: Fn(&T) -> usize,
{
    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_bytes = 0usize;

    for item in items {
        let item_bytes = size_of(&item);
        let would_overflow = !current.is_empty()
            && (current.len() >= max_items || current_bytes.saturating_add(item_bytes) > max_bytes);
        if would_overflow {
            batches.push(current);
            current = Vec::new();
            current_bytes = 0;
        }
        current_bytes = current_bytes.saturating_add(item_bytes);
        current.push(item);
    }

    if !current.is_empty() {
        batches.push(current);
    }

    batches
}
