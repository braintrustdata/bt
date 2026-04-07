use anyhow::{Context, Result};
use reqwest::header::{HeaderValue, CONTENT_TYPE};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::auth::LoginContext;

pub const DEFAULT_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Clone)]
pub struct ApiClient {
    http: Client,
    base_url: String,
    api_key: String,
    org_name: String,
}

#[derive(Debug)]
pub struct HttpError {
    pub status: reqwest::StatusCode,
    pub body: String,
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "request failed ({}): {}", self.status, self.body)
    }
}

impl std::error::Error for HttpError {}

#[derive(Debug, Deserialize)]
pub struct BtqlResponse<T> {
    pub data: Vec<T>,
}

impl ApiClient {
    pub fn new(ctx: &LoginContext) -> Result<Self> {
        let http = Client::builder()
            .timeout(DEFAULT_HTTP_TIMEOUT)
            .build()
            .context("failed to build HTTP client")?;

        Ok(Self {
            http,
            base_url: ctx.api_url.trim_end_matches('/').to_string(),
            api_key: ctx.login.api_key.clone(),
            org_name: ctx.login.org_name.clone(),
        })
    }

    pub fn url(&self, path: &str) -> String {
        let path = path.trim_start_matches('/');
        format!("{}/{}", self.base_url, path)
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    pub fn org_name(&self) -> &str {
        &self.org_name
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.url(path);
        let response = self
            .http
            .get(&url)
            .bearer_auth(&self.api_key)
            .send()
            .await
            .context("request failed")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(HttpError { status, body }.into());
        }

        parse_json_response(response).await
    }

    pub async fn post<T: DeserializeOwned, B: Serialize>(&self, path: &str, body: &B) -> Result<T> {
        let url = self.url(path);
        let response = self
            .http
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(body)
            .send()
            .await
            .context("request failed")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(HttpError { status, body }.into());
        }

        parse_json_response(response).await
    }

    pub async fn post_with_headers<T, B>(
        &self,
        path: &str,
        body: &B,
        headers: &[(&str, &str)],
    ) -> Result<T>
    where
        T: DeserializeOwned,
        B: Serialize,
    {
        self.post_with_headers_timeout(path, body, headers, None)
            .await
    }

    pub async fn post_with_headers_timeout<T, B>(
        &self,
        path: &str,
        body: &B,
        headers: &[(&str, &str)],
        timeout: Option<std::time::Duration>,
    ) -> Result<T>
    where
        T: DeserializeOwned,
        B: Serialize,
    {
        let url = self.url(path);
        let mut request = self.http.post(&url).bearer_auth(&self.api_key).json(body);

        for (key, value) in headers {
            request = request.header(*key, *value);
        }
        if let Some(t) = timeout {
            request = request.timeout(t);
        }

        let response = request.send().await.context("request failed")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(HttpError { status, body }.into());
        }

        parse_json_response(response).await
    }

    pub async fn post_with_headers_raw<B>(
        &self,
        path: &str,
        body: &B,
        headers: &[(&str, &str)],
    ) -> Result<reqwest::Response>
    where
        B: Serialize,
    {
        let url = self.url(path);
        let mut request = self.http.post(&url).bearer_auth(&self.api_key).json(body);

        for (key, value) in headers {
            request = request.header(*key, *value);
        }

        request.send().await.context("request failed")
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let url = self.url(path);
        let response = self
            .http
            .delete(&url)
            .bearer_auth(&self.api_key)
            .send()
            .await
            .context("request failed")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(HttpError { status, body }.into());
        }

        Ok(())
    }

    pub async fn btql<T: DeserializeOwned>(&self, query: &str) -> Result<BtqlResponse<T>> {
        let body = json!({
            "query": query,
            "fmt": "json",
        });

        let org_name = self.org_name();
        let headers = if !org_name.is_empty() {
            vec![("x-bt-org-name", org_name)]
        } else {
            Vec::new()
        };

        self.post_with_headers("/btql", &body, &headers).await
    }
}

async fn parse_json_response<T: DeserializeOwned>(response: reqwest::Response) -> Result<T> {
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response
        .text()
        .await
        .context("failed to read response body")?;
    serde_json::from_str(&body).with_context(|| {
        let mut message = String::from("failed to parse response");
        if let Some(content_type) = content_type.as_deref() {
            message.push_str(&format!(" (content-type: {content_type})"));
        }
        let preview = preview_response_body(&body, 512);
        if !preview.is_empty() {
            message.push_str(&format!("; body: {preview}"));
        }
        message
    })
}

fn preview_response_body(body: &str, max_chars: usize) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return String::new();
    }

    let mut preview = trimmed.chars().take(max_chars).collect::<String>();
    if trimmed.chars().count() > max_chars {
        preview.push_str("...");
    }
    preview
}

const UPLOAD_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

pub async fn put_signed_url(
    url: &str,
    body: Vec<u8>,
    content_encoding: Option<&str>,
) -> Result<()> {
    let client = Client::builder()
        .timeout(UPLOAD_HTTP_TIMEOUT)
        .build()
        .context("failed to build signed-url HTTP client")?;

    let mut request = client.put(url).body(body);
    if let Some(encoding) = content_encoding {
        request = request.header("Content-Encoding", encoding);
    }
    if url.contains(".blob.core.windows.net") {
        request = request.header("x-ms-blob-type", HeaderValue::from_static("BlockBlob"));
    }

    let response = request
        .send()
        .await
        .context("signed-url upload request failed")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(HttpError { status, body }.into());
    }
    Ok(())
}
