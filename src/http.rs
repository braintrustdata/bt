use anyhow::{Context, Result};
use reqwest::header::HeaderValue;
use reqwest::{Client, ClientBuilder};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::auth::LoginContext;

pub const DEFAULT_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

pub fn build_http_client(timeout: std::time::Duration) -> Result<Client> {
    build_http_client_from_builder(Client::builder().timeout(timeout))
}

pub fn build_http_client_from_builder(mut builder: ClientBuilder) -> Result<Client> {
    // Prefer the platform/native root store so standard envs like SSL_CERT_FILE
    // are honored consistently across the CLI.
    builder = builder
        .tls_built_in_native_certs(true)
        .tls_built_in_webpki_certs(false);

    builder.build().context("failed to build HTTP client")
}

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
    #[serde(default)]
    pub cursor: Option<String>,
}

impl ApiClient {
    pub fn new(ctx: &LoginContext) -> Result<Self> {
        let http = build_http_client(DEFAULT_HTTP_TIMEOUT)?;

        Ok(Self {
            http,
            base_url: ctx.api_url.trim_end_matches('/').to_string(),
            api_key: ctx.login.api_key().context("login state missing API key")?,
            org_name: ctx.login.org_name().unwrap_or_default(),
        })
    }

    pub fn url(&self, path: &str) -> String {
        let path = path.trim_start_matches('/');
        format!("{}/{}", self.base_url, path)
    }

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    pub fn base_url(&self) -> &str {
        &self.base_url
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

        response.json().await.context("failed to parse response")
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

        response.json().await.context("failed to parse response")
    }

    pub async fn patch<T: DeserializeOwned, B: Serialize>(
        &self,
        path: &str,
        body: &B,
    ) -> Result<T> {
        let url = self.url(path);
        let response = self
            .http
            .patch(&url)
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

        response.json().await.context("failed to parse response")
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

        response.json().await.context("failed to parse response")
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

const UPLOAD_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

pub async fn put_signed_url(
    url: &str,
    body: Vec<u8>,
    content_encoding: Option<&str>,
) -> Result<()> {
    let client =
        build_http_client(UPLOAD_HTTP_TIMEOUT).context("failed to build signed-url HTTP client")?;

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn btql_response_deserializes_optional_cursor() {
        let response: BtqlResponse<serde_json::Value> = serde_json::from_value(json!({
            "data": [],
            "cursor": "cursor-1",
        }))
        .expect("btql response");

        assert_eq!(response.cursor.as_deref(), Some("cursor-1"));
    }

    #[test]
    fn btql_response_cursor_defaults_to_none() {
        let response: BtqlResponse<serde_json::Value> = serde_json::from_value(json!({
            "data": [],
        }))
        .expect("btql response");

        assert_eq!(response.cursor, None);
    }
}
