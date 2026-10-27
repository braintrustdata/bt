//! Authenticated API access. Headers, errors, decoding, and retries live here.
//! Use `with_options(HttpOptions { .. })` before any HTTP or BTQL call.
//!
//! ```ignore
//! let project: Project = client.get("/v1/project/test-id").await?;
//! let created: Project = client.with_options(HttpOptions {
//!     timeout: Some(Duration::from_secs(60)), ..Default::default()
//! }).post("/v1/project", &body).await?;
//! let page: BtqlResponse<Row> = client.btql(&query).await?;
//! let options = BtqlOptions { lint_mode: Some("strict"), ..Default::default() };
//! let result: SqlResponse = client.btql_with_options(&sql, options).await?;
//! let response: WithHeaders<Project> = client.post("/v1/project", &body).await?;
//! let Text(body) = client.get("/text-endpoint").await?;
//! let NoContent = client.post("/action", &body).await?;
//! // Pages preserve cursors/schema; use TryStreamExt to fold or flatten them.
//! let pages = Pagination::default().pages(None, 100, |limit, cursor| async move {
//!     client.btql(&build_query(limit, cursor)).await
//! });
//! ```
pub mod btql;
pub use btql::{BtqlOptions, Pagination};

use backoff::{backoff::Backoff, ExponentialBackoffBuilder};
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use reqwest::header::{HeaderValue, CONTENT_TYPE};
use reqwest::{Client, ClientBuilder, Method, StatusCode};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};

use crate::auth::LoginContext;

pub const DEFAULT_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);
pub const BT_USER_AGENT: &str = concat!("bt-cli/", env!("CARGO_PKG_VERSION"));

pub fn build_http_client(timeout: std::time::Duration) -> Result<Client> {
    build_http_client_from_builder(Client::builder().timeout(timeout))
}

pub fn build_http_client_from_builder(mut builder: ClientBuilder) -> Result<Client> {
    // Prefer the platform/native root store so standard envs like SSL_CERT_FILE
    // are honored consistently across the CLI.
    builder = builder
        .user_agent(BT_USER_AGENT)
        .tls_built_in_native_certs(true)
        .tls_built_in_webpki_certs(false);

    builder.build().context("failed to build HTTP client")
}

#[derive(Clone)]
pub struct ApiClient {
    http: Client,
    base_url: String,
    api_key: String,
    org_id: String,
    org_name: String,
    options: HttpOptions,
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

// Keep the status and any received bytes when a server closes an error response early.
async fn response_error(mut response: reqwest::Response) -> HttpError {
    let status = response.status();
    let mut body = Vec::new();
    loop {
        match response.chunk().await {
            Ok(Some(chunk)) => body.extend_from_slice(&chunk),
            Ok(None) => break,
            Err(err) => {
                let detail = format!("\nfailed to read error response body: {err}");
                body.extend_from_slice(detail.as_bytes());
                break;
            }
        }
    }
    HttpError {
        status,
        body: String::from_utf8_lossy(&body).into_owned(),
    }
}

#[derive(Debug)]
pub struct ResponseParseError {
    message: String,
}

impl std::fmt::Display for ResponseParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for ResponseParseError {}

async fn parse_json_response<T: DeserializeOwned>(
    response: reqwest::Response,
    method: &str,
    path: &str,
) -> Result<T> {
    let status = response.status();
    let content_type = response
        .headers()
        .get(CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .map(str::to_string);
    let body = response
        .bytes()
        .await
        .context("failed to read response body")?;

    parse_json_body::<T>(&body, method, path, status, content_type.as_deref())
}

fn parse_json_body<T: DeserializeOwned>(
    body: &[u8],
    method: &str,
    path: &str,
    status: StatusCode,
    content_type: Option<&str>,
) -> Result<T> {
    let mut deserializer = serde_json::Deserializer::from_slice(body);
    let parsed = match serde_path_to_error::deserialize(&mut deserializer) {
        Ok(parsed) => parsed,
        Err(err) => {
            let json_path = err.path().to_string();
            let inner = err.into_inner();
            return Err(parse_response_error::<T>(
                method,
                path,
                status,
                content_type,
                &json_path,
                &inner,
                body.len(),
            ));
        }
    };

    if let Err(err) = deserializer.end() {
        return Err(parse_response_error::<T>(
            method,
            path,
            status,
            content_type,
            "<root>",
            &err,
            body.len(),
        ));
    }

    Ok(parsed)
}

fn parse_response_error<T>(
    method: &str,
    path: &str,
    status: StatusCode,
    content_type: Option<&str>,
    json_path: &str,
    err: &serde_json::Error,
    body_len: usize,
) -> anyhow::Error {
    let json_path = if json_path.is_empty() || json_path == "." {
        "<root>"
    } else {
        json_path
    };
    let content_type = content_type.unwrap_or("<missing>");
    let message = format!(
        "failed to parse response from {method} {path}\n  target: {}\n  JSON path: {json_path}\n  reason: {err}\n  status: {status}\n  content-type: {content_type}\n  body bytes: {body_len}",
        std::any::type_name::<T>(),
    );

    ResponseParseError { message }.into()
}

#[derive(Debug, Serialize, Deserialize)]
pub struct BtqlResponse<T = serde_json::Map<String, serde_json::Value>> {
    pub data: Vec<T>,
    #[serde(default, deserialize_with = "btql::deserialize_cursor")]
    pub cursor: Option<String>,
    #[serde(default)]
    pub schema: Option<serde_json::Value>,
}

impl ApiClient {
    pub fn new(ctx: &LoginContext) -> Result<Self> {
        let http = build_http_client(DEFAULT_HTTP_TIMEOUT)?;

        Ok(Self {
            http,
            base_url: ctx.api_url.trim_end_matches('/').to_string(),
            api_key: ctx.login.api_key().context("login state missing API key")?,
            org_id: ctx.login.org_id().unwrap_or_default(),
            org_name: ctx.login.org_name().unwrap_or_default(),
            options: HttpOptions::default(),
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

    pub fn org_id(&self) -> &str {
        &self.org_id
    }

    pub fn org_name(&self) -> &str {
        &self.org_name
    }

    /// Return a configured copy, leaving the original client unchanged.
    /// Unset scalar options inherit existing settings; headers and query params
    /// are appended. Settings apply to both HTTP and BTQL calls.
    pub fn with_options(&self, options: HttpOptions) -> Self {
        let mut configured = self.clone();
        configured.options.timeout = options.timeout.or(configured.options.timeout);
        configured.options.retry = options.retry.or(configured.options.retry);
        configured.options.on_retry = options.on_retry.or(configured.options.on_retry);
        configured.options.trace = options.trace.or(configured.options.trace);
        configured.options.headers.extend(options.headers);
        configured.options.query.extend(options.query);
        configured
    }

    fn prepare(&self, method: Method, path: &str) -> reqwest::RequestBuilder {
        let mut inner = self
            .http
            .request(method.clone(), self.url(path))
            .bearer_auth(&self.api_key);
        if !self.org_name.is_empty() {
            inner = inner.header("x-bt-org-name", &self.org_name);
        }
        for (name, value) in &self.options.headers {
            inner = inner.header(name, value);
        }
        if let Some(timeout) = self.options.timeout {
            inner = inner.timeout(timeout);
        }
        inner.query(&self.options.query)
    }

    /// Execute an HTTP method without a body; responses are JSON by default.
    /// Use `Text` or `WithHeaders<T>` as the result type for other response needs.
    pub async fn request<T: FromResponse>(&self, method: Method, path: &str) -> Result<T> {
        let request = self.prepare(method.clone(), path);
        self.execute(method, path, request).await
    }
    pub async fn get<T: FromResponse>(&self, path: &str) -> Result<T> {
        self.request(Method::GET, path).await
    }
    pub async fn post<T: FromResponse>(
        &self,
        path: &str,
        body: &(impl Serialize + ?Sized),
    ) -> Result<T> {
        self.send_body(Method::POST, path, body).await
    }
    pub async fn put<T: FromResponse>(
        &self,
        path: &str,
        body: &(impl Serialize + ?Sized),
    ) -> Result<T> {
        self.send_body(Method::PUT, path, body).await
    }
    pub async fn patch<T: FromResponse>(
        &self,
        path: &str,
        body: &(impl Serialize + ?Sized),
    ) -> Result<T> {
        self.send_body(Method::PATCH, path, body).await
    }
    async fn send_body<T: FromResponse>(
        &self,
        method: Method,
        path: &str,
        body: &(impl Serialize + ?Sized),
    ) -> Result<T> {
        let request = self.prepare(method.clone(), path).json(body);
        self.execute(method, path, request).await
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        self.request::<NoContent>(Method::DELETE, path)
            .await
            .map(|_| ())
    }
}

/// Transport settings shared by HTTP and BTQL. Unset fields use client defaults.
#[derive(Clone, Default)]
pub struct HttpOptions {
    pub timeout: Option<Duration>,
    pub headers: Vec<(String, String)>,
    pub query: Vec<(String, String)>,
    /// HTTP defaults to one attempt; BTQL defaults to retrying transient failures.
    /// Opt into retries only when an operation is safe to repeat.
    pub retry: Option<bool>,
    pub on_retry: Option<Arc<dyn Fn(RetryReason) + Send + Sync>>,
    pub trace: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryReason {
    Status(u16),
    Network,
}

const MAX_ATTEMPTS: usize = 5;
const MAX_DELAY: Duration = Duration::from_secs(8);

fn retry_reason(error: &anyhow::Error) -> Option<RetryReason> {
    if let Some(error) = error.downcast_ref::<HttpError>() {
        let status = error.status;
        return (status.is_server_error()
            || status == StatusCode::PAYLOAD_TOO_LARGE
            || status == StatusCode::TOO_MANY_REQUESTS)
            .then_some(RetryReason::Status(status.as_u16()));
    }
    error.downcast_ref::<reqwest::Error>().and_then(|error| {
        (!error.is_builder()
            && (error.is_connect() || error.is_timeout() || error.is_request() || error.is_body()))
        .then_some(RetryReason::Network)
    })
}

fn retry_delay(retry_after: Option<Duration>, backoff: &mut impl Backoff) -> Duration {
    // Server rate-limit windows take precedence over the local backoff cap.
    retry_after.unwrap_or_else(|| backoff.next_backoff().unwrap_or(MAX_DELAY).min(MAX_DELAY))
}

impl ApiClient {
    async fn execute<T: FromResponse>(
        &self,
        method: Method,
        path: &str,
        request: reqwest::RequestBuilder,
    ) -> Result<T> {
        let started = Instant::now();
        let mut backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(300))
            .with_multiplier(2.0)
            .with_randomization_factor(0.2)
            .with_max_interval(MAX_DELAY)
            .with_max_elapsed_time(None)
            .build();
        let attempts = if self.options.retry.unwrap_or(false) {
            MAX_ATTEMPTS
        } else {
            1
        };
        let (client, request) = request.build_split();
        let request = request.context("failed to build API request")?;
        let mut attempt = 0;
        loop {
            attempt += 1;
            let request = request
                .try_clone()
                .context("request body cannot be replayed for retry")?;
            let mut retry_after = None;
            let result = async {
                let response = client.execute(request).await?;
                retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(Duration::from_secs);
                if !response.status().is_success() {
                    return Err(response_error(response).await.into());
                }
                T::decode(response, method.as_str(), path).await
            }
            .await;
            if let Err(error) = &result {
                if attempt < attempts {
                    if let Some(reason) = retry_reason(error) {
                        if let Some(observer) = &self.options.on_retry {
                            observer(reason);
                        }
                        tokio::time::sleep(retry_delay(retry_after, &mut backoff)).await;
                        continue;
                    }
                }
            }
            if let Some(label) = &self.options.trace {
                eprintln!(
                    "{label}: {} {} {}ms {}",
                    method,
                    path,
                    started.elapsed().as_millis(),
                    if result.is_ok() { "ok" } else { "failed" }
                );
            }
            return result.map_err(|error| {
                let message = format!(
                    "{} {} failed after {attempt} attempt(s): {error}",
                    method, path
                );
                error.context(message)
            });
        }
    }
}

/// The response type selects decoding. Ordinary deserializable types use JSON.
/// Decoding stays inside the shared retry/error boundary, including body reads.
pub trait FromResponse: Sized {
    fn decode(
        response: reqwest::Response,
        method: &str,
        path: &str,
    ) -> impl Future<Output = Result<Self>> + Send;
}
impl<T: DeserializeOwned> FromResponse for T {
    async fn decode(response: reqwest::Response, method: &str, path: &str) -> Result<Self> {
        parse_json_response(response, method, path).await
    }
}

/// A successful response whose body is irrelevant (including an empty 204).
pub struct NoContent;
impl FromResponse for NoContent {
    async fn decode(_: reqwest::Response, _: &str, _: &str) -> Result<Self> {
        Ok(Self)
    }
}

/// A text response, without JSON decoding.
#[derive(Debug)]
pub struct Text(pub String);
impl FromResponse for Text {
    async fn decode(response: reqwest::Response, _: &str, _: &str) -> Result<Self> {
        response
            .text()
            .await
            .map(Self)
            .context("failed to read response body")
    }
}

/// Retain response headers alongside a decoded JSON or text body.
pub struct WithHeaders<T> {
    pub headers: reqwest::header::HeaderMap,
    pub body: T,
}
impl<T: FromResponse> FromResponse for WithHeaders<T> {
    async fn decode(response: reqwest::Response, method: &str, path: &str) -> Result<Self> {
        let headers = response.headers().clone();
        let body = T::decode(response, method, path).await?;
        Ok(Self { headers, body })
    }
}

const UPLOAD_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

pub async fn put_signed_url(
    url: &str,
    body: Vec<u8>,
    content_encoding: Option<&str>,
) -> Result<()> {
    let mut headers = HashMap::new();
    if let Some(encoding) = content_encoding {
        headers.insert("Content-Encoding".to_string(), encoding.to_string());
    }
    put_signed_url_with_headers(url, body, &headers).await
}

pub async fn put_signed_url_with_headers(
    url: &str,
    body: Vec<u8>,
    headers: &HashMap<String, String>,
) -> Result<()> {
    let client =
        build_http_client(UPLOAD_HTTP_TIMEOUT).context("failed to build signed-url HTTP client")?;

    let mut request = client.put(url).body(body);
    let mut has_azure_blob_type = false;
    for (key, value) in headers {
        if key.eq_ignore_ascii_case("x-ms-blob-type") {
            has_azure_blob_type = true;
        }
        request = request.header(key.as_str(), value.as_str());
    }
    if url.contains(".blob.core.windows.net") && !has_azure_blob_type {
        request = request.header("x-ms-blob-type", HeaderValue::from_static("BlockBlob"));
    }

    let response = request
        .send()
        .await
        .context("signed-url upload request failed")?;
    if !response.status().is_success() {
        return Err(response_error(response).await.into());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn truncated_error_response_preserves_status_body_and_read_failure() {
        use std::io::{Read, Write};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            let (mut socket, _) = listener.accept().unwrap();
            let mut request = [0; 4096];
            assert!(socket.read(&mut request).unwrap() > 0);
            socket.write_all(b"HTTP/1.1 502 Bad Gateway\r\nContent-Length: 1000\r\nConnection: close\r\n\r\nupstream unavailable").unwrap();
            socket.flush().unwrap();
            std::thread::sleep(std::time::Duration::from_millis(50));
        });
        let response = build_http_client(DEFAULT_HTTP_TIMEOUT)
            .unwrap()
            .get(url)
            .send()
            .await
            .unwrap();
        let error = response_error(response).await;
        assert_eq!(error.status, StatusCode::BAD_GATEWAY);
        assert!(error.body.contains("upstream unavailable"), "{error}");
        assert!(
            error.body.contains("failed to read error response body"),
            "{error}"
        );
        server.join().unwrap();
    }

    use super::*;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
    use serde::Deserialize;
    use serde_json::json;

    type RecordedUserAgent = Arc<Mutex<Option<String>>>;

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    struct TestResponse {
        data: Vec<TestRow>,
    }

    #[allow(dead_code)]
    #[derive(Debug, Deserialize)]
    struct TestRow {
        id: String,
    }

    async fn record_user_agent(
        state: web::Data<RecordedUserAgent>,
        req: HttpRequest,
    ) -> HttpResponse {
        let user_agent = req
            .headers()
            .get("user-agent")
            .and_then(|value| value.to_str().ok())
            .map(str::to_string);
        *state.lock().expect("user agent lock") = user_agent;
        HttpResponse::Ok().finish()
    }

    async fn captured_user_agent(client: Client) -> Option<String> {
        let state = Arc::new(Mutex::new(None));
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind mock server");
        let addr = listener.local_addr().expect("mock server addr");
        let data = web::Data::new(state.clone());
        let server = HttpServer::new(move || {
            App::new()
                .app_data(data.clone())
                .route("/", web::get().to(record_user_agent))
        })
        .workers(1)
        .listen(listener)
        .expect("listen mock server")
        .run();
        let handle = server.handle();
        tokio::spawn(server);

        let response = client
            .get(format!("http://{addr}/"))
            .send()
            .await
            .expect("send request");
        assert!(response.status().is_success());
        handle.stop(true).await;

        let user_agent = state.lock().expect("user agent lock").clone();
        user_agent
    }

    #[tokio::test]
    async fn build_http_client_sets_default_user_agent() {
        let client = build_http_client(DEFAULT_HTTP_TIMEOUT).expect("build client");
        let user_agent = captured_user_agent(client).await;

        assert_eq!(user_agent.as_deref(), Some(BT_USER_AGENT));
    }

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

    #[test]
    fn parse_json_body_reports_path_and_reason() {
        let err = parse_json_body::<TestResponse>(
            br#"{"data":[{"id":1}]}"#,
            "POST",
            "/btql",
            StatusCode::OK,
            Some("application/json"),
        )
        .unwrap_err();
        let message = err.to_string();

        assert!(message.contains("failed to parse response from POST /btql"));
        assert!(message.contains("JSON path: data[0].id"));
        assert!(message.contains("invalid type: integer `1`, expected a string"));
        assert!(message.contains("status: 200 OK"));
        assert!(message.contains("content-type: application/json"));
    }

    #[test]
    fn parse_json_body_rejects_trailing_characters() {
        let err = parse_json_body::<BtqlResponse<serde_json::Value>>(
            br#"{"data":[]} trailing"#,
            "GET",
            "/btql",
            StatusCode::OK,
            Some("application/json"),
        )
        .unwrap_err();
        let message = err.to_string();

        assert!(message.contains("JSON path: <root>"));
        assert!(message.contains("trailing characters"));
    }
}
