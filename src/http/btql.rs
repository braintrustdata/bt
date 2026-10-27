//! BTQL execution and pagination through the shared HTTP transport.
use super::{ApiClient, BtqlResponse};
use anyhow::{bail, Result};
use futures_util::{stream, Stream, TryStreamExt};
use serde::{de::DeserializeOwned, Serialize};
use std::collections::HashSet;
use std::future::Future;

#[derive(Serialize)]
struct QueryBody<'a, Q: ?Sized> {
    query: &'a Q,
    fmt: &'static str,
    #[serde(flatten)]
    options: BtqlOptions<'a>,
    #[serde(skip_serializing_if = "Option::is_none")]
    strict_lint_mode: Option<bool>,
}

/// Optional BTQL execution settings. Omitted fields preserve server defaults.
#[derive(Default, Serialize)]
pub struct BtqlOptions<'a> {
    #[serde(skip_serializing_if = "Option::is_none", rename = "query_source")]
    pub source: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lint_mode: Option<&'a str>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        rename = "brainstore_realtime"
    )]
    pub realtime: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub audit_log: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub use_columnstore: Option<bool>,
}

impl ApiClient {
    /// Execute SQL/pipeline text or a structured BTQL query with default settings.
    pub async fn btql<T: DeserializeOwned>(&self, query: &(impl Serialize + ?Sized)) -> Result<T> {
        self.btql_with_options(query, BtqlOptions::default()).await
    }

    /// Execute BTQL with explicit settings through the shared HTTP transport.
    pub async fn btql_with_options<T: DeserializeOwned>(
        &self,
        query: &(impl Serialize + ?Sized),
        options: BtqlOptions<'_>,
    ) -> Result<T> {
        let strict_lint_mode = options.lint_mode.map(|mode| mode == "strict");
        if let Some(label) = &self.options.trace {
            eprintln!("btql[{label}] query:\n{}\n", serde_json::to_string(query)?);
        }
        self.with_options(super::HttpOptions {
            retry: Some(self.options.retry.unwrap_or(true)),
            ..Default::default()
        })
        .post(
            "/btql",
            &QueryBody {
                query,
                fmt: "json",
                options,
                strict_lint_mode,
            },
        )
        .await
    }
}

pub(crate) fn deserialize_cursor<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    use serde::Deserialize;
    Ok(Option::<String>::deserialize(deserializer)?.filter(|cursor| !cursor.is_empty()))
}

/// Cursor validation is also usable by streaming consumers that checkpoint
/// individual pages rather than collecting them in memory.
#[derive(Default)]
pub struct Pagination {
    cursors: HashSet<String>,
    pages: usize,
    max_pages: Option<usize>,
    initial_cursor: Option<String>,
}
impl Pagination {
    /// Resume fetching after a durably checkpointed cursor.
    pub fn from_cursor(cursor: Option<String>) -> Self {
        Self {
            initial_cursor: cursor,
            ..Self::default()
        }
    }

    pub fn with_max_pages(max_pages: usize) -> Self {
        Self {
            max_pages: Some(max_pages),
            ..Self::default()
        }
    }
    pub fn check(&mut self, cursor: Option<&str>) -> Result<()> {
        self.pages += 1;
        if self.max_pages.is_some_and(|max| self.pages > max) {
            bail!(
                "query pagination exceeded {} pages",
                self.max_pages.unwrap_or_default()
            );
        }
        if let Some(cursor) = cursor.filter(|cursor| !cursor.is_empty()) {
            if !self.cursors.insert(cursor.to_owned()) {
                bail!("query pagination loop detected: repeated cursor");
            }
        }
        Ok(())
    }

    /// Lazily fetch pages, preserving cursors and schema for page-level consumers.
    /// Dropping the stream stops pagination; no requests run ahead of the reader.
    pub fn pages<T, F, Fut>(
        mut self,
        max_rows: Option<usize>,
        page_size: usize,
        fetch: F,
    ) -> impl Stream<Item = Result<Page<T>>>
    where
        F: FnMut(usize, Option<String>) -> Fut,
        Fut: Future<Output = Result<BtqlResponse<T>>>,
    {
        let cursor = self.initial_cursor.take();
        stream::try_unfold(
            (self, fetch, cursor, 0usize, false),
            move |(mut pagination, mut fetch, cursor, count, done)| async move {
                if page_size == 0 {
                    bail!("page size must be greater than zero");
                }
                let remaining = max_rows.map_or(usize::MAX, |max| max.saturating_sub(count));
                if done || remaining == 0 {
                    return Ok(None);
                }
                pagination.check(cursor.as_deref())?;
                let mut response = fetch(remaining.min(page_size), cursor).await?;
                if response.data.is_empty() && response.cursor.is_some() {
                    bail!("query returned an empty page with a cursor");
                }
                let truncated = response.data.len() > remaining
                    || (response.data.len() == remaining && response.cursor.is_some());
                response.data.truncate(remaining);
                let count = count + response.data.len();
                let cursor = response.cursor.clone();
                let done = truncated || cursor.is_none();
                Ok(Some((
                    Page {
                        response,
                        truncated,
                    },
                    (pagination, fetch, cursor, count, done),
                )))
            },
        )
    }

    /// Convenience for callers that need all rows and a truncation indicator.
    pub async fn collect<T, F, Fut>(
        self,
        max_rows: Option<usize>,
        page_size: usize,
        fetch: F,
    ) -> Result<(Vec<T>, bool)>
    where
        F: FnMut(usize, Option<String>) -> Fut,
        Fut: Future<Output = Result<BtqlResponse<T>>>,
    {
        self.pages(max_rows, page_size, fetch)
            .try_fold((Vec::new(), false), |(mut rows, _), page| async move {
                rows.extend(page.response.data);
                Ok((rows, page.truncated))
            })
            .await
    }
}

/// One fetched page and whether a requested row bound cut the result short.
/// `response.data` is a Vec, so use `.get(index)` for checked positional access.
pub struct Page<T> {
    pub response: BtqlResponse<T>,
    pub truncated: bool,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{
        retry_delay, HttpError, HttpOptions, NoContent, RetryReason, Text, WithHeaders,
        MAX_ATTEMPTS, MAX_DELAY,
    };
    use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
    use reqwest::StatusCode;
    use serde_json::{json, Value};
    use std::collections::VecDeque;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    struct MockState {
        replies: Mutex<VecDeque<(u16, &'static str)>>,
        requests: Mutex<Vec<(String, Value)>>,
    }

    async fn reply(
        state: web::Data<Arc<MockState>>,
        req: HttpRequest,
        body: web::Bytes,
    ) -> HttpResponse {
        assert_eq!(
            req.headers().get("authorization").unwrap(),
            "Bearer test-key"
        );
        assert_eq!(req.headers().get("x-bt-org-name").unwrap(), "test-org");
        if req.path() == "/test/http" {
            assert_eq!(req.headers().get("x-test-header").unwrap(), "test-value");
            assert_eq!(req.query_string(), "name=test+project%2F%26");
        }
        state.requests.lock().unwrap().push((
            req.method().to_string(),
            if body.is_empty() {
                Value::Null
            } else {
                serde_json::from_slice(&body).unwrap()
            },
        ));
        if req.path() == "/test/scoped" || req.headers().contains_key("x-test-option") {
            assert_eq!(req.headers().get("x-test-option").unwrap(), "test-value");
            assert_eq!(req.query_string(), "test-name=test+value");
        }
        if req.path() == "/test/unconfigured" {
            assert!(!req.headers().contains_key("x-test-option"));
            assert!(req.query_string().is_empty());
        }
        if req.headers().contains_key("x-test-delay") {
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
        let (status, body) = state
            .replies
            .lock()
            .unwrap()
            .pop_front()
            .expect("unexpected extra request");
        HttpResponse::build(actix_web::http::StatusCode::from_u16(status).unwrap())
            .insert_header(("retry-after", "0"))
            .insert_header(("content-type", "application/json"))
            .insert_header(("x-test-response", "test-value"))
            .body(body)
    }

    async fn mock(
        replies: Vec<(u16, &'static str)>,
    ) -> (ApiClient, Arc<MockState>, actix_web::dev::ServerHandle) {
        let state = Arc::new(MockState {
            replies: Mutex::new(replies.into()),
            requests: Mutex::new(Vec::new()),
        });
        let data = web::Data::new(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = HttpServer::new(move || {
            App::new()
                .app_data(data.clone())
                .default_service(web::to(reply))
        })
        .workers(1)
        .listen(listener)
        .unwrap()
        .run();
        let handle = server.handle();
        tokio::spawn(server);
        let client = ApiClient {
            http: super::super::build_http_client(Duration::from_secs(2)).unwrap(),
            base_url,
            api_key: "test-key".into(),
            org_id: "test-org-id".into(),
            org_name: "test-org".into(),
            options: HttpOptions::default(),
        };
        (client, state, handle)
    }

    #[tokio::test]
    async fn retries_transient_statuses_with_the_same_body_and_org() {
        for status in [413, 429, 500, 503] {
            let (client, state, handle) = mock(vec![
                (status, "test transient error"),
                (200, "{\"data\":[]}"),
            ])
            .await;
            let reasons = Arc::new(Mutex::new(Vec::new()));
            let query = json!({"from": {"op":"function","name":"dataset","args":["test-dataset"]}});
            let result: super::super::BtqlResponse<Value> = client
                .with_options(HttpOptions {
                    on_retry: Some(Arc::new({
                        let reasons = Arc::clone(&reasons);
                        move |reason| reasons.lock().unwrap().push(reason)
                    })),
                    ..Default::default()
                })
                .btql_with_options(
                    &query,
                    BtqlOptions {
                        source: Some("test-source"),
                        lint_mode: Some("strict"),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
            assert!(result.data.is_empty());
            let requests = state.requests.lock().unwrap().clone();
            assert_eq!(requests.len(), 2);
            assert_eq!(requests[0], requests[1]);
            assert_eq!(requests[0].1["query"], query);
            assert_eq!(requests[0].1["query_source"], "test-source");
            assert_eq!(requests[0].1["strict_lint_mode"], true);
            assert_eq!(*reasons.lock().unwrap(), vec![RetryReason::Status(status)]);
            handle.stop(false).await;
        }
    }

    #[tokio::test]
    async fn permanent_errors_and_invalid_json_are_not_retried() {
        for status in [400, 401, 403, 404] {
            let (client, state, handle) = mock(vec![(status, "test permanent error")]).await;
            let error = client
                .btql::<crate::http::BtqlResponse<Value>>(
                    "SELECT * FROM dataset('test-dataset') LIMIT 1",
                )
                .await
                .unwrap_err();
            assert_eq!(
                error.downcast_ref::<HttpError>().unwrap().status.as_u16(),
                status
            );
            assert!(error.to_string().contains("test permanent error"));
            assert_eq!(state.requests.lock().unwrap().len(), 1);
            handle.stop(false).await;
        }
        for body in ["{bad json", "{\"data\":false}"] {
            let (client, state, handle) = mock(vec![(200, body)]).await;
            let error = client
                .btql::<crate::http::BtqlResponse<Value>>(
                    "SELECT * FROM dataset('test-dataset') LIMIT 1",
                )
                .await
                .unwrap_err();
            assert!(error
                .downcast_ref::<super::super::ResponseParseError>()
                .is_some());
            assert_eq!(state.requests.lock().unwrap().len(), 1);
            handle.stop(false).await;
        }
    }

    #[tokio::test]
    async fn retries_are_bounded_and_keep_the_final_typed_error() {
        let (client, state, handle) = mock(vec![(503, "test exhausted error"); MAX_ATTEMPTS]).await;
        let error = client
            .btql::<crate::http::BtqlResponse<Value>>(
                "SELECT * FROM dataset('test-dataset') LIMIT 1",
            )
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<HttpError>().unwrap().status,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert!(error.to_string().contains("after 5 attempt(s)"));
        assert!(error.to_string().contains("test exhausted error"));
        assert_eq!(state.requests.lock().unwrap().len(), MAX_ATTEMPTS);
        handle.stop(false).await;
    }

    #[tokio::test]
    async fn async_result_gets_retry_but_api_writes_do_not() {
        let (client, state, handle) = mock(vec![(503, "test retry"), (200, "{\"rows\":[]}")]).await;
        let result: Value = client
            .with_options(HttpOptions {
                retry: Some(true),
                ..Default::default()
            })
            .get("/btql/async/test-id/result")
            .await
            .unwrap();
        assert_eq!(result["rows"], json!([]));
        assert_eq!(state.requests.lock().unwrap().len(), 2);
        handle.stop(false).await;

        for path in ["/v1/dataset", "/btql/async"] {
            let (client, state, handle) = mock(vec![(503, "test write error")]).await;
            let result: Result<Value> = client.post(path, &json!({})).await;
            assert!(result.is_err());
            assert_eq!(state.requests.lock().unwrap().len(), 1);
            handle.stop(false).await;
        }
    }

    #[tokio::test]
    async fn retries_a_dropped_query_connection() {
        use std::io::{Read, Write};
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let base_url = format!("http://{}", listener.local_addr().unwrap());
        let server = std::thread::spawn(move || {
            for attempt in 0..2 {
                let (mut socket, _) = listener.accept().unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = [0; 4096];
                assert!(socket.read(&mut request).unwrap() > 0);
                if attempt == 1 {
                    let body = "{\"data\":[]}";
                    write!(
                        socket,
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                        body.len()
                    )
                    .unwrap();
                }
            }
        });
        let client = ApiClient {
            http: super::super::build_http_client(Duration::from_secs(2)).unwrap(),
            base_url,
            api_key: "test-key".into(),
            org_id: "test-org-id".into(),
            org_name: "test-org".into(),
            options: HttpOptions::default(),
        };
        let reasons = Arc::new(Mutex::new(Vec::new()));
        let response: Value = client
            .with_options(HttpOptions {
                on_retry: Some(Arc::new({
                    let reasons = Arc::clone(&reasons);
                    move |reason| reasons.lock().unwrap().push(reason)
                })),
                ..Default::default()
            })
            .btql("SELECT * FROM dataset('test-dataset') LIMIT 1")
            .await
            .unwrap();
        assert_eq!(response["data"], json!([]));
        assert_eq!(*reasons.lock().unwrap(), vec![RetryReason::Network]);
        server.join().unwrap();
    }

    #[test]
    fn pagination_detects_cycles_and_preserves_optional_page_limits() {
        let mut pagination = Pagination::default();
        pagination.check(None).unwrap();
        pagination.check(Some("test-cursor-a")).unwrap();
        pagination.check(Some("test-cursor-b")).unwrap();
        assert!(pagination.check(Some("test-cursor-a")).is_err());
        let mut bounded = Pagination::with_max_pages(1);
        bounded.check(None).unwrap();
        assert!(bounded
            .check(Some("test-cursor"))
            .unwrap_err()
            .to_string()
            .contains("exceeded 1 pages"));
    }
    #[test]
    fn empty_and_null_cursors_end_pagination() {
        for value in [
            json!({"data":[]}),
            json!({"data":[],"cursor":null}),
            json!({"data":[],"cursor":""}),
        ] {
            let response: super::super::BtqlResponse<Value> =
                serde_json::from_value(value).unwrap();
            assert_eq!(response.cursor, None);
        }
    }
    #[tokio::test]
    async fn endpoint_specific_query_options_survive_retries() {
        let (client, state, handle) = mock(vec![(503, "test retry"), (200, "{\"data\":[]}")]).await;
        let body = json!({"query": "SELECT * FROM project_prompts('test-project') LIMIT 1", "fmt":"json", "audit_log":true, "use_columnstore":false, "brainstore_realtime":true});
        let _: Value = client
            .btql_with_options(
                &body["query"],
                BtqlOptions {
                    audit_log: Some(true),
                    use_columnstore: Some(false),
                    realtime: Some(true),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        let requests = state.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].1, body);
        assert_eq!(requests[1].1, body);
        handle.stop(false).await;
    }
    #[test]
    fn server_retry_after_is_not_shortened_by_the_local_backoff_cap() {
        let mut backoff = backoff::backoff::Constant::new(Duration::from_secs(20));
        assert_eq!(
            retry_delay(Some(Duration::from_secs(60)), &mut backoff),
            Duration::from_secs(60)
        );
        assert_eq!(
            retry_delay(Some(Duration::ZERO), &mut backoff),
            Duration::ZERO
        );
        assert_eq!(retry_delay(None, &mut backoff), MAX_DELAY);
    }
    #[tokio::test]
    async fn http_requests_share_auth_query_encoding_headers_and_response_decoding() {
        let (client, state, handle) =
            mock(vec![(200, "{\"ok\":true}"), (200, "{\"ok\":true}")]).await;
        let result: Value = client
            .with_options(HttpOptions {
                query: vec![("name".into(), "test project/&".into())],
                headers: vec![("x-test-header".into(), "test-value".into())],
                timeout: Some(Duration::from_secs(1)),
                ..Default::default()
            })
            .get("/test/http")
            .await
            .unwrap();
        assert_eq!(result["ok"], true);
        let response: WithHeaders<Value> = client.get("/test/response").await.unwrap();
        assert_eq!(
            response.headers.get("x-test-response").unwrap(),
            "test-value"
        );
        assert_eq!(response.body["ok"], true);
        assert_eq!(state.requests.lock().unwrap().len(), 2);
        handle.stop(false).await;
    }

    fn page(data: Vec<usize>, cursor: Option<&str>) -> BtqlResponse<usize> {
        BtqlResponse {
            data,
            cursor: cursor.map(str::to_owned),
            schema: None,
        }
    }

    #[tokio::test]
    async fn collector_follows_server_cursors_even_for_short_pages() {
        let mut pages = VecDeque::from([page(vec![1], Some("test-cursor")), page(vec![2], None)]);
        let mut calls = Vec::new();
        let (rows, truncated) = Pagination::default()
            .collect(Some(3), 2, |limit, cursor| {
                calls.push((limit, cursor));
                std::future::ready(Ok(pages.pop_front().unwrap()))
            })
            .await
            .unwrap();
        assert_eq!(rows, vec![1, 2]);
        assert!(!truncated);
        assert_eq!(calls, vec![(2, None), (2, Some("test-cursor".into()))]);
    }

    #[tokio::test]
    async fn collector_caps_requests_and_output_and_reports_truncation() {
        let mut pages = VecDeque::from([
            page(vec![1, 2], Some("test-cursor-a")),
            page(vec![3, 4], Some("test-cursor-b")),
        ]);
        let mut calls = Vec::new();
        let (rows, truncated) = Pagination::default()
            .collect(Some(3), 2, |limit, cursor| {
                calls.push((limit, cursor));
                std::future::ready(Ok(pages.pop_front().unwrap()))
            })
            .await
            .unwrap();
        assert_eq!(rows, vec![1, 2, 3]);
        assert!(truncated);
        assert_eq!(calls, vec![(2, None), (1, Some("test-cursor-a".into()))]);
    }

    #[tokio::test]
    async fn collector_stops_cycles_and_rejects_pages_without_progress() {
        let mut calls = 0;
        let error = Pagination::default()
            .collect(None, 1, |_, _| {
                calls += 1;
                std::future::ready(Ok(page(vec![1], Some("test-cursor"))))
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("pagination loop detected"));
        assert_eq!(calls, 2);
        let error = Pagination::default()
            .collect(None, 1, |_, _| {
                std::future::ready(Ok(page(vec![], Some("test-cursor"))))
            })
            .await
            .unwrap_err();
        assert!(error.to_string().contains("empty page with a cursor"));
        let (rows, truncated) = Pagination::default()
            .collect(Some(0), 1, |_, _| {
                std::future::ready(Err::<BtqlResponse<usize>, _>(anyhow::anyhow!(
                    "zero-limit collection must not make a request"
                )))
            })
            .await
            .unwrap();
        assert!(rows.is_empty());
        assert!(!truncated);
    }
    #[tokio::test]
    async fn page_stream_is_lazy_resumes_and_preserves_page_metadata() {
        let calls = std::cell::Cell::new(0);
        {
            let pages = Pagination::from_cursor(Some("test-resume".into())).pages(
                None,
                2,
                |limit, cursor| {
                    calls.set(calls.get() + 1);
                    assert_eq!(limit, 2);
                    assert_eq!(cursor.as_deref(), Some("test-resume"));
                    std::future::ready(Ok(BtqlResponse {
                        data: vec![1, 2],
                        cursor: Some("test-next".into()),
                        schema: Some(json!({"test-column": "number"})),
                    }))
                },
            );
            assert_eq!(calls.get(), 0);
            futures_util::pin_mut!(pages);
            let page = pages.try_next().await.unwrap().unwrap();
            assert_eq!(page.response.data, vec![1, 2]);
            assert_eq!(page.response.cursor.as_deref(), Some("test-next"));
            assert_eq!(page.response.schema, Some(json!({"test-column": "number"})));
            assert!(!page.truncated);
            // Dropping here must not fetch the next page.
        }
        assert_eq!(calls.get(), 1);
    }

    #[tokio::test]
    async fn page_stream_can_flatten_rows_and_propagates_fetch_errors_once() {
        let mut replies = VecDeque::from([
            Ok(page(vec![1, 2], Some("test-next"))),
            Err(anyhow::anyhow!("test page failure")),
        ]);
        let pages = Pagination::default().pages(None, 2, |_, _| {
            std::future::ready(replies.pop_front().unwrap())
        });
        let rows = pages
            .map_ok(|page| stream::iter(page.response.data.into_iter().map(Ok::<_, anyhow::Error>)))
            .try_flatten();
        futures_util::pin_mut!(rows);
        assert_eq!(rows.try_next().await.unwrap(), Some(1));
        assert_eq!(rows.try_next().await.unwrap(), Some(2));
        assert_eq!(
            rows.try_next().await.unwrap_err().to_string(),
            "test page failure"
        );
        assert!(rows.try_next().await.unwrap().is_none());
    }

    #[tokio::test]
    async fn default_btql_omits_optional_settings_and_http_shortcuts_share_transport() {
        let (client, state, handle) = mock(vec![
            (200, "{}"),
            (200, "{}"),
            (200, "{}"),
            (200, "{}"),
            (204, ""),
            (200, "{\"deleted\":true}"),
            (200, "{\"data\":[]}"),
        ])
        .await;
        let body = json!({"name": "test-object"});
        let _: Value = client.get("/test/get").await.unwrap();
        let _: Value = client.post("/test/post", &body).await.unwrap();
        let _: Value = client.put("/test/put", &body).await.unwrap();
        let _: Value = client.patch("/test/patch", &body).await.unwrap();
        let _: NoContent = client.delete("/test/delete").await.unwrap();
        let deleted: Value = client.delete("/test/delete-json").await.unwrap();
        assert_eq!(deleted["deleted"], true);
        let _: BtqlResponse<Value> = client
            .btql("SELECT * FROM dataset('test-dataset') LIMIT 1")
            .await
            .unwrap();
        let requests = state.requests.lock().unwrap().clone();
        assert_eq!(
            requests
                .iter()
                .map(|request| request.0.as_str())
                .collect::<Vec<_>>(),
            vec!["GET", "POST", "PUT", "PATCH", "DELETE", "DELETE", "POST"]
        );
        for request in &requests[1..4] {
            assert_eq!(request.1, body);
        }
        assert_eq!(
            requests[6].1,
            json!({"query": "SELECT * FROM dataset('test-dataset') LIMIT 1", "fmt": "json"})
        );
        handle.stop(false).await;
    }

    #[tokio::test]
    async fn scoped_options_can_be_reused_without_affecting_the_original_client() {
        let (client, state, handle) = mock(vec![
            (200, "{}"),
            (200, "test text"),
            (204, ""),
            (200, "{\"data\":[]}"),
            (200, "{}"),
        ])
        .await;
        let configured = client.with_options(HttpOptions {
            headers: vec![("x-test-option".into(), "test-value".into())],
            query: vec![("test-name".into(), "test value".into())],
            ..Default::default()
        });
        let _: Value = configured.get("/test/scoped").await.unwrap();
        let text: WithHeaders<Text> = configured.post("/test/scoped", &json!({})).await.unwrap();
        assert_eq!(text.body.0, "test text");
        assert_eq!(text.headers.get("x-test-response").unwrap(), "test-value");
        let _: NoContent = configured.post("/test/scoped", &json!({})).await.unwrap();
        let _: BtqlResponse<Value> = configured
            .btql("SELECT * FROM dataset('test-dataset') LIMIT 1")
            .await
            .unwrap();
        let _: Value = client.get("/test/unconfigured").await.unwrap();
        assert_eq!(state.requests.lock().unwrap().len(), 5);
        handle.stop(false).await;
    }

    #[tokio::test]
    async fn http_options_can_disable_btql_retries() {
        let (client, state, handle) = mock(vec![(503, "test transient error")]).await;
        let error = client
            .with_options(HttpOptions {
                retry: Some(false),
                ..Default::default()
            })
            .btql::<Value>("SELECT * FROM dataset('test-dataset') LIMIT 1")
            .await
            .unwrap_err();
        assert_eq!(
            error.downcast_ref::<HttpError>().unwrap().status,
            StatusCode::SERVICE_UNAVAILABLE
        );
        assert_eq!(state.requests.lock().unwrap().len(), 1);
        handle.stop(false).await;
    }

    #[tokio::test]
    async fn timeout_options_apply_to_both_http_and_btql() {
        for btql in [false, true] {
            let (client, _, handle) = mock(vec![(200, "{}")]).await;
            let configured = client
                .with_options(HttpOptions {
                    timeout: Some(Duration::from_millis(50)),
                    retry: Some(false),
                    ..Default::default()
                })
                .with_options(HttpOptions {
                    headers: vec![("x-test-delay".into(), "true".into())],
                    ..Default::default()
                });
            let result: Result<Value> = if btql {
                configured
                    .btql("SELECT * FROM dataset('test-dataset') LIMIT 1")
                    .await
            } else {
                configured.get("/test/timeout").await
            };
            assert!(result
                .unwrap_err()
                .downcast_ref::<reqwest::Error>()
                .unwrap()
                .is_timeout());
            handle.stop(false).await;
        }
    }

    #[tokio::test]
    async fn invalid_http_options_fail_before_network_or_retries() {
        let (client, state, handle) = mock(vec![]).await;
        let error = client
            .with_options(HttpOptions {
                headers: vec![("x-test".into(), "invalid\nvalue".into())],
                retry: Some(true),
                ..Default::default()
            })
            .get::<Value>("/test/invalid")
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<reqwest::Error>().unwrap().is_builder());
        assert!(state.requests.lock().unwrap().is_empty());
        handle.stop(false).await;
    }
}
