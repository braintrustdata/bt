//! BTQL adds query options to the same HTTP request builder.
use super::{ApiClient, BtqlResponse, Request, RetryReason};
use anyhow::{bail, Result};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{Map, Value};
use std::collections::HashSet;
use std::future::Future;

#[derive(Serialize)]
struct QueryBody<'a, Q: ?Sized> {
    query: &'a Q,
    fmt: &'static str,
    #[serde(flatten)]
    options: Map<String, Value>,
}

pub struct BtqlQuery<'a, Q: ?Sized> {
    request: Request,
    body: QueryBody<'a, Q>,
}

impl ApiClient {
    /// Accepts either SQL/pipeline text or a structured BTQL query. `send`
    /// decodes the requested response type, just like an HTTP request.
    pub fn btql<'a, Q: Serialize + ?Sized>(&self, query: &'a Q) -> BtqlQuery<'a, Q> {
        BtqlQuery {
            request: self.post("/btql").retry(),
            body: QueryBody {
                query,
                fmt: "json",
                options: Map::new(),
            },
        }
    }
}

impl<Q: Serialize + ?Sized> BtqlQuery<'_, Q> {
    pub fn source(mut self, source: &str) -> Self {
        self.body
            .options
            .insert("query_source".into(), source.into());
        self
    }
    pub fn lint_mode(mut self, mode: &str) -> Self {
        self.body.options.insert("lint_mode".into(), mode.into());
        self.body
            .options
            .insert("strict_lint_mode".into(), (mode == "strict").into());
        self
    }
    pub fn realtime(mut self, enabled: bool) -> Self {
        self.body
            .options
            .insert("brainstore_realtime".into(), enabled.into());
        self
    }
    pub fn audit_log(mut self, enabled: bool) -> Self {
        self.body.options.insert("audit_log".into(), enabled.into());
        self
    }
    pub fn use_columnstore(mut self, enabled: bool) -> Self {
        self.body
            .options
            .insert("use_columnstore".into(), enabled.into());
        self
    }
    pub fn on_retry(mut self, observer: impl Fn(RetryReason) + Send + Sync + 'static) -> Self {
        self.request = self.request.on_retry(observer);
        self
    }
    pub fn trace(mut self, enabled: bool, label: &str) -> Self {
        self.request = self.request.trace(enabled, label);
        self
    }
    pub async fn send<T: DeserializeOwned>(self) -> Result<T> {
        if let Some(label) = &self.request.trace {
            eprintln!(
                "btql[{label}] query:\n{}\n",
                serde_json::to_string(self.body.query)?
            );
        }
        self.request.json(&self.body).send().await
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
}
impl Pagination {
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

    /// Collect bounded or unbounded pages. The callback owns only query
    /// construction; limits, cursor checks, truncation and termination are shared.
    pub async fn collect<T, F, Fut>(
        mut self,
        max_rows: Option<usize>,
        page_size: usize,
        mut fetch: F,
    ) -> Result<(Vec<T>, bool)>
    where
        F: FnMut(usize, Option<String>) -> Fut,
        Fut: Future<Output = Result<BtqlResponse<T>>>,
    {
        if page_size == 0 {
            bail!("page size must be greater than zero");
        }
        let mut rows = Vec::new();
        let mut cursor = None;
        loop {
            let limit = max_rows.map_or(page_size, |max| {
                max.saturating_sub(rows.len()).min(page_size)
            });
            if limit == 0 {
                return Ok((rows, false));
            }
            self.check(cursor.as_deref())?;
            let page = fetch(limit, cursor).await?;
            cursor = page.cursor;
            if page.data.is_empty() {
                if cursor.is_some() {
                    bail!("query returned an empty page with a cursor");
                }
                return Ok((rows, false));
            }
            let remaining = max_rows.map_or(usize::MAX, |max| max.saturating_sub(rows.len()));
            let overflow = page.data.len() > remaining;
            rows.extend(page.data.into_iter().take(remaining));
            if overflow || (max_rows.is_some_and(|max| rows.len() >= max) && cursor.is_some()) {
                return Ok((rows, true));
            }
            if cursor.is_none() {
                return Ok((rows, false));
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::{retry_delay, HttpError, MAX_ATTEMPTS, MAX_DELAY};
    use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
    use reqwest::StatusCode;
    use serde_json::json;
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
                .btql(&query)
                .source("test-source")
                .lint_mode("strict")
                .on_retry({
                    let reasons = Arc::clone(&reasons);
                    move |reason| reasons.lock().unwrap().push(reason)
                })
                .send()
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
                .btql("SELECT * FROM dataset('test-dataset') LIMIT 1")
                .send::<crate::http::BtqlResponse<Value>>()
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
                .btql("SELECT * FROM dataset('test-dataset') LIMIT 1")
                .send::<crate::http::BtqlResponse<Value>>()
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
            .btql("SELECT * FROM dataset('test-dataset') LIMIT 1")
            .send::<crate::http::BtqlResponse<Value>>()
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
            .get("/btql/async/test-id/result")
            .retry()
            .send()
            .await
            .unwrap();
        assert_eq!(result["rows"], json!([]));
        assert_eq!(state.requests.lock().unwrap().len(), 2);
        handle.stop(false).await;

        for path in ["/v1/dataset", "/btql/async"] {
            let (client, state, handle) = mock(vec![(503, "test write error")]).await;
            let result: Result<Value> = client.post(path).json(&json!({})).send().await;
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
        };
        let reasons = Arc::new(Mutex::new(Vec::new()));
        let response: Value = client
            .btql("SELECT * FROM dataset('test-dataset') LIMIT 1")
            .on_retry({
                let reasons = Arc::clone(&reasons);
                move |reason| reasons.lock().unwrap().push(reason)
            })
            .send()
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
            .btql(&body["query"])
            .audit_log(true)
            .use_columnstore(false)
            .realtime(true)
            .send()
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
            .get("/test/http")
            .query(&[("name", "test project/&")])
            .header("x-test-header", "test-value")
            .timeout(Duration::from_secs(1))
            .send()
            .await
            .unwrap();
        assert_eq!(result["ok"], true);
        let response = client.get("/test/response").response().await.unwrap();
        assert_eq!(
            response.headers().get("x-test-response").unwrap(),
            "test-value"
        );
        assert_eq!(response.json::<Value>().await.unwrap()["ok"], true);
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
    async fn invalid_request_builders_fail_before_network_or_retries() {
        let (client, state, handle) = mock(vec![]).await;
        let error = client
            .get("/test/invalid")
            .header("x-test", "invalid\nvalue")
            .retry()
            .send::<Value>()
            .await
            .unwrap_err();
        assert!(error.downcast_ref::<reqwest::Error>().unwrap().is_builder());
        assert!(state.requests.lock().unwrap().is_empty());
        handle.stop(false).await;
    }
}
