//! Shared BTQL request policy. Queries are safe to retry; ordinary API writes
//! and async-query submission deliberately keep their single-attempt behavior.
use std::collections::HashSet;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use backoff::{backoff::Backoff, ExponentialBackoffBuilder};
use reqwest::{Method, StatusCode};
use serde::{de::DeserializeOwned, Serialize};
use serde_json::{json, Value};

use super::{parse_json_response, response_error, ApiClient, HttpError};

pub const SQL_QUERY_SOURCE: &str = "bt_sql_9f4b1e6d7c2a4a7b8d4f9a6c2b1e7f3d";
pub const TRACES_QUERY_SOURCE: &str = "bt_traces_9f4b1e6d7c2a4a7b8d4f9a6c2b1e7f3d";
pub const SYNC_QUERY_SOURCE: &str = "bt_sync_9f4b1e6d7c2a4a7b8d4f9a6c2b1e7f3d";
const MAX_ATTEMPTS: usize = 5;
const MAX_DELAY: Duration = Duration::from_secs(8);

#[derive(Clone, Copy, Default)]
pub struct QueryOptions<'a> {
    pub source: Option<&'a str>,
    pub lint_mode: Option<&'a str>,
}

pub fn query_body<Q: Serialize + ?Sized>(query: &Q, options: QueryOptions<'_>) -> Value {
    let mut body = json!({"query": query, "fmt": "json"});
    if let Some(source) = options.source {
        body["query_source"] = json!(source);
    }
    if let Some(mode) = options.lint_mode {
        body["lint_mode"] = json!(mode);
        body["strict_lint_mode"] = json!(mode == "strict");
    }
    body
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RetryReason {
    Status(u16),
    Network,
}

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

impl ApiClient {
    pub fn org_headers(&self) -> Vec<(&'static str, &str)> {
        if self.org_name().is_empty() {
            Vec::new()
        } else {
            vec![("x-bt-org-name", self.org_name())]
        }
    }

    pub async fn query<T, Q>(&self, query: &Q, options: QueryOptions<'_>) -> Result<T>
    where
        T: DeserializeOwned,
        Q: Serialize + ?Sized,
    {
        self.query_with_retry_observer(query, options, |_| {}).await
    }

    pub async fn query_with_retry_observer<T, Q>(
        &self,
        query: &Q,
        options: QueryOptions<'_>,
        on_retry: impl Fn(RetryReason),
    ) -> Result<T>
    where
        T: DeserializeOwned,
        Q: Serialize + ?Sized,
    {
        self.btql_request(
            Method::POST,
            "/btql",
            Some(&query_body(query, options)),
            on_retry,
        )
        .await
    }

    /// Send endpoint-specific BTQL options through the same retry/header policy.
    pub async fn query_with_body<T: DeserializeOwned, B: Serialize>(&self, body: &B) -> Result<T> {
        self.btql_request(
            Method::POST,
            "/btql",
            Some(&serde_json::to_value(body)?),
            |_| {},
        )
        .await
    }

    pub async fn get_query_result<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        self.btql_request(Method::GET, path, None, |_| {}).await
    }

    async fn btql_request<T: DeserializeOwned>(
        &self,
        method: Method,
        path: &str,
        body: Option<&Value>,
        on_retry: impl Fn(RetryReason),
    ) -> Result<T> {
        let mut backoff = ExponentialBackoffBuilder::new()
            .with_initial_interval(Duration::from_millis(300))
            .with_multiplier(2.0)
            .with_randomization_factor(0.2)
            .with_max_interval(MAX_DELAY)
            .with_max_elapsed_time(None)
            .build();
        for attempt in 1..=MAX_ATTEMPTS {
            let mut retry_after = None;
            let result = async {
                let response = self
                    .send_request(method.clone(), path, body, &self.org_headers(), None)
                    .await?;
                retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .map(Duration::from_secs);
                if !response.status().is_success() {
                    return Err(response_error(response).await.into());
                }
                parse_json_response(response, method.as_str(), path).await
            }
            .await;
            match result {
                Ok(value) => return Ok(value),
                Err(error) => {
                    let reason = retry_reason(&error);
                    if attempt == MAX_ATTEMPTS || reason.is_none() {
                        let message =
                            format!("BTQL request failed after {attempt} attempt(s): {error}");
                        return Err(error).context(message);
                    }
                    if let Some(reason) = reason {
                        on_retry(reason);
                    }
                    let delay =
                        retry_after.unwrap_or_else(|| backoff.next_backoff().unwrap_or(MAX_DELAY));
                    tokio::time::sleep(delay.min(MAX_DELAY)).await;
                }
            }
        }
        unreachable!("BTQL attempts always return a result")
    }
}

pub(crate) fn deserialize_cursor<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> std::result::Result<Option<String>, D::Error> {
    use serde::Deserialize;
    Ok(Option::<String>::deserialize(deserializer)?.filter(|cursor| !cursor.is_empty()))
}

/// Guard paginated reads against repeated cursors and optional page limits.
/// Call before each page request, including the first page of a resumed read.
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
        if let Some(max_pages) = self.max_pages {
            if self.pages > max_pages {
                bail!("query pagination exceeded {max_pages} pages");
            }
        }
        if let Some(cursor) = cursor.filter(|cursor| !cursor.is_empty()) {
            if !self.cursors.insert(cursor.to_owned()) {
                bail!("query pagination loop detected: repeated cursor");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
    use std::collections::VecDeque;
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

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
        if req.path().starts_with("/btql") {
            assert_eq!(req.headers().get("x-bt-org-name").unwrap(), "test-org");
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
            let reasons = Mutex::new(Vec::new());
            let query = json!({"from": {"op":"function","name":"dataset","args":["test-dataset"]}});
            let result: super::super::BtqlResponse<Value> = client
                .query_with_retry_observer(
                    &query,
                    QueryOptions {
                        source: Some("test-source"),
                        lint_mode: Some("strict"),
                    },
                    |reason| reasons.lock().unwrap().push(reason),
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
                .btql::<Value>("SELECT * FROM dataset('test-dataset') LIMIT 1")
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
                .btql::<Value>("SELECT * FROM dataset('test-dataset') LIMIT 1")
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
            .btql::<Value>("SELECT * FROM dataset('test-dataset') LIMIT 1")
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
            .get_query_result("/btql/async/test-id/result")
            .await
            .unwrap();
        assert_eq!(result["rows"], json!([]));
        assert_eq!(state.requests.lock().unwrap().len(), 2);
        handle.stop(false).await;

        for path in ["/v1/dataset", "/btql/async"] {
            let (client, state, handle) = mock(vec![(503, "test write error")]).await;
            let result: Result<Value> = client
                .post_with_headers(path, &json!({}), &client.org_headers())
                .await;
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
        let reasons = Mutex::new(Vec::new());
        let response: Value = client
            .query_with_retry_observer(
                "SELECT * FROM dataset('test-dataset') LIMIT 1",
                QueryOptions::default(),
                |reason| reasons.lock().unwrap().push(reason),
            )
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
        let _: Value = client.query_with_body(&body).await.unwrap();
        let requests = state.requests.lock().unwrap().clone();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].1, body);
        assert_eq!(requests[1].1, body);
        handle.stop(false).await;
    }
}
