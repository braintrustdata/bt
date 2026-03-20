use actix_web::dev::Service;
use actix_web::http::header::{
    HeaderName, HeaderValue, ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS,
    ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS,
    ACCESS_CONTROL_MAX_AGE, AUTHORIZATION, CACHE_CONTROL, CONNECTION, CONTENT_TYPE, ORIGIN, VARY,
};
use actix_web::{guard, web, App, HttpRequest, HttpResponse, HttpServer};
use anyhow::{Context, Result};
use futures_util::stream;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::mpsc;

use crate::args::BaseArgs;

use super::events::{EvalEvent, EvalProgressData, ExperimentSummary, SseProgressEventData};
use super::{
    detect_eval_language, drive_eval_runner, spawn_eval_runner, ConsolePolicy, EvalLanguage,
    EvalRunOptions, JsMode,
};

const MAIN_ORIGIN: &str = "https://www.braintrust.dev";
const BRAINTRUSTDATA_ORIGIN: &str = "https://www.braintrustdata.com";
const CORS_METHODS: &str = "GET, PATCH, POST, PUT, DELETE, OPTIONS";
const CORS_ALLOWED_HEADERS: &str = "Content-Type, X-Amz-Date, Authorization, X-Api-Key, X-Amz-Security-Token, x-bt-auth-token, x-bt-parent, x-bt-org-name, x-bt-project-id, x-bt-stream-fmt, x-bt-use-cache, x-bt-use-gateway, x-stainless-os, x-stainless-lang, x-stainless-package-version, x-stainless-runtime, x-stainless-runtime-version, x-stainless-arch";
const CORS_EXPOSED_HEADERS: &str =
    "x-bt-cursor, x-bt-found-existing-experiment, x-bt-span-id, x-bt-span-export";
const HEADER_BT_AUTH_TOKEN: &str = "x-bt-auth-token";
const HEADER_BT_ORG_NAME: &str = "x-bt-org-name";
const HEADER_CORS_REQ_PRIVATE_NETWORK: &str = "access-control-request-private-network";
const HEADER_CORS_ALLOW_PRIVATE_NETWORK: &str = "access-control-allow-private-network";

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EvalRequest {
    name: String,
    #[serde(default)]
    parameters: Option<Value>,
    data: Value,
    #[serde(default)]
    scores: Option<Vec<EvalScore>>,
    #[serde(default)]
    experiment_name: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    parent: Option<Value>,
    #[serde(default)]
    stream: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct EvalScore {
    name: String,
    function_id: Value,
}

#[derive(Debug, Deserialize)]
struct DatasetLookupRow {
    project_id: String,
    name: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
enum DatasetIdField {
    String(String),
    Other(Value),
}

#[derive(Debug, Clone, Deserialize)]
struct DatasetEvalDataInput {
    #[serde(default)]
    dataset_id: Option<DatasetIdField>,
    #[serde(default)]
    _internal_btql: Option<Value>,
}

#[derive(Debug, Clone, Serialize)]
struct ResolvedDatasetEvalData {
    project_id: String,
    dataset_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    _internal_btql: Option<Value>,
}

#[derive(Clone)]
pub(super) struct DevServerState {
    pub(super) base: BaseArgs,
    pub(super) language_override: Option<EvalLanguage>,
    pub(super) runner_override: Option<String>,
    pub(super) files: Vec<String>,
    pub(super) no_send_logs: bool,
    pub(super) options: EvalRunOptions,
    pub(super) host: String,
    pub(super) port: u16,
    pub(super) allowed_org_name: Option<String>,
    pub(super) allowed_origins: Vec<String>,
    pub(super) app_url: String,
    pub(super) http_client: Client,
}

#[derive(Debug)]
struct DevAuthContext {
    token: String,
    org_name: String,
}

pub(super) fn resolve_app_url(base: &BaseArgs) -> String {
    if let Some(app_url) = base.app_url.as_ref() {
        return app_url.clone();
    }
    "https://www.braintrust.dev".to_string()
}

fn app_origin_from_url(url: &str) -> Option<String> {
    reqwest::Url::parse(url).ok().and_then(|parsed| {
        let origin = parsed.origin();
        if origin.is_tuple() {
            Some(origin.ascii_serialization())
        } else {
            None
        }
    })
}

pub(super) fn collect_allowed_dev_origins(explicit: &[String], app_url: &str) -> Vec<String> {
    let mut deduped = std::collections::BTreeSet::new();
    for origin in explicit {
        let trimmed = origin.trim();
        if !trimmed.is_empty() {
            deduped.insert(trimmed.to_string());
        }
    }
    if let Some(origin) = app_origin_from_url(app_url) {
        deduped.insert(origin);
    }
    deduped.into_iter().collect()
}

fn join_app_url(app_url: &str, path: &str) -> Result<String> {
    let base = format!("{}/", app_url.trim_end_matches('/'));
    let base_url = reqwest::Url::parse(&base).context("invalid app URL")?;
    let joined = base_url
        .join(path.trim_start_matches('/'))
        .context("failed to join app URL path")?;
    Ok(joined.to_string())
}

fn json_error_response(status: actix_web::http::StatusCode, message: &str) -> HttpResponse {
    HttpResponse::build(status).json(json!({ "error": message }))
}

fn parse_auth_token(req: &HttpRequest) -> Option<String> {
    if let Some(token) = req.headers().get(HEADER_BT_AUTH_TOKEN) {
        if let Ok(value) = token.to_str() {
            if !value.trim().is_empty() {
                return Some(value.trim().to_string());
            }
        }
    }

    let auth = req.headers().get(AUTHORIZATION)?;
    let auth = auth.to_str().ok()?.trim();
    if auth.is_empty() {
        return None;
    }
    if let Some(token) = auth.strip_prefix("Bearer ") {
        let token = token.trim();
        if token.is_empty() {
            None
        } else {
            Some(token.to_string())
        }
    } else {
        Some(auth.to_string())
    }
}

async fn authenticate_dev_request(
    req: &HttpRequest,
    state: &DevServerState,
) -> std::result::Result<DevAuthContext, HttpResponse> {
    let token = match parse_auth_token(req) {
        Some(token) if !token.eq_ignore_ascii_case("null") => token,
        _ => {
            return Err(json_error_response(
                actix_web::http::StatusCode::UNAUTHORIZED,
                "Unauthorized",
            ));
        }
    };

    let org_name = match req
        .headers()
        .get(HEADER_BT_ORG_NAME)
        .and_then(|value| value.to_str().ok())
    {
        Some(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            return Err(json_error_response(
                actix_web::http::StatusCode::BAD_REQUEST,
                &format!("Missing {HEADER_BT_ORG_NAME} header"),
            ));
        }
    };

    if let Some(allowed_org_name) = state.allowed_org_name.as_ref() {
        if allowed_org_name != &org_name {
            let message = format!(
                "Org '{org_name}' is not allowed. Only org '{allowed_org_name}' is allowed."
            );
            return Err(json_error_response(
                actix_web::http::StatusCode::FORBIDDEN,
                &message,
            ));
        }
    }

    let login_url = match join_app_url(&state.app_url, "api/apikey/login") {
        Ok(url) => url,
        Err(err) => {
            return Err(json_error_response(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                &format!("{err:#}"),
            ));
        }
    };
    let response = state
        .http_client
        .post(login_url)
        .bearer_auth(&token)
        .send()
        .await
        .map_err(|_| {
            json_error_response(actix_web::http::StatusCode::UNAUTHORIZED, "Unauthorized")
        })?;
    if !response.status().is_success() {
        return Err(json_error_response(
            actix_web::http::StatusCode::UNAUTHORIZED,
            "Unauthorized",
        ));
    }

    let payload = response.json::<Value>().await.unwrap_or(Value::Null);
    if let Some(orgs) = payload.get("org_info").and_then(|value| value.as_array()) {
        let matched = orgs.iter().any(|org| {
            org.get("name")
                .and_then(|name| name.as_str())
                .map(|name| name == org_name)
                .unwrap_or(false)
        });
        if !matched {
            return Err(json_error_response(
                actix_web::http::StatusCode::UNAUTHORIZED,
                "Unauthorized",
            ));
        }
    } else {
        return Err(json_error_response(
            actix_web::http::StatusCode::UNAUTHORIZED,
            "Unauthorized",
        ));
    }

    Ok(DevAuthContext { token, org_name })
}

async fn resolve_dataset_ref_for_eval_request(
    state: &DevServerState,
    auth: &DevAuthContext,
    eval_request: &mut EvalRequest,
) -> std::result::Result<(), HttpResponse> {
    let input = match serde_json::from_value::<DatasetEvalDataInput>(eval_request.data.clone()) {
        Ok(value) => value,
        Err(_) => return Ok(()),
    };

    let dataset_id = match input.dataset_id {
        Some(DatasetIdField::String(dataset_id)) => dataset_id,
        Some(DatasetIdField::Other(value)) => {
            let received_type = match value {
                Value::Null => "null",
                Value::Bool(_) => "boolean",
                Value::Number(_) => "number",
                Value::String(_) => "string",
                Value::Array(_) => "array",
                Value::Object(_) => "object",
            };
            return Err(json_error_response(
                actix_web::http::StatusCode::BAD_REQUEST,
                &format!("Invalid dataset_id: expected a string, got {received_type}."),
            ));
        }
        None => {
            return Ok(());
        }
    };
    if dataset_id.trim().is_empty() {
        return Err(json_error_response(
            actix_web::http::StatusCode::BAD_REQUEST,
            "Invalid dataset_id: expected a non-empty string.",
        ));
    }

    let lookup_url = match join_app_url(&state.app_url, "api/dataset/get") {
        Ok(url) => url,
        Err(err) => {
            return Err(json_error_response(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                &format!("{err:#}"),
            ));
        }
    };
    let response = state
        .http_client
        .post(lookup_url)
        .bearer_auth(&auth.token)
        .header(HEADER_BT_ORG_NAME, auth.org_name.clone())
        .json(&json!({ "id": dataset_id }))
        .send()
        .await
        .map_err(|err| {
            json_error_response(
                actix_web::http::StatusCode::BAD_REQUEST,
                &format!("Failed to load dataset '{dataset_id}': {err}"),
            )
        })?;
    if !response.status().is_success() {
        return Err(json_error_response(
            actix_web::http::StatusCode::BAD_REQUEST,
            &format!(
                "Failed to load dataset '{dataset_id}' (status {}).",
                response.status()
            ),
        ));
    }

    let datasets = response
        .json::<Vec<DatasetLookupRow>>()
        .await
        .map_err(|err| {
            json_error_response(
                actix_web::http::StatusCode::BAD_REQUEST,
                &format!("Failed to parse dataset response for '{dataset_id}': {err}"),
            )
        })?;
    let Some(dataset) = datasets.first() else {
        return Err(json_error_response(
            actix_web::http::StatusCode::BAD_REQUEST,
            &format!("Dataset '{dataset_id}' not found."),
        ));
    };

    let resolved = ResolvedDatasetEvalData {
        project_id: dataset.project_id.clone(),
        dataset_name: dataset.name.clone(),
        _internal_btql: input._internal_btql,
    };
    eval_request.data = serde_json::to_value(resolved).map_err(|err| {
        json_error_response(
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            &format!("Failed to serialize resolved dataset reference: {err}"),
        )
    })?;
    Ok(())
}

fn make_dev_mode_env(
    auth: &DevAuthContext,
    state: &DevServerState,
    request: Option<&EvalRequest>,
    dev_mode: &str,
) -> Result<Vec<(String, String)>> {
    let mut env = vec![
        ("BRAINTRUST_API_KEY".to_string(), auth.token.clone()),
        ("BRAINTRUST_ORG_NAME".to_string(), auth.org_name.clone()),
        ("BRAINTRUST_APP_URL".to_string(), state.app_url.clone()),
        ("BT_EVAL_DEV_MODE".to_string(), dev_mode.to_string()),
    ];
    if let Some(request) = request {
        let serialized =
            serde_json::to_string(request).context("failed to serialize eval request payload")?;
        env.push(("BT_EVAL_DEV_REQUEST_JSON".to_string(), serialized));
    }
    Ok(env)
}

fn serialize_sse_event(event: &str, data: &str) -> String {
    format!("event: {event}\ndata: {data}\n\n")
}

fn is_eval_progress_payload(progress: &SseProgressEventData) -> bool {
    serde_json::from_str::<EvalProgressData>(&progress.data)
        .map(|payload| payload.kind_type == "eval_progress")
        .unwrap_or(false)
}

fn encode_eval_event_for_http(event: &EvalEvent) -> Option<String> {
    match event {
        EvalEvent::Processing(payload) => serde_json::to_string(payload)
            .ok()
            .map(|data| serialize_sse_event("processing", &data)),
        EvalEvent::Start(start) => serde_json::to_string(start)
            .ok()
            .map(|data| serialize_sse_event("start", &data)),
        EvalEvent::Summary(summary) => serde_json::to_string(summary)
            .ok()
            .map(|data| serialize_sse_event("summary", &data)),
        EvalEvent::Progress(progress) => {
            if is_eval_progress_payload(progress) {
                None
            } else {
                serde_json::to_string(progress)
                    .ok()
                    .map(|data| serialize_sse_event("progress", &data))
            }
        }
        EvalEvent::Dependencies { .. } => None,
        EvalEvent::Done => Some(serialize_sse_event("done", "")),
        EvalEvent::Error {
            message,
            stack,
            status,
        } => serde_json::to_string(&json!({
            "message": message,
            "stack": stack,
            "status": status,
        }))
        .ok()
        .map(|data| serialize_sse_event("error", &data)),
        EvalEvent::Console { .. } => None,
    }
}

async fn dev_server_index() -> HttpResponse {
    HttpResponse::Ok().body("Hello, world!")
}

async fn dev_server_options() -> HttpResponse {
    HttpResponse::Ok().finish()
}

fn is_allowed_preview_origin(origin: &str) -> bool {
    origin.starts_with("https://") && origin.ends_with(".preview.braintrust.dev")
}

fn is_allowed_origin(origin: &str, allowed_origins: &[String]) -> bool {
    if origin == MAIN_ORIGIN || origin == BRAINTRUSTDATA_ORIGIN || is_allowed_preview_origin(origin)
    {
        return true;
    }
    allowed_origins.iter().any(|value| value == origin)
}

fn apply_cors_headers(
    headers: &mut actix_web::http::header::HeaderMap,
    request_origin: Option<&str>,
    allow_private_network: bool,
    allowed_origins: &[String],
) {
    if let Some(origin) = request_origin {
        if is_allowed_origin(origin, allowed_origins) {
            if let Ok(origin_value) = HeaderValue::from_str(origin) {
                headers.insert(ACCESS_CONTROL_ALLOW_ORIGIN, origin_value);
                headers.insert(
                    ACCESS_CONTROL_ALLOW_METHODS,
                    HeaderValue::from_static(CORS_METHODS),
                );
                headers.insert(
                    ACCESS_CONTROL_ALLOW_HEADERS,
                    HeaderValue::from_static(CORS_ALLOWED_HEADERS),
                );
                headers.insert(
                    ACCESS_CONTROL_EXPOSE_HEADERS,
                    HeaderValue::from_static(CORS_EXPOSED_HEADERS),
                );
                headers.insert(
                    ACCESS_CONTROL_ALLOW_CREDENTIALS,
                    HeaderValue::from_static("true"),
                );
                headers.insert(ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("86400"));
                headers.insert(VARY, HeaderValue::from_static("Origin"));
            }
        }
    }

    if allow_private_network {
        headers.insert(
            HeaderName::from_static(HEADER_CORS_ALLOW_PRIVATE_NETWORK),
            HeaderValue::from_static("true"),
        );
    }
}

async fn dev_server_list(state: web::Data<DevServerState>, req: HttpRequest) -> HttpResponse {
    let auth = match authenticate_dev_request(&req, &state).await {
        Ok(auth) => auth,
        Err(response) => return response,
    };
    let extra_env = match make_dev_mode_env(&auth, &state, None, "list") {
        Ok(extra_env) => extra_env,
        Err(err) => {
            return json_error_response(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                &format!("{err:#}"),
            );
        }
    };

    let language = match detect_eval_language(&state.files, state.language_override) {
        Ok(language) => language,
        Err(err) => {
            return json_error_response(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                &format!("{err:#}"),
            );
        }
    };
    let spawned = match spawn_eval_runner(
        &state.base,
        language,
        state.runner_override.as_deref(),
        &state.files,
        state.no_send_logs,
        &state.options,
        &extra_env,
        JsMode::Auto,
    )
    .await
    {
        Ok(value) => value,
        Err(err) => {
            return json_error_response(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                &format!("{err:#}"),
            );
        }
    };

    let mut stdout_lines = Vec::new();
    let mut errors: Vec<(String, Option<u16>)> = Vec::new();
    let output =
        match drive_eval_runner(
            spawned.process,
            ConsolePolicy::Forward,
            |event| match event {
                EvalEvent::Console { stream, message } if stream == "stdout" => {
                    stdout_lines.push(message);
                }
                EvalEvent::Error {
                    message,
                    stack: _,
                    status,
                } => errors.push((message, status)),
                _ => {}
            },
        )
        .await
        {
            Ok(output) => output,
            Err(err) => {
                return json_error_response(
                    actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("{err:#}"),
                );
            }
        };

    if let Some((message, status)) = errors.first() {
        let status = status
            .and_then(|status| actix_web::http::StatusCode::from_u16(status).ok())
            .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR);
        return json_error_response(status, message);
    }
    if !output.status.success() {
        return json_error_response(
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Eval runner exited with an error.",
        );
    }

    let mut parsed_manifest: Option<Value> = None;
    for line in stdout_lines.iter().rev() {
        if let Ok(value) = serde_json::from_str::<Value>(line) {
            parsed_manifest = Some(value);
            break;
        }
    }
    if parsed_manifest.is_none() {
        let joined = stdout_lines.join("\n");
        if let Ok(value) = serde_json::from_str::<Value>(&joined) {
            parsed_manifest = Some(value);
        }
    }

    match parsed_manifest {
        Some(manifest) => HttpResponse::Ok().json(manifest),
        None => json_error_response(
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to parse evaluator manifest from runner output.",
        ),
    }
}

async fn dev_server_eval(
    state: web::Data<DevServerState>,
    req: HttpRequest,
    body: web::Bytes,
) -> HttpResponse {
    let auth = match authenticate_dev_request(&req, &state).await {
        Ok(auth) => auth,
        Err(response) => return response,
    };

    let mut eval_request: EvalRequest = match serde_json::from_slice(&body) {
        Ok(eval_request) => eval_request,
        Err(err) => {
            return json_error_response(actix_web::http::StatusCode::BAD_REQUEST, &err.to_string());
        }
    };
    if let Err(response) =
        resolve_dataset_ref_for_eval_request(&state, &auth, &mut eval_request).await
    {
        return response;
    }
    let stream_requested = eval_request.stream.unwrap_or(false);
    let extra_env = match make_dev_mode_env(&auth, &state, Some(&eval_request), "eval") {
        Ok(extra_env) => extra_env,
        Err(err) => {
            return json_error_response(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                &format!("{err:#}"),
            );
        }
    };

    let language = match detect_eval_language(&state.files, state.language_override) {
        Ok(language) => language,
        Err(err) => {
            return json_error_response(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                &format!("{err:#}"),
            );
        }
    };
    let spawned = match spawn_eval_runner(
        &state.base,
        language,
        state.runner_override.as_deref(),
        &state.files,
        state.no_send_logs,
        &state.options,
        &extra_env,
        JsMode::Auto,
    )
    .await
    {
        Ok(value) => value,
        Err(err) => {
            return json_error_response(
                actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                &format!("{err:#}"),
            );
        }
    };

    if stream_requested {
        let (tx, rx) = mpsc::unbounded_channel::<String>();
        tokio::spawn(async move {
            let mut saw_error = false;
            let mut stderr_lines: Vec<String> = Vec::new();
            let output = drive_eval_runner(spawned.process, ConsolePolicy::Forward, |event| {
                if matches!(event, EvalEvent::Error { .. }) {
                    saw_error = true;
                }
                if matches!(event, EvalEvent::Done) {
                    return;
                }
                if let EvalEvent::Console {
                    ref stream,
                    ref message,
                } = event
                {
                    for line in message.lines() {
                        let _ = tx.send(format!(": [{stream}] {line}\n"));
                    }
                    if stream == "stderr" {
                        stderr_lines.push(message.clone());
                    }
                    return;
                }
                if let Some(encoded) = encode_eval_event_for_http(&event) {
                    let _ = tx.send(encoded);
                }
            })
            .await;

            match output {
                Ok(output) => {
                    if !output.status.success() && !saw_error {
                        let mut detail = format!("Eval runner exited with {}.", output.status);
                        for line in stderr_lines.iter() {
                            detail.push('\n');
                            detail.push_str(line);
                        }
                        let error =
                            serialize_sse_event("error", &json!({ "message": detail }).to_string());
                        let _ = tx.send(error);
                    }
                }
                Err(err) => {
                    let error = serialize_sse_event(
                        "error",
                        &json!({ "message": format!("{err:#}") }).to_string(),
                    );
                    let _ = tx.send(error);
                }
            }

            let _ = tx.send(serialize_sse_event("done", ""));
        });

        let response_stream = stream::unfold(rx, |mut rx| async {
            rx.recv()
                .await
                .map(|chunk| (Ok::<_, actix_web::Error>(web::Bytes::from(chunk)), rx))
        });
        return HttpResponse::Ok()
            .append_header((CONTENT_TYPE, "text/event-stream"))
            .append_header((CACHE_CONTROL, "no-cache"))
            .append_header((CONNECTION, "keep-alive"))
            .streaming(response_stream);
    }

    let mut summary: Option<ExperimentSummary> = None;
    let mut errors: Vec<(String, Option<u16>)> = Vec::new();
    let output =
        match drive_eval_runner(
            spawned.process,
            ConsolePolicy::Forward,
            |event| match event {
                EvalEvent::Summary(current) => summary = Some(current),
                EvalEvent::Error {
                    message,
                    stack: _,
                    status,
                } => errors.push((message, status)),
                _ => {}
            },
        )
        .await
        {
            Ok(output) => output,
            Err(err) => {
                return json_error_response(
                    actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
                    &format!("{err:#}"),
                );
            }
        };

    if let Some((message, status)) = errors.first() {
        let status = status
            .and_then(|status| actix_web::http::StatusCode::from_u16(status).ok())
            .unwrap_or(actix_web::http::StatusCode::INTERNAL_SERVER_ERROR);
        return json_error_response(status, message);
    }
    if let Some(summary) = summary {
        return HttpResponse::Ok().json(summary);
    }
    if !output.status.success() {
        return json_error_response(
            actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
            "Eval runner exited with an error.",
        );
    }
    json_error_response(
        actix_web::http::StatusCode::INTERNAL_SERVER_ERROR,
        "Eval runner did not return a summary.",
    )
}

pub(super) async fn run_dev_server(state: DevServerState) -> Result<()> {
    println!(
        "Starting eval dev server on http://{}:{}",
        state.host, state.port
    );
    let host = state.host.clone();
    let port = state.port;
    HttpServer::new(move || {
        let allowed_origins = state.allowed_origins.clone();
        App::new()
            .wrap_fn({
                let allowed_origins = allowed_origins.clone();
                move |req, srv| {
                    let allowed_origins = allowed_origins.clone();
                    let request_origin = req
                        .headers()
                        .get(ORIGIN)
                        .and_then(|value| value.to_str().ok())
                        .map(str::to_owned);
                    let allow_private_network =
                        req.headers().contains_key(HEADER_CORS_REQ_PRIVATE_NETWORK);
                    let fut = srv.call(req);
                    async move {
                        let mut res = fut.await?;
                        apply_cors_headers(
                            res.headers_mut(),
                            request_origin.as_deref(),
                            allow_private_network,
                            &allowed_origins,
                        );
                        Ok::<_, actix_web::Error>(res)
                    }
                }
            })
            .app_data(web::Data::new(state.clone()))
            .route("/", web::get().to(dev_server_index))
            .route(
                "/",
                web::route().guard(guard::Options()).to(dev_server_options),
            )
            .route("/list", web::get().to(dev_server_list))
            .route(
                "/list",
                web::route().guard(guard::Options()).to(dev_server_options),
            )
            .route("/eval", web::post().to(dev_server_eval))
            .route(
                "/eval",
                web::route().guard(guard::Options()).to(dev_server_options),
            )
    })
    .bind((host.as_str(), port))
    .with_context(|| format!("failed to bind eval dev server on {host}:{port}"))?
    .run()
    .await
    .context("eval dev server exited unexpectedly")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_app_url_normalizes_slashes() {
        let joined =
            join_app_url("https://www.braintrust.dev/", "/api/dataset/get").expect("join app url");
        assert_eq!(joined, "https://www.braintrust.dev/api/dataset/get");
    }

    #[test]
    fn collect_allowed_dev_origins_includes_app_origin_and_dedupes() {
        let origins = collect_allowed_dev_origins(
            &[
                "https://example.com".to_string(),
                "https://example.com".to_string(),
            ],
            "https://app.example.dev/some/path",
        );
        assert_eq!(
            origins,
            vec![
                "https://app.example.dev".to_string(),
                "https://example.com".to_string()
            ]
        );
    }

    #[test]
    fn is_allowed_origin_accepts_configured_origin() {
        let allowed = vec!["https://example.com".to_string()];
        assert!(is_allowed_origin("https://example.com", &allowed));
        assert!(!is_allowed_origin("https://evil.example", &allowed));
    }

    #[test]
    fn encode_eval_event_for_http_filters_internal_eval_progress() {
        let event = EvalEvent::Progress(SseProgressEventData {
            id: "id-1".to_string(),
            object_type: "task".to_string(),
            origin: None,
            format: "global".to_string(),
            output_type: "any".to_string(),
            name: "My evaluation".to_string(),
            event: "progress".to_string(),
            data: r#"{"type":"eval_progress","kind":"start","total":1}"#.to_string(),
        });

        assert!(encode_eval_event_for_http(&event).is_none());
    }

    #[test]
    fn encode_eval_event_for_http_keeps_external_progress_events() {
        let event = EvalEvent::Progress(SseProgressEventData {
            id: "id-2".to_string(),
            object_type: "task".to_string(),
            origin: None,
            format: "code".to_string(),
            output_type: "completion".to_string(),
            name: "My evaluation".to_string(),
            event: "json_delta".to_string(),
            data: "\"China\"".to_string(),
        });

        let encoded = encode_eval_event_for_http(&event).expect("progress should be forwarded");
        assert!(encoded.contains("event: progress"));
        assert!(encoded.contains("json_delta"));
    }
}
