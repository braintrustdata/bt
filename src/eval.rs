use std::collections::{BTreeSet, HashMap, VecDeque};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use actix_web::dev::Service;
use actix_web::http::header::{
    HeaderName, HeaderValue, ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS,
    ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS,
    ACCESS_CONTROL_MAX_AGE, ORIGIN, VARY,
};
use actix_web::{guard, web, App, HttpRequest, HttpResponse, HttpServer};
use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use crossterm::queue;
use crossterm::style::{
    Attribute, Color as CtColor, ResetColor, SetAttribute, SetBackgroundColor, SetForegroundColor,
    Stylize,
};
use futures_util::stream;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use strip_ansi_escapes::strip;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::sync::mpsc;
use unicode_width::UnicodeWidthStr;

use ratatui::backend::TestBackend;
use ratatui::layout::{Alignment, Constraint};
use ratatui::style::{Color, Modifier, Style};
use ratatui::text::{Line, Span};
use ratatui::widgets::{Cell, Row, Table};
use ratatui::Terminal;

use crate::args::BaseArgs;
use crate::login::resolved_auth_env;

const MAX_NAME_LENGTH: usize = 40;
const WATCH_POLL_INTERVAL: Duration = Duration::from_millis(500);
const MAIN_ORIGIN: &str = "https://www.braintrust.dev";
const BRAINTRUSTDATA_ORIGIN: &str = "https://www.braintrustdata.com";
const CORS_METHODS: &str = "GET, PATCH, POST, PUT, DELETE, OPTIONS";
const CORS_ALLOWED_HEADERS: &str = "Content-Type, X-Amz-Date, Authorization, X-Api-Key, X-Amz-Security-Token, x-bt-auth-token, x-bt-parent, x-bt-org-name, x-bt-project-id, x-bt-stream-fmt, x-bt-use-cache, x-stainless-os, x-stainless-lang, x-stainless-package-version, x-stainless-runtime, x-stainless-runtime-version, x-stainless-arch";
const CORS_EXPOSED_HEADERS: &str =
    "x-bt-cursor, x-bt-found-existing-experiment, x-bt-span-id, x-bt-span-export";
const SSE_SOCKET_BIND_MAX_ATTEMPTS: u8 = 16;
static SSE_SOCKET_COUNTER: AtomicU64 = AtomicU64::new(0);

struct EvalRunOutput {
    status: ExitStatus,
    dependencies: Vec<PathBuf>,
}

struct EvalRunnerProcess {
    child: tokio::process::Child,
    rx: mpsc::UnboundedReceiver<EvalEvent>,
    sse_task: tokio::task::JoinHandle<()>,
    sse_connected: Arc<AtomicBool>,
    _socket_cleanup_guard: SocketCleanupGuard,
}

struct EvalProcessOutput {
    status: ExitStatus,
    dependency_files: Vec<String>,
}

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

#[derive(Clone)]
struct DevServerState {
    base: BaseArgs,
    language_override: Option<EvalLanguage>,
    runner_override: Option<String>,
    files: Vec<String>,
    no_send_logs: bool,
    options: EvalRunOptions,
    host: String,
    port: u16,
    allowed_org_name: Option<String>,
    app_url: String,
    http_client: Client,
}

#[derive(Debug)]
struct DevAuthContext {
    token: String,
    org_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
struct RunnerFilter {
    path: Vec<String>,
    pattern: String,
}

const JS_RUNNER_FILE: &str = "eval-runner.ts";
const PY_RUNNER_FILE: &str = "eval-runner.py";
const JS_RUNNER_SOURCE: &str = include_str!("../scripts/eval-runner.ts");
const PY_RUNNER_SOURCE: &str = include_str!("../scripts/eval-runner.py");

struct SocketCleanupGuard {
    path: PathBuf,
}

impl SocketCleanupGuard {
    fn new(path: PathBuf) -> Self {
        Self { path }
    }
}

impl Drop for SocketCleanupGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, ValueEnum)]
pub enum EvalLanguage {
    #[value(alias = "js")]
    JavaScript,
    #[value(alias = "py")]
    Python,
}

#[derive(Debug, Clone, Args)]
pub struct EvalArgs {
    /// One or more eval files to execute (e.g. foo.eval.ts)
    #[arg(required = true, value_name = "FILE")]
    pub files: Vec<String>,

    /// Eval runner binary (e.g. tsx, bun, ts-node, deno, python). Defaults to tsx for JS files.
    #[arg(long, short = 'r', env = "BT_EVAL_RUNNER", value_name = "RUNNER")]
    pub runner: Option<String>,

    /// Force eval language instead of inferring from file extensions.
    #[arg(
        long,
        short = 'l',
        env = "BT_EVAL_LANGUAGE",
        value_enum,
        value_name = "LANGUAGE"
    )]
    pub language: Option<EvalLanguage>,

    /// Run evals locally (do not send logs to Braintrust).
    #[arg(
        long,
        alias = "no-send-logs",
        env = "BT_EVAL_LOCAL",
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    pub no_send_logs: bool,

    /// Output one JSON summary per evaluator.
    #[arg(long)]
    pub jsonl: bool,

    /// Stop after the first failing evaluator.
    #[arg(long)]
    pub terminate_on_failure: bool,

    /// Number of worker threads for Python eval execution.
    #[arg(long, value_name = "COUNT")]
    pub num_workers: Option<usize>,

    /// List evaluators without executing them.
    #[arg(long)]
    pub list: bool,

    /// Filter expression(s) used to select which evaluators to run.
    #[arg(long, value_name = "FILTER")]
    pub filter: Vec<String>,

    /// Re-run evals when input files change.
    #[arg(long, short = 'w')]
    pub watch: bool,

    /// Start the eval dev web server.
    #[arg(long)]
    pub dev: bool,

    /// Host interface for eval dev server.
    #[arg(long, default_value = "localhost")]
    pub dev_host: String,

    /// Port for eval dev server.
    #[arg(long, default_value_t = 8300)]
    pub dev_port: u16,

    /// Restrict eval dev server access to a specific org name.
    #[arg(long)]
    pub dev_org_name: Option<String>,
}

#[derive(Debug, Clone)]
struct EvalRunOptions {
    jsonl: bool,
    terminate_on_failure: bool,
    num_workers: Option<usize>,
    list: bool,
    filter: Vec<String>,
}

pub async fn run(base: BaseArgs, args: EvalArgs) -> Result<()> {
    if args.dev && args.watch {
        anyhow::bail!("--watch is not supported with --dev.");
    }

    let options = EvalRunOptions {
        jsonl: args.jsonl,
        terminate_on_failure: args.terminate_on_failure,
        num_workers: args.num_workers,
        list: args.list,
        filter: args.filter,
    };

    if args.dev {
        let language = detect_eval_language(&args.files, args.language)?;
        let state = DevServerState {
            base: base.clone(),
            language_override: Some(language),
            runner_override: args.runner.clone(),
            files: args.files.clone(),
            no_send_logs: args.no_send_logs,
            options,
            host: args.dev_host.clone(),
            port: args.dev_port,
            allowed_org_name: args.dev_org_name.clone(),
            app_url: resolve_app_url(&base),
            http_client: Client::builder()
                .build()
                .context("failed to create dev server HTTP client")?,
        };
        return run_dev_server(state).await;
    }

    if args.watch {
        run_eval_files_watch(
            &base,
            args.language,
            args.runner.clone(),
            args.files.clone(),
            args.no_send_logs,
            options,
        )
        .await
    } else {
        let output = run_eval_files_once(
            &base,
            args.language,
            args.runner.clone(),
            args.files.clone(),
            args.no_send_logs,
            options,
        )
        .await?;
        if !output.status.success() {
            anyhow::bail!("eval runner exited with status {}", output.status);
        }
        Ok(())
    }
}

async fn run_eval_files_watch(
    base: &BaseArgs,
    language_override: Option<EvalLanguage>,
    runner_override: Option<String>,
    files: Vec<String>,
    no_send_logs: bool,
    options: EvalRunOptions,
) -> Result<()> {
    let input_watch_paths = resolve_watch_paths(&files)?;
    let mut active_watch_paths = input_watch_paths.clone();
    let mut watch_state = snapshot_watch_state(&active_watch_paths)?;

    eprintln!(
        "Watch mode enabled for {} path(s). Press Ctrl-C to stop.",
        active_watch_paths.len()
    );

    loop {
        match run_eval_files_once(
            base,
            language_override,
            runner_override.clone(),
            files.clone(),
            no_send_logs,
            options.clone(),
        )
        .await
        {
            Ok(output) => {
                let merged_paths = merge_watch_paths(&input_watch_paths, &output.dependencies);
                update_watch_targets(&mut active_watch_paths, &mut watch_state, merged_paths)?;
                if output.status.success() {
                    eprintln!(
                        "Eval run completed. Watching {} path(s). Waiting for changes...",
                        active_watch_paths.len()
                    );
                } else {
                    eprintln!(
                        "Eval run failed: eval runner exited with status {}",
                        output.status
                    );
                    eprintln!(
                        "Watching {} path(s). Waiting for changes...",
                        active_watch_paths.len()
                    );
                }
            }
            Err(err) => {
                eprintln!("Eval run failed: {err:#}");
                eprintln!("Waiting for changes...");
            }
        }

        let changed = wait_for_watch_changes(&active_watch_paths, &mut watch_state).await?;
        eprintln!(
            "Detected changes in {}. Re-running evals.\n",
            format_watch_paths(&changed)
        );
    }
}

async fn run_eval_files_once(
    base: &BaseArgs,
    language_override: Option<EvalLanguage>,
    runner_override: Option<String>,
    files: Vec<String>,
    no_send_logs: bool,
    options: EvalRunOptions,
) -> Result<EvalRunOutput> {
    let (process, language, show_js_runner_hint_on_failure) = spawn_eval_runner(
        base,
        language_override,
        runner_override,
        files.clone(),
        no_send_logs,
        &options,
        &[],
    )
    .await?;
    let mut ui = EvalUi::new(options.jsonl, options.list);
    let output = drive_eval_runner(process, |event| ui.handle(event)).await?;
    ui.finish();
    if !output.status.success() && show_js_runner_hint_on_failure {
        eprintln!(
            "Hint: If this eval uses ESM features (like top-level await), try `--runner vite-node`."
        );
    }
    let mut dependencies =
        normalize_watch_paths(output.dependency_files.into_iter().map(PathBuf::from))?;
    if language == EvalLanguage::JavaScript {
        let static_dependencies = collect_js_static_dependencies(&files)?;
        dependencies = merge_watch_paths(&dependencies, &static_dependencies);
    }

    Ok(EvalRunOutput {
        status: output.status,
        dependencies,
    })
}

async fn spawn_eval_runner(
    base: &BaseArgs,
    language_override: Option<EvalLanguage>,
    runner_override: Option<String>,
    files: Vec<String>,
    no_send_logs: bool,
    options: &EvalRunOptions,
    extra_env: &[(String, String)],
) -> Result<(EvalRunnerProcess, EvalLanguage, bool)> {
    let language = detect_eval_language(&files, language_override)?;
    if language != EvalLanguage::Python && options.num_workers.is_some() {
        anyhow::bail!("--num-workers is only supported for Python evals.");
    }
    let show_js_runner_hint_on_failure =
        language == EvalLanguage::JavaScript && runner_override.is_none();
    let (js_runner, py_runner) = prepare_eval_runners()?;

    let (listener, socket_path, socket_cleanup_guard) = bind_sse_listener()?;
    let (tx, rx) = mpsc::unbounded_channel();
    let sse_connected = Arc::new(AtomicBool::new(false));

    let tx_sse = tx.clone();
    let sse_connected_for_task = Arc::clone(&sse_connected);
    let sse_task = tokio::spawn(async move {
        match listener.accept().await {
            Ok((stream, _)) => {
                sse_connected_for_task.store(true, Ordering::Relaxed);
                if let Err(err) = read_sse_stream(stream, tx_sse.clone()).await {
                    let _ = tx_sse.send(EvalEvent::Error {
                        message: format!("SSE stream error: {err}"),
                        stack: None,
                        status: None,
                    });
                }
            }
            Err(err) => {
                let _ = tx_sse.send(EvalEvent::Error {
                    message: format!("Failed to accept SSE connection: {err}"),
                    stack: None,
                    status: None,
                });
            }
        };
    });

    let mut cmd = match language {
        EvalLanguage::Python => build_python_command(runner_override, &py_runner, &files)?,
        EvalLanguage::JavaScript => build_js_command(runner_override, &js_runner, &files)?,
    };

    cmd.envs(build_env(base).await?);
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
    if no_send_logs {
        cmd.env("BT_EVAL_NO_SEND_LOGS", "1");
        cmd.env("BT_EVAL_LOCAL", "1");
    }
    if options.jsonl {
        cmd.env("BT_EVAL_JSONL", "1");
    }
    if options.terminate_on_failure {
        cmd.env("BT_EVAL_TERMINATE_ON_FAILURE", "1");
    }
    if options.list {
        cmd.env("BT_EVAL_LIST", "1");
    }
    if let Some(num_workers) = options.num_workers {
        cmd.env("BT_EVAL_NUM_WORKERS", num_workers.to_string());
    }
    if !options.filter.is_empty() {
        let parsed = parse_eval_filter_expressions(&options.filter)?;
        let serialized =
            serde_json::to_string(&parsed).context("failed to serialize eval filters")?;
        cmd.env("BT_EVAL_FILTER_PARSED", serialized);
    }
    cmd.env(
        "BT_EVAL_SSE_SOCK",
        socket_path.to_string_lossy().to_string(),
    );
    cmd.stdout(Stdio::piped());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().context("failed to start eval runner")?;

    let stdout = child.stdout.take();
    let stderr = child.stderr.take();

    if let Some(stdout) = stdout {
        let tx_stdout = tx.clone();
        tokio::spawn(async move {
            if let Err(err) = forward_stream(stdout, "stdout", tx_stdout).await {
                eprintln!("Failed to read eval stdout: {err}");
            }
        });
    }

    if let Some(stderr) = stderr {
        let tx_stderr = tx.clone();
        tokio::spawn(async move {
            if let Err(err) = forward_stream(stderr, "stderr", tx_stderr).await {
                eprintln!("Failed to read eval stderr: {err}");
            }
        });
    }

    drop(tx);

    Ok((
        EvalRunnerProcess {
            child,
            rx,
            sse_task,
            sse_connected,
            _socket_cleanup_guard: socket_cleanup_guard,
        },
        language,
        show_js_runner_hint_on_failure,
    ))
}

async fn drive_eval_runner<F>(
    mut process: EvalRunnerProcess,
    mut on_event: F,
) -> Result<EvalProcessOutput>
where
    F: FnMut(EvalEvent),
{
    let mut status = None;
    let mut dependency_files: Vec<String> = Vec::new();

    loop {
        tokio::select! {
            event = process.rx.recv() => {
                match event {
                    Some(EvalEvent::Dependencies { files }) => {
                        dependency_files.extend(files.clone());
                        on_event(EvalEvent::Dependencies { files });
                    }
                    Some(event) => on_event(event),
                    None => {
                        if status.is_none() {
                            status = Some(process.child.wait().await.context("eval runner process failed")?);
                            if !process.sse_connected.load(Ordering::Relaxed) {
                                process.sse_task.abort();
                            }
                        }
                        break;
                    }
                }
            }
            exit_status = process.child.wait(), if status.is_none() => {
                status = Some(exit_status.context("eval runner process failed")?);
                if !process.sse_connected.load(Ordering::Relaxed) {
                    process.sse_task.abort();
                }
            }
        }

        if status.is_some() && process.rx.is_closed() {
            break;
        }
    }

    let _ = process.sse_task.await;

    Ok(EvalProcessOutput {
        status: status.context("eval runner process exited without a status")?,
        dependency_files,
    })
}

fn resolve_app_url(base: &BaseArgs) -> String {
    if let Some(app_url) = base.app_url.as_ref() {
        return app_url.clone();
    }
    if let Some(api_url) = base.api_url.as_ref() {
        return api_url
            .replace("api.braintrust", "www.braintrust")
            .replace("api.braintrustdata", "www.braintrustdata");
    }
    "https://www.braintrust.dev".to_string()
}

fn json_error_response(status: actix_web::http::StatusCode, message: &str) -> HttpResponse {
    HttpResponse::build(status).json(json!({ "error": message }))
}

fn parse_auth_token(req: &HttpRequest) -> Option<String> {
    if let Some(token) = req.headers().get("x-bt-auth-token") {
        if let Ok(value) = token.to_str() {
            if !value.trim().is_empty() {
                return Some(value.trim().to_string());
            }
        }
    }

    let auth = req.headers().get("authorization")?;
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
        .get("x-bt-org-name")
        .and_then(|value| value.to_str().ok())
    {
        Some(value) if !value.trim().is_empty() => value.trim().to_string(),
        _ => {
            return Err(json_error_response(
                actix_web::http::StatusCode::BAD_REQUEST,
                "Missing x-bt-org-name header",
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

    let login_url = format!("{}/api/apikey/login", state.app_url.trim_end_matches('/'));
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
    }

    Ok(DevAuthContext { token, org_name })
}

async fn resolve_dataset_ref_for_eval_request(
    state: &DevServerState,
    auth: &DevAuthContext,
    eval_request: &mut EvalRequest,
) -> std::result::Result<(), HttpResponse> {
    let data_obj = eval_request.data.as_object();
    let Some(data_obj) = data_obj else {
        return Ok(());
    };

    let Some(dataset_id_value) = data_obj.get("dataset_id") else {
        return Ok(());
    };
    let Some(dataset_id) = dataset_id_value.as_str() else {
        return Err(json_error_response(
            actix_web::http::StatusCode::BAD_REQUEST,
            "Invalid dataset_id: expected a string.",
        ));
    };
    if dataset_id.trim().is_empty() {
        return Err(json_error_response(
            actix_web::http::StatusCode::BAD_REQUEST,
            "Invalid dataset_id: expected a non-empty string.",
        ));
    }

    let lookup_url = format!("{}/api/dataset/get", state.app_url.trim_end_matches('/'));
    let response = state
        .http_client
        .post(lookup_url)
        .bearer_auth(&auth.token)
        .header("x-bt-org-name", auth.org_name.clone())
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

    let mut resolved = serde_json::Map::new();
    resolved.insert(
        "project_id".to_string(),
        Value::String(dataset.project_id.clone()),
    );
    resolved.insert(
        "dataset_name".to_string(),
        Value::String(dataset.name.clone()),
    );
    if let Some(btql) = data_obj.get("_internal_btql") {
        resolved.insert("_internal_btql".to_string(), btql.clone());
    }
    eval_request.data = Value::Object(resolved);
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
        EvalEvent::Start(summary) => serde_json::to_string(summary)
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

fn is_allowed_origin(origin: &str) -> bool {
    if origin == MAIN_ORIGIN || origin == BRAINTRUSTDATA_ORIGIN || is_allowed_preview_origin(origin)
    {
        return true;
    }
    if std::env::var("WHITELISTED_ORIGIN")
        .ok()
        .is_some_and(|value| value == origin)
    {
        return true;
    }
    if std::env::var("BRAINTRUST_APP_URL")
        .ok()
        .is_some_and(|value| value == origin)
    {
        return true;
    }
    false
}

fn apply_cors_headers(
    headers: &mut actix_web::http::header::HeaderMap,
    request_origin: Option<&str>,
    allow_private_network: bool,
) {
    if let Some(origin) = request_origin {
        if is_allowed_origin(origin) {
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
            HeaderName::from_static("access-control-allow-private-network"),
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

    let (process, _, _) = match spawn_eval_runner(
        &state.base,
        state.language_override,
        state.runner_override.clone(),
        state.files.clone(),
        state.no_send_logs,
        &state.options,
        &extra_env,
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
    let output = match drive_eval_runner(process, |event| match event {
        EvalEvent::Console { stream, message } if stream == "stdout" => {
            stdout_lines.push(message);
        }
        EvalEvent::Error {
            message,
            stack: _,
            status,
        } => errors.push((message, status)),
        _ => {}
    })
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

    let (process, _, _) = match spawn_eval_runner(
        &state.base,
        state.language_override,
        state.runner_override.clone(),
        state.files.clone(),
        state.no_send_logs,
        &state.options,
        &extra_env,
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
            let mut saw_done = false;
            let output = drive_eval_runner(process, |event| {
                if matches!(event, EvalEvent::Error { .. }) {
                    saw_error = true;
                }
                if matches!(event, EvalEvent::Done) {
                    saw_done = true;
                }
                if let Some(encoded) = encode_eval_event_for_http(&event) {
                    let _ = tx.send(encoded);
                }
            })
            .await;

            match output {
                Ok(output) => {
                    if !output.status.success() && !saw_error {
                        let error = serialize_sse_event(
                            "error",
                            &json!({ "message": "Eval runner exited with an error." }).to_string(),
                        );
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

            if !saw_done {
                let _ = tx.send(serialize_sse_event("done", ""));
            }
        });

        let response_stream = stream::unfold(rx, |mut rx| async {
            rx.recv()
                .await
                .map(|chunk| (Ok::<_, actix_web::Error>(web::Bytes::from(chunk)), rx))
        });
        return HttpResponse::Ok()
            .append_header(("Content-Type", "text/event-stream"))
            .append_header(("Cache-Control", "no-cache"))
            .append_header(("Connection", "keep-alive"))
            .streaming(response_stream);
    }

    let mut summary: Option<ExperimentSummary> = None;
    let mut errors: Vec<(String, Option<u16>)> = Vec::new();
    let output = match drive_eval_runner(process, |event| match event {
        EvalEvent::Summary(current) => summary = Some(current),
        EvalEvent::Error {
            message,
            stack: _,
            status,
        } => errors.push((message, status)),
        _ => {}
    })
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

async fn run_dev_server(state: DevServerState) -> Result<()> {
    println!(
        "Starting eval dev server on http://{}:{}",
        state.host, state.port
    );
    let host = state.host.clone();
    let port = state.port;
    HttpServer::new(move || {
        App::new()
            .wrap_fn(|req, srv| {
                let request_origin = req
                    .headers()
                    .get(ORIGIN)
                    .and_then(|value| value.to_str().ok())
                    .map(str::to_owned);
                let allow_private_network = req
                    .headers()
                    .contains_key("access-control-request-private-network");
                let fut = srv.call(req);
                async move {
                    let mut res = fut.await?;
                    apply_cors_headers(
                        res.headers_mut(),
                        request_origin.as_deref(),
                        allow_private_network,
                    );
                    Ok::<_, actix_web::Error>(res)
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
    .with_context(|| format!("failed to bind eval dev server on {}:{}", host, port))?
    .run()
    .await
    .context("eval dev server exited unexpectedly")
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct WatchEntry {
    modified: Option<SystemTime>,
    len: u64,
}

type WatchState = HashMap<PathBuf, Option<WatchEntry>>;

fn resolve_watch_paths(files: &[String]) -> Result<Vec<PathBuf>> {
    normalize_watch_paths(files.iter().map(PathBuf::from))
}

fn parse_eval_filter_expression(expression: &str) -> Result<RunnerFilter> {
    let (path, pattern) = expression
        .split_once('=')
        .ok_or_else(|| anyhow::anyhow!("Invalid filter {expression}"))?;
    let path = path.trim();
    if path.is_empty() {
        anyhow::bail!("Invalid filter {expression}");
    }
    Ok(RunnerFilter {
        path: path.split('.').map(str::to_string).collect(),
        pattern: pattern.to_string(),
    })
}

fn parse_eval_filter_expressions(filters: &[String]) -> Result<Vec<RunnerFilter>> {
    filters
        .iter()
        .map(|filter| parse_eval_filter_expression(filter))
        .collect()
}

fn normalize_watch_paths(paths: impl IntoIterator<Item = PathBuf>) -> Result<Vec<PathBuf>> {
    let cwd = std::env::current_dir().context("failed to read current directory")?;
    let mut deduped = BTreeSet::new();

    for path in paths {
        let absolute = if path.is_absolute() {
            path
        } else {
            cwd.join(path)
        };
        deduped.insert(absolute);
    }

    Ok(deduped.into_iter().collect())
}

fn merge_watch_paths(inputs: &[PathBuf], dependencies: &[PathBuf]) -> Vec<PathBuf> {
    let mut deduped = BTreeSet::new();
    deduped.extend(inputs.iter().cloned());
    deduped.extend(dependencies.iter().cloned());
    deduped.into_iter().collect()
}

fn collect_js_static_dependencies(files: &[String]) -> Result<Vec<PathBuf>> {
    let roots = resolve_watch_paths(files)?;
    let mut queue: VecDeque<PathBuf> = roots.into_iter().collect();
    let mut visited = BTreeSet::new();
    let mut discovered = BTreeSet::new();

    while let Some(file) = queue.pop_front() {
        if !visited.insert(file.clone()) {
            continue;
        }
        discovered.insert(file.clone());

        let content = match std::fs::read_to_string(&file) {
            Ok(content) => content,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => continue,
            Err(err) => {
                return Err(err).with_context(|| format!("failed to read {}", file.display()));
            }
        };

        for specifier in extract_js_local_specifiers(&content) {
            if let Some(resolved) = resolve_js_local_specifier(&file, &specifier) {
                if !visited.contains(&resolved) {
                    queue.push_back(resolved.clone());
                }
                discovered.insert(resolved);
            }
        }
    }

    Ok(discovered.into_iter().collect())
}

fn extract_js_local_specifiers(content: &str) -> Vec<String> {
    const PATTERNS: &[(&str, char)] = &[
        ("from \"", '"'),
        ("from '", '\''),
        ("import(\"", '"'),
        ("import('", '\''),
        ("require(\"", '"'),
        ("require('", '\''),
    ];

    let mut specifiers = Vec::new();
    for (prefix, quote) in PATTERNS {
        let mut offset = 0usize;
        while let Some(start) = content[offset..].find(prefix) {
            let specifier_start = offset + start + prefix.len();
            if let Some(end_rel) = content[specifier_start..].find(*quote) {
                let specifier = &content[specifier_start..specifier_start + end_rel];
                if specifier.starts_with("./")
                    || specifier.starts_with("../")
                    || specifier.starts_with("/")
                    || specifier.starts_with("file://")
                {
                    specifiers.push(specifier.to_string());
                }
                offset = specifier_start + end_rel + 1;
            } else {
                break;
            }
        }
    }
    specifiers
}

fn resolve_js_local_specifier(base_file: &Path, specifier: &str) -> Option<PathBuf> {
    let base_dir = base_file.parent()?;
    let candidate = if specifier.starts_with("file://") {
        PathBuf::from(specifier.trim_start_matches("file://"))
    } else if specifier.starts_with('/') {
        PathBuf::from(specifier)
    } else {
        base_dir.join(specifier)
    };

    let mut candidates = vec![candidate.clone()];
    if candidate.extension().is_none() {
        for ext in ["ts", "tsx", "js", "jsx", "mjs", "cjs", "mts", "cts", "json"] {
            candidates.push(candidate.with_extension(ext));
        }
        for ext in ["ts", "tsx", "js", "jsx", "mjs", "cjs", "mts", "cts", "json"] {
            candidates.push(candidate.join(format!("index.{ext}")));
        }
    }

    candidates.into_iter().find(|path| path.is_file())
}

fn read_watch_entry(path: &Path) -> Result<Option<WatchEntry>> {
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(Some(WatchEntry {
            modified: metadata.modified().ok(),
            len: metadata.len(),
        })),
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => {
            Err(err).with_context(|| format!("failed to read metadata for {}", path.display()))
        }
    }
}

fn snapshot_watch_state(paths: &[PathBuf]) -> Result<WatchState> {
    let mut state = HashMap::with_capacity(paths.len());
    for path in paths {
        state.insert(path.clone(), read_watch_entry(path)?);
    }
    Ok(state)
}

fn update_watch_targets(
    active_paths: &mut Vec<PathBuf>,
    state: &mut WatchState,
    next_paths: Vec<PathBuf>,
) -> Result<()> {
    let next_set: BTreeSet<PathBuf> = next_paths.into_iter().collect();
    let current_set: BTreeSet<PathBuf> = active_paths.iter().cloned().collect();
    if next_set == current_set {
        return Ok(());
    }

    state.retain(|path, _| next_set.contains(path));
    for path in &next_set {
        if !state.contains_key(path) {
            state.insert(path.clone(), read_watch_entry(path)?);
        }
    }

    *active_paths = next_set.into_iter().collect();
    Ok(())
}

fn detect_watch_changes(paths: &[PathBuf], state: &mut WatchState) -> Result<Vec<PathBuf>> {
    let mut changed = Vec::new();

    for path in paths {
        let current = read_watch_entry(path)?;
        let previous = state.get(path).cloned().unwrap_or(None);
        if current != previous {
            changed.push(path.clone());
            state.insert(path.clone(), current);
        }
    }

    Ok(changed)
}

async fn wait_for_watch_changes(paths: &[PathBuf], state: &mut WatchState) -> Result<Vec<PathBuf>> {
    loop {
        let changed = detect_watch_changes(paths, state)?;
        if !changed.is_empty() {
            return Ok(changed);
        }
        tokio::time::sleep(WATCH_POLL_INTERVAL).await;
    }
}

fn format_watch_paths(paths: &[PathBuf]) -> String {
    const MAX_DISPLAYED: usize = 3;

    let rendered = paths
        .iter()
        .take(MAX_DISPLAYED)
        .map(|path| path.display().to_string())
        .collect::<Vec<_>>();

    if paths.len() > MAX_DISPLAYED {
        format!(
            "{} and {} more path(s)",
            rendered.join(", "),
            paths.len() - MAX_DISPLAYED
        )
    } else {
        rendered.join(", ")
    }
}

async fn build_env(base: &BaseArgs) -> Result<Vec<(String, String)>> {
    let mut envs = resolved_auth_env(base).await?;
    if let Some(project) = base.project.as_ref() {
        envs.push(("BRAINTRUST_DEFAULT_PROJECT".to_string(), project.clone()));
    }
    Ok(envs)
}

fn detect_eval_language(
    files: &[String],
    language_override: Option<EvalLanguage>,
) -> Result<EvalLanguage> {
    if let Some(language) = language_override {
        return Ok(language);
    }

    let mut detected: Option<EvalLanguage> = None;
    for file in files {
        let ext = PathBuf::from(file)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();
        let current = match ext.as_str() {
            "py" => EvalLanguage::Python,
            "ts" | "tsx" | "js" | "mjs" | "cjs" => EvalLanguage::JavaScript,
            _ => {
                anyhow::bail!("Unsupported eval file extension: {ext}");
            }
        };
        if let Some(existing) = detected {
            if existing != current {
                anyhow::bail!(
                    "Mixed eval file types are not supported yet (found {existing:?} and {current:?})."
                );
            }
        } else {
            detected = Some(current);
        }
    }

    detected.ok_or_else(|| anyhow::anyhow!("No eval files provided"))
}

fn build_js_command(
    runner_override: Option<String>,
    runner: &PathBuf,
    files: &[String],
) -> Result<Command> {
    let command = if let Some(explicit) = runner_override.as_deref() {
        let resolved_runner = resolve_js_runner_command(explicit, files);
        if is_deno_runner(explicit) || is_deno_runner_path(resolved_runner.as_ref()) {
            let runner_script = prepare_js_runner_in_cwd()?;
            build_deno_js_command(resolved_runner.as_os_str(), &runner_script, files)
        } else {
            let runner_script = select_js_runner_entrypoint(runner, resolved_runner.as_ref())?;
            let mut command = Command::new(resolved_runner);
            command.arg(runner_script).args(files);
            command
        }
    } else if let Some(auto_runner) = find_js_runner_binary(files) {
        if is_deno_runner_path(&auto_runner) {
            let runner_script = prepare_js_runner_in_cwd()?;
            build_deno_js_command(auto_runner.as_os_str(), &runner_script, files)
        } else {
            let runner_script = select_js_runner_entrypoint(runner, auto_runner.as_ref())?;
            let mut command = Command::new(auto_runner);
            command.arg(runner_script).args(files);
            command
        }
    } else {
        let mut command = Command::new("npx");
        command.arg("--yes").arg("tsx").arg(runner).args(files);
        command
    };

    Ok(command)
}

fn build_deno_js_command(
    deno_runner: impl AsRef<OsStr>,
    runner: &Path,
    files: &[String],
) -> Command {
    let mut command = Command::new(deno_runner);
    command.args(deno_js_command_args(runner, files));
    command
}

fn deno_js_command_args(runner: &Path, files: &[String]) -> Vec<OsString> {
    let mut args = vec![
        OsString::from("run"),
        OsString::from("-A"),
        OsString::from("--node-modules-dir=auto"),
        OsString::from("--unstable-detect-cjs"),
        runner.as_os_str().to_os_string(),
    ];
    args.extend(files.iter().map(OsString::from));
    args
}

fn build_python_command(
    runner_override: Option<String>,
    runner: &PathBuf,
    files: &[String],
) -> Result<Command> {
    let runner_override = runner_override
        .or_else(|| std::env::var("BT_EVAL_PYTHON_RUNNER").ok())
        .or_else(|| std::env::var("BT_EVAL_PYTHON").ok());

    let command = if let Some(explicit) = runner_override {
        let mut command = Command::new(explicit);
        command.arg(runner).args(files);
        command
    } else if let Some(python) = find_python_binary() {
        let mut command = Command::new(python);
        command.arg(runner).args(files);
        command
    } else {
        anyhow::bail!(
            "No Python interpreter found in PATH. Please install python or pass --runner."
        );
    };

    Ok(command)
}

fn find_js_runner_binary(files: &[String]) -> Option<PathBuf> {
    // Prefer local project bins first, then PATH. `tsx` remains the preferred
    // default, with other common TS runners as fallback.
    const RUNNER_CANDIDATES: &[&str] = &["tsx", "vite-node", "ts-node", "ts-node-esm", "deno"];

    for candidate in RUNNER_CANDIDATES {
        if let Some(path) = find_node_module_bin_for_files(candidate, files) {
            return Some(path);
        }
    }

    find_binary_in_path(RUNNER_CANDIDATES)
}

fn resolve_js_runner_command(runner: &str, files: &[String]) -> PathBuf {
    if is_path_like_runner(runner) {
        return PathBuf::from(runner);
    }

    find_node_module_bin_for_files(runner, files)
        .or_else(|| find_binary_in_path(&[runner]))
        .unwrap_or_else(|| PathBuf::from(runner))
}

fn is_path_like_runner(runner: &str) -> bool {
    let path = Path::new(runner);
    path.is_absolute() || runner.contains('/') || runner.contains('\\') || runner.starts_with('.')
}

fn find_node_module_bin_for_files(binary: &str, files: &[String]) -> Option<PathBuf> {
    let search_roots = js_runner_search_roots(files);
    for root in &search_roots {
        if let Some(path) = find_node_module_bin(binary, root) {
            return Some(path);
        }
    }
    None
}

fn js_runner_search_roots(files: &[String]) -> Vec<PathBuf> {
    let mut search_roots = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        search_roots.push(cwd.clone());
        for file in files {
            let path = PathBuf::from(file);
            let absolute = if path.is_absolute() {
                path
            } else {
                cwd.join(path)
            };
            if let Some(parent) = absolute.parent() {
                search_roots.push(parent.to_path_buf());
            }
        }
    }
    search_roots
}

fn is_deno_runner(runner: &str) -> bool {
    let file_name = Path::new(runner)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(runner);
    file_name.eq_ignore_ascii_case("deno") || file_name.eq_ignore_ascii_case("deno.exe")
}

fn is_deno_runner_path(runner: &Path) -> bool {
    runner
        .file_name()
        .and_then(|value| value.to_str())
        .map(|name| name.eq_ignore_ascii_case("deno") || name.eq_ignore_ascii_case("deno.exe"))
        .unwrap_or(false)
}

fn select_js_runner_entrypoint(default_runner: &Path, runner_command: &Path) -> Result<PathBuf> {
    if is_ts_node_runner(runner_command) {
        return prepare_js_runner_in_cwd();
    }
    Ok(default_runner.to_path_buf())
}

fn prepare_js_runner_in_cwd() -> Result<PathBuf> {
    let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
    let cache_dir = cwd
        .join(".bt")
        .join("eval-runners")
        .join(env!("CARGO_PKG_VERSION"));
    std::fs::create_dir_all(&cache_dir).with_context(|| {
        format!(
            "failed to create eval runner cache dir {}",
            cache_dir.display()
        )
    })?;
    materialize_runner_script(&cache_dir, JS_RUNNER_FILE, JS_RUNNER_SOURCE)
}

fn is_ts_node_runner(runner_command: &Path) -> bool {
    let file_name = match runner_command.file_name().and_then(|name| name.to_str()) {
        Some(name) => name.to_ascii_lowercase(),
        None => return false,
    };

    let normalized = file_name.strip_suffix(".cmd").unwrap_or(&file_name);
    normalized == "ts-node" || normalized == "ts-node-esm"
}

fn find_python_binary() -> Option<PathBuf> {
    if let Some(venv_root) = std::env::var_os("VIRTUAL_ENV") {
        let candidate = PathBuf::from(venv_root).join("bin").join("python");
        if candidate.is_file() {
            return Some(candidate);
        }
    }
    find_binary_in_path(&["python3", "python"])
}

fn find_node_module_bin(binary: &str, start: &Path) -> Option<PathBuf> {
    let mut current = Some(start);
    while let Some(dir) = current {
        let base = dir.join("node_modules").join(".bin").join(binary);
        if base.is_file() {
            return Some(base);
        }
        if cfg!(windows) {
            let cmd = base.with_extension("cmd");
            if cmd.is_file() {
                return Some(cmd);
            }
        }
        current = dir.parent();
    }
    None
}

fn find_binary_in_path(candidates: &[&str]) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&paths) {
        for candidate in candidates {
            let path = dir.join(candidate);
            if path.is_file() {
                return Some(path);
            }
            if cfg!(windows) {
                let cmd = path.with_extension("cmd");
                if cmd.is_file() {
                    return Some(cmd);
                }
            }
        }
    }
    None
}

fn build_sse_socket_path() -> Result<PathBuf> {
    let pid = std::process::id();
    let serial = SSE_SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("failed to read system time")?
        .as_nanos();
    Ok(std::env::temp_dir().join(format!("bt-eval-{pid}-{now}-{serial}.sock")))
}

fn bind_sse_listener() -> Result<(UnixListener, PathBuf, SocketCleanupGuard)> {
    let mut last_bind_err: Option<std::io::Error> = None;
    for _ in 0..SSE_SOCKET_BIND_MAX_ATTEMPTS {
        let socket_path = build_sse_socket_path()?;
        let socket_cleanup_guard = SocketCleanupGuard::new(socket_path.clone());
        let _ = std::fs::remove_file(&socket_path);
        match UnixListener::bind(&socket_path) {
            Ok(listener) => return Ok((listener, socket_path, socket_cleanup_guard)),
            Err(err)
                if matches!(
                    err.kind(),
                    std::io::ErrorKind::AlreadyExists | std::io::ErrorKind::AddrInUse
                ) =>
            {
                last_bind_err = Some(err);
                continue;
            }
            Err(err) => {
                return Err(err).context("failed to bind SSE unix socket");
            }
        }
    }
    let err = last_bind_err.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AddrInUse,
            "failed to allocate a unique SSE socket path",
        )
    });
    Err(err).context(format!(
        "failed to bind SSE unix socket after {SSE_SOCKET_BIND_MAX_ATTEMPTS} attempts"
    ))
}

fn eval_runner_cache_dir() -> PathBuf {
    let root = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))
        .unwrap_or_else(std::env::temp_dir);

    root.join("bt")
        .join("eval-runners")
        .join(env!("CARGO_PKG_VERSION"))
}

fn prepare_eval_runners() -> Result<(PathBuf, PathBuf)> {
    prepare_eval_runners_in_dir(&eval_runner_cache_dir())
}

fn prepare_eval_runners_in_dir(cache_dir: &Path) -> Result<(PathBuf, PathBuf)> {
    std::fs::create_dir_all(cache_dir).with_context(|| {
        format!(
            "failed to create eval runner cache dir {}",
            cache_dir.display()
        )
    })?;

    let js_runner = materialize_runner_script(cache_dir, JS_RUNNER_FILE, JS_RUNNER_SOURCE)?;
    let py_runner = materialize_runner_script(cache_dir, PY_RUNNER_FILE, PY_RUNNER_SOURCE)?;
    Ok((js_runner, py_runner))
}

fn materialize_runner_script(cache_dir: &Path, file_name: &str, source: &str) -> Result<PathBuf> {
    let path = cache_dir.join(file_name);
    let current = std::fs::read_to_string(&path).ok();
    if current.as_deref() != Some(source) {
        std::fs::write(&path, source)
            .with_context(|| format!("failed to write eval runner script {}", path.display()))?;
    }
    Ok(path)
}

#[derive(Debug)]
enum EvalEvent {
    Start(ExperimentSummary),
    Summary(ExperimentSummary),
    Progress(SseProgressEventData),
    Dependencies {
        files: Vec<String>,
    },
    Done,
    Error {
        message: String,
        stack: Option<String>,
        status: Option<u16>,
    },
    Console {
        stream: String,
        message: String,
    },
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ExperimentSummary {
    project_name: String,
    experiment_name: String,
    project_id: Option<String>,
    experiment_id: Option<String>,
    project_url: Option<String>,
    experiment_url: Option<String>,
    comparison_experiment_name: Option<String>,
    scores: HashMap<String, ScoreSummary>,
    metrics: Option<HashMap<String, MetricSummary>>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ScoreSummary {
    name: String,
    score: f64,
    diff: Option<f64>,
    improvements: i64,
    regressions: i64,
}

#[derive(Debug, Deserialize)]
struct EvalErrorPayload {
    message: String,
    stack: Option<String>,
    status: Option<u16>,
}

#[derive(Debug, Deserialize, Serialize)]
struct MetricSummary {
    name: String,
    metric: f64,
    unit: String,
    diff: Option<f64>,
    improvements: i64,
    regressions: i64,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize, Serialize)]
struct SseProgressEventData {
    id: String,
    object_type: String,
    origin: Option<serde_json::Value>,
    format: String,
    output_type: String,
    name: String,
    event: String,
    data: String,
}

#[derive(Debug, Deserialize)]
struct EvalProgressData {
    #[serde(rename = "type")]
    kind_type: String,
    kind: String,
    total: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct SseConsoleEventData {
    stream: String,
    message: String,
}

#[derive(Debug, Deserialize)]
struct SseDependenciesEventData {
    files: Vec<String>,
}

async fn forward_stream<T>(
    stream: T,
    name: &'static str,
    tx: mpsc::UnboundedSender<EvalEvent>,
) -> Result<()>
where
    T: tokio::io::AsyncRead + Unpin,
{
    let mut lines = BufReader::new(stream).lines();
    while let Some(line) = lines.next_line().await? {
        let _ = tx.send(EvalEvent::Console {
            stream: name.to_string(),
            message: line,
        });
    }
    Ok(())
}

async fn read_sse_stream<T>(stream: T, tx: mpsc::UnboundedSender<EvalEvent>) -> Result<()>
where
    T: tokio::io::AsyncRead + Unpin,
{
    let mut lines = BufReader::new(stream).lines();
    let mut event: Option<String> = None;
    let mut data_lines: Vec<String> = Vec::new();

    while let Some(line) = lines.next_line().await? {
        if line.is_empty() {
            if event.is_some() || !data_lines.is_empty() {
                let data = data_lines.join("\n");
                handle_sse_event(event.take(), data, &tx);
                data_lines.clear();
            }
            continue;
        }

        if let Some(value) = line.strip_prefix("event:") {
            event = Some(value.trim().to_string());
        } else if let Some(value) = line.strip_prefix("data:") {
            data_lines.push(value.trim_start().to_string());
        }
    }

    if event.is_some() || !data_lines.is_empty() {
        let data = data_lines.join("\n");
        handle_sse_event(event.take(), data, &tx);
    }

    Ok(())
}

fn handle_sse_event(event: Option<String>, data: String, tx: &mpsc::UnboundedSender<EvalEvent>) {
    let event_name = event.unwrap_or_default();
    match event_name.as_str() {
        "start" => {
            if let Ok(summary) = serde_json::from_str::<ExperimentSummary>(&data) {
                let _ = tx.send(EvalEvent::Start(summary));
            }
        }
        "summary" => {
            if let Ok(summary) = serde_json::from_str::<ExperimentSummary>(&data) {
                let _ = tx.send(EvalEvent::Summary(summary));
            }
        }
        "progress" => {
            if let Ok(progress) = serde_json::from_str::<SseProgressEventData>(&data) {
                let _ = tx.send(EvalEvent::Progress(progress));
            }
        }
        "console" => {
            if let Ok(console) = serde_json::from_str::<SseConsoleEventData>(&data) {
                let _ = tx.send(EvalEvent::Console {
                    stream: console.stream,
                    message: console.message,
                });
            }
        }
        "error" => {
            if let Ok(payload) = serde_json::from_str::<EvalErrorPayload>(&data) {
                let _ = tx.send(EvalEvent::Error {
                    message: payload.message,
                    stack: payload.stack,
                    status: payload.status,
                });
            } else {
                let _ = tx.send(EvalEvent::Error {
                    message: data,
                    stack: None,
                    status: None,
                });
            }
        }
        "dependencies" => {
            if let Ok(payload) = serde_json::from_str::<SseDependenciesEventData>(&data) {
                let _ = tx.send(EvalEvent::Dependencies {
                    files: payload.files,
                });
            }
        }
        "done" => {
            let _ = tx.send(EvalEvent::Done);
        }
        _ => {}
    }
}

struct EvalUi {
    progress: MultiProgress,
    bars: HashMap<String, ProgressBar>,
    bar_style: ProgressStyle,
    spinner_style: ProgressStyle,
    jsonl: bool,
    list: bool,
}

impl EvalUi {
    fn new(jsonl: bool, list: bool) -> Self {
        let progress = MultiProgress::with_draw_target(ProgressDrawTarget::stderr_with_hz(10));
        let bar_style =
            ProgressStyle::with_template("{bar:10.blue} {msg} {percent}% {pos}/{len} {eta}")
                .unwrap();
        let spinner_style = ProgressStyle::with_template("{spinner} {msg}").unwrap();
        Self {
            progress,
            bars: HashMap::new(),
            bar_style,
            spinner_style,
            jsonl,
            list,
        }
    }

    fn finish(&mut self) {
        for (_, bar) in self.bars.drain() {
            bar.finish_and_clear();
        }
    }

    fn handle(&mut self, event: EvalEvent) {
        match event {
            EvalEvent::Start(summary) => {
                let line = format_start_line(&summary);
                let _ = self.progress.println(line);
            }
            EvalEvent::Summary(summary) => {
                if self.jsonl {
                    if let Ok(line) = serde_json::to_string(&summary) {
                        println!("{line}");
                    }
                } else {
                    let rendered = format_experiment_summary(&summary);
                    for line in rendered.lines() {
                        let _ = self.progress.println(line);
                    }
                }
            }
            EvalEvent::Progress(progress) => {
                self.handle_progress(progress);
            }
            EvalEvent::Dependencies { .. } => {}
            EvalEvent::Console { stream, message } => {
                if stream == "stdout" && (self.list || self.jsonl) {
                    println!("{message}");
                } else {
                    let _ = self.progress.println(message);
                }
            }
            EvalEvent::Error { message, stack, .. } => {
                let show_hint = message.contains("Please specify an api key");
                let line = message.as_str().red().to_string();
                let _ = self.progress.println(line);
                if let Some(stack) = stack {
                    for line in stack.lines() {
                        let _ = self.progress.println(line.dark_grey().to_string());
                    }
                }
                if show_hint {
                    let hint = "Hint: pass --api-key, set BRAINTRUST_API_KEY, run `bt login`/`bt login set --oauth`, or use --no-send-logs for local evals.";
                    let _ = self.progress.println(hint.dark_grey().to_string());
                }
            }
            EvalEvent::Done => {
                self.finish();
            }
        }
    }

    fn handle_progress(&mut self, progress: SseProgressEventData) {
        let payload = match serde_json::from_str::<EvalProgressData>(&progress.data) {
            Ok(payload) if payload.kind_type == "eval_progress" => payload,
            _ => return,
        };

        match payload.kind.as_str() {
            "start" => {
                let bar = if let Some(total) = payload.total {
                    if total > 0 {
                        let bar = self.progress.add(ProgressBar::new(total));
                        bar.set_style(self.bar_style.clone());
                        bar
                    } else {
                        let bar = self.progress.add(ProgressBar::new_spinner());
                        bar.set_style(self.spinner_style.clone());
                        bar
                    }
                } else {
                    let bar = self.progress.add(ProgressBar::new_spinner());
                    bar.set_style(self.spinner_style.clone());
                    bar
                };
                bar.set_message(fit_name_to_spaces(&progress.name, MAX_NAME_LENGTH));
                self.bars.insert(progress.name.clone(), bar);
            }
            "increment" => {
                if let Some(bar) = self.bars.get(&progress.name) {
                    bar.inc(1);
                    bar.set_message(fit_name_to_spaces(&progress.name, MAX_NAME_LENGTH));
                }
            }
            "set_total" => {
                if let Some(bar) = self.bars.get(&progress.name) {
                    if let Some(total) = payload.total {
                        bar.set_length(total);
                        bar.set_style(self.bar_style.clone());
                    }
                }
            }
            "stop" => {
                if let Some(bar) = self.bars.remove(&progress.name) {
                    bar.finish_and_clear();
                }
            }
            _ => {}
        }
    }
}

fn fit_name_to_spaces(name: &str, length: usize) -> String {
    let mut padded = name.to_string();
    let char_count = padded.chars().count();
    if char_count < length {
        padded.push_str(&" ".repeat(length - char_count));
        return padded;
    }
    if char_count <= length {
        return padded;
    }
    if length <= 3 {
        return padded.chars().take(length).collect();
    }
    let truncated: String = padded.chars().take(length - 3).collect();
    format!("{truncated}...")
}

fn format_start_line(summary: &ExperimentSummary) -> String {
    let arrow = "▶".cyan();
    let name = summary.experiment_name.as_str().bold();
    let link = summary.experiment_url.as_deref().unwrap_or("locally");
    format!("{arrow} Experiment {name} is running at {link}")
}

fn format_experiment_summary(summary: &ExperimentSummary) -> String {
    let mut parts: Vec<String> = Vec::new();

    if let Some(comparison) = summary.comparison_experiment_name.as_deref() {
        let line = format!(
            "{baseline} {baseline_tag} ← {comparison_name} {comparison_tag}",
            baseline = comparison,
            baseline_tag = "(baseline)".dark_grey(),
            comparison_name = summary.experiment_name,
            comparison_tag = "(comparison)".dark_grey(),
        );
        parts.push(line);
    }

    let has_scores = !summary.scores.is_empty();
    let has_metrics = summary
        .metrics
        .as_ref()
        .map(|metrics| !metrics.is_empty())
        .unwrap_or(false);

    if has_scores || has_metrics {
        let has_comparison = summary.comparison_experiment_name.is_some();
        let mut rows: Vec<Vec<Line>> = Vec::new();

        let header = if has_comparison {
            Some(vec![
                header_line("Name"),
                header_line("Value"),
                header_line("Change"),
                header_line("Improvements"),
                header_line("Regressions"),
            ])
        } else {
            None
        };

        let mut score_values: Vec<_> = summary.scores.values().collect();
        score_values.sort_by(|a, b| a.name.cmp(&b.name));
        for score in score_values {
            let score_percent =
                Line::from(format!("{:.2}%", score.score * 100.0)).alignment(Alignment::Right);
            let diff = format_diff_line(score.diff);
            let improvements = format_improvements_line(score.improvements);
            let regressions = format_regressions_line(score.regressions);
            let name = truncate_plain(&score.name, MAX_NAME_LENGTH);
            let name = Line::from(vec![
                Span::styled("◯", Style::default().fg(Color::Blue)),
                Span::raw(" "),
                Span::raw(name),
            ]);
            if has_comparison {
                rows.push(vec![name, score_percent, diff, improvements, regressions]);
            } else {
                rows.push(vec![name, score_percent]);
            }
        }

        if let Some(metrics) = &summary.metrics {
            let mut metric_values: Vec<_> = metrics.values().collect();
            metric_values.sort_by(|a, b| a.name.cmp(&b.name));
            for metric in metric_values {
                let formatted_value = Line::from(format_metric_value(metric.metric, &metric.unit))
                    .alignment(Alignment::Right);
                let diff = format_diff_line(metric.diff);
                let improvements = format_improvements_line(metric.improvements);
                let regressions = format_regressions_line(metric.regressions);
                let name = truncate_plain(&metric.name, MAX_NAME_LENGTH);
                let name = Line::from(vec![
                    Span::styled("◯", Style::default().fg(Color::Magenta)),
                    Span::raw(" "),
                    Span::raw(name),
                ]);
                if has_comparison {
                    rows.push(vec![name, formatted_value, diff, improvements, regressions]);
                } else {
                    rows.push(vec![name, formatted_value]);
                }
            }
        }

        parts.push(render_table_ratatui(header, rows, has_comparison));
    }

    if let Some(url) = &summary.experiment_url {
        parts.push(format!("See results at {url}"));
    }

    let content = parts.join("\n\n");
    box_with_title("Experiment summary", &content)
}

fn format_diff_line(diff: Option<f64>) -> Line<'static> {
    match diff {
        Some(value) => {
            let sign = if value > 0.0 { "+" } else { "" };
            let percent = format!("{sign}{:.2}%", value * 100.0);
            let style = if value > 0.0 {
                Style::default().fg(Color::Green)
            } else {
                Style::default().fg(Color::Red)
            };
            Line::from(Span::styled(percent, style)).alignment(Alignment::Right)
        }
        None => Line::from(Span::styled("-", Style::default().fg(Color::DarkGray)))
            .alignment(Alignment::Right),
    }
}

fn format_improvements_line(value: i64) -> Line<'static> {
    if value > 0 {
        Line::from(Span::styled(
            value.to_string(),
            Style::default()
                .fg(Color::Green)
                .add_modifier(Modifier::DIM),
        ))
        .alignment(Alignment::Right)
    } else {
        Line::from(Span::styled("-", Style::default().fg(Color::DarkGray)))
            .alignment(Alignment::Right)
    }
}

fn format_regressions_line(value: i64) -> Line<'static> {
    if value > 0 {
        Line::from(Span::styled(
            value.to_string(),
            Style::default().fg(Color::Red).add_modifier(Modifier::DIM),
        ))
        .alignment(Alignment::Right)
    } else {
        Line::from(Span::styled("-", Style::default().fg(Color::DarkGray)))
            .alignment(Alignment::Right)
    }
}

fn format_metric_value(metric: f64, unit: &str) -> String {
    let formatted = if metric.fract() == 0.0 {
        format!("{metric:.0}")
    } else {
        format!("{metric:.2}")
    };
    if unit == "$" {
        format!("{unit}{formatted}")
    } else {
        format!("{formatted}{unit}")
    }
}

fn render_table_ratatui(
    header: Option<Vec<Line<'static>>>,
    rows: Vec<Vec<Line<'static>>>,
    has_comparison: bool,
) -> String {
    if rows.is_empty() {
        return String::new();
    }

    let columns = if has_comparison { 5 } else { 2 };
    let mut widths = vec![0usize; columns];

    if let Some(header_row) = &header {
        for (idx, line) in header_row.iter().enumerate().take(columns) {
            widths[idx] = widths[idx].max(line.width());
        }
    }

    for row in &rows {
        for (idx, line) in row.iter().enumerate().take(columns) {
            widths[idx] = widths[idx].max(line.width());
        }
    }

    let column_spacing = 2;
    let total_width = widths.iter().sum::<usize>() + column_spacing * (columns - 1);
    let mut height = rows.len();
    if header.is_some() {
        height += 1;
    }
    let backend = TestBackend::new(total_width as u16, height as u16);
    let mut terminal = Terminal::new(backend).expect("failed to create table backend");

    let table_rows = rows.into_iter().map(|row| {
        let cells = row.into_iter().map(Cell::new).collect::<Vec<_>>();
        Row::new(cells)
    });

    let mut table = Table::new(
        table_rows,
        widths.iter().map(|w| Constraint::Length(*w as u16)),
    )
    .column_spacing(column_spacing as u16);

    if let Some(header_row) = header {
        let header_cells = header_row.into_iter().map(Cell::new).collect::<Vec<_>>();
        table = table.header(Row::new(header_cells));
    }

    terminal
        .draw(|frame| {
            let area = frame.area();
            frame.render_widget(table, area);
        })
        .expect("failed to render table");

    let buffer = terminal.backend().buffer();
    buffer_to_ansi_lines(buffer).join("\n")
}

fn header_line(text: &str) -> Line<'static> {
    Line::from(Span::styled(
        text.to_string(),
        Style::default()
            .fg(Color::DarkGray)
            .add_modifier(Modifier::BOLD),
    ))
}

fn truncate_plain(text: &str, max_len: usize) -> String {
    if text.chars().count() <= max_len {
        return text.to_string();
    }
    if max_len <= 3 {
        return text.chars().take(max_len).collect();
    }
    let truncated: String = text.chars().take(max_len - 3).collect();
    format!("{truncated}...")
}

fn box_with_title(title: &str, content: &str) -> String {
    let lines: Vec<&str> = content.lines().collect();
    let content_width = lines
        .iter()
        .map(|line| visible_width(line))
        .max()
        .unwrap_or(0);
    let padding = 1;
    let inner_width = content_width + padding * 2;

    let title_plain = format!(" {title} ");
    let title_width = visible_width(&title_plain);
    let mut top = String::from("╭");
    top.push_str(&title_plain.dark_grey().to_string());
    if inner_width > title_width {
        top.push_str(&"─".repeat(inner_width - title_width));
    }
    top.push('╮');

    let mut boxed = vec![top];
    for line in lines {
        let line_width = visible_width(line);
        // Defensive: if width accounting ever drifts (e.g. escape-sequence parsing),
        // avoid underflow and render without extra trailing padding.
        let right_padding = inner_width.saturating_sub(line_width + padding);
        let mut row = String::from("│");
        row.push_str(&" ".repeat(padding));
        row.push_str(line);
        row.push_str(&" ".repeat(right_padding));
        row.push('│');
        boxed.push(row);
    }

    let bottom = format!("╰{}╯", "─".repeat(inner_width));
    boxed.push(bottom);

    format!("\n{}", boxed.join("\n"))
}

fn visible_width(text: &str) -> usize {
    let stripped = strip(text.as_bytes());
    let stripped = String::from_utf8_lossy(&stripped);
    UnicodeWidthStr::width(stripped.as_ref())
}

fn buffer_to_ansi_lines(buffer: &ratatui::buffer::Buffer) -> Vec<String> {
    let width = buffer.area.width as usize;
    let height = buffer.area.height as usize;
    let mut lines = Vec::with_capacity(height);
    let mut current_style = Style::reset();

    for y in 0..height {
        let mut line = String::new();
        let mut skip = 0usize;
        for x in 0..width {
            let cell = &buffer[(x as u16, y as u16)];
            let symbol = cell.symbol();
            let symbol_width = UnicodeWidthStr::width(symbol);
            if skip > 0 {
                skip -= 1;
                continue;
            }

            let style = Style {
                fg: Some(cell.fg),
                bg: Some(cell.bg),
                add_modifier: cell.modifier,
                ..Style::default()
            };

            if style != current_style {
                line.push_str(&style_to_ansi(style));
                current_style = style;
            }

            line.push_str(symbol);
            skip = symbol_width.saturating_sub(1);
        }
        line.push_str(&style_to_ansi(Style::reset()));
        lines.push(line.trim_end().to_string());
    }

    lines
}

fn style_to_ansi(style: Style) -> String {
    let mut buf = Vec::new();
    let _ = queue!(buf, SetAttribute(Attribute::Reset), ResetColor);

    if let Some(fg) = style.fg {
        let _ = queue!(buf, SetForegroundColor(convert_color(fg)));
    }
    if let Some(bg) = style.bg {
        let _ = queue!(buf, SetBackgroundColor(convert_color(bg)));
    }

    let mods = style.add_modifier;
    if mods.contains(Modifier::BOLD) {
        let _ = queue!(buf, SetAttribute(Attribute::Bold));
    }
    if mods.contains(Modifier::DIM) {
        let _ = queue!(buf, SetAttribute(Attribute::Dim));
    }
    if mods.contains(Modifier::ITALIC) {
        let _ = queue!(buf, SetAttribute(Attribute::Italic));
    }
    if mods.contains(Modifier::UNDERLINED) {
        let _ = queue!(buf, SetAttribute(Attribute::Underlined));
    }
    if mods.contains(Modifier::REVERSED) {
        let _ = queue!(buf, SetAttribute(Attribute::Reverse));
    }
    if mods.contains(Modifier::CROSSED_OUT) {
        let _ = queue!(buf, SetAttribute(Attribute::CrossedOut));
    }
    if mods.contains(Modifier::SLOW_BLINK) {
        let _ = queue!(buf, SetAttribute(Attribute::SlowBlink));
    }
    if mods.contains(Modifier::RAPID_BLINK) {
        let _ = queue!(buf, SetAttribute(Attribute::RapidBlink));
    }

    String::from_utf8_lossy(&buf).to_string()
}

fn convert_color(color: Color) -> CtColor {
    match color {
        Color::Reset => CtColor::Reset,
        Color::Black => CtColor::Black,
        Color::Red => CtColor::Red,
        Color::Green => CtColor::Green,
        Color::Yellow => CtColor::Yellow,
        Color::Blue => CtColor::Blue,
        Color::Magenta => CtColor::Magenta,
        Color::Cyan => CtColor::Cyan,
        Color::Gray => CtColor::Grey,
        Color::DarkGray => CtColor::DarkGrey,
        Color::LightRed => CtColor::Red,
        Color::LightGreen => CtColor::Green,
        Color::LightYellow => CtColor::Yellow,
        Color::LightBlue => CtColor::Blue,
        Color::LightMagenta => CtColor::Magenta,
        Color::LightCyan => CtColor::Cyan,
        Color::White => CtColor::White,
        Color::Indexed(value) => CtColor::AnsiValue(value),
        Color::Rgb(r, g, b) => CtColor::Rgb { r, g, b },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_temp_dir(prefix: &str) -> PathBuf {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "bt-eval-tests-{prefix}-{}-{now}",
            std::process::id()
        ));
        fs::create_dir_all(&path).expect("create temp dir");
        path
    }

    #[test]
    fn materialize_runner_script_writes_file() {
        let dir = make_temp_dir("write");

        let path = materialize_runner_script(&dir, "runner.ts", "console.log('ok');")
            .expect("runner script should be materialized");
        let contents = fs::read_to_string(path).expect("runner script should be readable");
        assert_eq!(contents, "console.log('ok');");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn materialize_runner_script_overwrites_stale_content() {
        let dir = make_temp_dir("overwrite");
        let path = dir.join("runner.py");
        fs::write(&path, "stale").expect("stale file should be written");

        materialize_runner_script(&dir, "runner.py", "fresh")
            .expect("runner script should be updated");
        let contents = fs::read_to_string(path).expect("runner script should be readable");
        assert_eq!(contents, "fresh");

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn prepare_eval_runners_writes_embedded_scripts() {
        let dir = make_temp_dir("embedded");
        let (js_runner, py_runner) =
            prepare_eval_runners_in_dir(&dir).expect("embedded runners should be materialized");

        let js = fs::read_to_string(js_runner).expect("js runner should be readable");
        let py = fs::read_to_string(py_runner).expect("python runner should be readable");
        assert_eq!(js, JS_RUNNER_SOURCE);
        assert_eq!(py, PY_RUNNER_SOURCE);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_js_runner_command_finds_local_node_module_bin() {
        let dir = make_temp_dir("resolve-runner");
        let eval_dir = dir.join("evals");
        let bin_dir = dir.join("node_modules").join(".bin");
        std::fs::create_dir_all(&eval_dir).expect("eval dir should be created");
        std::fs::create_dir_all(&bin_dir).expect("bin dir should be created");
        let local_runner = bin_dir.join("vite-node");
        std::fs::write(&local_runner, "echo").expect("local runner should be written");

        let file = eval_dir.join("sample.eval.ts");
        let files = vec![file.to_string_lossy().to_string()];

        let resolved = resolve_js_runner_command("vite-node", &files);
        assert_eq!(resolved, local_runner);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn box_with_title_handles_ansi_content_without_panicking() {
        let content = "plain line\n\x1b[38;5;196mred text\x1b[0m";
        let boxed = box_with_title("Summary", content);
        assert!(boxed.contains("Summary"));
        assert!(boxed.contains("plain line"));
        assert!(boxed.contains("red text"));
    }

    #[test]
    fn detect_watch_changes_detects_file_create() {
        let dir = make_temp_dir("create");
        let file = dir.join("watch.eval.ts");
        let paths = vec![file.clone()];

        let mut state = snapshot_watch_state(&paths).expect("snapshot watch state");
        assert!(detect_watch_changes(&paths, &mut state)
            .expect("check changes")
            .is_empty());

        fs::write(&file, "export {}").expect("write test file");
        let changed = detect_watch_changes(&paths, &mut state).expect("check changes");
        assert_eq!(changed, vec![file.clone()]);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn detect_watch_changes_detects_file_update() {
        let dir = make_temp_dir("update");
        let file = dir.join("watch.eval.ts");
        fs::write(&file, "export const v = 1;").expect("write initial file");
        let paths = vec![file.clone()];

        let mut state = snapshot_watch_state(&paths).expect("snapshot watch state");
        assert!(detect_watch_changes(&paths, &mut state)
            .expect("check changes")
            .is_empty());

        fs::write(&file, "export const value = 2;").expect("write updated file");
        let changed = detect_watch_changes(&paths, &mut state).expect("check changes");
        assert_eq!(changed, vec![file.clone()]);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn merge_watch_paths_dedupes_and_includes_dependencies() {
        let input = vec![
            PathBuf::from("/tmp/a.eval.ts"),
            PathBuf::from("/tmp/b.eval.ts"),
        ];
        let deps = vec![
            PathBuf::from("/tmp/b.eval.ts"),
            PathBuf::from("/tmp/helper.ts"),
        ];

        let merged = merge_watch_paths(&input, &deps);
        assert_eq!(
            merged,
            vec![
                PathBuf::from("/tmp/a.eval.ts"),
                PathBuf::from("/tmp/b.eval.ts"),
                PathBuf::from("/tmp/helper.ts")
            ]
        );
    }

    #[test]
    fn collect_js_static_dependencies_follows_local_imports() {
        let dir = make_temp_dir("js-static");
        let entry = dir.join("entry.eval.ts");
        let helper = dir.join("helper.js");

        fs::write(
            &entry,
            "import { helper } from './helper.js';\nexport default helper;",
        )
        .expect("write entry file");
        fs::write(&helper, "export const helper = 'ok';").expect("write helper file");

        let files = vec![entry.to_string_lossy().to_string()];
        let dependencies = collect_js_static_dependencies(&files).expect("collect js dependencies");

        assert!(dependencies.contains(&entry));
        assert!(dependencies.contains(&helper));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn build_deno_js_command_includes_detect_cjs_flag() {
        let runner = PathBuf::from("/tmp/eval-runner.ts");
        let files = vec!["tests/basic.eval.ts".to_string()];
        let args: Vec<String> = deno_js_command_args(&runner, &files)
            .into_iter()
            .map(|arg| arg.to_string_lossy().to_string())
            .collect();
        assert_eq!(
            args,
            vec![
                "run",
                "-A",
                "--node-modules-dir=auto",
                "--unstable-detect-cjs",
                "/tmp/eval-runner.ts",
                "tests/basic.eval.ts",
            ]
        );
    }

    #[test]
    fn build_sse_socket_path_is_unique_for_consecutive_calls() {
        let first = build_sse_socket_path().expect("first socket path");
        let second = build_sse_socket_path().expect("second socket path");
        assert_ne!(first, second);
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

    #[test]
    fn parse_eval_filter_expression_splits_path_and_pattern() {
        let parsed =
            parse_eval_filter_expression("metadata.case=smoke.*").expect("parse should succeed");
        assert_eq!(
            parsed,
            RunnerFilter {
                path: vec!["metadata".to_string(), "case".to_string()],
                pattern: "smoke.*".to_string(),
            }
        );
    }

    #[test]
    fn parse_eval_filter_expression_rejects_missing_equals() {
        let err =
            parse_eval_filter_expression("metadata.case").expect_err("missing equals should fail");
        assert!(
            err.to_string().contains("Invalid filter"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn parse_eval_filter_expression_rejects_empty_path() {
        let err = parse_eval_filter_expression("=foo").expect_err("empty path should fail");
        assert!(
            err.to_string().contains("Invalid filter"),
            "unexpected error: {err}"
        );
    }
}
