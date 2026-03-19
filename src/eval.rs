use std::collections::{BTreeSet, HashMap, VecDeque};
use std::ffi::{OsStr, OsString};
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use actix_web::dev::Service;
use actix_web::http::header::{
    HeaderName, HeaderValue, ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_HEADERS,
    ACCESS_CONTROL_ALLOW_METHODS, ACCESS_CONTROL_ALLOW_ORIGIN, ACCESS_CONTROL_EXPOSE_HEADERS,
    ACCESS_CONTROL_MAX_AGE, AUTHORIZATION, CACHE_CONTROL, CONNECTION, CONTENT_TYPE, ORIGIN, VARY,
};
use actix_web::{guard, web, App, HttpRequest, HttpResponse, HttpServer};
use anyhow::{Context, Result};
use chrono::{SecondsFormat, Utc};
use clap::{Args, ValueEnum};
use crossterm::queue;
use crossterm::style::{
    Attribute, Color as CtColor, ResetColor, SetAttribute, SetBackgroundColor, SetForegroundColor,
    Stylize,
};
use futures_util::stream;
use futures_util::StreamExt;
use indicatif::{MultiProgress, ProgressBar, ProgressDrawTarget, ProgressStyle};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use strip_ansi_escapes::strip;
use tokio::io::AsyncWriteExt;
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
use crate::auth::login;
use crate::auth::resolved_auth_env;
use crate::experiments::api::create_experiment;
use crate::functions::publish_eval_sandbox_functions;
use crate::http::ApiClient;
use crate::source_language::SourceLanguage;
use crate::ui::{animations_enabled, is_quiet};

const MAX_NAME_LENGTH: usize = 40;
const WATCH_POLL_INTERVAL: Duration = Duration::from_millis(500);
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
const SSE_SOCKET_BIND_MAX_ATTEMPTS: u8 = 16;
const EVAL_NODE_MAX_OLD_SPACE_SIZE_MB: usize = 8192;
const MAX_DEFERRED_EVAL_ERRORS: usize = 8;
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
    error_messages: Vec<String>,
    stderr_lines: Vec<String>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RetryPolicy {
    Allow,
    Disallow,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum JsMode {
    Auto,
    ForceEsm,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ConsolePolicy {
    Forward,
    BufferStderr,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunnerKind {
    Tsx,
    ViteNode,
    Deno,
    Bun,
    Other,
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

#[derive(Debug, Serialize, Deserialize)]
struct EvalPullRequest {
    name: String,
    #[serde(default)]
    parameters: Option<Value>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EvalPullClientMessage {
    Next,
    Close,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
enum EvalPullResponse {
    Ready {
        evaluator_name: String,
        max_concurrency: usize,
        experiment_name: String,
    },
    Row {
        datum: Value,
        trial_index: usize,
    },
    Eof,
    Error {
        message: String,
    },
}

#[derive(Debug)]
struct EvalDataPuller {
    child: tokio::process::Child,
    writer: tokio::net::unix::OwnedWriteHalf,
    reader: BufReader<tokio::net::unix::OwnedReadHalf>,
    _socket_cleanup_guard: SocketCleanupGuard,
}

#[derive(Debug, Clone)]
struct EvalSandboxPlan {
    evaluator_name: String,
    function_id: String,
    project_id: String,
}

#[derive(Debug, Clone, Deserialize)]
struct SandboxSummaryRow {
    #[serde(default)]
    scores: HashMap<String, Option<f64>>,
    #[serde(default)]
    metrics: HashMap<String, Value>,
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
    allowed_origins: Vec<String>,
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

#[derive(Debug)]
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum EvalSandbox {
    Local,
    Lambda,
}

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt eval my.eval.ts
  bt eval --no-send-logs --runner tsx my.eval.ts
  bt eval --language python my_eval.py
")]
pub struct EvalArgs {
    /// Eval files, directories, or glob patterns to execute (e.g. foo.eval.ts, tests/, "**/*.eval.ts").
    /// Defaults to the current directory.
    #[arg(value_name = "FILE")]
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

    /// Execute evals locally or in a remote sandbox.
    #[arg(long, env = "BT_EVAL_SANDBOX", value_enum, default_value = "local")]
    pub sandbox: EvalSandbox,

    /// Run evals locally (do not send logs to Braintrust).
    #[arg(
        long,
        alias = "no-send-logs",
        env = "BT_EVAL_LOCAL",
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    pub no_send_logs: bool,

    /// Output one JSON summary per evaluator.
    #[arg(
        long,
        env = "BT_EVAL_JSONL",
        value_parser = clap::builder::BoolishValueParser::new(),
        default_value_t = false
    )]
    pub jsonl: bool,

    /// Stop after the first failing evaluator.
    #[arg(
        long,
        env = "BT_EVAL_TERMINATE_ON_FAILURE",
        value_parser = clap::builder::BoolishValueParser::new(),
        default_value_t = false
    )]
    pub terminate_on_failure: bool,

    /// Number of worker threads for Python eval execution.
    #[arg(long, env = "BT_EVAL_NUM_WORKERS", value_name = "COUNT")]
    pub num_workers: Option<usize>,

    /// List evaluators without executing them.
    #[arg(
        long,
        env = "BT_EVAL_LIST",
        value_parser = clap::builder::BoolishValueParser::new(),
        default_value_t = false
    )]
    pub list: bool,

    /// Filter expression(s) used to select which evaluators to run.
    #[arg(
        long,
        env = "BT_EVAL_FILTER",
        value_name = "FILTER",
        value_delimiter = ','
    )]
    pub filter: Vec<String>,

    /// Show verbose evaluator errors and stderr output.
    #[arg(
        long,
        env = "BT_EVAL_VERBOSE",
        value_parser = clap::builder::BoolishValueParser::new(),
        default_value_t = false
    )]
    pub verbose: bool,

    /// Re-run evals when input files change.
    #[arg(
        long,
        short = 'w',
        env = "BT_EVAL_WATCH",
        value_parser = clap::builder::BoolishValueParser::new(),
        default_value_t = false
    )]
    pub watch: bool,

    /// Arguments forwarded to the eval file via process.argv (everything after `--`).
    /// Example: bt eval foo.eval.ts -- --description "Prod" --shard=1/4
    #[arg(last = true, value_name = "ARG")]
    pub extra_args: Vec<String>,

    /// Start the eval dev web server.
    #[arg(
        long,
        env = "BT_EVAL_DEV",
        value_parser = clap::builder::BoolishValueParser::new(),
        default_value_t = false
    )]
    pub dev: bool,

    /// Host interface for eval dev server.
    #[arg(long, env = "BT_EVAL_DEV_HOST", default_value = "localhost")]
    pub dev_host: String,

    /// Port for eval dev server.
    #[arg(long, env = "BT_EVAL_DEV_PORT", default_value_t = 8300)]
    pub dev_port: u16,

    /// Restrict eval dev server access to a specific org name.
    #[arg(long, env = "BT_EVAL_DEV_ORG_NAME")]
    pub dev_org_name: Option<String>,

    /// Additional allowed browser origin(s) for eval dev server CORS checks.
    /// Repeat this flag or set BT_EVAL_DEV_ALLOWED_ORIGIN as a comma-separated list.
    #[arg(
        long = "dev-allowed-origin",
        env = "BT_EVAL_DEV_ALLOWED_ORIGIN",
        value_name = "ORIGIN",
        value_delimiter = ','
    )]
    pub dev_allowed_origin: Vec<String>,
}

#[derive(Debug, Clone)]
struct EvalRunOptions {
    jsonl: bool,
    terminate_on_failure: bool,
    num_workers: Option<usize>,
    list: bool,
    filter: Vec<String>,
    verbose: bool,
    extra_args: Vec<String>,
}

pub async fn run(base: BaseArgs, args: EvalArgs) -> Result<()> {
    if args.dev && args.watch {
        anyhow::bail!("--watch is not supported with --dev.");
    }
    let inputs: Vec<String> = if args.files.is_empty() {
        vec![".".to_string()]
    } else {
        args.files.clone()
    };
    let mut files = expand_eval_file_globs(&inputs)?;
    if args.files.is_empty() {
        files.retain(|p| !is_excluded_by_default(p));
        if files.is_empty() {
            anyhow::bail!("no eval files found in current directory");
        }
    }
    validate_eval_input_files(&files)?;

    let options = EvalRunOptions {
        jsonl: args.jsonl,
        terminate_on_failure: args.terminate_on_failure,
        num_workers: args.num_workers,
        list: args.list,
        filter: args.filter,
        verbose: args.verbose,
        extra_args: args.extra_args,
    };

    if args.sandbox != EvalSandbox::Local {
        if args.dev {
            anyhow::bail!("--sandbox is not supported with --dev.");
        }
        if args.watch {
            anyhow::bail!("--sandbox is not supported with --watch.");
        }
        if args.list {
            anyhow::bail!("--sandbox is not supported with --list.");
        }
        if files.len() != 1 {
            anyhow::bail!("`bt eval --sandbox lambda` currently supports exactly one eval file.");
        }
        return run_eval_files_sandbox(
            &base,
            args.sandbox,
            args.language,
            args.runner.as_deref(),
            &files,
            args.no_send_logs,
            &options,
        )
        .await;
    }

    if args.dev {
        let language = detect_eval_language(&files, args.language)?;
        let app_url = resolve_app_url(&base);
        let state = DevServerState {
            base: base.clone(),
            language_override: Some(language),
            runner_override: args.runner.clone(),
            files,
            no_send_logs: args.no_send_logs,
            options,
            host: args.dev_host.clone(),
            port: args.dev_port,
            allowed_org_name: args.dev_org_name.clone(),
            allowed_origins: collect_allowed_dev_origins(&args.dev_allowed_origin, &app_url),
            app_url,
            http_client: Client::builder()
                .timeout(crate::http::DEFAULT_HTTP_TIMEOUT)
                .build()
                .context("failed to create dev server HTTP client")?,
        };
        return run_dev_server(state).await;
    }

    if args.watch {
        run_eval_files_watch(
            &base,
            args.language,
            args.runner.as_deref(),
            &files,
            args.no_send_logs,
            &options,
        )
        .await
    } else {
        let output = run_eval_files_once(
            &base,
            args.language,
            args.runner.as_deref(),
            &files,
            args.no_send_logs,
            &options,
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
    runner_override: Option<&str>,
    files: &[String],
    no_send_logs: bool,
    options: &EvalRunOptions,
) -> Result<()> {
    let input_watch_paths = resolve_watch_paths(files)?;
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
            runner_override,
            files,
            no_send_logs,
            options,
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

async fn run_eval_files_sandbox(
    base: &BaseArgs,
    sandbox: EvalSandbox,
    language_override: Option<EvalLanguage>,
    runner_override: Option<&str>,
    files: &[String],
    no_send_logs: bool,
    options: &EvalRunOptions,
) -> Result<()> {
    if sandbox != EvalSandbox::Lambda {
        anyhow::bail!("unsupported sandbox mode");
    }
    if no_send_logs {
        anyhow::bail!("--sandbox lambda is not supported with --no-send-logs.");
    }
    let language = detect_eval_language(files, language_override)?;
    let source_language = match language {
        EvalLanguage::JavaScript => SourceLanguage::JsLike,
        EvalLanguage::Python => SourceLanguage::Python,
    };

    let source_file = PathBuf::from(
        files
            .first()
            .ok_or_else(|| anyhow::anyhow!("missing sandbox source file"))?,
    );
    let published =
        publish_eval_sandbox_functions(base, &source_file, runner_override, source_language)
            .await?;
    let evaluator_names = list_sandbox_evaluator_names(
        base,
        language,
        runner_override,
        files,
        no_send_logs,
        options,
    )
    .await?;
    if evaluator_names.is_empty() {
        anyhow::bail!("No evaluators found. Did you call Eval() in the file?");
    }

    let mut plans = Vec::new();
    for evaluator_name in evaluator_names {
        let slug = sandbox_slug_from_source(&source_file, &evaluator_name);
        let published_entry = published
            .iter()
            .find(|entry| entry.slug == slug)
            .ok_or_else(|| {
                anyhow::anyhow!(
                    "sandbox function '{}' for evaluator '{}' was not published",
                    slug,
                    evaluator_name
                )
            })?;
        plans.push(EvalSandboxPlan {
            evaluator_name,
            function_id: published_entry.function_id.clone(),
            project_id: published_entry.project_id.clone(),
        });
    }

    let login_ctx = login(base).await?;
    let client = ApiClient::new(&login_ctx)?;
    let started_at = Utc::now().to_rfc3339_opts(SecondsFormat::Millis, true);

    for plan in plans {
        let mut puller = spawn_eval_data_puller(
            base,
            language,
            runner_override,
            files,
            no_send_logs,
            options,
            &EvalPullRequest {
                name: plan.evaluator_name.clone(),
                parameters: Some(json!({})),
            },
        )
        .await?;
        let ready = puller.read_message().await?;
        let (max_concurrency, experiment_name) = match ready {
            EvalPullResponse::Ready {
                evaluator_name,
                max_concurrency,
                experiment_name,
            } => {
                if evaluator_name != plan.evaluator_name {
                    anyhow::bail!(
                        "sandbox runner selected unexpected evaluator '{}', expected '{}'",
                        evaluator_name,
                        plan.evaluator_name
                    );
                }
                (max_concurrency.max(1), experiment_name)
            }
            EvalPullResponse::Error { message } => anyhow::bail!("{message}"),
            other => anyhow::bail!("unexpected initial sandbox pull response: {other:?}"),
        };

        let experiment = create_experiment(&client, &plan.project_id, &experiment_name, true)
            .await
            .with_context(|| {
                format!(
                    "failed to create sandbox parent experiment '{}' for evaluator '{}'",
                    experiment_name, plan.evaluator_name
                )
            })?;
        let mut in_flight: tokio::task::JoinSet<Result<Option<ExperimentStart>>> =
            tokio::task::JoinSet::new();
        let mut saw_eof = false;
        let mut experiment_url: Option<String> = None;

        while !saw_eof || !in_flight.is_empty() {
            while !saw_eof && in_flight.len() < max_concurrency {
                puller.send_message(&EvalPullClientMessage::Next).await?;
                match puller.read_message().await? {
                    EvalPullResponse::Row {
                        datum,
                        trial_index: _trial_index,
                    } => {
                        let function_id = plan.function_id.clone();
                        let evaluator_name = plan.evaluator_name.clone();
                        let project_id = plan.project_id.clone();
                        let body = json!({
                            "api_version": 1,
                            "function_id": { "function_id": function_id },
                            "name": evaluator_name,
                            "project_id": project_id,
                            "scores": [],
                            "stream": true,
                            "experiment_name": experiment.name,
                            "parent": {
                                "object_type": "experiment",
                                "object_id": experiment.id,
                            },
                            "data": { "data": [datum] },
                        });
                        let client_cloned = client.clone();
                        let org_name = login_ctx.login.org_name.clone();
                        let project_id = plan.project_id.clone();
                        in_flight.spawn(async move {
                            invoke_sandbox_eval(&client_cloned, &org_name, &project_id, body).await
                        });
                    }
                    EvalPullResponse::Eof => saw_eof = true,
                    EvalPullResponse::Error { message } => anyhow::bail!("{message}"),
                    other => anyhow::bail!("unexpected sandbox pull response: {other:?}"),
                }
            }

            if let Some(joined) = in_flight.join_next().await {
                if let Some(start) = joined?? {
                    if experiment_url.is_none() {
                        experiment_url = start.experiment_url.clone();
                    }
                }
            }
        }

        puller.send_message(&EvalPullClientMessage::Close).await?;
        puller.wait().await?;

        let summary = summarize_sandbox_experiment(
            &client,
            &plan.project_id,
            &plan.project_id,
            &experiment.name,
            &experiment.id,
            experiment_url,
            &started_at,
        )
        .await?;
        let rendered = format_experiment_summary(&summary);
        println!("{rendered}");
    }

    Ok(())
}

impl EvalDataPuller {
    async fn send_message(&mut self, message: &EvalPullClientMessage) -> Result<()> {
        let mut payload =
            serde_json::to_string(message).context("failed to serialize pull request")?;
        payload.push('\n');
        self.writer
            .write_all(payload.as_bytes())
            .await
            .context("failed to write pull request")?;
        self.writer
            .flush()
            .await
            .context("failed to flush pull request")?;
        Ok(())
    }

    async fn read_message(&mut self) -> Result<EvalPullResponse> {
        let mut line = String::new();
        let read = self
            .reader
            .read_line(&mut line)
            .await
            .context("failed to read sandbox pull response")?;
        if read == 0 {
            let status = self
                .child
                .wait()
                .await
                .context("sandbox pull runner exited unexpectedly")?;
            anyhow::bail!("sandbox pull runner exited with status {status}");
        }
        serde_json::from_str(line.trim()).context("failed to parse sandbox pull response JSON")
    }

    async fn wait(mut self) -> Result<()> {
        let status = self
            .child
            .wait()
            .await
            .context("sandbox pull runner failed")?;
        if !status.success() {
            anyhow::bail!("sandbox pull runner exited with status {status}");
        }
        Ok(())
    }
}

fn sandbox_slugify(input: &str) -> String {
    let mut out = String::with_capacity(input.len());
    let mut previous_dash = false;
    for ch in input.chars() {
        let lower = ch.to_ascii_lowercase();
        if lower.is_ascii_alphanumeric() {
            out.push(lower);
            previous_dash = false;
        } else if !previous_dash {
            out.push('-');
            previous_dash = true;
        }
    }
    out.trim_matches('-').to_string()
}

fn sandbox_slug_from_source(source_file: &Path, eval_name: &str) -> String {
    let stem = source_file
        .file_stem()
        .and_then(|value| value.to_str())
        .map(|value| value.strip_suffix(".eval").unwrap_or(value))
        .unwrap_or("eval");
    sandbox_slugify(&format!("{stem}-{eval_name}-sandbox"))
}

async fn list_sandbox_evaluator_names(
    base: &BaseArgs,
    language: EvalLanguage,
    runner_override: Option<&str>,
    files: &[String],
    no_send_logs: bool,
    options: &EvalRunOptions,
) -> Result<Vec<String>> {
    let output = run_eval_runner_command_to_completion(
        base,
        language,
        runner_override,
        files,
        no_send_logs,
        options,
        &[("BT_EVAL_DEV_MODE".to_string(), "list".to_string())],
        JsMode::Auto,
    )
    .await?;

    let parsed: Value =
        serde_json::from_slice(&output.stdout).context("failed to parse sandbox evaluator list")?;
    let object = parsed
        .as_object()
        .ok_or_else(|| anyhow::anyhow!("sandbox evaluator list was not a JSON object"))?;
    Ok(object.keys().cloned().collect())
}

async fn spawn_eval_data_puller(
    base: &BaseArgs,
    language: EvalLanguage,
    runner_override: Option<&str>,
    files: &[String],
    no_send_logs: bool,
    options: &EvalRunOptions,
    request: &EvalPullRequest,
) -> Result<EvalDataPuller> {
    let (listener, socket_path, socket_cleanup_guard) =
        bind_unix_listener("bt-eval-pull").context("failed to bind sandbox pull socket")?;
    let request_json =
        serde_json::to_string(request).context("failed to serialize sandbox pull request")?;
    let extra_env = vec![
        ("BT_EVAL_DEV_MODE".to_string(), "rows".to_string()),
        ("BT_EVAL_DEV_REQUEST_JSON".to_string(), request_json),
        (
            "BT_EVAL_PULL_SOCK".to_string(),
            socket_path.to_string_lossy().to_string(),
        ),
    ];
    let child = spawn_eval_support_process(
        base,
        language,
        runner_override,
        files,
        no_send_logs,
        options,
        &extra_env,
        JsMode::Auto,
    )
    .await?;

    let (stream, _) = tokio::time::timeout(Duration::from_secs(30), listener.accept())
        .await
        .context("timed out waiting for sandbox pull runner to connect")?
        .context("sandbox pull runner failed to connect")?;
    let (read_half, write_half) = stream.into_split();
    Ok(EvalDataPuller {
        child,
        writer: write_half,
        reader: BufReader::new(read_half),
        _socket_cleanup_guard: socket_cleanup_guard,
    })
}

async fn spawn_eval_support_process(
    base: &BaseArgs,
    language: EvalLanguage,
    runner_override: Option<&str>,
    files: &[String],
    no_send_logs: bool,
    options: &EvalRunOptions,
    extra_env: &[(String, String)],
    js_mode: JsMode,
) -> Result<tokio::process::Child> {
    let (js_runner, py_runner) = prepare_eval_runners()?;
    let force_esm = matches!(js_mode, JsMode::ForceEsm);
    let (mut cmd, runner_kind) = match language {
        EvalLanguage::Python => (
            build_python_command(runner_override, &py_runner, files)?,
            RunnerKind::Other,
        ),
        EvalLanguage::JavaScript => {
            if force_esm {
                (
                    build_vite_node_fallback_command(&js_runner, files)?,
                    RunnerKind::ViteNode,
                )
            } else {
                let plan = build_js_plan(runner_override, &js_runner, files)?;
                (plan.cmd, plan.kind)
            }
        }
    };
    if language == EvalLanguage::JavaScript && should_set_node_heap_size(runner_kind) {
        set_node_heap_size_env(&mut cmd);
    }
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
    if language == EvalLanguage::JavaScript && force_esm {
        cmd.env("BT_EVAL_FORCE_ESM", "1");
    }
    if !options.extra_args.is_empty() {
        let serialized =
            serde_json::to_string(&options.extra_args).context("failed to serialize extra args")?;
        cmd.env("BT_EVAL_EXTRA_ARGS_JSON", serialized);
    }
    cmd.stdout(Stdio::inherit());
    cmd.stderr(Stdio::inherit());
    cmd.spawn().context("failed to start eval support runner")
}

async fn run_eval_runner_command_to_completion(
    base: &BaseArgs,
    language: EvalLanguage,
    runner_override: Option<&str>,
    files: &[String],
    no_send_logs: bool,
    options: &EvalRunOptions,
    extra_env: &[(String, String)],
    js_mode: JsMode,
) -> Result<std::process::Output> {
    let (js_runner, py_runner) = prepare_eval_runners()?;
    let force_esm = matches!(js_mode, JsMode::ForceEsm);
    let (mut cmd, runner_kind) = match language {
        EvalLanguage::Python => (
            build_python_command(runner_override, &py_runner, files)?,
            RunnerKind::Other,
        ),
        EvalLanguage::JavaScript => {
            if force_esm {
                (
                    build_vite_node_fallback_command(&js_runner, files)?,
                    RunnerKind::ViteNode,
                )
            } else {
                let plan = build_js_plan(runner_override, &js_runner, files)?;
                (plan.cmd, plan.kind)
            }
        }
    };
    if language == EvalLanguage::JavaScript && should_set_node_heap_size(runner_kind) {
        set_node_heap_size_env(&mut cmd);
    }
    cmd.envs(build_env(base).await?);
    for (key, value) in extra_env {
        cmd.env(key, value);
    }
    if no_send_logs {
        cmd.env("BT_EVAL_NO_SEND_LOGS", "1");
        cmd.env("BT_EVAL_LOCAL", "1");
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
    if !options.extra_args.is_empty() {
        let serialized =
            serde_json::to_string(&options.extra_args).context("failed to serialize extra args")?;
        cmd.env("BT_EVAL_EXTRA_ARGS_JSON", serialized);
    }
    let output = cmd
        .output()
        .await
        .context("failed to run eval support runner")?;
    if !output.status.success() {
        anyhow::bail!(
            "eval support runner exited with status {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(output)
}

async fn invoke_sandbox_eval(
    client: &ApiClient,
    org_name: &str,
    project_id: &str,
    body: Value,
) -> Result<Option<ExperimentStart>> {
    let response = client
        .post_with_headers_raw(
            "/function/sandbox",
            &body,
            &[("x-bt-org-name", org_name), ("x-bt-project-id", project_id)],
        )
        .await?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("sandbox invoke failed ({status}): {body}");
    }

    let mut bytes = response.bytes_stream();
    let mut buffer = String::new();
    let mut current_event: Option<String> = None;
    let mut data_lines: Vec<String> = Vec::new();
    let mut start: Option<ExperimentStart> = None;
    let mut saw_done = false;

    while let Some(chunk) = bytes.next().await {
        let chunk = chunk.context("failed to read sandbox SSE response")?;
        buffer.push_str(&String::from_utf8_lossy(&chunk));
        while let Some(pos) = buffer.find('\n') {
            let mut line: String = buffer.drain(..=pos).collect();
            if line.ends_with('\n') {
                line.pop();
            }
            if line.ends_with('\r') {
                line.pop();
            }
            if line.is_empty() {
                if current_event.is_some() || !data_lines.is_empty() {
                    let event_name = current_event.take().unwrap_or_default();
                    let data = data_lines.join("\n");
                    data_lines.clear();
                    match event_name.as_str() {
                        "start" => {
                            if let Ok(parsed) = serde_json::from_str::<ExperimentStart>(&data) {
                                if start.is_none() {
                                    start = Some(parsed);
                                }
                            }
                        }
                        "error" => {
                            if let Ok(payload) = serde_json::from_str::<Value>(&data) {
                                let message = payload
                                    .get("message")
                                    .or_else(|| payload.get("error"))
                                    .and_then(Value::as_str)
                                    .unwrap_or("sandbox eval failed");
                                anyhow::bail!("{message}");
                            }
                            anyhow::bail!("{data}");
                        }
                        "done" => {
                            saw_done = true;
                        }
                        _ => {}
                    }
                }
                continue;
            }
            if let Some(value) = line.strip_prefix("event:") {
                current_event = Some(value.trim().to_string());
            } else if let Some(value) = line.strip_prefix("data:") {
                data_lines.push(value.trim_start().to_string());
            }
        }
    }

    if !saw_done {
        anyhow::bail!("sandbox SSE stream ended before a done event");
    }
    Ok(start)
}

async fn summarize_sandbox_experiment(
    client: &ApiClient,
    project_name: &str,
    _project_id: &str,
    experiment_name: &str,
    experiment_id: &str,
    experiment_url: Option<String>,
    started_at: &str,
) -> Result<ExperimentSummary> {
    let query = build_sandbox_summary_query(experiment_id, started_at);
    let response = client.btql::<SandboxSummaryRow>(&query).await?;
    Ok(aggregate_sandbox_summary(
        project_name,
        experiment_name,
        experiment_id,
        experiment_url,
        &response.data,
    ))
}

fn build_sandbox_summary_query(experiment_id: &str, started_at: &str) -> String {
    format!(
        "select: scores, metrics | from: experiment('{}') summary | filter: created >= '{}' | limit: 1000",
        experiment_id.replace('\'', "''"),
        started_at.replace('\'', "''")
    )
}

fn aggregate_sandbox_summary(
    project_name: &str,
    experiment_name: &str,
    experiment_id: &str,
    experiment_url: Option<String>,
    rows: &[SandboxSummaryRow],
) -> ExperimentSummary {
    let mut scores: HashMap<String, (f64, usize)> = HashMap::new();
    let mut metrics: HashMap<String, (f64, usize)> = HashMap::new();
    for row in rows {
        for (name, value) in &row.scores {
            if let Some(value) = value {
                let entry = scores.entry(name.clone()).or_insert((0.0, 0));
                entry.0 += value;
                entry.1 += 1;
            }
        }
        for (name, value) in &row.metrics {
            let Some(number) = value.as_f64() else {
                continue;
            };
            let entry = metrics.entry(name.clone()).or_insert((0.0, 0));
            entry.0 += number;
            entry.1 += 1;
        }
    }

    ExperimentSummary {
        project_name: project_name.to_string(),
        experiment_name: experiment_name.to_string(),
        project_id: None,
        experiment_id: Some(experiment_id.to_string()),
        project_url: None,
        experiment_url,
        comparison_experiment_name: None,
        scores: scores
            .into_iter()
            .map(|(name, (total, count))| {
                let average = if count == 0 {
                    0.0
                } else {
                    total / count as f64
                };
                (
                    name.clone(),
                    ScoreSummary {
                        name,
                        score: average,
                        diff: None,
                        improvements: 0,
                        regressions: 0,
                    },
                )
            })
            .collect(),
        metrics: if metrics.is_empty() {
            None
        } else {
            Some(
                metrics
                    .into_iter()
                    .map(|(name, (total, count))| {
                        let average = if count == 0 {
                            0.0
                        } else {
                            total / count as f64
                        };
                        (
                            name.clone(),
                            MetricSummary {
                                name,
                                metric: average,
                                unit: String::new(),
                                diff: None,
                                improvements: 0,
                                regressions: 0,
                            },
                        )
                    })
                    .collect(),
            )
        },
    }
}

struct EvalPlan<'a> {
    language: EvalLanguage,
    files: &'a [String],
    runner_override: Option<&'a str>,
    show_js_hint: bool,
    retry_policy: RetryPolicy,
}

struct EvalAttemptOutput {
    status: ExitStatus,
    dependency_files: Vec<String>,
    error_messages: Vec<String>,
    stderr_lines: Vec<String>,
    runner_kind: RunnerKind,
}

fn build_eval_plan<'a>(
    files: &'a [String],
    language_override: Option<EvalLanguage>,
    runner_override: Option<&'a str>,
) -> Result<EvalPlan<'a>> {
    let language = detect_eval_language(files, language_override)?;
    let show_js_hint = language == EvalLanguage::JavaScript && runner_override.is_none();
    let has_ts_files = language == EvalLanguage::JavaScript && has_ts_eval_files(files);
    let retry_policy = if show_js_hint && has_ts_files {
        RetryPolicy::Allow
    } else {
        RetryPolicy::Disallow
    };

    Ok(EvalPlan {
        language,
        files,
        runner_override,
        show_js_hint,
        retry_policy,
    })
}

async fn run_eval_files_once(
    base: &BaseArgs,
    language_override: Option<EvalLanguage>,
    runner_override: Option<&str>,
    files: &[String],
    no_send_logs: bool,
    options: &EvalRunOptions,
) -> Result<EvalRunOutput> {
    let plan = build_eval_plan(files, language_override, runner_override)?;
    let console_policy = match plan.retry_policy {
        RetryPolicy::Allow => ConsolePolicy::BufferStderr,
        RetryPolicy::Disallow => ConsolePolicy::Forward,
    };

    let mut output = run_eval_attempt(
        base,
        &plan,
        no_send_logs,
        options,
        &[],
        JsMode::Auto,
        console_policy,
    )
    .await?;

    if !output.status.success() && should_retry_esm(&plan, &output) {
        let first_attempt_stderr = std::mem::take(&mut output.stderr_lines);
        eprintln!("Eval failed with ESM/CJS interop error. Retrying in ESM mode...");
        output = run_eval_attempt(
            base,
            &plan,
            no_send_logs,
            options,
            &[],
            JsMode::ForceEsm,
            ConsolePolicy::Forward,
        )
        .await?;

        if !output.status.success() {
            eprintln!("\nFirst attempt (CJS mode) error:");
            report_buffered_stderr(&first_attempt_stderr, options.verbose);
        }
    } else if matches!(plan.retry_policy, RetryPolicy::Allow) {
        report_buffered_stderr(&output.stderr_lines, options.verbose);
    }

    if !output.status.success() && plan.show_js_hint && should_retry_esm(&plan, &output) {
        eprintln!("Hint: If this eval uses ESM features (like top-level await), try `--runner vite-node`.");
    }

    let mut dependencies =
        normalize_watch_paths(output.dependency_files.into_iter().map(PathBuf::from))?;
    if plan.language == EvalLanguage::JavaScript {
        let static_dependencies = collect_js_static_dependencies(files)?;
        dependencies = merge_watch_paths(&dependencies, &static_dependencies);
    }

    Ok(EvalRunOutput {
        status: output.status,
        dependencies,
    })
}

async fn run_eval_attempt(
    base: &BaseArgs,
    plan: &EvalPlan<'_>,
    no_send_logs: bool,
    options: &EvalRunOptions,
    extra_env: &[(String, String)],
    js_mode: JsMode,
    console_policy: ConsolePolicy,
) -> Result<EvalAttemptOutput> {
    let spawned = spawn_eval_runner(
        base,
        plan.language,
        plan.runner_override,
        plan.files,
        no_send_logs,
        options,
        extra_env,
        js_mode,
    )
    .await?;
    let mut ui = EvalUi::new(options.jsonl, options.list, options.verbose);
    let output =
        drive_eval_runner(spawned.process, console_policy, |event| ui.handle(event)).await?;
    ui.finish();

    Ok(EvalAttemptOutput {
        status: output.status,
        dependency_files: output.dependency_files,
        error_messages: output.error_messages,
        stderr_lines: output.stderr_lines,
        runner_kind: spawned.runner_kind,
    })
}

struct EvalSpawned {
    process: EvalRunnerProcess,
    runner_kind: RunnerKind,
}

#[allow(clippy::too_many_arguments)]
async fn spawn_eval_runner(
    base: &BaseArgs,
    language: EvalLanguage,
    runner_override: Option<&str>,
    files: &[String],
    no_send_logs: bool,
    options: &EvalRunOptions,
    extra_env: &[(String, String)],
    js_mode: JsMode,
) -> Result<EvalSpawned> {
    if language != EvalLanguage::Python && options.num_workers.is_some() {
        anyhow::bail!("--num-workers is only supported for Python evals.");
    }
    let (js_runner, py_runner) = prepare_eval_runners()?;
    let force_esm = matches!(js_mode, JsMode::ForceEsm);

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

    let (mut cmd, runner_kind) = match language {
        EvalLanguage::Python => (
            build_python_command(runner_override, &py_runner, files)?,
            RunnerKind::Other,
        ),
        EvalLanguage::JavaScript => {
            if force_esm {
                (
                    build_vite_node_fallback_command(&js_runner, files)?,
                    RunnerKind::ViteNode,
                )
            } else {
                let plan = build_js_plan(runner_override, &js_runner, files)?;
                (plan.cmd, plan.kind)
            }
        }
    };
    if language == EvalLanguage::JavaScript && should_set_node_heap_size(runner_kind) {
        set_node_heap_size_env(&mut cmd);
    }

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
    if language == EvalLanguage::JavaScript && force_esm {
        cmd.env("BT_EVAL_FORCE_ESM", "1");
    }
    if language == EvalLanguage::JavaScript {
        let runner_name = match runner_kind {
            RunnerKind::Tsx => "tsx",
            RunnerKind::ViteNode => "vite-node",
            RunnerKind::Deno => "deno",
            RunnerKind::Bun => "bun",
            RunnerKind::Other => "other",
        };
        cmd.env("BT_EVAL_RUNNER_KIND", runner_name);
    }
    if !options.extra_args.is_empty() {
        let serialized =
            serde_json::to_string(&options.extra_args).context("failed to serialize extra args")?;
        cmd.env("BT_EVAL_EXTRA_ARGS_JSON", serialized);
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

    Ok(EvalSpawned {
        process: EvalRunnerProcess {
            child,
            rx,
            sse_task,
            sse_connected,
            _socket_cleanup_guard: socket_cleanup_guard,
        },
        runner_kind,
    })
}

async fn drive_eval_runner<F>(
    mut process: EvalRunnerProcess,
    console_policy: ConsolePolicy,
    mut on_event: F,
) -> Result<EvalProcessOutput>
where
    F: FnMut(EvalEvent),
{
    let mut status = None;
    let mut dependency_files: Vec<String> = Vec::new();
    let mut error_messages: Vec<String> = Vec::new();
    let mut stderr_lines: Vec<String> = Vec::new();

    loop {
        tokio::select! {
            event = process.rx.recv() => {
                match event {
                    Some(EvalEvent::Dependencies { files }) => {
                        dependency_files.extend(files.clone());
                        on_event(EvalEvent::Dependencies { files });
                    }
                    Some(EvalEvent::Error { message, stack, status }) => {
                        error_messages.push(message.clone());
                        if let Some(stack) = stack.as_ref() {
                            error_messages.push(stack.clone());
                        }
                        on_event(EvalEvent::Error { message, stack, status });
                    }
                    Some(EvalEvent::Console { stream, message }) => {
                        if stream == "stderr" && matches!(console_policy, ConsolePolicy::BufferStderr)
                        {
                            stderr_lines.push(message);
                        } else {
                            on_event(EvalEvent::Console { stream, message });
                        }
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
        error_messages,
        stderr_lines,
    })
}

fn flush_stderr(lines: &[String]) {
    for line in lines {
        eprintln!("{line}");
    }
}

fn report_buffered_stderr(lines: &[String], verbose: bool) {
    if lines.is_empty() {
        return;
    }
    if verbose {
        flush_stderr(lines);
    } else {
        eprintln!(
            "Suppressed {} stderr line(s). Re-run with `bt eval --verbose ...` to inspect details.",
            lines.len()
        );
    }
}

fn should_retry_esm(plan: &EvalPlan<'_>, output: &EvalAttemptOutput) -> bool {
    if matches!(plan.retry_policy, RetryPolicy::Disallow) {
        return false;
    }
    if output.runner_kind != RunnerKind::Tsx {
        return false;
    }
    output
        .stderr_lines
        .iter()
        .chain(&output.error_messages)
        .any(|line| is_esm_interop_error(line))
}

fn has_ts_eval_files(files: &[String]) -> bool {
    files.iter().any(|file| {
        let ext = Path::new(file)
            .extension()
            .and_then(|ext| ext.to_str())
            .unwrap_or("");
        matches!(ext.to_ascii_lowercase().as_str(), "ts" | "tsx")
    })
}

fn is_esm_interop_error(message: &str) -> bool {
    const PATTERNS: &[&str] = &[
        "ERR_REQUIRE_ESM",
        "ERR_PACKAGE_PATH_NOT_EXPORTED",
        "No \"exports\" main defined",
        "Cannot use import statement outside a module",
        "ERR_UNKNOWN_FILE_EXTENSION",
    ];

    PATTERNS.iter().any(|pattern| message.contains(pattern))
}

fn resolve_app_url(base: &BaseArgs) -> String {
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

fn collect_allowed_dev_origins(explicit: &[String], app_url: &str) -> Vec<String> {
    let mut deduped = BTreeSet::new();
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
            // Filter out internal eval_progress events (start/increment/stop)
            // which are used for CLI progress bars but crash the UI stream
            // parser.  Only forward external progress events (e.g. json_delta).
            if serde_json::from_str::<EvalProgressData>(&progress.data)
                .map(|p| p.kind_type == "eval_progress")
                .unwrap_or(false)
            {
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

async fn run_dev_server(state: DevServerState) -> Result<()> {
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
    let project = base
        .project
        .clone()
        .or_else(|| crate::config::load().ok().and_then(|c| c.project));
    if let Some(project) = &project {
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

const DEFAULT_EVAL_GLOBS: &[&str] = &[
    "**/*.eval.ts",
    "**/*.eval.js",
    "**/*.eval.mjs",
    "**/*.eval.cjs",
    "**/*.eval.py",
    "**/eval_*.py",
];

/// Directory name segments excluded during default discovery.
/// Explicit user-provided files/globs bypass this list.
const DEFAULT_EXCLUDE_DIRS: &[&str] = &[
    "node_modules",
    "site-packages",
    "dist-packages",
    "__pycache__",
    ".venv",
    "venv",
];

fn is_excluded_by_default(path: &str) -> bool {
    Path::new(path).components().any(|c| {
        if let std::path::Component::Normal(s) = c {
            DEFAULT_EXCLUDE_DIRS.contains(&s.to_string_lossy().as_ref())
        } else {
            false
        }
    })
}

fn expand_eval_file_globs(inputs: &[String]) -> Result<Vec<String>> {
    let mut expanded: Vec<String> = Vec::new();
    let mut seen: std::collections::HashSet<String> = std::collections::HashSet::new();

    let mut push = |path: String| {
        if seen.insert(path.clone()) {
            expanded.push(path);
        }
    };

    for input in inputs {
        let path = Path::new(input);
        if path.is_dir() {
            let mut dir_files: Vec<String> = Vec::new();
            for glob_suffix in DEFAULT_EVAL_GLOBS {
                let pattern = format!("{input}/{glob_suffix}");
                let matches: Vec<PathBuf> = glob::glob(&pattern)
                    .with_context(|| format!("invalid glob pattern: {pattern}"))?
                    .collect::<Result<_, _>>()
                    .with_context(|| format!("error expanding glob: {pattern}"))?;
                dir_files.extend(
                    matches
                        .into_iter()
                        .map(|p| p.to_string_lossy().into_owned()),
                );
            }
            if dir_files.is_empty() {
                anyhow::bail!("no eval files found in directory: {input}");
            }
            dir_files.into_iter().for_each(&mut push);
            continue;
        }
        // Treat paths that end with a separator as intended directories even if
        // they don't exist, so the error is "directory not found" rather than
        // the more confusing "file not found".
        if input.ends_with(std::path::MAIN_SEPARATOR) || input.ends_with('/') {
            anyhow::bail!("directory not found: {input}");
        }
        if !input.contains(['*', '?', '[']) {
            push(input.clone());
            continue;
        }
        let matches: Vec<PathBuf> = glob::glob(input)
            .with_context(|| format!("invalid glob pattern: {input}"))?
            .collect::<Result<_, _>>()
            .with_context(|| format!("error expanding glob: {input}"))?;
        if matches.is_empty() {
            anyhow::bail!("glob pattern matched no files: {input}");
        }
        matches
            .into_iter()
            .map(|p| p.to_string_lossy().into_owned())
            .for_each(&mut push);
    }
    Ok(expanded)
}

fn validate_eval_input_files(files: &[String]) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to read current directory")?;
    validate_eval_input_files_in_cwd(files, &cwd)
}

fn validate_eval_input_files_in_cwd(files: &[String], cwd: &Path) -> Result<()> {
    for input in files {
        let input_path = Path::new(input);
        let resolved = if input_path.is_absolute() {
            input_path.to_path_buf()
        } else {
            cwd.join(input_path)
        };

        match std::fs::metadata(&resolved) {
            Ok(metadata) => {
                if !metadata.is_file() {
                    anyhow::bail!(
                        "Eval file is not a regular file: {} (from input `{input}`).",
                        resolved.display()
                    );
                }
            }
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => {
                if let Some(suggested) = maybe_missing_eval_file_suggestion(input, cwd) {
                    anyhow::bail!(
                        "Eval file not found: {} (from input `{input}` in `{}`). Did you mean `bt eval {suggested}`?",
                        resolved.display(),
                        cwd.display()
                    );
                }
                anyhow::bail!(
                    "Eval file not found: {} (from input `{input}` in `{}`).",
                    resolved.display(),
                    cwd.display()
                );
            }
            Err(err) => {
                return Err(err)
                    .with_context(|| format!("failed to read eval file {}", resolved.display()));
            }
        }
    }

    Ok(())
}

fn maybe_missing_eval_file_suggestion(input: &str, cwd: &Path) -> Option<String> {
    if Path::new(input).is_absolute() {
        return None;
    }
    let cwd_name = cwd.file_name()?.to_str()?;
    let prefixed = format!("{cwd_name}/");
    let suggested = input.strip_prefix(&prefixed)?;
    if suggested.is_empty() {
        return None;
    }
    let candidate = cwd.join(suggested);
    if candidate.is_file() {
        Some(suggested.to_string())
    } else {
        None
    }
}

struct JsRunnerPlan {
    cmd: Command,
    kind: RunnerKind,
}

fn build_js_plan(
    runner_override: Option<&str>,
    runner: &Path,
    files: &[String],
) -> Result<JsRunnerPlan> {
    if let Some(explicit) = runner_override {
        let resolved_runner = resolve_js_runner_command(explicit, files);
        if is_deno_runner(explicit) || is_deno_runner_path(resolved_runner.as_ref()) {
            let runner_script = prepare_js_runner_in_cwd()?;
            return Ok(JsRunnerPlan {
                cmd: build_deno_js_command(resolved_runner.as_os_str(), &runner_script, files),
                kind: RunnerKind::Deno,
            });
        }
        let kind = runner_kind_for_bin(resolved_runner.as_ref());
        let runner_script = select_js_runner_entrypoint(runner, resolved_runner.as_ref())?;
        let mut command = Command::new(resolved_runner);
        command.arg(runner_script).args(files);
        return Ok(JsRunnerPlan { cmd: command, kind });
    }

    if let Some(auto_runner) = find_js_runner_binary(files) {
        if is_deno_runner_path(&auto_runner) {
            let runner_script = prepare_js_runner_in_cwd()?;
            return Ok(JsRunnerPlan {
                cmd: build_deno_js_command(auto_runner.as_os_str(), &runner_script, files),
                kind: RunnerKind::Deno,
            });
        }
        let kind = runner_kind_for_bin(auto_runner.as_ref());
        let runner_script = select_js_runner_entrypoint(runner, auto_runner.as_ref())?;
        let mut command = Command::new(auto_runner);
        command.arg(runner_script).args(files);
        return Ok(JsRunnerPlan { cmd: command, kind });
    }

    let mut command = Command::new("npx");
    command.arg("--yes").arg("tsx").arg(runner).args(files);
    Ok(JsRunnerPlan {
        cmd: command,
        kind: RunnerKind::Tsx,
    })
}

fn build_vite_node_fallback_command(runner: &Path, files: &[String]) -> Result<Command> {
    if let Some(path) = find_node_module_bin_for_files("vite-node", files)
        .or_else(|| find_binary_in_path(&["vite-node"]))
    {
        let mut command = Command::new(path);
        command.arg(runner).args(files);
        return Ok(command);
    }

    let mut command = Command::new("npx");
    command
        .arg("--yes")
        .arg("vite-node")
        .arg(runner)
        .args(files);
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
    runner_override: Option<&str>,
    runner: &Path,
    files: &[String],
) -> Result<Command> {
    let runner_override = runner_override
        .map(ToOwned::to_owned)
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

fn runner_bin_name(runner_command: &Path) -> Option<String> {
    let name = runner_command.file_name()?.to_str()?.to_ascii_lowercase();
    Some(name.strip_suffix(".cmd").unwrap_or(&name).to_string())
}

fn runner_kind_for_bin(runner_command: &Path) -> RunnerKind {
    match runner_bin_name(runner_command).as_deref() {
        Some("tsx") => RunnerKind::Tsx,
        Some("vite-node") => RunnerKind::ViteNode,
        Some("bun") | Some("bunx") => RunnerKind::Bun,
        _ => RunnerKind::Other,
    }
}

fn should_set_node_heap_size(runner_kind: RunnerKind) -> bool {
    !matches!(runner_kind, RunnerKind::Deno | RunnerKind::Bun)
}

fn set_node_heap_size_env(command: &mut Command) {
    let heap_option = format!("--max-old-space-size={EVAL_NODE_MAX_OLD_SPACE_SIZE_MB}");
    let existing = std::env::var("NODE_OPTIONS").unwrap_or_default();
    let has_existing_max_old_space = existing
        .split_whitespace()
        .any(|arg| arg.starts_with("--max-old-space-size"));
    let merged = if existing.trim().is_empty() {
        heap_option
    } else if has_existing_max_old_space {
        existing
    } else {
        format!("{existing} {heap_option}")
    };
    command.env("NODE_OPTIONS", merged);
}

fn is_ts_node_runner(runner_command: &Path) -> bool {
    runner_bin_name(runner_command).is_some_and(|n| n == "ts-node" || n == "ts-node-esm")
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

fn build_socket_path(prefix: &str) -> Result<PathBuf> {
    let pid = std::process::id();
    let serial = SSE_SOCKET_COUNTER.fetch_add(1, Ordering::Relaxed);
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("failed to read system time")?
        .as_nanos();
    Ok(std::env::temp_dir().join(format!("{prefix}-{pid}-{now}-{serial}.sock")))
}

fn bind_unix_listener(prefix: &str) -> Result<(UnixListener, PathBuf, SocketCleanupGuard)> {
    let mut last_bind_err: Option<std::io::Error> = None;
    for _ in 0..SSE_SOCKET_BIND_MAX_ATTEMPTS {
        let socket_path = build_socket_path(prefix)?;
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
        "failed to bind unix socket after {SSE_SOCKET_BIND_MAX_ATTEMPTS} attempts"
    ))
}

fn bind_sse_listener() -> Result<(UnixListener, PathBuf, SocketCleanupGuard)> {
    bind_unix_listener("bt-eval")
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
    Processing(ProcessingEventData),
    Start(ExperimentStart),
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

#[derive(Debug, Deserialize, Serialize)]
struct ProcessingEventData {
    #[serde(default)]
    evaluators: usize,
}

#[derive(Debug, Deserialize, Serialize, Default)]
#[serde(rename_all = "camelCase")]
struct ExperimentStart {
    #[serde(default, alias = "project_name")]
    project_name: Option<String>,
    #[serde(default, alias = "experiment_name")]
    experiment_name: Option<String>,
    #[serde(default, alias = "project_id")]
    project_id: Option<String>,
    #[serde(default, alias = "experiment_id")]
    experiment_id: Option<String>,
    #[serde(default, alias = "project_url")]
    project_url: Option<String>,
    #[serde(default, alias = "experiment_url")]
    experiment_url: Option<String>,
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
    #[serde(default)]
    improvements: i64,
    #[serde(default)]
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
    #[serde(default)]
    unit: String,
    diff: Option<f64>,
    #[serde(default)]
    improvements: i64,
    #[serde(default)]
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
        "processing" => {
            if let Ok(payload) = serde_json::from_str::<ProcessingEventData>(&data) {
                let _ = tx.send(EvalEvent::Processing(payload));
            }
        }
        "start" => {
            if let Ok(start) = serde_json::from_str::<ExperimentStart>(&data) {
                let _ = tx.send(EvalEvent::Start(start));
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
    verbose: bool,
    deferred_errors: Vec<String>,
    suppressed_stderr_lines: usize,
    finished: bool,
}

impl EvalUi {
    fn new(jsonl: bool, list: bool, verbose: bool) -> Self {
        let draw_target = if std::io::stderr().is_terminal() && animations_enabled() && !is_quiet()
        {
            ProgressDrawTarget::stderr_with_hz(10)
        } else {
            ProgressDrawTarget::stderr()
        };
        let progress = MultiProgress::with_draw_target(draw_target);
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
            verbose,
            deferred_errors: Vec::new(),
            suppressed_stderr_lines: 0,
            finished: false,
        }
    }

    fn finish(&mut self) {
        if self.finished {
            return;
        }
        for (_, bar) in self.bars.drain() {
            bar.finish_and_clear();
        }
        let _ = self.progress.clear();
        self.progress.set_draw_target(ProgressDrawTarget::hidden());
        self.print_deferred_error_footnote();
        self.finished = true;
    }

    fn handle(&mut self, event: EvalEvent) {
        match event {
            EvalEvent::Processing(payload) => {
                self.print_persistent_line(format_processing_line(payload.evaluators));
            }
            EvalEvent::Start(start) => {
                if let Some(line) = format_start_line(&start) {
                    self.print_persistent_line(line);
                }
            }
            EvalEvent::Summary(summary) => {
                if self.jsonl {
                    if let Ok(line) = serde_json::to_string(&summary) {
                        println!("{line}");
                    }
                } else {
                    let rendered = format_experiment_summary(&summary);
                    self.print_persistent_multiline(rendered);
                }
            }
            EvalEvent::Progress(progress) => {
                self.handle_progress(progress);
            }
            EvalEvent::Dependencies { .. } => {}
            EvalEvent::Console { stream, message } => {
                if stream == "stdout" && (self.list || self.jsonl) {
                    println!("{message}");
                } else if stream == "stderr" && !self.verbose {
                    self.suppressed_stderr_lines += 1;
                } else {
                    let _ = self.progress.println(message);
                }
            }
            EvalEvent::Error { message, stack, .. } => {
                let show_hint = message.contains("Please specify an api key");
                if self.verbose {
                    let line = message.as_str().red().to_string();
                    let _ = self.progress.println(line);
                    if let Some(stack) = stack {
                        for line in stack.lines() {
                            let _ = self.progress.println(line.dark_grey().to_string());
                        }
                    }
                } else {
                    self.record_deferred_error(message);
                }
                if show_hint {
                    let hint = "Hint: pass --api-key, set BRAINTRUST_API_KEY, run `bt auth login`/`bt auth login --oauth`, or use --no-send-logs for local evals.";
                    if self.verbose {
                        let _ = self.progress.println(hint.dark_grey().to_string());
                    } else {
                        self.record_deferred_error(hint.to_string());
                    }
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

    fn print_persistent_line(&self, line: String) {
        self.progress.suspend(|| {
            eprintln!("{line}");
        });
    }

    fn print_persistent_multiline(&self, text: String) {
        self.progress.suspend(|| {
            for line in text.lines() {
                eprintln!("{line}");
            }
        });
    }

    fn record_deferred_error(&mut self, message: String) {
        let trimmed = message.trim();
        if trimmed.is_empty() {
            return;
        }
        if self
            .deferred_errors
            .iter()
            .any(|existing| existing == trimmed)
        {
            return;
        }
        if self.deferred_errors.len() < MAX_DEFERRED_EVAL_ERRORS {
            self.deferred_errors.push(trimmed.to_string());
        }
    }

    fn print_deferred_error_footnote(&self) {
        if self.verbose {
            return;
        }
        if self.deferred_errors.is_empty() && self.suppressed_stderr_lines == 0 {
            return;
        }

        eprintln!();
        if !self.deferred_errors.is_empty() {
            let noun = if self.deferred_errors.len() == 1 {
                "error"
            } else {
                "errors"
            };
            eprintln!(
                "Encountered {} evaluator {noun}:",
                self.deferred_errors.len()
            );
            for message in &self.deferred_errors {
                eprintln!("  - {message}");
            }
        }
        if self.suppressed_stderr_lines > 0 {
            eprintln!(
                "Suppressed {} stderr line(s). Re-run with `bt eval --verbose ...` to inspect details.",
                self.suppressed_stderr_lines
            );
        }
    }
}

impl Drop for EvalUi {
    fn drop(&mut self) {
        self.finish();
    }
}

fn fit_name_to_spaces(name: &str, length: usize) -> String {
    let char_count = name.chars().count();
    if char_count < length {
        let mut padded = name.to_string();
        padded.push_str(&" ".repeat(length - char_count));
        return padded;
    }
    if char_count == length {
        return name.to_string();
    }
    if length <= 3 {
        return name.chars().take(length).collect();
    }
    if length <= 5 {
        let truncated: String = name.chars().take(length - 3).collect();
        return format!("{truncated}...");
    }

    // Keep both prefix and suffix so similarly named evaluators remain distinguishable.
    let keep_total = length - 3;
    let head_len = keep_total / 2;
    let tail_len = keep_total - head_len;
    let head: String = name.chars().take(head_len).collect();
    let tail: String = name
        .chars()
        .rev()
        .take(tail_len)
        .collect::<String>()
        .chars()
        .rev()
        .collect();
    format!("{head}...{tail}")
}

fn format_processing_line(evaluators: usize) -> String {
    let noun = if evaluators == 1 {
        "evaluator"
    } else {
        "evaluators"
    };
    format!("Processing {evaluators} {noun}...")
}

fn format_start_line(start: &ExperimentStart) -> Option<String> {
    let experiment_name = start
        .experiment_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let experiment_url = start
        .experiment_url
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty());
    let arrow = "▶".cyan();

    match (experiment_name, experiment_url) {
        (Some(name), Some(url)) => Some(format!(
            "{arrow} Experiment {} is running at {url}",
            name.bold()
        )),
        (Some(name), None) => Some(format!(
            "{arrow} Experiment {} is running at locally",
            name.bold()
        )),
        (None, Some(url)) => Some(format!("{arrow} Experiment is running at {url}")),
        (None, None) => None,
    }
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
    use clap::Parser;
    use std::fs;
    use std::path::PathBuf;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    #[derive(Debug, Parser)]
    struct EvalArgsHarness {
        #[command(flatten)]
        eval: EvalArgs,
    }

    fn env_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn set_env_var(key: &str, value: &str) -> Option<String> {
        let previous = std::env::var(key).ok();
        // Safe in tests because access is serialized with env_test_lock().
        unsafe { std::env::set_var(key, value) };
        previous
    }

    fn clear_env_var(key: &str) -> Option<String> {
        let previous = std::env::var(key).ok();
        // Safe in tests because access is serialized with env_test_lock().
        unsafe { std::env::remove_var(key) };
        previous
    }

    fn restore_env_var(key: &str, previous: Option<String>) {
        match previous {
            Some(value) => {
                // Safe in tests because access is serialized with env_test_lock().
                unsafe { std::env::set_var(key, value) };
            }
            None => {
                // Safe in tests because access is serialized with env_test_lock().
                unsafe { std::env::remove_var(key) };
            }
        }
    }

    fn get_command_env(command: &Command, key: &str) -> Option<String> {
        command.as_std().get_envs().find_map(|(env_key, value)| {
            if env_key == OsStr::new(key) {
                value.map(|v| v.to_string_lossy().to_string())
            } else {
                None
            }
        })
    }

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
    fn expand_eval_file_globs_expands_flat_glob() {
        let dir = make_temp_dir("glob-flat");
        fs::write(dir.join("a.eval.ts"), "").unwrap();
        fs::write(dir.join("b.eval.ts"), "").unwrap();

        let pattern = dir.join("*.eval.ts").to_string_lossy().into_owned();
        let mut result = expand_eval_file_globs(&[pattern]).expect("glob should expand");
        result.sort();

        assert_eq!(result.len(), 2);
        assert!(result[0].ends_with("a.eval.ts"));
        assert!(result[1].ends_with("b.eval.ts"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn expand_eval_file_globs_expands_recursive_glob() {
        let dir = make_temp_dir("glob-recursive");
        let sub = dir.join("sub");
        fs::create_dir_all(&sub).unwrap();
        fs::write(dir.join("a.eval.ts"), "").unwrap();
        fs::write(sub.join("b.eval.ts"), "").unwrap();

        let pattern = dir.join("**/*.eval.ts").to_string_lossy().into_owned();
        let mut result = expand_eval_file_globs(&[pattern]).expect("glob should expand");
        result.sort();

        assert_eq!(result.len(), 2);
        assert!(result[0].ends_with("a.eval.ts"));
        assert!(result[1].ends_with("b.eval.ts"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn expand_eval_file_globs_errors_on_no_matches() {
        let dir = make_temp_dir("glob-no-match");
        let pattern = dir.join("*.eval.ts").to_string_lossy().into_owned();
        let err = expand_eval_file_globs(&[pattern]).expect_err("empty glob should fail");
        assert!(format!("{err}").contains("glob pattern matched no files"));
        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn expand_eval_file_globs_passes_through_non_glob() {
        let result = expand_eval_file_globs(&["src/foo.eval.ts".to_string()])
            .expect("non-glob should pass through");
        assert_eq!(result, vec!["src/foo.eval.ts"]);
    }

    #[test]
    fn expand_eval_file_globs_expands_directory() {
        let dir = make_temp_dir("glob-dir");
        let sub = dir.join("sub");
        fs::create_dir_all(&sub).unwrap();
        // eval files at root and in subdirectory
        fs::write(dir.join("a.eval.ts"), "").unwrap();
        fs::write(sub.join("b.eval.py"), "").unwrap();
        // non-eval file — should NOT be included
        fs::write(dir.join("helper.ts"), "").unwrap();

        let mut result = expand_eval_file_globs(&[dir.to_string_lossy().into_owned()])
            .expect("dir should expand");
        result.sort();

        assert_eq!(
            result.len(),
            2,
            "expected only *.eval.* files, got: {result:?}"
        );
        assert!(result.iter().any(|p| p.ends_with("a.eval.ts")));
        assert!(result.iter().any(|p| p.ends_with("b.eval.py")));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn expand_eval_file_globs_directory_finds_eval_prefix_files() {
        // `bt eval .` should find eval_*.py just like the defaults do.
        let dir = make_temp_dir("glob-dir-eval-prefix");
        fs::write(dir.join("eval_foo.py"), "").unwrap();
        fs::write(dir.join("foo.eval.ts"), "").unwrap();
        fs::write(dir.join("helper.py"), "").unwrap(); // not an eval file

        let mut result =
            expand_eval_file_globs(&[dir.to_string_lossy().into_owned()]).expect("should expand");
        result.sort();

        assert_eq!(
            result.len(),
            2,
            "should find both eval_*.py and *.eval.ts: {result:?}"
        );
        assert!(result.iter().any(|p| p.ends_with("eval_foo.py")));
        assert!(result.iter().any(|p| p.ends_with("foo.eval.ts")));

        let _ = fs::remove_dir_all(&dir);
    }

    // --- Mixed inputs ---

    #[test]
    fn expand_eval_file_globs_dir_and_explicit_file() {
        let dir = make_temp_dir("glob-mixed-dir-file");
        fs::write(dir.join("a.eval.ts"), "").unwrap();
        let explicit = dir.join("explicit.eval.ts");
        fs::write(&explicit, "").unwrap();

        // Pass the dir (which finds a.eval.ts) plus the explicit file separately.
        // The explicit file also lives inside the dir, so dedup must keep it once.
        let dir_str = dir.to_string_lossy().into_owned();
        let explicit_str = explicit.to_string_lossy().into_owned();
        let result = expand_eval_file_globs(&[dir_str, explicit_str]).expect("should expand");

        assert_eq!(
            result.len(),
            2,
            "dir already covers explicit, should dedup: {result:?}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn expand_eval_file_globs_glob_and_explicit_file() {
        let dir = make_temp_dir("glob-mixed-glob-file");
        fs::write(dir.join("a.eval.ts"), "").unwrap();
        fs::write(dir.join("b.eval.ts"), "").unwrap();
        // explicit file outside what the glob matches
        fs::write(dir.join("c.eval.py"), "").unwrap();

        let glob_pat = dir.join("*.eval.ts").to_string_lossy().into_owned();
        let explicit = dir.join("c.eval.py").to_string_lossy().into_owned();
        let mut result = expand_eval_file_globs(&[glob_pat, explicit]).expect("should expand");
        result.sort();

        assert_eq!(result.len(), 3, "glob + explicit file: {result:?}");
        assert!(result.iter().any(|p| p.ends_with("a.eval.ts")));
        assert!(result.iter().any(|p| p.ends_with("b.eval.ts")));
        assert!(result.iter().any(|p| p.ends_with("c.eval.py")));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn expand_eval_file_globs_dir_and_glob_deduplicates() {
        let dir = make_temp_dir("glob-dedup");
        fs::write(dir.join("a.eval.ts"), "").unwrap();
        fs::write(dir.join("b.eval.ts"), "").unwrap();

        // Both the directory expansion and the glob would match a.eval.ts and b.eval.ts.
        let dir_str = dir.to_string_lossy().into_owned();
        let glob_pat = dir.join("*.eval.ts").to_string_lossy().into_owned();
        let result = expand_eval_file_globs(&[dir_str, glob_pat]).expect("should expand");

        assert_eq!(
            result.len(),
            2,
            "overlapping dir + glob should dedup: {result:?}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    // --- Default discovery exclusions ---

    #[test]
    fn is_excluded_by_default_matches_known_dirs() {
        assert!(is_excluded_by_default("node_modules/foo/bar.eval.ts"));
        assert!(is_excluded_by_default("a/b/node_modules/pkg/x.eval.ts"));
        assert!(is_excluded_by_default("site-packages/lib/x.eval.py"));
        assert!(is_excluded_by_default("__pycache__/x.eval.py"));
        assert!(is_excluded_by_default(".venv/lib/x.eval.py"));
        assert!(is_excluded_by_default("venv/lib/x.eval.py"));
    }

    #[test]
    fn is_excluded_by_default_allows_normal_paths() {
        assert!(!is_excluded_by_default("src/foo.eval.ts"));
        assert!(!is_excluded_by_default("tests/sub/bar.eval.py"));
        assert!(!is_excluded_by_default("my_node_modules_backup/x.eval.ts"));
    }

    #[test]
    fn expand_eval_file_globs_defaults_excludes_node_modules() {
        let dir = make_temp_dir("glob-defaults-exclude");
        let nm = dir.join("node_modules").join("pkg");
        fs::create_dir_all(&nm).unwrap();
        fs::write(dir.join("a.eval.ts"), "").unwrap();
        fs::write(nm.join("b.eval.ts"), "").unwrap();

        let orig = std::env::current_dir().unwrap();
        std::env::set_current_dir(&dir).unwrap();
        let mut result =
            expand_eval_file_globs(&[".".to_string()]).expect("defaults should expand");
        result.retain(|p| !is_excluded_by_default(p));
        std::env::set_current_dir(orig).unwrap();

        assert_eq!(
            result.len(),
            1,
            "node_modules should be excluded: {result:?}"
        );
        assert!(result[0].ends_with("a.eval.ts"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn expand_eval_file_globs_explicit_does_not_exclude_node_modules() {
        let dir = make_temp_dir("glob-explicit-node-modules");
        let nm = dir.join("node_modules").join("pkg");
        fs::create_dir_all(&nm).unwrap();
        let target = nm.join("b.eval.ts");
        fs::write(&target, "").unwrap();

        let pattern = target.to_string_lossy().into_owned();
        let result = expand_eval_file_globs(&[pattern]).expect("explicit path should pass through");

        assert_eq!(
            result.len(),
            1,
            "explicit path in node_modules should not be excluded: {result:?}"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    // --- Edge cases ---

    #[test]
    fn expand_eval_file_globs_errors_on_dir_with_no_eval_files() {
        let dir = make_temp_dir("glob-empty-dir");
        fs::write(dir.join("helper.ts"), "").unwrap(); // not an eval file

        let err = expand_eval_file_globs(&[dir.to_string_lossy().into_owned()])
            .expect_err("empty dir should fail");
        assert!(format!("{err}").contains("no eval files found in directory"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn expand_eval_file_globs_errors_on_missing_directory() {
        let err = expand_eval_file_globs(&["nonexistent/".to_string()])
            .expect_err("missing dir should fail");
        assert!(format!("{err}").contains("directory not found"));
    }

    #[test]
    fn expand_eval_file_globs_missing_file_passes_through_to_validation() {
        // Non-glob, non-directory paths pass through so validate_eval_input_files
        // can produce its existing "file not found" error with suggestions.
        let result = expand_eval_file_globs(&["nonexistent.eval.ts".to_string()])
            .expect("missing file should pass through expansion");
        assert_eq!(result, vec!["nonexistent.eval.ts"]);
    }

    #[test]
    fn validate_eval_input_files_reports_missing_path_with_prefixed_directory_hint() {
        let dir = make_temp_dir("missing-file-hint");
        let eval_file = dir.join("real-world-facets.eval.ts");
        fs::write(&eval_file, "export {};").expect("eval file should be written");

        let cwd_name = dir
            .file_name()
            .and_then(|name| name.to_str())
            .expect("temp dir should have a UTF-8 name");
        let input = format!("{cwd_name}/real-world-facets.eval.ts");
        let err = validate_eval_input_files_in_cwd(&[input], &dir)
            .expect_err("duplicated cwd prefix should fail");
        let message = format!("{err:#}");

        assert!(message.contains("Eval file not found:"));
        assert!(message.contains("Did you mean `bt eval real-world-facets.eval.ts`?"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn validate_eval_input_files_rejects_directory_inputs() {
        let dir = make_temp_dir("directory-input");
        let child_dir = dir.join("evals");
        fs::create_dir_all(&child_dir).expect("child directory should be created");

        let err = validate_eval_input_files_in_cwd(&["evals".to_string()], &dir)
            .expect_err("directory inputs should fail");
        let message = format!("{err:#}");

        assert!(message.contains("Eval file is not a regular file:"));
        assert!(message.contains("from input `evals`"));

        let _ = fs::remove_dir_all(&dir);
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
    fn runner_kind_for_bin_detects_bun() {
        assert_eq!(runner_kind_for_bin(Path::new("bun")), RunnerKind::Bun);
        assert_eq!(runner_kind_for_bin(Path::new("bunx")), RunnerKind::Bun);
        assert_eq!(runner_kind_for_bin(Path::new("deno")), RunnerKind::Other);
    }

    #[test]
    fn set_node_heap_size_env_sets_default_when_absent() {
        let _guard = env_test_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = clear_env_var("NODE_OPTIONS");
        let mut command = Command::new("node");

        set_node_heap_size_env(&mut command);
        let configured =
            get_command_env(&command, "NODE_OPTIONS").expect("NODE_OPTIONS should be set");
        assert!(configured.contains("--max-old-space-size=8192"));

        restore_env_var("NODE_OPTIONS", previous);
    }

    #[test]
    fn set_node_heap_size_env_appends_to_existing_options() {
        let _guard = env_test_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = set_env_var("NODE_OPTIONS", "--trace-warnings");
        let mut command = Command::new("node");

        set_node_heap_size_env(&mut command);
        let configured =
            get_command_env(&command, "NODE_OPTIONS").expect("NODE_OPTIONS should be set");
        assert!(configured.contains("--trace-warnings"));
        assert!(configured.contains("--max-old-space-size=8192"));

        restore_env_var("NODE_OPTIONS", previous);
    }

    #[test]
    fn set_node_heap_size_env_preserves_existing_heap_override() {
        let _guard = env_test_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let previous = set_env_var("NODE_OPTIONS", "--max-old-space-size=2048");
        let mut command = Command::new("node");

        set_node_heap_size_env(&mut command);
        let configured =
            get_command_env(&command, "NODE_OPTIONS").expect("NODE_OPTIONS should be set");
        assert_eq!(configured, "--max-old-space-size=2048");

        restore_env_var("NODE_OPTIONS", previous);
    }

    #[test]
    fn should_set_node_heap_size_skips_non_node_runtimes() {
        assert!(should_set_node_heap_size(RunnerKind::Tsx));
        assert!(should_set_node_heap_size(RunnerKind::ViteNode));
        assert!(should_set_node_heap_size(RunnerKind::Other));
        assert!(!should_set_node_heap_size(RunnerKind::Deno));
        assert!(!should_set_node_heap_size(RunnerKind::Bun));
    }

    #[test]
    fn build_sse_socket_path_is_unique_for_consecutive_calls() {
        let first = build_socket_path("bt-eval").expect("first socket path");
        let second = build_socket_path("bt-eval").expect("second socket path");
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
    fn format_processing_line_handles_pluralization() {
        assert_eq!(format_processing_line(1), "Processing 1 evaluator...");
        assert_eq!(format_processing_line(2), "Processing 2 evaluators...");
    }

    #[test]
    fn format_start_line_handles_partial_payload() {
        let start = ExperimentStart {
            experiment_name: Some("my-exp".to_string()),
            experiment_url: Some("https://example.dev/exp".to_string()),
            ..Default::default()
        };
        let line = format_start_line(&start).expect("line should be rendered");
        assert!(line.contains("my-exp"));
        assert!(line.contains("https://example.dev/exp"));

        assert!(format_start_line(&ExperimentStart::default()).is_none());
    }

    #[test]
    fn fit_name_to_spaces_preserves_suffix_when_truncating() {
        let rendered =
            fit_name_to_spaces("Topics [experimentName=facets-real-world-30b-f5a78312]", 40);
        assert_eq!(rendered.chars().count(), 40);
        assert!(rendered.contains("..."));
        assert!(rendered.contains("f5a78312]"));
    }

    #[test]
    fn fit_name_to_spaces_pads_short_names() {
        let rendered = fit_name_to_spaces("short", 10);
        assert_eq!(rendered, "short     ");
    }

    #[test]
    fn handle_sse_event_parses_processing_and_start_payloads() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        handle_sse_event(
            Some("processing".to_string()),
            r#"{"evaluators": 2}"#.to_string(),
            &tx,
        );
        handle_sse_event(
            Some("start".to_string()),
            r#"{"experiment_name":"my-exp","experiment_url":"https://example.dev/exp"}"#
                .to_string(),
            &tx,
        );

        match rx.try_recv().expect("processing event should be emitted") {
            EvalEvent::Processing(payload) => assert_eq!(payload.evaluators, 2),
            other => panic!("unexpected first event: {other:?}"),
        }
        match rx.try_recv().expect("start event should be emitted") {
            EvalEvent::Start(start) => {
                assert_eq!(start.experiment_name.as_deref(), Some("my-exp"));
                assert_eq!(
                    start.experiment_url.as_deref(),
                    Some("https://example.dev/exp")
                );
            }
            other => panic!("unexpected second event: {other:?}"),
        }
    }

    #[test]
    fn handle_sse_event_parses_summary_without_comparison_fields() {
        let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
        handle_sse_event(
            Some("summary".to_string()),
            r#"{
              "projectName":"Topics",
              "experimentName":"facets-real-world-thinking-on",
              "scores":{
                "Factuality":{"name":"Factuality","score":0.62}
              }
            }"#
            .to_string(),
            &tx,
        );

        match rx.try_recv().expect("summary event should be emitted") {
            EvalEvent::Summary(summary) => {
                let factuality = summary
                    .scores
                    .get("Factuality")
                    .expect("score should exist");
                assert_eq!(factuality.improvements, 0);
                assert_eq!(factuality.regressions, 0);
                assert!(factuality.diff.is_none());
            }
            other => panic!("unexpected event: {other:?}"),
        }
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

    #[test]
    fn eval_args_from_env_populates_supported_fields() {
        let _guard = env_test_lock()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());
        let keys = [
            "BT_EVAL_JSONL",
            "BT_EVAL_TERMINATE_ON_FAILURE",
            "BT_EVAL_NUM_WORKERS",
            "BT_EVAL_LIST",
            "BT_EVAL_SANDBOX",
            "BT_EVAL_FILTER",
            "BT_EVAL_VERBOSE",
            "BT_EVAL_WATCH",
            "BT_EVAL_DEV",
            "BT_EVAL_DEV_HOST",
            "BT_EVAL_DEV_PORT",
            "BT_EVAL_DEV_ORG_NAME",
        ];
        let previous: Vec<(&str, Option<String>)> =
            keys.iter().map(|key| (*key, clear_env_var(key))).collect();
        set_env_var("BT_EVAL_JSONL", "true");
        set_env_var("BT_EVAL_TERMINATE_ON_FAILURE", "1");
        set_env_var("BT_EVAL_NUM_WORKERS", "4");
        set_env_var("BT_EVAL_LIST", "yes");
        set_env_var("BT_EVAL_SANDBOX", "lambda");
        set_env_var("BT_EVAL_FILTER", "metadata.case=smoke.*,metadata.kind=fast");
        set_env_var("BT_EVAL_VERBOSE", "1");
        set_env_var("BT_EVAL_WATCH", "on");
        set_env_var("BT_EVAL_DEV", "true");
        set_env_var("BT_EVAL_DEV_HOST", "127.0.0.1");
        set_env_var("BT_EVAL_DEV_PORT", "9999");
        set_env_var("BT_EVAL_DEV_ORG_NAME", "acme");

        let parsed = EvalArgsHarness::try_parse_from(["bt", "sample.eval.ts"])
            .expect("env vars should parse into eval args");
        assert!(parsed.eval.jsonl);
        assert!(parsed.eval.terminate_on_failure);
        assert_eq!(parsed.eval.num_workers, Some(4));
        assert!(parsed.eval.list);
        assert_eq!(parsed.eval.sandbox, EvalSandbox::Lambda);
        assert_eq!(
            parsed.eval.filter,
            vec![
                "metadata.case=smoke.*".to_string(),
                "metadata.kind=fast".to_string()
            ]
        );
        assert!(parsed.eval.verbose);
        assert!(parsed.eval.watch);
        assert!(parsed.eval.dev);
        assert_eq!(parsed.eval.dev_host, "127.0.0.1");
        assert_eq!(parsed.eval.dev_port, 9999);
        assert_eq!(parsed.eval.dev_org_name, Some("acme".to_string()));

        for (key, value) in previous {
            restore_env_var(key, value);
        }
    }

    #[test]
    fn build_sandbox_summary_query_includes_timestamp_filter() {
        let query = build_sandbox_summary_query("exp'123", "2026-03-19T12:00:00.000Z");
        assert!(query.contains("from: experiment('exp''123') summary"));
        assert!(query.contains("filter: created >= '2026-03-19T12:00:00.000Z'"));
        assert!(query.contains("select: scores, metrics"));
    }

    #[test]
    fn sandbox_slug_from_source_uses_source_stem_and_eval_name() {
        let slug = sandbox_slug_from_source(Path::new("/tmp/My Eval.ts"), "Demo Eval");
        assert_eq!(slug, "my-eval-demo-eval-sandbox");
    }
}
