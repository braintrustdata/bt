use std::collections::{BTreeSet, HashMap, VecDeque};
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::{Args, ValueEnum};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::net::UnixListener;
use tokio::process::Command;
use tokio::sync::mpsc;

use crate::args::BaseArgs;
use crate::auth::resolved_auth_env;

mod dev_server;
mod events;
mod ui;

use self::dev_server::{
    collect_allowed_dev_origins, resolve_app_url, run_dev_server, DevServerState,
};
use self::events::{
    EvalErrorPayload, EvalEvent, ExperimentStart, ExperimentSummary, ProcessingEventData,
    SseConsoleEventData, SseDependenciesEventData, SseProgressEventData,
};
use self::ui::EvalUi;

const MAX_NAME_LENGTH: usize = 40;
const WATCH_POLL_INTERVAL: Duration = Duration::from_millis(500);
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
            false,
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
            true,
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
    collect_dependencies: bool,
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

    if let Some(message) = missing_vite_node_retry_message(&output) {
        anyhow::bail!(message);
    }

    let dependencies = if collect_dependencies {
        let mut dependencies =
            normalize_watch_paths(output.dependency_files.into_iter().map(PathBuf::from))?;
        if plan.language == EvalLanguage::JavaScript {
            let static_dependencies = collect_js_static_dependencies(files)?;
            dependencies = merge_watch_paths(&dependencies, &static_dependencies);
        }
        dependencies
    } else {
        Vec::new()
    };

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

fn missing_vite_node_retry_message(output: &EvalAttemptOutput) -> Option<&'static str> {
    if output.runner_kind != RunnerKind::ViteNode {
        return None;
    }

    let missing_runner = output
        .stderr_lines
        .iter()
        .chain(&output.error_messages)
        .any(|line| line.contains("vite-node: command not found"));
    let exited_with_command_not_found = output.status.code() == Some(127);
    if !missing_runner && !exited_with_command_not_found {
        return None;
    }

    Some(
        "The eval could not be retried in ESM mode. The initial `tsx` load hit an ESM/CJS interop error while loading the eval (for example an ESM-only dependency), and the `vite-node` fallback exited before the eval started. This usually means `vite-node` is not installed in this workspace or available on PATH. Install `vite-node` in the eval workspace, for example `pnpm add -D vite-node`, then rerun `bt eval`.",
    )
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

    #[cfg(unix)]
    fn exit_status(code: i32) -> ExitStatus {
        use std::os::unix::process::ExitStatusExt;
        ExitStatus::from_raw(code << 8)
    }

    #[cfg(windows)]
    fn exit_status(code: i32) -> ExitStatus {
        use std::os::windows::process::ExitStatusExt;
        ExitStatus::from_raw(code as u32)
    }

    fn success_status() -> ExitStatus {
        exit_status(0)
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
    fn missing_vite_node_retry_message_is_user_facing() {
        let output = EvalAttemptOutput {
            status: success_status(),
            dependency_files: Vec::new(),
            error_messages: Vec::new(),
            stderr_lines: vec!["sh: vite-node: command not found".to_string()],
            runner_kind: RunnerKind::ViteNode,
        };

        let message = missing_vite_node_retry_message(&output).expect("missing vite-node message");
        assert!(message.contains("could not be retried in ESM mode"));
        assert!(message.contains("pnpm add -D vite-node"));
        assert!(message.contains("`tsx` load hit an ESM/CJS interop error"));
    }

    #[test]
    fn missing_vite_node_retry_message_uses_exit_code_127_fallback() {
        let output = EvalAttemptOutput {
            status: exit_status(127),
            dependency_files: Vec::new(),
            error_messages: Vec::new(),
            stderr_lines: Vec::new(),
            runner_kind: RunnerKind::ViteNode,
        };

        let message = missing_vite_node_retry_message(&output).expect("missing vite-node message");
        assert!(message.contains("`vite-node` fallback exited before the eval started"));
        assert!(message.contains("pnpm add -D vite-node"));
    }

    #[test]
    fn build_sse_socket_path_is_unique_for_consecutive_calls() {
        let first = build_sse_socket_path().expect("first socket path");
        let second = build_sse_socket_path().expect("second socket path");
        assert_ne!(first, second);
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
}
