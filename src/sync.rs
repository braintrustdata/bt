use std::collections::{BTreeMap, HashSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufRead, BufReader, BufWriter, IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context, Result};
use backoff::future::retry_notify;
use backoff::{Error as BackoffError, ExponentialBackoffBuilder};
use braintrust_sdk_rust::Logs3BatchUploader;
use clap::{Args, Subcommand, ValueEnum};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use sha2::{Digest, Sha256};
use urlencoding::encode;

use crate::args::BaseArgs;
use crate::http::ApiClient;
use crate::login::{login, LoginContext};
use crate::projects::api::list_projects;
use crate::ui::fuzzy_select;

const STATE_SCHEMA_VERSION: u32 = 1;
const DEFAULT_PULL_LIMIT: usize = 100;
const DEFAULT_PAGE_SIZE: usize = 200;
// BTQL currently enforces limit <= 1000.
const ROOT_DISCOVERY_PAGE_SIZE: usize = 1000;
const ROOT_FETCH_CHUNK_SIZE: usize = 100;
const BTQL_MAX_ATTEMPTS: usize = 5;
const BTQL_RETRY_BASE_DELAY_MS: u64 = 300;
const BTQL_MAX_BACKOFF_SECS: u64 = 8;
const PULL_OUTPUT_PART_MAX_BYTES: u64 = 128 * 1024 * 1024;

#[derive(Debug, Clone, Args)]
pub struct SyncArgs {
    #[command(subcommand)]
    command: SyncCommand,
}

#[derive(Debug, Clone, Subcommand)]
enum SyncCommand {
    /// Download objects from Braintrust into local JSONL files/directories.
    Pull(PullArgs),
    /// Upload local JSONL rows back into Braintrust.
    Push(PushArgs),
    /// Show the local state/manifest for a sync spec.
    Status(StatusArgs),
}

#[derive(Debug, Clone, Args)]
struct PullArgs {
    /// Object reference, format: object_type:object_id (e.g. project_logs:1234-uuid)
    object_ref: Option<String>,

    /// SQL filter expression.
    #[arg(long)]
    filter: Option<String>,

    /// Number of traces to fetch (default when no limit flag is set).
    #[arg(long)]
    traces: Option<usize>,

    /// Number of spans to fetch.
    #[arg(long)]
    spans: Option<usize>,

    /// Page size for BTQL pagination.
    #[arg(long, default_value_t = DEFAULT_PAGE_SIZE)]
    page_size: usize,

    /// Initial cursor for spans mode. Implies a fresh run.
    #[arg(long)]
    cursor: Option<String>,

    /// Ignore previous state and start over for this spec.
    #[arg(long)]
    fresh: bool,

    /// Root directory for sync artifacts.
    #[arg(long, default_value = "bt-sync")]
    root: PathBuf,

    /// Number of concurrent workers for trace fetch mode.
    #[arg(long, default_value_t = 8)]
    workers: usize,
}

#[derive(Debug, Clone, Args)]
struct PushArgs {
    /// Object reference, format: object_type:object_id (e.g. project_logs:1234-uuid)
    object_ref: String,

    /// Input JSONL/NDJSON file or directory of part files. If omitted, bt sync uses the latest completed pull output.
    #[arg(long = "in")]
    input: Option<PathBuf>,

    /// SQL filter expression (used in spec hashing / pull auto-resolution).
    #[arg(long)]
    filter: Option<String>,

    /// Upload rows belonging to at most N distinct root traces.
    #[arg(long)]
    traces: Option<usize>,

    /// Upload at most N span rows.
    #[arg(long)]
    spans: Option<usize>,

    /// Rows per upload batch.
    #[arg(long, default_value_t = DEFAULT_PAGE_SIZE)]
    page_size: usize,

    /// Ignore previous state and start over for this spec.
    #[arg(long)]
    fresh: bool,

    /// Root directory for sync artifacts.
    #[arg(long, default_value = "bt-sync")]
    root: PathBuf,

    /// Number of concurrent workers for upload mode.
    #[arg(long, default_value_t = 8)]
    workers: usize,
}

#[derive(Debug, Clone, Args)]
struct StatusArgs {
    /// Object reference, format: object_type:object_id (e.g. project_logs:1234-uuid)
    object_ref: String,

    /// Direction for status lookup.
    #[arg(long, value_enum, default_value = "pull")]
    direction: DirectionArg,

    /// SQL filter expression.
    #[arg(long)]
    filter: Option<String>,

    /// Trace limit for this spec.
    #[arg(long)]
    traces: Option<usize>,

    /// Span limit for this spec.
    #[arg(long)]
    spans: Option<usize>,

    /// Page size used in this spec.
    #[arg(long, default_value_t = DEFAULT_PAGE_SIZE)]
    page_size: usize,

    /// Root directory for sync artifacts.
    #[arg(long, default_value = "bt-sync")]
    root: PathBuf,
}

#[derive(Debug, Clone, Copy, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum DirectionArg {
    Pull,
    Push,
}

impl DirectionArg {
    fn as_str(self) -> &'static str {
        match self {
            DirectionArg::Pull => "pull",
            DirectionArg::Push => "push",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
enum ScopeArg {
    Traces,
    Spans,
    All,
}

impl ScopeArg {
    fn as_str(&self) -> &'static str {
        match self {
            ScopeArg::Traces => "traces",
            ScopeArg::Spans => "spans",
            ScopeArg::All => "all",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum RunStatus {
    Running,
    Interrupted,
    Completed,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PullPhase {
    DiscoverRoots,
    FetchRoots,
    Spans,
    Completed,
}

#[derive(Debug, Clone)]
struct ObjectRef {
    object_type: String,
    object_name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SyncSpec {
    schema_version: u32,
    object_ref: String,
    object_type: String,
    object_name: String,
    direction: String,
    scope: String,
    filter: Option<String>,
    limit: Option<usize>,
    page_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct SyncManifest {
    schema_version: u32,
    spec_hash: String,
    spec: SyncSpec,
    last_run_id: String,
    status: RunStatus,
    items_done: usize,
    pages_done: usize,
    bytes_processed: u64,
    output_path: Option<String>,
    input_path: Option<String>,
    started_at: u64,
    updated_at: u64,
    completed_at: Option<u64>,
    message: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PullState {
    schema_version: u32,
    run_id: String,
    status: RunStatus,
    phase: PullPhase,
    scope: String,
    limit: usize,
    filter: Option<String>,
    page_size: usize,
    cursor: Option<String>,
    root_discovery_cursor: Option<String>,
    root_ids: Vec<String>,
    current_root_index: usize,
    current_root_cursor: Option<String>,
    #[serde(default)]
    trace_chunks: Vec<TraceChunkState>,
    items_done: usize,
    pages_done: usize,
    bytes_written: u64,
    output_path: String,
    started_at: u64,
    updated_at: u64,
    completed_at: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct TraceChunkState {
    chunk_index: usize,
    start: usize,
    end: usize,
    cursor: Option<String>,
    completed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct PushState {
    schema_version: u32,
    run_id: String,
    status: RunStatus,
    scope: String,
    limit: Option<usize>,
    page_size: usize,
    source_path: String,
    line_offset: usize,
    items_done: usize,
    pages_done: usize,
    bytes_sent: u64,
    distinct_roots_done: usize,
    started_at: u64,
    updated_at: u64,
    completed_at: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct BtqlResponse {
    data: Vec<Map<String, Value>>,
    #[serde(default)]
    cursor: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct NamedObject {
    id: String,
    name: String,
}

#[derive(Debug, Deserialize)]
struct NamedObjectListResponse {
    objects: Vec<NamedObject>,
}

#[derive(Debug, Clone)]
struct InteractiveChoice {
    label: String,
    object_ref: String,
}

#[derive(Debug)]
struct PushBatchWork {
    batch_index: usize,
    rows: Vec<Map<String, Value>>,
    end_line_offset: usize,
    distinct_roots_done: usize,
}

#[derive(Debug)]
struct PushBatchResult {
    batch_index: usize,
    row_count: usize,
    bytes_sent: u64,
    end_line_offset: usize,
    distinct_roots_done: usize,
}

#[derive(Debug, Default)]
struct BtqlRetryTracker {
    total_retries: AtomicUsize,
    counts_by_type: Mutex<BTreeMap<String, usize>>,
}

impl BtqlRetryTracker {
    fn record_status(&self, status_code: u16) {
        self.record(format!("{status_code}"));
    }

    fn record_network(&self) {
        self.record("network".to_string());
    }

    fn summary_line(&self) -> Option<String> {
        let total = self.total_retries.load(Ordering::Relaxed);
        if total == 0 {
            return None;
        }

        let counts = match self.counts_by_type.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut entries: Vec<(String, usize)> =
            counts.iter().map(|(k, v)| (k.clone(), *v)).collect();
        entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));

        if entries.len() == 1 {
            let (kind, count) = &entries[0];
            return Some(format!(
                "BTQL retries: {} ({})",
                format_usize_commas(*count),
                retry_kind_label(kind)
            ));
        }

        let detail = entries
            .iter()
            .take(3)
            .map(|(kind, count)| {
                format!("{} {}", retry_kind_label(kind), format_usize_commas(*count))
            })
            .collect::<Vec<_>>()
            .join(", ");
        Some(format!(
            "BTQL retries: {} total ({detail})",
            format_usize_commas(total)
        ))
    }

    fn record(&self, kind: String) {
        self.total_retries.fetch_add(1, Ordering::Relaxed);
        let mut counts = match self.counts_by_type.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        *counts.entry(kind).or_insert(0) += 1;
    }
}

fn retry_kind_label(kind: &str) -> String {
    if kind == "network" {
        "network".to_string()
    } else if kind.chars().all(|ch| ch.is_ascii_digit()) {
        format!("HTTP {kind}")
    } else {
        kind.to_string()
    }
}

struct JsonlPartWriter {
    base_dir: PathBuf,
    part_index: usize,
    current_bytes: u64,
    writer: BufWriter<File>,
}

pub async fn run(base: BaseArgs, args: SyncArgs) -> Result<()> {
    match args.command {
        SyncCommand::Pull(pull) => {
            let ctx = login(&base).await?;
            let client = ApiClient::new(&ctx)?;
            run_pull(base.json, &ctx, &client, pull).await
        }
        SyncCommand::Push(push) => {
            let ctx = login(&base).await?;
            run_push(base.json, &ctx, push).await
        }
        SyncCommand::Status(status) => run_status(base.json, status),
    }
}

async fn run_pull(
    json_output: bool,
    ctx: &LoginContext,
    client: &ApiClient,
    args: PullArgs,
) -> Result<()> {
    let resolved_object_ref = resolve_pull_object_ref(client, args.object_ref.as_deref()).await?;
    let object = parse_object_ref(&resolved_object_ref)?;
    let source_expr = btql_source_expr(&object)?;
    let (scope, limit) = resolve_pull_scope_and_limit(args.traces, args.spans)?;
    let fresh = args.fresh || args.cursor.is_some();

    let spec = SyncSpec {
        schema_version: STATE_SCHEMA_VERSION,
        object_ref: resolved_object_ref,
        object_type: object.object_type.clone(),
        object_name: object.object_name.clone(),
        direction: DirectionArg::Pull.as_str().to_string(),
        scope: scope.as_str().to_string(),
        filter: trim_optional(args.filter.clone()),
        limit: Some(limit),
        page_size: args.page_size,
    };

    let spec_hash = spec_hash(&spec)?;
    let spec_dir = resolve_spec_dir(
        &args.root,
        &object,
        DirectionArg::Pull,
        &scope,
        &spec_hash,
        !fresh,
    )?;
    fs::create_dir_all(&spec_dir)
        .with_context(|| format!("failed to create {}", spec_dir.display()))?;

    let spec_path = spec_dir.join("spec.json");
    write_json_atomic(&spec_path, &spec)?;

    let state_path = spec_dir.join("state.json");
    let manifest_path = spec_dir.join("manifest.json");
    let output_path = spec_dir.join("data");

    let mut state = if fresh || !state_path.exists() {
        new_pull_state(
            &scope,
            limit,
            args.page_size,
            spec.filter.clone(),
            args.cursor.clone(),
            output_path.to_string_lossy().to_string(),
        )
    } else {
        read_json_file::<PullState>(&state_path)?
    };

    if state.status == RunStatus::Completed && !fresh {
        if json_output {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "status": "completed",
                    "message": "already completed for this spec",
                    "spec_dir": spec_dir,
                    "output_path": state.output_path,
                    "items_done": state.items_done,
                    "pages_done": state.pages_done
                }))?
            );
        } else {
            println!(
                "Sync already completed for this spec. output={} items={} pages={}",
                state.output_path, state.items_done, state.pages_done
            );
        }
        return Ok(());
    }

    let previous_output_path = state.output_path.clone();

    let effective_fresh = fresh;

    if effective_fresh && output_path.exists() {
        if output_path.is_dir() {
            fs::remove_dir_all(&output_path)
                .with_context(|| format!("failed to remove {}", output_path.display()))?;
        } else {
            fs::remove_file(&output_path)
                .with_context(|| format!("failed to remove {}", output_path.display()))?;
        }
    }
    if effective_fresh {
        let legacy_jsonl_path = spec_dir.join("data.jsonl");
        if legacy_jsonl_path.exists() {
            fs::remove_file(&legacy_jsonl_path)
                .with_context(|| format!("failed to remove {}", legacy_jsonl_path.display()))?;
        }
        let legacy_output_path = spec_dir.join("data.ndjson");
        if legacy_output_path.exists() {
            fs::remove_file(&legacy_output_path)
                .with_context(|| format!("failed to remove {}", legacy_output_path.display()))?;
        }
    } else {
        let legacy_output_path = PathBuf::from(previous_output_path);
        if legacy_output_path.is_file() {
            fs::create_dir_all(&output_path)
                .with_context(|| format!("failed to create {}", output_path.display()))?;
            let migrated = output_part_path(&output_path, 1);
            if !migrated.exists() {
                fs::rename(&legacy_output_path, &migrated).with_context(|| {
                    format!(
                        "failed to migrate legacy output {} to {}",
                        legacy_output_path.display(),
                        migrated.display()
                    )
                })?;
            }
        }
    }
    if state.items_done == 0 && output_path.exists() {
        if output_path.is_dir() {
            fs::remove_dir_all(&output_path)
                .with_context(|| format!("failed to remove {}", output_path.display()))?;
        } else {
            fs::remove_file(&output_path)
                .with_context(|| format!("failed to remove {}", output_path.display()))?;
        }
    }
    state.output_path = output_path.to_string_lossy().to_string();

    state.status = RunStatus::Running;
    state.updated_at = epoch_seconds();
    write_json_atomic(&state_path, &state)?;
    update_manifest_from_pull_state(
        &manifest_path,
        &spec_hash,
        &spec,
        &state,
        Some(RunStatus::Running),
    )?;
    let btql_retry_tracker = Arc::new(BtqlRetryTracker::default());

    match scope {
        ScopeArg::Spans => {
            pull_spans_mode(
                client,
                ctx,
                &source_expr,
                &spec,
                &output_path,
                &state_path,
                &mut state,
                &btql_retry_tracker,
                !json_output,
            )
            .await?
        }
        ScopeArg::Traces => {
            pull_traces_mode(
                client,
                ctx,
                &source_expr,
                &spec,
                &output_path,
                &state_path,
                &mut state,
                args.workers.max(1),
                &btql_retry_tracker,
                !json_output,
            )
            .await?
        }
        ScopeArg::All => bail!("invalid pull scope"),
    }

    state.status = RunStatus::Completed;
    state.phase = PullPhase::Completed;
    state.completed_at = Some(epoch_seconds());
    state.updated_at = epoch_seconds();
    write_json_atomic(&state_path, &state)?;
    update_manifest_from_pull_state(
        &manifest_path,
        &spec_hash,
        &spec,
        &state,
        Some(RunStatus::Completed),
    )?;

    if json_output {
        let warning = if state.items_done == 0 {
            Some(format!(
                "no rows found for {} in org '{}'; verify object id and active credentials",
                spec.object_ref,
                if ctx.login.org_name.trim().is_empty() {
                    "(default)".to_string()
                } else {
                    ctx.login.org_name.clone()
                }
            ))
        } else {
            None
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "status": "completed",
                "spec_dir": spec_dir,
                "output_path": output_path,
                "items_done": state.items_done,
                "pages_done": state.pages_done,
                "bytes_written": state.bytes_written,
                "scope": scope.as_str(),
                "limit": limit,
                "warning": warning
            }))?
        );
    } else {
        let elapsed_secs = epoch_seconds().saturating_sub(state.started_at).max(1);
        let traces_done = state.root_ids.len();
        let spans_done = state.items_done;
        let traces_per_sec = traces_done as f64 / elapsed_secs as f64;
        let spans_per_sec = spans_done as f64 / elapsed_secs as f64;
        let bytes_per_sec = state.bytes_written as f64 / elapsed_secs as f64;
        if state.items_done == 0 {
            let org_label = if ctx.login.org_name.trim().is_empty() {
                "(default)".to_string()
            } else {
                ctx.login.org_name.clone()
            };
            println!(
                "Warning: no rows found for {} in org '{}'; verify object id and active credentials.",
                spec.object_ref, org_label
            );
        }
        println!("Pull complete");
        println!("  Output: {}", output_path.display());
        println!("  Time: {}", format_duration(elapsed_secs));
        println!("  Traces: {}", format_usize_commas(traces_done));
        println!("  Spans: {}", format_usize_commas(spans_done));
        println!("  Pages: {}", format_usize_commas(state.pages_done));
        println!(
            "  Data: {} ({} bytes)",
            format_bytes(state.bytes_written as f64),
            format_u64_commas(state.bytes_written)
        );
        println!(
            "  Rates: {:.2} traces/s | {:.2} spans/s | {}/s",
            traces_per_sec,
            spans_per_sec,
            format_bytes(bytes_per_sec)
        );
        if let Some(retry_summary) = btql_retry_tracker.summary_line() {
            println!("  BTQL: {retry_summary}");
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn pull_spans_mode(
    client: &ApiClient,
    ctx: &LoginContext,
    source_expr: &str,
    spec: &SyncSpec,
    output_dir: &Path,
    state_path: &Path,
    state: &mut PullState,
    btql_retry_tracker: &Arc<BtqlRetryTracker>,
    show_checkpoint_hint: bool,
) -> Result<()> {
    let limit = state.limit;
    let phase_started_at = epoch_seconds();
    let baseline_roots_done = state.root_ids.len();
    let baseline_items_done = state.items_done;
    let baseline_bytes_written = state.bytes_written;
    let ui = bounded_bar_with_status_line(limit as u64, "Fetching spans", "spans");
    let pb = ui.main.clone();
    let status_line = ui.status_line.clone();
    pb.set_position(state.items_done as u64);
    pb.set_message(pull_spans_progress_message_with_baseline(
        state,
        phase_started_at,
        baseline_roots_done,
        baseline_items_done,
        baseline_bytes_written,
    ));
    status_line.set_message(pull_status_line(
        show_checkpoint_hint,
        btql_retry_tracker.summary_line().as_deref(),
    ));
    let mut seen_roots: HashSet<String> = state.root_ids.iter().cloned().collect();

    let mut writer = open_jsonl_part_writer(output_dir, state.items_done > 0)?;

    while state.items_done < limit {
        let batch_limit = (limit - state.items_done).min(spec.page_size);
        let query = build_spans_query(
            source_expr,
            spec.filter.as_deref(),
            batch_limit,
            state.cursor.as_deref(),
        );
        let response =
            execute_btql_query(client, ctx, &query, Some(Arc::clone(btql_retry_tracker))).await?;
        let batch_count = response.data.len();

        if batch_count == 0 {
            state.cursor = None;
            break;
        }

        for row in &response.data {
            if let Some(root_id) = row_root_span_id(row) {
                if !root_id.is_empty() && seen_roots.insert(root_id.clone()) {
                    state.root_ids.push(root_id);
                }
            }
            state.bytes_written += write_jsonl_row(&mut writer, row)? as u64;
        }
        writer.flush().context("failed to flush JSONL output")?;

        state.items_done += batch_count;
        state.pages_done += 1;
        state.cursor = response.cursor.filter(|c| !c.is_empty());
        state.updated_at = epoch_seconds();

        write_json_atomic(state_path, state)?;
        pb.set_position(state.items_done.min(limit) as u64);
        pb.set_message(pull_spans_progress_message_with_baseline(
            state,
            phase_started_at,
            baseline_roots_done,
            baseline_items_done,
            baseline_bytes_written,
        ));
        status_line.set_message(pull_status_line(
            show_checkpoint_hint,
            btql_retry_tracker.summary_line().as_deref(),
        ));

        if state.cursor.is_none() {
            break;
        }
    }

    status_line.finish_and_clear();
    pb.finish_and_clear();
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn pull_traces_mode(
    client: &ApiClient,
    ctx: &LoginContext,
    source_expr: &str,
    spec: &SyncSpec,
    output_dir: &Path,
    state_path: &Path,
    state: &mut PullState,
    workers: usize,
    btql_retry_tracker: &Arc<BtqlRetryTracker>,
    show_checkpoint_hint: bool,
) -> Result<()> {
    if trace_state_needs_discovery(state) {
        state.phase = PullPhase::DiscoverRoots;
    }

    let fetch_phase_started_at = epoch_seconds();
    if state.trace_chunks.is_empty() && !state.root_ids.is_empty() {
        initialize_trace_chunks(state, ROOT_FETCH_CHUNK_SIZE);
    }
    append_trace_chunks(state, ROOT_FETCH_CHUNK_SIZE, false);
    state.updated_at = epoch_seconds();
    write_json_atomic(state_path, state)?;
    let trace_progress_roots = Arc::new(tokio::sync::Mutex::new(trace_progress_roots_from_state(
        state,
    )));
    let fetch_baseline_trace_progress = trace_progress_roots.lock().await.len();
    let fetch_baseline_items_done = state.items_done;
    let fetch_baseline_bytes_written = state.bytes_written;
    let ui = bounded_bar_with_status_line(state.limit as u64, "Syncing traces", "traces");
    let trace_fetch_bar = ui.main.clone();
    let status_line = ui.status_line.clone();
    trace_fetch_bar.set_position(fetch_baseline_trace_progress.min(state.limit) as u64);
    trace_fetch_bar.set_message(pull_trace_progress_message_with_baseline(
        state,
        fetch_phase_started_at,
        fetch_baseline_trace_progress,
        fetch_baseline_trace_progress,
        fetch_baseline_items_done,
        fetch_baseline_bytes_written,
    ));
    status_line.set_message(pull_status_line(
        show_checkpoint_hint,
        btql_retry_tracker.summary_line().as_deref(),
    ));

    let shared_state = Arc::new(tokio::sync::Mutex::new(state.clone()));
    let shared_writer = Arc::new(tokio::sync::Mutex::new(open_jsonl_part_writer(
        output_dir,
        state.items_done > 0,
    )?));
    let work_notify = Arc::new(tokio::sync::Notify::new());
    let active_chunks = Arc::new(tokio::sync::Mutex::new(HashSet::<usize>::new()));
    let needs_discovery = state.phase == PullPhase::DiscoverRoots;
    let discovery_done = Arc::new(AtomicBool::new(!needs_discovery));
    let worker_count = workers.max(1);
    let mut join_set = tokio::task::JoinSet::new();

    for _ in 0..worker_count {
        let client = client.clone();
        let ctx = ctx.clone();
        let source_expr = source_expr.to_string();
        let state_path = state_path.to_path_buf();
        let shared_state = Arc::clone(&shared_state);
        let shared_writer = Arc::clone(&shared_writer);
        let work_notify = Arc::clone(&work_notify);
        let active_chunks = Arc::clone(&active_chunks);
        let discovery_done = Arc::clone(&discovery_done);
        let progress = trace_fetch_bar.clone();
        let status_line = status_line.clone();
        let page_size = spec.page_size;
        let fetch_filter = spec.filter.clone();
        let btql_retry_tracker = Arc::clone(btql_retry_tracker);
        let trace_progress_roots = Arc::clone(&trace_progress_roots);

        join_set.spawn(async move {
            loop {
                let Some(chunk_idx) = claim_next_trace_chunk(
                    &shared_state,
                    &active_chunks,
                    &discovery_done,
                    &work_notify,
                )
                .await?
                else {
                    break;
                };

                let result = process_trace_chunk(
                    &client,
                    &ctx,
                    &source_expr,
                    fetch_filter.as_deref(),
                    page_size,
                    &state_path,
                    chunk_idx,
                    &shared_state,
                    &shared_writer,
                    &progress,
                    &status_line,
                    show_checkpoint_hint,
                    fetch_phase_started_at,
                    fetch_baseline_trace_progress,
                    fetch_baseline_items_done,
                    fetch_baseline_bytes_written,
                    &trace_progress_roots,
                    &btql_retry_tracker,
                )
                .await;

                {
                    let mut active = active_chunks.lock().await;
                    active.remove(&chunk_idx);
                }
                work_notify.notify_waiters();
                result?;
            }
            Result::<()>::Ok(())
        });
    }

    if needs_discovery {
        let mut seen = state.root_ids.iter().cloned().collect::<HashSet<String>>();
        let mut discovery_cursor = state.root_discovery_cursor.clone();

        while seen.len() < state.limit {
            let page_limit = ROOT_DISCOVERY_PAGE_SIZE;
            let query = build_root_discovery_query(
                source_expr,
                spec.filter.as_deref(),
                page_limit,
                discovery_cursor.as_deref(),
            );
            let response =
                execute_btql_query(client, ctx, &query, Some(Arc::clone(btql_retry_tracker)))
                    .await?;
            let row_count = response.data.len();

            let new_chunks;
            {
                let mut shared = shared_state.lock().await;
                for row in response.data {
                    if let Some(root_id) = row_root_span_id(&row) {
                        if !root_id.is_empty()
                            && seen.len() < shared.limit
                            && seen.insert(root_id.clone())
                        {
                            shared.root_ids.push(root_id);
                        }
                    }
                }

                shared.pages_done += 1;
                discovery_cursor = response.cursor.filter(|c| !c.is_empty());
                shared.root_discovery_cursor = discovery_cursor.clone();
                new_chunks = append_trace_chunks(&mut shared, ROOT_FETCH_CHUNK_SIZE, false);
                shared.updated_at = epoch_seconds();
                write_json_atomic(state_path, &*shared)?;
                let trace_progress_done = trace_progress_roots.lock().await.len();
                trace_fetch_bar.set_position(trace_progress_done.min(shared.limit) as u64);
                trace_fetch_bar.set_message(pull_trace_progress_message_with_baseline(
                    &shared,
                    fetch_phase_started_at,
                    trace_progress_done,
                    fetch_baseline_trace_progress,
                    fetch_baseline_items_done,
                    fetch_baseline_bytes_written,
                ));
                status_line.set_message(pull_status_line(
                    show_checkpoint_hint,
                    btql_retry_tracker.summary_line().as_deref(),
                ));
            }
            if new_chunks > 0 {
                work_notify.notify_waiters();
            }

            if row_count == 0 || discovery_cursor.is_none() {
                break;
            }
        }

        {
            let mut shared = shared_state.lock().await;
            shared.phase = PullPhase::FetchRoots;
            shared.root_discovery_cursor = None;
            let new_chunks = append_trace_chunks(&mut shared, ROOT_FETCH_CHUNK_SIZE, true);
            shared.updated_at = epoch_seconds();
            write_json_atomic(state_path, &*shared)?;
            let trace_progress_done = trace_progress_roots.lock().await.len();
            trace_fetch_bar.set_position(trace_progress_done.min(shared.limit) as u64);
            trace_fetch_bar.set_message(pull_trace_progress_message_with_baseline(
                &shared,
                fetch_phase_started_at,
                trace_progress_done,
                fetch_baseline_trace_progress,
                fetch_baseline_items_done,
                fetch_baseline_bytes_written,
            ));
            status_line.set_message(pull_status_line(
                show_checkpoint_hint,
                btql_retry_tracker.summary_line().as_deref(),
            ));
            if new_chunks > 0 {
                work_notify.notify_waiters();
            }
        }
        discovery_done.store(true, Ordering::SeqCst);
        work_notify.notify_waiters();
    } else {
        discovery_done.store(true, Ordering::SeqCst);
        work_notify.notify_waiters();
    }

    while let Some(join_result) = join_set.join_next().await {
        let worker_result = join_result.context("trace fetch worker join failed")?;
        worker_result?;
    }

    {
        let mut writer = shared_writer.lock().await;
        writer.flush().context("failed to flush JSONL output")?;
    }
    *state = shared_state.lock().await.clone();

    status_line.finish_and_clear();
    trace_fetch_bar.finish_and_clear();
    Ok(())
}

fn trace_state_needs_discovery(state: &PullState) -> bool {
    let no_trace_work = state.root_ids.is_empty() && state.trace_chunks.is_empty();
    (state.phase == PullPhase::DiscoverRoots
        || state.phase == PullPhase::Spans
        || state.phase == PullPhase::FetchRoots
        || state.phase == PullPhase::Completed)
        && state.items_done == 0
        && state.current_root_index == 0
        && no_trace_work
}

fn initialize_trace_chunks(state: &mut PullState, chunk_size: usize) {
    if !state.trace_chunks.is_empty() {
        return;
    }

    let root_count = state.root_ids.len();
    if root_count == 0 {
        state.current_root_index = 0;
        state.current_root_cursor = None;
        return;
    }

    let legacy_index = state.current_root_index.min(root_count);
    let legacy_cursor = state.current_root_cursor.clone();
    let mut chunk_index = 0usize;
    let mut chunks = Vec::new();

    let mut start = 0usize;
    while start < root_count {
        let end = (start + chunk_size).min(root_count);
        let mut chunk = TraceChunkState {
            chunk_index,
            start,
            end,
            cursor: None,
            completed: end <= legacy_index,
        };

        if start == legacy_index {
            chunk.cursor = legacy_cursor.clone();
        }

        chunks.push(chunk);
        chunk_index += 1;
        start = end;
    }

    state.trace_chunks = chunks;
    state.current_root_index = completed_root_count(state);
    state.current_root_cursor = None;
}

fn append_trace_chunks(state: &mut PullState, chunk_size: usize, flush_partial: bool) -> usize {
    let mut next_start = state
        .trace_chunks
        .last()
        .map(|chunk| chunk.end)
        .unwrap_or(0);
    let root_count = state.root_ids.len();
    let mut added = 0usize;

    while next_start < root_count {
        let remaining = root_count - next_start;
        if !flush_partial && remaining < chunk_size {
            break;
        }
        let end = (next_start + chunk_size).min(root_count);
        state.trace_chunks.push(TraceChunkState {
            chunk_index: state.trace_chunks.len(),
            start: next_start,
            end,
            cursor: None,
            completed: false,
        });
        next_start = end;
        added += 1;
    }

    state.current_root_index = completed_root_count(state);
    state.current_root_cursor = None;
    added
}

async fn claim_next_trace_chunk(
    shared_state: &Arc<tokio::sync::Mutex<PullState>>,
    active_chunks: &Arc<tokio::sync::Mutex<HashSet<usize>>>,
    discovery_done: &Arc<AtomicBool>,
    work_notify: &Arc<tokio::sync::Notify>,
) -> Result<Option<usize>> {
    loop {
        {
            let state = shared_state.lock().await;
            let mut active = active_chunks.lock().await;
            if let Some((chunk_idx, _)) = state
                .trace_chunks
                .iter()
                .enumerate()
                .find(|(idx, chunk)| !chunk.completed && !active.contains(idx))
            {
                active.insert(chunk_idx);
                return Ok(Some(chunk_idx));
            }
            if discovery_done.load(Ordering::SeqCst) {
                return Ok(None);
            }
        }
        work_notify.notified().await;
    }
}

fn completed_root_count(state: &PullState) -> usize {
    state
        .trace_chunks
        .iter()
        .filter(|chunk| chunk.completed)
        .map(|chunk| chunk.end.saturating_sub(chunk.start))
        .sum()
}

fn trace_progress_roots_from_state(state: &PullState) -> HashSet<String> {
    let mut seen = HashSet::new();
    for chunk in state.trace_chunks.iter().filter(|chunk| chunk.completed) {
        let start = chunk.start.min(state.root_ids.len());
        let end = chunk.end.min(state.root_ids.len());
        for root_id in &state.root_ids[start..end] {
            seen.insert(root_id.clone());
        }
    }
    seen
}

#[allow(clippy::too_many_arguments)]
async fn process_trace_chunk(
    client: &ApiClient,
    ctx: &LoginContext,
    source_expr: &str,
    fetch_filter: Option<&str>,
    page_size: usize,
    state_path: &Path,
    chunk_idx: usize,
    shared_state: &Arc<tokio::sync::Mutex<PullState>>,
    shared_writer: &Arc<tokio::sync::Mutex<JsonlPartWriter>>,
    progress: &ProgressBar,
    status_line: &ProgressBar,
    show_checkpoint_hint: bool,
    fetch_phase_started_at: u64,
    fetch_baseline_trace_progress: usize,
    fetch_baseline_items_done: usize,
    fetch_baseline_bytes_written: u64,
    trace_progress_roots: &Arc<tokio::sync::Mutex<HashSet<String>>>,
    btql_retry_tracker: &Arc<BtqlRetryTracker>,
) -> Result<()> {
    loop {
        let (root_chunk, cursor, already_completed) = {
            let state = shared_state.lock().await;
            let chunk = state
                .trace_chunks
                .get(chunk_idx)
                .ok_or_else(|| anyhow!("invalid trace chunk index {chunk_idx}"))?;
            let root_chunk = state.root_ids[chunk.start..chunk.end].to_vec();
            (root_chunk, chunk.cursor.clone(), chunk.completed)
        };

        if already_completed || root_chunk.is_empty() {
            return Ok(());
        }

        let query = build_root_spans_query(
            source_expr,
            &root_chunk,
            fetch_filter,
            page_size,
            cursor.as_deref(),
        );
        let response =
            execute_btql_query(client, ctx, &query, Some(Arc::clone(btql_retry_tracker))).await?;
        let batch_count = response.data.len();
        let next_cursor = response.cursor.filter(|c| !c.is_empty());
        let mut seen_roots_in_batch = HashSet::new();
        for row in &response.data {
            if let Some(root_id) = row_root_span_id(row) {
                if !root_id.is_empty() {
                    seen_roots_in_batch.insert(root_id);
                }
            }
        }
        let mut mark_chunk_roots_done = false;

        if batch_count > 0 {
            let mut bytes_written = 0u64;
            let serialized = response
                .data
                .iter()
                .map(|row| {
                    let line =
                        serde_json::to_string(row).context("failed to serialize trace row")?;
                    bytes_written += (line.len() + 1) as u64;
                    Result::<String>::Ok(line)
                })
                .collect::<Result<Vec<String>>>()?;

            {
                let mut writer = shared_writer.lock().await;
                for line in &serialized {
                    writer
                        .write_line(line)
                        .context("failed to write JSONL row")?;
                }
                writer.flush().context("failed to flush JSONL output")?;
            }

            {
                let mut state = shared_state.lock().await;
                state.items_done += batch_count;
                state.pages_done += 1;
                state.bytes_written += bytes_written;
                state.trace_chunks[chunk_idx].cursor = next_cursor.clone();
                if next_cursor.is_none() {
                    state.trace_chunks[chunk_idx].completed = true;
                    mark_chunk_roots_done = true;
                }
                state.current_root_index = completed_root_count(&state);
                state.current_root_cursor = None;
                state.updated_at = epoch_seconds();
                write_json_atomic(state_path, &*state)?;
                drop(state);
                if !seen_roots_in_batch.is_empty() {
                    let mut seen_roots = trace_progress_roots.lock().await;
                    for root_id in seen_roots_in_batch {
                        seen_roots.insert(root_id);
                    }
                }
                if mark_chunk_roots_done {
                    let mut seen_roots = trace_progress_roots.lock().await;
                    for root_id in &root_chunk {
                        seen_roots.insert(root_id.clone());
                    }
                }
                let trace_progress_done = trace_progress_roots.lock().await.len();
                let state = shared_state.lock().await;
                progress.set_position(trace_progress_done.min(state.limit) as u64);
                progress.set_message(pull_trace_progress_message_with_baseline(
                    &state,
                    fetch_phase_started_at,
                    trace_progress_done,
                    fetch_baseline_trace_progress,
                    fetch_baseline_items_done,
                    fetch_baseline_bytes_written,
                ));
                status_line.set_message(pull_status_line(
                    show_checkpoint_hint,
                    btql_retry_tracker.summary_line().as_deref(),
                ));
            }
        } else {
            let mut state = shared_state.lock().await;
            state.trace_chunks[chunk_idx].cursor = None;
            state.trace_chunks[chunk_idx].completed = true;
            state.current_root_index = completed_root_count(&state);
            state.current_root_cursor = None;
            state.updated_at = epoch_seconds();
            write_json_atomic(state_path, &*state)?;
            drop(state);
            {
                let mut seen_roots = trace_progress_roots.lock().await;
                for root_id in &root_chunk {
                    seen_roots.insert(root_id.clone());
                }
            }
            let trace_progress_done = trace_progress_roots.lock().await.len();
            let state = shared_state.lock().await;
            progress.set_position(trace_progress_done.min(state.limit) as u64);
            progress.set_message(pull_trace_progress_message_with_baseline(
                &state,
                fetch_phase_started_at,
                trace_progress_done,
                fetch_baseline_trace_progress,
                fetch_baseline_items_done,
                fetch_baseline_bytes_written,
            ));
            status_line.set_message(pull_status_line(
                show_checkpoint_hint,
                btql_retry_tracker.summary_line().as_deref(),
            ));
        }

        if next_cursor.is_none() {
            return Ok(());
        }
    }
}

async fn run_push(json_output: bool, ctx: &LoginContext, args: PushArgs) -> Result<()> {
    let object = parse_object_ref(&args.object_ref)?;
    if object.object_type != "project_logs" {
        bail!(
            "push currently supports only project_logs:<project_id>; got {}:{}",
            object.object_type,
            object.object_name
        );
    }
    let (scope, limit) = resolve_push_scope_and_limit(args.traces, args.spans)?;

    let spec = SyncSpec {
        schema_version: STATE_SCHEMA_VERSION,
        object_ref: args.object_ref.clone(),
        object_type: object.object_type.clone(),
        object_name: object.object_name.clone(),
        direction: DirectionArg::Push.as_str().to_string(),
        scope: scope.as_str().to_string(),
        filter: trim_optional(args.filter.clone()),
        limit,
        page_size: args.page_size,
    };
    let spec_hash = spec_hash(&spec)?;
    let spec_dir = resolve_spec_dir(
        &args.root,
        &object,
        DirectionArg::Push,
        &scope,
        &spec_hash,
        !args.fresh,
    )?;
    fs::create_dir_all(&spec_dir)
        .with_context(|| format!("failed to create {}", spec_dir.display()))?;

    let spec_path = spec_dir.join("spec.json");
    let state_path = spec_dir.join("state.json");
    let manifest_path = spec_dir.join("manifest.json");
    write_json_atomic(&spec_path, &spec)?;

    let input_path = if let Some(path) = args.input {
        path
    } else {
        resolve_default_push_input(&args.root, &object)?
    };
    if !input_path.exists() {
        bail!("input path does not exist: {}", input_path.display());
    }

    let mut state = if args.fresh || !state_path.exists() {
        new_push_state(
            scope.as_str().to_string(),
            limit,
            args.page_size,
            input_path.to_string_lossy().to_string(),
        )
    } else {
        read_json_file::<PushState>(&state_path)?
    };

    if state.status == RunStatus::Completed && !args.fresh {
        if json_output {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "status": "completed",
                    "message": "already completed for this spec",
                    "source_path": state.source_path,
                    "items_done": state.items_done,
                    "pages_done": state.pages_done
                }))?
            );
        } else {
            println!(
                "Sync already completed for this spec. input={} items={} pages={}",
                state.source_path, state.items_done, state.pages_done
            );
        }
        return Ok(());
    }

    state.status = RunStatus::Running;
    state.updated_at = epoch_seconds();
    write_json_atomic(&state_path, &state)?;
    update_manifest_from_push_state(
        &manifest_path,
        &spec_hash,
        &spec,
        &state,
        Some(RunStatus::Running),
    )?;

    let project_id = object.object_name.clone();
    let input_files = resolve_push_input_files(&input_path)?;
    let upload_total = upload_total_for_progress(&input_files, &scope, limit)?;

    let pb = if let Some(total) = upload_total {
        bounded_bar(total as u64, "Uploading rows", "spans")
    } else {
        spinner_bar("Uploading rows")
    };
    pb.set_prefix("Uploading rows".to_string());
    if let Some(total) = upload_total {
        pb.set_position(state.items_done.min(total) as u64);
    }

    let interrupted = Arc::new(AtomicBool::new(false));
    let interrupted_signal = Arc::clone(&interrupted);
    let ctrlc_task = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            interrupted_signal.store(true, Ordering::SeqCst);
        }
    });

    let uploader_template = Logs3BatchUploader::new(
        ctx.api_url.clone(),
        ctx.login.api_key.clone(),
        (!ctx.login.org_name.trim().is_empty()).then_some(ctx.login.org_name.clone()),
    )
    .context("failed to initialize logs3 uploader")?;

    let mut batch: Vec<Map<String, Value>> = Vec::with_capacity(args.page_size);
    let mut seen_roots: HashSet<String> = if state.line_offset > 0 {
        collect_seen_roots_until_offset(&input_files, state.line_offset)?
    } else {
        HashSet::new()
    };
    if state.distinct_roots_done < seen_roots.len() {
        state.distinct_roots_done = seen_roots.len();
    }
    let push_phase_started_at = epoch_seconds();
    let push_baseline_roots_done = state.distinct_roots_done;
    let push_baseline_items_done = state.items_done;
    let push_baseline_bytes_sent = state.bytes_sent;
    pb.set_message(push_progress_message_with_baseline(
        &state,
        push_phase_started_at,
        push_baseline_roots_done,
        push_baseline_items_done,
        push_baseline_bytes_sent,
        upload_total,
    ));
    if !json_output {
        show_checkpoint_hint_line(&pb);
    }

    let worker_count = args.workers.max(1);
    let mut selected_count: usize = state.items_done;
    let mut batch_end_line_offset = state.line_offset;
    let mut batch_distinct_roots_done = state.distinct_roots_done;
    let mut next_batch_index = 0usize;
    let mut next_commit_index = 0usize;
    let mut pending_results: BTreeMap<usize, PushBatchResult> = BTreeMap::new();
    let mut join_set = tokio::task::JoinSet::new();

    let mut global_line_index = 0usize;
    'files: for file_path in &input_files {
        let file = File::open(file_path)
            .with_context(|| format!("failed to open input {}", file_path.display()))?;
        let reader = BufReader::new(file);

        for (line_index, line) in reader.lines().enumerate() {
            if interrupted.load(Ordering::SeqCst) {
                break 'files;
            }
            let line = line.with_context(|| format!("failed reading {}", file_path.display()))?;
            let current_line_offset = global_line_index;
            global_line_index += 1;
            if current_line_offset < state.line_offset {
                continue;
            }

            if line.trim().is_empty() {
                continue;
            }
            let mut row: Map<String, Value> = serde_json::from_str(&line).with_context(|| {
                format!(
                    "invalid JSON in {} at line {}",
                    file_path.display(),
                    line_index + 1
                )
            })?;

            if let Some(max_spans) = limit.filter(|_| matches!(scope, ScopeArg::Spans)) {
                if selected_count >= max_spans {
                    break 'files;
                }
            }

            let root_id = row_root_span_id(&row).unwrap_or_else(|| "__missing__".to_string());
            if let Some(max_traces) = limit.filter(|_| matches!(scope, ScopeArg::Traces)) {
                let is_new_root = !seen_roots.contains(&root_id);
                if is_new_root && seen_roots.len() >= max_traces {
                    break 'files;
                }
            }
            if !root_id.is_empty() {
                seen_roots.insert(root_id);
            }

            prepare_row_for_upload(&mut row, &project_id, state.run_id.as_str(), selected_count);

            selected_count += 1;
            batch_end_line_offset = current_line_offset + 1;
            batch_distinct_roots_done = seen_roots.len();
            batch.push(row);

            if batch.len() >= args.page_size {
                let work = PushBatchWork {
                    batch_index: next_batch_index,
                    rows: std::mem::take(&mut batch),
                    end_line_offset: batch_end_line_offset,
                    distinct_roots_done: batch_distinct_roots_done,
                };
                next_batch_index += 1;
                spawn_push_upload_task(
                    &mut join_set,
                    uploader_template.clone(),
                    work,
                    args.page_size,
                );
                batch_end_line_offset = state.line_offset;
                batch_distinct_roots_done = state.distinct_roots_done;

                while join_set.len() >= worker_count {
                    let result = join_set
                        .join_next()
                        .await
                        .ok_or_else(|| anyhow!("push upload worker queue unexpectedly empty"))?
                        .context("push upload worker join failed")??;
                    pending_results.insert(result.batch_index, result);
                    flush_ready_push_results(
                        &mut pending_results,
                        &mut next_commit_index,
                        &mut state,
                        &state_path,
                        &pb,
                        upload_total,
                        push_phase_started_at,
                        push_baseline_roots_done,
                        push_baseline_items_done,
                        push_baseline_bytes_sent,
                    )?;
                }
            }
        }
    }

    if !batch.is_empty() && !interrupted.load(Ordering::SeqCst) {
        let work = PushBatchWork {
            batch_index: next_batch_index,
            rows: std::mem::take(&mut batch),
            end_line_offset: batch_end_line_offset,
            distinct_roots_done: batch_distinct_roots_done,
        };
        next_batch_index += 1;
        spawn_push_upload_task(
            &mut join_set,
            uploader_template.clone(),
            work,
            args.page_size,
        );
    }

    while let Some(joined) = join_set.join_next().await {
        let result = joined.context("push upload worker join failed")??;
        pending_results.insert(result.batch_index, result);
        flush_ready_push_results(
            &mut pending_results,
            &mut next_commit_index,
            &mut state,
            &state_path,
            &pb,
            upload_total,
            push_phase_started_at,
            push_baseline_roots_done,
            push_baseline_items_done,
            push_baseline_bytes_sent,
        )?;
    }

    if !pending_results.is_empty() || next_commit_index != next_batch_index {
        bail!(
            "push checkpoint mismatch: committed {next_commit_index} of {next_batch_index} batch(es)"
        );
    }

    let was_interrupted = interrupted.load(Ordering::SeqCst);
    ctrlc_task.abort();

    if was_interrupted {
        state.status = RunStatus::Interrupted;
        state.updated_at = epoch_seconds();
        write_json_atomic(&state_path, &state)?;
        update_manifest_from_push_state(
            &manifest_path,
            &spec_hash,
            &spec,
            &state,
            Some(RunStatus::Interrupted),
        )?;
        pb.finish_and_clear();

        if json_output {
            println!(
                "{}",
                serde_json::to_string_pretty(&json!({
                    "status": "interrupted",
                    "spec_dir": spec_dir,
                    "input_path": input_path,
                    "rows_uploaded": state.items_done,
                    "pages_done": state.pages_done,
                    "bytes_sent": state.bytes_sent,
                    "message": "resume by rerunning the same command; use --fresh to restart"
                }))?
            );
        } else {
            println!("Push interrupted");
            println!(
                "  Uploaded so far: {} rows, {} batches, {} bytes",
                format_usize_commas(state.items_done),
                format_usize_commas(state.pages_done),
                format_u64_commas(state.bytes_sent)
            );
            println!("  Resume: rerun the same command (use --fresh to restart)");
        }
        return Ok(());
    }

    state.status = RunStatus::Completed;
    state.completed_at = Some(epoch_seconds());
    state.updated_at = epoch_seconds();
    write_json_atomic(&state_path, &state)?;
    update_manifest_from_push_state(
        &manifest_path,
        &spec_hash,
        &spec,
        &state,
        Some(RunStatus::Completed),
    )?;
    pb.finish_and_clear();

    if json_output {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "status": "completed",
                "spec_dir": spec_dir,
                "input_path": input_path,
                "rows_uploaded": state.items_done,
                "pages_done": state.pages_done,
                "bytes_sent": state.bytes_sent
            }))?
        );
    } else {
        let elapsed_secs = epoch_seconds().saturating_sub(state.started_at).max(1);
        let traces_done = state.distinct_roots_done;
        let spans_done = state.items_done;
        let traces_per_sec = traces_done as f64 / elapsed_secs as f64;
        let spans_per_sec = spans_done as f64 / elapsed_secs as f64;
        let bytes_per_sec = state.bytes_sent as f64 / elapsed_secs as f64;
        println!("Push complete");
        println!("  Input: {}", input_path.display());
        println!("  Time: {}", format_duration(elapsed_secs));
        println!("  Traces: {}", format_usize_commas(traces_done));
        println!("  Spans: {}", format_usize_commas(spans_done));
        println!("  Batches: {}", format_usize_commas(state.pages_done));
        println!(
            "  Data: {} ({} bytes)",
            format_bytes(state.bytes_sent as f64),
            format_u64_commas(state.bytes_sent)
        );
        println!(
            "  Rates: {:.2} traces/s | {:.2} spans/s | {}/s",
            traces_per_sec,
            spans_per_sec,
            format_bytes(bytes_per_sec)
        );
    }
    Ok(())
}

fn run_status(json_output: bool, args: StatusArgs) -> Result<()> {
    let object = parse_object_ref(&args.object_ref)?;
    let (scope, limit) = resolve_status_scope_and_limit(args.traces, args.spans)?;

    let spec = SyncSpec {
        schema_version: STATE_SCHEMA_VERSION,
        object_ref: args.object_ref.clone(),
        object_type: object.object_type.clone(),
        object_name: object.object_name.clone(),
        direction: args.direction.as_str().to_string(),
        scope: scope.as_str().to_string(),
        filter: trim_optional(args.filter.clone()),
        limit,
        page_size: args.page_size,
    };
    let spec_hash = spec_hash(&spec)?;
    let spec_dir = resolve_spec_dir(
        &args.root,
        &object,
        args.direction,
        &scope,
        &spec_hash,
        true,
    )?;
    let state_path = spec_dir.join("state.json");
    let manifest_path = spec_dir.join("manifest.json");
    let spec_path = spec_dir.join("spec.json");

    if !spec_dir.exists() {
        bail!("no sync state found for spec at {}", spec_dir.display());
    }

    let mut output = BTreeMap::<String, Value>::new();
    output.insert(
        "spec_dir".to_string(),
        Value::String(spec_dir.display().to_string()),
    );
    output.insert("spec_hash".to_string(), Value::String(spec_hash));
    output.insert(
        "direction".to_string(),
        Value::String(args.direction.as_str().to_string()),
    );

    if spec_path.exists() {
        let spec_value: Value = read_json_file(&spec_path)?;
        output.insert("spec".to_string(), spec_value);
    }
    if state_path.exists() {
        let state_value: Value = read_json_file(&state_path)?;
        output.insert("state".to_string(), state_value);
    }
    if manifest_path.exists() {
        let manifest_value: Value = read_json_file(&manifest_path)?;
        output.insert("manifest".to_string(), manifest_value);
    }

    if json_output {
        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        println!("Spec: {}", spec_dir.display());
        if let Some(state) = output.get("state") {
            println!("State: {}", serde_json::to_string_pretty(state)?);
        } else {
            println!("State: (missing)");
        }
        if let Some(manifest) = output.get("manifest") {
            println!("Manifest: {}", serde_json::to_string_pretty(manifest)?);
        } else {
            println!("Manifest: (missing)");
        }
    }
    Ok(())
}

async fn execute_btql_query(
    client: &ApiClient,
    ctx: &LoginContext,
    query: &str,
    btql_retry_tracker: Option<Arc<BtqlRetryTracker>>,
) -> Result<BtqlResponse> {
    let body = json!({
        "query": query,
        "fmt": "json",
    });
    let url = client.url("/btql");
    let http = Client::new();
    let api_key = ctx.login.api_key.clone();
    let org_name = ctx.login.org_name.clone();
    let attempt_counter = Arc::new(AtomicUsize::new(0));

    let backoff = ExponentialBackoffBuilder::new()
        .with_initial_interval(Duration::from_millis(BTQL_RETRY_BASE_DELAY_MS))
        .with_multiplier(2.0)
        .with_randomization_factor(0.2)
        .with_max_interval(Duration::from_secs(BTQL_MAX_BACKOFF_SECS))
        .with_max_elapsed_time(None)
        .build();

    let result = retry_notify(
        backoff,
        || {
            let http = http.clone();
            let url = url.clone();
            let body = body.clone();
            let api_key = api_key.clone();
            let org_name = org_name.clone();
            let attempt_counter = Arc::clone(&attempt_counter);
            let btql_retry_tracker = btql_retry_tracker.clone();

            async move {
                let attempt = attempt_counter.fetch_add(1, Ordering::Relaxed) + 1;
                let mut request = http
                    .post(&url)
                    .bearer_auth(&api_key)
                    .header("content-type", "application/json")
                    .json(&body);
                if !org_name.is_empty() {
                    request = request.header("x-bt-org-name", org_name.clone());
                }

                match request.send().await {
                    Ok(response) => {
                        let status = response.status();
                        if status.is_success() {
                            return response.json::<BtqlResponse>().await.map_err(|err| {
                                BackoffError::permanent(anyhow!("failed to parse BTQL response: {err}"))
                            });
                        }

                        let body = response.text().await.unwrap_or_default();
                        let should_retry = status.is_server_error() || status.as_u16() == 413;
                        if should_retry && attempt < BTQL_MAX_ATTEMPTS {
                            if let Some(tracker) = btql_retry_tracker.as_ref() {
                                tracker.record_status(status.as_u16());
                            }
                            Err(BackoffError::transient(anyhow!(
                                "BTQL retryable failure ({status}) on attempt {attempt}/{BTQL_MAX_ATTEMPTS}: {body}"
                            )))
                        } else {
                            Err(BackoffError::permanent(anyhow!(
                                "BTQL request failed ({status}) after {attempt} attempt(s): {body}"
                            )))
                        }
                    }
                    Err(err) => {
                        if attempt < BTQL_MAX_ATTEMPTS {
                            if let Some(tracker) = btql_retry_tracker.as_ref() {
                                tracker.record_network();
                            }
                            Err(BackoffError::transient(anyhow!(
                                "BTQL network error on attempt {attempt}/{BTQL_MAX_ATTEMPTS}: {err}"
                            )))
                        } else {
                            Err(BackoffError::permanent(anyhow!(
                                "BTQL request failed after {attempt} attempt(s): {err}"
                            )))
                        }
                    }
                }
            }
        },
        |_, _| {},
    )
    .await;

    result.map_err(|err| {
        let attempts = attempt_counter.load(Ordering::Relaxed).max(1);
        anyhow!(err).context(format!("BTQL request failed after {attempts} attempt(s)"))
    })
}

fn build_spans_query(
    source_expr: &str,
    filter: Option<&str>,
    page_size: usize,
    cursor: Option<&str>,
) -> String {
    let mut parts = vec![
        "select: *".to_string(),
        format!("from: {source_expr} spans"),
        format!("limit: {}", page_size),
        "sort: _pagination_key DESC".to_string(),
    ];
    if let Some(filter_expr) = filter.map(str::trim).filter(|s| !s.is_empty()) {
        parts.push(format!("filter: {filter_expr}"));
    }
    if let Some(c) = cursor {
        parts.push(format!("cursor: {}", btql_quote(c)));
    }
    parts.join(" | ")
}

fn build_root_discovery_query(
    source_expr: &str,
    filter: Option<&str>,
    page_size: usize,
    cursor: Option<&str>,
) -> String {
    let mut parts = vec![
        "select: root_span_id, span_id, id".to_string(),
        format!("from: {source_expr} spans"),
        format!("limit: {}", page_size),
        "sort: _pagination_key DESC".to_string(),
    ];
    if let Some(filter_expr) = filter.map(str::trim).filter(|s| !s.is_empty()) {
        parts.push(format!("filter: {filter_expr}"));
    }
    if let Some(c) = cursor {
        parts.push(format!("cursor: {}", btql_quote(c)));
    }
    parts.join(" | ")
}

fn build_root_spans_query(
    source_expr: &str,
    root_span_ids: &[String],
    filter: Option<&str>,
    page_size: usize,
    cursor: Option<&str>,
) -> String {
    let root_filter = if root_span_ids.len() == 1 {
        let quoted = sql_quote(&root_span_ids[0]);
        format!("(root_span_id = {quoted} OR span_id = {quoted} OR id = {quoted})")
    } else {
        let joined = root_span_ids
            .iter()
            .map(|id| sql_quote(id))
            .collect::<Vec<_>>()
            .join(", ");
        format!("(root_span_id IN [{joined}] OR span_id IN [{joined}] OR id IN [{joined}])")
    };

    let combined_filter = if let Some(filter_expr) = filter.map(str::trim).filter(|s| !s.is_empty())
    {
        format!("({root_filter}) AND ({filter_expr})")
    } else {
        root_filter
    };

    let mut parts = vec![
        "select: *".to_string(),
        format!("from: {source_expr} spans"),
        format!("filter: {combined_filter}"),
        format!("limit: {}", page_size),
        "sort: _pagination_key ASC".to_string(),
    ];
    if let Some(c) = cursor {
        parts.push(format!("cursor: {}", btql_quote(c)));
    }
    parts.join(" | ")
}

async fn submit_logs_batch(
    uploader: &mut Logs3BatchUploader,
    rows: &[Map<String, Value>],
    page_size: usize,
) -> Result<usize> {
    let result = uploader
        .upload_rows(rows, page_size)
        .await
        .context("logs3 upload failed")?;
    Ok(result.bytes_processed)
}

fn spawn_push_upload_task(
    join_set: &mut tokio::task::JoinSet<Result<PushBatchResult>>,
    mut uploader: Logs3BatchUploader,
    work: PushBatchWork,
    page_size: usize,
) {
    join_set.spawn(async move {
        let bytes = submit_logs_batch(&mut uploader, &work.rows, page_size).await?;
        Ok(PushBatchResult {
            batch_index: work.batch_index,
            row_count: work.rows.len(),
            bytes_sent: bytes as u64,
            end_line_offset: work.end_line_offset,
            distinct_roots_done: work.distinct_roots_done,
        })
    });
}

#[allow(clippy::too_many_arguments)]
fn flush_ready_push_results(
    pending_results: &mut BTreeMap<usize, PushBatchResult>,
    next_commit_index: &mut usize,
    state: &mut PushState,
    state_path: &Path,
    pb: &ProgressBar,
    upload_total: Option<usize>,
    push_phase_started_at: u64,
    push_baseline_roots_done: usize,
    push_baseline_items_done: usize,
    push_baseline_bytes_sent: u64,
) -> Result<()> {
    while let Some(result) = pending_results.remove(next_commit_index) {
        commit_push_batch_state(
            state,
            result.row_count,
            result.bytes_sent,
            result.end_line_offset,
            result.distinct_roots_done,
        );
        write_json_atomic(state_path, state)?;
        if let Some(total) = upload_total {
            pb.set_position(state.items_done.min(total) as u64);
        }
        pb.set_message(push_progress_message_with_baseline(
            state,
            push_phase_started_at,
            push_baseline_roots_done,
            push_baseline_items_done,
            push_baseline_bytes_sent,
            upload_total,
        ));
        *next_commit_index += 1;
    }
    Ok(())
}

fn prepare_row_for_upload(
    row: &mut Map<String, Value>,
    project_id: &str,
    run_id: &str,
    row_index: usize,
) {
    row.remove("_xact_id");
    row.remove("_pagination_key");
    row.remove("_async_scoring_state");
    row.remove("org_id");
    row.remove("created");

    row.insert(
        "project_id".to_string(),
        Value::String(project_id.to_string()),
    );
    row.insert("log_id".to_string(), Value::String("g".to_string()));

    let id = row
        .get("id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            row.get("span_id")
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .unwrap_or_else(|| format!("bt-sync-{run_id}-{row_index}"));
    row.insert("id".to_string(), Value::String(id.clone()));

    let span_id = row
        .get("span_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| id.clone());
    row.insert("span_id".to_string(), Value::String(span_id.clone()));

    let root_span_id = row
        .get("root_span_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .unwrap_or_else(|| span_id.clone());
    row.insert("root_span_id".to_string(), Value::String(root_span_id));

    if !row.contains_key("span_parents") {
        row.insert("span_parents".to_string(), Value::Array(Vec::new()));
    }
}

fn commit_push_batch_state(
    state: &mut PushState,
    uploaded_rows: usize,
    bytes_sent: u64,
    batch_end_line_offset: usize,
    distinct_roots_done: usize,
) {
    state.items_done += uploaded_rows;
    state.pages_done += 1;
    state.bytes_sent += bytes_sent;
    state.line_offset = batch_end_line_offset;
    state.distinct_roots_done = distinct_roots_done;
    state.updated_at = epoch_seconds();
}

fn resolve_pull_scope_and_limit(
    traces: Option<usize>,
    spans: Option<usize>,
) -> Result<(ScopeArg, usize)> {
    match (traces, spans) {
        (Some(_), Some(_)) => bail!("--traces and --spans are mutually exclusive"),
        (Some(limit), None) => Ok((ScopeArg::Traces, nonzero(limit, "--traces")?)),
        (None, Some(limit)) => Ok((ScopeArg::Spans, nonzero(limit, "--spans")?)),
        (None, None) => Ok((ScopeArg::Traces, DEFAULT_PULL_LIMIT)),
    }
}

fn resolve_push_scope_and_limit(
    traces: Option<usize>,
    spans: Option<usize>,
) -> Result<(ScopeArg, Option<usize>)> {
    match (traces, spans) {
        (Some(_), Some(_)) => bail!("--traces and --spans are mutually exclusive"),
        (Some(limit), None) => Ok((ScopeArg::Traces, Some(nonzero(limit, "--traces")?))),
        (None, Some(limit)) => Ok((ScopeArg::Spans, Some(nonzero(limit, "--spans")?))),
        (None, None) => Ok((ScopeArg::All, None)),
    }
}

fn resolve_status_scope_and_limit(
    traces: Option<usize>,
    spans: Option<usize>,
) -> Result<(ScopeArg, Option<usize>)> {
    match (traces, spans) {
        (Some(_), Some(_)) => bail!("--traces and --spans are mutually exclusive"),
        (Some(limit), None) => Ok((ScopeArg::Traces, Some(nonzero(limit, "--traces")?))),
        (None, Some(limit)) => Ok((ScopeArg::Spans, Some(nonzero(limit, "--spans")?))),
        (None, None) => Ok((ScopeArg::All, None)),
    }
}

fn nonzero(value: usize, flag: &str) -> Result<usize> {
    if value == 0 {
        bail!("{flag} must be > 0");
    }
    Ok(value)
}

async fn resolve_pull_object_ref(client: &ApiClient, object_ref: Option<&str>) -> Result<String> {
    if let Some(value) = object_ref.map(str::trim).filter(|v| !v.is_empty()) {
        return Ok(value.to_string());
    }

    if !std::io::stdin().is_terminal() {
        bail!("OBJECT_REF is required in non-interactive mode");
    }

    let projects = list_projects(client).await?;
    if projects.is_empty() {
        bail!("no projects found for your organization");
    }

    let project_labels: Vec<String> = projects
        .iter()
        .map(|p| format!("{} ({})", p.name, p.id))
        .collect();
    let project_idx = fuzzy_select("Select project", &project_labels)?;
    let project = &projects[project_idx];

    let mut choices: Vec<InteractiveChoice> = vec![InteractiveChoice {
        label: format!("project_logs:{}  [{}]", project.id, project.name),
        object_ref: format!("project_logs:{}", project.id),
    }];

    match list_project_named_objects(client, "experiment", &project.id).await {
        Ok(mut experiments) => {
            experiments.sort_by(|a, b| a.name.cmp(&b.name));
            choices.extend(experiments.into_iter().map(|obj| InteractiveChoice {
                label: format!("experiment:{}  [{}]", obj.id, obj.name),
                object_ref: format!("experiment:{}", obj.id),
            }));
        }
        Err(err) => eprintln!(
            "warning: failed to list experiments for {}: {err}",
            project.name
        ),
    }

    match list_project_named_objects(client, "dataset", &project.id).await {
        Ok(mut datasets) => {
            datasets.sort_by(|a, b| a.name.cmp(&b.name));
            choices.extend(datasets.into_iter().map(|obj| InteractiveChoice {
                label: format!("dataset:{}  [{}]", obj.id, obj.name),
                object_ref: format!("dataset:{}", obj.id),
            }));
        }
        Err(err) => eprintln!(
            "warning: failed to list datasets for {}: {err}",
            project.name
        ),
    }

    let labels: Vec<String> = choices.iter().map(|c| c.label.clone()).collect();
    let object_idx = fuzzy_select("Select object", &labels)?;
    Ok(choices[object_idx].object_ref.clone())
}

async fn list_project_named_objects(
    client: &ApiClient,
    object_type: &str,
    project_id: &str,
) -> Result<Vec<NamedObject>> {
    let path = format!(
        "/v1/{}?org_name={}&project_id={}",
        encode(object_type),
        encode(client.org_name()),
        encode(project_id)
    );
    let response: NamedObjectListResponse = client.get(&path).await?;
    Ok(response.objects)
}

fn parse_object_ref(value: &str) -> Result<ObjectRef> {
    let parts: Vec<&str> = value.splitn(2, ':').collect();
    if parts.len() != 2 {
        bail!(
            "invalid object ref '{value}'. expected format object_type:object_id (for example: project_logs:<project_id>)"
        );
    }
    let object_type = parts[0].trim();
    let object_name = parts[1].trim();
    if !matches!(object_type, "project_logs" | "experiment" | "dataset") {
        bail!(
            "unsupported object type '{object_type}'. supported types: project_logs, experiment, dataset"
        );
    }
    if object_name.is_empty() {
        bail!("object id cannot be empty in '{value}'");
    }
    Ok(ObjectRef {
        object_type: object_type.to_string(),
        object_name: object_name.to_string(),
    })
}

fn btql_source_expr(object: &ObjectRef) -> Result<String> {
    let source = match object.object_type.as_str() {
        "project_logs" => "project_logs",
        "experiment" => "experiment",
        "dataset" => "dataset",
        other => bail!(
            "unsupported object type '{}' for pull. supported: project_logs, experiment, dataset",
            other
        ),
    };
    Ok(format!("{source}({})", sql_quote(&object.object_name)))
}

fn sanitize_segment(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        "_".to_string()
    } else {
        out
    }
}

fn spec_dir(root: &Path, object: &ObjectRef, hash: &str) -> PathBuf {
    let object_key = format!(
        "{}_{}",
        sanitize_segment(&object.object_type),
        sanitize_segment(&object.object_name)
    );
    root.join(object_key).join(&hash[..12])
}

fn legacy_spec_dir(
    root: &Path,
    object: &ObjectRef,
    direction: DirectionArg,
    scope: &ScopeArg,
    hash: &str,
) -> PathBuf {
    root.join(sanitize_segment(&object.object_type))
        .join(sanitize_segment(&object.object_name))
        .join(direction.as_str())
        .join(scope.as_str())
        .join(format!("spec_{}", &hash[..12]))
}

fn resolve_spec_dir(
    root: &Path,
    object: &ObjectRef,
    direction: DirectionArg,
    scope: &ScopeArg,
    hash: &str,
    reuse_existing: bool,
) -> Result<PathBuf> {
    let new_dir = spec_dir(root, object, hash);
    if !reuse_existing {
        return Ok(new_dir);
    }

    if new_dir.exists() {
        return Ok(new_dir);
    }

    let old_dir = legacy_spec_dir(root, object, direction, scope, hash);
    if !old_dir.exists() {
        return Ok(new_dir);
    }

    if let Some(parent) = new_dir.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    match fs::rename(&old_dir, &new_dir) {
        Ok(()) => Ok(new_dir),
        Err(_) => Ok(old_dir),
    }
}

fn spec_hash(spec: &SyncSpec) -> Result<String> {
    let canonical = serde_json::to_vec(spec).context("failed to serialize sync spec")?;
    let mut hasher = Sha256::new();
    hasher.update(&canonical);
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for b in digest {
        out.push_str(&format!("{b:02x}"));
    }
    Ok(out)
}

fn epoch_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or_default()
}

fn pull_spans_progress_message_with_baseline(
    state: &PullState,
    phase_started_at: u64,
    baseline_roots_done: usize,
    baseline_items_done: usize,
    baseline_bytes_written: u64,
) -> String {
    let elapsed = elapsed_seconds(phase_started_at);
    let traces_done = state.root_ids.len().saturating_sub(baseline_roots_done);
    let spans_done = state.items_done.saturating_sub(baseline_items_done);
    let bytes_done = state.bytes_written.saturating_sub(baseline_bytes_written);
    let traces_per_sec = traces_done as f64 / elapsed;
    let spans_per_sec = spans_done as f64 / elapsed;
    let bytes_per_sec = bytes_done as f64 / elapsed;
    let eta = format_eta(
        state.items_done.min(state.limit),
        state.limit,
        spans_per_sec,
    );
    format!(
        "{} traces ({:.2}/s) | {} spans ({:.2}/s) | {} ({}/s) | ETA {}",
        format_usize_commas(state.root_ids.len()),
        traces_per_sec,
        format_usize_commas(state.items_done),
        spans_per_sec,
        format_bytes(state.bytes_written as f64),
        format_bytes(bytes_per_sec),
        eta
    )
}

fn pull_trace_progress_message_with_baseline(
    state: &PullState,
    phase_started_at: u64,
    trace_progress_done: usize,
    baseline_trace_progress: usize,
    baseline_items_done: usize,
    baseline_bytes_written: u64,
) -> String {
    let elapsed = elapsed_seconds(phase_started_at);
    let traces_done = trace_progress_done.saturating_sub(baseline_trace_progress);
    let total_traces = state
        .limit
        .saturating_sub(baseline_trace_progress)
        .max(traces_done);
    let spans_done = state.items_done.saturating_sub(baseline_items_done);
    let bytes_done = state.bytes_written.saturating_sub(baseline_bytes_written);
    let traces_per_sec = traces_done as f64 / elapsed;
    let spans_per_sec = spans_done as f64 / elapsed;
    let bytes_per_sec = bytes_done as f64 / elapsed;
    let eta = format_eta(traces_done, total_traces, traces_per_sec);
    format!(
        "{} traces ({:.2}/s) | {} spans ({:.2}/s) | {} ({}/s) | ETA {}",
        format_usize_commas(trace_progress_done),
        traces_per_sec,
        format_usize_commas(state.items_done),
        spans_per_sec,
        format_bytes(state.bytes_written as f64),
        format_bytes(bytes_per_sec),
        eta
    )
}

fn push_progress_message_with_baseline(
    state: &PushState,
    phase_started_at: u64,
    baseline_roots_done: usize,
    baseline_items_done: usize,
    baseline_bytes_sent: u64,
    upload_total: Option<usize>,
) -> String {
    let elapsed = elapsed_seconds(phase_started_at);
    let traces_done = state
        .distinct_roots_done
        .saturating_sub(baseline_roots_done);
    let spans_done = state.items_done.saturating_sub(baseline_items_done);
    let bytes_done = state.bytes_sent.saturating_sub(baseline_bytes_sent);
    let traces_per_sec = traces_done as f64 / elapsed;
    let spans_per_sec = spans_done as f64 / elapsed;
    let bytes_per_sec = bytes_done as f64 / elapsed;

    let eta = if let Some(total_spans) = upload_total {
        format_eta(
            state.items_done.min(total_spans),
            total_spans,
            spans_per_sec,
        )
    } else if state.scope == ScopeArg::Traces.as_str() {
        match state.limit {
            Some(trace_limit) => {
                let total_traces = trace_limit
                    .saturating_sub(baseline_roots_done)
                    .max(traces_done);
                format_eta(traces_done, total_traces, traces_per_sec)
            }
            None => "--:--".to_string(),
        }
    } else {
        "--:--".to_string()
    };

    format!(
        "{} traces ({:.2}/s) | {} spans ({:.2}/s) | {} ({}/s) | ETA {}",
        format_usize_commas(state.distinct_roots_done),
        traces_per_sec,
        format_usize_commas(state.items_done),
        spans_per_sec,
        format_bytes(state.bytes_sent as f64),
        format_bytes(bytes_per_sec),
        eta
    )
}

fn format_eta(done: usize, total: usize, rate_per_sec: f64) -> String {
    let remaining = total.saturating_sub(done);
    if remaining == 0 {
        return "00:00".to_string();
    }
    if rate_per_sec <= 0.0 {
        return "--:--".to_string();
    }
    let eta_secs = (remaining as f64 / rate_per_sec).ceil() as u64;
    format_duration(eta_secs)
}

fn elapsed_seconds(started_at: u64) -> f64 {
    let now = epoch_seconds();
    let elapsed = now.saturating_sub(started_at).max(1);
    elapsed as f64
}

fn format_bytes(bytes: f64) -> String {
    const UNITS: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    let mut value = bytes.max(0.0);
    let mut unit_idx = 0usize;
    while value >= 1024.0 && unit_idx < UNITS.len() - 1 {
        value /= 1024.0;
        unit_idx += 1;
    }
    format!("{value:.2} {}", UNITS[unit_idx])
}

fn format_duration(total_secs: u64) -> String {
    let hours = total_secs / 3600;
    let minutes = (total_secs % 3600) / 60;
    let seconds = total_secs % 60;
    if hours > 0 {
        format!("{hours:02}:{minutes:02}:{seconds:02}")
    } else {
        format!("{minutes:02}:{seconds:02}")
    }
}

fn format_usize_commas(value: usize) -> String {
    format_u64_commas(value as u64)
}

fn format_u64_commas(value: u64) -> String {
    let digits = value.to_string();
    let mut out = String::with_capacity(digits.len() + digits.len() / 3);
    for (idx, ch) in digits.chars().rev().enumerate() {
        if idx > 0 && idx % 3 == 0 {
            out.push(',');
        }
        out.push(ch);
    }
    out.chars().rev().collect()
}

fn new_pull_state(
    scope: &ScopeArg,
    limit: usize,
    page_size: usize,
    filter: Option<String>,
    cursor: Option<String>,
    output_path: String,
) -> PullState {
    let now = epoch_seconds();
    PullState {
        schema_version: STATE_SCHEMA_VERSION,
        run_id: format!("run-{}", now),
        status: RunStatus::Running,
        phase: if matches!(scope, ScopeArg::Traces) {
            PullPhase::DiscoverRoots
        } else {
            PullPhase::Spans
        },
        scope: scope.as_str().to_string(),
        limit,
        filter,
        page_size,
        cursor,
        root_discovery_cursor: None,
        root_ids: Vec::new(),
        current_root_index: 0,
        current_root_cursor: None,
        trace_chunks: Vec::new(),
        items_done: 0,
        pages_done: 0,
        bytes_written: 0,
        output_path,
        started_at: now,
        updated_at: now,
        completed_at: None,
    }
}

fn new_push_state(
    scope: String,
    limit: Option<usize>,
    page_size: usize,
    source_path: String,
) -> PushState {
    let now = epoch_seconds();
    PushState {
        schema_version: STATE_SCHEMA_VERSION,
        run_id: format!("run-{}", now),
        status: RunStatus::Running,
        scope,
        limit,
        page_size,
        source_path,
        line_offset: 0,
        items_done: 0,
        pages_done: 0,
        bytes_sent: 0,
        distinct_roots_done: 0,
        started_at: now,
        updated_at: now,
        completed_at: None,
    }
}

fn open_jsonl_part_writer(base_dir: &Path, append: bool) -> Result<JsonlPartWriter> {
    JsonlPartWriter::new(base_dir, append)
}

fn write_jsonl_row(writer: &mut JsonlPartWriter, row: &Map<String, Value>) -> Result<usize> {
    let encoded = serde_json::to_string(row).context("failed to serialize row to JSONL")?;
    writer
        .write_line(&encoded)
        .context("failed to write JSONL row")
}

impl JsonlPartWriter {
    fn new(base_dir: &Path, append: bool) -> Result<Self> {
        fs::create_dir_all(base_dir)
            .with_context(|| format!("failed to create {}", base_dir.display()))?;

        let mut part_indices = list_output_part_indices(base_dir)?;
        let (part_index, current_bytes, writer) = if append && !part_indices.is_empty() {
            part_indices.sort_unstable();
            let idx = *part_indices.last().unwrap_or(&1usize);
            let path = output_part_path(base_dir, idx);
            let file = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("failed to open output file {}", path.display()))?;
            let bytes = file
                .metadata()
                .with_context(|| format!("failed to stat {}", path.display()))?
                .len();
            (idx, bytes, BufWriter::new(file))
        } else {
            let idx = 1usize;
            let path = output_part_path(base_dir, idx);
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .with_context(|| format!("failed to open output file {}", path.display()))?;
            (idx, 0u64, BufWriter::new(file))
        };

        Ok(Self {
            base_dir: base_dir.to_path_buf(),
            part_index,
            current_bytes,
            writer,
        })
    }

    fn write_line(&mut self, line: &str) -> Result<usize> {
        let line_bytes = (line.len() + 1) as u64;
        if self.current_bytes > 0 && self.current_bytes + line_bytes > PULL_OUTPUT_PART_MAX_BYTES {
            self.rotate()?;
        }
        self.writer
            .write_all(line.as_bytes())
            .context("failed to write JSONL row")?;
        self.writer
            .write_all(b"\n")
            .context("failed to write JSONL newline")?;
        self.current_bytes += line_bytes;
        Ok(line_bytes as usize)
    }

    fn flush(&mut self) -> Result<()> {
        self.writer.flush().context("failed to flush JSONL output")
    }

    fn rotate(&mut self) -> Result<()> {
        self.writer
            .flush()
            .context("failed to flush JSONL output")?;
        self.part_index += 1;
        let path = output_part_path(&self.base_dir, self.part_index);
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .with_context(|| format!("failed to open output file {}", path.display()))?;
        self.writer = BufWriter::new(file);
        self.current_bytes = 0;
        Ok(())
    }
}

fn output_part_path(base_dir: &Path, part_index: usize) -> PathBuf {
    base_dir.join(format!("part-{part_index:06}.jsonl"))
}

fn list_output_part_indices(base_dir: &Path) -> Result<Vec<usize>> {
    if !base_dir.exists() {
        return Ok(Vec::new());
    }

    let mut indices = Vec::new();
    for entry in
        fs::read_dir(base_dir).with_context(|| format!("failed to read {}", base_dir.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let Some(name) = entry.file_name().to_str().map(str::to_string) else {
            continue;
        };
        if !name.starts_with("part-") || !name.ends_with(".jsonl") {
            continue;
        }
        let num = &name["part-".len()..name.len() - ".jsonl".len()];
        if let Ok(idx) = num.parse::<usize>() {
            indices.push(idx);
        }
    }
    Ok(indices)
}

fn update_manifest_from_pull_state(
    path: &Path,
    spec_hash: &str,
    spec: &SyncSpec,
    state: &PullState,
    status_override: Option<RunStatus>,
) -> Result<()> {
    let manifest = SyncManifest {
        schema_version: STATE_SCHEMA_VERSION,
        spec_hash: spec_hash.to_string(),
        spec: spec.clone(),
        last_run_id: state.run_id.clone(),
        status: status_override.unwrap_or(state.status),
        items_done: state.items_done,
        pages_done: state.pages_done,
        bytes_processed: state.bytes_written,
        output_path: Some(state.output_path.clone()),
        input_path: None,
        started_at: state.started_at,
        updated_at: state.updated_at,
        completed_at: state.completed_at,
        message: None,
    };
    write_json_atomic(path, &manifest)
}

fn update_manifest_from_push_state(
    path: &Path,
    spec_hash: &str,
    spec: &SyncSpec,
    state: &PushState,
    status_override: Option<RunStatus>,
) -> Result<()> {
    let manifest = SyncManifest {
        schema_version: STATE_SCHEMA_VERSION,
        spec_hash: spec_hash.to_string(),
        spec: spec.clone(),
        last_run_id: state.run_id.clone(),
        status: status_override.unwrap_or(state.status),
        items_done: state.items_done,
        pages_done: state.pages_done,
        bytes_processed: state.bytes_sent,
        output_path: None,
        input_path: Some(state.source_path.clone()),
        started_at: state.started_at,
        updated_at: state.updated_at,
        completed_at: state.completed_at,
        message: None,
    };
    write_json_atomic(path, &manifest)
}

fn resolve_default_push_input(root: &Path, object: &ObjectRef) -> Result<PathBuf> {
    let mut best: Option<(u64, PathBuf)> = None;
    for spec_dir in collect_object_spec_dirs(root, object)? {
        let manifest_path = spec_dir.join("manifest.json");
        if !manifest_path.exists() {
            continue;
        }
        let manifest = read_json_file::<SyncManifest>(&manifest_path)?;
        if manifest.status != RunStatus::Completed || manifest.spec.direction != "pull" {
            continue;
        }
        let output_path = manifest
            .output_path
            .as_ref()
            .map(PathBuf::from)
            .filter(|path| path.exists())
            .or_else(|| resolve_pull_spec_output_path(&spec_dir).ok().flatten());
        let Some(output_path) = output_path else {
            continue;
        };
        let updated_at = manifest.updated_at;
        if best
            .as_ref()
            .map(|(best_time, _)| updated_at > *best_time)
            .unwrap_or(true)
        {
            best = Some((updated_at, output_path));
        }
    }

    best.map(|(_, path)| path).ok_or_else(|| {
        anyhow!(
            "no completed pull output found for object {}. run `bt sync pull {}:{} ...` first or pass --in",
            object.object_name,
            object.object_type,
            object.object_name
        )
    })
}

fn collect_object_spec_dirs(root: &Path, object: &ObjectRef) -> Result<Vec<PathBuf>> {
    let mut spec_dirs = Vec::new();

    let new_base = root.join(format!(
        "{}_{}",
        sanitize_segment(&object.object_type),
        sanitize_segment(&object.object_name)
    ));
    if new_base.is_dir() {
        for entry in fs::read_dir(&new_base)
            .with_context(|| format!("failed to read {}", new_base.display()))?
        {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                spec_dirs.push(entry.path());
            }
        }
    }

    let legacy_base = root
        .join(sanitize_segment(&object.object_type))
        .join(sanitize_segment(&object.object_name));
    if legacy_base.is_dir() {
        for direction_dir in fs::read_dir(&legacy_base)
            .with_context(|| format!("failed to read {}", legacy_base.display()))?
        {
            let direction_dir = direction_dir?;
            if !direction_dir.file_type()?.is_dir() {
                continue;
            }
            for scope_dir in fs::read_dir(direction_dir.path())? {
                let scope_dir = scope_dir?;
                if !scope_dir.file_type()?.is_dir() {
                    continue;
                }
                for spec_dir in fs::read_dir(scope_dir.path())? {
                    let spec_dir = spec_dir?;
                    if spec_dir.file_type()?.is_dir() {
                        spec_dirs.push(spec_dir.path());
                    }
                }
            }
        }
    }

    spec_dirs.sort();
    spec_dirs.dedup();
    Ok(spec_dirs)
}

fn resolve_push_input_files(input_path: &Path) -> Result<Vec<PathBuf>> {
    if input_path.is_file() {
        return Ok(vec![input_path.to_path_buf()]);
    }
    if !input_path.is_dir() {
        bail!(
            "input path is neither file nor directory: {}",
            input_path.display()
        );
    }

    let mut files = collect_json_input_files(input_path)?;
    if files.is_empty() {
        if let Some(resolved_path) = resolve_pull_spec_output_path(input_path)? {
            files = if resolved_path.is_file() {
                vec![resolved_path]
            } else if resolved_path.is_dir() {
                collect_json_input_files(&resolved_path)?
            } else {
                Vec::new()
            };
        }
    }
    if files.is_empty() {
        bail!(
            "no .jsonl or .ndjson files found in input directory {}",
            input_path.display()
        );
    }
    Ok(files)
}

fn collect_json_input_files(dir: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for entry in fs::read_dir(dir).with_context(|| format!("failed to read {}", dir.display()))? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            continue;
        }
        let path = entry.path();
        let Some(ext) = path.extension().and_then(|s| s.to_str()) else {
            continue;
        };
        if matches!(ext, "jsonl" | "ndjson") {
            files.push(path);
        }
    }
    files.sort();
    Ok(files)
}

fn resolve_pull_spec_output_path(spec_dir: &Path) -> Result<Option<PathBuf>> {
    let manifest_path = spec_dir.join("manifest.json");
    if manifest_path.exists() {
        let manifest = read_json_file::<SyncManifest>(&manifest_path)
            .with_context(|| format!("failed to parse {}", manifest_path.display()))?;
        if let Some(path) = manifest.output_path {
            let candidate = PathBuf::from(path);
            if candidate.exists() {
                return Ok(Some(candidate));
            }
            let joined = spec_dir.join(candidate);
            if joined.exists() {
                return Ok(Some(joined));
            }
        }
    }

    let data_dir = spec_dir.join("data");
    if data_dir.is_dir() {
        return Ok(Some(data_dir));
    }
    let legacy_jsonl = spec_dir.join("data.jsonl");
    if legacy_jsonl.is_file() {
        return Ok(Some(legacy_jsonl));
    }
    let legacy_ndjson = spec_dir.join("data.ndjson");
    if legacy_ndjson.is_file() {
        return Ok(Some(legacy_ndjson));
    }
    Ok(None)
}

fn upload_total_for_progress(
    input_files: &[PathBuf],
    scope: &ScopeArg,
    limit: Option<usize>,
) -> Result<Option<usize>> {
    if matches!(scope, ScopeArg::Traces) {
        return Ok(None);
    }

    let total_lines = count_lines(input_files)?;
    let capped = limit.map(|l| l.min(total_lines)).unwrap_or(total_lines);
    Ok(Some(capped))
}

fn count_lines(paths: &[PathBuf]) -> Result<usize> {
    let mut count = 0usize;
    for path in paths {
        let file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let reader = BufReader::new(file);
        for line in reader.lines() {
            line.with_context(|| format!("failed reading {}", path.display()))?;
            count += 1;
        }
    }
    Ok(count)
}

fn collect_seen_roots_until_offset(
    paths: &[PathBuf],
    line_offset: usize,
) -> Result<HashSet<String>> {
    let mut seen = HashSet::new();
    let mut global_line_index = 0usize;

    'files: for path in paths {
        let file =
            File::open(path).with_context(|| format!("failed to open {}", path.display()))?;
        let reader = BufReader::new(file);
        for (line_index, line) in reader.lines().enumerate() {
            if global_line_index >= line_offset {
                break 'files;
            }
            let line = line.with_context(|| format!("failed reading {}", path.display()))?;
            if !line.trim().is_empty() {
                let row: Map<String, Value> = serde_json::from_str(&line).with_context(|| {
                    format!(
                        "invalid JSON in {} at line {} while rebuilding trace resume state",
                        path.display(),
                        line_index + 1
                    )
                })?;
                if let Some(root_id) = row_root_span_id(&row) {
                    seen.insert(root_id);
                }
            }
            global_line_index += 1;
        }
    }
    Ok(seen)
}

fn row_root_span_id(row: &Map<String, Value>) -> Option<String> {
    value_as_string(row.get("root_span_id"))
        .or_else(|| value_as_string(row.get("span_id")))
        .or_else(|| value_as_string(row.get("id")))
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(s)) => Some(s.to_string()),
        Some(Value::Number(n)) => Some(n.to_string()),
        _ => None,
    }
}

fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("path has no parent: {}", path.display()))?;
    fs::create_dir_all(parent).with_context(|| format!("failed to create {}", parent.display()))?;

    let bytes = serde_json::to_vec_pretty(value).context("failed to serialize JSON")?;
    let tmp = path.with_extension("tmp");
    fs::write(&tmp, bytes).with_context(|| format!("failed to write {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| {
        format!(
            "failed to move temporary file {} to {}",
            tmp.display(),
            path.display()
        )
    })?;
    Ok(())
}

fn read_json_file<T: DeserializeOwned>(path: &Path) -> Result<T> {
    let bytes = fs::read(path).with_context(|| format!("failed to read {}", path.display()))?;
    serde_json::from_slice(&bytes).with_context(|| format!("failed to parse {}", path.display()))
}

fn trim_optional(value: Option<String>) -> Option<String> {
    value.and_then(|v| {
        let trimmed = v.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(trimmed.to_string())
        }
    })
}

fn btql_quote(value: &str) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| format!("\"{}\"", value.replace('\\', "\\\\").replace('\"', "\\\"")))
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn show_checkpoint_hint_line(pb: &ProgressBar) {
    if std::io::stderr().is_terminal() {
        pb.println("  Ctrl+C safely checkpoints; rerun same command to resume (--fresh restarts).");
    }
}

struct PullProgressUi {
    _multi: Option<Arc<MultiProgress>>,
    main: ProgressBar,
    status_line: ProgressBar,
}

fn bounded_bar_with_status_line(total: u64, message: &str, unit_label: &str) -> PullProgressUi {
    if !std::io::stderr().is_terminal() {
        return PullProgressUi {
            _multi: None,
            main: ProgressBar::hidden(),
            status_line: ProgressBar::hidden(),
        };
    }

    let multi = Arc::new(MultiProgress::new());
    let main = multi.add(ProgressBar::new(total));
    let template = format!(
        "{{spinner:.cyan}} {{prefix}} [{{bar:40.cyan/blue}}] {{pos}}/{{len}} {unit_label} ({{percent:>3}}%) | {{msg}}"
    );
    main.set_style(ProgressStyle::with_template(&template).unwrap());
    main.set_prefix(message.to_string());
    main.enable_steady_tick(Duration::from_millis(80));

    let status_line = multi.add(ProgressBar::new_spinner());
    status_line.set_style(ProgressStyle::with_template("  {msg}").unwrap());
    status_line.enable_steady_tick(Duration::from_millis(300));

    PullProgressUi {
        _multi: Some(multi),
        main,
        status_line,
    }
}

fn pull_status_line(show_checkpoint_hint: bool, retry_summary: Option<&str>) -> String {
    let mut parts = Vec::new();
    if show_checkpoint_hint {
        parts.push(
            "Ctrl+C checkpoints; rerun same command to resume (--fresh restarts).".to_string(),
        );
    }
    if let Some(summary) = retry_summary.filter(|s| !s.is_empty()) {
        parts.push(summary.to_string());
    }
    if parts.is_empty() {
        String::new()
    } else {
        parts.join("  ")
    }
}

fn bounded_bar(total: u64, message: &str, unit_label: &str) -> ProgressBar {
    if !std::io::stderr().is_terminal() {
        return ProgressBar::hidden();
    }
    let pb = ProgressBar::new(total);
    let template = format!(
        "{{spinner:.cyan}} {{prefix}} [{{bar:40.cyan/blue}}] {{pos}}/{{len}} {unit_label} ({{percent:>3}}%) | {{msg}}"
    );
    pb.set_style(ProgressStyle::with_template(&template).unwrap());
    pb.set_prefix(message.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(80));
    pb
}

fn spinner_bar(message: &str) -> ProgressBar {
    if !std::io::stderr().is_terminal() {
        return ProgressBar::hidden();
    }
    let pb = ProgressBar::new_spinner();
    pb.set_style(
        ProgressStyle::with_template("{spinner:.cyan} {prefix} {msg}")
            .unwrap()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏", " "]),
    );
    pb.set_prefix(message.to_string());
    pb.enable_steady_tick(std::time::Duration::from_millis(80));
    pb
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn push_checkpoint_line_offset_advances_only_after_commit() {
        let mut state =
            new_push_state("traces".to_string(), Some(10), 2, "input.jsonl".to_string());
        assert_eq!(state.line_offset, 0);

        // Simulate scanning rows into an in-memory batch but getting interrupted before upload.
        let _scanned_until = 2usize;
        assert_eq!(state.line_offset, 0);

        commit_push_batch_state(&mut state, 2, 128, 2, 1);

        assert_eq!(state.items_done, 2);
        assert_eq!(state.pages_done, 1);
        assert_eq!(state.bytes_sent, 128);
        assert_eq!(state.line_offset, 2);
        assert_eq!(state.distinct_roots_done, 1);
    }

    #[test]
    fn resume_root_rebuild_respects_committed_line_offset() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let path = std::env::temp_dir().join(format!(
            "bt-sync-resume-root-rebuild-{}-{}.jsonl",
            std::process::id(),
            unique
        ));

        fs::write(
            &path,
            "{\"root_span_id\":\"r1\"}\n{\"root_span_id\":\"r2\"}\n{\"root_span_id\":\"r3\"}\n",
        )?;

        let seen = collect_seen_roots_until_offset(&[path.clone()], 2)?;
        assert!(seen.contains("r1"));
        assert!(seen.contains("r2"));
        assert!(!seen.contains("r3"));

        let _ = fs::remove_file(&path);
        Ok(())
    }

    #[test]
    fn push_flush_results_commits_in_order() -> Result<()> {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or_default();
        let state_path = std::env::temp_dir().join(format!(
            "bt-sync-push-state-{}-{}.json",
            std::process::id(),
            unique
        ));

        let mut state = new_push_state("spans".to_string(), Some(10), 2, "input.jsonl".to_string());
        let mut pending = BTreeMap::new();
        let mut next_commit_index = 0usize;
        let pb = ProgressBar::hidden();
        let started_at = epoch_seconds();

        pending.insert(
            1,
            PushBatchResult {
                batch_index: 1,
                row_count: 2,
                bytes_sent: 200,
                end_line_offset: 4,
                distinct_roots_done: 2,
            },
        );
        flush_ready_push_results(
            &mut pending,
            &mut next_commit_index,
            &mut state,
            &state_path,
            &pb,
            Some(10),
            started_at,
            0,
            0,
            0,
        )?;
        assert_eq!(next_commit_index, 0);
        assert_eq!(state.items_done, 0);
        assert_eq!(state.line_offset, 0);

        pending.insert(
            0,
            PushBatchResult {
                batch_index: 0,
                row_count: 2,
                bytes_sent: 100,
                end_line_offset: 2,
                distinct_roots_done: 1,
            },
        );
        flush_ready_push_results(
            &mut pending,
            &mut next_commit_index,
            &mut state,
            &state_path,
            &pb,
            Some(10),
            started_at,
            0,
            0,
            0,
        )?;

        assert_eq!(next_commit_index, 2);
        assert_eq!(state.items_done, 4);
        assert_eq!(state.pages_done, 2);
        assert_eq!(state.bytes_sent, 300);
        assert_eq!(state.line_offset, 4);
        assert_eq!(state.distinct_roots_done, 2);
        assert!(pending.is_empty());

        let _ = fs::remove_file(&state_path);
        Ok(())
    }

    #[test]
    fn trace_resume_with_empty_fetch_state_reenters_discovery() {
        let mut state = new_pull_state(
            &ScopeArg::Traces,
            100,
            200,
            None,
            None,
            "out/data".to_string(),
        );
        state.phase = PullPhase::FetchRoots;
        state.items_done = 0;
        state.current_root_index = 0;
        state.root_ids.clear();
        state.trace_chunks.clear();

        assert!(trace_state_needs_discovery(&state));

        state.root_ids.push("r1".to_string());
        assert!(!trace_state_needs_discovery(&state));
    }

    #[test]
    fn root_spans_query_applies_user_filter() {
        let roots = vec!["root-1".to_string(), "root-2".to_string()];
        let query = build_root_spans_query(
            "project_logs('p')",
            &roots,
            Some("span_attributes.purpose != 'scorer'"),
            200,
            None,
        );

        assert!(query.contains("filter: ("));
        assert!(query.contains("root_span_id IN ["));
        assert!(query.contains(") AND (span_attributes.purpose != 'scorer')"));
    }
}
