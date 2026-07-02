use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;

use actix_web::{web, App, HttpResponse, HttpServer};
use anyhow::{anyhow, bail, Context, Result};
use clap::{builder::BoolishValueParser, Args, Subcommand};
use reqwest::Url;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use urlencoding::encode;

use crate::args::BaseArgs;
use crate::auth;
use crate::datasets::api as datasets_api;
use crate::functions::{self, api as functions_api, IfExistsMode};
use crate::http::ApiClient;
use crate::js_runner;
use crate::project_context::resolve_required_project;
use crate::projects::api::{get_project_by_name, list_projects, Project};
use crate::ui::{self, with_spinner};
use crate::utils::{app_project_url, app_project_url_with_encoded_path};

const VIEWS_JS_RUNNER_FILE: &str = "views-runner.ts";
const VIEWS_JS_SDK_FILE: &str = "views-sdk.ts";
const VIEWS_JS_RUNNER_SOURCE: &str = include_str!("../scripts/views-runner.ts");
const VIEWS_JS_SDK_SOURCE: &str = include_str!("../scripts/views-sdk.ts");
const DEFAULT_TRACE_PREVIEW_LIMIT: usize = 1000;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt views push ./views
  bt views push ./conversation.view.tsx --if-exists replace
  bt views trace bootstrap
  bt views dataset bootstrap --dataset test-dataset
  bt views trace preview ./conversation.view.tsx --url <BRAINTRUST_TRACE_URL>
  bt views trace preview ./conversation.view.tsx --trace-id <ROOT_SPAN_ID>
  bt views dataset preview ./dataset.view.tsx --dataset test-dataset --row-index 0
")]
pub struct ViewsArgs {
    #[command(subcommand)]
    command: ViewsCommands,
}

#[derive(Debug, Clone, Subcommand)]
enum ViewsCommands {
    /// Push local custom view definitions
    Push(ViewsPushArgs),
    /// Work with trace custom views
    #[command(visible_alias = "traces")]
    Trace(TraceViewsArgs),
    /// Work with dataset custom views
    #[command(visible_alias = "datasets")]
    Dataset(DatasetViewsArgs),
    /// Preview one custom view locally
    #[command(hide = true)]
    Preview(ViewsPreviewArgs),
}

#[derive(Debug, Clone, Args)]
struct ViewsPushArgs {
    /// File or directory path(s) to scan for custom view definitions.
    #[arg(value_name = "PATH")]
    paths: Vec<PathBuf>,

    /// File or directory path(s) to scan for custom view definitions.
    #[arg(
        long = "file",
        env = "BT_VIEWS_PUSH_FILES",
        value_name = "PATH",
        value_delimiter = ','
    )]
    file_flag: Vec<PathBuf>,

    /// Behavior when a custom view with the same slug already exists.
    #[arg(
        long = "if-exists",
        env = "BT_VIEWS_PUSH_IF_EXISTS",
        value_enum,
        default_value = "error"
    )]
    if_exists: IfExistsMode,

    /// Override JS runner binary (e.g. tsx, vite-node, deno).
    #[arg(long, env = "BT_VIEWS_PUSH_RUNNER", value_name = "RUNNER")]
    runner: Option<String>,

    /// Optional tsconfig path for JS runner and browser bundling.
    #[arg(long, env = "BT_VIEWS_PUSH_TSCONFIG", value_name = "PATH")]
    tsconfig: Option<PathBuf>,

    /// Skip confirmation prompt.
    #[arg(
        long,
        short = 'y',
        env = "BT_VIEWS_PUSH_YES",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    yes: bool,
}

impl ViewsPushArgs {
    fn resolved_paths(&self) -> Vec<PathBuf> {
        let mut paths = self.paths.clone();
        paths.extend(self.file_flag.iter().cloned());
        if paths.is_empty() {
            vec![PathBuf::from(".")]
        } else {
            paths
        }
    }
}

#[derive(Debug, Clone, Args)]
struct TraceViewsArgs {
    #[command(subcommand)]
    command: TraceViewsCommands,
}

#[derive(Debug, Clone, Subcommand)]
enum TraceViewsCommands {
    /// Create a starter trace custom view file
    Bootstrap(TraceViewBootstrapArgs),
    /// Preview one trace custom view locally
    Preview(TraceViewPreviewArgs),
}

#[derive(Debug, Clone, Args)]
struct DatasetViewsArgs {
    #[command(subcommand)]
    command: DatasetViewsCommands,
}

#[derive(Debug, Clone, Subcommand)]
enum DatasetViewsCommands {
    /// Create a starter dataset custom view file
    Bootstrap(DatasetViewBootstrapArgs),
    /// Preview one dataset custom view locally
    Preview(DatasetViewPreviewArgs),
}

#[derive(Debug, Clone, Args)]
struct BootstrapCommonArgs {
    /// Output file path. Defaults to views/<type>.view.tsx.
    #[arg(value_name = "PATH", conflicts_with = "file_flag")]
    path: Option<PathBuf>,

    /// Output file path. Defaults to views/<type>.view.tsx.
    #[arg(long = "file", env = "BT_VIEWS_BOOTSTRAP_FILE", value_name = "PATH")]
    file_flag: Option<PathBuf>,

    /// Overwrite an existing file.
    #[arg(
        long,
        short = 'f',
        env = "BT_VIEWS_BOOTSTRAP_FORCE",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    force: bool,
}

#[derive(Debug, Clone, Args)]
struct TraceViewBootstrapArgs {
    #[command(flatten)]
    common: BootstrapCommonArgs,
}

#[derive(Debug, Clone, Args)]
struct DatasetViewBootstrapArgs {
    #[command(flatten)]
    common: BootstrapCommonArgs,

    /// Dataset name to reference in the starter view.
    #[arg(
        long,
        env = "BT_VIEWS_BOOTSTRAP_DATASET",
        value_name = "NAME",
        conflicts_with = "dataset_id"
    )]
    dataset: Option<String>,

    /// Dataset id to reference in the starter view.
    #[arg(
        long = "dataset-id",
        env = "BT_VIEWS_BOOTSTRAP_DATASET_ID",
        value_name = "ID",
        conflicts_with = "dataset"
    )]
    dataset_id: Option<String>,
}

#[derive(Debug, Serialize)]
struct BootstrapResult {
    path: String,
    view_type: ViewType,
}

#[derive(Debug, Clone, Args)]
struct PreviewCommonArgs {
    /// Custom view file to preview.
    #[arg(value_name = "PATH")]
    path: PathBuf,

    /// View slug or name when the file registers multiple views.
    #[arg(long, env = "BT_VIEWS_PREVIEW_VIEW")]
    view: Option<String>,

    /// Local port to bind. Defaults to an ephemeral port.
    #[arg(long, env = "BT_VIEWS_PREVIEW_PORT", default_value_t = 0)]
    port: u16,

    /// Do not open a browser.
    #[arg(
        long,
        env = "BT_VIEWS_PREVIEW_NO_OPEN",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    no_open: bool,

    /// Override JS runner binary (e.g. tsx, vite-node, deno).
    #[arg(long, env = "BT_VIEWS_PREVIEW_RUNNER", value_name = "RUNNER")]
    runner: Option<String>,

    /// Optional tsconfig path for JS runner and browser bundling.
    #[arg(long, env = "BT_VIEWS_PREVIEW_TSCONFIG", value_name = "PATH")]
    tsconfig: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
struct TraceViewPreviewArgs {
    #[command(flatten)]
    common: PreviewCommonArgs,

    #[command(flatten)]
    target: TracePreviewTargetArgs,

    /// Print BTQL queries used for preview data.
    #[arg(
        long,
        env = "BT_VIEWS_PREVIEW_PRINT_QUERIES",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    print_queries: bool,
}

#[derive(Debug, Clone, Args)]
struct TracePreviewTargetArgs {
    /// Braintrust app URL to resolve trace preview data from.
    #[arg(long, env = "BT_VIEWS_PREVIEW_URL")]
    url: Option<String>,

    /// Object reference, format: project_logs:<project_id>.
    #[arg(long, env = "BT_VIEWS_PREVIEW_OBJECT_REF")]
    object_ref: Option<String>,

    /// Project ID to query for trace preview data.
    #[arg(long, env = "BT_VIEWS_PREVIEW_PROJECT_ID")]
    project_id: Option<String>,

    /// Root span id for trace preview data.
    #[arg(
        long = "trace-id",
        alias = "root-span-id",
        env = "BT_VIEWS_PREVIEW_TRACE_ID"
    )]
    trace_id: Option<String>,

    /// Selected span id or row id for trace preview data.
    #[arg(long, env = "BT_VIEWS_PREVIEW_SPAN_ID")]
    span_id: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct DatasetViewPreviewArgs {
    #[command(flatten)]
    common: PreviewCommonArgs,

    #[command(flatten)]
    target: DatasetPreviewTargetArgs,
}

#[derive(Debug, Clone, Args)]
struct DatasetPreviewTargetArgs {
    /// Dataset name or id for dataset preview data.
    #[arg(long, env = "BT_VIEWS_PREVIEW_DATASET")]
    dataset: Option<String>,

    /// Dataset row id for dataset preview data.
    #[arg(long, env = "BT_VIEWS_PREVIEW_ROW_ID")]
    row_id: Option<String>,

    /// Dataset row index for dataset preview data.
    #[arg(long, env = "BT_VIEWS_PREVIEW_ROW_INDEX")]
    row_index: Option<usize>,
}

#[derive(Debug, Clone, Args)]
struct ViewsPreviewArgs {
    #[command(flatten)]
    common: PreviewCommonArgs,

    #[command(flatten)]
    trace: TracePreviewTargetArgs,

    #[command(flatten)]
    dataset: DatasetPreviewTargetArgs,

    /// Print BTQL queries used for preview data.
    #[arg(
        long,
        env = "BT_VIEWS_PREVIEW_PRINT_QUERIES",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    print_queries: bool,
}

#[derive(Debug, Deserialize)]
struct ViewsManifest {
    runtime_context: ViewsRuntimeContext,
    #[serde(default)]
    files: Vec<ViewsManifestFile>,
}

#[derive(Debug, Deserialize)]
struct ViewsRuntimeContext {
    runtime: String,
    version: String,
}

#[derive(Debug, Deserialize)]
struct ViewsManifestFile {
    source_file: String,
    #[serde(default)]
    entries: Vec<ViewManifestEntry>,
}

#[derive(Debug, Deserialize, Clone)]
struct ViewManifestEntry {
    view_type: ViewType,
    name: String,
    slug: String,
    code: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    project_name: Option<String>,
    #[serde(default)]
    dataset_id: Option<String>,
    #[serde(default)]
    dataset_name: Option<String>,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    tags: Option<Vec<String>>,
}

#[derive(Debug, Deserialize, Serialize, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[serde(rename_all = "snake_case")]
enum ViewType {
    Trace,
    Dataset,
}

impl ViewType {
    fn label(self) -> &'static str {
        match self {
            Self::Trace => "trace",
            Self::Dataset => "dataset",
        }
    }
}

#[derive(Debug, Clone)]
struct PreparedView {
    source_file: String,
    entry: ViewManifestEntry,
    project: Project,
    dataset: Option<datasets_api::Dataset>,
}

#[derive(Debug, Serialize)]
struct PushedView {
    source_file: String,
    name: String,
    slug: String,
    view_type: ViewType,
    project_id: String,
    project_name: String,
    dataset_id: Option<String>,
    function_id: Option<String>,
    url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct BtqlResponse {
    data: Vec<Map<String, Value>>,
}

#[derive(Clone)]
struct PreviewServerState {
    client: ApiClient,
    project_id: Option<String>,
    data: Value,
}

struct PreviewContext {
    client: ApiClient,
    project: Project,
    entry: ViewManifestEntry,
}

struct TracePreviewData {
    project_id: String,
    data: Value,
}

#[derive(Debug, Deserialize)]
struct SpanFieldsRequest {
    #[serde(rename = "spanIds")]
    span_ids: Vec<String>,
    fields: Option<Vec<String>>,
}

pub async fn run(base: BaseArgs, args: ViewsArgs) -> Result<()> {
    match args.command {
        ViewsCommands::Push(push_args) => push(base, push_args).await,
        ViewsCommands::Trace(trace_args) => match trace_args.command {
            TraceViewsCommands::Bootstrap(bootstrap_args) => bootstrap_trace(base, bootstrap_args),
            TraceViewsCommands::Preview(preview_args) => preview_trace(base, preview_args).await,
        },
        ViewsCommands::Dataset(dataset_args) => match dataset_args.command {
            DatasetViewsCommands::Bootstrap(bootstrap_args) => {
                bootstrap_dataset(base, bootstrap_args)
            }
            DatasetViewsCommands::Preview(preview_args) => {
                preview_dataset(base, preview_args).await
            }
        },
        ViewsCommands::Preview(preview_args) => preview(base, preview_args).await,
    }
}

fn bootstrap_trace(base: BaseArgs, args: TraceViewBootstrapArgs) -> Result<()> {
    let path = write_bootstrap_file(args.common, "trace.view.tsx", TRACE_VIEW_BOOTSTRAP_TEMPLATE)?;
    print_bootstrap_result(base.json, ViewType::Trace, &path)
}

fn bootstrap_dataset(base: BaseArgs, args: DatasetViewBootstrapArgs) -> Result<()> {
    let dataset_ref = match (
        args.dataset_id.as_deref().map(str::trim),
        args.dataset.as_deref().map(str::trim),
    ) {
        (Some(dataset_id), _) if !dataset_id.is_empty() => {
            format!("{{ id: {dataset_id:?} }}")
        }
        (_, Some(dataset)) if !dataset.is_empty() => format!("{{ name: {dataset:?} }}"),
        _ => "{ name: \"test-dataset\" }".to_string(),
    };
    let content = dataset_view_bootstrap_template(&dataset_ref);
    let path = write_bootstrap_file(args.common, "dataset.view.tsx", &content)?;
    print_bootstrap_result(base.json, ViewType::Dataset, &path)
}

fn write_bootstrap_file(
    args: BootstrapCommonArgs,
    default_file_name: &str,
    content: &str,
) -> Result<PathBuf> {
    let selected_path = args
        .path
        .or(args.file_flag)
        .unwrap_or_else(|| PathBuf::from("views"));
    let path = if selected_path.is_dir() || selected_path.extension().is_none() {
        selected_path.join(default_file_name)
    } else {
        selected_path
    };
    let is_tsx_view_file = path
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(".view.tsx"));
    if !is_tsx_view_file {
        bail!("custom view bootstrap path must end with .view.tsx");
    }
    if path.exists() && !args.force {
        bail!(
            "custom view file already exists: {}. Use --force to overwrite.",
            path.display()
        );
    }
    if let Some(parent) = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    std::fs::write(&path, content)
        .with_context(|| format!("failed to write custom view file {}", path.display()))?;
    Ok(path)
}

fn print_bootstrap_result(json_output: bool, view_type: ViewType, path: &Path) -> Result<()> {
    let result = BootstrapResult {
        path: path.display().to_string(),
        view_type,
    };
    if json_output {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!(
            "Created {} custom view starter at {}",
            view_type.label(),
            path.display()
        );
        println!(
            "Preview it with: bt views {} preview {}",
            view_type.label(),
            path.display()
        );
    }
    Ok(())
}

const TRACE_VIEW_BOOTSTRAP_TEMPLATE: &str = r##"import React from "react";
import {
  customTraceView,
  formatJson,
  type TraceViewProps,
} from "braintrust/custom-views";

function pretty(value: unknown) {
  return value === undefined ? "" : formatJson(value);
}

function StarterTraceView({ trace, span, selectSpan }: TraceViewProps) {
  const spanIds = trace.spanOrder.slice(0, 100);

  return (
    <div style={{ fontFamily: "Inter, system-ui, sans-serif", padding: 16, color: "#111827" }}>
      <div style={{ display: "flex", alignItems: "center", gap: 12, marginBottom: 16 }}>
        <strong>Trace starter view</strong>
        <select value={span.span_id} onChange={(event) => selectSpan(event.target.value)}>
          {spanIds.map((spanId) => (
            <option key={spanId} value={spanId}>
              {spanId}
            </option>
          ))}
        </select>
      </div>

      <section style={{ border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, marginBottom: 12 }}>
        <div style={{ fontSize: 12, color: "#6b7280", marginBottom: 4 }}>Selected span</div>
        <div><strong>row id:</strong> {span.id}</div>
        <div><strong>span id:</strong> {span.span_id}</div>
        <div><strong>children:</strong> {span.children.length}</div>
      </section>

      <section style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: 12 }}>
        <pre style={{ margin: 0, background: "#f9fafb", border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, whiteSpace: "pre-wrap" }}>
          {pretty(span.data.input)}
        </pre>
        <pre style={{ margin: 0, background: "#f9fafb", border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, whiteSpace: "pre-wrap" }}>
          {pretty(span.data.output)}
        </pre>
      </section>
    </div>
  );
}

customTraceView({
  name: "Starter Trace View",
  slug: "starter-trace-view",
  description: "Starter trace custom view generated by bt.",
  component: StarterTraceView,
});
"##;

const DATASET_VIEW_BOOTSTRAP_TEMPLATE: &str = r##"import React from "react";
import {
  customDatasetView,
  formatJson,
  type DatasetViewProps,
} from "braintrust/custom-views";

function pretty(value: unknown) {
  return value === undefined ? "" : formatJson(value);
}

function StarterDatasetView({ id, input, expected, metadata, tags }: DatasetViewProps) {
  return (
    <div style={{ fontFamily: "Inter, system-ui, sans-serif", padding: 16, color: "#111827" }}>
      <div style={{ marginBottom: 16 }}>
        <strong>Dataset starter view</strong>
        <div style={{ color: "#6b7280", fontSize: 12 }}>row id: {id}</div>
        {tags.length > 0 ? (
          <div style={{ color: "#6b7280", fontSize: 12 }}>tags: {tags.join(", ")}</div>
        ) : null}
      </div>

      <section style={{ display: "grid", gridTemplateColumns: "repeat(auto-fit, minmax(260px, 1fr))", gap: 12 }}>
        <pre style={{ margin: 0, background: "#f9fafb", border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, whiteSpace: "pre-wrap" }}>
          {pretty(input)}
        </pre>
        <pre style={{ margin: 0, background: "#f9fafb", border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, whiteSpace: "pre-wrap" }}>
          {pretty(expected)}
        </pre>
        <pre style={{ margin: 0, background: "#f9fafb", border: "1px solid #e5e7eb", borderRadius: 8, padding: 12, whiteSpace: "pre-wrap" }}>
          {pretty(metadata)}
        </pre>
      </section>
    </div>
  );
}

customDatasetView({
  name: "Starter Dataset View",
  slug: "starter-dataset-view",
  description: "Starter dataset custom view generated by bt.",
  dataset: __DATASET_REF__,
  component: StarterDatasetView,
});
"##;

fn dataset_view_bootstrap_template(dataset_ref: &str) -> String {
    DATASET_VIEW_BOOTSTRAP_TEMPLATE.replace("__DATASET_REF__", dataset_ref)
}

async fn push(base: BaseArgs, args: ViewsPushArgs) -> Result<()> {
    let auth_ctx = functions::resolve_auth_context(&base).await?;
    let default_project = resolve_required_project(&base, &auth_ctx.client, true).await?;
    let files = collect_view_files(&args.resolved_paths())?;
    if files.is_empty() {
        bail!("no custom view files found; expected files matching *.view.tsx, *.view.ts, *.view.jsx, or *.view.js");
    }

    let manifest = run_views_runner(args.runner.as_deref(), args.tsconfig.as_deref(), &files)?;
    validate_manifest_runtime(&manifest)?;
    let prepared = prepare_views(&auth_ctx.client, &default_project, &manifest).await?;
    if prepared.is_empty() {
        bail!("no custom views were registered by the selected files");
    }

    if !args.yes && ui::can_prompt() {
        let prompt = format!("Push {} custom view(s)?", prepared.len());
        let confirmed = dialoguer::Confirm::new()
            .with_prompt(prompt)
            .default(true)
            .interact()?;
        if !confirmed {
            bail!("custom view push cancelled");
        }
    }

    let events = prepared
        .iter()
        .map(|view| build_insert_event(view, args.if_exists))
        .collect::<Vec<_>>();

    let ignored = with_spinner(
        "Pushing custom views...",
        functions_api::insert_functions(&auth_ctx.client, &events),
    )
    .await?
    .ignored_entries
    .unwrap_or(0);

    let pushed = resolve_pushed_views(&auth_ctx.client, &auth_ctx.app_url, &prepared).await?;
    if base.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "pushed": pushed,
                "ignored": ignored,
            }))?
        );
        return Ok(());
    }

    eprintln!(
        "{} Pushed {} custom view(s)",
        dialoguer::console::style("✓").green(),
        prepared.len().saturating_sub(ignored)
    );
    for view in pushed {
        match view.url {
            Some(url) => println!("{} ({}) {}", view.name, view.slug, url),
            None => println!("{} ({})", view.name, view.slug),
        }
    }

    Ok(())
}

async fn preview(base: BaseArgs, args: ViewsPreviewArgs) -> Result<()> {
    let context = resolve_preview_context(&base, &args.common, None).await?;
    match context.entry.view_type {
        ViewType::Trace => {
            let trace_data = build_trace_preview_data(
                &context.client,
                &context.project,
                &args.trace,
                args.print_queries,
            )
            .await?;
            serve_preview(
                base,
                &args.common,
                context.client,
                Some(trace_data.project_id),
                context.entry,
                trace_data.data,
            )
            .await
        }
        ViewType::Dataset => {
            let preview_data = build_dataset_preview_data(
                &context.client,
                &context.project,
                &context.entry,
                &args.dataset,
            )
            .await?;
            serve_preview(
                base,
                &args.common,
                context.client,
                None,
                context.entry,
                preview_data,
            )
            .await
        }
    }
}

async fn preview_trace(base: BaseArgs, args: TraceViewPreviewArgs) -> Result<()> {
    let context = resolve_preview_context(&base, &args.common, Some(ViewType::Trace)).await?;
    let trace_data = build_trace_preview_data(
        &context.client,
        &context.project,
        &args.target,
        args.print_queries,
    )
    .await?;
    serve_preview(
        base,
        &args.common,
        context.client,
        Some(trace_data.project_id),
        context.entry,
        trace_data.data,
    )
    .await
}

async fn preview_dataset(base: BaseArgs, args: DatasetViewPreviewArgs) -> Result<()> {
    let context = resolve_preview_context(&base, &args.common, Some(ViewType::Dataset)).await?;
    let preview_data = build_dataset_preview_data(
        &context.client,
        &context.project,
        &context.entry,
        &args.target,
    )
    .await?;
    serve_preview(
        base,
        &args.common,
        context.client,
        None,
        context.entry,
        preview_data,
    )
    .await
}

async fn resolve_preview_context(
    base: &BaseArgs,
    args: &PreviewCommonArgs,
    view_type: Option<ViewType>,
) -> Result<PreviewContext> {
    let files = vec![args.path.clone()];
    let manifest = run_views_runner(args.runner.as_deref(), args.tsconfig.as_deref(), &files)?;
    validate_manifest_runtime(&manifest)?;
    let entry = select_preview_entry(&manifest, args.view.as_deref(), view_type)?.clone();

    let auth_ctx = auth::login_read_only(&base).await?;
    let client = ApiClient::new(&auth_ctx)?;
    let default_project = resolve_required_project(&base, &client, true).await?;
    let project = resolve_project_for_entry(&client, &default_project, &entry).await?;
    Ok(PreviewContext {
        client,
        project,
        entry,
    })
}

async fn serve_preview(
    base: BaseArgs,
    args: &PreviewCommonArgs,
    client: ApiClient,
    trace_project_id: Option<String>,
    entry: ViewManifestEntry,
    preview_data: Value,
) -> Result<()> {
    let listener = TcpListener::bind(("127.0.0.1", args.port))
        .with_context(|| format!("failed to bind preview server on port {}", args.port))?;
    let addr = listener
        .local_addr()
        .context("failed to read preview address")?;
    let url = format!("http://{addr}");
    let state = web::Data::new(PreviewServerState {
        client,
        project_id: trace_project_id,
        data: preview_data,
    });
    let code = entry.code.clone();
    let name = entry.name.clone();
    let server = HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .app_data(web::Data::new(PreviewPage {
                title: name.clone(),
                code: code.clone(),
            }))
            .route("/", web::get().to(preview_index))
            .route("/span-fields", web::post().to(preview_span_fields))
    })
    .workers(1)
    .listen(listener)?
    .run();
    let handle = server.handle();
    tokio::spawn(server);

    if base.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "url": url,
                "view": {
                    "name": entry.name,
                    "slug": entry.slug,
                    "type": entry.view_type,
                },
            }))?
        );
    } else {
        println!("Previewing {} at {}", entry.name, url);
    }

    if !args.no_open {
        open::that(&url)?;
    }

    tokio::signal::ctrl_c()
        .await
        .context("failed to wait for Ctrl+C")?;
    handle.stop(true).await;
    Ok(())
}

#[derive(Clone)]
struct PreviewPage {
    title: String,
    code: String,
}

async fn preview_index(
    state: web::Data<PreviewServerState>,
    page: web::Data<PreviewPage>,
) -> HttpResponse {
    let html = render_preview_html(&page.title, &page.code, &state.data);
    HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html)
}

async fn preview_span_fields(
    state: web::Data<PreviewServerState>,
    body: web::Json<SpanFieldsRequest>,
) -> HttpResponse {
    let Some(project_id) = state.project_id.as_deref() else {
        return HttpResponse::BadRequest().json(json!({
            "error": "fetchSpanFields is only available for trace previews"
        }));
    };

    let mut response = Map::new();
    let spans = state
        .data
        .get("trace")
        .and_then(|trace| trace.get("spans"))
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    for span_id in &body.span_ids {
        let row_id = resolve_preview_row_id(&spans, span_id).unwrap_or_else(|| span_id.clone());
        match fetch_full_span_row(&state.client, project_id, &row_id, false).await {
            Ok(Some(row)) => {
                response.insert(
                    span_id.clone(),
                    fields_from_row(row, body.fields.as_deref()),
                );
            }
            Ok(None) => {
                response.insert(span_id.clone(), json!({}));
            }
            Err(err) => {
                return HttpResponse::InternalServerError().json(json!({
                    "error": format!("{err:#}")
                }));
            }
        }
    }

    HttpResponse::Ok().json(Value::Object(response))
}

fn render_preview_html(title: &str, code: &str, data: &Value) -> String {
    let data_json = script_json(data);
    let code_json = script_json(&Value::String(code.to_string()));
    let title_json = script_json(&Value::String(title.to_string()));
    format!(
        r##"<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>{}</title>
  <script src="https://cdn.tailwindcss.com"></script>
  <script crossorigin src="https://unpkg.com/react@18/umd/react.development.js"></script>
  <script crossorigin src="https://unpkg.com/react-dom@18/umd/react-dom.development.js"></script>
  <style>
    html, body, #root {{ min-height: 100%; margin: 0; }}
    body {{ font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; }}
  </style>
</head>
<body>
  <div id="root"></div>
  <script>
    const previewTitle = {title_json};
    const initialData = {data_json};
    const customViewCode = {code_json};
    const root = ReactDOM.createRoot(document.getElementById("root"));
    let selectedSpanId = initialData.trace?.selectedSpanId;
    let trace = initialData.trace;
    let span = initialData.span;
    const datasetProps = initialData.props;

    function resolveSpan(id) {{
      if (!trace?.spans) return null;
      if (trace.spans[id]) return trace.spans[id];
      return Object.values(trace.spans).find((candidate) => candidate.id === id) || null;
    }}

    async function fetchSpanFields(spanIds, fields) {{
      const ids = Array.isArray(spanIds) ? spanIds : [spanIds];
      const response = await fetch("/span-fields", {{
        method: "POST",
        headers: {{ "content-type": "application/json" }},
        body: JSON.stringify({{ spanIds: ids, fields }}),
      }});
      if (!response.ok) {{
        const text = await response.text();
        throw new Error(text || "fetchSpanFields failed");
      }}
      return response.json();
    }}

    function update(fieldOrPatch, value) {{
      if (typeof fieldOrPatch === "string") {{
        if (span) {{
          span = {{
            ...span,
            data: {{
              ...span.data,
              metadata: {{ ...(span.data?.metadata || {{}}), [fieldOrPatch]: value }},
            }},
          }};
          if (trace?.spans?.[span.span_id]) {{
            trace = {{
              ...trace,
              spans: {{ ...trace.spans, [span.span_id]: span }},
            }};
          }}
        }}
        console.info("Preview update only:", fieldOrPatch, value);
      }} else {{
        const patch = fieldOrPatch || {{}};
        if (patch.metadata && span) {{
          span = {{
            ...span,
            data: {{
              ...span.data,
              metadata: {{ ...(span.data?.metadata || {{}}), ...patch.metadata }},
            }},
          }};
          if (trace?.spans?.[span.span_id]) {{
            trace = {{
              ...trace,
              spans: {{ ...trace.spans, [span.span_id]: span }},
            }};
          }}
        }}
        console.info("Preview update only:", patch);
      }}
      render();
    }}

    function selectSpan(spanId) {{
      const next = resolveSpan(spanId);
      if (!next) {{
        console.warn("Span not found:", spanId);
        return;
      }}
      selectedSpanId = next.span_id;
      trace = {{ ...trace, selectedSpanId }};
      span = next;
      render();
    }}

    function componentFromCode() {{
      const module = {{ exports: {{}} }};
      const wrapper = new Function("module", "exports", "React", customViewCode);
      wrapper(module, module.exports, React);
      return module.exports.default || module.exports.CustomTraceRenderer || module.exports;
    }}

    function render() {{
      try {{
        const Component = componentFromCode();
        const props = trace && span
          ? {{ trace: {{ ...trace, fetchSpanFields }}, span, update, selectSpan }}
          : datasetProps;
        root.render(React.createElement(Component, props));
      }} catch (error) {{
        root.render(React.createElement("pre", {{
          style: {{ padding: "16px", color: "#b91c1c", whiteSpace: "pre-wrap" }}
        }}, `${{previewTitle}}\n\n${{error?.stack || error}}`));
      }}
    }}

    render();
  </script>
</body>
</html>"##,
        html_escape(title),
    )
}

fn script_json(value: &Value) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| "null".to_string())
        .replace("</", "<\\/")
}

fn html_escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

fn collect_view_files(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    for path in paths {
        let path = path.as_path();
        if !path.exists() {
            bail!("custom view path not found: {}", path.display());
        }
        if path.is_file() {
            if is_view_file(path) {
                files.push(path.to_path_buf());
            }
            continue;
        }
        collect_view_files_in_dir(path, &mut files)?;
    }
    files.sort();
    files.dedup();
    Ok(files)
}

fn collect_view_files_in_dir(dir: &Path, files: &mut Vec<PathBuf>) -> Result<()> {
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("failed to read directory {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            if should_skip_dir(&path) {
                continue;
            }
            collect_view_files_in_dir(&path, files)?;
        } else if path.is_file() && is_view_file(&path) {
            files.push(path);
        }
    }
    Ok(())
}

fn should_skip_dir(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            matches!(
                name,
                ".git" | ".bt" | "node_modules" | "target" | "dist" | "build" | ".venv" | "venv"
            )
        })
}

fn is_view_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    [".view.tsx", ".view.ts", ".view.jsx", ".view.js"]
        .iter()
        .any(|suffix| name.ends_with(suffix))
}

fn run_views_runner(
    runner: Option<&str>,
    tsconfig: Option<&Path>,
    files: &[PathBuf],
) -> Result<ViewsManifest> {
    let sdk_script = js_runner::materialize_runner_script_in_cwd(
        "views-runners",
        VIEWS_JS_SDK_FILE,
        VIEWS_JS_SDK_SOURCE,
    )
    .context("failed to materialize custom views SDK helper")?;
    let runner_script = js_runner::materialize_runner_script_in_cwd(
        "views-runners",
        VIEWS_JS_RUNNER_FILE,
        VIEWS_JS_RUNNER_SOURCE,
    )
    .context("failed to materialize custom views runner")?;
    let mut command = js_runner::build_js_runner_command(runner, &runner_script, files);
    command.env("BT_VIEWS_SDK_PATH", &sdk_script);
    if let Some(tsconfig) = tsconfig {
        command.env("TS_NODE_PROJECT", tsconfig);
        command.env("TSX_TSCONFIG_PATH", tsconfig);
    }

    let output = command.output().with_context(|| {
        format!(
            "failed to spawn custom views runner: {}",
            command_display(&command)
        )
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        let stdout = String::from_utf8_lossy(&output.stdout);
        let details = stderr.trim();
        let details = if details.is_empty() {
            stdout.trim()
        } else {
            details
        };
        bail!(
            "custom views runner exited with status {}: {}",
            output.status,
            details
        );
    }

    let stdout =
        String::from_utf8(output.stdout).context("custom views runner output was not UTF-8")?;
    serde_json::from_str(&stdout).with_context(|| {
        format!(
            "failed to parse custom views runner output as JSON: {}",
            stdout.trim()
        )
    })
}

fn command_display(command: &Command) -> String {
    let mut rendered = command.get_program().to_string_lossy().to_string();
    for arg in command.get_args() {
        rendered.push(' ');
        rendered.push_str(&arg.to_string_lossy());
    }
    rendered
}

fn validate_manifest_runtime(manifest: &ViewsManifest) -> Result<()> {
    if manifest.runtime_context.runtime != "browser" {
        bail!(
            "custom views runner returned unsupported runtime '{}'",
            manifest.runtime_context.runtime
        );
    }
    if manifest.runtime_context.version.trim().is_empty() {
        bail!("custom views runner returned an empty runtime version");
    }
    Ok(())
}

async fn prepare_views(
    client: &ApiClient,
    default_project: &Project,
    manifest: &ViewsManifest,
) -> Result<Vec<PreparedView>> {
    let mut prepared = Vec::new();
    let mut seen = BTreeSet::new();
    for file in &manifest.files {
        for entry in &file.entries {
            let key = (
                file.source_file.clone(),
                entry.view_type,
                entry.slug.clone(),
            );
            if !seen.insert(key) {
                bail!(
                    "duplicate custom view slug '{}' in {}",
                    entry.slug,
                    file.source_file
                );
            }
            let project = resolve_project_for_entry(client, default_project, entry).await?;
            let dataset = if entry.view_type == ViewType::Dataset {
                Some(resolve_dataset_for_entry(client, &project, entry).await?)
            } else {
                None
            };
            prepared.push(PreparedView {
                source_file: file.source_file.clone(),
                entry: entry.clone(),
                project,
                dataset,
            });
        }
    }
    Ok(prepared)
}

async fn resolve_project_for_entry(
    client: &ApiClient,
    default_project: &Project,
    entry: &ViewManifestEntry,
) -> Result<Project> {
    if let Some(project_id) = entry
        .project_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if project_id == default_project.id {
            return Ok(default_project.clone());
        }
        let projects = list_projects(client).await?;
        return projects
            .into_iter()
            .find(|project| project.id == project_id)
            .ok_or_else(|| anyhow!("project id '{project_id}' not found"));
    }

    if let Some(project_name) = entry
        .project_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if project_name == default_project.name {
            return Ok(default_project.clone());
        }
        return get_project_by_name(client, project_name)
            .await?
            .ok_or_else(|| anyhow!("project '{project_name}' not found"));
    }

    Ok(default_project.clone())
}

async fn resolve_dataset_for_entry(
    client: &ApiClient,
    project: &Project,
    entry: &ViewManifestEntry,
) -> Result<datasets_api::Dataset> {
    if let Some(dataset_id) = entry
        .dataset_id
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        let datasets = datasets_api::list_datasets(client, &project.id).await?;
        return datasets
            .into_iter()
            .find(|dataset| dataset.id == dataset_id)
            .ok_or_else(|| {
                anyhow!(
                    "dataset id '{dataset_id}' not found in project '{}'",
                    project.name
                )
            });
    }

    let dataset_name = entry
        .dataset_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| {
            anyhow!(
                "dataset custom view '{}' requires dataset id or name",
                entry.slug
            )
        })?;

    datasets_api::get_dataset_by_name(client, &project.id, dataset_name)
        .await?
        .ok_or_else(|| {
            anyhow!(
                "dataset '{dataset_name}' not found in project '{}'",
                project.name
            )
        })
}

fn build_insert_event(view: &PreparedView, if_exists: IfExistsMode) -> Value {
    let mut object = Map::new();
    object.insert(
        "project_id".to_string(),
        Value::String(view.project.id.clone()),
    );
    object.insert("name".to_string(), Value::String(view.entry.name.clone()));
    object.insert("slug".to_string(), Value::String(view.entry.slug.clone()));
    object.insert(
        "description".to_string(),
        Value::String(view.entry.description.clone().unwrap_or_default()),
    );
    object.insert(
        "function_type".to_string(),
        Value::String("custom_view".to_string()),
    );
    object.insert(
        "if_exists".to_string(),
        Value::String(if_exists.as_str().to_string()),
    );
    object.insert(
        "function_data".to_string(),
        json!({
            "type": "code",
            "data": {
                "type": "inline",
                "runtime_context": {
                    "runtime": "browser",
                    "version": "latest",
                },
                "code": view.entry.code,
            }
        }),
    );

    if let Some(metadata) = &view.entry.metadata {
        object.insert("metadata".to_string(), metadata.clone());
    }
    if let Some(tags) = &view.entry.tags {
        object.insert(
            "tags".to_string(),
            Value::Array(tags.iter().cloned().map(Value::String).collect()),
        );
    }
    if let Some(dataset) = &view.dataset {
        object.insert(
            "origin".to_string(),
            json!({
                "object_type": "dataset",
                "object_id": dataset.id,
            }),
        );
    }

    Value::Object(object)
}

async fn resolve_pushed_views(
    client: &ApiClient,
    app_url: &str,
    views: &[PreparedView],
) -> Result<Vec<PushedView>> {
    let mut pushed = Vec::new();
    for view in views {
        let function =
            functions_api::get_function_by_slug(client, &view.project.id, &view.entry.slug, None)
                .await
                .ok()
                .flatten();
        let function_id = function.as_ref().map(|function| function.id.clone());
        let url = function_id
            .as_deref()
            .map(|id| custom_view_url(app_url, client.org_name(), view, id));
        pushed.push(PushedView {
            source_file: view.source_file.clone(),
            name: view.entry.name.clone(),
            slug: view.entry.slug.clone(),
            view_type: view.entry.view_type,
            project_id: view.project.id.clone(),
            project_name: view.project.name.clone(),
            dataset_id: view.dataset.as_ref().map(|dataset| dataset.id.clone()),
            function_id,
            url,
        });
    }
    Ok(pushed)
}

fn custom_view_url(
    app_url: &str,
    org_name: &str,
    view: &PreparedView,
    function_id: &str,
) -> String {
    match (&view.entry.view_type, view.dataset.as_ref()) {
        (ViewType::Dataset, Some(dataset)) => {
            let mut url = app_project_url(
                app_url,
                org_name,
                &view.project.name,
                &["datasets", &dataset.name],
            );
            write!(url, "?dvt=custom&dv={}", encode(function_id)).ok();
            url
        }
        _ => app_project_url_with_encoded_path(
            app_url,
            org_name,
            &view.project.name,
            &format!("logs?tvt=custom&tv={}", encode(function_id)),
        ),
    }
}

fn select_preview_entry<'a>(
    manifest: &'a ViewsManifest,
    selector: Option<&str>,
    view_type: Option<ViewType>,
) -> Result<&'a ViewManifestEntry> {
    let entries = manifest
        .files
        .iter()
        .flat_map(|file| file.entries.iter())
        .filter(|entry| view_type.is_none_or(|view_type| entry.view_type == view_type))
        .collect::<Vec<_>>();
    if entries.is_empty() {
        match view_type {
            Some(view_type) => bail!(
                "selected file did not register any {} custom views",
                view_type.label()
            ),
            None => bail!("selected file did not register any custom views"),
        }
    }
    if let Some(selector) = selector {
        let matches = entries
            .into_iter()
            .filter(|entry| entry.slug == selector || entry.name == selector)
            .collect::<Vec<_>>();
        return match matches.as_slice() {
            [entry] => Ok(*entry),
            [] => match view_type {
                Some(view_type) => bail!(
                    "{} custom view '{selector}' not found in selected file",
                    view_type.label()
                ),
                None => bail!("custom view '{selector}' not found in selected file"),
            },
            _ => match view_type {
                Some(view_type) => bail!(
                    "{} custom view selector '{selector}' matched multiple views",
                    view_type.label()
                ),
                None => bail!("custom view selector '{selector}' matched multiple views"),
            },
        };
    }
    if entries.len() == 1 {
        return Ok(entries[0]);
    }
    match view_type {
        Some(view_type) => bail!(
            "selected file registers multiple {} custom views; pass --view <slug-or-name>",
            view_type.label()
        ),
        None => bail!("selected file registers multiple custom views; pass --view <slug-or-name>"),
    }
}

async fn build_dataset_preview_data(
    client: &ApiClient,
    project: &Project,
    entry: &ViewManifestEntry,
    args: &DatasetPreviewTargetArgs,
) -> Result<Value> {
    let dataset = match args.dataset.as_deref() {
        Some(selector) => resolve_dataset_by_selector(client, project, selector).await?,
        None => resolve_dataset_for_entry(client, project, entry).await?,
    };

    let row = if let Some(row_id) = args.row_id.as_deref() {
        datasets_api::get_dataset_row_by_id(client, &dataset.id, row_id)
            .await?
            .ok_or_else(|| anyhow!("dataset row id '{row_id}' not found in '{}'", dataset.name))?
    } else {
        let index = args.row_index.unwrap_or(0);
        let limit = index + 1;
        let (rows, _) = datasets_api::list_dataset_rows_limited(
            client,
            &dataset.id,
            Some(limit),
            datasets_api::DatasetRowsPreviewLength::Full,
        )
        .await?;
        rows.into_iter().nth(index).ok_or_else(|| {
            anyhow!(
                "dataset '{}' does not have row index {}",
                dataset.name,
                index
            )
        })?
    };

    Ok(json!({
        "props": {
            "id": row.get("id").cloned().unwrap_or(Value::Null),
            "input": row.get("input").cloned().unwrap_or(Value::Null),
            "expected": row.get("expected").cloned().unwrap_or(Value::Null),
            "metadata": row.get("metadata").cloned().unwrap_or_else(|| json!({})),
            "tags": row.get("tags").cloned().unwrap_or_else(|| json!([])),
        },
        "dataset": dataset,
    }))
}

async fn resolve_dataset_by_selector(
    client: &ApiClient,
    project: &Project,
    selector: &str,
) -> Result<datasets_api::Dataset> {
    let datasets = datasets_api::list_datasets(client, &project.id).await?;
    datasets
        .into_iter()
        .find(|dataset| dataset.id == selector || dataset.name == selector)
        .ok_or_else(|| {
            anyhow!(
                "dataset '{selector}' not found in project '{}'",
                project.name
            )
        })
}

async fn build_trace_preview_data(
    client: &ApiClient,
    project: &Project,
    args: &TracePreviewTargetArgs,
    print_queries: bool,
) -> Result<TracePreviewData> {
    let target = resolve_trace_preview_target(client, project, args).await?;
    let rows = fetch_trace_rows(
        client,
        &target.project_id,
        &target.root_span_id,
        DEFAULT_TRACE_PREVIEW_LIMIT,
        print_queries,
    )
    .await?;
    if rows.is_empty() {
        bail!("trace '{}' returned no spans", target.root_span_id);
    }
    let (trace, selected_span) =
        build_trace_payload(rows, &target.root_span_id, target.span_id.as_deref())?;
    Ok(TracePreviewData {
        project_id: target.project_id,
        data: json!({
            "trace": trace,
            "span": selected_span,
        }),
    })
}

#[derive(Debug)]
struct TracePreviewTarget {
    project_id: String,
    root_span_id: String,
    span_id: Option<String>,
}

async fn resolve_trace_preview_target(
    client: &ApiClient,
    default_project: &Project,
    args: &TracePreviewTargetArgs,
) -> Result<TracePreviewTarget> {
    if let Some(url) = args.url.as_deref() {
        let parsed = parse_trace_url(url)?;
        let project_id = match parsed.project.as_deref() {
            Some(project) if project == default_project.id || project == default_project.name => {
                default_project.id.clone()
            }
            Some(project) if is_uuid_like(project) => project.to_string(),
            Some(project) => {
                get_project_by_name(client, project)
                    .await?
                    .ok_or_else(|| anyhow!("project '{project}' from trace URL not found"))?
                    .id
            }
            None => default_project.id.clone(),
        };
        let root_span_id = parsed
            .row_ref
            .or(parsed.span_id.clone())
            .ok_or_else(|| anyhow!("trace URL must include query parameter r or s"))?;
        return Ok(TracePreviewTarget {
            project_id,
            root_span_id,
            span_id: args.span_id.clone().or(parsed.span_id),
        });
    }

    let project_id = if let Some(object_ref) = args.object_ref.as_deref() {
        let (object_type, object_name) = object_ref.split_once(':').ok_or_else(|| {
            anyhow!("invalid --object-ref '{object_ref}', expected project_logs:<project_id>")
        })?;
        if object_type != "project_logs" {
            bail!("bt views preview currently supports project_logs object refs");
        }
        object_name.to_string()
    } else {
        args.project_id
            .clone()
            .unwrap_or_else(|| default_project.id.clone())
    };
    let root_span_id = args
        .trace_id
        .clone()
        .ok_or_else(|| anyhow!("trace preview requires --trace-id or --url"))?;
    Ok(TracePreviewTarget {
        project_id,
        root_span_id,
        span_id: args.span_id.clone(),
    })
}

#[derive(Debug)]
struct ParsedPreviewTraceUrl {
    project: Option<String>,
    row_ref: Option<String>,
    span_id: Option<String>,
}

fn parse_trace_url(input: &str) -> Result<ParsedPreviewTraceUrl> {
    let parsed_url = Url::parse(input)
        .or_else(|_| Url::parse(&format!("https://{}", input.trim_start_matches('/'))))
        .context("invalid trace URL")?;
    let mut parsed = ParsedPreviewTraceUrl {
        project: None,
        row_ref: None,
        span_id: None,
    };
    if let Some(segments) = parsed_url.path_segments() {
        let parts = segments.filter(|part| !part.is_empty()).collect::<Vec<_>>();
        if parts.len() >= 4 && parts[0] == "app" && parts[2] == "p" {
            parsed.project = Some(parts[3].to_string());
        }
    }
    for (key, value) in parsed_url.query_pairs() {
        match key.as_ref() {
            "r" if !value.is_empty() => parsed.row_ref = Some(value.to_string()),
            "s" if !value.is_empty() => parsed.span_id = Some(value.to_string()),
            _ => {}
        }
    }
    Ok(parsed)
}

fn is_uuid_like(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (idx, b) in bytes.iter().enumerate() {
        let is_hyphen = matches!(idx, 8 | 13 | 18 | 23);
        if is_hyphen {
            if *b != b'-' {
                return false;
            }
        } else if !(*b as char).is_ascii_hexdigit() {
            return false;
        }
    }
    true
}

async fn fetch_trace_rows(
    client: &ApiClient,
    project_id: &str,
    root_span_id: &str,
    limit: usize,
    print_queries: bool,
) -> Result<Vec<Map<String, Value>>> {
    let query = format!(
        "select: * | from: project_logs({}) spans | filter: root_span_id = {} | preview_length: 125 | sort: _pagination_key ASC | limit: {}",
        sql_quote(project_id),
        sql_quote(root_span_id),
        limit,
    );
    maybe_print_query(print_queries, "custom-view-preview-trace", &query);
    execute_query(client, &query)
        .await
        .with_context(|| format!("BTQL query failed: {query}"))
        .map(|response| response.data)
}

async fn fetch_full_span_row(
    client: &ApiClient,
    project_id: &str,
    row_id: &str,
    print_queries: bool,
) -> Result<Option<Map<String, Value>>> {
    let query = format!(
        "select: * | from: project_logs({}) spans | filter: id = {} | preview_length: -1 | limit: 1",
        sql_quote(project_id),
        sql_quote(row_id),
    );
    maybe_print_query(print_queries, "custom-view-preview-span-fields", &query);
    execute_query(client, &query)
        .await
        .with_context(|| format!("BTQL query failed: {query}"))
        .map(|response| response.data.into_iter().next())
}

async fn execute_query(client: &ApiClient, query: &str) -> Result<BtqlResponse> {
    let body = json!({
        "query": query,
        "fmt": "json",
        "query_source": "bt_views_preview_7e8680d0f2484e2fbef8a61f2de0b9df",
    });
    let org_name = client.org_name();
    let headers = if org_name.is_empty() {
        Vec::new()
    } else {
        vec![("x-bt-org-name", org_name)]
    };
    client.post_with_headers("/btql", &body, &headers).await
}

fn maybe_print_query(enabled: bool, label: &str, query: &str) {
    if enabled {
        eprintln!("bt views [{label}] BTQL:\n{query}\n");
    }
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn build_trace_payload(
    rows: Vec<Map<String, Value>>,
    root_span_id: &str,
    selected_selector: Option<&str>,
) -> Result<(Value, Value)> {
    let mut spans = Map::new();
    let mut span_order = Vec::new();
    let mut parent_children: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut row_id_to_span_id = BTreeMap::new();

    for row in &rows {
        let Some(span_id) = row.get("span_id").and_then(Value::as_str) else {
            continue;
        };
        span_order.push(Value::String(span_id.to_string()));
        if let Some(row_id) = row.get("id").and_then(Value::as_str) {
            row_id_to_span_id.insert(row_id.to_string(), span_id.to_string());
        }
        if let Some(parent) = parent_span_id(row) {
            parent_children
                .entry(parent)
                .or_default()
                .push(span_id.to_string());
        }
    }

    for row in rows {
        let Some(span_id) = row.get("span_id").and_then(Value::as_str) else {
            continue;
        };
        let span_id = span_id.to_string();
        let children = parent_children
            .get(&span_id)
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .map(Value::String)
            .collect::<Vec<_>>();
        spans.insert(span_id.clone(), row_to_custom_span(row, children));
    }

    let selected_span_id = selected_selector
        .and_then(|selector| {
            spans
                .contains_key(selector)
                .then(|| selector.to_string())
                .or_else(|| row_id_to_span_id.get(selector).cloned())
        })
        .or_else(|| {
            spans
                .contains_key(root_span_id)
                .then(|| root_span_id.to_string())
        })
        .or_else(|| {
            span_order
                .first()
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
        .ok_or_else(|| anyhow!("trace has no selectable spans"))?;
    let selected_span = spans
        .get(&selected_span_id)
        .cloned()
        .ok_or_else(|| anyhow!("selected span '{selected_span_id}' not found"))?;

    Ok((
        json!({
            "rootSpanId": root_span_id,
            "selectedSpanId": selected_span_id,
            "spanOrder": span_order,
            "spans": spans,
        }),
        selected_span,
    ))
}

fn row_to_custom_span(row: Map<String, Value>, children: Vec<Value>) -> Value {
    let data_fields = [
        "input",
        "output",
        "expected",
        "metadata",
        "scores",
        "metrics",
        "error",
        "tags",
        "span_attributes",
    ];
    let mut data = Map::new();
    for field in data_fields {
        if let Some(value) = row.get(field) {
            data.insert(field.to_string(), value.clone());
        }
    }

    let mut span = Map::new();
    for field in ["id", "span_id", "root_span_id", "parent_span_id"] {
        if let Some(value) = row.get(field) {
            span.insert(field.to_string(), value.clone());
        }
    }
    if let Some(value) = row.get("span_parents") {
        span.insert("span_parents".to_string(), value.clone());
    }
    span.insert("data".to_string(), Value::Object(data));
    span.insert("children".to_string(), Value::Array(children));
    Value::Object(span)
}

fn parent_span_id(row: &Map<String, Value>) -> Option<String> {
    row.get("parent_span_id")
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            row.get("span_parents")
                .and_then(Value::as_array)
                .and_then(|parents| parents.last())
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
        })
}

fn resolve_preview_row_id(spans: &Map<String, Value>, requested: &str) -> Option<String> {
    spans
        .get(requested)
        .and_then(|span| span.get("id"))
        .and_then(Value::as_str)
        .map(ToOwned::to_owned)
        .or_else(|| {
            spans.values().find_map(|span| {
                let row_id = span.get("id").and_then(Value::as_str)?;
                (row_id == requested).then(|| row_id.to_string())
            })
        })
}

fn fields_from_row(row: Map<String, Value>, fields: Option<&[String]>) -> Value {
    let requested = fields
        .map(|fields| fields.iter().map(String::as_str).collect::<Vec<_>>())
        .unwrap_or_else(|| vec!["input", "output", "expected", "metadata"]);
    let mut object = Map::new();
    for field in requested {
        if matches!(field, "input" | "output" | "expected" | "metadata") {
            if let Some(value) = row.get(field) {
                object.insert(field.to_string(), value.clone());
            }
        }
    }
    Value::Object(object)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_view_files() {
        assert!(is_view_file(Path::new("conversation.view.tsx")));
        assert!(is_view_file(Path::new("dataset.view.js")));
        assert!(!is_view_file(Path::new("regular.tsx")));
    }

    #[test]
    fn builds_inline_custom_view_insert_event() {
        let view = PreparedView {
            source_file: "test.view.tsx".to_string(),
            entry: ViewManifestEntry {
                view_type: ViewType::Trace,
                name: "Test View".to_string(),
                slug: "test-view".to_string(),
                code: "module.exports = function View() {}".to_string(),
                description: Some("desc".to_string()),
                project_id: None,
                project_name: None,
                dataset_id: None,
                dataset_name: None,
                metadata: Some(json!({ "kind": "test" })),
                tags: Some(vec!["review".to_string()]),
            },
            project: Project {
                id: "proj_test".to_string(),
                name: "test-project".to_string(),
                org_id: "org_test".to_string(),
                description: None,
            },
            dataset: None,
        };

        let event = build_insert_event(&view, IfExistsMode::Replace);
        assert_eq!(event["function_type"], "custom_view");
        assert_eq!(event["if_exists"], "replace");
        assert_eq!(event["function_data"]["type"], "code");
        assert_eq!(event["function_data"]["data"]["type"], "inline");
        assert_eq!(
            event["function_data"]["data"]["runtime_context"]["runtime"],
            "browser"
        );
        assert!(event.get("origin").is_none());
    }

    #[test]
    fn dataset_insert_event_sets_origin() {
        let view = PreparedView {
            source_file: "dataset.view.tsx".to_string(),
            entry: ViewManifestEntry {
                view_type: ViewType::Dataset,
                name: "Dataset View".to_string(),
                slug: "dataset-view".to_string(),
                code: "module.exports = function View() {}".to_string(),
                description: None,
                project_id: None,
                project_name: None,
                dataset_id: Some("dataset_test".to_string()),
                dataset_name: None,
                metadata: None,
                tags: None,
            },
            project: Project {
                id: "proj_test".to_string(),
                name: "test-project".to_string(),
                org_id: "org_test".to_string(),
                description: None,
            },
            dataset: Some(datasets_api::Dataset {
                id: "dataset_test".to_string(),
                name: "test-dataset".to_string(),
                project_id: Some("proj_test".to_string()),
                description: None,
                created: None,
                created_at: None,
                metadata: None,
            }),
        };

        let event = build_insert_event(&view, IfExistsMode::Error);
        assert_eq!(event["origin"]["object_type"], "dataset");
        assert_eq!(event["origin"]["object_id"], "dataset_test");
    }
}
