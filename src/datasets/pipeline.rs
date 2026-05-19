use std::collections::HashMap;
use std::fs;
use std::io::{self, BufRead, BufReader, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use indicatif::{ProgressBar, ProgressStyle};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::args::BaseArgs;
use crate::auth::{login, resolved_runner_env, LoginContext};
use crate::http::ApiClient;
use crate::js_runner::{build_js_runner_command, materialize_runner_script};
use crate::projects::api::{create_project, get_project_by_name, Project};
use crate::python_runner;
use crate::runner_sse;
use crate::source_language::{classify_runtime_extension, SourceLanguage};
use crate::sync::discovery::{
    discover_project_log_refs, ProjectLogRefDiscoveryResult, ProjectLogRefScope,
};
use crate::sync::{
    artifact_base_dir, artifact_spec_dir, create_jsonl_file_writer, epoch_seconds, read_json_file,
    read_jsonl_values, stable_spec_hash, write_json_atomic, write_jsonl_value, SyncPushFileArgs,
};
use tokio::sync::mpsc;

use super::{api as datasets_api, records, utils, ResolvedContext};

const RUNNER_FILE: &str = "dataset-pipeline-runner.ts";
const RUNNER_SOURCE: &str = include_str!("../../scripts/dataset-pipeline-runner.ts");
const PY_RUNNER_FILE: &str = "dataset-pipeline-runner.py";
const PY_RUNNER_SOURCE: &str = include_str!("../../scripts/dataset-pipeline-runner.py");
const PIPELINE_ARTIFACT_OBJECT_TYPE: &str = "dataset_pipeline";
const PIPELINE_ARTIFACT_SCHEMA_VERSION: u32 = 1;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Use `run` to run the whole pipeline.

For staged workflows, run `pull`, then `transform`, inspect or edit the transformed JSONL, then upload it with:
  bt datasets pipeline push ./pipeline.ts

`push` reads the pipeline target and delegates to `bt sync push`.
")]
pub struct PipelineArgs {
    #[command(subcommand)]
    command: PipelineCommands,
}

#[derive(Debug, Clone, Subcommand)]
enum PipelineCommands {
    /// Pull, transform, and insert dataset rows
    Run(PipelineRunArgs),
    /// Pull source trace/span refs to JSONL
    Pull(PipelineFetchArgs),
    /// Transform candidate JSONL into proposed dataset row JSONL
    Transform(PipelineTransformArgs),
    /// Push transformed dataset rows to the pipeline target
    Push(PipelinePushArgs),
}

#[derive(Debug, Clone, Args)]
struct PipelineRunnerArgs {
    /// Dataset pipeline file to execute
    #[arg(value_name = "PIPELINE")]
    pipeline: PathBuf,

    /// Pipeline name, required when the file defines multiple pipelines
    #[arg(long)]
    name: Option<String>,

    /// Runner binary (e.g. tsx, vite-node, ts-node, python)
    #[arg(
        long,
        short = 'r',
        env = "BT_DATASET_PIPELINE_RUNNER",
        value_name = "RUNNER"
    )]
    runner: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct PipelineSourceArgs {
    /// Override the source project name from the pipeline file
    #[arg(long = "source-project")]
    source_project: Option<String>,

    /// Override the source project id from the pipeline file
    #[arg(long = "source-project-id")]
    source_project_id: Option<String>,

    /// Override the source org name from the pipeline file
    #[arg(long = "source-org")]
    source_org: Option<String>,

    /// Override the source filter from the pipeline file
    #[arg(long = "source-filter")]
    source_filter: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct PipelineTargetArgs {
    /// Override the target project name from the pipeline file
    #[arg(long = "target-project")]
    target_project: Option<String>,

    /// Override the target project id from the pipeline file
    #[arg(long = "target-project-id")]
    target_project_id: Option<String>,

    /// Override the target org name from the pipeline file
    #[arg(long = "target-org")]
    target_org: Option<String>,

    /// Override the target dataset name from the pipeline file
    #[arg(long = "target-dataset")]
    target_dataset: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct PipelineFetchOptions {
    /// Maximum number of source refs to discover
    #[arg(
        long,
        alias = "target",
        default_value_t = 100,
        value_parser = parse_positive_usize
    )]
    limit: usize,

    /// Restrict the source query to one or more root span ids
    #[arg(long = "root-span-id")]
    root_span_ids: Vec<String>,

    /// Page size for discovery BTQL pagination
    #[arg(long, default_value_t = 1000, value_parser = parse_positive_usize)]
    page_size: usize,
}

#[derive(Debug, Clone, Args)]
struct PipelineTransformOptions {
    /// Maximum concurrent transform calls. Defaults to the logical CPU count.
    #[arg(long, value_parser = parse_positive_usize)]
    max_concurrency: Option<usize>,
}

impl PipelineTransformOptions {
    fn max_concurrency(&self) -> usize {
        self.max_concurrency
            .unwrap_or_else(default_transform_concurrency)
    }
}

#[derive(Debug, Clone, Args)]
struct PipelineArtifactArgs {
    /// Root directory for pipeline artifacts.
    #[arg(long, default_value = "bt-sync")]
    root: PathBuf,
}

#[derive(Debug, Clone, Args)]
struct PipelineRunArgs {
    #[command(flatten)]
    runner: PipelineRunnerArgs,

    #[command(flatten)]
    source: PipelineSourceArgs,

    #[command(flatten)]
    target: PipelineTargetArgs,

    #[command(flatten)]
    fetch: PipelineFetchOptions,

    #[command(flatten)]
    transform: PipelineTransformOptions,
}

#[derive(Debug, Clone, Args)]
struct PipelineFetchArgs {
    #[command(flatten)]
    runner: PipelineRunnerArgs,

    #[command(flatten)]
    artifacts: PipelineArtifactArgs,

    #[command(flatten)]
    source: PipelineSourceArgs,

    #[command(flatten)]
    fetch: PipelineFetchOptions,

    /// Output JSONL file. Defaults to a managed path under --root.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
struct PipelineTransformArgs {
    #[command(flatten)]
    runner: PipelineRunnerArgs,

    #[command(flatten)]
    artifacts: PipelineArtifactArgs,

    #[command(flatten)]
    source: PipelineSourceArgs,

    #[command(flatten)]
    transform: PipelineTransformOptions,

    /// Input candidate JSONL file. Defaults to the latest pull output under --root.
    #[arg(long = "in")]
    input: Option<PathBuf>,

    /// Output proposed dataset row JSONL file. Defaults to a managed path under --root.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
struct PipelinePushArgs {
    #[command(flatten)]
    runner: PipelineRunnerArgs,

    #[command(flatten)]
    artifacts: PipelineArtifactArgs,

    #[command(flatten)]
    target: PipelineTargetArgs,

    /// Input transformed dataset row JSONL file. Defaults to the latest transform output under --root.
    #[arg(long = "in")]
    input: Option<PathBuf>,

    /// Ignore previous sync push state and upload from the beginning.
    #[arg(long)]
    fresh: bool,
}

pub async fn run(base: BaseArgs, args: PipelineArgs) -> Result<()> {
    match args.command {
        PipelineCommands::Run(args) => {
            let inspect = inspect_with_overrides(
                inspect_pipeline(&base, &args.runner).await?,
                Some(&args.source),
                Some(&args.target),
            );
            let tempdir =
                tempfile::tempdir().context("failed to create dataset pipeline temp dir")?;
            let refs_path = tempdir.path().join("discovered.jsonl");
            print_pipeline_status(&base, "Fetching source refs...");
            let fetch_result = discover_refs(&base, &inspect, &args.fetch, &refs_path).await?;
            print_pipeline_status(
                &base,
                format!(
                    "Fetched {} source ref(s) across {} page(s).",
                    fetch_result.refs, fetch_result.pages
                ),
            );

            let refs = read_jsonl_values(&refs_path)?;
            let source_project = resolve_pipeline_source_project(&base, &inspect.source).await?;
            let attachment_dir = tempdir.path().join("attachments");
            let transform_response = transform_source_refs(
                &base,
                &args.runner,
                &source_project.id,
                &inspect.source,
                refs,
                args.transform.max_concurrency(),
                Some(&attachment_dir),
                None,
            )
            .await?;
            let row_count = transform_response.rows.len();
            let inserted =
                upload_dataset_rows(&base, &inspect.target, transform_response.rows).await?;
            print_summary(
                &base,
                json!({
                    "refs": transform_response.candidates,
                    "rows": row_count,
                    "inserted": inserted,
                }),
                false,
            )
        }
        PipelineCommands::Pull(args) => {
            let inspect = inspect_with_overrides(
                inspect_pipeline(&base, &args.runner).await?,
                Some(&args.source),
                None,
            );
            fetch_refs(&base, args, inspect).await
        }
        PipelineCommands::Transform(args) => transform_refs(&base, args).await,
        PipelineCommands::Push(args) => push_rows(&base, args).await,
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PipelineInspect {
    source: PipelineSourceInspect,
    target: PipelineTargetInspect,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PipelineSourceInspect {
    #[serde(skip_serializing_if = "Option::is_none")]
    project_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    project_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    org_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    filter: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    scope: Option<PipelineScope>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct PipelineTargetInspect {
    project_id: Option<String>,
    project_name: Option<String>,
    org_name: Option<String>,
    dataset_name: String,
    description: Option<String>,
    metadata: Option<Value>,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
enum PipelineScope {
    Span,
    Trace,
}

impl PipelineScope {
    fn from_source(source: &PipelineSourceInspect) -> Self {
        source.scope.unwrap_or(PipelineScope::Span)
    }
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PipelineTransformResponse {
    candidates: usize,
    row_count: usize,
    rows: Vec<Value>,
}

#[derive(Debug)]
enum PipelineRunnerEvent {
    Response(Value),
    Progress(PipelineProgressEvent),
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

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PipelineProgressEvent {
    #[serde(rename = "type")]
    kind_type: String,
    kind: String,
    #[serde(default)]
    rows: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct PipelineRunnerErrorPayload {
    message: String,
    #[serde(default)]
    stack: Option<String>,
    #[serde(default)]
    status: Option<u16>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PipelineArtifactStage {
    Fetch,
    Transform,
}

impl PipelineArtifactStage {
    fn command(self) -> &'static str {
        match self {
            PipelineArtifactStage::Fetch => "pull",
            PipelineArtifactStage::Transform => "transform",
        }
    }

    fn output_file(self) -> &'static str {
        match self {
            PipelineArtifactStage::Fetch => "fetched.jsonl",
            PipelineArtifactStage::Transform => "transformed.jsonl",
        }
    }

    fn spec_file(self) -> &'static str {
        match self {
            PipelineArtifactStage::Fetch => "fetch.spec.json",
            PipelineArtifactStage::Transform => "transform.spec.json",
        }
    }

    fn manifest_file(self) -> &'static str {
        match self {
            PipelineArtifactStage::Fetch => "fetch.manifest.json",
            PipelineArtifactStage::Transform => "transform.manifest.json",
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PipelineFetchArtifactOptions {
    limit: usize,
    root_span_ids: Vec<String>,
    page_size: usize,
}

impl From<&PipelineFetchOptions> for PipelineFetchArtifactOptions {
    fn from(options: &PipelineFetchOptions) -> Self {
        Self {
            limit: options.limit,
            root_span_ids: options.root_span_ids.clone(),
            page_size: options.page_size,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PipelineTransformArtifactOptions {
    max_concurrency: usize,
}

impl From<&PipelineTransformOptions> for PipelineTransformArtifactOptions {
    fn from(options: &PipelineTransformOptions) -> Self {
        Self {
            max_concurrency: options.max_concurrency(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PipelineArtifactSpec {
    schema_version: u32,
    kind: String,
    pipeline: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cli_project: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cli_org: Option<String>,
    stage: PipelineArtifactStage,
    #[serde(skip_serializing_if = "Option::is_none")]
    source: Option<PipelineSourceInspect>,
    #[serde(skip_serializing_if = "Option::is_none")]
    target: Option<PipelineTargetInspect>,
    #[serde(skip_serializing_if = "Option::is_none")]
    fetch: Option<PipelineFetchArtifactOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transform: Option<PipelineTransformArtifactOptions>,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_path: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
enum PipelineArtifactStatus {
    Completed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PipelineArtifactManifest {
    schema_version: u32,
    spec_hash: String,
    spec: PipelineArtifactSpec,
    status: PipelineArtifactStatus,
    stage: PipelineArtifactStage,
    #[serde(skip_serializing_if = "Option::is_none")]
    input_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    output_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    refs: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    candidates: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    rows: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pages: Option<usize>,
    started_at: u64,
    updated_at: u64,
    completed_at: Option<u64>,
}

#[derive(Debug, Clone)]
struct PipelineOutputArtifact {
    spec_hash: String,
    spec: PipelineArtifactSpec,
    stage: PipelineArtifactStage,
    spec_dir: PathBuf,
    output_path: PathBuf,
}

async fn inspect_pipeline(base: &BaseArgs, runner: &PipelineRunnerArgs) -> Result<PipelineInspect> {
    run_runner_json(base, "inspect", runner, None, |event| {
        handle_pipeline_runner_event(None, event);
    })
    .await
}

fn inspect_with_overrides(
    mut inspect: PipelineInspect,
    source: Option<&PipelineSourceArgs>,
    target: Option<&PipelineTargetArgs>,
) -> PipelineInspect {
    if let Some(source) = source {
        apply_source_overrides(&mut inspect.source, source);
    }
    if let Some(target) = target {
        apply_target_overrides(&mut inspect.target, target);
    }
    inspect
}

fn apply_source_overrides(source: &mut PipelineSourceInspect, args: &PipelineSourceArgs) {
    if let Some(project_name) = args.source_project.as_deref() {
        source.project_name = Some(project_name.to_string());
        source.project_id = None;
    }
    if let Some(project_id) = args.source_project_id.as_deref() {
        source.project_id = Some(project_id.to_string());
    }
    if let Some(org_name) = args.source_org.as_deref() {
        source.org_name = Some(org_name.to_string());
    }
    if let Some(filter) = args.source_filter.as_deref() {
        source.filter = Some(filter.to_string());
    }
}

fn source_with_resolved_project(
    source: &PipelineSourceInspect,
    project: &Project,
) -> PipelineSourceInspect {
    let mut source = source.clone();
    source.project_id = Some(project.id.clone());
    source.project_name = Some(project.name.clone());
    source
}

fn apply_target_overrides(target: &mut PipelineTargetInspect, args: &PipelineTargetArgs) {
    if let Some(project_name) = args.target_project.as_deref() {
        target.project_name = Some(project_name.to_string());
        target.project_id = None;
    }
    if let Some(project_id) = args.target_project_id.as_deref() {
        target.project_id = Some(project_id.to_string());
    }
    if let Some(org_name) = args.target_org.as_deref() {
        target.org_name = Some(org_name.to_string());
    }
    if let Some(dataset_name) = args.target_dataset.as_deref() {
        target.dataset_name = dataset_name.to_string();
    }
}

async fn build_runner_command<F>(
    base: &BaseArgs,
    stage: &'static str,
    runner: &PipelineRunnerArgs,
    configure: F,
) -> Result<Command>
where
    F: FnOnce(&mut Command, &'static str) -> Result<()>,
{
    let pipeline_file = runner.pipeline.clone();
    let files = vec![pipeline_file.clone()];
    let mut command = build_pipeline_runner_command(runner, &pipeline_file, &files)?;

    command.envs(resolved_runner_env(base).await?);
    command.env("BT_DATASET_PIPELINE_STAGE", stage);
    if let Some(name) = runner.name.as_deref() {
        command.env("BT_DATASET_PIPELINE_NAME", name);
    }
    configure(&mut command, stage)?;
    Ok(command)
}

fn build_pipeline_runner_command(
    runner: &PipelineRunnerArgs,
    pipeline_file: &Path,
    files: &[PathBuf],
) -> Result<Command> {
    match pipeline_language(pipeline_file)? {
        SourceLanguage::JsLike => {
            let runner_script = materialize_dataset_pipeline_runner(RUNNER_FILE, RUNNER_SOURCE)?;
            Ok(build_js_runner_command(
                runner.runner.as_deref(),
                &runner_script,
                files,
            ))
        }
        SourceLanguage::Python => {
            let runner_script =
                materialize_dataset_pipeline_runner(PY_RUNNER_FILE, PY_RUNNER_SOURCE)?;
            let python = python_runner::resolve_python_interpreter_for_roots(
                runner.runner.as_deref(),
                &["BT_DATASET_PIPELINE_PYTHON"],
                files,
            )
            .context("No Python interpreter found. Install python, create a virtualenv, or pass --runner.")?;
            let mut command = Command::new(python);
            command.arg(runner_script).arg(pipeline_file);
            Ok(command)
        }
    }
}

fn materialize_dataset_pipeline_runner(file_name: &str, source: &str) -> Result<PathBuf> {
    materialize_runner_script(&dataset_pipeline_runner_cache_dir(), file_name, source)
}

fn dataset_pipeline_runner_cache_dir() -> PathBuf {
    let root = std::env::var_os("XDG_CACHE_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))
        .unwrap_or_else(std::env::temp_dir);

    root.join("bt")
        .join("dataset-pipeline-runners")
        .join(env!("CARGO_PKG_VERSION"))
}

fn pipeline_language(pipeline_file: &Path) -> Result<SourceLanguage> {
    let extension = pipeline_file
        .extension()
        .and_then(|extension| extension.to_str())
        .with_context(|| {
            format!(
                "dataset pipeline file '{}' has no extension",
                pipeline_file.display()
            )
        })?;
    classify_runtime_extension(extension).with_context(|| {
        format!(
            "unsupported dataset pipeline file extension '.{extension}'; expected .ts, .tsx, .js, .jsx, or .py"
        )
    })
}

async fn fetch_refs(
    base: &BaseArgs,
    args: PipelineFetchArgs,
    mut inspect: PipelineInspect,
) -> Result<()> {
    let source_project = resolve_pipeline_source_project(base, &inspect.source).await?;
    inspect.source = source_with_resolved_project(&inspect.source, &source_project);
    let spec = pipeline_fetch_artifact_spec(base, &args.runner, &inspect.source, &args.fetch);
    let artifact = resolve_pipeline_output_artifact(
        &args.artifacts.root,
        &args.runner,
        spec,
        args.out.as_deref(),
        None,
    )?;
    artifact.write_spec()?;
    let started_at = epoch_seconds();
    let result = discover_refs(base, &inspect, &args.fetch, &artifact.output_path).await?;
    artifact.write_manifest(PipelineArtifactManifest {
        schema_version: PIPELINE_ARTIFACT_SCHEMA_VERSION,
        spec_hash: artifact.spec_hash.clone(),
        spec: artifact.spec.clone(),
        status: PipelineArtifactStatus::Completed,
        stage: PipelineArtifactStage::Fetch,
        input_path: None,
        output_path: Some(artifact.output_path.display().to_string()),
        refs: Some(result.refs),
        candidates: None,
        rows: None,
        pages: Some(result.pages),
        started_at,
        updated_at: epoch_seconds(),
        completed_at: Some(epoch_seconds()),
    })?;
    print_summary(
        base,
        json!({
            "refs": result.refs,
            "pages": result.pages,
            "scope": match PipelineScope::from_source(&inspect.source) { PipelineScope::Trace => "trace", PipelineScope::Span => "span" },
            "source_project": source_project.name,
            "source_project_id": source_project.id,
            "out": artifact.output_path.display().to_string(),
        }),
        false,
    )
}

fn pipeline_fetch_artifact_spec(
    base: &BaseArgs,
    runner: &PipelineRunnerArgs,
    source: &PipelineSourceInspect,
    options: &PipelineFetchOptions,
) -> PipelineArtifactSpec {
    base_pipeline_artifact_spec(base, runner, PipelineArtifactStage::Fetch)
        .with_source(source.clone())
        .with_fetch(options.into())
}

fn pipeline_transform_artifact_spec(
    base: &BaseArgs,
    runner: &PipelineRunnerArgs,
    source: &PipelineSourceInspect,
    options: &PipelineTransformOptions,
    input_path: &Path,
) -> PipelineArtifactSpec {
    base_pipeline_artifact_spec(base, runner, PipelineArtifactStage::Transform)
        .with_source(source.clone())
        .with_transform(options.into())
        .with_input_path(input_path)
}

fn base_pipeline_artifact_spec(
    base: &BaseArgs,
    runner: &PipelineRunnerArgs,
    stage: PipelineArtifactStage,
) -> PipelineArtifactSpec {
    PipelineArtifactSpec {
        schema_version: PIPELINE_ARTIFACT_SCHEMA_VERSION,
        kind: PIPELINE_ARTIFACT_OBJECT_TYPE.to_string(),
        pipeline: runner.pipeline.display().to_string(),
        name: runner.name.clone(),
        cli_project: base.project.clone(),
        cli_org: base.org_name.clone(),
        stage,
        source: None,
        target: None,
        fetch: None,
        transform: None,
        input_path: None,
    }
}

impl PipelineArtifactSpec {
    fn with_source(mut self, source: PipelineSourceInspect) -> Self {
        self.source = Some(source);
        self
    }

    fn with_fetch(mut self, fetch: PipelineFetchArtifactOptions) -> Self {
        self.fetch = Some(fetch);
        self
    }

    fn with_transform(mut self, transform: PipelineTransformArtifactOptions) -> Self {
        self.transform = Some(transform);
        self
    }

    fn with_input_path(mut self, input_path: &Path) -> Self {
        self.input_path = Some(input_path.display().to_string());
        self
    }
}

fn resolve_pipeline_output_artifact(
    root: &Path,
    runner: &PipelineRunnerArgs,
    spec: PipelineArtifactSpec,
    explicit_out: Option<&Path>,
    input_path: Option<&Path>,
) -> Result<PipelineOutputArtifact> {
    let spec_hash = stable_spec_hash(&spec)?;
    let stage = spec.stage;
    let hashed_spec_dir = artifact_spec_dir(
        root,
        PIPELINE_ARTIFACT_OBJECT_TYPE,
        &pipeline_artifact_name(runner),
        &spec_hash,
    );
    let spec_dir = if matches!(stage, PipelineArtifactStage::Fetch) {
        hashed_spec_dir
    } else {
        input_path
            .and_then(Path::parent)
            .filter(|parent| !parent.as_os_str().is_empty())
            .map(Path::to_path_buf)
            .unwrap_or(hashed_spec_dir)
    };
    let output_path = explicit_out
        .map(Path::to_path_buf)
        .unwrap_or_else(|| spec_dir.join(stage.output_file()));
    Ok(PipelineOutputArtifact {
        spec_hash,
        spec,
        stage,
        spec_dir,
        output_path,
    })
}

fn resolve_pipeline_input_path(
    explicit_input: &Option<PathBuf>,
    root: &Path,
    runner: &PipelineRunnerArgs,
    stage: PipelineArtifactStage,
) -> Result<PathBuf> {
    if let Some(input) = explicit_input {
        Ok(input.clone())
    } else {
        resolve_latest_pipeline_stage_output(root, runner, stage)
    }
}

fn read_pipeline_stage_manifest_for_output(
    output_path: &Path,
    stage: PipelineArtifactStage,
) -> Result<Option<PipelineArtifactManifest>> {
    let Some(parent) = output_path.parent() else {
        return Ok(None);
    };
    let manifest_path = parent.join(stage.manifest_file());
    if !manifest_path.exists() {
        return Ok(None);
    }
    let manifest = read_json_file::<PipelineArtifactManifest>(&manifest_path)
        .with_context(|| format!("failed to read {}", manifest_path.display()))?;
    if manifest.stage != stage || manifest.status != PipelineArtifactStatus::Completed {
        return Ok(None);
    }
    Ok(Some(manifest))
}

fn base_with_pipeline_artifact_context(
    base: &BaseArgs,
    manifest: Option<&PipelineArtifactManifest>,
) -> BaseArgs {
    let mut base = base.clone();
    if let Some(spec) = manifest.map(|manifest| &manifest.spec) {
        if base.project.is_none() {
            base.project = spec.cli_project.clone();
        }
        if base.org_name.is_none() {
            base.org_name = spec.cli_org.clone();
        }
    }
    base
}

fn resolve_latest_pipeline_stage_output(
    root: &Path,
    runner: &PipelineRunnerArgs,
    stage: PipelineArtifactStage,
) -> Result<PathBuf> {
    let base = artifact_base_dir(
        root,
        PIPELINE_ARTIFACT_OBJECT_TYPE,
        &pipeline_artifact_name(runner),
    );
    let mut best: Option<(u64, PathBuf)> = None;
    if base.is_dir() {
        for entry in
            fs::read_dir(&base).with_context(|| format!("failed to read {}", base.display()))?
        {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let manifest_path = entry.path().join(stage.manifest_file());
            if !manifest_path.exists() {
                continue;
            }
            let manifest = read_json_file::<PipelineArtifactManifest>(&manifest_path)?;
            if manifest.stage != stage || manifest.status != PipelineArtifactStatus::Completed {
                continue;
            }
            let Some(output_path) = manifest
                .output_path
                .as_ref()
                .map(PathBuf::from)
                .filter(|path| path.exists())
            else {
                continue;
            };
            if best
                .as_ref()
                .map(|(best_time, _)| manifest.updated_at > *best_time)
                .unwrap_or(true)
            {
                best = Some((manifest.updated_at, output_path));
            }
        }
    }

    best.map(|(_, path)| path).ok_or_else(|| {
        anyhow::anyhow!(
            "no completed dataset pipeline {} output found for '{}'. run `bt datasets pipeline {} {}` first or pass --in",
            stage.command(),
            pipeline_artifact_name(runner),
            stage.command(),
            runner.pipeline.display()
        )
    })
}

fn pipeline_artifact_name(runner: &PipelineRunnerArgs) -> String {
    runner
        .name
        .clone()
        .or_else(|| {
            runner
                .pipeline
                .file_stem()
                .and_then(|stem| stem.to_str())
                .map(ToString::to_string)
        })
        .unwrap_or_else(|| "pipeline".to_string())
}

impl PipelineOutputArtifact {
    fn write_spec(&self) -> Result<()> {
        write_json_atomic(&self.spec_dir.join(self.stage.spec_file()), &self.spec)
    }

    fn write_manifest(&self, manifest: PipelineArtifactManifest) -> Result<()> {
        write_json_atomic(&self.spec_dir.join(self.stage.manifest_file()), &manifest)
    }
}

async fn run_runner_json<T, F>(
    base: &BaseArgs,
    stage: &'static str,
    runner: &PipelineRunnerArgs,
    request: Option<&Value>,
    mut on_event: F,
) -> Result<T>
where
    T: DeserializeOwned,
    F: FnMut(PipelineRunnerEvent),
{
    let mut command = build_runner_command(base, stage, runner, |_, _| Ok(())).await?;
    let (listener, sse_guard) = runner_sse::bind_sse_listener("bt-dataset-pipeline")?;
    let (tx, rx) = mpsc::unbounded_channel::<PipelineRunnerEvent>();
    let sse_connected = Arc::new(AtomicBool::new(false));

    let tx_sse = tx.clone();
    let sse_connected_for_task = Arc::clone(&sse_connected);
    let mut sse_task = tokio::spawn(async move {
        if let Err(err) = runner_sse::accept_and_read_sse_stream(
            listener,
            || {
                sse_connected_for_task.store(true, Ordering::Relaxed);
            },
            |event, data| {
                handle_pipeline_sse_event(event, data, &tx_sse);
            },
        )
        .await
        {
            let _ = tx_sse.send(PipelineRunnerEvent::Error {
                message: format!("SSE stream error: {err}"),
                stack: None,
                status: None,
            });
        }
    });

    let (sse_env_name, sse_env_value) = sse_guard.env(
        "BT_DATASET_PIPELINE_SSE_SOCK",
        "BT_DATASET_PIPELINE_SSE_ADDR",
    );
    command.env(sse_env_name, sse_env_value);
    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::piped());

    let mut child = command
        .spawn()
        .context("failed to start dataset pipeline runner")?;
    if let Some(request) = request {
        let mut stdin = child
            .stdin
            .take()
            .context("dataset pipeline runner stdin was not available")?;
        serde_json::to_writer(&mut stdin, request)
            .context("failed to write dataset pipeline runner request")?;
        stdin
            .write_all(b"\n")
            .context("failed to finish dataset pipeline runner request")?;
    }

    if let Some(stdout) = child.stdout.take() {
        forward_blocking_stream(stdout, "stdout", tx.clone());
    }
    if let Some(stderr) = child.stderr.take() {
        forward_blocking_stream(stderr, "stderr", tx.clone());
    }
    drop(tx);

    let wait_task = tokio::task::spawn_blocking(move || child.wait());
    let mut response: Option<Value> = None;
    let mut errors = Vec::<String>::new();
    let wait = Box::pin(async move {
        wait_task
            .await
            .context("dataset pipeline runner wait task failed")?
            .context("dataset pipeline runner process failed")
    });
    let status = runner_sse::drive_runner_events(
        rx,
        wait,
        &mut sse_task,
        &sse_connected,
        "dataset pipeline runner exited without a status",
        |event| match event {
            PipelineRunnerEvent::Response(value) => {
                response = Some(value);
            }
            PipelineRunnerEvent::Error {
                message,
                stack,
                status: _,
            } => {
                errors.push(message.clone());
                if let Some(stack) = stack {
                    errors.push(stack);
                }
                on_event(PipelineRunnerEvent::Error {
                    message,
                    stack: None,
                    status: None,
                });
            }
            event => on_event(event),
        },
    )
    .await?;

    let _sse_guard = sse_guard;
    if !status.success() {
        let detail = if errors.is_empty() {
            String::new()
        } else {
            format!(": {}", errors.join("\n"))
        };
        bail!(
            "dataset pipeline runner failed with status {}{}",
            status,
            detail
        );
    }

    let response = response.context("dataset pipeline runner did not send a response")?;
    serde_json::from_value(response).context("failed to parse dataset pipeline runner response")
}

fn handle_pipeline_sse_event(
    event: Option<String>,
    data: String,
    tx: &mpsc::UnboundedSender<PipelineRunnerEvent>,
) {
    match event.unwrap_or_default().as_str() {
        "response" => {
            if let Ok(value) = serde_json::from_str::<Value>(&data) {
                let _ = tx.send(PipelineRunnerEvent::Response(value));
            }
        }
        "progress" => {
            if let Ok(progress) = serde_json::from_str::<PipelineProgressEvent>(&data) {
                if progress.kind_type == "dataset_pipeline_progress" {
                    let _ = tx.send(PipelineRunnerEvent::Progress(progress));
                }
            }
        }
        "error" => {
            if let Ok(payload) = serde_json::from_str::<PipelineRunnerErrorPayload>(&data) {
                let _ = tx.send(PipelineRunnerEvent::Error {
                    message: payload.message,
                    stack: payload.stack,
                    status: payload.status,
                });
            } else {
                let _ = tx.send(PipelineRunnerEvent::Error {
                    message: data,
                    stack: None,
                    status: None,
                });
            }
        }
        _ => {}
    }
}

fn forward_blocking_stream<T>(
    stream: T,
    name: &'static str,
    tx: mpsc::UnboundedSender<PipelineRunnerEvent>,
) where
    T: Read + Send + 'static,
{
    std::thread::spawn(move || {
        let lines = BufReader::new(stream).lines();
        for line in lines {
            match line {
                Ok(message) => {
                    let _ = tx.send(PipelineRunnerEvent::Console {
                        stream: name.to_string(),
                        message,
                    });
                }
                Err(err) => {
                    let _ = tx.send(PipelineRunnerEvent::Error {
                        message: format!("failed to read dataset pipeline runner {name}: {err}"),
                        stack: None,
                        status: None,
                    });
                    break;
                }
            }
        }
    });
}

async fn transform_refs(base: &BaseArgs, args: PipelineTransformArgs) -> Result<()> {
    let input_path = resolve_pipeline_input_path(
        &args.input,
        &args.artifacts.root,
        &args.runner,
        PipelineArtifactStage::Fetch,
    )?;
    let fetch_manifest =
        read_pipeline_stage_manifest_for_output(&input_path, PipelineArtifactStage::Fetch)?;
    let inspect = inspect_pipeline(base, &args.runner).await?;
    let mut source = fetch_manifest
        .as_ref()
        .and_then(|manifest| manifest.spec.source.clone())
        .unwrap_or(inspect.source);
    apply_source_overrides(&mut source, &args.source);
    let source_base = base_with_pipeline_artifact_context(base, fetch_manifest.as_ref());
    let source_project = resolve_pipeline_source_project(&source_base, &source).await?;
    let refs = read_jsonl_values(&input_path)?;
    let spec = pipeline_transform_artifact_spec(
        &source_base,
        &args.runner,
        &source,
        &args.transform,
        &input_path,
    );
    let artifact = resolve_pipeline_output_artifact(
        &args.artifacts.root,
        &args.runner,
        spec,
        args.out.as_deref(),
        Some(&input_path),
    )?;
    artifact.write_spec()?;
    let attachment_dir = artifact.spec_dir.join("attachments");
    let started_at = epoch_seconds();
    let mut writer = create_jsonl_file_writer(&artifact.output_path)?;
    let response = transform_source_refs(
        &source_base,
        &args.runner,
        &source_project.id,
        &source,
        refs,
        args.transform.max_concurrency(),
        Some(&attachment_dir),
        Some(&mut writer as &mut dyn Write),
    )
    .await?;
    writer.flush().context("failed to flush transform output")?;
    artifact.write_manifest(PipelineArtifactManifest {
        schema_version: PIPELINE_ARTIFACT_SCHEMA_VERSION,
        spec_hash: artifact.spec_hash.clone(),
        spec: artifact.spec.clone(),
        status: PipelineArtifactStatus::Completed,
        stage: PipelineArtifactStage::Transform,
        input_path: Some(input_path.display().to_string()),
        output_path: Some(artifact.output_path.display().to_string()),
        refs: None,
        candidates: Some(response.candidates),
        rows: Some(response.row_count),
        pages: None,
        started_at,
        updated_at: epoch_seconds(),
        completed_at: Some(epoch_seconds()),
    })?;
    let row_count = response.row_count;
    print_summary(
        base,
        json!({
            "candidates": response.candidates,
            "rows": row_count,
            "out": args
                .out
                .as_deref()
                .unwrap_or(&artifact.output_path)
                .display()
                .to_string(),
        }),
        false,
    )
}

async fn transform_source_refs(
    base: &BaseArgs,
    runner: &PipelineRunnerArgs,
    source_project_id: &str,
    source: &PipelineSourceInspect,
    refs: Vec<Value>,
    max_concurrency: usize,
    attachment_dir: Option<&Path>,
    mut row_writer: Option<&mut dyn Write>,
) -> Result<PipelineTransformResponse> {
    if let Some(attachment_dir) = attachment_dir {
        fs::create_dir_all(attachment_dir)
            .with_context(|| format!("failed to create {}", attachment_dir.display()))?;
    }
    let progress = pipeline_progress_bar(base, refs.len() as u64, "Transforming candidates");
    progress.set_message("output rows: 0");
    let mut combined = PipelineTransformResponse {
        candidates: 0,
        row_count: 0,
        rows: Vec::new(),
    };
    let batch_size = max_concurrency.max(1);
    let mut completed_candidates = 0usize;
    for batch in refs.chunks(batch_size) {
        let request = json!({
            "sourceProjectId": source_project_id,
            "source": source,
            "refs": batch,
            "attachmentDir": attachment_dir.map(|path| path.display().to_string()),
            "maxConcurrency": max_concurrency,
        });
        let mut completed_in_batch = 0usize;
        let mut batch_rows = 0usize;
        let base_row_count = combined.row_count;
        let response: PipelineTransformResponse = run_runner_json(
            base,
            "transform",
            runner,
            Some(&request),
            |event| match event {
                PipelineRunnerEvent::Progress(progress_event)
                    if progress_event.kind == "candidate" =>
                {
                    if completed_in_batch < batch.len() {
                        completed_in_batch += 1;
                        completed_candidates += 1;
                        batch_rows += progress_event.rows.unwrap_or(0);
                        progress.set_position(completed_candidates.min(refs.len()) as u64);
                    }
                    progress.set_message(format!("output rows: {}", base_row_count + batch_rows));
                }
                event => handle_pipeline_runner_event(Some(&progress), event),
            },
        )
        .await?;
        validate_transform_response(&response)?;
        if completed_in_batch < batch.len() {
            completed_candidates += batch.len() - completed_in_batch;
            progress.set_position(completed_candidates.min(refs.len()) as u64);
        }
        combined.candidates += response.candidates;
        combined.row_count += response.row_count;
        if let Some(writer) = row_writer.as_deref_mut() {
            for row in response.rows {
                write_jsonl_value(writer, &row).context("failed to write transform output row")?;
            }
            writer.flush().context("failed to flush transform output")?;
        } else {
            combined.rows.extend(response.rows);
        }
        progress.set_message(format!("output rows: {}", combined.row_count));
    }
    progress.finish_and_clear();
    Ok(combined)
}

fn pipeline_progress_bar(base: &BaseArgs, total: u64, label: &str) -> ProgressBar {
    if base.json || base.quiet || !io::stderr().is_terminal() {
        return ProgressBar::hidden();
    }
    let pb = ProgressBar::new(total);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner:.cyan} {prefix} [{bar:40.cyan/blue}] {pos}/{len} candidates ({percent:>3}%) | {msg}",
        )
        .unwrap(),
    );
    pb.set_prefix(label.to_string());
    pb.enable_steady_tick(Duration::from_millis(80));
    pb
}

fn handle_pipeline_runner_event(progress: Option<&ProgressBar>, event: PipelineRunnerEvent) {
    match event {
        PipelineRunnerEvent::Console { stream, message } => {
            let line = if stream == "stdout" {
                format!("[pipeline stdout] {message}")
            } else {
                message
            };
            if let Some(progress) = progress {
                progress.suspend(|| eprintln!("{line}"));
            } else {
                eprintln!("{line}");
            }
        }
        PipelineRunnerEvent::Error {
            message,
            stack,
            status,
        } => {
            let line = if let Some(status) = status {
                format!("dataset pipeline runner error ({status}): {message}")
            } else {
                format!("dataset pipeline runner error: {message}")
            };
            if let Some(progress) = progress {
                progress.suspend(|| {
                    eprintln!("{line}");
                    if let Some(stack) = stack {
                        eprintln!("{stack}");
                    }
                });
            } else {
                eprintln!("{line}");
                if let Some(stack) = stack {
                    eprintln!("{stack}");
                }
            }
        }
        PipelineRunnerEvent::Progress(_) | PipelineRunnerEvent::Response(_) => {}
    }
}

fn validate_transform_response(response: &PipelineTransformResponse) -> Result<()> {
    if response.row_count != response.rows.len() {
        bail!(
            "dataset pipeline runner response rowCount {} did not match rows length {}",
            response.row_count,
            response.rows.len()
        );
    }
    Ok(())
}

async fn push_rows(base: &BaseArgs, args: PipelinePushArgs) -> Result<()> {
    let inspect = inspect_with_overrides(
        inspect_pipeline(base, &args.runner).await?,
        None,
        Some(&args.target),
    );
    let input_path = resolve_pipeline_input_path(
        &args.input,
        &args.artifacts.root,
        &args.runner,
        PipelineArtifactStage::Transform,
    )?;
    let target_base = base_with_pipeline_target(base, &inspect.target);
    let input_path =
        materialize_deferred_attachments_for_push(base, &inspect.target, &input_path).await?;

    crate::sync::push_jsonl_file(
        target_base,
        SyncPushFileArgs {
            object_ref: pipeline_target_dataset_ref(&inspect.target)?,
            input: input_path,
            root: args.artifacts.root,
            fresh: args.fresh,
        },
    )
    .await
}

fn base_with_pipeline_target(base: &BaseArgs, target: &PipelineTargetInspect) -> BaseArgs {
    let mut target_base = base.clone();
    if let Some(org_name) = target.org_name.as_deref() {
        target_base.org_name = Some(org_name.to_string());
    }
    if let Some(project_id) = target.project_id.as_deref() {
        target_base.project = Some(project_id.to_string());
    } else if let Some(project_name) = target.project_name.as_deref() {
        target_base.project = Some(project_name.to_string());
    }
    target_base
}

fn pipeline_target_dataset_ref(target: &PipelineTargetInspect) -> Result<String> {
    let dataset_name = target.dataset_name.trim();
    if dataset_name.is_empty() {
        bail!("dataset pipeline target.datasetName cannot be empty");
    }
    Ok(format!("dataset:{dataset_name}"))
}

async fn materialize_deferred_attachments_for_push(
    base: &BaseArgs,
    target: &PipelineTargetInspect,
    input_path: &Path,
) -> Result<PathBuf> {
    let rows = read_jsonl_values(input_path)?;
    if !rows.iter().any(contains_deferred_attachment) {
        return Ok(input_path.to_path_buf());
    }

    let target_ctx = resolve_target_context(base, target).await?;
    let rows =
        materialize_deferred_attachments(rows, &target_ctx.client, input_path.parent()).await?;
    let output_path = input_path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."))
        .join("materialized_for_push.jsonl");
    let mut writer = create_jsonl_file_writer(&output_path)?;
    for row in rows {
        write_jsonl_value(&mut writer, &row)
            .with_context(|| format!("failed to write {}", output_path.display()))?;
    }
    writer
        .flush()
        .with_context(|| format!("failed to flush {}", output_path.display()))?;
    Ok(output_path)
}

async fn upload_dataset_rows(
    base: &BaseArgs,
    target: &PipelineTargetInspect,
    rows: Vec<Value>,
) -> Result<usize> {
    let target_ctx = resolve_target_context(base, target).await?;
    let dataset = resolve_target_dataset(&target_ctx.client, target, &target_ctx.project).await?;
    let rows = materialize_deferred_attachments(rows, &target_ctx.client, None).await?;
    let records = prepare_pipeline_records(rows)?;
    let inserted = records.len();

    utils::submit_prepared_records(
        &target_ctx,
        &dataset.id,
        &records,
        false,
        "Uploading dataset rows...",
        "dataset pipeline upload failed",
    )
    .await?;

    Ok(inserted)
}

fn contains_deferred_attachment(value: &Value) -> bool {
    match value {
        Value::Object(object) => {
            is_deferred_attachment_marker(object)
                || object.values().any(contains_deferred_attachment)
        }
        Value::Array(items) => items.iter().any(contains_deferred_attachment),
        _ => false,
    }
}

async fn materialize_deferred_attachments(
    mut rows: Vec<Value>,
    client: &ApiClient,
    base_dir: Option<&Path>,
) -> Result<Vec<Value>> {
    let mut specs = Vec::new();
    for row in &rows {
        collect_deferred_attachment_specs(row, &mut specs)?;
    }
    if specs.is_empty() {
        return Ok(rows);
    }

    let mut replacements = HashMap::new();
    for spec in specs {
        if replacements.contains_key(&spec.key) {
            continue;
        }
        let reference = upload_deferred_attachment(client, &spec, base_dir)
            .await
            .with_context(|| format!("failed to upload deferred attachment {}", spec.filename))?;
        replacements.insert(spec.key, reference);
    }

    for row in &mut rows {
        replace_deferred_attachment_specs(row, &replacements)?;
    }
    Ok(rows)
}

#[derive(Debug, Clone)]
struct DeferredAttachmentSpec {
    key: String,
    filename: String,
    content_type: String,
    path: Option<PathBuf>,
    data: Option<Value>,
    pretty: bool,
}

fn collect_deferred_attachment_specs(
    value: &Value,
    specs: &mut Vec<DeferredAttachmentSpec>,
) -> Result<()> {
    match value {
        Value::Object(object) if is_deferred_attachment_marker(object) => {
            specs.push(parse_deferred_attachment_spec(object)?);
        }
        Value::Object(object) => {
            for value in object.values() {
                collect_deferred_attachment_specs(value, specs)?;
            }
        }
        Value::Array(items) => {
            for value in items {
                collect_deferred_attachment_specs(value, specs)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn replace_deferred_attachment_specs(
    value: &mut Value,
    replacements: &HashMap<String, Value>,
) -> Result<()> {
    match value {
        Value::Object(object) if is_deferred_attachment_marker(object) => {
            let spec = parse_deferred_attachment_spec(object)?;
            let replacement = replacements
                .get(&spec.key)
                .with_context(|| format!("missing replacement for {}", spec.filename))?;
            *value = replacement.clone();
        }
        Value::Object(object) => {
            for value in object.values_mut() {
                replace_deferred_attachment_specs(value, replacements)?;
            }
        }
        Value::Array(items) => {
            for value in items {
                replace_deferred_attachment_specs(value, replacements)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn is_deferred_attachment_marker(object: &serde_json::Map<String, Value>) -> bool {
    object
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|value| value == "braintrust_deferred_attachment")
}

fn parse_deferred_attachment_spec(
    object: &serde_json::Map<String, Value>,
) -> Result<DeferredAttachmentSpec> {
    let filename = object
        .get("filename")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .context("deferred attachment is missing filename")?
        .to_string();
    let content_type = object
        .get("content_type")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .unwrap_or("application/json")
        .to_string();
    let path = object
        .get("path")
        .and_then(Value::as_str)
        .filter(|value| !value.trim().is_empty())
        .map(PathBuf::from);
    let data = object.get("data").cloned();
    if path.is_none() && data.is_none() {
        bail!("deferred attachment {filename} is missing path or data");
    }
    let pretty = object
        .get("pretty")
        .and_then(Value::as_bool)
        .unwrap_or(false);
    let key = serde_json::to_string(object)
        .context("failed to build deferred attachment replacement key")?;

    Ok(DeferredAttachmentSpec {
        key,
        filename,
        content_type,
        path,
        data,
        pretty,
    })
}

async fn upload_deferred_attachment(
    client: &ApiClient,
    spec: &DeferredAttachmentSpec,
    base_dir: Option<&Path>,
) -> Result<Value> {
    let key = uuid::Uuid::new_v4().to_string();
    let request = json!({
        "key": key,
        "filename": spec.filename,
        "content_type": spec.content_type,
        "org_id": client.org_id(),
    });
    let metadata: AttachmentUploadMetadata = client
        .post("/attachment", &request)
        .await
        .context("failed to request signed URL from API server")?;
    let data = deferred_attachment_bytes(spec, base_dir)?;
    let upload_result =
        crate::http::put_signed_url_with_headers(&metadata.signed_url, data, &metadata.headers)
            .await;

    let status = match &upload_result {
        Ok(()) => json!({ "upload_status": "done" }),
        Err(err) => json!({ "upload_status": "error", "error_message": err.to_string() }),
    };
    let status_request = json!({
        "key": key,
        "org_id": client.org_id(),
        "status": status,
    });
    let _: Value = client
        .post("/attachment/status", &status_request)
        .await
        .context("failed to log attachment status")?;
    upload_result.context("failed to upload attachment to object store")?;

    Ok(json!({
        "type": "braintrust_attachment",
        "filename": spec.filename,
        "content_type": spec.content_type,
        "key": key,
    }))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AttachmentUploadMetadata {
    signed_url: String,
    #[serde(default)]
    headers: HashMap<String, String>,
}

fn deferred_attachment_bytes(
    spec: &DeferredAttachmentSpec,
    base_dir: Option<&Path>,
) -> Result<Vec<u8>> {
    if let Some(path) = spec.path.as_ref() {
        let path = if path.is_absolute() {
            path.clone()
        } else {
            base_dir.unwrap_or_else(|| Path::new(".")).join(path)
        };
        return fs::read(&path).with_context(|| format!("failed to read {}", path.display()));
    }

    let data = spec
        .data
        .as_ref()
        .context("deferred attachment is missing data")?;
    let text = if spec.pretty {
        serde_json::to_string_pretty(data)
    } else {
        serde_json::to_string(data)
    }
    .context("failed to serialize deferred attachment data")?;
    Ok(text.into_bytes())
}

fn prepare_pipeline_records(rows: Vec<Value>) -> Result<Vec<records::PreparedDatasetRecord>> {
    let mut objects = Vec::with_capacity(rows.len());
    for (index, row) in rows.into_iter().enumerate() {
        match row {
            Value::Object(row) => objects.push(row),
            _ => bail!("dataset pipeline row {} must be a JSON object", index + 1),
        }
    }

    records::prepare_records(objects, "id", false)
        .context("dataset pipeline transform produced invalid dataset rows")
}

async fn resolve_target_context(
    base: &BaseArgs,
    target: &PipelineTargetInspect,
) -> Result<ResolvedContext> {
    let mut target_base = base.clone();
    if let Some(org_name) = target.org_name.as_deref() {
        target_base.org_name = Some(org_name.to_string());
    }
    let ctx = login(&target_base).await?;
    let client = ApiClient::new(&ctx)?;
    let project = resolve_target_project(&client, target).await?;
    Ok(ResolvedContext {
        client,
        app_url: ctx.app_url,
        project,
    })
}

async fn resolve_target_project(
    client: &ApiClient,
    target: &PipelineTargetInspect,
) -> Result<Project> {
    if let Some(project_id) = target.project_id.as_deref() {
        return Ok(Project {
            id: project_id.to_string(),
            name: target
                .project_name
                .clone()
                .unwrap_or_else(|| project_id.to_string()),
            org_id: String::new(),
            description: None,
        });
    }
    let project_name = target
        .project_name
        .as_deref()
        .context("dataset pipeline target requires projectName or projectId")?;
    if let Some(project) = get_project_by_name(client, project_name).await? {
        Ok(project)
    } else {
        create_project(client, project_name)
            .await
            .with_context(|| format!("project '{project_name}' not found, and creating it failed"))
    }
}

async fn resolve_target_dataset(
    client: &ApiClient,
    target: &PipelineTargetInspect,
    project: &Project,
) -> Result<datasets_api::Dataset> {
    let dataset_name = target.dataset_name.trim();
    if dataset_name.is_empty() {
        bail!("dataset pipeline target.datasetName cannot be empty");
    }

    let datasets = datasets_api::list_datasets(client, &project.id).await?;
    if let Some(dataset) = datasets
        .iter()
        .find(|dataset| dataset.id == dataset_name || dataset.name == dataset_name)
    {
        return Ok(dataset.clone());
    }

    if is_uuid_like(dataset_name) {
        bail!(
            "dataset id '{}' not found in project '{}'",
            dataset_name,
            project.name
        );
    }

    datasets_api::create_dataset_with_metadata(
        client,
        &project.id,
        dataset_name,
        target.description.as_deref(),
        target.metadata.as_ref(),
    )
    .await
    .with_context(|| format!("dataset '{dataset_name}' not found, and creating it failed"))
}

async fn discover_refs(
    base: &BaseArgs,
    inspect: &PipelineInspect,
    options: &PipelineFetchOptions,
    out: &Path,
) -> Result<ProjectLogRefDiscoveryResult> {
    let (ctx, client, project) = resolve_pipeline_source_context(base, &inspect.source).await?;
    let scope = PipelineScope::from_source(&inspect.source);
    let limit = options.limit;
    let filter = discovery_filter(&inspect.source, options);

    let mut writer = create_jsonl_file_writer(out)?;

    let result = discover_project_log_refs(
        &client,
        &ctx,
        &project.id,
        filter.as_ref(),
        project_log_ref_scope(scope),
        limit,
        options.page_size,
        |reference| {
            write_jsonl_value(&mut writer, &reference.to_value())?;
            writer.flush().context("failed to flush discovery output")?;
            Ok(())
        },
    )
    .await?;
    writer.flush().context("failed to flush discovery output")?;

    Ok(result)
}

fn project_log_ref_scope(scope: PipelineScope) -> ProjectLogRefScope {
    match scope {
        PipelineScope::Trace => ProjectLogRefScope::Trace,
        PipelineScope::Span => ProjectLogRefScope::Span,
    }
}

async fn resolve_pipeline_source_project(
    base: &BaseArgs,
    source: &PipelineSourceInspect,
) -> Result<Project> {
    let (_, _, project) = resolve_pipeline_source_context(base, source).await?;
    Ok(project)
}

async fn resolve_pipeline_source_context(
    base: &BaseArgs,
    source: &PipelineSourceInspect,
) -> Result<(LoginContext, ApiClient, Project)> {
    let mut source_base = base.clone();
    if let Some(org_name) = source.org_name.as_deref() {
        source_base.org_name = Some(org_name.to_string());
    }
    let ctx = login(&source_base).await?;
    let client = ApiClient::new(&ctx)?;
    let project = resolve_source_project(base, &client, source).await?;
    Ok((ctx, client, project))
}

fn discovery_filter(
    source: &PipelineSourceInspect,
    options: &PipelineFetchOptions,
) -> Option<Value> {
    let mut filters = Vec::new();
    if let Some(filter) = source
        .filter
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        filters.push(json!({ "btql": filter }));
    }
    if !options.root_span_ids.is_empty() {
        filters.push(root_span_id_filter(&options.root_span_ids));
    }
    match filters.len() {
        0 => None,
        1 => filters.into_iter().next(),
        _ => Some(json!({ "op": "and", "children": filters })),
    }
}

fn root_span_id_filter(root_span_ids: &[String]) -> Value {
    json!({
        "op": "in",
        "left": { "op": "ident", "name": ["root_span_id"] },
        "right": { "op": "literal", "value": root_span_ids }
    })
}

fn default_transform_concurrency() -> usize {
    std::thread::available_parallelism()
        .map(|parallelism| parallelism.get())
        .unwrap_or(16)
}

async fn resolve_source_project(
    base: &BaseArgs,
    client: &ApiClient,
    source: &PipelineSourceInspect,
) -> Result<Project> {
    if let Some(project_id) = source.project_id.as_deref() {
        return Ok(Project {
            id: project_id.to_string(),
            name: source
                .project_name
                .clone()
                .unwrap_or_else(|| project_id.to_string()),
            org_id: String::new(),
            description: None,
        });
    }
    let configured_project =
        crate::config::configured_project_for_context(base, Some(client.org_name()));
    let project_name = source
        .project_name
        .as_deref()
        .or(base.project.as_deref())
        .or(configured_project.as_deref())
        .context(
            "dataset pipeline source requires projectName or projectId; pass --source-project or set an active project",
        )?;
    get_project_by_name(client, project_name)
        .await?
        .with_context(|| format!("project '{project_name}' not found"))
}

fn print_summary(base: &BaseArgs, summary: Value, force_stderr: bool) -> Result<()> {
    let object = summary
        .as_object()
        .context("dataset pipeline summary must be an object")?;
    if base.json && !force_stderr {
        println!("{}", serde_json::to_string(&summary)?);
        return Ok(());
    }
    let parts = object
        .iter()
        .map(|(key, value)| format!("{key}: {}", summary_value(value)))
        .collect::<Vec<_>>();
    eprintln!("{}", parts.join(", "));
    Ok(())
}

fn print_pipeline_status(base: &BaseArgs, message: impl AsRef<str>) {
    if !base.json && !base.quiet {
        eprintln!("{}", message.as_ref());
    }
}

fn summary_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Number(value) => value.to_string(),
        Value::Bool(value) => value.to_string(),
        Value::Null => "null".to_string(),
        Value::Array(_) | Value::Object(_) => value.to_string(),
    }
}

fn is_uuid_like(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    for (index, byte) in bytes.iter().enumerate() {
        match index {
            8 | 13 | 18 | 23 => {
                if *byte != b'-' {
                    return false;
                }
            }
            _ if !byte.is_ascii_hexdigit() => return false,
            _ => {}
        }
    }
    true
}

fn parse_positive_usize(value: &str) -> std::result::Result<usize, String> {
    let parsed = value
        .parse::<usize>()
        .map_err(|_| format!("invalid positive integer '{value}'"))?;
    if parsed == 0 {
        return Err("value must be greater than 0".to_string());
    }
    Ok(parsed)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_base_args() -> BaseArgs {
        BaseArgs {
            json: false,
            verbose: false,
            verbose_source: None,
            quiet: false,
            quiet_source: None,
            no_color: false,
            no_input: false,
            profile: None,
            profile_explicit: false,
            org_name: None,
            project: None,
            api_key: None,
            api_key_source: None,
            prefer_profile: false,
            api_url: None,
            app_url: None,
            ca_cert: None,
            env_file: None,
        }
    }

    #[test]
    fn prepare_pipeline_records_reuses_dataset_record_validation() {
        let err = prepare_pipeline_records(vec![json!({
            "input": "hello",
            "span_attributes": { "type": "llm" },
        })])
        .expect_err("unexpected dataset row fields should be rejected");

        assert!(err
            .to_string()
            .contains("dataset pipeline transform produced invalid dataset rows"));
    }

    #[test]
    fn prepare_pipeline_records_uses_dataset_record_schema() {
        let records = prepare_pipeline_records(vec![json!({
            "id": "row-1",
            "input": { "question": "hello" },
            "expected": "world",
            "tags": ["smoke"],
            "metadata": { "source": "test" },
            "origin": {
                "object_type": "project_logs",
                "object_id": "source-project",
                "id": "source-span"
            }
        })])
        .expect("valid dataset pipeline row should deserialize");

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].id, "row-1");
        let upload = records[0].to_upload_row("target-dataset", false);
        assert_eq!(upload.get("id"), Some(&json!("row-1")));
        assert_eq!(upload.get("dataset_id"), Some(&json!("target-dataset")));
        assert_eq!(upload.get("expected"), Some(&json!("world")));
        assert!(!upload.contains_key("span_id"));
        assert!(!upload.contains_key("root_span_id"));
        assert!(!upload.contains_key("project_id"));
    }

    #[test]
    fn transform_response_validation_rejects_row_count_mismatch() {
        let response = PipelineTransformResponse {
            candidates: 1,
            row_count: 2,
            rows: vec![json!({ "input": "one" })],
        };

        let err =
            validate_transform_response(&response).expect_err("rowCount should match rows length");
        assert!(err.to_string().contains("rowCount 2"));
    }

    #[test]
    fn pipeline_target_dataset_ref_validates_dataset_name() {
        let target = PipelineTargetInspect {
            project_id: None,
            project_name: Some("Target Project".to_string()),
            org_name: None,
            dataset_name: "  Ground Truth  ".to_string(),
            description: None,
            metadata: None,
        };
        assert_eq!(
            pipeline_target_dataset_ref(&target).expect("dataset ref"),
            "dataset:Ground Truth"
        );

        let err = pipeline_target_dataset_ref(&PipelineTargetInspect {
            dataset_name: " ".to_string(),
            ..target
        })
        .expect_err("empty dataset names should fail");
        assert!(err.to_string().contains("target.datasetName"));
    }

    #[test]
    fn pipeline_push_base_uses_target_org_and_project() {
        let base = test_base_args();
        let target = PipelineTargetInspect {
            project_id: Some("project-id".to_string()),
            project_name: Some("Project Name".to_string()),
            org_name: Some("target-org".to_string()),
            dataset_name: "Dataset".to_string(),
            description: None,
            metadata: None,
        };

        let target_base = base_with_pipeline_target(&base, &target);
        assert_eq!(target_base.org_name.as_deref(), Some("target-org"));
        assert_eq!(target_base.project.as_deref(), Some("project-id"));
    }

    #[test]
    fn deferred_attachment_detection_finds_nested_marker() {
        let row = json!({
            "id": "row-1",
            "input": {
                "full_trace": {
                    "type": "braintrust_deferred_attachment",
                    "kind": "json",
                    "filename": "trace.json",
                    "content_type": "application/json",
                    "path": "attachments/trace.json"
                }
            }
        });

        assert!(contains_deferred_attachment(&row));

        let mut specs = Vec::new();
        collect_deferred_attachment_specs(&row, &mut specs).expect("collect specs");
        assert_eq!(specs.len(), 1);
        assert_eq!(specs[0].filename, "trace.json");
        assert_eq!(
            specs[0].path.as_deref(),
            Some(Path::new("attachments/trace.json"))
        );
    }

    #[test]
    fn deferred_attachment_replacement_rewrites_nested_marker() {
        let mut row = json!({
            "id": "row-1",
            "input": {
                "full_trace": {
                    "type": "braintrust_deferred_attachment",
                    "kind": "json",
                    "filename": "trace.json",
                    "content_type": "application/json",
                    "data": { "ok": true }
                }
            }
        });
        let mut specs = Vec::new();
        collect_deferred_attachment_specs(&row, &mut specs).expect("collect specs");
        let replacement = json!({
            "type": "braintrust_attachment",
            "filename": "trace.json",
            "content_type": "application/json",
            "key": "uploaded-key"
        });
        let replacements = HashMap::from([(specs[0].key.clone(), replacement.clone())]);

        replace_deferred_attachment_specs(&mut row, &replacements).expect("replace specs");

        assert_eq!(row["input"]["full_trace"], replacement);
    }

    #[test]
    fn typescript_runner_defers_json_attachments_during_transform() {
        let Ok(strip_check) = Command::new("node")
            .arg("--experimental-strip-types")
            .arg("--eval")
            .arg("")
            .output()
        else {
            return;
        };
        if !strip_check.status.success() {
            return;
        }

        let root = tempfile::tempdir().expect("tempdir");
        let node_modules = root.path().join("node_modules").join("braintrust");
        fs::create_dir_all(&node_modules).expect("create fake braintrust package");
        fs::write(
            node_modules.join("package.json"),
            r#"{"name":"braintrust","type":"module","exports":{".":{"import":"./index.mjs","require":"./index.cjs"}}}"#,
        )
        .expect("write fake package.json");
        fs::write(
            node_modules.join("index.cjs"),
            r#"
const pipelines = [];

class OriginalJSONAttachment {
  constructor() {
    throw new Error("original JSONAttachment should be shimmed");
  }
}

module.exports = {
  DatasetPipeline(definition) {
    pipelines.push(definition);
    return definition;
  },
  getRegisteredDatasetPipelines() {
    return pipelines;
  },
  isDatasetPipelineDefinition(value) {
    return !!value && typeof value.transform === "function";
  },
  LocalTrace: class {
    constructor(options) {
      this.options = options;
    }
    getConfiguration() {
      return { root_span_id: this.options.rootSpanId };
    }
  },
  _internalGetGlobalState() {
    return {
      loggedIn: true,
      orgName: "source-org",
      login: async function () {
        return this;
      },
    };
  },
  loginToState: async function ({ orgName }) {
    return {
      loggedIn: true,
      orgName,
      login: async function () {
        return this;
      },
    };
  },
  JSONAttachment: OriginalJSONAttachment,
};
"#,
        )
        .expect("write fake braintrust cjs module");
        fs::write(
            node_modules.join("index.mjs"),
            r#"
export function DatasetPipeline(definition) {
  return definition;
}

export class JSONAttachment {
  constructor(data, options) {
    const hook = globalThis.__BT_DATASET_PIPELINE_DEFER_JSON_ATTACHMENT__;
    if (hook) {
      return hook(data, options);
    }
    throw new Error("dataset pipeline deferred JSON hook was not installed");
  }
}
"#,
        )
        .expect("write fake braintrust esm module");

        let runner_path = root.path().join("dataset-pipeline-runner.ts");
        fs::write(&runner_path, RUNNER_SOURCE).expect("write runner source");
        let pipeline_path = root.path().join("pipeline.ts");
        fs::write(
            &pipeline_path,
            r#"
import { DatasetPipeline, JSONAttachment } from "braintrust";

export default DatasetPipeline({
  name: "ts-json-attachment-smoke",
  source: { projectName: "source-project" },
  target: { projectName: "target-project", datasetName: "traces" },
  transform: () => ({
    input: {
      full_trace: new JSONAttachment(
        { ok: true },
        { filename: "trace.json", pretty: true },
      ),
    },
  }),
});
"#,
        )
        .expect("write pipeline");

        let attachment_dir = root.path().join("attachments");
        let request = json!({
            "refs": [{ "root_span_id": "root-span" }],
            "sourceProjectId": "source-project-id",
            "attachmentDir": attachment_dir,
        });
        let mut child = Command::new("node")
            .arg("--experimental-strip-types")
            .arg(&runner_path)
            .arg(&pipeline_path)
            .current_dir(root.path())
            .env("BT_DATASET_PIPELINE_STAGE", "transform")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn node runner");
        child
            .stdin
            .as_mut()
            .expect("runner stdin")
            .write_all(request.to_string().as_bytes())
            .expect("write runner request");
        let output = child.wait_with_output().expect("runner output");
        assert!(
            output.status.success(),
            "runner failed\nstdout:\n{}\nstderr:\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );

        let response: Value = serde_json::from_slice(&output.stdout).expect("runner JSON response");
        assert_eq!(response["rowCount"], json!(1));
        let marker = &response["rows"][0]["input"]["full_trace"];
        assert_eq!(marker["type"], "braintrust_deferred_attachment");
        assert_eq!(marker["kind"], "json");
        assert_eq!(marker["filename"], "trace.json");
        assert_eq!(marker["content_type"], "application/json");
        assert!(marker.get("data").is_none());

        let sidecar_path = marker["path"].as_str().expect("sidecar path");
        let sidecar = fs::read_to_string(sidecar_path).expect("read sidecar JSON");
        assert_eq!(
            serde_json::from_str::<Value>(&sidecar).expect("parse sidecar"),
            json!({ "ok": true })
        );
        assert!(sidecar.contains("\n  \"ok\": true\n"));
    }

    #[test]
    fn pipeline_source_artifact_records_resolved_project() {
        let source = PipelineSourceInspect {
            project_id: None,
            project_name: None,
            org_name: None,
            filter: Some("span_attributes.type = 'llm'".to_string()),
            scope: Some(PipelineScope::Span),
        };
        let project = Project {
            id: "project-id".to_string(),
            name: "Loop".to_string(),
            org_id: "org-id".to_string(),
            description: None,
        };

        let resolved = source_with_resolved_project(&source, &project);

        assert_eq!(resolved.project_id.as_deref(), Some("project-id"));
        assert_eq!(resolved.project_name.as_deref(), Some("Loop"));
        assert_eq!(resolved.filter, source.filter);
        assert_eq!(resolved.scope, source.scope);
    }

    #[test]
    fn pipeline_transform_base_inherits_fetch_artifact_context() {
        let base = test_base_args();
        let manifest = PipelineArtifactManifest {
            schema_version: PIPELINE_ARTIFACT_SCHEMA_VERSION,
            spec_hash: "hash".to_string(),
            spec: PipelineArtifactSpec {
                schema_version: PIPELINE_ARTIFACT_SCHEMA_VERSION,
                kind: PIPELINE_ARTIFACT_OBJECT_TYPE.to_string(),
                pipeline: "facet_pipeline.py".to_string(),
                name: None,
                cli_project: Some("Loop".to_string()),
                cli_org: Some("braintrustdata.com".to_string()),
                stage: PipelineArtifactStage::Fetch,
                source: None,
                target: None,
                fetch: None,
                transform: None,
                input_path: None,
            },
            status: PipelineArtifactStatus::Completed,
            stage: PipelineArtifactStage::Fetch,
            input_path: None,
            output_path: None,
            refs: Some(1),
            candidates: None,
            rows: None,
            pages: Some(1),
            started_at: 1,
            updated_at: 2,
            completed_at: Some(2),
        };

        let inherited = base_with_pipeline_artifact_context(&base, Some(&manifest));

        assert_eq!(inherited.project.as_deref(), Some("Loop"));
        assert_eq!(inherited.org_name.as_deref(), Some("braintrustdata.com"));
    }

    #[test]
    fn pipeline_artifacts_default_to_sync_root_shape() {
        let root = tempfile::tempdir().expect("tempdir");
        let runner = PipelineRunnerArgs {
            pipeline: PathBuf::from("facet_pipeline.py"),
            name: None,
            runner: None,
        };
        let spec =
            base_pipeline_artifact_spec(&test_base_args(), &runner, PipelineArtifactStage::Fetch);

        let artifact = resolve_pipeline_output_artifact(root.path(), &runner, spec, None, None)
            .expect("artifact path");

        assert!(artifact
            .output_path
            .starts_with(root.path().join("dataset_pipeline_facet_pipeline")));
        assert_eq!(artifact.output_path.file_name().unwrap(), "fetched.jsonl");
    }

    #[test]
    fn pipeline_input_defaults_to_latest_completed_stage_output() {
        let root = tempfile::tempdir().expect("tempdir");
        let runner = PipelineRunnerArgs {
            pipeline: PathBuf::from("facet_pipeline.py"),
            name: None,
            runner: None,
        };
        let spec =
            base_pipeline_artifact_spec(&test_base_args(), &runner, PipelineArtifactStage::Fetch);
        let artifact = resolve_pipeline_output_artifact(root.path(), &runner, spec, None, None)
            .expect("artifact path");

        artifact.write_spec().expect("write spec");
        crate::sync::write_jsonl_values(
            Some(&artifact.output_path),
            &[json!({ "root_span_id": "root-1" })],
        )
        .expect("write output");
        artifact
            .write_manifest(PipelineArtifactManifest {
                schema_version: PIPELINE_ARTIFACT_SCHEMA_VERSION,
                spec_hash: artifact.spec_hash.clone(),
                spec: artifact.spec.clone(),
                status: PipelineArtifactStatus::Completed,
                stage: PipelineArtifactStage::Fetch,
                input_path: None,
                output_path: Some(artifact.output_path.display().to_string()),
                refs: Some(1),
                candidates: None,
                rows: None,
                pages: Some(1),
                started_at: 1,
                updated_at: 2,
                completed_at: Some(2),
            })
            .expect("write manifest");

        let resolved =
            resolve_pipeline_input_path(&None, root.path(), &runner, PipelineArtifactStage::Fetch)
                .expect("default input");

        assert_eq!(resolved, artifact.output_path);
    }

    #[test]
    fn pipeline_transform_output_defaults_to_fetch_artifact_dir() {
        let root = tempfile::tempdir().expect("tempdir");
        let runner = PipelineRunnerArgs {
            pipeline: PathBuf::from("facet_pipeline.py"),
            name: None,
            runner: None,
        };
        let source = PipelineSourceInspect {
            project_id: None,
            project_name: Some("Loop".to_string()),
            org_name: None,
            filter: None,
            scope: Some(PipelineScope::Span),
        };
        let fetch_spec =
            base_pipeline_artifact_spec(&test_base_args(), &runner, PipelineArtifactStage::Fetch);
        let fetch_artifact =
            resolve_pipeline_output_artifact(root.path(), &runner, fetch_spec, None, None)
                .expect("fetch artifact");
        let transform_spec = pipeline_transform_artifact_spec(
            &test_base_args(),
            &runner,
            &source,
            &PipelineTransformOptions {
                max_concurrency: Some(16),
            },
            &fetch_artifact.output_path,
        );

        let transform_artifact = resolve_pipeline_output_artifact(
            root.path(),
            &runner,
            transform_spec,
            None,
            Some(&fetch_artifact.output_path),
        )
        .expect("transform artifact");

        assert_eq!(transform_artifact.spec_dir, fetch_artifact.spec_dir);
        assert_eq!(
            transform_artifact.output_path,
            fetch_artifact.spec_dir.join("transformed.jsonl")
        );
    }
}
