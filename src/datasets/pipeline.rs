use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::args::BaseArgs;
use crate::auth::{login, resolved_runner_env, LoginContext};
use crate::http::ApiClient;
use crate::js_runner::{build_js_runner_command, materialize_runner_script_in_cwd};
use crate::projects::api::{create_project, get_project_by_name, Project};
use crate::sync::discovery::{discover_project_log_refs, ProjectLogRefScope};
use crate::sync::{read_jsonl_values, write_jsonl_value, write_jsonl_values};

use super::{api as datasets_api, records, utils, ResolvedContext};

const RUNNER_FILE: &str = "dataset-pipeline-runner.ts";
const RUNNER_SOURCE: &str = include_str!("../../scripts/dataset-pipeline-runner.ts");

#[derive(Debug, Clone, Args)]
pub struct PipelineArgs {
    #[command(subcommand)]
    command: PipelineCommands,
}

#[derive(Debug, Clone, Subcommand)]
enum PipelineCommands {
    /// Fetch, transform, and insert dataset rows
    Run(PipelineRunArgs),
    /// Discover source trace/span refs to JSONL
    Fetch(PipelineFetchArgs),
    /// Transform candidate JSONL into proposed dataset row JSONL
    Transform(PipelineTransformArgs),
    /// Copy proposed row JSONL for human or agent review
    Review(PipelineReviewArgs),
    /// Insert approved row JSONL into the target dataset
    Commit(PipelineCommitArgs),
}

#[derive(Debug, Clone, Args)]
struct PipelineRunnerArgs {
    /// Dataset pipeline file to execute
    #[arg(value_name = "PIPELINE")]
    pipeline: PathBuf,

    /// Pipeline name, required when the file defines multiple pipelines
    #[arg(long)]
    name: Option<String>,

    /// JavaScript/TypeScript runner binary (e.g. tsx, vite-node, ts-node)
    #[arg(
        long,
        short = 'r',
        env = "BT_DATASET_PIPELINE_RUNNER",
        value_name = "RUNNER"
    )]
    runner: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct PipelineFetchOptions {
    /// Maximum number of source refs to discover
    #[arg(long, default_value_t = 100, value_parser = parse_positive_usize)]
    target: usize,

    /// Restrict the source query to one or more root span ids
    #[arg(long = "root-span-id")]
    root_span_ids: Vec<String>,

    /// Additional SQL predicate appended to the source WHERE clause
    #[arg(long)]
    extra_where_sql: Option<String>,

    /// Page size for discovery BTQL pagination
    #[arg(long, default_value_t = 1000, value_parser = parse_positive_usize)]
    page_size: usize,
}

#[derive(Debug, Clone, Args)]
struct PipelineTransformOptions {
    /// Maximum concurrent transform calls
    #[arg(long, default_value_t = 16, value_parser = parse_positive_usize)]
    max_concurrency: usize,
}

#[derive(Debug, Clone, Args)]
struct PipelineRunArgs {
    #[command(flatten)]
    runner: PipelineRunnerArgs,

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
    fetch: PipelineFetchOptions,

    /// Output JSONL file. Defaults to stdout.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
struct PipelineTransformArgs {
    #[command(flatten)]
    runner: PipelineRunnerArgs,

    #[command(flatten)]
    transform: PipelineTransformOptions,

    /// Input candidate JSONL file
    #[arg(long = "in")]
    input: PathBuf,

    /// Output proposed dataset row JSONL file. Defaults to stdout.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
struct PipelineReviewArgs {
    #[command(flatten)]
    runner: PipelineRunnerArgs,

    /// Input proposed dataset row JSONL file
    #[arg(long = "in")]
    input: PathBuf,

    /// Output approved dataset row JSONL file. Defaults to stdout.
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Debug, Clone, Args)]
struct PipelineCommitArgs {
    #[command(flatten)]
    runner: PipelineRunnerArgs,

    /// Input approved dataset row JSONL file
    #[arg(long = "in")]
    input: PathBuf,
}

pub async fn run(base: BaseArgs, args: PipelineArgs) -> Result<()> {
    match args.command {
        PipelineCommands::Run(args) => {
            let inspect = inspect_pipeline(&base, &args.runner).await?;
            let tempdir =
                tempfile::tempdir().context("failed to create dataset pipeline temp dir")?;
            let refs_path = tempdir.path().join("discovered.jsonl");
            discover_refs(&base, &inspect, &args.fetch, Some(&refs_path), false).await?;

            let refs = read_jsonl_values(&refs_path)?;
            let source_project = resolve_pipeline_source_project(&base, &inspect.source).await?;
            let transform_response: PipelineTransformResponse = run_runner_json(
                &base,
                "transform",
                &args.runner,
                &json!({
                    "sourceProjectId": source_project.id,
                    "refs": refs,
                    "maxConcurrency": args.transform.max_concurrency,
                }),
            )
            .await?;
            validate_transform_response(&transform_response)?;
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
        PipelineCommands::Fetch(args) => {
            let inspect = inspect_pipeline(&base, &args.runner).await?;
            discover_refs(&base, &inspect, &args.fetch, args.out.as_deref(), true).await
        }
        PipelineCommands::Transform(args) => transform_refs(&base, args).await,
        PipelineCommands::Review(args) => review_rows(&base, args),
        PipelineCommands::Commit(args) => commit_rows(&base, args).await,
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PipelineInspect {
    source: PipelineSourceInspect,
    target: PipelineTargetInspect,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PipelineSourceInspect {
    project_id: Option<String>,
    project_name: Option<String>,
    org_name: Option<String>,
    filter: Option<String>,
    scope: Option<PipelineScope>,
    limit: Option<usize>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PipelineTargetInspect {
    project_id: Option<String>,
    project_name: Option<String>,
    org_name: Option<String>,
    dataset_name: String,
    description: Option<String>,
    metadata: Option<Value>,
}

#[derive(Debug, Clone, Copy, Deserialize, PartialEq, Eq)]
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

async fn inspect_pipeline(base: &BaseArgs, runner: &PipelineRunnerArgs) -> Result<PipelineInspect> {
    let output = build_runner_command(base, "inspect", runner, |_, _| Ok(()))
        .await?
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .output()
        .context("failed to start dataset pipeline inspect runner")?;
    if !output.status.success() {
        bail!(
            "dataset pipeline inspect runner failed with status {}",
            output.status
        );
    }
    serde_json::from_slice(&output.stdout)
        .context("failed to parse dataset pipeline inspect output")
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
    let runner_script =
        materialize_runner_script_in_cwd("dataset-pipeline-runners", RUNNER_FILE, RUNNER_SOURCE)?;
    let pipeline_file = runner.pipeline.clone();
    let files = vec![pipeline_file.clone()];
    let mut command = build_js_runner_command(runner.runner.as_deref(), &runner_script, &files);

    command.envs(resolved_runner_env(base).await?);
    command.env("BT_DATASET_PIPELINE_STAGE", stage);
    if let Some(name) = runner.name.as_deref() {
        command.env("BT_DATASET_PIPELINE_NAME", name);
    }
    configure(&mut command, stage)?;
    Ok(command)
}

async fn run_runner_json<T>(
    base: &BaseArgs,
    stage: &'static str,
    runner: &PipelineRunnerArgs,
    request: &Value,
) -> Result<T>
where
    T: DeserializeOwned,
{
    let mut command = build_runner_command(base, stage, runner, |_, _| Ok(())).await?;
    command.stdin(Stdio::piped());
    command.stdout(Stdio::piped());
    command.stderr(Stdio::inherit());

    let mut child = command
        .spawn()
        .context("failed to start dataset pipeline runner")?;
    {
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

    let output = child
        .wait_with_output()
        .context("failed to wait for dataset pipeline runner")?;
    if !output.status.success() {
        bail!(
            "dataset pipeline runner failed with status {}",
            output.status
        );
    }
    serde_json::from_slice(&output.stdout)
        .context("failed to parse dataset pipeline runner response")
}

async fn transform_refs(base: &BaseArgs, args: PipelineTransformArgs) -> Result<()> {
    let inspect = inspect_pipeline(base, &args.runner).await?;
    let source_project = resolve_pipeline_source_project(base, &inspect.source).await?;
    let refs = read_jsonl_values(&args.input)?;
    let response: PipelineTransformResponse = run_runner_json(
        base,
        "transform",
        &args.runner,
        &json!({
            "sourceProjectId": source_project.id,
            "refs": refs,
            "maxConcurrency": args.transform.max_concurrency,
        }),
    )
    .await?;
    validate_transform_response(&response)?;
    let row_count = response.rows.len();
    write_jsonl_values(args.out.as_deref(), &response.rows)?;
    print_summary(
        base,
        json!({
            "candidates": response.candidates,
            "rows": row_count,
            "out": args
                .out
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "stdout".to_string()),
        }),
        args.out.is_none(),
    )
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

fn review_rows(base: &BaseArgs, args: PipelineReviewArgs) -> Result<()> {
    let rows = read_jsonl_values(&args.input)?;
    write_jsonl_values(args.out.as_deref(), &rows)?;
    print_summary(
        base,
        json!({
            "rows": rows.len(),
            "out": args
                .out
                .as_ref()
                .map(|path| path.display().to_string())
                .unwrap_or_else(|| "stdout".to_string()),
        }),
        args.out.is_none(),
    )
}

async fn commit_rows(base: &BaseArgs, args: PipelineCommitArgs) -> Result<()> {
    let inspect = inspect_pipeline(base, &args.runner).await?;
    let rows = read_jsonl_values(&args.input)?;
    let row_count = rows.len();
    let inserted = upload_dataset_rows(base, &inspect.target, rows).await?;
    print_summary(
        base,
        json!({
            "rows": row_count,
            "inserted": inserted,
        }),
        false,
    )
}

async fn upload_dataset_rows(
    base: &BaseArgs,
    target: &PipelineTargetInspect,
    rows: Vec<Value>,
) -> Result<usize> {
    let target_ctx = resolve_target_context(base, target).await?;
    let dataset = resolve_target_dataset(&target_ctx.client, target, &target_ctx.project).await?;
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

fn prepare_pipeline_records(rows: Vec<Value>) -> Result<Vec<records::PreparedDatasetRecord>> {
    let mut objects = Vec::with_capacity(rows.len());
    for (index, row) in rows.into_iter().enumerate() {
        match row {
            Value::Object(row) => objects.push(row),
            _ => bail!("dataset pipeline row {} must be a JSON object", index + 1),
        }
    }

    records::prepare_upload_records(objects)
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
    out: Option<&Path>,
    emit_summary: bool,
) -> Result<()> {
    let (ctx, client, project) = resolve_pipeline_source_context(base, &inspect.source).await?;
    let scope = PipelineScope::from_source(&inspect.source);
    let target = inspect.source.limit.unwrap_or(options.target);
    let filter = discovery_filter(&inspect.source, options);

    let mut writer: Box<dyn Write> = if let Some(path) = out {
        if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("failed to create {}", parent.display()))?;
        }
        Box::new(BufWriter::new(File::create(path).with_context(|| {
            format!("failed to create {}", path.display())
        })?))
    } else {
        Box::new(BufWriter::new(io::stdout()))
    };

    let result = discover_project_log_refs(
        &client,
        &ctx,
        &project.id,
        filter.as_ref(),
        project_log_ref_scope(scope),
        target,
        options.page_size,
        |reference| write_jsonl_value(writer.as_mut(), &reference.to_value()).map(|_| ()),
    )
    .await?;
    writer.flush().context("failed to flush discovery output")?;

    let out_label = out
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| "stdout".to_string());
    if emit_summary {
        print_summary(
            base,
            json!({
                "refs": result.refs,
                "pages": result.pages,
                "scope": match scope { PipelineScope::Trace => "trace", PipelineScope::Span => "span" },
                "out": out_label,
            }),
            out.is_none(),
        )?;
    }
    Ok(())
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
    let project = resolve_source_project(&client, source).await?;
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
    if let Some(filter) = options
        .extra_where_sql
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    {
        filters.push(json!({ "btql": filter }));
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

async fn resolve_source_project(
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
    let project_name = source
        .project_name
        .as_deref()
        .context("dataset pipeline source requires projectName or projectId")?;
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
}
