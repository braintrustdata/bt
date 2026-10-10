use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use clap::{builder::BoolishValueParser, Args, Subcommand};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::args::BaseArgs;
use crate::auth;
use crate::functions::{
    self, api as functions_api, FunctionCommands, FunctionTypeFilter, IfExistsMode,
};
use crate::http::ApiClient;
use crate::js_bundle::{
    collect_matching_files, run_node_metadata_runner, swc_bundle_js_module, SwcBundleTarget,
};
use crate::project_context::{resolve_definition_project, resolve_project_optional};
use crate::projects::api::Project;
use crate::traces::{parse_trace_url, resolve_trace_ref, TraceRefSelector};
use crate::ui::{self, with_spinner};
use crate::utils::slug_from_name;

const PREPROCESSORS_JS_RUNNER_SOURCE: &str = include_str!("../scripts/preprocessors-runner.mjs");
const PREPROCESSOR_RUNTIME: &str = "quickjs";
const DEFAULT_PREPROCESSORS_DIR: &str = "braintrust-preprocessors";
const PREPROCESSOR_FILE_PATTERN_HELP: &str =
    "*.preprocessor.ts, *.preprocessor.js, *-preprocessor.ts, or *-preprocessor.js";

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt preprocessors list
  bt preprocessors bootstrap 'Conversation'
  bt preprocessors preview ./conversation.preprocessor.ts --url <BRAINTRUST_TRACE_URL>
  bt preprocessors push ./braintrust-preprocessors --if-exists replace
  bt scorers create 'Helpfulness' --preprocessor conversation ...
")]
pub struct PreprocessorsArgs {
    #[command(subcommand)]
    command: Option<PreprocessorsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum PreprocessorsCommands {
    /// Push local preprocessor definitions
    Push(PushArgs),
    /// Create a starter preprocessor file
    Bootstrap(BootstrapArgs),
    /// Run a local preprocessor on a trace
    Preview(PreviewArgs),
    #[command(flatten)]
    Function(FunctionCommands),
}

#[derive(Debug, Clone, Args)]
struct PushArgs {
    /// File or directory path(s) to scan for preprocessor definitions.
    #[arg(value_name = "PATH")]
    paths: Vec<PathBuf>,

    /// File or directory path(s) to scan for preprocessor definitions.
    #[arg(
        long = "file",
        env = "BT_PREPROCESSORS_PUSH_FILES",
        value_name = "PATH",
        value_delimiter = ','
    )]
    file_flag: Vec<PathBuf>,

    /// Behavior when a preprocessor with the same slug already exists.
    #[arg(
        long = "if-exists",
        env = "BT_PREPROCESSORS_PUSH_IF_EXISTS",
        value_enum,
        default_value = "error"
    )]
    if_exists: IfExistsMode,

    /// Skip confirmation prompt.
    #[arg(
        long,
        short = 'y',
        env = "BT_PREPROCESSORS_PUSH_YES",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    yes: bool,
}

impl PushArgs {
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
struct BootstrapArgs {
    /// Preprocessor name.
    #[arg(value_name = "NAME", required_unless_present = "name_flag")]
    name: Option<String>,

    /// Preprocessor name (positional NAME takes precedence).
    #[arg(
        long = "name",
        env = "BT_PREPROCESSORS_BOOTSTRAP_NAME",
        value_name = "NAME"
    )]
    name_flag: Option<String>,

    /// Output file or directory path. Defaults to braintrust-preprocessors/<name>.preprocessor.ts.
    #[arg(
        long = "file",
        env = "BT_PREPROCESSORS_BOOTSTRAP_FILE",
        value_name = "PATH"
    )]
    file_flag: Option<PathBuf>,

    /// Overwrite an existing file.
    #[arg(
        long,
        short = 'f',
        env = "BT_PREPROCESSORS_BOOTSTRAP_FORCE",
        value_parser = BoolishValueParser::new(),
        default_value_t = false
    )]
    force: bool,
}

impl BootstrapArgs {
    fn name(&self) -> &str {
        self.name
            .as_deref()
            .or(self.name_flag.as_deref())
            .expect("clap requires a positional name or --name")
    }
}

#[derive(Debug, Clone, Args)]
struct PreviewArgs {
    /// Preprocessor file to preview.
    #[arg(value_name = "PATH", required_unless_present = "file_flag")]
    path: Option<PathBuf>,

    /// Preprocessor file to preview (positional PATH takes precedence).
    #[arg(
        long = "file",
        alias = "path",
        env = "BT_PREPROCESSORS_PREVIEW_FILE",
        value_name = "PATH"
    )]
    file_flag: Option<PathBuf>,

    /// Braintrust app URL of the trace to preprocess.
    #[arg(long, env = "BT_PREPROCESSORS_PREVIEW_URL")]
    url: Option<String>,

    /// Project ID of the trace to preprocess.
    #[arg(long, env = "BT_PREPROCESSORS_PREVIEW_PROJECT_ID")]
    project_id: Option<String>,

    /// Root span id of the trace to preprocess.
    #[arg(
        long = "trace-id",
        alias = "root-span-id",
        env = "BT_PREPROCESSORS_PREVIEW_TRACE_ID"
    )]
    trace_id: Option<String>,

    /// Lookback window when resolving a URL's span ID (e.g. 7d, 30d).
    /// Root span IDs and row IDs do not require a time window.
    #[arg(
        long,
        env = "BT_PREPROCESSORS_PREVIEW_LOOKUP_WINDOW",
        default_value = "30d"
    )]
    lookup_window: String,
}

impl PreviewArgs {
    fn path(&self) -> &Path {
        self.path
            .as_deref()
            .or(self.file_flag.as_deref())
            .expect("clap requires a positional path or --file")
    }
}

#[derive(Debug, Deserialize)]
struct PreprocessorsManifest {
    runtime_context: PreprocessorsRuntimeContext,
    #[serde(default)]
    files: Vec<PreprocessorsManifestFile>,
}

#[derive(Debug, Serialize)]
struct PreprocessorsDiscoveryInput {
    files: Vec<PreprocessorsDiscoveryInputFile>,
}

#[derive(Debug, Serialize)]
struct PreprocessorsDiscoveryInputFile {
    source_file: String,
    bundle_file: String,
}

#[derive(Debug, Deserialize)]
struct PreprocessorsRuntimeContext {
    runtime: String,
    version: String,
}

#[derive(Debug, Deserialize)]
struct PreprocessorsManifestFile {
    source_file: String,
    #[serde(default)]
    entries: Vec<PreprocessorManifestEntry>,
}

#[derive(Debug, Deserialize, Clone)]
struct PreprocessorManifestEntry {
    name: String,
    slug: String,
    #[serde(default)]
    description: Option<String>,
    code: String,
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    project_name: Option<String>,
}

#[derive(Debug, Clone)]
struct PreparedPreprocessor {
    source_file: String,
    entry: PreprocessorManifestEntry,
    project: Project,
    runtime_version: String,
}

#[derive(Debug, Serialize)]
struct PushedPreprocessor {
    source_file: String,
    name: String,
    slug: String,
    project_id: String,
    project_name: String,
    function_id: String,
}

#[derive(Debug, Serialize)]
struct BootstrapResult {
    path: String,
}

pub(crate) struct LocalPreprocessor {
    pub(crate) name: String,
    pub(crate) slug: String,
    pub(crate) code: String,
    runtime_version: String,
}

pub async fn run(base: BaseArgs, args: PreprocessorsArgs) -> Result<()> {
    match args.command {
        Some(PreprocessorsCommands::Push(push_args)) => push(base, push_args).await,
        Some(PreprocessorsCommands::Bootstrap(bootstrap_args)) => bootstrap(base, bootstrap_args),
        Some(PreprocessorsCommands::Preview(preview_args)) => preview(base, preview_args).await,
        Some(PreprocessorsCommands::Function(command)) => {
            functions::run_typed_command(base, Some(command), FunctionTypeFilter::Preprocessor)
                .await
        }
        None => functions::run_typed_command(base, None, FunctionTypeFilter::Preprocessor).await,
    }
}

fn bootstrap(base: BaseArgs, args: BootstrapArgs) -> Result<()> {
    let slug = slug_from_name(args.name(), "preprocessor")?;
    let selected_path = args
        .file_flag
        .clone()
        .unwrap_or_else(|| PathBuf::from(DEFAULT_PREPROCESSORS_DIR));
    let path = if selected_path.is_dir() || selected_path.extension().is_none() {
        selected_path.join(format!("{slug}.preprocessor.ts"))
    } else {
        selected_path
    };
    if !is_preprocessor_file(&path) {
        bail!("preprocessor bootstrap path must match {PREPROCESSOR_FILE_PATTERN_HELP}");
    }
    if path.exists() && !args.force {
        bail!(
            "preprocessor file already exists: {}. Use --force to overwrite.",
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
    std::fs::write(&path, preprocessor_bootstrap_template(args.name(), &slug))
        .with_context(|| format!("failed to write preprocessor file {}", path.display()))?;

    let result = BootstrapResult {
        path: path.display().to_string(),
    };
    if base.json {
        println!("{}", serde_json::to_string_pretty(&result)?);
    } else {
        println!("Created preprocessor starter at {}", result.path);
        println!(
            "Preview it with: bt preprocessors preview {} --url <BRAINTRUST_TRACE_URL>",
            result.path
        );
        println!("Push it with: bt preprocessors push {}", result.path);
    }
    Ok(())
}

const PREPROCESSOR_BOOTSTRAP_TEMPLATE: &str = r##"type PreprocessorArgs = {
  input: any;
  output: any;
  error: any;
  metadata: Record<string, any>;
  span_attributes: { name?: string; type?: string };
};

export default {
  name: __PREPROCESSOR_NAME__,
  slug: __PREPROCESSOR_SLUG__,
  handler({ input, output, span_attributes }: PreprocessorArgs): unknown[] {
    if (span_attributes?.type !== "llm") {
      return [];
    }

    const messages: unknown[] = Array.isArray(input) ? [...input] : [];
    if (output !== undefined && output !== null) {
      messages.push({
        role: "assistant",
        content: typeof output === "string" ? output : JSON.stringify(output),
      });
    }
    return messages;
  },
};
"##;

fn preprocessor_bootstrap_template(name: &str, slug: &str) -> String {
    PREPROCESSOR_BOOTSTRAP_TEMPLATE
        .replace(
            "__PREPROCESSOR_NAME__",
            &serde_json::to_string(name).expect("strings serialize to JSON"),
        )
        .replace(
            "__PREPROCESSOR_SLUG__",
            &serde_json::to_string(slug).expect("strings serialize to JSON"),
        )
}

async fn push(base: BaseArgs, args: PushArgs) -> Result<()> {
    let auth_ctx = functions::resolve_auth_context(&base).await?;
    let default_project = resolve_project_optional(&base, &auth_ctx.client, false).await?;
    let files = collect_preprocessor_files(&args.resolved_paths())?;
    if files.is_empty() {
        bail!(
            "no preprocessor files found; expected files matching {PREPROCESSOR_FILE_PATTERN_HELP}"
        );
    }

    let manifest = run_preprocessors_runner(&files)?;
    validate_manifest_runtime(&manifest)?;
    let prepared =
        prepare_preprocessors(&auth_ctx.client, default_project.as_ref(), &manifest).await?;
    if prepared.is_empty() {
        bail!("no preprocessors were registered by the selected files");
    }

    if !args.yes && ui::can_prompt() {
        let prompt = format!("Push {} preprocessor(s)?", prepared.len());
        let confirmed = dialoguer::Confirm::new()
            .with_prompt(prompt)
            .default(false)
            .interact()?;
        if !confirmed {
            return Ok(());
        }
    }

    let events = prepared
        .iter()
        .map(|preprocessor| build_insert_event(preprocessor, args.if_exists))
        .collect::<Vec<_>>();

    let result = with_spinner(
        "Pushing preprocessors...",
        functions_api::insert_functions(&auth_ctx.client, &events),
    )
    .await
    .map_err(|err| {
        anyhow!(format_preprocessors_insert_error(
            &prepared,
            args.if_exists,
            &err
        ))
    })?;

    let pushed = resolve_pushed_preprocessors(&prepared, &result.functions, args.if_exists)?;
    let ignored = prepared.len() - pushed.len();
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

    ui::print_command_status(
        ui::CommandStatus::Success,
        &format!("Pushed {} preprocessor(s)", pushed.len()),
    );
    for preprocessor in pushed.iter().filter(|_| !ui::is_quiet()) {
        eprintln!(
            "  {} ({}): {}",
            preprocessor.name, preprocessor.project_name, preprocessor.slug
        );
    }
    if ignored > 0 && !ui::is_quiet() {
        eprintln!("  Ignored {} existing preprocessor(s)", ignored);
    }
    Ok(())
}

async fn preview(base: BaseArgs, args: PreviewArgs) -> Result<()> {
    let preprocessor = load_local_preprocessor(args.path())?;

    let parsed_url = args.url.as_deref().map(parse_trace_url).transpose()?;
    let mut base = base.clone();
    base.apply_url_org_hint(parsed_url.as_ref().and_then(|url| url.org.as_deref()));
    let auth_ctx = auth::login_read_only(&base).await?;
    let client = ApiClient::new(&auth_ctx)?;
    let project = if args.project_id.is_some()
        || parsed_url
            .as_ref()
            .is_some_and(|parsed| parsed.project.is_some())
    {
        None
    } else {
        resolve_project_optional(&base, &client, false).await?
    };
    let target = resolve_trace_ref(
        &client,
        project.as_ref(),
        &TraceRefSelector {
            url: args.url.as_deref(),
            project_id: args.project_id.as_deref(),
            trace_id: args.trace_id.as_deref(),
            span_id: None,
            lookup_window: &args.lookup_window,
        },
    )
    .await?;

    let request = build_preview_invoke_request(
        &preprocessor,
        target.object_type,
        &target.object_id,
        &target.root_span_id,
    );
    let output = with_spinner(
        "Running preprocessor...",
        functions_api::invoke_function(&client, &request),
    )
    .await
    .with_context(|| format!("failed to run preprocessor '{}'", preprocessor.slug))?;

    if base.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&json!({
                "preprocessor": {
                    "path": args.path().display().to_string(),
                    "name": preprocessor.name,
                    "slug": preprocessor.slug,
                },
                "trace_ref": {
                    "object_type": target.object_type,
                    "object_id": target.object_id,
                    "root_span_id": target.root_span_id,
                },
                "output": output,
            }))?
        );
        return Ok(());
    }

    ui::print_command_status(
        ui::CommandStatus::Success,
        &format!(
            "Ran '{}' on trace {}",
            preprocessor.name, target.root_span_id
        ),
    );
    println!("{}", serde_json::to_string_pretty(&output)?);
    Ok(())
}

fn build_preview_invoke_request(
    preprocessor: &LocalPreprocessor,
    object_type: &str,
    object_id: &str,
    root_span_id: &str,
) -> Value {
    json!({
        "inline_context": {
            "runtime": PREPROCESSOR_RUNTIME,
            "version": preprocessor.runtime_version,
        },
        "code": preprocessor.code,
        "function_type": "preprocessor",
        "name": preprocessor.name,
        "input": {
            "trace_ref": {
                "object_type": object_type,
                "object_id": object_id,
                "root_span_id": root_span_id,
            }
        },
        "mode": "json",
    })
}

pub(crate) fn load_local_preprocessor(path: &Path) -> Result<LocalPreprocessor> {
    if !path.is_file() {
        bail!("preprocessor file not found: {}", path.display());
    }
    let manifest = run_preprocessors_runner(&[path.to_path_buf()])?;
    validate_manifest_runtime(&manifest)?;
    let runtime_version = manifest.runtime_context.version;
    let entry = manifest
        .files
        .into_iter()
        .flat_map(|file| file.entries)
        .next()
        .ok_or_else(|| anyhow!("no preprocessor was registered by {}", path.display()))?;
    Ok(LocalPreprocessor {
        name: entry.name,
        slug: entry.slug,
        code: entry.code,
        runtime_version,
    })
}

fn collect_preprocessor_files(paths: &[PathBuf]) -> Result<Vec<PathBuf>> {
    collect_matching_files(paths, is_preprocessor_file, "preprocessor")
}

fn is_preprocessor_file(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    [
        ".preprocessor.ts",
        ".preprocessor.js",
        "-preprocessor.ts",
        "-preprocessor.js",
    ]
    .iter()
    .any(|suffix| name.ends_with(suffix))
}

fn run_preprocessors_runner(files: &[PathBuf]) -> Result<PreprocessorsManifest> {
    let temp_dir = tempfile::tempdir().context("failed to create preprocessors temp directory")?;
    let mut input_files = Vec::new();
    let mut bundled_files = BTreeMap::new();
    for (index, file) in files.iter().enumerate() {
        let source_file = std::fs::canonicalize(file)
            .with_context(|| format!("failed to resolve preprocessor file {}", file.display()))?;
        let source_key = source_file.display().to_string();
        let safe_name = source_file
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or("preprocessor")
            .replace(|character: char| !character.is_ascii_alphanumeric(), "_");

        let discovery = swc_bundle_js_module(
            &source_file,
            SwcBundleTarget::Discovery,
            &[],
            "preprocessor",
        )?;
        let discovery_bundle = temp_dir
            .path()
            .join(format!("{index}-{safe_name}.discovery.cjs"));
        std::fs::write(
            &discovery_bundle,
            module_exports_from_swc_iife(&discovery.code),
        )
        .with_context(|| {
            format!(
                "failed to write preprocessor discovery bundle {}",
                discovery_bundle.display()
            )
        })?;

        let quickjs =
            swc_bundle_js_module(&source_file, SwcBundleTarget::QuickJs, &[], "preprocessor")?;
        bundled_files.insert(
            source_key.clone(),
            quickjs_handler_from_swc_iife(&quickjs.code),
        );
        input_files.push(PreprocessorsDiscoveryInputFile {
            source_file: source_key,
            bundle_file: discovery_bundle.display().to_string(),
        });
    }
    let input_path = temp_dir.path().join("preprocessors-discovery-input.json");
    std::fs::write(
        &input_path,
        serde_json::to_vec(&PreprocessorsDiscoveryInput { files: input_files })?,
    )
    .with_context(|| {
        format!(
            "failed to write preprocessors discovery input {}",
            input_path.display()
        )
    })?;

    let stdout = run_node_metadata_runner(
        PREPROCESSORS_JS_RUNNER_SOURCE,
        &input_path,
        "preprocessors metadata runner",
    )?;
    let mut manifest: PreprocessorsManifest = serde_json::from_str(&stdout).with_context(|| {
        format!(
            "failed to parse preprocessors metadata runner output as JSON: {}",
            stdout.trim()
        )
    })?;
    for file in &mut manifest.files {
        if let Some(bundled) = bundled_files.get(&file.source_file) {
            for entry in &mut file.entries {
                entry.code.clone_from(bundled);
            }
        }
    }
    Ok(manifest)
}

fn module_exports_from_swc_iife(code: &str) -> String {
    let expression = code.trim().trim_end_matches(';');
    format!(
        "var __BraintrustPreprocessor = {expression};\nmodule.exports = __BraintrustPreprocessor.default;\n"
    )
}

fn quickjs_handler_from_swc_iife(code: &str) -> String {
    let expression = code.trim().trim_end_matches(';');
    format!(
        "var __BraintrustPreprocessor = ({expression}).default;\nfunction handler() {{\n  return __BraintrustPreprocessor.handler.apply(__BraintrustPreprocessor, arguments);\n}}\n"
    )
}

fn validate_manifest_runtime(manifest: &PreprocessorsManifest) -> Result<()> {
    if manifest.runtime_context.runtime != PREPROCESSOR_RUNTIME {
        bail!(
            "preprocessors runner returned unsupported runtime '{}'",
            manifest.runtime_context.runtime
        );
    }
    if manifest.runtime_context.version.trim().is_empty() {
        bail!("preprocessors runner returned an empty runtime version");
    }
    Ok(())
}

async fn prepare_preprocessors(
    client: &ApiClient,
    default_project: Option<&Project>,
    manifest: &PreprocessorsManifest,
) -> Result<Vec<PreparedPreprocessor>> {
    let mut prepared = Vec::new();
    let mut seen = BTreeSet::new();
    for file in &manifest.files {
        for entry in &file.entries {
            let project = resolve_definition_project(
                client,
                default_project,
                entry.project_id.as_deref(),
                entry.project_name.as_deref(),
            )
            .await?
            .ok_or_else(|| {
                anyhow!(
                    "preprocessor '{}' requires a project; set project in its definition or pass --project",
                    entry.slug
                )
            })?;
            if !seen.insert((project.id.clone(), entry.slug.clone())) {
                bail!(
                    "duplicate preprocessor slug '{}' in project '{}'",
                    entry.slug,
                    project.name
                );
            }
            prepared.push(PreparedPreprocessor {
                source_file: file.source_file.clone(),
                entry: entry.clone(),
                project,
                runtime_version: manifest.runtime_context.version.clone(),
            });
        }
    }
    Ok(prepared)
}

fn build_insert_event(preprocessor: &PreparedPreprocessor, if_exists: IfExistsMode) -> Value {
    let mut event = json!({
        "project_id": preprocessor.project.id,
        "name": preprocessor.entry.name,
        "slug": preprocessor.entry.slug,
        "function_type": "preprocessor",
        "if_exists": if_exists.as_str(),
        "function_data": {
            "type": "code",
            "data": {
                "type": "inline",
                "runtime_context": {
                    "runtime": PREPROCESSOR_RUNTIME,
                    "version": preprocessor.runtime_version,
                },
                "code": preprocessor.entry.code,
            }
        },
    });

    if let Some(description) = &preprocessor.entry.description {
        event["description"] = Value::String(description.clone());
    }

    event
}

fn format_preprocessors_insert_error(
    preprocessors: &[PreparedPreprocessor],
    if_exists: IfExistsMode,
    err: &anyhow::Error,
) -> String {
    let preprocessor_list = preprocessors
        .iter()
        .map(|preprocessor| {
            format!(
                "'{}' in project '{}'",
                preprocessor.entry.slug, preprocessor.project.name
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    let preprocessor_list = if preprocessor_list.is_empty() {
        "selected preprocessors".to_string()
    } else {
        preprocessor_list
    };
    let details = format!("{err:#}");
    let retry_hint = if if_exists == IfExistsMode::Error {
        " If you are updating an existing preprocessor, rerun with `bt preprocessors push --if-exists replace`."
    } else {
        ""
    };

    format!(
        "failed to push preprocessors ({preprocessor_list}) with --if-exists {}.{retry_hint} Server response: {details}",
        if_exists.as_str()
    )
}

fn resolve_pushed_preprocessors(
    preprocessors: &[PreparedPreprocessor],
    results: &[functions_api::InsertedFunctionResult],
    if_exists: IfExistsMode,
) -> Result<Vec<PushedPreprocessor>> {
    let mut pushed = Vec::new();
    for preprocessor in preprocessors {
        let result = results
            .iter()
            .find(|result| {
                result.project_id == preprocessor.project.id
                    && result.slug == preprocessor.entry.slug
            })
            .with_context(|| {
                format!(
                    "preprocessor insert succeeded, but its response omitted '{}' in project '{}'; check the published preprocessors before retrying",
                    preprocessor.entry.slug, preprocessor.project.name
                )
            })?;
        if if_exists == IfExistsMode::Ignore && result.found_existing {
            continue;
        }
        pushed.push(PushedPreprocessor {
            source_file: preprocessor.source_file.clone(),
            name: preprocessor.entry.name.clone(),
            slug: preprocessor.entry.slug.clone(),
            project_id: preprocessor.project.id.clone(),
            project_name: preprocessor.project.name.clone(),
            function_id: result.id.clone(),
        });
    }
    Ok(pushed)
}

#[cfg(test)]
mod tests {
    use std::process::Command;

    use super::*;

    fn test_project() -> Project {
        Project {
            id: "proj_test".to_string(),
            name: "test-project".to_string(),
            org_id: "org_test".to_string(),
            description: None,
        }
    }

    fn test_preprocessor(slug: &str) -> PreparedPreprocessor {
        PreparedPreprocessor {
            source_file: format!("{slug}.preprocessor.ts"),
            entry: PreprocessorManifestEntry {
                name: slug.to_string(),
                slug: slug.to_string(),
                description: None,
                code: "function handler() { return []; }".to_string(),
                project_id: None,
                project_name: None,
            },
            project: test_project(),
            runtime_version: "latest".to_string(),
        }
    }

    #[test]
    fn detects_preprocessor_files() {
        assert!(is_preprocessor_file(Path::new(
            "conversation.preprocessor.ts"
        )));
        assert!(is_preprocessor_file(Path::new(
            "conversation.preprocessor.js"
        )));
        assert!(is_preprocessor_file(Path::new(
            "conversation-preprocessor.ts"
        )));
        assert!(!is_preprocessor_file(Path::new(
            "conversation.preprocessor.tsx"
        )));
        assert!(!is_preprocessor_file(Path::new("regular.ts")));
    }

    #[test]
    fn push_runner_bundles_quickjs_handler_with_swc() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.preprocessor.ts");
        std::fs::write(
            dir.path().join("roles.ts"),
            "export const assistantRole: string = \"assistant\";\n",
        )
        .expect("write dependency");
        std::fs::write(
            &path,
            r#"
import { assistantRole } from "./roles";

console.log("test discovery log");
export default {
  name: "Test Preprocessor",
  slug: "test-preprocessor",
  description: "Synthetic preprocessor",
  project: { id: "proj_test" },
  handler({ output }: { output: string }): unknown[] {
    return [{ role: assistantRole, content: output }];
  },
};
"#,
        )
        .expect("write preprocessor");

        let manifest =
            run_preprocessors_runner(std::slice::from_ref(&path)).expect("preprocessors manifest");

        assert_eq!(manifest.runtime_context.runtime, PREPROCESSOR_RUNTIME);
        assert_eq!(manifest.files.len(), 1);
        let file = &manifest.files[0];
        assert_eq!(
            file.source_file,
            path.canonicalize()
                .expect("canonical preprocessor")
                .display()
                .to_string()
        );
        assert_eq!(file.entries.len(), 1);
        let entry = &file.entries[0];
        assert_eq!(entry.name, "Test Preprocessor");
        assert_eq!(entry.slug, "test-preprocessor");
        assert_eq!(entry.description.as_deref(), Some("Synthetic preprocessor"));
        assert_eq!(entry.project_id.as_deref(), Some("proj_test"));
        assert!(entry.code.contains("function handler()"));
        assert!(!entry.code.contains(": string"));

        let output = Command::new("node")
            .arg("-e")
            .arg(format!(
                "{}\nconsole.log(JSON.stringify(handler({{ output: \"test output\" }})));",
                entry.code
            ))
            .output()
            .expect("execute preprocessor bundle");
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        let stdout = String::from_utf8(output.stdout).expect("UTF-8 output");
        let result: Value = serde_json::from_str(stdout.lines().last().expect("handler output"))
            .expect("handler result");
        assert_eq!(
            result,
            json!([{ "role": "assistant", "content": "test output" }])
        );
    }

    #[test]
    fn push_runner_rejects_invalid_definitions() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("test.preprocessor.ts");
        std::fs::write(
            &path,
            "export default { name: \"Test\", slug: \"test\" };\n",
        )
        .expect("write preprocessor");

        let err = run_preprocessors_runner(&[path]).expect_err("missing handler should fail");

        assert!(format!("{err:#}").contains("handler"));
    }

    #[test]
    fn bootstrap_template_defines_named_handler() {
        let template =
            preprocessor_bootstrap_template("Test \"Conversation\"", "test-conversation");

        assert!(template.contains(r#"name: "Test \"Conversation\"","#));
        assert!(template.contains(r#"slug: "test-conversation","#));
        assert!(template.contains("handler({ input, output, span_attributes }"));
    }

    #[test]
    fn builds_inline_preprocessor_insert_event() {
        let preprocessor = test_preprocessor("test-preprocessor");

        let event = build_insert_event(&preprocessor, IfExistsMode::Replace);
        assert_eq!(event["project_id"], "proj_test");
        assert_eq!(event["function_type"], "preprocessor");
        assert_eq!(event["if_exists"], "replace");
        assert_eq!(event["function_data"]["type"], "code");
        assert_eq!(event["function_data"]["data"]["type"], "inline");
        assert_eq!(
            event["function_data"]["data"]["runtime_context"],
            json!({ "runtime": "quickjs", "version": "latest" })
        );
        assert!(event.get("description").is_none());
        assert!(event.get("metadata").is_none());
        assert!(event.get("tags").is_none());
    }

    #[test]
    fn builds_inline_preview_invoke_request() {
        let preprocessor = LocalPreprocessor {
            name: "Test Preprocessor".to_string(),
            slug: "test-preprocessor".to_string(),
            code: "function handler() { return []; }".to_string(),
            runtime_version: "latest".to_string(),
        };

        let request =
            build_preview_invoke_request(&preprocessor, "project_logs", "proj_test", "root-span");
        assert_eq!(
            request["inline_context"],
            json!({ "runtime": "quickjs", "version": "latest" })
        );
        assert_eq!(request["function_type"], "preprocessor");
        assert_eq!(
            request["input"]["trace_ref"],
            json!({
                "object_type": "project_logs",
                "object_id": "proj_test",
                "root_span_id": "root-span",
            })
        );
    }

    #[test]
    fn formats_actionable_preprocessor_insert_error() {
        let err = anyhow!("request failed (400 Bad Request): slug already exists");

        let message = format_preprocessors_insert_error(
            &[test_preprocessor("test-preprocessor")],
            IfExistsMode::Error,
            &err,
        );

        assert!(message.contains("'test-preprocessor' in project 'test-project'"));
        assert!(message.contains("--if-exists error"));
        assert!(message.contains("bt preprocessors push --if-exists replace"));
        assert!(message.contains("slug already exists"));
    }

    #[test]
    fn push_reporting_distinguishes_ignored_preprocessors_from_replacements() {
        let preprocessors = ["existing-preprocessor", "new-preprocessor"].map(test_preprocessor);
        let results = [
            functions_api::InsertedFunctionResult {
                id: "fn_test_new".to_string(),
                project_id: "proj_test".to_string(),
                slug: "new-preprocessor".to_string(),
                found_existing: false,
            },
            functions_api::InsertedFunctionResult {
                id: "fn_test_existing".to_string(),
                project_id: "proj_test".to_string(),
                slug: "existing-preprocessor".to_string(),
                found_existing: true,
            },
        ];
        for (mode, expected) in [(IfExistsMode::Ignore, 1), (IfExistsMode::Replace, 2)] {
            let pushed =
                resolve_pushed_preprocessors(&preprocessors, &results, mode).expect("push results");
            assert_eq!(pushed.len(), expected);
            assert_eq!(pushed.last().unwrap().function_id, "fn_test_new");
        }
    }
}
