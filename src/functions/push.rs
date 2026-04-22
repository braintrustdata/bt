use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};
use std::time::{SystemTime, UNIX_EPOCH};

use std::io::IsTerminal;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use dialoguer::console::style;
use dialoguer::Confirm;
use indicatif::{ProgressBar, ProgressStyle};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::args::BaseArgs;
use crate::args::DEFAULT_API_URL;

use crate::auth::{list_available_orgs, resolve_auth};
use crate::config;
use crate::functions::report::{
    CommandStatus, FileStatus, HardFailureReason, PushFileReport, PushSummary, ReportError,
    SoftSkipReason,
};
use crate::js_runner;
use crate::projects::api::{create_project, get_project_by_name, list_projects};
use crate::python_runner;
use crate::source_language::{classify_runtime_extension, SourceLanguage};
use crate::ui::{animations_enabled, is_interactive, is_quiet};

use super::api;
use super::{
    current_org_label, resolve_auth_context, validate_explicit_org_selection, PushArgs,
    PushLanguage,
};

const FUNCTIONS_JS_RUNNER_FILE: &str = "functions-runner.ts";
const FUNCTIONS_JS_BUNDLER_FILE: &str = "functions-bundler.ts";
const FUNCTIONS_PY_RUNNER_FILE: &str = "functions-runner.py";
const RUNNER_COMMON_FILE: &str = "runner-common.ts";
const PYTHON_RUNNER_COMMON_FILE: &str = "python_runner_common.py";
const FUNCTIONS_JS_RUNNER_SOURCE: &str = include_str!("../../scripts/functions-runner.ts");
const FUNCTIONS_JS_BUNDLER_SOURCE: &str = include_str!("../../scripts/functions-bundler.ts");
const FUNCTIONS_PY_RUNNER_SOURCE: &str = include_str!("../../scripts/functions-runner.py");
const RUNNER_COMMON_SOURCE: &str = include_str!("../../scripts/runner-common.ts");
const PYTHON_RUNNER_COMMON_SOURCE: &str = include_str!("../../scripts/python_runner_common.py");
const PYTHON_BASELINE_DEPS: &[&str] =
    &["pydantic", "braintrust", "autoevals", "requests", "openai"];
// Compatibility shim for existing test harnesses and eval workflows that set
// Python interpreter via BT_EVAL_* variables. Preferred path is still
// --runner / BT_FUNCTIONS_PUSH_RUNNER.
const PYTHON_INTERPRETER_ENV_OVERRIDES: &[&str] = &["BT_EVAL_PYTHON_RUNNER", "BT_EVAL_PYTHON"];

#[derive(Debug, Deserialize)]
struct RunnerManifest {
    runtime_context: RuntimeContext,
    files: Vec<ManifestFile>,
    #[serde(default)]
    baseline_dep_versions: Vec<String>,
}

#[derive(Debug, Deserialize)]
struct RuntimeContext {
    runtime: String,
    version: String,
}

#[derive(Debug, Deserialize)]
struct ManifestFile {
    source_file: String,
    #[serde(default)]
    entries: Vec<ManifestEntry>,
    #[serde(default)]
    python_bundle: Option<PythonBundle>,
}

#[derive(Debug, Deserialize, Clone)]
struct PythonBundle {
    entry_module: String,
    #[serde(default)]
    sources: Vec<String>,
}

#[derive(Debug, Deserialize)]
#[serde(tag = "kind")]
#[allow(clippy::large_enum_variant)]
enum ManifestEntry {
    #[serde(rename = "code")]
    Code(CodeEntry),
    #[serde(rename = "function_event")]
    FunctionEvent(FunctionEventEntry),
}

#[derive(Debug, Deserialize)]
struct CodeEntry {
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    project_name: Option<String>,
    name: String,
    slug: String,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    function_type: Option<String>,
    #[serde(default)]
    if_exists: Option<String>,
    #[serde(default)]
    metadata: Option<Value>,
    #[serde(default)]
    tags: Option<Vec<String>>,
    #[serde(default)]
    function_schema: Option<Value>,
    #[serde(default)]
    location: Option<Value>,
    #[serde(default)]
    preview: Option<String>,
}

#[derive(Debug, Deserialize)]
struct FunctionEventEntry {
    #[serde(default)]
    project_id: Option<String>,
    #[serde(default)]
    project_name: Option<String>,
    event: Value,
}

#[derive(Debug, Clone)]
struct FileFailure {
    reason: HardFailureReason,
    message: String,
}

fn error_chain(err: &anyhow::Error) -> String {
    format!("{err:#}")
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProjectSelector {
    Id(String),
    Name(String),
    Fallback,
}

#[derive(Debug, Clone)]
struct ProjectPreflight {
    default_project_name: Option<String>,
    requires_default_project: bool,
    named_projects: BTreeSet<String>,
    direct_project_ids: BTreeSet<String>,
}

#[derive(Debug, Clone)]
struct ResolvedEntryTarget {
    source_file: String,
    slug: String,
    project_id: String,
}

#[derive(Debug, Clone)]
struct ResolvedFileTargets {
    source_file: String,
    entry_project_ids: Vec<String>,
}

#[derive(Debug, Clone)]
struct ResolvedManifestTargets {
    entries: Vec<ResolvedEntryTarget>,
    per_file: Vec<ResolvedFileTargets>,
}

#[derive(Debug, Default)]
struct ClassifiedFiles {
    js_like: Vec<PathBuf>,
    python: Vec<PathBuf>,
    explicit_file_inputs: usize,
    explicit_supported_files: usize,
    explicit_js_like: usize,
    explicit_python: usize,
    allowed_roots: Vec<PathBuf>,
}

impl ClassifiedFiles {
    fn files_for_language(&self, language: SourceLanguage) -> Vec<PathBuf> {
        match language {
            SourceLanguage::JsLike => self.js_like.clone(),
            SourceLanguage::Python => self.python.clone(),
        }
    }
}

pub async fn run(base: BaseArgs, args: PushArgs) -> Result<()> {
    let resolved_auth = match resolve_auth(&base).await {
        Ok(auth) => auth,
        Err(err) => {
            return fail_push(
                &base,
                0,
                HardFailureReason::AuthFailed,
                error_chain(&err),
                "failed to resolve auth context",
            );
        }
    };
    let has_app_url = resolved_auth
        .app_url
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    let custom_api_without_app_url = resolved_auth
        .api_url
        .as_deref()
        .map(str::trim)
        .map(|value| value.trim_end_matches('/'))
        .is_some_and(|api_url| {
            !api_url.eq_ignore_ascii_case(DEFAULT_API_URL.trim_end_matches('/'))
        })
        && !has_app_url;
    if custom_api_without_app_url {
        return fail_push(
            &base,
            0,
            HardFailureReason::AuthFailed,
            "functions push with a custom API URL requires --app-url or BRAINTRUST_APP_URL"
                .to_string(),
            "missing app URL for custom API URL",
        );
    }

    let explicit_org_selected = base
        .org_name
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    if explicit_org_selected {
        let available_orgs = match list_available_orgs(&base)
            .await
            .context("failed to list available orgs")
        {
            Ok(orgs) => orgs,
            Err(err) => {
                return fail_push(
                    &base,
                    0,
                    HardFailureReason::AuthFailed,
                    error_chain(&err),
                    "failed to list available orgs",
                );
            }
        };

        if let Err(err) = validate_explicit_org_selection(&base, &available_orgs) {
            return fail_push(
                &base,
                0,
                HardFailureReason::ResponseInvalid,
                error_chain(&err),
                "invalid org selection",
            );
        }
    }

    let auth_ctx = match resolve_auth_context(&base)
        .await
        .context("failed to resolve auth context")
    {
        Ok(ctx) => ctx,
        Err(err) => {
            return fail_push(
                &base,
                0,
                HardFailureReason::AuthFailed,
                error_chain(&err),
                "failed to resolve auth context",
            );
        }
    };

    let files = args.resolved_files();
    let classified = match collect_classified_files(&files) {
        Ok(files) => files,
        Err(err) => {
            return fail_push(
                &base,
                0,
                HardFailureReason::ManifestPathMissing,
                err.to_string(),
                "failed to collect input files",
            );
        }
    };
    if classified.explicit_file_inputs > 0 && classified.explicit_supported_files == 0 {
        return fail_push(
            &base,
            0,
            HardFailureReason::ManifestPathMissing,
            "no eligible source files found in explicit file inputs; supported extensions: .ts, .tsx, .js, .jsx, .py".to_string(),
            "no eligible source files found",
        );
    }

    let selected_language = match select_push_language(&args, &classified) {
        Ok(language) => language,
        Err(err) => {
            return fail_push(
                &base,
                0,
                HardFailureReason::ManifestSchemaInvalid,
                err.to_string(),
                "failed to select push language",
            );
        }
    };
    emit_language_selection_notice(&classified, selected_language);

    if !args.external_packages.is_empty() && selected_language != SourceLanguage::JsLike {
        return fail_push(
            &base,
            0,
            HardFailureReason::ManifestSchemaInvalid,
            "--external-packages can only be used when pushing JS/TS sources".to_string(),
            "invalid --external-packages usage",
        );
    }

    if args.requirements.is_some() && selected_language != SourceLanguage::Python {
        return fail_push(
            &base,
            0,
            HardFailureReason::ManifestSchemaInvalid,
            "--requirements can only be used when pushing Python sources".to_string(),
            "invalid --requirements usage",
        );
    }
    if args.tsconfig.is_some() {
        eprintln!(
            "Notice: --tsconfig is enabled for JS runner and JS bundling (TS_NODE_PROJECT/TSX_TSCONFIG_PATH)."
        );
    }
    if !args.external_packages.is_empty() {
        eprintln!("Notice: --external-packages will be applied to JS bundle builds.");
    }

    let files = classified.files_for_language(selected_language);
    if files.is_empty() {
        if args.language != PushLanguage::Auto {
            let selected = match args.language {
                PushLanguage::JavaScript => "javascript",
                PushLanguage::Python => "python",
                PushLanguage::Auto => "auto",
            };
            return fail_push(
                &base,
                0,
                HardFailureReason::ManifestPathMissing,
                format!("no eligible files matched selected language '{selected}'"),
                "no matching files for selected language",
            );
        }
        let summary = PushSummary {
            status: CommandStatus::Success,
            total_files: 0,
            uploaded_files: 0,
            failed_files: 0,
            skipped_files: 0,
            ignored_entries: 0,
            files: vec![],
            warnings: vec![],
            errors: vec![],
        };
        emit_summary(&base, &summary)?;
        return Ok(());
    }

    let manifest =
        match run_functions_runner(&args, &files, selected_language, auth_ctx.client.api_key()) {
            Ok(manifest) => manifest,
            Err(failure) => {
                return fail_push_with_all_skipped(
                    &base,
                    &files,
                    failure.reason,
                    &failure.message,
                    "skipped because manifest generation failed",
                );
            }
        };

    if let Err(failure) = validate_manifest_paths(
        &manifest,
        &files,
        selected_language,
        &classified.allowed_roots,
    ) {
        return fail_push_with_all_skipped(
            &base,
            &files,
            failure.reason,
            &failure.message,
            "skipped because manifest validation failed",
        );
    }

    let has_code_entries = manifest.files.iter().any(|file| {
        file.entries
            .iter()
            .any(|entry| matches!(entry, ManifestEntry::Code(_)))
    });
    let requirements_path = if selected_language == SourceLanguage::Python {
        if let Some(requirements) = args.requirements.as_deref() {
            if has_code_entries {
                match validate_requirements_path(requirements, &classified.allowed_roots) {
                    Ok(validated) => Some(validated),
                    Err(err) => {
                        return fail_push_with_all_skipped(
                            &base,
                            &files,
                            HardFailureReason::ManifestPathMissing,
                            &err.to_string(),
                            "skipped because requirements validation failed",
                        );
                    }
                }
            } else {
                eprintln!(
                    "Notice: ignoring --requirements because no Python code functions were discovered."
                );
                None
            }
        } else {
            None
        }
    } else {
        None
    };

    let fail_manifest_preflight = |message: String, file_message: &str| {
        fail_push_with_all_skipped(
            &base,
            &files,
            HardFailureReason::ManifestSchemaInvalid,
            &message,
            file_message,
        )
    };

    let preflight = match collect_project_preflight(&base, &manifest) {
        Ok(preflight) => preflight,
        Err(err) => {
            let message = format!("failed to resolve project selectors in manifest: {err}");
            return fail_manifest_preflight(
                message,
                "skipped because project selector preflight failed",
            );
        }
    };

    let preflight_source_files: Vec<&str> = manifest
        .files
        .iter()
        .map(|f| f.source_file.as_str())
        .collect();
    let preflight_project_names: Vec<String> = preflight.named_projects.iter().cloned().collect();

    if !args.yes && is_interactive() {
        let prompt =
            build_push_confirm_prompt(&auth_ctx, &preflight_source_files, &preflight_project_names);
        let confirmed = Confirm::new()
            .with_prompt(prompt)
            .default(false)
            .interact()?;
        if !confirmed {
            return cancel_push(&base, &files);
        }
    }

    let mut project_name_cache = match resolve_named_projects(
        &auth_ctx,
        &preflight.named_projects,
        args.create_missing_projects,
    )
    .await
    {
        Ok(cache) => cache,
        Err(err) => {
            let message = format!("failed to resolve target projects for push: {err}");
            return fail_manifest_preflight(
                message,
                "skipped because project target resolution failed",
            );
        }
    };

    if let Err(err) = validate_direct_project_ids(&auth_ctx, &preflight.direct_project_ids).await {
        let message = format!("failed to validate project ids for push: {err}");
        return fail_manifest_preflight(message, "skipped because project id validation failed");
    }

    let default_project_id = match resolve_default_project_id(&preflight, &project_name_cache) {
        Ok(id) => id,
        Err(err) => {
            let message = format!("failed to resolve default project for push: {err}");
            return fail_manifest_preflight(
                message,
                "skipped because default project resolution failed",
            );
        }
    };

    let resolved_targets = match resolve_manifest_targets(
        &auth_ctx,
        default_project_id.as_deref(),
        &manifest,
        &mut project_name_cache,
        args.create_missing_projects,
    )
    .await
    {
        Ok(targets) => targets,
        Err(err) => {
            let message = format!("failed to resolve target projects for push: {err}");
            return fail_manifest_preflight(
                message,
                "skipped because project target resolution failed",
            );
        }
    };

    if let Err(err) = validate_duplicate_slugs(&resolved_targets.entries) {
        return fail_manifest_preflight(
            err.to_string(),
            "skipped because duplicate slug validation failed",
        );
    }

    let mut summary = PushSummary {
        status: CommandStatus::Success,
        total_files: manifest.files.len(),
        uploaded_files: 0,
        failed_files: 0,
        skipped_files: 0,
        ignored_entries: 0,
        files: Vec::with_capacity(manifest.files.len()),
        warnings: vec![],
        errors: vec![],
    };

    if resolved_targets.per_file.len() != manifest.files.len() {
        return fail_manifest_preflight(
            "internal error: resolved target count did not match manifest file count".to_string(),
            "skipped because internal target resolution failed",
        );
    }

    let use_progress =
        !base.json && std::io::stderr().is_terminal() && animations_enabled() && !is_quiet();

    let file_parts: Vec<&str> = manifest
        .files
        .iter()
        .map(|f| {
            Path::new(&f.source_file)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(&f.source_file)
        })
        .collect();
    let file_label = file_parts.join(", ");

    let spinner = if use_progress {
        let spinner_style = ProgressStyle::default_spinner()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏", " "])
            .template("{spinner:.cyan} {msg}")
            .unwrap();
        let pb = ProgressBar::new_spinner();
        pb.set_style(spinner_style);
        pb.set_message(format!("Pushing {file_label}..."));
        pb.enable_steady_tick(Duration::from_millis(80));
        pb
    } else {
        ProgressBar::hidden()
    };

    for (index, (file, resolved_file)) in manifest
        .files
        .iter()
        .zip(resolved_targets.per_file.iter())
        .enumerate()
    {
        if resolved_file.source_file != file.source_file {
            spinner.finish_and_clear();
            return fail_manifest_preflight(
                "internal error: resolved target source mismatch".to_string(),
                "skipped because internal target resolution failed",
            );
        }

        let source_path = PathBuf::from(&file.source_file);
        let file_result = push_file(
            &auth_ctx,
            default_project_id.as_deref(),
            &manifest.runtime_context,
            &source_path,
            file,
            &resolved_file.entry_project_ids,
            &args,
            selected_language,
            requirements_path.as_deref(),
            &classified.allowed_roots,
            &mut project_name_cache,
            &manifest.baseline_dep_versions,
        )
        .await;

        match file_result {
            Ok(file_success) => {
                summary.ignored_entries += file_success.ignored_entries;
                summary.uploaded_files += 1;
                summary.files.push(PushFileReport {
                    source_file: file.source_file.clone(),
                    status: FileStatus::Success,
                    uploaded_entries: file_success.uploaded_entries,
                    skipped_reason: None,
                    error_reason: None,
                    bundle_id: file_success.bundle_id,
                    message: None,
                });
            }
            Err(file_failure) => {
                summary.failed_files += 1;
                summary.status = CommandStatus::Failed;
                summary.errors.push(ReportError {
                    reason: file_failure.reason,
                    message: file_failure.message.clone(),
                });
                summary.files.push(PushFileReport {
                    source_file: file.source_file.clone(),
                    status: FileStatus::Failed,
                    uploaded_entries: 0,
                    skipped_reason: None,
                    error_reason: Some(file_failure.reason),
                    bundle_id: None,
                    message: Some(file_failure.message),
                });

                if args.terminate_on_failure {
                    for remaining in manifest.files.iter().skip(index + 1) {
                        summary.skipped_files += 1;
                        summary.files.push(PushFileReport {
                            source_file: remaining.source_file.clone(),
                            status: FileStatus::Skipped,
                            uploaded_entries: 0,
                            skipped_reason: Some(SoftSkipReason::TerminatedAfterFailure),
                            error_reason: None,
                            bundle_id: None,
                            message: Some(
                                "skipped because --terminate-on-failure was set".to_string(),
                            ),
                        });
                    }
                    break;
                }
            }
        }
    }

    spinner.finish_and_clear();
    if summary.status == CommandStatus::Failed {
        eprintln!("{} Failed to push {}", style("✗").red(), file_label);
    } else {
        eprintln!("{} Successfully pushed {}", style("✓").green(), file_label);
    }

    emit_summary(&base, &summary)?;

    if summary.status == CommandStatus::Failed {
        bail!("functions push failed; see summary for details");
    }

    Ok(())
}

struct FileSuccess {
    uploaded_entries: usize,
    ignored_entries: usize,
    bundle_id: Option<String>,
}

fn default_code_location(index: usize) -> Value {
    json!({
        "type": "function",
        "index": index
    })
}

fn build_code_function_data(
    runtime_context: &RuntimeContext,
    location: Value,
    bundle_id: &str,
    preview: Option<&str>,
) -> Value {
    let mut data = Map::new();
    data.insert("type".to_string(), Value::String("bundle".to_string()));
    data.insert(
        "runtime_context".to_string(),
        json!({
            "runtime": runtime_context.runtime,
            "version": runtime_context.version,
        }),
    );
    data.insert("location".to_string(), location);
    data.insert(
        "bundle_id".to_string(),
        Value::String(bundle_id.to_string()),
    );
    if let Some(preview) = preview.map(str::trim).filter(|preview| !preview.is_empty()) {
        data.insert("preview".to_string(), Value::String(preview.to_string()));
    }

    json!({
        "type": "code",
        "data": Value::Object(data),
    })
}

#[allow(clippy::too_many_arguments)]
async fn push_file(
    auth_ctx: &super::AuthContext,
    default_project_id: Option<&str>,
    runtime_context: &RuntimeContext,
    source_path: &Path,
    manifest_file: &ManifestFile,
    entry_project_ids: &[String],
    args: &PushArgs,
    selected_language: SourceLanguage,
    requirements_path: Option<&Path>,
    allowed_roots: &[PathBuf],
    project_name_cache: &mut BTreeMap<String, String>,
    baseline_dep_versions: &[String],
) -> std::result::Result<FileSuccess, FileFailure> {
    let mut code_entries = Vec::new();
    let mut events = Vec::new();

    for (entry_index, entry) in manifest_file.entries.iter().enumerate() {
        let project_id =
            entry_project_ids
                .get(entry_index)
                .cloned()
                .ok_or_else(|| FileFailure {
                    reason: HardFailureReason::ManifestSchemaInvalid,
                    message: format!(
                        "internal error: missing resolved project id for '{}' entry {}",
                        manifest_file.source_file, entry_index
                    ),
                })?;
        match entry {
            ManifestEntry::Code(code) => code_entries.push((code, project_id)),
            ManifestEntry::FunctionEvent(event) => events.push((event, project_id)),
        }
    }

    let mut bundle_id: Option<String> = None;

    let mut function_events: Vec<Value> = Vec::new();

    if !code_entries.is_empty() {
        let (upload_bytes, content_encoding) = match selected_language {
            SourceLanguage::JsLike => {
                let bundle_bytes = build_js_bundle(source_path, args)?;
                let gzipped = gzip_bytes(&bundle_bytes).map_err(|err| FileFailure {
                    reason: HardFailureReason::BundleUploadFailed,
                    message: format!("failed to gzip {}: {err}", source_path.display()),
                })?;
                (gzipped, Some("gzip"))
            }
            SourceLanguage::Python => {
                let bundle = validate_python_bundle(manifest_file, source_path, allowed_roots)
                    .map_err(|err| FileFailure {
                        reason: HardFailureReason::ManifestSchemaInvalid,
                        message: format!("{err:#}"),
                    })?;
                let archive = build_python_bundle_archive(
                    &bundle.entry_module,
                    &bundle.sources,
                    &bundle.archive_root,
                    requirements_path,
                    args.runner.as_deref(),
                    baseline_dep_versions,
                    &runtime_context.version,
                )
                .map_err(|err| FileFailure {
                    reason: HardFailureReason::BundleUploadFailed,
                    message: format!("{err:#}"),
                })?;
                (archive, None)
            }
        };

        let slot = api::request_code_upload_slot(
            &auth_ctx.client,
            &auth_ctx.org_id,
            &runtime_context.runtime,
            &runtime_context.version,
        )
        .await
        .map_err(|err| FileFailure {
            reason: HardFailureReason::UploadSlotFailed,
            message: format!("{err:#}"),
        })?;

        api::upload_bundle(&slot.url, upload_bytes, content_encoding)
            .await
            .map_err(|err| FileFailure {
                reason: HardFailureReason::BundleUploadFailed,
                message: format!("{err:#}"),
            })?;

        bundle_id = Some(slot.bundle_id.clone());

        for (index, (code, project_id)) in code_entries.iter().enumerate() {
            let mut obj = Map::new();
            obj.insert("project_id".to_string(), Value::String(project_id.clone()));
            obj.insert("name".to_string(), Value::String(code.name.clone()));
            obj.insert("slug".to_string(), Value::String(code.slug.clone()));
            obj.insert(
                "description".to_string(),
                Value::String(code.description.clone().unwrap_or_default()),
            );
            obj.insert(
                "function_data".to_string(),
                build_code_function_data(
                    runtime_context,
                    code.location
                        .clone()
                        .unwrap_or_else(|| default_code_location(index)),
                    &slot.bundle_id,
                    code.preview.as_deref(),
                ),
            );

            if let Some(function_type) = &code.function_type {
                obj.insert(
                    "function_type".to_string(),
                    Value::String(function_type.clone()),
                );
            }
            if let Some(metadata) = &code.metadata {
                obj.insert("metadata".to_string(), metadata.clone());
            }
            if let Some(tags) = &code.tags {
                obj.insert(
                    "tags".to_string(),
                    Value::Array(tags.iter().cloned().map(Value::String).collect()),
                );
            }
            if let Some(function_schema) = &code.function_schema {
                obj.insert("function_schema".to_string(), function_schema.clone());
            }
            let if_exists = code
                .if_exists
                .as_deref()
                .map(ToOwned::to_owned)
                .unwrap_or_else(|| args.if_exists.as_str().to_string());
            obj.insert("if_exists".to_string(), Value::String(if_exists));

            function_events.push(Value::Object(obj));
        }
    }

    for (event_entry, resolved_project_id) in &events {
        let mut event = event_entry.event.clone();
        if !event.is_object() {
            return Err(FileFailure {
                reason: HardFailureReason::ManifestSchemaInvalid,
                message: "function_event entry must be a JSON object".to_string(),
            });
        }

        let mut placeholders = BTreeSet::new();
        collect_project_name_placeholders_checked(&event, &mut placeholders).map_err(|err| {
            FileFailure {
                reason: HardFailureReason::ManifestSchemaInvalid,
                message: format!("{err:#}"),
            }
        })?;

        let mut resolved_placeholders = BTreeMap::new();
        for project_name in placeholders {
            let resolved = resolve_project_id(
                &auth_ctx.client,
                default_project_id,
                None,
                Some(&project_name),
                project_name_cache,
                args.create_missing_projects,
            )
            .await
            .map_err(|err| FileFailure {
                reason: HardFailureReason::ManifestSchemaInvalid,
                message: format!("{err:#}"),
            })?;
            resolved_placeholders.insert(project_name, resolved);
        }

        replace_project_name_placeholders(&mut event, &resolved_placeholders);

        let fallback_project_id = resolved_project_id.clone();

        if let Some(object) = event.as_object_mut() {
            let needs_project_id = object
                .get("project_id")
                .and_then(Value::as_str)
                .map(|value| value.trim().is_empty())
                .unwrap_or(true);
            if needs_project_id {
                object.insert("project_id".to_string(), Value::String(fallback_project_id));
            }
            if object.get("if_exists").is_none() {
                object.insert(
                    "if_exists".to_string(),
                    Value::String(args.if_exists.as_str().to_string()),
                );
            }
        }

        function_events.push(event);
    }

    if function_events.is_empty() {
        return Ok(FileSuccess {
            uploaded_entries: 0,
            ignored_entries: 0,
            bundle_id,
        });
    }

    let insert_result = api::insert_functions(&auth_ctx.client, &function_events)
        .await
        .map_err(|err| FileFailure {
            reason: HardFailureReason::InsertFunctionsFailed,
            message: {
                let details = format!("{err:#}");
                if let Some(id) = &bundle_id {
                    format!(
                        "failed to save function definitions for {} (bundle_id={}): {}. Retry by re-running `bt functions push --file {}`",
                        source_path.display(),
                        id,
                        details,
                        source_path.display()
                    )
                } else {
                    format!(
                        "failed to save function definitions for {}: {}",
                        source_path.display(),
                        details
                    )
                }
            },
        })?;

    let (uploaded_entries, ignored_entries) =
        calculate_upload_counts(function_events.len(), insert_result.ignored_entries);

    Ok(FileSuccess {
        uploaded_entries,
        ignored_entries,
        bundle_id,
    })
}

fn build_js_bundle(
    source_path: &Path,
    args: &PushArgs,
) -> std::result::Result<Vec<u8>, FileFailure> {
    let build_dir = TempBuildDir::create("bt-functions-js-bundle").map_err(|err| FileFailure {
        reason: HardFailureReason::BundleUploadFailed,
        message: format!("{err:#}"),
    })?;
    let output_bundle = build_dir.path.join("bundle.js");

    let bundler_script = js_runner::materialize_runner_script_in_cwd(
        "functions-runners",
        FUNCTIONS_JS_BUNDLER_FILE,
        FUNCTIONS_JS_BUNDLER_SOURCE,
    )
    .map_err(|err| FileFailure {
        reason: HardFailureReason::RunnerSpawnFailed,
        message: format!("failed to materialize JS bundler script: {err}"),
    })?;

    let mut command = js_runner::build_js_runner_command(
        args.runner.as_deref(),
        &bundler_script,
        &[source_path.to_path_buf(), output_bundle.clone()],
    );
    if let Some(tsconfig) = &args.tsconfig {
        command.env("TS_NODE_PROJECT", tsconfig);
        command.env("TSX_TSCONFIG_PATH", tsconfig);
    }
    if !args.external_packages.is_empty() {
        command.env(
            "BT_FUNCTIONS_PUSH_EXTERNAL_PACKAGES",
            args.external_packages.join(","),
        );
    }

    let output = command.output().map_err(|err| FileFailure {
        reason: HardFailureReason::RunnerSpawnFailed,
        message: format!("failed to spawn JS bundler: {err}"),
    })?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(FileFailure {
            reason: HardFailureReason::BundleUploadFailed,
            message: format!(
                "JS bundler exited with status {}: {}",
                output.status,
                stderr.trim()
            ),
        });
    }

    std::fs::read(&output_bundle).map_err(|err| FileFailure {
        reason: HardFailureReason::BundleUploadFailed,
        message: format!(
            "failed to read bundled JS output {}: {err}",
            output_bundle.display()
        ),
    })
}

fn calculate_upload_counts(total_entries: usize, ignored_entries: Option<usize>) -> (usize, usize) {
    let ignored_entries = ignored_entries.unwrap_or(0);
    let uploaded_entries = total_entries.saturating_sub(ignored_entries);
    (uploaded_entries, ignored_entries)
}

fn run_functions_runner(
    args: &PushArgs,
    files: &[PathBuf],
    language: SourceLanguage,
    api_key: &str,
) -> std::result::Result<RunnerManifest, FileFailure> {
    let mut command = match language {
        SourceLanguage::JsLike => {
            let _common = js_runner::materialize_runner_script_in_cwd(
                "functions-runners",
                RUNNER_COMMON_FILE,
                RUNNER_COMMON_SOURCE,
            )
            .map_err(|err| FileFailure {
                reason: HardFailureReason::RunnerSpawnFailed,
                message: format!("failed to materialize shared runner helper: {err}"),
            })?;
            let runner_script = js_runner::materialize_runner_script_in_cwd(
                "functions-runners",
                FUNCTIONS_JS_RUNNER_FILE,
                FUNCTIONS_JS_RUNNER_SOURCE,
            )
            .map_err(|err| FileFailure {
                reason: HardFailureReason::RunnerSpawnFailed,
                message: format!("failed to materialize functions runner: {err}"),
            })?;
            js_runner::build_js_runner_command(args.runner.as_deref(), &runner_script, files)
        }
        SourceLanguage::Python => {
            let _common = js_runner::materialize_runner_script_in_cwd(
                "functions-runners",
                PYTHON_RUNNER_COMMON_FILE,
                PYTHON_RUNNER_COMMON_SOURCE,
            )
            .map_err(|err| FileFailure {
                reason: HardFailureReason::RunnerSpawnFailed,
                message: format!("failed to materialize shared Python runner helper: {err}"),
            })?;
            let runner_script = js_runner::materialize_runner_script_in_cwd(
                "functions-runners",
                FUNCTIONS_PY_RUNNER_FILE,
                FUNCTIONS_PY_RUNNER_SOURCE,
            )
            .map_err(|err| FileFailure {
                reason: HardFailureReason::RunnerSpawnFailed,
                message: format!("failed to materialize Python functions runner: {err}"),
            })?;
            let Some(python) = python_runner::resolve_python_interpreter(
                args.runner.as_deref(),
                PYTHON_INTERPRETER_ENV_OVERRIDES,
            ) else {
                return Err(FileFailure {
                    reason: HardFailureReason::RunnerSpawnFailed,
                    message: "No Python interpreter found. Install python or pass --runner."
                        .to_string(),
                });
            };
            let mut command = Command::new(python);
            command.arg(runner_script);
            for file in files {
                command.arg(file);
            }
            command
        }
    };

    command.env("BRAINTRUST_API_KEY", api_key);
    if let Some(tsconfig) = &args.tsconfig {
        command.env("TS_NODE_PROJECT", tsconfig);
        command.env("TSX_TSCONFIG_PATH", tsconfig);
    }
    if !args.external_packages.is_empty() {
        command.env(
            "BT_FUNCTIONS_PUSH_EXTERNAL_PACKAGES",
            args.external_packages.join(","),
        );
    }

    let output = command.output().map_err(|err| FileFailure {
        reason: HardFailureReason::RunnerSpawnFailed,
        message: format!("failed to spawn functions runner: {err}"),
    })?;

    parse_runner_manifest_output(output)
}

fn parse_runner_manifest_output(
    output: Output,
) -> std::result::Result<RunnerManifest, FileFailure> {
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(FileFailure {
            reason: HardFailureReason::RunnerExitNonzero,
            message: format!(
                "runner exited with status {}: {}",
                output.status,
                stderr.trim()
            ),
        });
    }

    let stdout = String::from_utf8(output.stdout).map_err(|err| FileFailure {
        reason: HardFailureReason::ManifestInvalidJson,
        message: format!("runner output was not valid UTF-8: {err}"),
    })?;
    serde_json::from_str(&stdout).map_err(|err| FileFailure {
        reason: HardFailureReason::ManifestInvalidJson,
        message: format!("failed to parse functions runner manifest JSON: {err}"),
    })
}

fn classify_source_file(path: &Path) -> Option<SourceLanguage> {
    path.extension()
        .and_then(|ext| ext.to_str())
        .and_then(classify_runtime_extension)
}

fn collect_classified_files(inputs: &[PathBuf]) -> Result<ClassifiedFiles> {
    let mut js_like = BTreeSet::new();
    let mut python = BTreeSet::new();
    let mut allowed_roots = BTreeSet::new();
    if let Ok(cwd) = std::env::current_dir() {
        if let Ok(canonical_cwd) = cwd.canonicalize() {
            allowed_roots.insert(canonical_cwd);
        }
    }
    let mut explicit_file_inputs = 0usize;
    let mut explicit_supported_files = 0usize;
    let mut explicit_js_like = 0usize;
    let mut explicit_python = 0usize;

    for input in inputs {
        let path = if input.is_absolute() {
            input.clone()
        } else {
            std::env::current_dir()
                .context("failed to resolve current directory")?
                .join(input)
        };

        if !path.exists() {
            bail!("path does not exist: {}", input.display());
        }

        if path.is_file() {
            explicit_file_inputs += 1;
            let canonical = path
                .canonicalize()
                .with_context(|| format!("failed to canonicalize file {}", path.display()))?;
            let parent = canonical
                .parent()
                .map(Path::to_path_buf)
                .ok_or_else(|| anyhow!("failed to find parent dir for {}", canonical.display()))?;
            allowed_roots.insert(parent);
            match classify_source_file(&canonical) {
                Some(SourceLanguage::JsLike) => {
                    explicit_supported_files += 1;
                    explicit_js_like += 1;
                    js_like.insert(canonical);
                }
                Some(SourceLanguage::Python) => {
                    explicit_supported_files += 1;
                    explicit_python += 1;
                    python.insert(canonical);
                }
                None => {}
            }
            continue;
        }

        let canonical_dir = path
            .canonicalize()
            .with_context(|| format!("failed to canonicalize directory {}", path.display()))?;
        allowed_roots.insert(canonical_dir.clone());
        collect_from_dir(&canonical_dir, &mut js_like, &mut python)?;
    }

    Ok(ClassifiedFiles {
        js_like: js_like.into_iter().collect(),
        python: python.into_iter().collect(),
        explicit_file_inputs,
        explicit_supported_files,
        explicit_js_like,
        explicit_python,
        allowed_roots: allowed_roots.into_iter().collect(),
    })
}

const MAX_DIR_DEPTH: usize = 256;

fn collect_from_dir(
    dir: &Path,
    js_like: &mut BTreeSet<PathBuf>,
    python: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    collect_from_dir_inner(dir, js_like, python, 0)
}

fn collect_from_dir_inner(
    dir: &Path,
    js_like: &mut BTreeSet<PathBuf>,
    python: &mut BTreeSet<PathBuf>,
    depth: usize,
) -> Result<()> {
    if depth > MAX_DIR_DEPTH {
        bail!(
            "directory traversal exceeded maximum depth ({}); possible symlink loop at {}",
            MAX_DIR_DEPTH,
            dir.display()
        );
    }
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("failed to read directory {}", dir.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read entry in {}", dir.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to read file type in {}", dir.display()))?;
        let path = entry.path();
        if file_type.is_dir() && !file_type.is_symlink() {
            collect_from_dir_inner(&path, js_like, python, depth + 1)?;
        } else if file_type.is_file() {
            let canonical = path
                .canonicalize()
                .with_context(|| format!("failed to canonicalize file {}", path.display()))?;
            match classify_source_file(&canonical) {
                Some(SourceLanguage::JsLike) => {
                    js_like.insert(canonical);
                }
                Some(SourceLanguage::Python) => {
                    python.insert(canonical);
                }
                None => {}
            }
        }
    }

    Ok(())
}

fn select_push_language(args: &PushArgs, files: &ClassifiedFiles) -> Result<SourceLanguage> {
    if files.explicit_js_like > 0 && files.explicit_python > 0 {
        bail!(
            "mixed source languages are not supported in one push invocation; run separate commands for Python and JS/TS files"
        );
    }

    match args.language {
        PushLanguage::Auto => {
            if !files.js_like.is_empty() && !files.python.is_empty() {
                bail!(
                    "mixed source languages are not supported in one push invocation; run separate commands for Python and JS/TS files"
                );
            } else if !files.python.is_empty() {
                Ok(SourceLanguage::Python)
            } else {
                Ok(SourceLanguage::JsLike)
            }
        }
        PushLanguage::JavaScript => Ok(SourceLanguage::JsLike),
        PushLanguage::Python => Ok(SourceLanguage::Python),
    }
}

fn emit_language_selection_notice(files: &ClassifiedFiles, selected_language: SourceLanguage) {
    let has_mixed = !files.js_like.is_empty() && !files.python.is_empty();
    if !has_mixed {
        return;
    }

    let (selected_count, skipped_count, skipped_label) = match selected_language {
        SourceLanguage::JsLike => (files.js_like.len(), files.python.len(), "python"),
        SourceLanguage::Python => (files.python.len(), files.js_like.len(), "js/ts"),
    };

    if skipped_count > 0 {
        eprintln!(
            "Notice: selected {} runtime; processing {selected_count} files and skipping {skipped_count} {skipped_label} files.",
            language_label(selected_language)
        );
    }
}

fn language_label(language: SourceLanguage) -> &'static str {
    match language {
        SourceLanguage::JsLike => "javascript",
        SourceLanguage::Python => "python",
    }
}

fn validate_manifest_paths(
    manifest: &RunnerManifest,
    files: &[PathBuf],
    language: SourceLanguage,
    allowed_roots: &[PathBuf],
) -> std::result::Result<(), FileFailure> {
    let expected: BTreeSet<PathBuf> = files.iter().cloned().collect();
    let mut seen = BTreeSet::new();

    for file in &manifest.files {
        let path = PathBuf::from(&file.source_file)
            .canonicalize()
            .map_err(|err| FileFailure {
                reason: HardFailureReason::ManifestPathMissing,
                message: format!("manifest source file missing: {} ({err})", file.source_file),
            })?;
        if !expected.contains(&path) {
            return Err(FileFailure {
                reason: HardFailureReason::ManifestPathMissing,
                message: format!("manifest referenced unexpected file: {}", path.display()),
            });
        }
        let has_code_entries = file
            .entries
            .iter()
            .any(|entry| matches!(entry, ManifestEntry::Code(_)));
        if language != SourceLanguage::Python && file.python_bundle.is_some() {
            return Err(FileFailure {
                reason: HardFailureReason::ManifestSchemaInvalid,
                message: format!(
                    "manifest file '{}' contained python_bundle metadata for non-Python runtime",
                    file.source_file
                ),
            });
        }
        if language == SourceLanguage::Python && !has_code_entries && file.python_bundle.is_some() {
            return Err(FileFailure {
                reason: HardFailureReason::ManifestSchemaInvalid,
                message: format!(
                    "manifest file '{}' contained python_bundle metadata without code entries",
                    file.source_file
                ),
            });
        }
        if language == SourceLanguage::Python && has_code_entries {
            validate_python_bundle(file, &path, allowed_roots).map_err(|err| FileFailure {
                reason: HardFailureReason::ManifestSchemaInvalid,
                message: format!("{err:#}"),
            })?;
        }
        seen.insert(path);
    }

    if let Some(missing) = expected.difference(&seen).next() {
        return Err(FileFailure {
            reason: HardFailureReason::ManifestPathMissing,
            message: format!("manifest missing expected file: {}", missing.display()),
        });
    }

    Ok(())
}

#[derive(Debug)]
struct ValidatedPythonBundle {
    entry_module: String,
    sources: Vec<PathBuf>,
    archive_root: PathBuf,
}

fn validate_python_bundle(
    manifest_file: &ManifestFile,
    source_path: &Path,
    allowed_roots: &[PathBuf],
) -> Result<ValidatedPythonBundle> {
    let python_bundle = manifest_file.python_bundle.as_ref().ok_or_else(|| {
        anyhow!(
            "manifest file '{}' includes Python code entries but is missing python_bundle metadata",
            manifest_file.source_file
        )
    })?;
    let entry_module = python_bundle.entry_module.trim();
    if entry_module.is_empty() {
        bail!(
            "manifest file '{}' has empty python_bundle.entry_module",
            manifest_file.source_file
        );
    }
    if python_bundle.sources.is_empty() {
        bail!(
            "manifest file '{}' has empty python_bundle.sources",
            manifest_file.source_file
        );
    }

    let mut sources = BTreeSet::new();
    for raw_source in &python_bundle.sources {
        let canonical = PathBuf::from(raw_source).canonicalize().with_context(|| {
            format!(
                "manifest file '{}' referenced missing python source {}",
                manifest_file.source_file, raw_source
            )
        })?;
        if !canonical.is_file() {
            bail!(
                "manifest file '{}' referenced non-file python source {}",
                manifest_file.source_file,
                canonical.display()
            );
        }
        if !is_within_allowed_roots(&canonical, allowed_roots) {
            bail!(
                "manifest file '{}' referenced python source outside allowed roots: {}",
                manifest_file.source_file,
                canonical.display()
            );
        }
        sources.insert(canonical);
    }

    let source_list: Vec<PathBuf> = sources.into_iter().collect();
    let archive_root = infer_python_archive_root(entry_module, source_path)?;
    for source in &source_list {
        let archive_path = archive_source_path(source, &archive_root)?;
        validate_python_archive_path(&archive_path)?;
    }

    if !entry_module_matches_sources(entry_module, &source_list, allowed_roots) {
        bail!(
            "python_bundle.entry_module '{}' does not match any bundled source module for '{}'",
            entry_module,
            source_path.display()
        );
    }

    Ok(ValidatedPythonBundle {
        entry_module: entry_module.to_string(),
        sources: source_list,
        archive_root,
    })
}

fn infer_python_archive_root(entry_module: &str, source_path: &Path) -> Result<PathBuf> {
    let module_parts = entry_module
        .split('.')
        .filter(|part| !part.trim().is_empty())
        .collect::<Vec<_>>();
    if module_parts.is_empty() {
        bail!("python_bundle.entry_module cannot be empty");
    }

    let parent = source_path
        .parent()
        .ok_or_else(|| anyhow!("source file has no parent: {}", source_path.display()))?;
    let file_name = source_path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| {
            anyhow!(
                "source file has invalid utf-8 name: {}",
                source_path.display()
            )
        })?;

    let module_depth = if file_name == "__init__.py" {
        module_parts.len()
    } else {
        module_parts.len().saturating_sub(1)
    };

    let mut root = parent.to_path_buf();
    for _ in 0..module_depth {
        root = root.parent().map(Path::to_path_buf).ok_or_else(|| {
            anyhow!(
                "failed to infer archive root for module '{}' from source '{}'",
                entry_module,
                source_path.display()
            )
        })?;
    }

    Ok(root)
}

fn is_within_allowed_roots(path: &Path, allowed_roots: &[PathBuf]) -> bool {
    allowed_roots.iter().any(|root| path.starts_with(root))
}

fn entry_module_matches_sources(
    entry_module: &str,
    sources: &[PathBuf],
    allowed_roots: &[PathBuf],
) -> bool {
    let entry_tail = entry_module
        .rsplit('.')
        .next()
        .unwrap_or(entry_module)
        .trim();
    if entry_tail.is_empty() {
        return false;
    }

    for source in sources {
        if source
            .file_stem()
            .and_then(|stem| stem.to_str())
            .is_some_and(|stem| stem == entry_tail)
        {
            return true;
        }

        for root in allowed_roots {
            if let Some(candidate) = module_name_for_source(source, root) {
                if candidate == entry_module {
                    return true;
                }
            }
        }
    }

    false
}

fn module_name_for_source(source: &Path, root: &Path) -> Option<String> {
    let rel = source.strip_prefix(root).ok()?;
    if rel.extension().and_then(|ext| ext.to_str()) != Some("py") {
        return None;
    }

    let mut parts = Vec::new();
    let components: Vec<_> = rel.iter().collect();
    if components.is_empty() {
        return None;
    }
    for (index, component) in components.iter().enumerate() {
        let component = component.to_str()?;
        if component.is_empty() {
            return None;
        }
        if index + 1 == components.len() {
            let stem = component.strip_suffix(".py").unwrap_or(component);
            if stem != "__init__" {
                parts.push(stem.to_string());
            }
        } else {
            parts.push(component.to_string());
        }
    }

    if parts.is_empty() {
        None
    } else {
        Some(parts.join("."))
    }
}

struct TempBuildDir {
    path: PathBuf,
}

impl TempBuildDir {
    fn create(prefix: &str) -> Result<Self> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .context("failed to read system clock")?
            .as_nanos();
        let path = std::env::temp_dir().join(format!("{prefix}-{}-{now}", std::process::id()));
        std::fs::create_dir_all(&path)
            .with_context(|| format!("failed to create temp directory {}", path.display()))?;
        Ok(Self { path })
    }
}

impl Drop for TempBuildDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.path);
    }
}

fn build_python_bundle_archive(
    entry_module: &str,
    sources: &[PathBuf],
    archive_root: &Path,
    requirements_path: Option<&Path>,
    runner: Option<&str>,
    baseline_dep_versions: &[String],
    python_version: &str,
) -> Result<Vec<u8>> {
    let Some(python) =
        python_runner::resolve_python_interpreter(runner, PYTHON_INTERPRETER_ENV_OVERRIDES)
    else {
        bail!("No Python interpreter found. Install python or pass --runner.")
    };

    let build_dir = TempBuildDir::create("bt-functions-python-bundle")?;
    let pkg_dir = build_dir.path.join("pkg");
    std::fs::create_dir_all(&pkg_dir)
        .with_context(|| format!("failed to create {}", pkg_dir.display()))?;

    install_python_dependencies(
        &pkg_dir,
        requirements_path,
        &python,
        baseline_dep_versions,
        python_version,
    )?;
    ensure_python_package_staged(&pkg_dir, &python, "braintrust")
        .context("failed to stage required Python package 'braintrust'")?;

    let stage_dir = build_dir.path.join("stage");
    std::fs::create_dir_all(&stage_dir)
        .with_context(|| format!("failed to create {}", stage_dir.display()))?;

    copy_directory_files_into_stage(&pkg_dir, &stage_dir)?;
    for source in sources {
        let archive_path = archive_source_path(source, archive_root)?;
        copy_file_into_stage(source, &archive_path, &stage_dir)?;
    }
    std::fs::write(
        stage_dir.join("register.py"),
        format!("import {entry_module} as _\n"),
    )
    .context("failed to write register.py")?;

    let zip_path = build_dir.path.join("pkg.zip");
    create_zip_with_python(&python, &stage_dir, &zip_path)?;
    std::fs::read(&zip_path)
        .with_context(|| format!("failed to read generated archive {}", zip_path.display()))
}

fn archive_source_path(source: &Path, archive_root: &Path) -> Result<PathBuf> {
    let rel = source.strip_prefix(archive_root).with_context(|| {
        format!(
            "source '{}' is not under archive root '{}'",
            source.display(),
            archive_root.display()
        )
    })?;
    if rel.as_os_str().is_empty() {
        bail!(
            "refusing to archive source with empty path: {}",
            source.display()
        );
    }
    Ok(rel.to_path_buf())
}

fn validate_python_archive_path(archive_path: &Path) -> Result<()> {
    for component in archive_path.iter() {
        let component = component.to_str().ok_or_else(|| {
            anyhow!(
                "python bundle source path contains invalid utf-8: {}",
                archive_path.display()
            )
        })?;
        if component.chars().any(char::is_whitespace) {
            bail!(
                "python bundle source path '{}' contains whitespace in path component '{}'; rename the file or directory before running `bt functions push`",
                archive_path.display(),
                component
            );
        }
    }
    Ok(())
}

fn copy_directory_files_into_stage(source_root: &Path, stage_root: &Path) -> Result<()> {
    let files = collect_regular_files_recursive(source_root)?;
    for file in files {
        let rel = file
            .strip_prefix(source_root)
            .with_context(|| format!("failed to strip prefix for {}", file.display()))?;
        copy_file_into_stage(&file, rel, stage_root)?;
    }
    Ok(())
}

fn copy_file_into_stage(source: &Path, rel_path: &Path, stage_root: &Path) -> Result<()> {
    let archive_rel = normalized_archive_relative_path(rel_path)?;
    let dest = stage_root.join(archive_rel);
    if let Some(parent) = dest.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("failed to create {}", parent.display()))?;
    }
    std::fs::copy(source, &dest)
        .with_context(|| format!("failed to copy {} -> {}", source.display(), dest.display()))?;
    Ok(())
}

fn normalized_archive_relative_path(path: &Path) -> Result<PathBuf> {
    let mut out = PathBuf::new();
    for component in path.components() {
        match component {
            std::path::Component::Normal(segment) => out.push(segment),
            std::path::Component::CurDir => {}
            _ => {
                bail!("invalid archive path component in '{}'", path.display());
            }
        }
    }
    if out.as_os_str().is_empty() {
        bail!(
            "archive path resolved to empty name for '{}'",
            path.display()
        );
    }
    Ok(out)
}

fn create_zip_with_python(python: &Path, stage_root: &Path, zip_path: &Path) -> Result<()> {
    const ZIP_SCRIPT: &str = r#"import os
import sys
import zipfile

stage_root = sys.argv[1]
zip_path = sys.argv[2]

with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED, compresslevel=9) as zf:
    for root, dirs, files in os.walk(stage_root):
        dirs.sort()
        files.sort()
        for filename in files:
            source = os.path.join(root, filename)
            rel = os.path.relpath(source, stage_root)
            zf.write(source, rel)
"#;

    let output = Command::new(python)
        .arg("-c")
        .arg(ZIP_SCRIPT)
        .arg(stage_root)
        .arg(zip_path)
        .output()
        .context("failed to spawn Python archive builder")?;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let excerpt = stderr
        .lines()
        .take(20)
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string();
    if excerpt.is_empty() {
        bail!(
            "Python archive builder failed with status {}",
            output.status
        );
    }
    bail!(
        "Python archive builder failed with status {}: {}",
        output.status,
        excerpt
    );
}

fn baseline_uv_install_args(
    pkg_dir: &Path,
    python: &Path,
    baseline_dep_versions: &[String],
    python_version: &str,
) -> Vec<OsString> {
    let platform = std::env::var("BRAINTRUST_INTERNAL_PY_BUNDLE_PLATFORM_OVERRIDE")
        .unwrap_or_else(|_| "linux".to_string());
    let version = std::env::var("BRAINTRUST_INTERNAL_PY_BUNDLE_VERSION_OVERRIDE")
        .unwrap_or_else(|_| python_version.to_string());
    let mut args = vec![
        OsString::from("pip"),
        OsString::from("install"),
        OsString::from("--python"),
        python.as_os_str().to_os_string(),
        OsString::from("--target"),
        pkg_dir.as_os_str().to_os_string(),
        OsString::from("--python-platform"),
        OsString::from(&platform),
        OsString::from("--python-version"),
        OsString::from(&version),
    ];
    if baseline_dep_versions.is_empty() {
        args.extend(PYTHON_BASELINE_DEPS.iter().map(OsString::from));
    } else {
        args.extend(baseline_dep_versions.iter().map(OsString::from));
    }
    args
}

fn requirements_uv_install_args(
    pkg_dir: &Path,
    requirements: &Path,
    python: &Path,
    python_version: &str,
) -> Vec<OsString> {
    let platform = std::env::var("BRAINTRUST_INTERNAL_PY_BUNDLE_PLATFORM_OVERRIDE")
        .unwrap_or_else(|_| "linux".to_string());
    let version = std::env::var("BRAINTRUST_INTERNAL_PY_BUNDLE_VERSION_OVERRIDE")
        .unwrap_or_else(|_| python_version.to_string());
    vec![
        OsString::from("pip"),
        OsString::from("install"),
        OsString::from("--python"),
        python.as_os_str().to_os_string(),
        OsString::from("--target"),
        pkg_dir.as_os_str().to_os_string(),
        OsString::from("--python-platform"),
        OsString::from(&platform),
        OsString::from("--python-version"),
        OsString::from(&version),
        OsString::from("-r"),
        requirements.as_os_str().to_os_string(),
    ]
}

fn install_python_dependencies(
    pkg_dir: &Path,
    requirements_path: Option<&Path>,
    python: &Path,
    baseline_dep_versions: &[String],
    python_version: &str,
) -> Result<()> {
    let uv = python_runner::find_binary_in_path(&["uv"]).ok_or_else(|| {
        anyhow!("`uv` is required to build Python code bundles; please install uv")
    })?;

    let baseline_args =
        baseline_uv_install_args(pkg_dir, python, baseline_dep_versions, python_version);
    run_uv_command(
        &uv,
        &baseline_args,
        "installing baseline Python bundle dependencies",
    )?;

    if let Some(requirements) = requirements_path {
        let args = requirements_uv_install_args(pkg_dir, requirements, python, python_version);
        run_uv_command(&uv, &args, "installing requirements file dependencies")?;
    }

    Ok(())
}

fn ensure_python_package_staged(pkg_dir: &Path, python: &Path, package_name: &str) -> Result<()> {
    if python_package_staged(pkg_dir, package_name) {
        return Ok(());
    }

    vendor_python_package_from_interpreter(pkg_dir, python, package_name)?;

    if python_package_staged(pkg_dir, package_name) {
        return Ok(());
    }

    bail!(
        "python bundle staging is missing required package '{}' under {}",
        package_name,
        pkg_dir.display()
    );
}

fn python_package_staged(pkg_dir: &Path, package_name: &str) -> bool {
    pkg_dir.join(package_name).is_dir() || pkg_dir.join(format!("{package_name}.py")).is_file()
}

fn vendor_python_package_from_interpreter(
    pkg_dir: &Path,
    python: &Path,
    package_name: &str,
) -> Result<()> {
    const VENDOR_PACKAGE_SCRIPT: &str = r#"import importlib
import pathlib
import shutil
import sys

target_root = pathlib.Path(sys.argv[1])
package_name = sys.argv[2]
module = importlib.import_module(package_name)
module_file = getattr(module, "__file__", None)
if not module_file:
    raise RuntimeError(f"package {package_name!r} has no __file__")
source = pathlib.Path(module_file).resolve()

if source.name == "__init__.py":
    src_dir = source.parent
    dest = target_root / package_name
    if dest.exists():
        if dest.is_dir():
            shutil.rmtree(dest)
        else:
            dest.unlink()
    shutil.copytree(src_dir, dest)
else:
    dest = target_root / f"{package_name}.py"
    dest.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source, dest)
"#;

    let output = Command::new(python)
        .arg("-c")
        .arg(VENDOR_PACKAGE_SCRIPT)
        .arg(pkg_dir)
        .arg(package_name)
        .output()
        .with_context(|| {
            format!(
                "failed to spawn Python package vendor helper for '{}'",
                package_name
            )
        })?;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let excerpt = stderr
        .lines()
        .take(20)
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string();
    if excerpt.is_empty() {
        bail!(
            "Python package vendor helper failed with status {} for '{}'",
            output.status,
            package_name
        );
    }
    bail!(
        "Python package vendor helper failed with status {} for '{}': {}",
        output.status,
        package_name,
        excerpt
    );
}

fn run_uv_command(uv: &Path, args: &[OsString], stage: &str) -> Result<()> {
    let args_debug = args
        .iter()
        .map(|arg| arg.to_string_lossy().to_string())
        .collect::<Vec<_>>()
        .join(" ");
    let output = Command::new(uv)
        .args(args)
        .output()
        .with_context(|| format!("failed to run `{} {args_debug}`", uv.display()))?;
    if output.status.success() {
        return Ok(());
    }

    let stderr = String::from_utf8_lossy(&output.stderr);
    let excerpt = stderr
        .lines()
        .take(20)
        .collect::<Vec<_>>()
        .join("\n")
        .trim()
        .to_string();
    let message = if excerpt.is_empty() {
        format!("{stage} failed with status {}", output.status)
    } else {
        format!("{stage} failed with status {}: {excerpt}", output.status)
    };
    bail!(message);
}

fn collect_regular_files_recursive(root: &Path) -> Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    collect_regular_files_recursive_impl(root, &mut files, 0)?;
    files.sort();
    Ok(files)
}

fn collect_regular_files_recursive_impl(
    root: &Path,
    out: &mut Vec<PathBuf>,
    depth: usize,
) -> Result<()> {
    if depth > MAX_DIR_DEPTH {
        bail!(
            "directory traversal exceeded maximum depth ({}); possible symlink loop at {}",
            MAX_DIR_DEPTH,
            root.display()
        );
    }
    for entry in
        std::fs::read_dir(root).with_context(|| format!("failed to read {}", root.display()))?
    {
        let entry = entry.with_context(|| format!("failed to read entry in {}", root.display()))?;
        let file_type = entry
            .file_type()
            .with_context(|| format!("failed to read file type in {}", root.display()))?;
        let path = entry.path();
        if file_type.is_dir() && !file_type.is_symlink() {
            collect_regular_files_recursive_impl(&path, out, depth + 1)?;
        } else if file_type.is_file() {
            out.push(path);
        }
    }
    Ok(())
}

fn validate_requirements_path(path: &Path, allowed_roots: &[PathBuf]) -> Result<PathBuf> {
    let canonical = path
        .canonicalize()
        .with_context(|| format!("requirements file not found: {}", path.display()))?;
    if !canonical.is_file() {
        bail!("requirements path is not a file: {}", canonical.display());
    }
    let mut visited = BTreeSet::new();
    validate_requirements_local_refs(&canonical, allowed_roots, &mut visited)?;
    Ok(canonical)
}

fn validate_requirements_local_refs(
    path: &Path,
    allowed_roots: &[PathBuf],
    visited: &mut BTreeSet<PathBuf>,
) -> Result<()> {
    if !visited.insert(path.to_path_buf()) {
        return Ok(());
    }

    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("requirements path has no parent: {}", path.display()))?;
    let content = std::fs::read_to_string(path)
        .with_context(|| format!("failed to read requirements file {}", path.display()))?;

    for (line_index, raw_line) in content.lines().enumerate() {
        let line = strip_requirement_comment(raw_line).trim();
        if line.is_empty() {
            continue;
        }

        if let Some(reference) = parse_requirement_include(line) {
            let resolved = resolve_requirement_path(reference, parent)?;
            ensure_path_within_allowed_roots(&resolved, allowed_roots, path, line_index + 1)?;
            validate_requirements_local_refs(&resolved, allowed_roots, visited)?;
            continue;
        }

        if let Some(reference) = parse_editable_local_path(line) {
            let resolved = resolve_requirement_path(reference, parent)?;
            ensure_path_within_allowed_roots(&resolved, allowed_roots, path, line_index + 1)?;
            continue;
        }

        if let Some(reference) = parse_local_dependency_path(line) {
            let resolved = resolve_requirement_path(reference, parent)?;
            ensure_path_within_allowed_roots(&resolved, allowed_roots, path, line_index + 1)?;
        }
    }

    Ok(())
}

fn strip_requirement_comment(line: &str) -> &str {
    line.split_once('#').map_or(line, |(head, _)| head)
}

fn parse_requirement_include(line: &str) -> Option<&str> {
    let mut parts = line.split_whitespace();
    let first = parts.next()?;
    match first {
        "-r" | "--requirement" | "-c" | "--constraint" => parts.next(),
        _ => first
            .strip_prefix("-r")
            .or_else(|| first.strip_prefix("-c"))
            .or_else(|| first.strip_prefix("--requirement="))
            .or_else(|| first.strip_prefix("--constraint="))
            .filter(|value| !value.is_empty()),
    }
}

fn parse_editable_local_path(line: &str) -> Option<&str> {
    let mut parts = line.split_whitespace();
    let first = parts.next()?;
    let value = match first {
        "-e" | "--editable" => parts.next(),
        _ => first
            .strip_prefix("-e")
            .or_else(|| first.strip_prefix("--editable="))
            .filter(|value| !value.is_empty()),
    }?;
    if is_local_path_spec(value) {
        Some(value)
    } else {
        None
    }
}

fn parse_local_dependency_path(line: &str) -> Option<&str> {
    let spec = line.split(';').next()?.trim();
    if is_local_path_spec(spec) {
        Some(spec)
    } else {
        None
    }
}

fn is_local_path_spec(spec: &str) -> bool {
    if spec.is_empty() {
        return false;
    }
    if spec.starts_with("file:") {
        return true;
    }
    if spec.contains("://") {
        return false;
    }
    spec.starts_with("./")
        || spec.starts_with("../")
        || spec.starts_with('/')
        || spec.starts_with("~/")
        || spec.contains('/')
        || spec.contains('\\')
        || spec.ends_with(".whl")
        || spec.ends_with(".tar.gz")
        || spec.ends_with(".zip")
}

fn resolve_requirement_path(reference: &str, parent: &Path) -> Result<PathBuf> {
    let normalized = reference.trim();
    if normalized.is_empty() {
        bail!("empty requirements reference");
    }

    let candidate = if let Some(file) = normalized.strip_prefix("file://") {
        PathBuf::from(file)
    } else if let Some(file) = normalized.strip_prefix("file:") {
        PathBuf::from(file)
    } else if let Some(home_relative) = normalized.strip_prefix("~/") {
        let home =
            dirs::home_dir().ok_or_else(|| anyhow!("unable to resolve HOME for {}", normalized))?;
        home.join(home_relative)
    } else {
        PathBuf::from(normalized)
    };

    let absolute = if candidate.is_absolute() {
        candidate
    } else {
        parent.join(candidate)
    };
    absolute
        .canonicalize()
        .with_context(|| format!("failed to resolve requirements reference {}", normalized))
}

fn ensure_path_within_allowed_roots(
    path: &Path,
    allowed_roots: &[PathBuf],
    requirements_path: &Path,
    line_number: usize,
) -> Result<()> {
    if is_within_allowed_roots(path, allowed_roots) {
        return Ok(());
    }
    bail!(
        "requirements reference escapes allowed roots at {}:{} -> {}",
        requirements_path.display(),
        line_number,
        path.display()
    );
}

fn build_push_confirm_prompt(
    auth_ctx: &super::AuthContext,
    source_files: &[&str],
    project_names: &[String],
) -> String {
    let file_names: Vec<&str> = source_files
        .iter()
        .map(|f| {
            Path::new(f)
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or(f)
        })
        .collect();
    let files_part = file_names
        .iter()
        .map(|f| style(f).green().to_string())
        .collect::<Vec<_>>()
        .join(", ");
    let org_label = current_org_label(auth_ctx);
    let targets_part = if project_names.is_empty() {
        style(&org_label).green().to_string()
    } else {
        project_names
            .iter()
            .map(|p| style(format!("{org_label}/{p}")).green().to_string())
            .collect::<Vec<_>>()
            .join(", ")
    };

    format!("Push {files_part} to {targets_part}")
}

fn cancel_push(base: &BaseArgs, files: &[PathBuf]) -> Result<()> {
    if base.json {
        let summary = PushSummary {
            status: CommandStatus::Failed,
            total_files: files.len(),
            uploaded_files: 0,
            failed_files: 0,
            skipped_files: files.len(),
            ignored_entries: 0,
            files: files
                .iter()
                .map(|path| PushFileReport {
                    source_file: path.display().to_string(),
                    status: FileStatus::Skipped,
                    uploaded_entries: 0,
                    skipped_reason: Some(SoftSkipReason::TerminatedAfterFailure),
                    error_reason: Some(HardFailureReason::UserCancelled),
                    bundle_id: None,
                    message: Some("push cancelled by user".to_string()),
                })
                .collect(),
            warnings: vec![],
            errors: vec![ReportError {
                reason: HardFailureReason::UserCancelled,
                message: "push cancelled by user".to_string(),
            }],
        };
        emit_summary(base, &summary)?;
    } else {
        eprintln!("Push cancelled. No changes were made.");
    }

    bail!("push cancelled by user");
}

fn resolve_default_project_name(base: &BaseArgs) -> Result<Option<String>> {
    let configured = base
        .project
        .clone()
        .or_else(|| config::load().ok().and_then(|value| value.project));
    let Some(configured) = configured else {
        return Ok(None);
    };
    let trimmed = configured.trim();
    if trimmed.is_empty() {
        bail!("default project name cannot be empty");
    }
    Ok(Some(trimmed.to_string()))
}

fn collect_project_preflight(
    base: &BaseArgs,
    manifest: &RunnerManifest,
) -> Result<ProjectPreflight> {
    let default_project_name = resolve_default_project_name(base)?;
    let mut requires_default_project = false;
    let mut named_projects = BTreeSet::new();
    let mut direct_project_ids = BTreeSet::new();
    for file in &manifest.files {
        for entry in &file.entries {
            let selector = match entry {
                ManifestEntry::Code(code) => project_selector_for_code(code)?,
                ManifestEntry::FunctionEvent(event) => {
                    let mut placeholders = BTreeSet::new();
                    collect_project_name_placeholders_checked(&event.event, &mut placeholders)?;
                    named_projects.extend(placeholders);
                    project_selector_for_event(event)?
                }
            };

            add_selector_requirement(
                file,
                entry_slug(entry)?,
                &selector,
                default_project_name.as_deref(),
                &mut named_projects,
                &mut direct_project_ids,
                &mut requires_default_project,
            )?;
        }
    }

    Ok(ProjectPreflight {
        default_project_name,
        requires_default_project,
        named_projects,
        direct_project_ids,
    })
}

fn entry_slug(entry: &ManifestEntry) -> Result<&str> {
    match entry {
        ManifestEntry::Code(code) => Ok(code.slug.as_str()),
        ManifestEntry::FunctionEvent(event) => event
            .event
            .get("slug")
            .and_then(Value::as_str)
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .ok_or_else(|| anyhow!("function_event missing non-empty slug")),
    }
}

fn add_selector_requirement(
    file: &ManifestFile,
    slug: &str,
    selector: &ProjectSelector,
    default_project_name: Option<&str>,
    named_projects: &mut BTreeSet<String>,
    direct_project_ids: &mut BTreeSet<String>,
    requires_default_project: &mut bool,
) -> Result<()> {
    match selector {
        ProjectSelector::Id(project_id) => {
            direct_project_ids.insert(project_id.clone());
        }
        ProjectSelector::Name(project_name) => {
            named_projects.insert(project_name.clone());
        }
        ProjectSelector::Fallback => {
            let Some(default_project_name) = default_project_name else {
                bail!(
                    "missing project for slug '{}' in '{}'; set project in the definition or pass --project",
                    slug,
                    file.source_file
                );
            };
            *requires_default_project = true;
            named_projects.insert(default_project_name.to_string());
        }
    }
    Ok(())
}

fn normalize_project_id_field(project_id: Option<&str>) -> Result<Option<String>> {
    let Some(project_id) = project_id else {
        return Ok(None);
    };
    let trimmed = project_id.trim();
    if trimmed.is_empty() {
        return Ok(None);
    }
    if let Some(name) = trimmed.strip_prefix("name:") {
        let name = name.trim();
        if name.is_empty() {
            bail!("invalid project selector '{trimmed}': expected non-empty name after 'name:'");
        }
        return Ok(Some(format!("name:{name}")));
    }
    Ok(Some(trimmed.to_string()))
}

fn normalize_project_name_field(project_name: Option<&str>) -> Result<Option<String>> {
    let Some(project_name) = project_name else {
        return Ok(None);
    };
    let trimmed = project_name.trim();
    if trimmed.is_empty() {
        bail!("project_name cannot be empty when provided");
    }
    Ok(Some(trimmed.to_string()))
}

fn parse_project_selector(
    project_id: Option<&str>,
    project_name: Option<&str>,
) -> Result<ProjectSelector> {
    let normalized_id = normalize_project_id_field(project_id)?;
    if let Some(project_id) = normalized_id {
        if let Some(name) = project_id.strip_prefix("name:") {
            return Ok(ProjectSelector::Name(name.to_string()));
        }
        return Ok(ProjectSelector::Id(project_id));
    }

    let normalized_name = normalize_project_name_field(project_name)?;
    if let Some(project_name) = normalized_name {
        return Ok(ProjectSelector::Name(project_name));
    }

    Ok(ProjectSelector::Fallback)
}

fn project_selector_for_code(code: &CodeEntry) -> Result<ProjectSelector> {
    parse_project_selector(code.project_id.as_deref(), code.project_name.as_deref())
}

fn project_selector_for_event(event: &FunctionEventEntry) -> Result<ProjectSelector> {
    let event_project_id =
        normalize_project_id_field(event.event.get("project_id").and_then(Value::as_str))?;
    let entry_project_id = normalize_project_id_field(event.project_id.as_deref())?;
    let entry_project_name = normalize_project_name_field(event.project_name.as_deref())?;

    if let Some(project_id) = event_project_id.or(entry_project_id) {
        if let Some(name) = project_id.strip_prefix("name:") {
            return Ok(ProjectSelector::Name(name.to_string()));
        }
        return Ok(ProjectSelector::Id(project_id));
    }
    if let Some(project_name) = entry_project_name {
        return Ok(ProjectSelector::Name(project_name));
    }

    Ok(ProjectSelector::Fallback)
}

fn resolve_default_project_id(
    preflight: &ProjectPreflight,
    project_name_cache: &BTreeMap<String, String>,
) -> Result<Option<String>> {
    if !preflight.requires_default_project {
        return Ok(None);
    }

    let default_project_name = preflight
        .default_project_name
        .as_deref()
        .ok_or_else(|| anyhow!("default project is required but not configured"))?;
    let project_id = project_name_cache
        .get(default_project_name)
        .cloned()
        .ok_or_else(|| anyhow!("default project '{default_project_name}' was not resolved"))?;
    Ok(Some(project_id))
}

async fn resolve_named_projects(
    auth_ctx: &super::AuthContext,
    named_projects: &BTreeSet<String>,
    create_missing_projects: bool,
) -> Result<BTreeMap<String, String>> {
    let mut project_name_cache = BTreeMap::new();
    let mut missing = Vec::new();

    for project_name in named_projects {
        if let Some(project) = get_project_by_name(&auth_ctx.client, project_name).await? {
            project_name_cache.insert(project_name.clone(), project.id);
            continue;
        }

        if !create_missing_projects {
            missing.push(project_name.clone());
            continue;
        }

        match create_project(&auth_ctx.client, project_name).await {
            Ok(project) => {
                project_name_cache.insert(project_name.clone(), project.id);
            }
            Err(_) => {
                // Another writer may have created the project concurrently.
                if let Some(project) = get_project_by_name(&auth_ctx.client, project_name).await? {
                    project_name_cache.insert(project_name.clone(), project.id);
                } else {
                    bail!(
                        "failed to create project '{project_name}' in org '{}'",
                        current_org_label(auth_ctx)
                    );
                }
            }
        }
    }

    if !missing.is_empty() {
        let joined = missing.join(", ");
        let org = current_org_label(auth_ctx);
        bail!("project(s) not found in org '{org}': {joined}");
    }

    Ok(project_name_cache)
}

async fn validate_direct_project_ids(
    auth_ctx: &super::AuthContext,
    direct_project_ids: &BTreeSet<String>,
) -> Result<()> {
    if direct_project_ids.is_empty() {
        return Ok(());
    }

    let projects = list_projects(&auth_ctx.client).await?;
    let known_project_ids = projects
        .into_iter()
        .map(|project| project.id)
        .collect::<BTreeSet<_>>();

    if let Some(inaccessible) = direct_project_ids
        .iter()
        .find(|project_id| !known_project_ids.contains(project_id.as_str()))
    {
        bail!(
            "project_id '{}' is not accessible in org '{}'; verify --org and project selector",
            inaccessible,
            current_org_label(auth_ctx)
        );
    }

    Ok(())
}

async fn resolve_manifest_targets(
    auth_ctx: &super::AuthContext,
    default_project_id: Option<&str>,
    manifest: &RunnerManifest,
    project_name_cache: &mut BTreeMap<String, String>,
    create_missing_projects: bool,
) -> Result<ResolvedManifestTargets> {
    let mut entries = Vec::new();
    let mut per_file = Vec::with_capacity(manifest.files.len());

    for file in &manifest.files {
        let mut entry_project_ids = Vec::with_capacity(file.entries.len());
        for entry in &file.entries {
            let slug = entry_slug(entry)?.to_string();
            let selector = match entry {
                ManifestEntry::Code(code) => project_selector_for_code(code)?,
                ManifestEntry::FunctionEvent(event) => project_selector_for_event(event)?,
            };
            let project_id = resolve_project_selector(
                &auth_ctx.client,
                default_project_id,
                &selector,
                project_name_cache,
                create_missing_projects,
            )
            .await?;
            entry_project_ids.push(project_id.clone());
            entries.push(ResolvedEntryTarget {
                source_file: file.source_file.clone(),
                slug,
                project_id,
            });
        }

        per_file.push(ResolvedFileTargets {
            source_file: file.source_file.clone(),
            entry_project_ids,
        });
    }

    Ok(ResolvedManifestTargets { entries, per_file })
}

fn validate_duplicate_slugs(entries: &[ResolvedEntryTarget]) -> Result<()> {
    let mut seen: BTreeMap<(String, String), String> = BTreeMap::new();
    for entry in entries {
        if let Some(existing_file) = seen.get(&(entry.project_id.clone(), entry.slug.clone())) {
            bail!(
                "duplicate slug '{}' for project '{}' in files '{}' and '{}'",
                entry.slug,
                entry.project_id,
                existing_file,
                entry.source_file
            );
        }

        seen.insert(
            (entry.project_id.clone(), entry.slug.clone()),
            entry.source_file.clone(),
        );
    }

    Ok(())
}

async fn resolve_project_selector(
    client: &crate::http::ApiClient,
    default_project_id: Option<&str>,
    selector: &ProjectSelector,
    project_name_cache: &mut BTreeMap<String, String>,
    create_missing_projects: bool,
) -> Result<String> {
    match selector {
        ProjectSelector::Id(project_id) => {
            resolve_project_id(
                client,
                default_project_id,
                Some(project_id.as_str()),
                None,
                project_name_cache,
                create_missing_projects,
            )
            .await
        }
        ProjectSelector::Name(project_name) => {
            resolve_project_id(
                client,
                default_project_id,
                None,
                Some(project_name.as_str()),
                project_name_cache,
                create_missing_projects,
            )
            .await
        }
        ProjectSelector::Fallback => {
            resolve_project_id(
                client,
                default_project_id,
                None,
                None,
                project_name_cache,
                create_missing_projects,
            )
            .await
        }
    }
}

async fn resolve_project_id(
    client: &crate::http::ApiClient,
    default_project_id: Option<&str>,
    project_id: Option<&str>,
    project_name: Option<&str>,
    project_name_cache: &mut BTreeMap<String, String>,
    create_missing_projects: bool,
) -> Result<String> {
    let normalized_project_id = normalize_project_id_field(project_id)?;
    if let Some(project_id) = normalized_project_id {
        if let Some(name) = project_id.strip_prefix("name:") {
            return resolve_project_name(
                client,
                name.trim(),
                project_name_cache,
                create_missing_projects,
            )
            .await;
        }
        return Ok(project_id);
    }

    let normalized_project_name = normalize_project_name_field(project_name)?;
    if let Some(project_name) = normalized_project_name {
        return resolve_project_name(
            client,
            project_name.trim(),
            project_name_cache,
            create_missing_projects,
        )
        .await;
    }

    default_project_id.map(ToOwned::to_owned).ok_or_else(|| {
        anyhow!("project is required; set project in the definition or pass --project")
    })
}

async fn resolve_project_name(
    client: &crate::http::ApiClient,
    project_name: &str,
    project_name_cache: &mut BTreeMap<String, String>,
    create_missing_projects: bool,
) -> Result<String> {
    let project_name = project_name.trim();
    if project_name.is_empty() {
        bail!("project name cannot be empty");
    }

    if let Some(cached) = project_name_cache.get(project_name) {
        return Ok(cached.clone());
    }

    let project = if let Some(project) = get_project_by_name(client, project_name).await? {
        project
    } else if create_missing_projects {
        match create_project(client, project_name).await {
            Ok(project) => project,
            Err(_) => get_project_by_name(client, project_name)
                .await?
                .ok_or_else(|| anyhow!("failed to create project '{project_name}'"))?,
        }
    } else {
        return Err(anyhow!("project '{project_name}' not found"));
    };

    project_name_cache.insert(project_name.to_string(), project.id.clone());
    Ok(project.id)
}

fn collect_project_name_placeholders_checked(
    value: &Value,
    out: &mut BTreeSet<String>,
) -> Result<()> {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if key == "project_id" {
                    if let Some(project_id) = value.as_str() {
                        if let Some(name) = project_id.strip_prefix("name:") {
                            let name = name.trim();
                            if name.is_empty() {
                                bail!(
                                    "invalid nested project selector 'name:' in function_event payload"
                                );
                            }
                            out.insert(name.to_string());
                        }
                    }
                }
                collect_project_name_placeholders_checked(value, out)?;
            }
        }
        Value::Array(items) => {
            for item in items {
                collect_project_name_placeholders_checked(item, out)?;
            }
        }
        _ => {}
    }
    Ok(())
}

fn replace_project_name_placeholders(
    value: &mut Value,
    project_name_to_id: &BTreeMap<String, String>,
) {
    match value {
        Value::Object(map) => {
            for (key, value) in map {
                if key == "project_id" {
                    if let Some(project_id) = value.as_str() {
                        if let Some(name) = project_id.strip_prefix("name:") {
                            if let Some(resolved) = project_name_to_id.get(name.trim()) {
                                *value = Value::String(resolved.clone());
                                continue;
                            }
                        }
                    }
                }
                replace_project_name_placeholders(value, project_name_to_id);
            }
        }
        Value::Array(items) => {
            for item in items {
                replace_project_name_placeholders(item, project_name_to_id);
            }
        }
        _ => {}
    }
}

fn gzip_bytes(bytes: &[u8]) -> Result<Vec<u8>> {
    use std::io::Write;

    let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    encoder
        .write_all(bytes)
        .context("failed to write gzip input bytes")?;
    encoder.finish().context("failed to finalize gzip bytes")
}

fn emit_summary(base: &BaseArgs, summary: &PushSummary) -> Result<()> {
    if base.json {
        println!("{}", serde_json::to_string(summary)?);
    } else {
        for error in &summary.errors {
            let code = serde_json::to_value(error.reason)
                .ok()
                .and_then(|v| v.as_str().map(ToOwned::to_owned))
                .unwrap_or_else(|| format!("{:?}", error.reason));
            eprintln!("error ({code}): {}", error.message);
        }
    }
    Ok(())
}

enum FailedPushFiles<'a> {
    SingleFailed {
        total_files: usize,
        file_message: &'a str,
        reason: HardFailureReason,
    },
    AllSkipped {
        files: &'a [PathBuf],
        file_message: &'a str,
    },
}

fn emit_failed_push_summary(
    base: &BaseArgs,
    reason: HardFailureReason,
    message: &str,
    file_shape: FailedPushFiles<'_>,
) -> Result<()> {
    if !base.json {
        return Ok(());
    }

    let (total_files, files) = match file_shape {
        FailedPushFiles::SingleFailed {
            total_files,
            file_message,
            reason,
        } => (
            total_files,
            vec![PushFileReport {
                source_file: String::new(),
                status: FileStatus::Failed,
                uploaded_entries: 0,
                skipped_reason: None,
                error_reason: Some(reason),
                bundle_id: None,
                message: Some(file_message.to_string()),
            }],
        ),
        FailedPushFiles::AllSkipped {
            files,
            file_message,
        } => (
            files.len(),
            files
                .iter()
                .map(|path| PushFileReport {
                    source_file: path.display().to_string(),
                    status: FileStatus::Skipped,
                    uploaded_entries: 0,
                    skipped_reason: Some(SoftSkipReason::TerminatedAfterFailure),
                    error_reason: None,
                    bundle_id: None,
                    message: Some(file_message.to_string()),
                })
                .collect(),
        ),
    };

    let summary = PushSummary {
        status: CommandStatus::Failed,
        total_files,
        uploaded_files: 0,
        failed_files: 0,
        skipped_files: total_files,
        ignored_entries: 0,
        files,
        warnings: vec![],
        errors: vec![ReportError {
            reason,
            message: message.to_string(),
        }],
    };
    emit_summary(base, &summary)
}

fn fail_push(
    base: &BaseArgs,
    total_files: usize,
    reason: HardFailureReason,
    message: String,
    file_message: &str,
) -> Result<()> {
    emit_failed_push_summary(
        base,
        reason,
        &message,
        FailedPushFiles::SingleFailed {
            total_files,
            file_message,
            reason,
        },
    )?;
    bail!(message);
}

fn fail_push_with_all_skipped(
    base: &BaseArgs,
    files: &[PathBuf],
    reason: HardFailureReason,
    message: &str,
    file_message: &str,
) -> Result<()> {
    emit_failed_push_summary(
        base,
        reason,
        message,
        FailedPushFiles::AllSkipped {
            files,
            file_message,
        },
    )?;
    bail!(message.to_string());
}

#[cfg(test)]
mod tests {
    use crate::args::BaseArgs;
    use crate::auth::AvailableOrg;
    use crate::functions::IfExistsMode;

    use super::*;

    #[test]
    fn supported_extension_filtering() {
        assert_eq!(
            classify_source_file(Path::new("a.ts")),
            Some(SourceLanguage::JsLike)
        );
        assert_eq!(
            classify_source_file(Path::new("a.tsx")),
            Some(SourceLanguage::JsLike)
        );
        assert_eq!(
            classify_source_file(Path::new("a.js")),
            Some(SourceLanguage::JsLike)
        );
        assert_eq!(
            classify_source_file(Path::new("a.jsx")),
            Some(SourceLanguage::JsLike)
        );
        assert_eq!(
            classify_source_file(Path::new("a.py")),
            Some(SourceLanguage::Python)
        );
        assert_eq!(classify_source_file(Path::new("a.txt")), None);
    }

    #[test]
    fn parse_project_selector_rejects_empty_name_prefix() {
        let err = parse_project_selector(Some("name:   "), None).expect_err("must fail");
        assert!(err.to_string().contains("non-empty name"));
    }

    #[test]
    fn fallback_selector_requires_default_project_name() {
        let file = ManifestFile {
            source_file: "a.ts".to_string(),
            entries: vec![],
            python_bundle: None,
        };
        let mut named_projects = BTreeSet::new();
        let mut direct_project_ids = BTreeSet::new();
        let mut requires_default_project = false;

        let err = add_selector_requirement(
            &file,
            "same",
            &ProjectSelector::Fallback,
            None,
            &mut named_projects,
            &mut direct_project_ids,
            &mut requires_default_project,
        )
        .expect_err("must fail");
        assert!(err.to_string().contains("missing project"));
    }

    #[test]
    fn collect_project_preflight_uses_default_project_when_needed() {
        let mut base = test_base_args();
        base.project = Some("demo-project".to_string());
        let manifest = RunnerManifest {
            runtime_context: RuntimeContext {
                runtime: "node".to_string(),
                version: "20.0.0".to_string(),
            },
            files: vec![ManifestFile {
                source_file: "a.ts".to_string(),
                entries: vec![ManifestEntry::Code(CodeEntry {
                    project_id: None,
                    project_name: None,
                    name: "A".to_string(),
                    slug: "same".to_string(),
                    description: None,
                    function_type: Some("tool".to_string()),
                    if_exists: None,
                    metadata: None,
                    tags: None,
                    function_schema: None,
                    location: None,
                    preview: None,
                })],
                python_bundle: None,
            }],
            baseline_dep_versions: vec![],
        };

        let preflight = collect_project_preflight(&base, &manifest).expect("preflight");
        assert!(preflight.requires_default_project);
        assert!(
            preflight.named_projects.contains("demo-project"),
            "default project should be included in named set"
        );
    }

    #[test]
    fn explicit_org_validation_rejects_unknown_org() {
        let mut base = test_base_args();
        base.org_name = Some("missing-org".to_string());
        let orgs = vec![AvailableOrg {
            id: "o1".to_string(),
            name: "existing-org".to_string(),
            api_url: None,
        }];

        let err = validate_explicit_org_selection(&base, &orgs).expect_err("must fail");
        assert!(err.to_string().contains("missing-org"));
    }

    #[test]
    fn select_push_language_auto_rejects_mixed_scan() {
        let args = PushArgs {
            files: vec![PathBuf::from(".")],
            file_flag: vec![],
            if_exists: IfExistsMode::Error,
            terminate_on_failure: false,
            create_missing_projects: true,
            runner: None,
            language: PushLanguage::Auto,
            requirements: None,
            tsconfig: None,
            external_packages: vec![],
            yes: false,
        };
        let classified = ClassifiedFiles {
            js_like: vec![PathBuf::from("/tmp/a.ts")],
            python: vec![PathBuf::from("/tmp/a.py")],
            explicit_file_inputs: 0,
            explicit_supported_files: 0,
            explicit_js_like: 0,
            explicit_python: 0,
            allowed_roots: Vec::new(),
        };

        let err = select_push_language(&args, &classified).expect_err("must fail");
        assert!(err.to_string().contains("mixed source languages"));
    }

    #[test]
    fn select_push_language_rejects_mixed_explicit_files() {
        let args = PushArgs {
            files: vec![PathBuf::from("a.ts"), PathBuf::from("b.py")],
            file_flag: vec![],
            if_exists: IfExistsMode::Error,
            terminate_on_failure: false,
            create_missing_projects: true,
            runner: None,
            language: PushLanguage::Auto,
            requirements: None,
            tsconfig: None,
            external_packages: vec![],
            yes: false,
        };
        let classified = ClassifiedFiles {
            js_like: vec![PathBuf::from("/tmp/a.ts")],
            python: vec![PathBuf::from("/tmp/b.py")],
            explicit_file_inputs: 2,
            explicit_supported_files: 2,
            explicit_js_like: 1,
            explicit_python: 1,
            allowed_roots: Vec::new(),
        };

        let err = select_push_language(&args, &classified).expect_err("must fail");
        assert!(err.to_string().contains("mixed source languages"));
    }

    #[test]
    fn placeholder_rewrite_updates_nested_project_ids() {
        let mut value = serde_json::json!({
            "project_id": "name:alpha",
            "nested": {
                "tool": {
                    "project_id": "name:beta"
                }
            }
        });

        let mut mappings = BTreeMap::new();
        mappings.insert("alpha".to_string(), "p1".to_string());
        mappings.insert("beta".to_string(), "p2".to_string());

        replace_project_name_placeholders(&mut value, &mappings);

        assert_eq!(value["project_id"], "p1");
        assert_eq!(value["nested"]["tool"]["project_id"], "p2");
    }

    #[test]
    fn placeholder_rewrite_trims_nested_project_ids() {
        let mut value = serde_json::json!({
            "project_id": "name: alpha",
            "nested": {
                "tool": {
                    "project_id": "name:\tbeta   "
                }
            }
        });

        let mut mappings = BTreeMap::new();
        mappings.insert("alpha".to_string(), "p1".to_string());
        mappings.insert("beta".to_string(), "p2".to_string());

        replace_project_name_placeholders(&mut value, &mappings);

        assert_eq!(value["project_id"], "p1");
        assert_eq!(value["nested"]["tool"]["project_id"], "p2");
    }

    #[test]
    fn nested_placeholder_validation_rejects_empty_name() {
        let value = serde_json::json!({
            "project_id": "name:   "
        });
        let mut placeholders = BTreeSet::new();
        let err = collect_project_name_placeholders_checked(&value, &mut placeholders)
            .expect_err("must fail");
        assert!(err.to_string().contains("invalid nested project selector"));
    }

    #[test]
    fn upload_count_calculation_respects_ignored_entries() {
        assert_eq!(calculate_upload_counts(3, Some(1)), (2, 1));
        assert_eq!(calculate_upload_counts(3, Some(10)), (0, 10));
        assert_eq!(calculate_upload_counts(3, None), (3, 0));
    }

    #[test]
    fn requirements_reference_escape_is_rejected() {
        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("root");
        std::fs::create_dir_all(&root).expect("create root");
        let req = root.join("requirements.txt");
        std::fs::write(&req, "-r ../outside.txt\n").expect("write requirements");
        let outside = dir.path().join("outside.txt");
        std::fs::write(&outside, "requests\n").expect("write outside");

        let err =
            validate_requirements_path(&req, std::slice::from_ref(&root)).expect_err("must fail");
        assert!(err.to_string().contains("escapes allowed roots"));
    }

    #[test]
    fn validate_manifest_paths_rejects_python_bundle_for_non_python_runtime() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = dir.path().join("tool.js");
        std::fs::write(&source, "export const x = 1;\n").expect("write source file");
        let source = source.canonicalize().expect("canonicalize source");
        let root = dir.path().canonicalize().expect("canonicalize root");

        let manifest = RunnerManifest {
            runtime_context: RuntimeContext {
                runtime: "node".to_string(),
                version: "20.0.0".to_string(),
            },
            files: vec![ManifestFile {
                source_file: source.to_string_lossy().to_string(),
                entries: vec![],
                python_bundle: Some(PythonBundle {
                    entry_module: "tool".to_string(),
                    sources: vec![source.to_string_lossy().to_string()],
                }),
            }],
            baseline_dep_versions: vec![],
        };

        let err = validate_manifest_paths(
            &manifest,
            std::slice::from_ref(&source),
            SourceLanguage::JsLike,
            std::slice::from_ref(&root),
        )
        .expect_err("must fail");
        assert_eq!(err.reason, HardFailureReason::ManifestSchemaInvalid);
        assert!(err
            .message
            .contains("python_bundle metadata for non-Python"));
    }

    #[test]
    fn validate_manifest_paths_rejects_missing_python_bundle_for_code_entries() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = dir.path().join("tool.py");
        std::fs::write(&source, "VALUE = 1\n").expect("write source file");
        let source = source.canonicalize().expect("canonicalize source");
        let root = dir.path().canonicalize().expect("canonicalize root");

        let manifest = RunnerManifest {
            runtime_context: RuntimeContext {
                runtime: "python".to_string(),
                version: "3.12.0".to_string(),
            },
            files: vec![ManifestFile {
                source_file: source.to_string_lossy().to_string(),
                entries: vec![ManifestEntry::Code(CodeEntry {
                    project_id: None,
                    project_name: None,
                    name: "Tool".to_string(),
                    slug: "tool".to_string(),
                    description: None,
                    function_type: Some("tool".to_string()),
                    if_exists: None,
                    metadata: None,
                    tags: None,
                    function_schema: None,
                    location: Some(serde_json::json!({"type":"function","index":0})),
                    preview: None,
                })],
                python_bundle: None,
            }],
            baseline_dep_versions: vec![],
        };

        let err = validate_manifest_paths(
            &manifest,
            std::slice::from_ref(&source),
            SourceLanguage::Python,
            std::slice::from_ref(&root),
        )
        .expect_err("must fail");
        assert_eq!(err.reason, HardFailureReason::ManifestSchemaInvalid);
        assert!(err.message.contains("missing python_bundle metadata"));
    }

    #[test]
    fn validate_manifest_paths_accepts_valid_python_bundle() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = dir.path().join("tool.py");
        std::fs::write(&source, "VALUE = 1\n").expect("write source file");
        let source = source.canonicalize().expect("canonicalize source");
        let root = dir.path().canonicalize().expect("canonicalize root");

        let manifest = RunnerManifest {
            runtime_context: RuntimeContext {
                runtime: "python".to_string(),
                version: "3.12.0".to_string(),
            },
            files: vec![ManifestFile {
                source_file: source.to_string_lossy().to_string(),
                entries: vec![ManifestEntry::Code(CodeEntry {
                    project_id: None,
                    project_name: None,
                    name: "Tool".to_string(),
                    slug: "tool".to_string(),
                    description: None,
                    function_type: Some("tool".to_string()),
                    if_exists: None,
                    metadata: None,
                    tags: None,
                    function_schema: None,
                    location: Some(serde_json::json!({"type":"function","index":0})),
                    preview: None,
                })],
                python_bundle: Some(PythonBundle {
                    entry_module: "tool".to_string(),
                    sources: vec![source.to_string_lossy().to_string()],
                }),
            }],
            baseline_dep_versions: vec![],
        };

        validate_manifest_paths(
            &manifest,
            std::slice::from_ref(&source),
            SourceLanguage::Python,
            std::slice::from_ref(&root),
        )
        .expect("valid python bundle should pass validation");
    }

    #[test]
    fn validate_manifest_paths_rejects_entry_module_mismatch() {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = dir.path().join("tool.py");
        std::fs::write(&source, "VALUE = 1\n").expect("write source file");
        let source = source.canonicalize().expect("canonicalize source");
        let root = dir.path().canonicalize().expect("canonicalize root");

        let manifest = RunnerManifest {
            runtime_context: RuntimeContext {
                runtime: "python".to_string(),
                version: "3.12.0".to_string(),
            },
            files: vec![ManifestFile {
                source_file: source.to_string_lossy().to_string(),
                entries: vec![ManifestEntry::Code(CodeEntry {
                    project_id: None,
                    project_name: None,
                    name: "Tool".to_string(),
                    slug: "tool".to_string(),
                    description: None,
                    function_type: Some("tool".to_string()),
                    if_exists: None,
                    metadata: None,
                    tags: None,
                    function_schema: None,
                    location: Some(serde_json::json!({"type":"function","index":0})),
                    preview: None,
                })],
                python_bundle: Some(PythonBundle {
                    entry_module: "pkg.missing".to_string(),
                    sources: vec![source.to_string_lossy().to_string()],
                }),
            }],
            baseline_dep_versions: vec![],
        };

        let err = validate_manifest_paths(
            &manifest,
            std::slice::from_ref(&source),
            SourceLanguage::Python,
            std::slice::from_ref(&root),
        )
        .expect_err("must fail");
        assert_eq!(err.reason, HardFailureReason::ManifestSchemaInvalid);
        assert!(err
            .message
            .contains("does not match any bundled source module"));
    }

    fn assert_whitespace_in_filename_rejected(filename: &str, entry_module: &str) {
        let dir = tempfile::tempdir().expect("tempdir");
        let source = dir.path().join(filename);
        std::fs::write(&source, "VALUE = 1\n").expect("write source file");
        let source = source.canonicalize().expect("canonicalize source");
        let root = dir.path().canonicalize().expect("canonicalize root");

        let manifest = RunnerManifest {
            runtime_context: RuntimeContext {
                runtime: "python".to_string(),
                version: "3.12.0".to_string(),
            },
            files: vec![ManifestFile {
                source_file: source.to_string_lossy().to_string(),
                entries: vec![ManifestEntry::Code(CodeEntry {
                    project_id: None,
                    project_name: None,
                    name: "Tool".to_string(),
                    slug: "tool".to_string(),
                    description: None,
                    function_type: Some("tool".to_string()),
                    if_exists: None,
                    metadata: None,
                    tags: None,
                    function_schema: None,
                    location: Some(serde_json::json!({"type":"function","index":0})),
                    preview: None,
                })],
                python_bundle: Some(PythonBundle {
                    entry_module: entry_module.to_string(),
                    sources: vec![source.to_string_lossy().to_string()],
                }),
            }],
            baseline_dep_versions: vec![],
        };

        let err = validate_manifest_paths(
            &manifest,
            std::slice::from_ref(&source),
            SourceLanguage::Python,
            std::slice::from_ref(&root),
        )
        .expect_err("must fail");
        assert_eq!(err.reason, HardFailureReason::ManifestSchemaInvalid);
        assert!(err
            .message
            .contains("contains whitespace in path component"));
    }

    #[test]
    fn validate_manifest_paths_rejects_python_bundle_with_whitespace_in_filename() {
        assert_whitespace_in_filename_rejected("my tool.py", "my tool");
    }

    #[cfg(unix)]
    #[test]
    fn validate_manifest_paths_rejects_python_bundle_with_leading_whitespace_in_filename() {
        assert_whitespace_in_filename_rejected(" tool.py", " tool");
    }

    #[test]
    fn code_function_data_includes_non_empty_preview() {
        let runtime = RuntimeContext {
            runtime: "python".to_string(),
            version: "3.12".to_string(),
        };
        let value = build_code_function_data(
            &runtime,
            serde_json::json!({"type": "function", "index": 0}),
            "bundle-123",
            Some("print('hello')"),
        );

        assert_eq!(value["type"], "code");
        assert_eq!(value["data"]["type"], "bundle");
        assert_eq!(value["data"]["bundle_id"], "bundle-123");
        assert_eq!(value["data"]["preview"], "print('hello')");
    }

    #[test]
    fn code_function_data_omits_empty_preview() {
        let runtime = RuntimeContext {
            runtime: "node".to_string(),
            version: "20.0.0".to_string(),
        };
        let value = build_code_function_data(
            &runtime,
            serde_json::json!({"type": "function", "index": 1}),
            "bundle-456",
            Some("   "),
        );

        assert_eq!(value["type"], "code");
        assert!(value["data"].get("preview").is_none());
    }

    #[test]
    fn python_package_staged_detects_module_files_and_directories() {
        let dir = tempfile::tempdir().expect("tempdir");
        let pkg_dir = dir.path();

        assert!(
            !python_package_staged(pkg_dir, "braintrust"),
            "package should be missing initially"
        );

        let package_dir = pkg_dir.join("braintrust");
        std::fs::create_dir_all(&package_dir).expect("create package dir");
        std::fs::write(package_dir.join("__init__.py"), "").expect("write __init__");
        assert!(
            python_package_staged(pkg_dir, "braintrust"),
            "package directory should be detected"
        );

        std::fs::remove_dir_all(&package_dir).expect("remove package dir");
        std::fs::write(pkg_dir.join("braintrust.py"), "VALUE = 1\n").expect("write module file");
        assert!(
            python_package_staged(pkg_dir, "braintrust"),
            "single-file module should be detected"
        );
    }

    #[test]
    fn js_bundler_defaults_do_not_externalize_braintrust_sdk() {
        assert!(
            !FUNCTIONS_JS_BUNDLER_SOURCE.contains("\"braintrust\"")
                && !FUNCTIONS_JS_BUNDLER_SOURCE.contains("\"autoevals\"")
                && !FUNCTIONS_JS_BUNDLER_SOURCE.contains("\"@braintrust/\""),
            "default JS bundler externals must not include Braintrust SDK packages"
        );
    }

    #[test]
    fn uv_install_args_include_selected_python() {
        let pkg_dir = PathBuf::from("/tmp/pkg");
        let python = PathBuf::from("/tmp/custom-python");
        let rendered = baseline_uv_install_args(&pkg_dir, &python, &[], "3.12")
            .into_iter()
            .map(|value| value.to_string_lossy().to_string())
            .collect::<Vec<_>>();
        let python_str = python.to_string_lossy().to_string();

        assert!(
            rendered
                .windows(2)
                .any(|window| window[0] == "--python" && window[1] == python_str.as_str()),
            "baseline uv args should pin the selected python interpreter"
        );
    }

    #[test]
    fn requirements_uv_install_args_include_selected_python() {
        let pkg_dir = PathBuf::from("/tmp/pkg");
        let requirements = PathBuf::from("/tmp/requirements.txt");
        let python = PathBuf::from("/tmp/custom-python");
        let rendered = requirements_uv_install_args(&pkg_dir, &requirements, &python, "3.12")
            .into_iter()
            .map(|value| value.to_string_lossy().to_string())
            .collect::<Vec<_>>();
        let python_str = python.to_string_lossy().to_string();

        assert!(
            rendered
                .windows(2)
                .any(|window| window[0] == "--python" && window[1] == python_str.as_str()),
            "requirements uv args should pin the selected python interpreter"
        );
    }

    #[cfg(unix)]
    #[test]
    fn collect_from_dir_skips_symlinked_files() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("root");
        std::fs::create_dir_all(&root).expect("create root");

        let inside = root.join("inside.ts");
        std::fs::write(&inside, "export const inside = 1;\n").expect("write inside");

        let outside = dir.path().join("outside.ts");
        std::fs::write(&outside, "export const outside = 2;\n").expect("write outside");
        symlink(&outside, root.join("outside-link.ts")).expect("create symlink");

        let mut js_like = BTreeSet::new();
        let mut python = BTreeSet::new();
        collect_from_dir(&root, &mut js_like, &mut python).expect("collect sources");

        let inside = inside.canonicalize().expect("canonicalize inside");
        let outside = outside.canonicalize().expect("canonicalize outside");
        assert!(js_like.contains(&inside));
        assert!(
            !js_like.contains(&outside),
            "directory scan should not follow symlinked files"
        );
    }

    #[cfg(unix)]
    #[test]
    fn collect_regular_files_recursive_skips_symlinked_files() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let root = dir.path().join("root");
        std::fs::create_dir_all(&root).expect("create root");

        let inside = root.join("inside.txt");
        std::fs::write(&inside, "inside\n").expect("write inside");

        let outside = dir.path().join("outside.txt");
        std::fs::write(&outside, "outside\n").expect("write outside");
        symlink(&outside, root.join("outside-link.txt")).expect("create symlink");

        let files = collect_regular_files_recursive(&root).expect("collect regular files");
        assert!(files.contains(&inside));
        assert!(
            files.iter().all(|path| path != &outside),
            "collector must not include symlink targets outside root"
        );
        assert!(
            files.iter().all(|path| path
                .file_name()
                .and_then(|value| value.to_str())
                .is_none_or(|value| value != "outside-link.txt")),
            "collector must skip symlink file entries"
        );
    }

    fn test_base_args() -> BaseArgs {
        BaseArgs {
            json: false,
            verbose: false,
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
            env_file: None,
        }
    }
}
