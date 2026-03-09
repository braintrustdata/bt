use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};
use std::fs::OpenOptions;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use dialoguer::console::style;
use indicatif::{ProgressBar, ProgressStyle};
use serde::Deserialize;
use serde_json::Value;

use crate::args::BaseArgs;
use crate::functions::report::{
    CommandStatus, FileStatus, HardFailureReason, PullFileReport, PullSummary, ReportError,
    ReportWarning, SoftSkipReason, WarningReason,
};
use crate::projects::api::{list_projects, Project};
use crate::utils::{write_text_atomic, GitRepo};

use super::api::{self, FunctionListQuery};
use super::{
    current_org_label, resolve_auth_context, resolve_project_context_optional, FunctionsLanguage,
    PullArgs,
};
use crate::ui::{animations_enabled, is_quiet};

const PAGINATION_PAGE_LIMIT: usize = 10_000;
const OUTPUT_LOCK_FILE: &str = ".bt-functions-pull.lock";
// Pretty version IDs are a reversible encoding of internal transaction IDs
// (_xact_id). The encoding multiplies the xact ID (with a fixed top-nibble tag)
// by COPRIME mod 2^64, producing a 16-hex-char string that looks random but
// decodes back via the modular inverse.  This lets `--version` accept either the
// raw numeric xact ID or the pretty hex form transparently.
const TOP_BITS: u64 = 0x0DE1u64 << 48;
const MODULUS: u128 = 1u128 << 64;
const COPRIME: u64 = 205_891_132_094_649;
const COPRIME_INVERSE: u64 = 1_522_336_535_492_693_385;

#[derive(Debug, Clone, Deserialize)]
struct PullFunctionRow {
    id: String,
    name: String,
    slug: String,
    project_id: String,
    #[serde(default)]
    project_name: Option<String>,
    #[serde(default)]
    description: Option<String>,
    #[serde(default)]
    prompt_data: Option<Value>,
    #[serde(default)]
    function_data: Option<Value>,
    #[serde(default)]
    created: Option<String>,
    #[serde(default)]
    _xact_id: Option<String>,
}

#[derive(Debug, Clone)]
struct NormalizedPrompt {
    variable_seed: String,
    name: String,
    slug: String,
    description: Option<String>,
    prompt: Option<Value>,
    messages: Option<Value>,
    model: Option<Value>,
    params: Option<Value>,
    tools: Option<Value>,
}

#[derive(Debug)]
struct OutputLock {
    path: PathBuf,
}

impl OutputLock {
    fn acquire(output_dir: &Path) -> Result<Self> {
        let path = output_dir.join(OUTPUT_LOCK_FILE);
        OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&path)
            .with_context(|| {
                format!(
                    "failed to acquire output lock {}; another pull may be running",
                    path.display()
                )
            })?;
        Ok(Self { path })
    }
}

impl Drop for OutputLock {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
    }
}

pub async fn run(base: BaseArgs, args: PullArgs) -> Result<()> {
    let mut summary = PullSummary {
        status: CommandStatus::Success,
        projects_total: 0,
        files_written: 0,
        files_skipped: 0,
        files_failed: 0,
        functions_seen: 0,
        functions_materialized: 0,
        malformed_records_skipped: 0,
        unsupported_records_skipped: 0,
        files: vec![],
        warnings: vec![],
        errors: vec![],
    };
    let mut projects_cache: Option<Vec<Project>> = None;
    let auth_ctx = match resolve_auth_context(&base)
        .await
        .context("failed to resolve auth context")
    {
        Ok(ctx) => ctx,
        Err(err) => {
            return fail_pull(
                &base,
                &mut summary,
                HardFailureReason::AuthFailed,
                err.to_string(),
            );
        }
    };

    let mut query = FunctionListQuery::default();
    let mut resolved_project_name: Option<String> = None;

    if let Some(project_id) = &args.project_id {
        query.project_id = Some(project_id.clone());
    } else if let Some(project_name) = &args.project_name {
        let projects = match get_projects_cached(&auth_ctx.client, &mut projects_cache).await {
            Ok(projects) => projects,
            Err(err) => {
                return fail_pull(
                    &base,
                    &mut summary,
                    HardFailureReason::ResponseInvalid,
                    err.to_string(),
                );
            }
        };
        if let Err(err) = ensure_unambiguous_project_name(projects, project_name) {
            return fail_pull(
                &base,
                &mut summary,
                HardFailureReason::ResponseInvalid,
                err.to_string(),
            );
        }
        resolved_project_name = Some(project_name.clone());
        query.project_name = Some(project_name.clone());
    } else {
        let project = match resolve_project_context_optional(&base, &auth_ctx, false).await {
            Ok(project) => project,
            Err(err) => {
                return fail_pull(
                    &base,
                    &mut summary,
                    HardFailureReason::ResponseInvalid,
                    err.to_string(),
                );
            }
        };
        if let Some(project) = project {
            resolved_project_name = Some(project.name.clone());
            query.project_id = Some(project.id);
        }
    }

    if let Some(id) = &args.id {
        query.id = Some(id.clone());
    }
    if let Some(version) = &args.version {
        query.version = match load_pretty_xact_compat(version) {
            Ok(value) => Some(value),
            Err(err) => {
                return fail_pull(
                    &base,
                    &mut summary,
                    HardFailureReason::ResponseInvalid,
                    err.to_string(),
                );
            }
        };
    }
    let resolved_slugs = args.resolved_slugs();
    if resolved_slugs.len() == 1 {
        query.slug = Some(resolved_slugs[0].clone());
    }

    let org_label = current_org_label(&auth_ctx);
    let subject = if !resolved_slugs.is_empty() {
        resolved_slugs.join(", ")
    } else {
        "functions".to_string()
    };
    let from_label = match &resolved_project_name {
        Some(project) => format!("{org_label}/{project}"),
        None => org_label.clone(),
    };
    let use_progress =
        !base.json && std::io::stderr().is_terminal() && animations_enabled() && !is_quiet();
    let spinner = if use_progress {
        let spinner_style = ProgressStyle::default_spinner()
            .tick_strings(&["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏", " "])
            .template("{spinner:.cyan} {msg}")
            .unwrap();
        let pb = ProgressBar::new_spinner();
        pb.set_style(spinner_style);
        pb.set_message(format!("Pulling {subject} from {from_label}..."));
        pb.enable_steady_tick(Duration::from_millis(80));
        pb
    } else {
        ProgressBar::hidden()
    };

    let fetched = match fetch_all_function_rows(&auth_ctx.client, &query).await {
        Ok(fetched) => fetched,
        Err(err) => {
            spinner.finish_and_clear();
            return fail_pull(
                &base,
                &mut summary,
                HardFailureReason::PaginationUnsupported,
                err.to_string(),
            );
        }
    };
    summary.functions_seen = fetched.rows.len();
    summary.warnings.extend(fetched.warnings);

    let mut parsed_rows = Vec::new();
    for raw_row in fetched.rows {
        match serde_json::from_value::<PullFunctionRow>(raw_row) {
            Ok(row) => parsed_rows.push(row),
            Err(err) => {
                summary.malformed_records_skipped += 1;
                summary.files.push(PullFileReport {
                    output_file: String::new(),
                    status: FileStatus::Skipped,
                    skipped_reason: Some(SoftSkipReason::MalformedRecord),
                    error_reason: None,
                    message: Some(format!("skipped malformed function row: {err}")),
                });
            }
        }
    }

    let narrowed_rows = match apply_selector_narrowing(parsed_rows, &args) {
        Ok(rows) => rows,
        Err(err) => {
            spinner.finish_and_clear();
            return fail_pull(
                &base,
                &mut summary,
                HardFailureReason::SelectorNotFound,
                err.to_string(),
            );
        }
    };

    let winners = select_winner_rows(narrowed_rows, &mut summary);

    if (args.id.is_some() || args.has_slug_selector()) && winners.is_empty() {
        spinner.finish_and_clear();
        return fail_pull(
            &base,
            &mut summary,
            HardFailureReason::SelectorNotFound,
            "no matching function rows found for selector".to_string(),
        );
    }

    let project_ids_with_matches = winners
        .iter()
        .map(|row| row.project_id.clone())
        .collect::<BTreeSet<_>>();

    let mut materializable = Vec::new();
    for row in winners.iter().cloned() {
        if is_prompt_row(&row) {
            materializable.push(row);
        } else {
            summary.unsupported_records_skipped += 1;
        }
    }

    let output_dir = if args.output_dir.is_absolute() {
        args.output_dir.clone()
    } else {
        std::env::current_dir()
            .context("failed to resolve current directory")?
            .join(&args.output_dir)
    };

    if let Err(err) = std::fs::create_dir_all(&output_dir)
        .with_context(|| format!("failed to create output directory {}", output_dir.display()))
    {
        spinner.finish_and_clear();
        return fail_pull(
            &base,
            &mut summary,
            HardFailureReason::OutputDirInvalid,
            err.to_string(),
        );
    }

    let canonical_output_dir = match output_dir
        .canonicalize()
        .with_context(|| format!("failed to canonicalize output dir {}", output_dir.display()))
    {
        Ok(path) => path,
        Err(err) => {
            spinner.finish_and_clear();
            return fail_pull(
                &base,
                &mut summary,
                HardFailureReason::OutputDirInvalid,
                err.to_string(),
            );
        }
    };

    let _lock = match OutputLock::acquire(&canonical_output_dir) {
        Ok(lock) => lock,
        Err(err) => {
            spinner.finish_and_clear();
            return fail_pull(
                &base,
                &mut summary,
                HardFailureReason::OutputDirInvalid,
                err.to_string(),
            );
        }
    };
    let repo = GitRepo::discover_from(&canonical_output_dir);

    let project_names = if project_ids_with_matches.is_empty() {
        BTreeMap::new()
    } else {
        let projects = match get_projects_cached(&auth_ctx.client, &mut projects_cache).await {
            Ok(projects) => projects,
            Err(err) => {
                spinner.finish_and_clear();
                return fail_pull(
                    &base,
                    &mut summary,
                    HardFailureReason::ResponseInvalid,
                    err.to_string(),
                );
            }
        };
        match resolve_project_names(&winners, projects) {
            Ok(names) => names,
            Err(err) => {
                spinner.finish_and_clear();
                return fail_pull(
                    &base,
                    &mut summary,
                    HardFailureReason::ResponseInvalid,
                    err.to_string(),
                );
            }
        }
    };

    let ext = match args.language {
        FunctionsLanguage::Typescript => "ts",
        FunctionsLanguage::Python => "py",
    };

    if !resolved_slugs.is_empty() {
        let found_slugs: BTreeSet<&str> = materializable.iter().map(|r| r.slug.as_str()).collect();
        for slug in &resolved_slugs {
            if !found_slugs.contains(slug.as_str()) {
                summary.warnings.push(ReportWarning {
                    reason: WarningReason::SelectorPartialMatch,
                    message: format!("slug '{}' not found", slug),
                });
            }
        }
    }

    // Legacy-compatible project mode: one output file per project, even for
    // selector pulls that only matched unsupported record types.
    let mut grouped_by_project = BTreeMap::<(String, String), Vec<PullFunctionRow>>::new();
    for project_id in project_ids_with_matches {
        let Some(project_name) = project_names.get(&project_id).cloned() else {
            return fail_pull(
                &base,
                &mut summary,
                HardFailureReason::ResponseInvalid,
                format!(
                    "missing resolved project name for project id '{}'",
                    project_id
                ),
            );
        };
        grouped_by_project
            .entry((project_id, project_name))
            .or_default();
    }
    for row in materializable {
        let Some(project_name) = project_names.get(&row.project_id).cloned() else {
            return fail_pull(
                &base,
                &mut summary,
                HardFailureReason::ResponseInvalid,
                format!(
                    "missing resolved project name for project id '{}'",
                    row.project_id
                ),
            );
        };
        grouped_by_project
            .entry((row.project_id.clone(), project_name))
            .or_default()
            .push(row);
    }

    summary.projects_total = grouped_by_project.len();

    let file_names = build_project_file_names(&grouped_by_project, ext);

    for ((project_id, project_name), rows) in grouped_by_project {
        let file_name = file_names
            .get(&(project_id.clone(), project_name.clone()))
            .ok_or_else(|| anyhow!("missing output file mapping"))?
            .clone();
        write_pull_file(
            &mut summary,
            &canonical_output_dir,
            &repo,
            args.force,
            args.language,
            &project_name,
            &file_name,
            &rows,
        );
    }

    if summary.status != CommandStatus::Failed
        && (summary.files_skipped > 0
            || summary.unsupported_records_skipped > 0
            || summary.malformed_records_skipped > 0
            || !summary.warnings.is_empty())
    {
        summary.status = CommandStatus::Partial;
    }

    let failure = summary.status == CommandStatus::Failed;
    spinner.finish_and_clear();
    if use_progress {
        if failure {
            eprintln!("{} Failed to pull {subject}", style("✗").red());
        } else {
            let cwd = std::env::current_dir().ok();
            let pulled_files: Vec<_> = summary
                .files
                .iter()
                .filter(|f| f.status == FileStatus::Success)
                .map(|f| short_display_path(&f.output_file, cwd.as_deref()))
                .collect();
            let file_label = if pulled_files.is_empty() {
                from_label.clone()
            } else {
                pulled_files.join(", ")
            };
            eprintln!("{} Pulled {subject} to {file_label}", style("✓").green(),);
        }
    }
    emit_summary(&base, &summary, args.verbose)?;
    if failure {
        bail!("functions pull failed; see summary for details");
    }

    Ok(())
}

async fn get_projects_cached<'a>(
    client: &crate::http::ApiClient,
    cache: &'a mut Option<Vec<Project>>,
) -> Result<&'a [Project]> {
    if cache.is_none() {
        *cache = Some(list_projects(client).await?);
    }
    Ok(cache
        .as_deref()
        .expect("project cache should be initialized"))
}

fn ensure_unambiguous_project_name(projects: &[Project], project_name: &str) -> Result<()> {
    let exact: Vec<_> = projects
        .iter()
        .filter(|project| project.name == project_name)
        .collect();

    match exact.len() {
        0 => bail!("project '{project_name}' not found"),
        1 => Ok(()),
        count => {
            bail!("project-name '{project_name}' is ambiguous ({count} matches); use --project-id")
        }
    }
}

fn modular_multiply(value: u64, prime: u64) -> u64 {
    ((value as u128 * prime as u128) % MODULUS) as u64
}

fn load_pretty_xact_compat(encoded_hex: &str) -> Result<String> {
    if encoded_hex.len() != 16 {
        return Ok(encoded_hex.to_string());
    }
    let value = u64::from_str_radix(encoded_hex, 16).with_context(|| {
        format!("invalid pretty version '{encoded_hex}' (expected 16 hex characters)")
    })?;
    let multiplied_inverse = modular_multiply(value, COPRIME_INVERSE);
    let with_top_bits = TOP_BITS | multiplied_inverse;
    let roundtrip = modular_multiply(with_top_bits, COPRIME);
    if roundtrip != value {
        bail!("invalid pretty version '{encoded_hex}' (failed compatibility decode)");
    }
    Ok(with_top_bits.to_string())
}

struct FetchRowsResult {
    rows: Vec<Value>,
    warnings: Vec<ReportWarning>,
}

async fn fetch_all_function_rows(
    client: &crate::http::ApiClient,
    query: &FunctionListQuery,
) -> Result<FetchRowsResult> {
    let mut page_count = 0usize;
    let mut rows = Vec::new();
    let mut cursor: Option<String> = None;
    let mut snapshot: Option<String> = None;
    let mut seen_cursors = BTreeSet::new();
    seen_cursors.insert("__start__".to_string());
    let mut warnings = Vec::new();
    let mut snapshot_consistent = true;

    loop {
        if page_count >= PAGINATION_PAGE_LIMIT {
            bail!("pagination page limit exceeded");
        }

        let mut page_query = query.clone();
        page_query.cursor = cursor.clone();
        page_query.snapshot = snapshot.clone();

        let page = api::list_functions_page(client, &page_query).await?;

        if page_count == 0 && !page.pagination_field_present {
            page_count += 1;
            rows.extend(page.objects);
            break;
        }

        page_count += 1;

        if page_count == 1 {
            snapshot = page.snapshot.clone();
        } else if snapshot.is_none() || !page.snapshot_field_present {
            snapshot_consistent = false;
        }

        if page.objects.is_empty() && page.next_cursor.is_some() {
            bail!("pagination returned empty page with non-empty next cursor");
        }

        rows.extend(page.objects);

        let Some(next_cursor) = page.next_cursor else {
            break;
        };

        if cursor.as_deref() == Some(next_cursor.as_str()) || seen_cursors.contains(&next_cursor) {
            bail!("pagination cursor did not advance");
        }
        seen_cursors.insert(next_cursor.clone());
        cursor = Some(next_cursor);
    }

    if page_count > 1 && !snapshot_consistent {
        warnings.push(ReportWarning {
            reason: WarningReason::PaginationNotSnapshotConsistent,
            message: "pagination endpoint does not appear to support snapshot-consistent traversal"
                .to_string(),
        });
    }

    Ok(FetchRowsResult { rows, warnings })
}

fn apply_selector_narrowing(
    rows: Vec<PullFunctionRow>,
    args: &PullArgs,
) -> Result<Vec<PullFunctionRow>> {
    let resolved_slugs = args.resolved_slugs();
    let slug_set: BTreeSet<&str> = resolved_slugs.iter().map(String::as_str).collect();
    let has_id_selector = args.id.is_some();
    let has_slug_selector = !slug_set.is_empty();

    let narrowed = rows
        .into_iter()
        .filter(|row| args.id.as_ref().is_none_or(|id| row.id == *id))
        .filter(|row| !has_slug_selector || slug_set.contains(row.slug.as_str()))
        .collect::<Vec<_>>();

    if (has_id_selector || has_slug_selector) && narrowed.is_empty() {
        bail!("selector did not match any function rows");
    }

    Ok(narrowed)
}

fn select_winner_rows(
    rows: Vec<PullFunctionRow>,
    summary: &mut PullSummary,
) -> Vec<PullFunctionRow> {
    let mut winners: BTreeMap<(String, String), PullFunctionRow> = BTreeMap::new();

    for row in rows {
        let key = (row.project_id.clone(), row.slug.clone());
        if let Some(existing) = winners.get_mut(&key) {
            summary.files_skipped += 1;
            if compare_rows_desc(&row, existing) == Ordering::Less {
                *existing = row;
            }
        } else {
            winners.insert(key, row);
        }
    }

    winners.into_values().collect()
}

fn is_prompt_row(row: &PullFunctionRow) -> bool {
    row.function_data
        .as_ref()
        .and_then(|data| data.get("type"))
        .and_then(Value::as_str)
        == Some("prompt")
}

fn compare_rows_desc(left: &PullFunctionRow, right: &PullFunctionRow) -> Ordering {
    let left_xact = left
        ._xact_id
        .as_deref()
        .and_then(|value| value.parse::<u128>().ok())
        .unwrap_or(0);
    let right_xact = right
        ._xact_id
        .as_deref()
        .and_then(|value| value.parse::<u128>().ok())
        .unwrap_or(0);

    match right_xact.cmp(&left_xact) {
        Ordering::Equal => {}
        non_eq => return non_eq,
    }

    match right.created.cmp(&left.created) {
        Ordering::Equal => {}
        non_eq => return non_eq,
    }

    right.id.cmp(&left.id)
}

fn resolve_project_names(
    rows: &[PullFunctionRow],
    projects: &[Project],
) -> Result<BTreeMap<String, String>> {
    let mut names_by_id = BTreeMap::new();
    if rows.is_empty() {
        return Ok(names_by_id);
    }

    for project in projects {
        names_by_id.insert(project.id.clone(), project.name.clone());
    }

    for row in rows {
        if let Some(project_name) = row
            .project_name
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
        {
            names_by_id
                .entry(row.project_id.clone())
                .or_insert_with(|| project_name.to_string());
        }
    }

    for row in rows {
        if !names_by_id.contains_key(&row.project_id) {
            bail!(
                "failed to resolve project name for project id '{}'",
                row.project_id
            );
        }
    }

    Ok(names_by_id)
}

#[allow(clippy::too_many_arguments)]
fn write_pull_file(
    summary: &mut PullSummary,
    canonical_output_dir: &Path,
    repo: &Option<GitRepo>,
    force: bool,
    language: FunctionsLanguage,
    project_name: &str,
    file_name: &str,
    rows: &[PullFunctionRow],
) {
    let target = canonical_output_dir.join(file_name);
    let display_target = display_output_path(&target);
    if !target.starts_with(canonical_output_dir) {
        record_pull_file_failure(
            summary,
            target.display().to_string(),
            HardFailureReason::UnsafeOutputPath,
            format!("refusing to write outside output dir: {}", target.display()),
        );
        return;
    }

    let skip_reason = match should_skip_target(repo, &target, force) {
        Ok(reason) => reason,
        Err(err) => {
            record_pull_file_failure(
                summary,
                target.display().to_string(),
                HardFailureReason::RequestFailed,
                err.to_string(),
            );
            return;
        }
    };
    if let Some(reason) = skip_reason {
        summary.files_skipped += 1;
        summary.files.push(PullFileReport {
            output_file: target.display().to_string(),
            status: FileStatus::Skipped,
            skipped_reason: Some(reason),
            error_reason: None,
            message: None,
        });
        return;
    }

    let rendered = match render_project_file(language, project_name, &display_target, rows) {
        Ok(rendered) => rendered,
        Err(err) => {
            record_pull_file_failure(
                summary,
                target.display().to_string(),
                HardFailureReason::ResponseInvalid,
                err.to_string(),
            );
            return;
        }
    };
    match write_text_atomic(&target, &rendered) {
        Ok(()) => {
            summary.files_written += 1;
            summary.functions_materialized += rows.len();
            summary.files.push(PullFileReport {
                output_file: target.display().to_string(),
                status: FileStatus::Success,
                skipped_reason: None,
                error_reason: None,
                message: None,
            });
        }
        Err(err) => {
            record_pull_file_failure(
                summary,
                target.display().to_string(),
                HardFailureReason::AtomicWriteFailed,
                err.to_string(),
            );
        }
    }
}

fn build_project_file_names(
    grouped_by_project: &BTreeMap<(String, String), Vec<PullFunctionRow>>,
    ext: &str,
) -> BTreeMap<(String, String), String> {
    let mut used_casefold = BTreeSet::new();
    let mut names = BTreeMap::new();

    for (project_id, project_name) in grouped_by_project.keys() {
        let base = sanitize_filename(project_name);
        let mut candidate = if base.is_empty() {
            "project".to_string()
        } else {
            base
        };
        if is_reserved_filename(&candidate) {
            candidate.push_str("-file");
        }

        let casefold = candidate.to_ascii_lowercase();
        if used_casefold.contains(&casefold) {
            candidate = format!("{}-{}", candidate, sanitize_filename(project_id));
        }

        used_casefold.insert(candidate.to_ascii_lowercase());
        names.insert(
            (project_id.clone(), project_name.clone()),
            format!("{candidate}.{ext}"),
        );
    }

    names
}

fn sanitize_filename(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    let mut previous_dash = false;
    for ch in value.chars() {
        let normalized = if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' || ch == '.' {
            ch.to_ascii_lowercase()
        } else {
            '-'
        };
        if normalized == '-' {
            if !previous_dash {
                out.push('-');
                previous_dash = true;
            }
        } else {
            out.push(normalized);
            previous_dash = false;
        }
    }

    out.trim_matches('-').to_string()
}

fn is_reserved_filename(value: &str) -> bool {
    matches!(
        value.to_ascii_lowercase().as_str(),
        "con"
            | "prn"
            | "aux"
            | "nul"
            | "com1"
            | "com2"
            | "com3"
            | "com4"
            | "com5"
            | "com6"
            | "com7"
            | "com8"
            | "com9"
            | "lpt1"
            | "lpt2"
            | "lpt3"
            | "lpt4"
            | "lpt5"
            | "lpt6"
            | "lpt7"
            | "lpt8"
            | "lpt9"
    )
}

fn sanitize_renderer_identifier(
    seed: &str,
    language: FunctionsLanguage,
    used: &mut BTreeSet<String>,
) -> String {
    let mut candidate = match language {
        FunctionsLanguage::Typescript => sanitize_typescript_identifier(seed),
        FunctionsLanguage::Python => sanitize_python_identifier(seed),
    };
    if used.contains(&candidate) {
        let base = candidate.clone();
        let mut suffix = 1usize;
        while used.contains(&candidate) {
            candidate = format!("{base}_{suffix}");
            suffix += 1;
        }
    }
    used.insert(candidate.clone());
    candidate
}

fn sanitize_typescript_identifier(seed: &str) -> String {
    let mut parts = Vec::new();
    let mut current = String::new();
    for ch in seed.chars() {
        if ch.is_ascii_alphanumeric() || ch == '_' || ch == '$' {
            current.push(ch);
        } else if !current.is_empty() {
            parts.push(current.clone());
            current.clear();
        }
    }
    if !current.is_empty() {
        parts.push(current);
    }

    if parts.is_empty() {
        return "prompt".to_string();
    }

    let mut out = String::new();
    for (index, part) in parts.iter().enumerate() {
        if index == 0 {
            out.push_str(&part.to_ascii_lowercase());
        } else {
            let mut chars = part.chars();
            if let Some(first) = chars.next() {
                out.push(first.to_ascii_uppercase());
            }
            out.push_str(&chars.as_str().to_ascii_lowercase());
        }
    }

    if out.is_empty() {
        return "prompt".to_string();
    }
    if out
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_digit())
    {
        out.insert_str(0, "prompt");
    }
    if out == "project" || out == "braintrust" {
        out.push('_');
    }
    out
}

fn sanitize_python_identifier(seed: &str) -> String {
    let mut out = String::with_capacity(seed.len());
    let mut previous_was_underscore = false;
    for ch in seed.chars() {
        let normalized = if ch.is_ascii_alphanumeric() { ch } else { '_' };
        if normalized == '_' {
            if !previous_was_underscore {
                out.push('_');
            }
            previous_was_underscore = true;
        } else {
            out.push(normalized.to_ascii_lowercase());
            previous_was_underscore = false;
        }
    }

    let mut out = out.trim_matches('_').to_string();
    if out.is_empty() {
        out = "prompt".to_string();
    }
    if out
        .chars()
        .next()
        .is_some_and(|first| first.is_ascii_digit())
    {
        out.insert_str(0, "prompt_");
    }
    if is_python_keyword(&out) || out == "project" || out == "braintrust" {
        out.push('_');
    }
    out
}

fn is_python_keyword(value: &str) -> bool {
    matches!(
        value,
        "false"
            | "none"
            | "true"
            | "and"
            | "as"
            | "assert"
            | "async"
            | "await"
            | "break"
            | "class"
            | "continue"
            | "def"
            | "del"
            | "elif"
            | "else"
            | "except"
            | "finally"
            | "for"
            | "from"
            | "global"
            | "if"
            | "import"
            | "in"
            | "is"
            | "lambda"
            | "nonlocal"
            | "not"
            | "or"
            | "pass"
            | "raise"
            | "return"
            | "try"
            | "while"
            | "with"
            | "yield"
    )
}

fn should_skip_target(
    repo: &Option<GitRepo>,
    target: &Path,
    force: bool,
) -> Result<Option<SoftSkipReason>> {
    if force {
        return Ok(None);
    }

    if !target.exists() {
        return Ok(None);
    }

    let Some(repo) = repo else {
        return Ok(Some(SoftSkipReason::ExistingNonGitNoForce));
    };

    if !target.starts_with(repo.root()) {
        return Ok(Some(SoftSkipReason::ExistingNonGitNoForce));
    }

    if repo.is_dirty_or_untracked(target)? {
        return Ok(Some(SoftSkipReason::DirtyTarget));
    }

    Ok(None)
}

fn display_output_path(target: &Path) -> String {
    let cwd = match std::env::current_dir() {
        Ok(cwd) => cwd,
        Err(_) => return target.display().to_string(),
    };

    pathdiff::diff_paths(target, &cwd)
        .filter(|path| !path.as_os_str().is_empty())
        .map(|path| path.display().to_string())
        .unwrap_or_else(|| target.display().to_string())
}

fn render_project_file(
    language: FunctionsLanguage,
    project_name: &str,
    file_name: &str,
    rows: &[PullFunctionRow],
) -> Result<String> {
    let mut sorted_rows = rows.to_vec();
    sorted_rows.sort_by(compare_rows_for_render);

    let mut normalized = Vec::with_capacity(sorted_rows.len());
    for row in &sorted_rows {
        normalized.push(normalize_prompt_row(row)?);
    }

    match language {
        FunctionsLanguage::Typescript => {
            render_project_file_ts(project_name, file_name, &normalized)
        }
        FunctionsLanguage::Python => render_project_file_py(project_name, file_name, &normalized),
    }
}

fn compare_rows_for_render(left: &PullFunctionRow, right: &PullFunctionRow) -> Ordering {
    match left.slug.cmp(&right.slug) {
        Ordering::Equal => {}
        non_eq => return non_eq,
    }
    match left.name.cmp(&right.name) {
        Ordering::Equal => {}
        non_eq => return non_eq,
    }
    left.id.cmp(&right.id)
}

fn normalize_prompt_row(row: &PullFunctionRow) -> Result<NormalizedPrompt> {
    let description = row
        .description
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    let prompt_data = row
        .prompt_data
        .as_ref()
        .ok_or_else(|| anyhow!("prompt row '{}' missing prompt_data", row.slug))?;
    let prompt_block = prompt_data
        .get("prompt")
        .ok_or_else(|| anyhow!("prompt row '{}' missing prompt_data.prompt", row.slug))?;

    let mut prompt = None;
    let mut messages = None;
    if prompt_block
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|value| value == "completion")
    {
        if let Some(content) = prompt_block.get("content") {
            if !is_empty_render_value(content) {
                prompt = Some(content.clone());
            }
        }
    } else if prompt_block
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|value| value == "chat")
    {
        if let Some(raw_messages) = prompt_block.get("messages") {
            if !is_empty_render_value(raw_messages) {
                messages = Some(raw_messages.clone());
            }
        }
    }

    let model = prompt_data
        .get("options")
        .and_then(|options| options.get("model"))
        .filter(|value| !is_empty_render_value(value))
        .cloned();
    let params = prompt_data
        .get("options")
        .and_then(|options| options.get("params"))
        .filter(|value| !is_empty_render_value(value))
        .cloned();

    let mut tools: Vec<Value> = prompt_data
        .get("tool_functions")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    if let Some(raw_tools) = prompt_block.get("tools").and_then(Value::as_str) {
        if !raw_tools.trim().is_empty() {
            if let Ok(parsed) = serde_json::from_str::<Value>(raw_tools) {
                if let Some(items) = parsed.as_array() {
                    tools.extend(items.iter().cloned());
                }
            }
        }
    }
    let tools = if tools.is_empty() {
        None
    } else {
        Some(Value::Array(tools))
    };

    Ok(NormalizedPrompt {
        variable_seed: row.slug.clone(),
        name: row.name.clone(),
        slug: row.slug.clone(),
        description,
        prompt,
        messages,
        model,
        params,
        tools,
    })
}

fn render_project_file_ts(
    project_name: &str,
    file_name: &str,
    prompts: &[NormalizedPrompt],
) -> Result<String> {
    let mut out = String::new();
    out.push_str("// This file was automatically generated by bt functions pull. You can\n");
    out.push_str("// generate it again by running:\n");
    out.push_str(&format!(
        "//  $ bt functions pull --project-name {}\n",
        serde_json::to_string(project_name)?
    ));
    out.push_str(
        "// Feel free to edit this file manually, but once you do, you should make sure to\n",
    );
    out.push_str("// sync your changes with Braintrust by running:\n");
    out.push_str(&format!(
        "//  $ bt functions push --file {}\n\n",
        serde_json::to_string(file_name)?
    ));

    out.push_str("import braintrust from \"braintrust\";\n\n");
    out.push_str("const project = braintrust.projects.create({\n");
    out.push_str(&format!(
        "  name: {},\n",
        serde_json::to_string(project_name)?
    ));
    out.push_str("});\n\n");

    let mut seen_names = BTreeSet::new();

    for row in prompts {
        let var_name = sanitize_renderer_identifier(
            &row.variable_seed,
            FunctionsLanguage::Typescript,
            &mut seen_names,
        );

        let mut body_lines = Vec::new();
        body_lines.push(format!("  name: {},", serde_json::to_string(&row.name)?));
        body_lines.push(format!("  slug: {},", serde_json::to_string(&row.slug)?));

        if let Some(description) = &row.description {
            body_lines.push(format!(
                "  description: {},",
                serde_json::to_string(description)?
            ));
        }

        if let Some(prompt) = &row.prompt {
            body_lines.push(format!("  prompt: {},", format_ts_value(prompt, 2)));
        }
        if let Some(messages) = &row.messages {
            body_lines.push(format!("  messages: {},", format_ts_value(messages, 2)));
        }
        if let Some(model) = &row.model {
            body_lines.push(format!("  model: {},", format_ts_value(model, 2)));
        }
        if let Some(params) = &row.params {
            body_lines.push(format!("  params: {},", format_ts_value(params, 2)));
        }
        if let Some(tools) = &row.tools {
            body_lines.push(format!("  tools: {},", format_ts_value(tools, 2)));
        }

        out.push_str(&format!(
            "export const {var_name} = project.prompts.create({{\n"
        ));
        out.push_str(&body_lines.join("\n"));
        out.push_str("\n});\n\n");
    }

    Ok(out)
}

fn render_project_file_py(
    project_name: &str,
    file_name: &str,
    prompts: &[NormalizedPrompt],
) -> Result<String> {
    let mut out = String::new();
    out.push_str("# This file was automatically generated by bt functions pull. You can\n");
    out.push_str("# generate it again by running:\n");
    out.push_str(&format!(
        "#  $ bt functions pull --project-name {} --language python\n",
        serde_json::to_string(project_name)?
    ));
    out.push_str(
        "# Feel free to edit this file manually, but once you do, you should make sure to\n",
    );
    out.push_str("# sync your changes with Braintrust by running:\n");
    out.push_str(&format!(
        "#  $ bt functions push --file {}\n\n",
        serde_json::to_string(file_name)?
    ));
    out.push_str("import braintrust\n\n");
    out.push_str(&format!(
        "project = braintrust.projects.create(name={})\n\n",
        serde_json::to_string(project_name)?
    ));

    let mut seen_names = BTreeSet::new();
    for row in prompts {
        let var_name = sanitize_renderer_identifier(
            &row.variable_seed,
            FunctionsLanguage::Python,
            &mut seen_names,
        );
        out.push_str(&format!("{var_name} = project.prompts.create(\n"));
        out.push_str(&format!(
            "    name={},\n",
            serde_json::to_string(&row.name)?
        ));
        out.push_str(&format!(
            "    slug={},\n",
            serde_json::to_string(&row.slug)?
        ));
        if let Some(description) = &row.description {
            out.push_str(&format!(
                "    description={},\n",
                serde_json::to_string(description)?
            ));
        }
        if let Some(prompt) = &row.prompt {
            out.push_str(&format!("    prompt={},\n", format_py_value(prompt, 4)));
        }
        if let Some(messages) = &row.messages {
            out.push_str(&format!("    messages={},\n", format_py_value(messages, 4)));
        }
        if let Some(model) = &row.model {
            out.push_str(&format!("    model={},\n", format_py_value(model, 4)));
        }
        if let Some(params) = &row.params {
            out.push_str(&format!("    params={},\n", format_py_value(params, 4)));
        }
        if let Some(tools) = &row.tools {
            out.push_str(&format!("    tools={},\n", format_py_value(tools, 4)));
        }
        out.push_str(")\n\n");
    }

    Ok(out)
}

fn format_ts_value(value: &Value, indent: usize) -> String {
    let json = format_ts_value_inner(value, 0);
    let pad = " ".repeat(indent);
    let mut lines = json.lines();
    let Some(first) = lines.next() else {
        return "null".to_string();
    };

    let mut out = first.to_string();
    for line in lines {
        out.push('\n');
        out.push_str(&pad);
        out.push_str(line);
    }
    out
}

fn format_ts_value_inner(value: &Value, depth: usize) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(boolean) => boolean.to_string(),
        Value::Number(number) => number.to_string(),
        Value::String(string) => {
            serde_json::to_string(string).unwrap_or_else(|_| "\"\"".to_string())
        }
        Value::Array(items) => {
            if items.is_empty() {
                return "[]".to_string();
            }

            let indent = "  ".repeat(depth + 1);
            let closing_indent = "  ".repeat(depth);
            let mut out = String::from("[\n");
            for (index, item) in items.iter().enumerate() {
                out.push_str(&indent);
                out.push_str(&format_ts_value_inner(item, depth + 1));
                if index + 1 < items.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&closing_indent);
            out.push(']');
            out
        }
        Value::Object(object) => {
            if object.is_empty() {
                return "{}".to_string();
            }

            let indent = "  ".repeat(depth + 1);
            let closing_indent = "  ".repeat(depth);
            let mut out = String::from("{\n");
            for (index, (key, val)) in object.iter().enumerate() {
                out.push_str(&indent);
                out.push_str(&format_ts_object_key(key));
                out.push_str(": ");
                out.push_str(&format_ts_value_inner(val, depth + 1));
                if index + 1 < object.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&closing_indent);
            out.push('}');
            out
        }
    }
}

fn format_ts_object_key(key: &str) -> String {
    if should_unquote_object_key(key) {
        key.to_string()
    } else {
        serde_json::to_string(key).unwrap_or_else(|_| "\"\"".to_string())
    }
}

fn format_py_value(value: &Value, indent: usize) -> String {
    let rendered = format_py_value_inner(value, 0);
    let pad = " ".repeat(indent);
    let mut lines = rendered.lines();
    let Some(first) = lines.next() else {
        return "None".to_string();
    };

    let mut out = first.to_string();
    for line in lines {
        out.push('\n');
        out.push_str(&pad);
        out.push_str(line);
    }
    out
}

fn format_py_value_inner(value: &Value, depth: usize) -> String {
    match value {
        Value::Null => "None".to_string(),
        Value::Bool(boolean) => {
            if *boolean {
                "True".to_string()
            } else {
                "False".to_string()
            }
        }
        Value::Number(number) => number.to_string(),
        Value::String(string) => {
            serde_json::to_string(string).unwrap_or_else(|_| "\"\"".to_string())
        }
        Value::Array(items) => {
            if items.is_empty() {
                return "[]".to_string();
            }
            let indent = "    ".repeat(depth + 1);
            let closing_indent = "    ".repeat(depth);
            let mut out = String::from("[\n");
            for (index, item) in items.iter().enumerate() {
                out.push_str(&indent);
                out.push_str(&format_py_value_inner(item, depth + 1));
                if index + 1 < items.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&closing_indent);
            out.push(']');
            out
        }
        Value::Object(object) => {
            if object.is_empty() {
                return "{}".to_string();
            }
            let indent = "    ".repeat(depth + 1);
            let closing_indent = "    ".repeat(depth);
            let mut out = String::from("{\n");
            let mut entries = object.iter().collect::<Vec<_>>();
            entries.sort_by(|(left, _), (right, _)| left.cmp(right));
            for (index, (key, val)) in entries.into_iter().enumerate() {
                out.push_str(&indent);
                out.push_str(&serde_json::to_string(key).unwrap_or_else(|_| "\"\"".to_string()));
                out.push_str(": ");
                out.push_str(&format_py_value_inner(val, depth + 1));
                if index + 1 < object.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            out.push_str(&closing_indent);
            out.push('}');
            out
        }
    }
}

fn should_unquote_object_key(key: &str) -> bool {
    if key.is_empty() || key == "__proto__" {
        return false;
    }

    let mut chars = key.chars();
    let Some(first) = chars.next() else {
        return false;
    };
    if !(first == '$' || first == '_' || first.is_ascii_alphabetic()) {
        return false;
    }

    chars.all(|ch| ch == '$' || ch == '_' || ch.is_ascii_alphanumeric())
}

fn is_empty_render_value(value: &Value) -> bool {
    match value {
        Value::Null => true,
        Value::String(value) => value.trim().is_empty(),
        Value::Array(value) => value.is_empty(),
        Value::Object(value) => value.is_empty(),
        Value::Bool(_) | Value::Number(_) => false,
    }
}

fn skip_reason_label(reason: Option<SoftSkipReason>) -> &'static str {
    match reason {
        Some(SoftSkipReason::DirtyTarget) => "dirty target",
        Some(SoftSkipReason::ExistingNonGitNoForce) => "already exists",
        Some(SoftSkipReason::MalformedRecord) => "malformed record",
        Some(SoftSkipReason::UnsupportedFunctionType) => "unsupported type",
        Some(SoftSkipReason::SupersededVersion) => "superseded",
        Some(SoftSkipReason::TerminatedAfterFailure) => "terminated after failure",
        Some(SoftSkipReason::IfExistsIgnored) => "ignored",
        Some(SoftSkipReason::NoDefinitionsFound) => "no definitions found",
        None => "skipped",
    }
}

fn short_display_path(path_str: &str, cwd: Option<&Path>) -> String {
    let p = Path::new(path_str);
    let file_name = p.file_name().unwrap_or(p.as_os_str());
    match (cwd, p.parent()) {
        (Some(cwd), Some(parent)) if parent == cwd => file_name.to_string_lossy().into_owned(),
        _ => {
            // parent_dir/file_name
            let parent_name = p.parent().and_then(|p| p.file_name()).unwrap_or_default();
            Path::new(parent_name).join(file_name).display().to_string()
        }
    }
}

fn emit_summary(base: &BaseArgs, summary: &PullSummary, verbose: bool) -> Result<()> {
    if base.json {
        println!("{}", serde_json::to_string(summary)?);
        return Ok(());
    }

    let cwd = std::env::current_dir().ok();
    for f in &summary.files {
        let name = short_display_path(&f.output_file, cwd.as_deref());
        match f.status {
            FileStatus::Success => {}
            FileStatus::Failed => {
                let msg = f.message.as_deref().unwrap_or("unknown error");
                eprintln!("Failed to pull {name} ({msg})");
            }
            FileStatus::Skipped if verbose => {
                let reason = skip_reason_label(f.skipped_reason);
                eprintln!("Skipped {name} ({reason})");
            }
            FileStatus::Skipped => {}
        }
    }

    for warning in &summary.warnings {
        eprintln!("warning: {}", warning.message);
    }
    for error in &summary.errors {
        eprintln!("error: {}", error.message);
    }

    Ok(())
}

fn fail_pull(
    base: &BaseArgs,
    summary: &mut PullSummary,
    reason: HardFailureReason,
    message: String,
) -> Result<()> {
    summary.status = CommandStatus::Failed;
    summary.errors.push(ReportError {
        reason,
        message: message.clone(),
    });
    if base.json {
        emit_summary(base, summary, false)?;
    }
    bail!(message);
}

fn record_pull_file_failure(
    summary: &mut PullSummary,
    output_file: String,
    reason: HardFailureReason,
    message: String,
) {
    summary.files_failed += 1;
    summary.status = CommandStatus::Failed;
    summary.errors.push(ReportError {
        reason,
        message: message.clone(),
    });
    summary.files.push(PullFileReport {
        output_file,
        status: FileStatus::Failed,
        skipped_reason: None,
        error_reason: Some(reason),
        message: Some(message),
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sanitize_identifier_helpers() {
        assert_eq!(sanitize_typescript_identifier("my-prompt"), "myPrompt");
        assert_eq!(sanitize_typescript_identifier("1prompt"), "prompt1prompt");
        assert_eq!(sanitize_typescript_identifier("doc-search"), "docSearch");
        assert_eq!(sanitize_typescript_identifier("tt-6bb2"), "tt6bb2");
        assert_eq!(sanitize_python_identifier("1prompt"), "prompt_1prompt");
        assert_eq!(sanitize_python_identifier("class"), "class_");
    }

    #[test]
    fn file_name_builder_handles_case_collisions() {
        let mut grouped = BTreeMap::new();
        grouped.insert(
            ("p1".to_string(), "Project".to_string()),
            Vec::<PullFunctionRow>::new(),
        );
        grouped.insert(
            ("p2".to_string(), "project".to_string()),
            Vec::<PullFunctionRow>::new(),
        );

        let names = build_project_file_names(&grouped, "ts");
        let first = names
            .get(&("p1".to_string(), "Project".to_string()))
            .expect("first");
        let second = names
            .get(&("p2".to_string(), "project".to_string()))
            .expect("second");

        assert_ne!(first.to_ascii_lowercase(), second.to_ascii_lowercase());
    }

    #[test]
    fn selector_narrowing_enforces_presence() {
        let row = PullFunctionRow {
            id: "f1".to_string(),
            name: "Prompt".to_string(),
            slug: "prompt".to_string(),
            project_id: "p1".to_string(),
            project_name: Some("Proj".to_string()),
            description: None,
            prompt_data: None,
            function_data: None,
            created: None,
            _xact_id: None,
        };
        let args = PullArgs {
            slugs: vec![],
            slug_flag: vec![],
            output_dir: PathBuf::from("."),
            language: FunctionsLanguage::Typescript,
            project_name: None,
            project_id: None,
            id: Some("missing".to_string()),
            version: None,
            force: false,
            verbose: false,
        };

        let err = apply_selector_narrowing(vec![row], &args).expect_err("should fail");
        assert!(err.to_string().contains("selector"));
    }

    #[test]
    fn multi_slug_narrowing_filters_to_matching() {
        let rows = vec![
            PullFunctionRow {
                id: "f1".to_string(),
                name: "A".to_string(),
                slug: "alpha".to_string(),
                project_id: "p1".to_string(),
                project_name: Some("Proj".to_string()),
                description: None,
                prompt_data: None,
                function_data: None,
                created: None,
                _xact_id: None,
            },
            PullFunctionRow {
                id: "f2".to_string(),
                name: "B".to_string(),
                slug: "beta".to_string(),
                project_id: "p1".to_string(),
                project_name: Some("Proj".to_string()),
                description: None,
                prompt_data: None,
                function_data: None,
                created: None,
                _xact_id: None,
            },
            PullFunctionRow {
                id: "f3".to_string(),
                name: "G".to_string(),
                slug: "gamma".to_string(),
                project_id: "p1".to_string(),
                project_name: Some("Proj".to_string()),
                description: None,
                prompt_data: None,
                function_data: None,
                created: None,
                _xact_id: None,
            },
        ];
        let args = PullArgs {
            slugs: vec!["alpha".to_string()],
            slug_flag: vec!["gamma".to_string()],
            output_dir: PathBuf::from("."),
            language: FunctionsLanguage::Typescript,
            project_name: None,
            project_id: None,
            id: None,
            version: None,
            force: false,
            verbose: false,
        };

        let narrowed = apply_selector_narrowing(rows, &args).expect("should narrow");
        assert_eq!(narrowed.len(), 2);
        let slugs: Vec<&str> = narrowed.iter().map(|r| r.slug.as_str()).collect();
        assert!(slugs.contains(&"alpha"));
        assert!(slugs.contains(&"gamma"));
    }

    #[test]
    fn resolve_project_names_uses_project_lookup() {
        let row = PullFunctionRow {
            id: "f1".to_string(),
            name: "Prompt".to_string(),
            slug: "prompt".to_string(),
            project_id: "p1".to_string(),
            project_name: None,
            description: None,
            prompt_data: None,
            function_data: None,
            created: None,
            _xact_id: None,
        };

        let projects = vec![crate::projects::api::Project {
            id: "p1".to_string(),
            name: "Woohoo".to_string(),
            org_id: "o1".to_string(),
            description: None,
        }];

        let names = resolve_project_names(&[row], &projects).expect("resolved names");
        assert_eq!(names.get("p1").map(String::as_str), Some("Woohoo"));
    }

    #[test]
    fn resolve_project_names_fails_when_missing() {
        let row = PullFunctionRow {
            id: "f1".to_string(),
            name: "Prompt".to_string(),
            slug: "prompt".to_string(),
            project_id: "p1".to_string(),
            project_name: None,
            description: None,
            prompt_data: None,
            function_data: None,
            created: None,
            _xact_id: None,
        };

        let err = resolve_project_names(&[row], &[]).expect_err("should fail");
        assert!(err.to_string().contains("failed to resolve project name"));
    }

    #[test]
    fn project_file_names_use_sanitized_project_name() {
        let mut grouped = BTreeMap::new();
        grouped.insert(
            ("p1".to_string(), "Doc Search".to_string()),
            Vec::<PullFunctionRow>::new(),
        );

        let names = build_project_file_names(&grouped, "ts");
        assert_eq!(
            names
                .get(&("p1".to_string(), "Doc Search".to_string()))
                .map(String::as_str),
            Some("doc-search.ts")
        );
    }

    #[test]
    fn project_file_names_include_project_id_suffix_on_collision() {
        let mut grouped = BTreeMap::new();
        grouped.insert(
            ("p1".to_string(), "Project One".to_string()),
            Vec::<PullFunctionRow>::new(),
        );
        grouped.insert(
            ("p2".to_string(), "project-one".to_string()),
            Vec::<PullFunctionRow>::new(),
        );

        let names = build_project_file_names(&grouped, "ts");
        let first = names
            .get(&("p1".to_string(), "Project One".to_string()))
            .expect("first");
        let second = names
            .get(&("p2".to_string(), "project-one".to_string()))
            .expect("second");
        assert_ne!(first.to_ascii_lowercase(), second.to_ascii_lowercase());
    }

    #[test]
    fn render_project_file_matches_legacy_shape() {
        let row = PullFunctionRow {
            id: "f1".to_string(),
            name: "Doc Search".to_string(),
            slug: "doc-search".to_string(),
            project_id: "p1".to_string(),
            project_name: Some("woohoo".to_string()),
            description: Some(String::new()),
            prompt_data: Some(serde_json::json!({
                "prompt": {
                    "type": "chat",
                    "messages": [
                        { "content": "Hello", "role": "system" }
                    ]
                },
                "options": {
                    "model": "gpt-4o-mini"
                },
                "tool_functions": [
                    { "type": "function", "id": "tool-1" }
                ]
            })),
            function_data: Some(serde_json::json!({ "type": "prompt" })),
            created: None,
            _xact_id: Some("123".to_string()),
        };

        let rendered = render_project_file(
            FunctionsLanguage::Typescript,
            "woohoo",
            "braintrust/woohoo.ts",
            &[row],
        )
        .expect("rendered");

        assert!(rendered.contains("automatically generated by bt functions pull"));
        assert!(rendered.contains("bt functions pull --project-name \"woohoo\""));
        assert!(rendered.contains("bt functions push --file \"braintrust/woohoo.ts\""));
        assert!(
            rendered.contains("const project = braintrust.projects.create({\n  name: \"woohoo\",")
        );
        assert!(rendered.contains("export const docSearch = project.prompts.create({"));
        assert!(!rendered.contains("description: \"\","));
        assert!(!rendered.contains("version:"));
        assert!(!rendered.contains("id: \"f1\""));
    }

    #[test]
    fn render_project_file_python_shape() {
        let row = PullFunctionRow {
            id: "f1".to_string(),
            name: "Doc Search".to_string(),
            slug: "doc-search".to_string(),
            project_id: "p1".to_string(),
            project_name: Some("woohoo".to_string()),
            description: Some("find docs".to_string()),
            prompt_data: Some(serde_json::json!({
                "prompt": {
                    "type": "chat",
                    "messages": [
                        { "content": "Hello", "role": "system" }
                    ]
                },
                "options": {
                    "model": "gpt-4o-mini",
                    "params": { "temperature": 0 }
                },
                "tool_functions": [
                    { "type": "function", "id": "tool-1" }
                ]
            })),
            function_data: Some(serde_json::json!({ "type": "prompt" })),
            created: None,
            _xact_id: Some("123".to_string()),
        };

        let rendered = render_project_file(
            FunctionsLanguage::Python,
            "woohoo",
            "braintrust/woohoo.py",
            &[row],
        )
        .expect("rendered");

        assert!(rendered.contains("bt functions pull --project-name \"woohoo\" --language python"));
        assert!(rendered.contains("bt functions push --file \"braintrust/woohoo.py\""));
        assert!(rendered.contains("import braintrust"));
        assert!(rendered.contains("project = braintrust.projects.create(name=\"woohoo\")"));
        assert!(rendered.contains("doc_search = project.prompts.create("));
        assert!(rendered.contains("messages=["));
        assert!(rendered.contains("model=\"gpt-4o-mini\""));
    }

    #[test]
    fn format_ts_value_unquotes_safe_keys_only() {
        let value = serde_json::json!({
            "content": "Hello",
            "role": "system",
            "$valid_1": true,
            "foo-bar": 1,
            "__proto__": { "x": 1 }
        });

        let rendered = format_ts_value(&value, 0);
        assert!(rendered.contains("content: \"Hello\""));
        assert!(rendered.contains("role: \"system\""));
        assert!(rendered.contains("$valid_1: true"));
        assert!(rendered.contains("\"foo-bar\": 1"));
        assert!(rendered.contains("\"__proto__\": {"));
        assert!(!rendered.contains("\"content\":"));
        assert!(!rendered.contains("\"role\":"));
    }

    #[test]
    fn format_py_value_maps_literals() {
        let value = serde_json::json!({
            "null": null,
            "bool_true": true,
            "bool_false": false,
            "items": [1, "x"]
        });

        let rendered = format_py_value(&value, 0);
        assert!(rendered.contains("\"null\": None"));
        assert!(rendered.contains("\"bool_true\": True"));
        assert!(rendered.contains("\"bool_false\": False"));
        assert!(rendered.contains("\"items\": ["));
    }

    #[test]
    fn is_empty_render_value_handles_supported_shapes() {
        assert!(is_empty_render_value(&Value::Null));
        assert!(is_empty_render_value(&Value::String("".to_string())));
        assert!(is_empty_render_value(&Value::String("   ".to_string())));
        assert!(is_empty_render_value(&Value::Array(Vec::new())));
        assert!(is_empty_render_value(
            &Value::Object(serde_json::Map::new())
        ));

        assert!(!is_empty_render_value(&Value::String("x".to_string())));
        assert!(!is_empty_render_value(&serde_json::json!(false)));
        assert!(!is_empty_render_value(&serde_json::json!(0)));
        assert!(!is_empty_render_value(&serde_json::json!([1])));
        assert!(!is_empty_render_value(&serde_json::json!({ "a": 1 })));
    }

    #[test]
    fn display_output_path_prefers_relative_path_when_available() {
        let cwd = std::env::current_dir().expect("cwd");
        let target = cwd.join("braintrust").join("woohoo.ts");
        let display = display_output_path(&target);
        assert_eq!(
            display,
            Path::new("braintrust")
                .join("woohoo.ts")
                .display()
                .to_string()
        );
    }
}
