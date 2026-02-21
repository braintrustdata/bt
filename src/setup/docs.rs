use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use regex::Regex;
use reqwest::Client;
use serde::Serialize;

use crate::args::BaseArgs;
use crate::sync::default_workers;

use super::{resolve_workflow_selection, write_text_file, WorkflowArg};

pub(super) const DEFAULT_DOCS_LLMS_URL: &str = "https://www.braintrust.dev/docs/llms.txt";
const DEFAULT_DOCS_LLMS_FULL_URL: &str = "https://www.braintrust.dev/docs/llms-full.txt";
const CORE_REFERENCE_WORKFLOW: &str = "reference";
const SQL_REFERENCE_DOC_TITLE: &str = "SQL queries";
const SQL_REFERENCE_DOC_URL: &str = "https://www.braintrust.dev/docs/reference/sql.md";

#[derive(Debug, Clone, Args)]
pub struct DocsArgs {
    #[command(subcommand)]
    command: Option<DocsSubcommand>,

    #[command(flatten)]
    fetch: DocsFetchArgs,
}

#[derive(Debug, Clone, Subcommand)]
enum DocsSubcommand {
    /// Download workflow docs markdown from Mintlify llms index
    Fetch(DocsFetchArgs),
}

#[derive(Debug, Clone, Args)]
pub(super) struct DocsFetchArgs {
    /// llms index URL (Mintlify markdown index)
    #[arg(long, default_value = DEFAULT_DOCS_LLMS_URL)]
    pub(super) llms_url: String,

    /// Output directory for downloaded docs
    #[arg(long, default_value = ".bt/skills/docs")]
    pub(super) output_dir: PathBuf,

    /// Workflow(s) to include (repeatable)
    #[arg(long = "workflow", value_enum)]
    pub(super) workflows: Vec<WorkflowArg>,

    /// Discover links only; do not write files
    #[arg(long)]
    pub(super) dry_run: bool,

    /// Fail command if any page download fails
    #[arg(long)]
    pub(super) strict: bool,

    /// Refresh docs by clearing output directory before download
    #[arg(long)]
    pub(super) refresh: bool,

    /// Number of concurrent workers for docs downloads.
    #[arg(long, default_value_t = default_workers())]
    pub(super) workers: usize,
}

#[derive(Debug, Clone, Serialize)]
struct DocsFileResult {
    title: String,
    url: String,
    workflow: String,
    status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DocsFetchJsonReport {
    llms_url: String,
    output_dir: String,
    dry_run: bool,
    discovered: usize,
    written: usize,
    failed: usize,
    workflows: Vec<String>,
    files: Vec<DocsFileResult>,
    warnings: Vec<String>,
}

#[derive(Debug, Clone)]
pub(super) struct DocsFetchResult {
    pub(super) discovered: usize,
    pub(super) written: usize,
    pub(super) failed: usize,
    files: Vec<DocsFileResult>,
    pub(super) warnings: Vec<String>,
}

pub async fn run_docs_top(base: BaseArgs, args: DocsArgs) -> Result<()> {
    run_docs(base, args).await
}

async fn run_docs(base: BaseArgs, args: DocsArgs) -> Result<()> {
    match args.command {
        Some(DocsSubcommand::Fetch(fetch)) => run_docs_fetch(base, fetch).await,
        None => run_docs_fetch(base, args.fetch).await,
    }
}

async fn run_docs_fetch(base: BaseArgs, args: DocsFetchArgs) -> Result<()> {
    let selected_workflows = resolve_workflow_selection(&args.workflows);
    let fetch_result = fetch_docs_pages(&args, &selected_workflows).await?;

    if base.json {
        let report = DocsFetchJsonReport {
            llms_url: args.llms_url,
            output_dir: args.output_dir.display().to_string(),
            dry_run: args.dry_run,
            discovered: fetch_result.discovered,
            written: fetch_result.written,
            failed: fetch_result.failed,
            workflows: selected_workflows
                .iter()
                .map(|workflow| workflow.as_str().to_string())
                .collect(),
            files: fetch_result.files.clone(),
            warnings: fetch_result.warnings.clone(),
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&report).context("failed to serialize docs report")?
        );
    } else {
        let workflow_set: BTreeSet<&str> = selected_workflows.iter().map(|w| w.as_str()).collect();
        println!("Fetched docs index: {}", args.llms_url);
        println!(
            "Workflows: {}",
            workflow_set.into_iter().collect::<Vec<_>>().join(", ")
        );
        println!(
            "Discovered {} page(s), wrote {} page(s){}",
            fetch_result.discovered,
            fetch_result.written,
            if args.dry_run { " (dry-run)" } else { "" }
        );
        for file in &fetch_result.files {
            match &file.path {
                Some(path) => println!("  - {} [{}] -> {}", file.title, file.workflow, path),
                None => println!("  - {} [{}] -> {}", file.title, file.workflow, file.url),
            }
        }
        if !fetch_result.warnings.is_empty() {
            println!("Warnings:");
            for warning in &fetch_result.warnings {
                println!("  - {warning}");
            }
        }
    }

    if args.strict && fetch_result.failed > 0 {
        bail!(
            "{} docs page(s) failed to download in strict mode",
            fetch_result.failed
        );
    }

    Ok(())
}

pub(super) async fn fetch_docs_pages(
    args: &DocsFetchArgs,
    selected_workflows: &[WorkflowArg],
) -> Result<DocsFetchResult> {
    struct DownloadJob {
        index: usize,
        title: String,
        workflow: String,
        url: String,
        target: PathBuf,
    }

    let workflow_set: BTreeSet<&str> = selected_workflows.iter().map(|w| w.as_str()).collect();
    let workflow_link_re =
        Regex::new(r"\[([^\]]+)\]\(([^)\s]+)\)").context("failed to build markdown link regex")?;
    let bare_url_re =
        Regex::new(r#"(?m)\b(https?://[^\s<>"')]+)"#).context("failed to build URL regex")?;
    let llms_base = reqwest::Url::parse(&args.llms_url)
        .with_context(|| format!("invalid llms URL: {}", args.llms_url))?;
    let client = Client::builder()
        .build()
        .context("failed to build HTTP client")?;

    let index_response = client
        .get(&args.llms_url)
        .send()
        .await
        .with_context(|| format!("failed to fetch llms index {}", args.llms_url))?;
    if !index_response.status().is_success() {
        let status = index_response.status();
        let body = index_response.text().await.unwrap_or_default();
        bail!("failed to fetch llms index ({status}): {body}");
    }
    let index_body = index_response
        .text()
        .await
        .context("failed to read llms index response body")?;

    let mut discovered = collect_docs_links(
        &index_body,
        &workflow_set,
        &workflow_link_re,
        &bare_url_re,
        &llms_base,
    );
    if discovered.is_empty() && args.llms_url == DEFAULT_DOCS_LLMS_URL {
        let fallback_response = client
            .get(DEFAULT_DOCS_LLMS_FULL_URL)
            .send()
            .await
            .with_context(|| {
                format!("failed to fetch fallback index {DEFAULT_DOCS_LLMS_FULL_URL}")
            })?;
        if fallback_response.status().is_success() {
            let fallback_body = fallback_response
                .text()
                .await
                .context("failed to read fallback llms-full response body")?;
            if let Ok(fallback_base) = reqwest::Url::parse(DEFAULT_DOCS_LLMS_FULL_URL) {
                discovered = collect_docs_links(
                    &fallback_body,
                    &workflow_set,
                    &workflow_link_re,
                    &bare_url_re,
                    &fallback_base,
                );
            }
        }
    }
    append_core_reference_links(&mut discovered);

    let mut written = 0usize;
    let mut failed = 0usize;
    let mut warnings = Vec::new();
    let mut seen_targets = BTreeSet::new();

    if !args.dry_run {
        if args.refresh && args.output_dir.exists() {
            fs::remove_dir_all(&args.output_dir).with_context(|| {
                format!(
                    "failed to clear output directory {}",
                    args.output_dir.display()
                )
            })?;
        }
        fs::create_dir_all(&args.output_dir).with_context(|| {
            format!(
                "failed to create output directory {}",
                args.output_dir.display()
            )
        })?;
    }

    let mut ordered_results: Vec<Option<DocsFileResult>> = Vec::new();
    let mut download_jobs = Vec::new();

    for (title, workflow, url) in discovered {
        if args.dry_run {
            ordered_results.push(Some(DocsFileResult {
                title,
                url,
                workflow,
                status: "discovered".to_string(),
                path: None,
                error: None,
            }));
            continue;
        }

        let workflow_dir = args.output_dir.join(&workflow);
        fs::create_dir_all(&workflow_dir).with_context(|| {
            format!(
                "failed to create workflow directory {}",
                workflow_dir.display()
            )
        })?;
        let rel_path = workflow_relative_path(&url, &workflow);
        let target = workflow_dir.join(&rel_path);
        let target_key = target.to_string_lossy().to_ascii_lowercase();
        if !seen_targets.insert(target_key) {
            let warning = format!(
                "skipping duplicate output path for {} [{}] -> {}",
                title,
                workflow,
                target.display()
            );
            warnings.push(warning);
            ordered_results.push(Some(DocsFileResult {
                title,
                url,
                workflow,
                status: "skipped".to_string(),
                path: None,
                error: Some("duplicate output path".to_string()),
            }));
            continue;
        }
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create docs subdirectory {}", parent.display())
            })?;
        }

        let index = ordered_results.len();
        ordered_results.push(None);
        download_jobs.push(DownloadJob {
            index,
            title,
            workflow,
            url,
            target,
        });
    }

    if !args.dry_run {
        let worker_count = args.workers.max(1);
        let mut next_job = 0usize;
        let mut in_flight = tokio::task::JoinSet::new();

        while next_job < download_jobs.len() || !in_flight.is_empty() {
            while next_job < download_jobs.len() && in_flight.len() < worker_count {
                let job = DownloadJob {
                    index: download_jobs[next_job].index,
                    title: download_jobs[next_job].title.clone(),
                    workflow: download_jobs[next_job].workflow.clone(),
                    url: download_jobs[next_job].url.clone(),
                    target: download_jobs[next_job].target.clone(),
                };
                let client = client.clone();
                in_flight.spawn(async move {
                    let result = async {
                        let response =
                            client.get(&job.url).send().await.with_context(|| {
                                format!("failed to fetch docs page {}", job.url)
                            })?;
                        if !response.status().is_success() {
                            let status = response.status();
                            let body = response.text().await.unwrap_or_default();
                            bail!("docs page returned error ({status}): {body}");
                        }
                        let content = response.text().await.with_context(|| {
                            format!("failed to read docs page body {}", job.url)
                        })?;
                        write_text_file(&job.target, &content)?;
                        Result::<()>::Ok(())
                    }
                    .await;
                    (job, result.err().map(|err| err.to_string()))
                });
                next_job += 1;
            }

            let Some(join_result) = in_flight.join_next().await else {
                break;
            };
            let (job, error) = join_result.context("docs fetch worker task failed")?;
            match error {
                None => {
                    written += 1;
                    ordered_results[job.index] = Some(DocsFileResult {
                        title: job.title,
                        url: job.url,
                        workflow: job.workflow,
                        status: "written".to_string(),
                        path: Some(job.target.display().to_string()),
                        error: None,
                    });
                }
                Some(err) => {
                    failed += 1;
                    warnings.push(format!("failed to fetch {}: {}", job.url, err));
                    ordered_results[job.index] = Some(DocsFileResult {
                        title: job.title,
                        url: job.url,
                        workflow: job.workflow,
                        status: "failed".to_string(),
                        path: None,
                        error: Some(err),
                    });
                }
            }
        }
    }

    for (idx, slot) in ordered_results.iter_mut().enumerate() {
        if slot.is_none() {
            failed += 1;
            let fallback = download_jobs
                .iter()
                .find(|job| job.index == idx)
                .map(|job| (job.title.clone(), job.workflow.clone(), job.url.clone()))
                .unwrap_or_else(|| {
                    (
                        "unknown".to_string(),
                        "unknown".to_string(),
                        "unknown".to_string(),
                    )
                });
            warnings.push(format!(
                "failed to fetch {}: worker exited unexpectedly",
                fallback.2
            ));
            *slot = Some(DocsFileResult {
                title: fallback.0,
                url: fallback.2,
                workflow: fallback.1,
                status: "failed".to_string(),
                path: None,
                error: Some("worker exited unexpectedly".to_string()),
            });
        }
    }

    let file_results = ordered_results.into_iter().flatten().collect::<Vec<_>>();

    if !args.dry_run {
        let sections = docs_index_sections(selected_workflows, &file_results);
        write_docs_indexes(&args.output_dir, &sections, &file_results)?;
    }

    Ok(DocsFetchResult {
        discovered: file_results.len(),
        written,
        failed,
        files: file_results,
        warnings,
    })
}

fn workflow_from_url(url: &str) -> Option<&'static str> {
    let canonical = url
        .split(['?', '#'])
        .next()
        .unwrap_or(url)
        .to_ascii_lowercase();
    for workflow in ["instrument", "observe", "annotate", "evaluate", "deploy"] {
        if canonical.contains(&format!("/docs/{workflow}/"))
            || canonical.ends_with(&format!("/docs/{workflow}.md"))
            || canonical.ends_with(&format!("/docs/{workflow}"))
        {
            return Some(workflow);
        }
    }
    None
}

fn slug_for_url(url: &str) -> String {
    let canonical = url
        .split(['?', '#'])
        .next()
        .unwrap_or(url)
        .trim_end_matches('/');
    let mut slug = canonical.rsplit('/').next().unwrap_or("index").to_string();
    if let Some(stripped) = slug.strip_suffix(".md") {
        slug = stripped.to_string();
    }
    if slug.ends_with(".html") {
        slug = slug.trim_end_matches(".html").to_string();
    }
    if slug.is_empty() {
        slug = "index".to_string();
    }
    slug.chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
                ch
            } else {
                '-'
            }
        })
        .collect()
}

fn collect_docs_links(
    body: &str,
    workflow_set: &BTreeSet<&str>,
    workflow_link_re: &Regex,
    bare_url_re: &Regex,
    base_url: &reqwest::Url,
) -> Vec<(String, String, String)> {
    let mut discovered: Vec<(String, String, String)> = Vec::new();
    let mut by_canonical: BTreeMap<String, usize> = BTreeMap::new();

    for capture in workflow_link_re.captures_iter(body) {
        let title = capture
            .get(1)
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_default();
        let raw_url = capture
            .get(2)
            .map(|m| m.as_str().trim().to_string())
            .unwrap_or_default();
        let Some(url) = absolutize_url(&raw_url, base_url) else {
            continue;
        };
        let Some(workflow) = workflow_from_url(&url) else {
            continue;
        };
        if !workflow_set.contains(workflow) {
            continue;
        }
        let canonical = canonical_docs_url(&url);
        if let Some(existing_idx) = by_canonical.get(&canonical).copied() {
            if should_replace_docs_url(&discovered[existing_idx].2, &url) {
                discovered[existing_idx] = (title, workflow.to_string(), url);
            }
            continue;
        }
        by_canonical.insert(canonical, discovered.len());
        discovered.push((title, workflow.to_string(), url));
    }

    for capture in bare_url_re.captures_iter(body) {
        let url = capture
            .get(1)
            .map(|m| {
                m.as_str()
                    .trim()
                    .trim_end_matches([',', '.', ';', ')'])
                    .to_string()
            })
            .unwrap_or_default();
        let Some(workflow) = workflow_from_url(&url) else {
            continue;
        };
        if !workflow_set.contains(workflow) {
            continue;
        }
        let canonical = canonical_docs_url(&url);
        if let Some(existing_idx) = by_canonical.get(&canonical).copied() {
            if should_replace_docs_url(&discovered[existing_idx].2, &url) {
                discovered[existing_idx] = (slug_for_url(&url), workflow.to_string(), url);
            }
            continue;
        }
        by_canonical.insert(canonical, discovered.len());
        discovered.push((slug_for_url(&url), workflow.to_string(), url));
    }

    discovered
}

fn append_core_reference_links(discovered: &mut Vec<(String, String, String)>) {
    let mut seen = discovered
        .iter()
        .map(|(_, _, url)| canonical_docs_url(url))
        .collect::<BTreeSet<_>>();
    for (title, workflow, url) in [(
        SQL_REFERENCE_DOC_TITLE,
        CORE_REFERENCE_WORKFLOW,
        SQL_REFERENCE_DOC_URL,
    )] {
        if seen.insert(canonical_docs_url(url)) {
            discovered.push((title.to_string(), workflow.to_string(), url.to_string()));
        }
    }
}

fn canonical_docs_url(url: &str) -> String {
    let mut canonical = url
        .split(['?', '#'])
        .next()
        .unwrap_or(url)
        .trim_end_matches('/')
        .to_ascii_lowercase();
    if let Some(stripped) = canonical.strip_prefix("https://www.braintrust.dev/") {
        canonical = format!("https://braintrust.dev/{stripped}");
    } else if let Some(stripped) = canonical.strip_prefix("http://www.braintrust.dev/") {
        canonical = format!("http://braintrust.dev/{stripped}");
    }
    if canonical.ends_with(".md") {
        canonical.truncate(canonical.len() - 3);
    } else if canonical.ends_with(".html") {
        canonical.truncate(canonical.len() - 5);
    }
    if canonical.ends_with("/index") {
        canonical.truncate(canonical.len() - "/index".len());
    }
    canonical
}

fn docs_url_preference_score(url: &str) -> i32 {
    let canonical = url
        .split(['?', '#'])
        .next()
        .unwrap_or(url)
        .to_ascii_lowercase();
    if canonical.ends_with("/index.md") {
        return 3;
    }
    if canonical.ends_with(".md") {
        return 2;
    }
    if canonical.ends_with(".html") {
        return 0;
    }
    1
}

fn should_replace_docs_url(existing_url: &str, candidate_url: &str) -> bool {
    docs_url_preference_score(candidate_url) > docs_url_preference_score(existing_url)
}

fn absolutize_url(raw: &str, base_url: &reqwest::Url) -> Option<String> {
    if raw.starts_with("http://") || raw.starts_with("https://") {
        return Some(raw.to_string());
    }
    base_url.join(raw).ok().map(|url| url.to_string())
}

fn workflow_relative_path(url: &str, workflow: &str) -> PathBuf {
    let canonical = url
        .split(['?', '#'])
        .next()
        .unwrap_or(url)
        .trim_end_matches('/');
    let marker = format!("/docs/{workflow}/");
    let tail = if let Some(idx) = canonical.find(&marker) {
        canonical[idx + marker.len()..].to_string()
    } else if canonical.ends_with(&format!("/docs/{workflow}")) {
        "index.md".to_string()
    } else if canonical.ends_with(&format!("/docs/{workflow}.md")) {
        "index.md".to_string()
    } else {
        format!("{}.md", slug_for_url(url))
    };

    let mut clean_segments = Vec::new();
    for segment in tail.split('/') {
        let segment = segment.trim();
        if segment.is_empty() {
            continue;
        }
        clean_segments.push(sanitize_path_segment(segment));
    }
    if clean_segments.is_empty() {
        clean_segments.push("index.md".to_string());
    }

    let mut rel = PathBuf::new();
    for segment in clean_segments {
        rel.push(segment);
    }
    if rel.extension().is_none() {
        rel.set_extension("md");
    }
    rel
}

fn sanitize_path_segment(segment: &str) -> String {
    let mut out = String::new();
    for ch in segment.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '-' | '_' | '.') {
            out.push(ch);
        } else {
            out.push('-');
        }
    }
    if out.is_empty() {
        "index".to_string()
    } else {
        out
    }
}

fn docs_index_sections(
    selected_workflows: &[WorkflowArg],
    files: &[DocsFileResult],
) -> Vec<String> {
    let mut sections = selected_workflows
        .iter()
        .map(|workflow| workflow.as_str().to_string())
        .collect::<Vec<_>>();
    let mut seen = sections.iter().cloned().collect::<BTreeSet<_>>();
    let mut extras = files
        .iter()
        .map(|file| file.workflow.clone())
        .filter(|workflow| seen.insert(workflow.clone()))
        .collect::<Vec<_>>();
    extras.sort();
    sections.extend(extras);
    sections
}

fn write_docs_indexes(
    output_dir: &Path,
    workflows: &[String],
    files: &[DocsFileResult],
) -> Result<()> {
    let mut top_lines = Vec::new();
    top_lines.push("# Braintrust Docs".to_string());
    top_lines.push(String::new());
    top_lines.push("Generated by `bt docs fetch`.".to_string());
    top_lines.push(String::new());

    for workflow in workflows {
        let workflow_files = files
            .iter()
            .filter(|file| file.workflow == workflow.as_str() && file.status == "written")
            .collect::<Vec<_>>();

        top_lines.push(format!("## {workflow}"));
        if workflow_files.is_empty() {
            top_lines.push("- no pages downloaded".to_string());
            top_lines.push(String::new());
            continue;
        }

        let mut workflow_index_lines = Vec::new();
        workflow_index_lines.push(format!("# {workflow} Docs"));
        workflow_index_lines.push(String::new());

        for file in workflow_files {
            let Some(path) = file.path.as_deref() else {
                continue;
            };
            let workflow_dir = output_dir.join(workflow);
            let relative = Path::new(path)
                .strip_prefix(&workflow_dir)
                .unwrap_or_else(|_| Path::new(path))
                .display()
                .to_string();
            workflow_index_lines.push(format!("- [{}]({})", file.title, relative));
            workflow_index_lines.push(format!("  source: `{}`", file.url));
            top_lines.push(format!("- [{}]({}/{})", file.title, workflow, relative));
        }

        let workflow_index = output_dir.join(workflow).join("_index.md");
        write_text_file(&workflow_index, &workflow_index_lines.join("\n"))?;
        top_lines.push(format!("- [_index]({workflow}/_index.md)"));
        top_lines.push(String::new());
    }

    let top_index = output_dir.join("README.md");
    write_text_file(&top_index, &top_lines.join("\n"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_from_url_detects_expected_sections() {
        assert_eq!(
            workflow_from_url("https://www.braintrust.dev/docs/evaluate/overview.md"),
            Some("evaluate")
        );
        assert_eq!(
            workflow_from_url("https://www.braintrust.dev/docs/observe.md"),
            Some("observe")
        );
        assert_eq!(
            workflow_from_url("https://www.braintrust.dev/docs/changelog.md"),
            None
        );
        assert_eq!(
            workflow_from_url("https://www.braintrust.dev/docs/instrument"),
            Some("instrument")
        );
    }

    #[test]
    fn slug_for_url_handles_suffixes_and_query_params() {
        assert_eq!(
            slug_for_url("https://www.braintrust.dev/docs/evaluate/overview.md?x=1#y"),
            "overview"
        );
        assert_eq!(
            slug_for_url("https://www.braintrust.dev/docs/evaluate/custom-scorers.md"),
            "custom-scorers"
        );
        assert_eq!(
            slug_for_url("https://www.braintrust.dev/docs/evaluate/overview.html"),
            "overview"
        );
    }

    #[test]
    fn workflow_relative_path_preserves_nested_structure() {
        let rel = workflow_relative_path(
            "https://www.braintrust.dev/docs/evaluate/models/custom-scorers.md",
            "evaluate",
        );
        assert_eq!(rel, PathBuf::from("models/custom-scorers.md"));
    }

    #[test]
    fn workflow_relative_path_handles_workflow_root_page() {
        let rel = workflow_relative_path("https://www.braintrust.dev/docs/observe", "observe");
        assert_eq!(rel, PathBuf::from("index.md"));
    }

    #[test]
    fn workflow_relative_path_handles_sql_reference_page() {
        let rel =
            workflow_relative_path("https://www.braintrust.dev/docs/reference/sql", "reference");
        assert_eq!(rel, PathBuf::from("sql.md"));
    }

    #[test]
    fn append_core_reference_links_dedupes_sql_docs_variants() {
        let mut discovered = vec![(
            "sql".to_string(),
            "reference".to_string(),
            "https://www.braintrust.dev/docs/reference/sql.md".to_string(),
        )];
        append_core_reference_links(&mut discovered);
        assert_eq!(discovered.len(), 1);
    }

    #[test]
    fn docs_index_sections_include_core_reference_section() {
        let files = vec![DocsFileResult {
            title: "SQL queries".to_string(),
            url: "https://www.braintrust.dev/docs/reference/sql".to_string(),
            workflow: "reference".to_string(),
            status: "written".to_string(),
            path: Some(".bt/skills/docs/reference/sql.md".to_string()),
            error: None,
        }];
        let sections = docs_index_sections(&[WorkflowArg::Evaluate], &files);
        assert_eq!(
            sections,
            vec!["evaluate".to_string(), "reference".to_string()]
        );
    }

    #[test]
    fn canonical_docs_url_normalizes_www_and_index_aliases() {
        assert_eq!(
            canonical_docs_url("https://www.braintrust.dev/docs/evaluate/index.md"),
            "https://braintrust.dev/docs/evaluate"
        );
        assert_eq!(
            canonical_docs_url("https://braintrust.dev/docs/evaluate"),
            "https://braintrust.dev/docs/evaluate"
        );
        assert_eq!(
            canonical_docs_url("https://braintrust.dev/docs/deploy/ai-proxy.md"),
            "https://braintrust.dev/docs/deploy/ai-proxy"
        );
        assert_eq!(
            canonical_docs_url("https://www.braintrust.dev/docs/deploy/ai-proxy"),
            "https://braintrust.dev/docs/deploy/ai-proxy"
        );
    }

    #[test]
    fn collect_docs_links_dedupes_canonical_url_variants() {
        let body = r#"
- [Evaluate](https://www.braintrust.dev/docs/evaluate)
- [Evaluate system](https://braintrust.dev/docs/evaluate/index.md)
- [AI proxy](https://www.braintrust.dev/docs/deploy/ai-proxy)
- [AI proxy alt](https://braintrust.dev/docs/deploy/ai-proxy.md)
"#;
        let workflow_set = ["evaluate", "deploy"].into_iter().collect::<BTreeSet<_>>();
        let link_re = Regex::new(r"\[([^\]]+)\]\(([^)\s]+)\)").expect("link regex");
        let bare_re = Regex::new(r#"(?m)\b(https?://[^\s<>"')]+)"#).expect("url regex");
        let base = reqwest::Url::parse("https://www.braintrust.dev/docs/llms.txt").expect("base");
        let links = collect_docs_links(body, &workflow_set, &link_re, &bare_re, &base);

        let evaluate_count = links.iter().filter(|(_, w, _)| w == "evaluate").count();
        let deploy_count = links.iter().filter(|(_, w, _)| w == "deploy").count();
        assert_eq!(evaluate_count, 1);
        assert_eq!(deploy_count, 1);
    }

    #[test]
    fn collect_docs_links_prefers_markdown_url_variants() {
        let body = r#"
- [Evaluate](https://www.braintrust.dev/docs/evaluate)
- [Evaluate system](https://braintrust.dev/docs/evaluate/index.md)
- [AI proxy](https://www.braintrust.dev/docs/deploy/ai-proxy)
- [AI proxy alt](https://braintrust.dev/docs/deploy/ai-proxy.md)
"#;
        let workflow_set = ["evaluate", "deploy"].into_iter().collect::<BTreeSet<_>>();
        let link_re = Regex::new(r"\[([^\]]+)\]\(([^)\s]+)\)").expect("link regex");
        let bare_re = Regex::new(r#"(?m)\b(https?://[^\s<>"')]+)"#).expect("url regex");
        let base = reqwest::Url::parse("https://www.braintrust.dev/docs/llms.txt").expect("base");
        let links = collect_docs_links(body, &workflow_set, &link_re, &bare_re, &base);

        let evaluate_url = links
            .iter()
            .find(|(_, workflow, _)| workflow == "evaluate")
            .map(|(_, _, url)| url.clone())
            .expect("evaluate url");
        assert!(evaluate_url.ends_with("/index.md"));

        let deploy_url = links
            .iter()
            .find(|(_, workflow, _)| workflow == "deploy")
            .map(|(_, _, url)| url.clone())
            .expect("deploy url");
        assert!(deploy_url.ends_with(".md"));
    }
}
