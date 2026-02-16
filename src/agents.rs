use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use dialoguer::{theme::ColorfulTheme, MultiSelect};
use regex::Regex;
use reqwest::Client;
use serde::Serialize;
use serde_json::{Map, Value};

use crate::args::BaseArgs;

const SHARED_SKILL_BODY: &str = include_str!("../skills/shared/braintrust-cli-body.md");
const SHARED_WORKFLOW_GUIDE: &str = include_str!("../skills/shared/workflows.md");
const BT_README: &str = include_str!("../README.md");
const DEFAULT_DOCS_LLMS_URL: &str = "https://www.braintrust.dev/docs/llms.txt";
const DEFAULT_DOCS_LLMS_FULL_URL: &str = "https://www.braintrust.dev/docs/llms-full.txt";

#[derive(Debug, Clone, Args)]
pub struct AgentsArgs {
    #[command(subcommand)]
    command: Option<AgentsSubcommand>,
}

#[derive(Debug, Clone, Subcommand)]
enum AgentsSubcommand {
    /// Configure coding agents to use Braintrust
    Setup(AgentsSetupArgs),
    /// Fetch docs markdown for workflow-oriented agent skills
    Docs(AgentsDocsArgs),
}

#[derive(Debug, Clone, Args)]
pub struct SkillsCompatArgs {
    /// Install coding-agent integrations
    #[arg(long)]
    install: bool,

    #[command(flatten)]
    setup: AgentsSetupArgs,
}

#[derive(Debug, Clone, Args)]
struct AgentsSetupArgs {
    /// Agent(s) to configure (repeatable)
    #[arg(long = "agent", value_enum)]
    agents: Vec<AgentArg>,

    /// Configure the current git repo root
    #[arg(long, conflicts_with = "global")]
    local: bool,

    /// Configure user-wide state
    #[arg(long)]
    global: bool,

    /// Also configure MCP server settings
    #[arg(long)]
    with_mcp: bool,

    /// Skip confirmation prompts and use defaults
    #[arg(long, short = 'y')]
    yes: bool,
}

#[derive(Debug, Clone, Args)]
struct AgentsDocsArgs {
    #[command(subcommand)]
    command: Option<AgentsDocsSubcommand>,
}

#[derive(Debug, Clone, Subcommand)]
enum AgentsDocsSubcommand {
    /// Download workflow docs markdown from Mintlify llms index
    Fetch(AgentsDocsFetchArgs),
}

#[derive(Debug, Clone, Args)]
struct AgentsDocsFetchArgs {
    /// llms index URL (Mintlify markdown index)
    #[arg(long, default_value = DEFAULT_DOCS_LLMS_URL)]
    llms_url: String,

    /// Output directory for downloaded docs
    #[arg(long, default_value = "skills/docs")]
    output_dir: PathBuf,

    /// Workflow(s) to include (repeatable)
    #[arg(long = "workflow", value_enum)]
    workflows: Vec<WorkflowArg>,

    /// Discover links only; do not write files
    #[arg(long)]
    dry_run: bool,

    /// Fail command if any page download fails
    #[arg(long)]
    strict: bool,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, ValueEnum, Serialize)]
#[serde(rename_all = "lowercase")]
enum AgentArg {
    Claude,
    Codex,
    Cursor,
    Opencode,
    All,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize)]
#[serde(rename_all = "lowercase")]
enum Agent {
    Claude,
    Codex,
    Cursor,
    Opencode,
}

impl Agent {
    fn as_str(self) -> &'static str {
        match self {
            Agent::Claude => "claude",
            Agent::Codex => "codex",
            Agent::Cursor => "cursor",
            Agent::Opencode => "opencode",
        }
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, ValueEnum)]
#[serde(rename_all = "lowercase")]
enum WorkflowArg {
    Instrument,
    Observe,
    Annotate,
    Evaluate,
    Deploy,
    All,
}

impl WorkflowArg {
    fn as_str(self) -> &'static str {
        match self {
            WorkflowArg::Instrument => "instrument",
            WorkflowArg::Observe => "observe",
            WorkflowArg::Annotate => "annotate",
            WorkflowArg::Evaluate => "evaluate",
            WorkflowArg::Deploy => "deploy",
            WorkflowArg::All => "all",
        }
    }
}

#[derive(Debug, Clone, Copy)]
enum InstallScope {
    Local,
    Global,
}

impl InstallScope {
    fn as_str(self) -> &'static str {
        match self {
            InstallScope::Local => "local",
            InstallScope::Global => "global",
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
enum InstallStatus {
    Installed,
    Skipped,
    Failed,
}

#[derive(Debug, Clone, Serialize)]
struct AgentInstallResult {
    agent: Agent,
    status: InstallStatus,
    message: String,
    #[serde(default)]
    paths: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DetectionSignal {
    agent: Agent,
    reason: String,
}

#[derive(Debug, Serialize)]
struct SetupJsonReport {
    scope: String,
    selected_agents: Vec<Agent>,
    detected_agents: Vec<DetectionSignal>,
    results: Vec<AgentInstallResult>,
    warnings: Vec<String>,
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

pub async fn run(base: BaseArgs, args: AgentsArgs) -> Result<()> {
    match args.command {
        Some(AgentsSubcommand::Setup(setup)) => run_setup(base, setup),
        Some(AgentsSubcommand::Docs(docs)) => run_docs(base, docs).await,
        None => {
            bail!("subcommand required. Use: `bt agents setup --local|--global [--agent ...]`")
        }
    }
}

pub async fn run_skills_compat(base: BaseArgs, args: SkillsCompatArgs) -> Result<()> {
    if !args.install {
        bail!("`bt skills` compatibility mode requires --install");
    }
    run_setup(base, args.setup)
}

async fn run_docs(base: BaseArgs, args: AgentsDocsArgs) -> Result<()> {
    match args.command {
        Some(AgentsDocsSubcommand::Fetch(fetch)) => run_docs_fetch(base, fetch).await,
        None => bail!("subcommand required. Use: `bt agents docs fetch [--workflow ...]`"),
    }
}

async fn run_docs_fetch(base: BaseArgs, args: AgentsDocsFetchArgs) -> Result<()> {
    let selected_workflows = resolve_workflow_selection(&args.workflows);
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
                format!(
                    "failed to fetch fallback index {}",
                    DEFAULT_DOCS_LLMS_FULL_URL
                )
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

    let mut written = 0usize;
    let mut failed = 0usize;
    let mut file_results = Vec::new();
    let mut warnings = Vec::new();
    let mut seen_targets = BTreeSet::new();

    if !args.dry_run {
        fs::create_dir_all(&args.output_dir).with_context(|| {
            format!(
                "failed to create output directory {}",
                args.output_dir.display()
            )
        })?;
    }

    for (title, workflow, url) in discovered {
        if args.dry_run {
            file_results.push(DocsFileResult {
                title,
                url,
                workflow,
                status: "discovered".to_string(),
                path: None,
                error: None,
            });
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
            file_results.push(DocsFileResult {
                title,
                url,
                workflow,
                status: "skipped".to_string(),
                path: None,
                error: Some("duplicate output path".to_string()),
            });
            continue;
        }
        if let Some(parent) = target.parent() {
            fs::create_dir_all(parent).with_context(|| {
                format!("failed to create docs subdirectory {}", parent.display())
            })?;
        }

        let fetch_result = async {
            let response = client
                .get(&url)
                .send()
                .await
                .with_context(|| format!("failed to fetch docs page {url}"))?;
            if !response.status().is_success() {
                let status = response.status();
                let body = response.text().await.unwrap_or_default();
                bail!("docs page returned error ({status}): {body}");
            }
            let content = response
                .text()
                .await
                .with_context(|| format!("failed to read docs page body {url}"))?;
            write_text_file(&target, &content)?;
            Result::<()>::Ok(())
        }
        .await;

        match fetch_result {
            Ok(()) => {
                written += 1;
                file_results.push(DocsFileResult {
                    title,
                    url,
                    workflow,
                    status: "written".to_string(),
                    path: Some(target.display().to_string()),
                    error: None,
                });
            }
            Err(err) => {
                failed += 1;
                let warning = format!("failed to fetch {url}: {err}");
                warnings.push(warning.clone());
                file_results.push(DocsFileResult {
                    title,
                    url,
                    workflow,
                    status: "failed".to_string(),
                    path: None,
                    error: Some(err.to_string()),
                });
            }
        }
    }

    if !args.dry_run {
        write_docs_indexes(
            &args.output_dir,
            selected_workflows
                .iter()
                .map(|workflow| workflow.as_str())
                .collect::<Vec<_>>(),
            &file_results,
        )?;
    }

    if base.json {
        let report = DocsFetchJsonReport {
            llms_url: args.llms_url,
            output_dir: args.output_dir.display().to_string(),
            dry_run: args.dry_run,
            discovered: file_results.len(),
            written,
            failed,
            workflows: selected_workflows
                .iter()
                .map(|workflow| workflow.as_str().to_string())
                .collect(),
            files: file_results,
            warnings: warnings.clone(),
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&report).context("failed to serialize docs report")?
        );
    } else {
        println!("Fetched docs index: {}", args.llms_url);
        println!(
            "Workflows: {}",
            workflow_set.into_iter().collect::<Vec<_>>().join(", ")
        );
        println!(
            "Discovered {} page(s), wrote {} page(s){}",
            file_results.len(),
            written,
            if args.dry_run { " (dry-run)" } else { "" }
        );
        for file in &file_results {
            match &file.path {
                Some(path) => println!("  - {} [{}] -> {}", file.title, file.workflow, path),
                None => println!("  - {} [{}] -> {}", file.title, file.workflow, file.url),
            }
        }
        if !warnings.is_empty() {
            println!("Warnings:");
            for warning in &warnings {
                println!("  - {warning}");
            }
        }
    }

    if args.strict && failed > 0 {
        bail!("{} docs page(s) failed to download in strict mode", failed);
    }

    Ok(())
}

fn run_setup(base: BaseArgs, args: AgentsSetupArgs) -> Result<()> {
    let scope = resolve_scope(&args)?;
    let local_root = if matches!(scope, InstallScope::Local) {
        Some(find_git_root().ok_or_else(|| {
            anyhow!(
                "--local requires running inside a git repository (could not find .git in parent chain)"
            )
        })?)
    } else {
        None
    };
    let home = home_dir().ok_or_else(|| anyhow!("failed to resolve HOME/USERPROFILE"))?;

    let detected = detect_agents(local_root.as_deref(), &home);
    let selected_agents = select_agents(&args, &detected)?;

    if selected_agents.is_empty() {
        bail!("no agents selected for installation");
    }

    let mut warnings = Vec::new();
    let mut results = Vec::new();

    for agent in selected_agents.iter().copied() {
        let result = match agent {
            Agent::Claude => install_claude(scope, local_root.as_deref(), &home, args.with_mcp),
            Agent::Codex => install_codex(scope, local_root.as_deref(), &home, args.with_mcp),
            Agent::Cursor => install_cursor(scope, local_root.as_deref(), &home, args.with_mcp),
            Agent::Opencode => install_opencode(scope, local_root.as_deref(), &home, args.with_mcp),
        };

        match result {
            Ok(r) => {
                if matches!(r.status, InstallStatus::Skipped)
                    && r.message.to_ascii_lowercase().contains("warning")
                {
                    warnings.push(r.message.clone());
                }
                results.push(r);
            }
            Err(err) => {
                results.push(AgentInstallResult {
                    agent,
                    status: InstallStatus::Failed,
                    message: format!("install failed: {err}"),
                    paths: Vec::new(),
                });
            }
        }
    }

    let installed_count = results
        .iter()
        .filter(|r| matches!(r.status, InstallStatus::Installed))
        .count();

    if base.json {
        let report = SetupJsonReport {
            scope: scope.as_str().to_string(),
            selected_agents,
            detected_agents: detected,
            results: results.clone(),
            warnings,
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&report).context("failed to serialize setup report")?
        );
    } else {
        print_human_report(scope, &selected_agents, &detected, &results);
    }

    if installed_count == 0 {
        bail!("no agents were installed successfully");
    }

    Ok(())
}

fn resolve_scope(args: &AgentsSetupArgs) -> Result<InstallScope> {
    if args.local {
        return Ok(InstallScope::Local);
    }
    if args.global {
        return Ok(InstallScope::Global);
    }
    if args.yes {
        return Ok(InstallScope::Global);
    }

    if !std::io::stdin().is_terminal() {
        bail!("scope required in non-interactive mode: pass --local or --global");
    }

    let choices = ["local (current git repo)", "global (user-wide)"];
    let idx = crate::ui::fuzzy_select("Select install scope", &choices)?;
    Ok(if idx == 0 {
        InstallScope::Local
    } else {
        InstallScope::Global
    })
}

fn resolve_selected_agents(requested: &[AgentArg], detected: &[DetectionSignal]) -> Vec<Agent> {
    if requested.is_empty() {
        let mut inferred = BTreeSet::new();
        for signal in detected {
            inferred.insert(signal.agent);
        }
        if inferred.is_empty() {
            return vec![Agent::Claude, Agent::Codex, Agent::Cursor, Agent::Opencode];
        }
        return inferred.into_iter().collect();
    }

    if requested.contains(&AgentArg::All) {
        return vec![Agent::Claude, Agent::Codex, Agent::Cursor, Agent::Opencode];
    }

    let mut out = BTreeSet::new();
    for value in requested {
        let mapped = match value {
            AgentArg::Claude => Some(Agent::Claude),
            AgentArg::Codex => Some(Agent::Codex),
            AgentArg::Cursor => Some(Agent::Cursor),
            AgentArg::Opencode => Some(Agent::Opencode),
            AgentArg::All => None,
        };
        if let Some(agent) = mapped {
            out.insert(agent);
        }
    }
    out.into_iter().collect()
}

fn resolve_workflow_selection(requested: &[WorkflowArg]) -> Vec<WorkflowArg> {
    if requested.is_empty() || requested.contains(&WorkflowArg::All) {
        return vec![
            WorkflowArg::Instrument,
            WorkflowArg::Observe,
            WorkflowArg::Annotate,
            WorkflowArg::Evaluate,
            WorkflowArg::Deploy,
        ];
    }

    let mut out = BTreeSet::new();
    for workflow in requested {
        if !matches!(workflow, WorkflowArg::All) {
            out.insert(*workflow);
        }
    }
    out.into_iter().collect()
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
    let mut discovered = Vec::new();
    let mut seen = BTreeSet::new();

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
        if !seen.insert(url.clone()) {
            continue;
        }
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
        if !seen.insert(url.clone()) {
            continue;
        }
        discovered.push((slug_for_url(&url), workflow.to_string(), url));
    }

    discovered
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

fn write_docs_indexes(
    output_dir: &Path,
    workflows: Vec<&str>,
    files: &[DocsFileResult],
) -> Result<()> {
    let mut top_lines = Vec::new();
    top_lines.push("# Braintrust Workflow Docs".to_string());
    top_lines.push(String::new());
    top_lines.push("Generated by `bt agents docs fetch`.".to_string());
    top_lines.push(String::new());

    for workflow in workflows {
        let workflow_files = files
            .iter()
            .filter(|file| file.workflow == workflow && file.status == "written")
            .collect::<Vec<_>>();

        top_lines.push(format!("## {}", workflow));
        if workflow_files.is_empty() {
            top_lines.push("- no pages downloaded".to_string());
            top_lines.push(String::new());
            continue;
        }

        let mut workflow_index_lines = Vec::new();
        workflow_index_lines.push(format!("# {} Docs", workflow));
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
        top_lines.push(format!("- [_index]({}/_index.md)", workflow));
        top_lines.push(String::new());
    }

    let top_index = output_dir.join("README.md");
    write_text_file(&top_index, &top_lines.join("\n"))?;
    Ok(())
}

fn select_agents(args: &AgentsSetupArgs, detected: &[DetectionSignal]) -> Result<Vec<Agent>> {
    let inferred = resolve_selected_agents(&args.agents, detected);
    if !args.agents.is_empty() || args.yes || !std::io::stdin().is_terminal() {
        return Ok(inferred);
    }

    let all = [Agent::Claude, Agent::Codex, Agent::Cursor, Agent::Opencode];
    let defaults: BTreeSet<Agent> = inferred.into_iter().collect();
    let mut labels = Vec::with_capacity(all.len());
    let mut default_flags = Vec::with_capacity(all.len());

    for agent in all {
        let detected_count = detected
            .iter()
            .filter(|signal| signal.agent == agent)
            .count();
        if detected_count > 0 {
            labels.push(format!("{} (detected: {detected_count})", agent.as_str()));
        } else {
            labels.push(agent.as_str().to_string());
        }
        default_flags.push(defaults.contains(&agent));
    }

    let selected = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Select agents to configure")
        .items(&labels)
        .defaults(&default_flags)
        .interact()?;

    let mut out = Vec::with_capacity(selected.len());
    for index in selected {
        out.push(all[index]);
    }

    Ok(out)
}

fn detect_agents(local_root: Option<&Path>, home: &Path) -> Vec<DetectionSignal> {
    let mut by_agent: BTreeMap<Agent, BTreeSet<String>> = BTreeMap::new();

    if let Some(root) = local_root {
        if root.join(".claude").exists() {
            add_signal(&mut by_agent, Agent::Claude, ".claude exists in repo root");
        }
        if root.join(".cursor").exists() {
            add_signal(&mut by_agent, Agent::Cursor, ".cursor exists in repo root");
        }
        if root.join(".opencode").exists() {
            add_signal(
                &mut by_agent,
                Agent::Opencode,
                ".opencode exists in repo root",
            );
        }
        if root.join(".agents").exists() || root.join(".agents/skills").exists() {
            add_signal(
                &mut by_agent,
                Agent::Codex,
                ".agents/skills exists in repo root",
            );
            add_signal(
                &mut by_agent,
                Agent::Opencode,
                ".agents/skills exists in repo root",
            );
        }
        if root.join("AGENTS.md").exists() {
            add_signal(&mut by_agent, Agent::Codex, "AGENTS.md exists in repo root");
        }
    }

    if home.join(".claude").exists() {
        add_signal(&mut by_agent, Agent::Claude, "~/.claude exists");
    }
    if home.join(".codex").exists() {
        add_signal(&mut by_agent, Agent::Codex, "~/.codex exists");
    }
    if home.join(".agents/skills").exists() {
        add_signal(&mut by_agent, Agent::Codex, "~/.agents/skills exists");
        add_signal(&mut by_agent, Agent::Opencode, "~/.agents/skills exists");
    }
    if home.join(".opencode").exists() || home.join(".config/opencode").exists() {
        add_signal(
            &mut by_agent,
            Agent::Opencode,
            "opencode config directory exists",
        );
    }

    if command_exists("claude") {
        add_signal(
            &mut by_agent,
            Agent::Claude,
            "`claude` binary found in PATH",
        );
    }

    let mut out = Vec::new();
    for (agent, reasons) in by_agent {
        for reason in reasons {
            out.push(DetectionSignal { agent, reason });
        }
    }
    out
}

fn add_signal(map: &mut BTreeMap<Agent, BTreeSet<String>>, agent: Agent, reason: &str) {
    map.entry(agent).or_default().insert(reason.to_string());
}

fn install_claude(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
    with_mcp: bool,
) -> Result<AgentInstallResult> {
    let root = scope_root(scope, local_root, home)?;
    let skill_path = root.join(".claude/skills/braintrust/SKILL.md");
    let skill_content = render_braintrust_skill();
    write_text_file(&skill_path, &skill_content)?;

    let mut paths = vec![skill_path.display().to_string()];
    if with_mcp {
        let mcp_path = match scope {
            InstallScope::Local => root.join(".mcp.json"),
            InstallScope::Global => home.join(".mcp.json"),
        };
        merge_mcp_config(&mcp_path)?;
        paths.push(mcp_path.display().to_string());
    }

    Ok(AgentInstallResult {
        agent: Agent::Claude,
        status: InstallStatus::Installed,
        message: if with_mcp {
            "installed skill and MCP config".to_string()
        } else {
            "installed skill".to_string()
        },
        paths,
    })
}

fn install_codex(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
    with_mcp: bool,
) -> Result<AgentInstallResult> {
    let root = scope_root(scope, local_root, home)?;
    let skill_path = root.join(".agents/skills/braintrust/SKILL.md");
    let skill_content = render_braintrust_skill();
    write_text_file(&skill_path, &skill_content)?;

    let mut paths = vec![skill_path.display().to_string()];

    if with_mcp {
        let mcp_path = match scope {
            InstallScope::Local => root.join(".mcp.json"),
            InstallScope::Global => home.join(".mcp.json"),
        };
        merge_mcp_config(&mcp_path)?;
        paths.push(mcp_path.display().to_string());
    }

    Ok(AgentInstallResult {
        agent: Agent::Codex,
        status: InstallStatus::Installed,
        message: if with_mcp {
            "installed skill and MCP config".to_string()
        } else {
            "installed skill".to_string()
        },
        paths,
    })
}

fn install_opencode(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
    with_mcp: bool,
) -> Result<AgentInstallResult> {
    let root = scope_root(scope, local_root, home)?;
    let skill_path = root.join(".agents/skills/braintrust/SKILL.md");
    let skill_content = render_braintrust_skill();
    write_text_file(&skill_path, &skill_content)?;

    let mut paths = vec![skill_path.display().to_string()];
    if with_mcp {
        let mcp_path = match scope {
            InstallScope::Local => root.join(".mcp.json"),
            InstallScope::Global => home.join(".mcp.json"),
        };
        merge_mcp_config(&mcp_path)?;
        paths.push(mcp_path.display().to_string());
    }

    Ok(AgentInstallResult {
        agent: Agent::Opencode,
        status: InstallStatus::Installed,
        message: if with_mcp {
            "installed skill and MCP config".to_string()
        } else {
            "installed skill".to_string()
        },
        paths,
    })
}

fn install_cursor(
    scope: InstallScope,
    local_root: Option<&Path>,
    _home: &Path,
    with_mcp: bool,
) -> Result<AgentInstallResult> {
    if matches!(scope, InstallScope::Global) {
        return Ok(AgentInstallResult {
            agent: Agent::Cursor,
            status: InstallStatus::Skipped,
            message: "warning: cursor currently supports only --local in bt agents setup"
                .to_string(),
            paths: Vec::new(),
        });
    }

    let root = scope_root(scope, local_root, _home)?;
    let rule_path = root.join(".cursor/rules/braintrust.mdc");
    let cursor_rule = render_cursor_rule();
    write_text_file(&rule_path, &cursor_rule)?;

    let mut paths = vec![rule_path.display().to_string()];
    if with_mcp {
        let mcp_path = root.join(".cursor/mcp.json");
        merge_mcp_config(&mcp_path)?;
        paths.push(mcp_path.display().to_string());
    }

    Ok(AgentInstallResult {
        agent: Agent::Cursor,
        status: InstallStatus::Installed,
        message: if with_mcp {
            "installed rule and MCP config".to_string()
        } else {
            "installed rule".to_string()
        },
        paths,
    })
}

fn merge_mcp_config(path: &Path) -> Result<()> {
    let mut root = load_json_object_or_default(path)?;
    let servers_value = root
        .entry("mcpServers".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let servers = servers_value.as_object_mut().ok_or_else(|| {
        anyhow!(
            "field 'mcpServers' in {} must be a JSON object",
            path.display()
        )
    })?;

    servers.insert(
        "braintrust".to_string(),
        serde_json::json!({
            "type": "http",
            "url": "https://api.braintrust.dev/mcp",
            "headers": {
                "Authorization": "Bearer ${BRAINTRUST_API_KEY}"
            }
        }),
    );

    write_json_object(path, &root)
}

fn load_json_object_or_default(path: &Path) -> Result<Map<String, Value>> {
    if !path.exists() {
        return Ok(Map::new());
    }

    let data = fs::read_to_string(path)
        .with_context(|| format!("failed to read JSON file {}", path.display()))?;
    let value: Value = serde_json::from_str(&data)
        .with_context(|| format!("failed to parse JSON file {}", path.display()))?;

    value
        .as_object()
        .cloned()
        .ok_or_else(|| anyhow!("{} must contain a JSON object", path.display()))
}

fn write_json_object(path: &Path, object: &Map<String, Value>) -> Result<()> {
    let data = serde_json::to_string_pretty(&Value::Object(object.clone()))
        .with_context(|| format!("failed to serialize JSON for {}", path.display()))?;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    let tmp = path.with_extension("tmp");
    fs::write(&tmp, format!("{data}\n"))
        .with_context(|| format!("failed to finalize temp JSON file {}", tmp.display()))?;
    fs::rename(&tmp, path).with_context(|| format!("failed to replace {}", path.display()))?;

    Ok(())
}

fn write_text_file(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    fs::write(path, format!("{}\n", content.trim_end()))
        .with_context(|| format!("failed to write {}", path.display()))
}

fn render_braintrust_skill() -> String {
    format!(
        "---\nname: braintrust-cli\nversion: 1.0.0\ndescription: Use the Braintrust `bt` CLI for projects, traces, prompts, and key Braintrust workflows.\n---\n\n## Purpose\n\n{}\n\n## Key Workflows\n\n{}\n\n## bt CLI Reference (Inlined README)\n\n{}",
        SHARED_SKILL_BODY.trim(),
        SHARED_WORKFLOW_GUIDE.trim(),
        BT_README.trim()
    )
}

fn render_cursor_rule() -> String {
    format!(
        "---\ndescription: Braintrust CLI workflow\nalwaysApply: false\n---\n\n{}",
        SHARED_SKILL_BODY.trim()
    )
}

fn scope_root<'a>(
    scope: InstallScope,
    local_root: Option<&'a Path>,
    home: &'a Path,
) -> Result<&'a Path> {
    match scope {
        InstallScope::Local => {
            local_root.ok_or_else(|| anyhow!("local scope requires a git repository root"))
        }
        InstallScope::Global => Ok(home),
    }
}

fn find_git_root() -> Option<PathBuf> {
    let mut current = std::env::current_dir().ok()?;
    loop {
        if current.join(".git").exists() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

fn home_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        std::env::var_os("USERPROFILE").map(PathBuf::from)
    }

    #[cfg(not(windows))]
    {
        std::env::var_os("HOME").map(PathBuf::from)
    }
}

fn command_exists(binary: &str) -> bool {
    let Some(path_var) = std::env::var_os("PATH") else {
        return false;
    };

    for dir in std::env::split_paths(&path_var) {
        let candidate = dir.join(binary);
        if candidate.is_file() {
            return true;
        }
        #[cfg(windows)]
        {
            if dir.join(format!("{binary}.exe")).is_file() {
                return true;
            }
        }
    }

    false
}

fn print_human_report(
    scope: InstallScope,
    selected_agents: &[Agent],
    detected: &[DetectionSignal],
    results: &[AgentInstallResult],
) {
    println!("Configuring coding agents for Braintrust");
    println!("Scope: {}", scope.as_str());

    let selected = selected_agents
        .iter()
        .map(|a| a.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    println!("Selected agents: {selected}");

    if !detected.is_empty() {
        println!("Detected signals:");
        for signal in detected {
            println!("  - {}: {}", signal.agent.as_str(), signal.reason);
        }
    }

    println!("Results:");
    for result in results {
        let status = match result.status {
            InstallStatus::Installed => "installed",
            InstallStatus::Skipped => "skipped",
            InstallStatus::Failed => "failed",
        };
        println!(
            "  - {}: {} ({})",
            result.agent.as_str(),
            status,
            result.message
        );
        for path in &result.paths {
            println!("      path: {path}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn all_agent_arg_expands_to_all_agents() {
        let detected = vec![];
        let resolved = resolve_selected_agents(&[AgentArg::All], &detected);
        assert_eq!(
            resolved,
            vec![Agent::Claude, Agent::Codex, Agent::Cursor, Agent::Opencode]
        );
    }

    #[test]
    fn detection_drives_default_selection() {
        let detected = vec![DetectionSignal {
            agent: Agent::Codex,
            reason: "hint".to_string(),
        }];
        let resolved = resolve_selected_agents(&[], &detected);
        assert_eq!(resolved, vec![Agent::Codex]);
    }

    #[test]
    fn merge_mcp_config_upserts_braintrust_server() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("bt-agents-mcp-{unique}"));
        fs::create_dir_all(&dir).expect("create temp dir");
        let path = dir.join("mcp.json");

        fs::write(
            &path,
            r#"{ "mcpServers": { "existing": { "type": "http", "url": "https://example.com" } } }"#,
        )
        .expect("seed mcp");

        merge_mcp_config(&path).expect("merge mcp");

        let parsed: Value =
            serde_json::from_str(&fs::read_to_string(&path).expect("read mcp")).expect("json");
        let servers = parsed
            .get("mcpServers")
            .and_then(|v| v.as_object())
            .expect("servers object");
        assert!(servers.contains_key("existing"));
        assert!(servers.contains_key("braintrust"));
    }

    #[test]
    fn find_git_root_detects_git_file() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-agents-git-{unique}"));
        let nested = root.join("a/b/c");

        fs::create_dir_all(&nested).expect("create nested");
        fs::write(root.join(".git"), "gitdir: /tmp/fake").expect("write git file");

        let old = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&nested).expect("cd nested");
        let detected = find_git_root();
        std::env::set_current_dir(old).expect("restore cwd");

        let detected = detected
            .as_deref()
            .and_then(|p| p.canonicalize().ok())
            .expect("canonical detected path");
        let expected = root.canonicalize().expect("canonical expected path");
        assert_eq!(detected, expected);
    }

    #[test]
    fn resolve_workflow_selection_defaults_to_all() {
        let resolved = resolve_workflow_selection(&[]);
        assert_eq!(
            resolved,
            vec![
                WorkflowArg::Instrument,
                WorkflowArg::Observe,
                WorkflowArg::Annotate,
                WorkflowArg::Evaluate,
                WorkflowArg::Deploy
            ]
        );
    }

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
}
