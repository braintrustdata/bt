use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use dialoguer::{theme::ColorfulTheme, FuzzySelect, MultiSelect};
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
pub struct SetupArgs {
    #[command(subcommand)]
    command: Option<SetupSubcommand>,

    #[command(flatten)]
    agents: AgentsSetupArgs,
}

#[derive(Debug, Clone, Subcommand)]
enum SetupSubcommand {
    /// Configure coding-agent skills to use Braintrust
    Skills(AgentsSetupArgs),
    /// Configure MCP server settings for coding agents
    Mcp(AgentsMcpSetupArgs),
    /// Diagnose coding-agent setup for Braintrust
    Doctor(AgentsDoctorArgs),
}

#[derive(Debug, Clone, Args)]
pub struct DocsArgs {
    #[command(subcommand)]
    command: Option<DocsSubcommand>,

    #[command(flatten)]
    fetch: AgentsDocsFetchArgs,
}

#[derive(Debug, Clone, Subcommand)]
enum DocsSubcommand {
    /// Download workflow docs markdown from Mintlify llms index
    Fetch(AgentsDocsFetchArgs),
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

    /// Workflow docs to prefetch (repeatable)
    #[arg(long = "workflow", value_enum)]
    workflows: Vec<WorkflowArg>,

    /// Skip confirmation prompts and use defaults
    #[arg(long, short = 'y')]
    yes: bool,

    /// Do not auto-fetch workflow docs during setup
    #[arg(long)]
    no_fetch_docs: bool,

    /// Refresh prefetched docs by clearing existing output before download
    #[arg(long, conflicts_with = "no_fetch_docs")]
    refresh_docs: bool,
}

#[derive(Debug, Clone, Args)]
struct AgentsMcpSetupArgs {
    /// Agent(s) to configure MCP for (repeatable)
    #[arg(long = "agent", value_enum)]
    agents: Vec<AgentArg>,

    /// Configure MCP in the current git repo root
    #[arg(long, conflicts_with = "global")]
    local: bool,

    /// Configure MCP in user-wide state
    #[arg(long)]
    global: bool,

    /// Skip confirmation prompts and use defaults
    #[arg(long, short = 'y')]
    yes: bool,
}

#[derive(Debug, Clone, Args)]
struct AgentsDoctorArgs {
    /// Diagnose local repo setup
    #[arg(long, conflicts_with = "global")]
    local: bool,

    /// Diagnose user-wide setup
    #[arg(long)]
    global: bool,
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

    /// Refresh docs by clearing output directory before download
    #[arg(long)]
    refresh: bool,
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
    notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DoctorAgentStatus {
    agent: Agent,
    detected: bool,
    detected_signals: Vec<String>,
    configured: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    config_path: Option<String>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    notes: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
struct DoctorJsonReport {
    scope: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    root: Option<String>,
    docs_path: String,
    docs_present: bool,
    agents: Vec<DoctorAgentStatus>,
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

#[derive(Debug, Clone)]
struct DocsFetchResult {
    discovered: usize,
    written: usize,
    failed: usize,
    files: Vec<DocsFileResult>,
    warnings: Vec<String>,
}

struct SetupSelection {
    scope: InstallScope,
    local_root: Option<PathBuf>,
    detected: Vec<DetectionSignal>,
    selected_agents: Vec<Agent>,
    selected_workflows: Vec<WorkflowArg>,
}

struct McpSelection {
    scope: InstallScope,
    local_root: Option<PathBuf>,
    detected: Vec<DetectionSignal>,
    selected_agents: Vec<Agent>,
}

pub async fn run_setup_top(base: BaseArgs, args: SetupArgs) -> Result<()> {
    match args.command {
        Some(SetupSubcommand::Skills(setup)) => run_setup(base, setup).await,
        Some(SetupSubcommand::Mcp(mcp)) => run_mcp_setup(base, mcp),
        Some(SetupSubcommand::Doctor(doctor)) => run_doctor(base, doctor),
        None => run_setup(base, args.agents).await,
    }
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

async fn run_docs_fetch(base: BaseArgs, args: AgentsDocsFetchArgs) -> Result<()> {
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

async fn fetch_docs_pages(
    args: &AgentsDocsFetchArgs,
    selected_workflows: &[WorkflowArg],
) -> Result<DocsFetchResult> {
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

    Ok(DocsFetchResult {
        discovered: file_results.len(),
        written,
        failed,
        files: file_results,
        warnings,
    })
}

async fn run_setup(base: BaseArgs, args: AgentsSetupArgs) -> Result<()> {
    let home = home_dir().ok_or_else(|| anyhow!("failed to resolve HOME/USERPROFILE"))?;
    let selection = resolve_setup_selection(&args, &home)?;
    let scope = selection.scope;
    let local_root = selection.local_root;
    let detected = selection.detected;
    let selected_agents = selection.selected_agents;
    let selected_workflows = selection.selected_workflows;
    let mut warnings = Vec::new();
    let mut notes = Vec::new();
    let mut results = Vec::new();

    for agent in selected_agents.iter().copied() {
        let result = match agent {
            Agent::Claude => install_claude(scope, local_root.as_deref(), &home),
            Agent::Codex => install_codex(scope, local_root.as_deref(), &home),
            Agent::Cursor => install_cursor(scope, local_root.as_deref(), &home),
            Agent::Opencode => install_opencode(scope, local_root.as_deref(), &home),
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

    if installed_count == 0 {
        notes.push("Skipped workflow docs prefetch (no agents installed).".to_string());
    } else if args.no_fetch_docs {
        notes.push("Skipped workflow docs prefetch (`--no-fetch-docs`).".to_string());
    } else if selected_workflows.is_empty() {
        notes.push("Skipped workflow docs prefetch (no workflows selected).".to_string());
    } else {
        let docs_output_dir = setup_docs_output_dir(scope, local_root.as_deref(), &home)?;
        let docs_args = AgentsDocsFetchArgs {
            llms_url: DEFAULT_DOCS_LLMS_URL.to_string(),
            output_dir: docs_output_dir.clone(),
            workflows: selected_workflows.clone(),
            dry_run: false,
            strict: false,
            refresh: args.refresh_docs,
        };
        match fetch_docs_pages(&docs_args, &selected_workflows).await {
            Ok(fetch_result) => {
                notes.push(format!(
                    "Prefetched workflow docs ({}) to {} ({} written, {} failed).",
                    selected_workflows
                        .iter()
                        .map(|workflow| workflow.as_str())
                        .collect::<Vec<_>>()
                        .join(", "),
                    docs_output_dir.display(),
                    fetch_result.written,
                    fetch_result.failed
                ));
                warnings.extend(fetch_result.warnings);
            }
            Err(err) => {
                warnings.push(format!("workflow docs prefetch failed: {err}"));
            }
        }
    }

    if base.json {
        let report = SetupJsonReport {
            scope: scope.as_str().to_string(),
            selected_agents,
            detected_agents: detected,
            results: results.clone(),
            warnings,
            notes,
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&report).context("failed to serialize setup report")?
        );
    } else {
        print_human_report(
            scope,
            &selected_agents,
            &detected,
            &results,
            &warnings,
            &notes,
        );
    }

    if installed_count == 0 {
        bail!("no agents were installed successfully");
    }

    Ok(())
}

fn run_mcp_setup(base: BaseArgs, args: AgentsMcpSetupArgs) -> Result<()> {
    let home = home_dir().ok_or_else(|| anyhow!("failed to resolve HOME/USERPROFILE"))?;
    let selection = resolve_mcp_selection(&args, &home)?;
    let scope = selection.scope;
    let local_root = selection.local_root;
    let detected = selection.detected;
    let selected_agents = selection.selected_agents;

    let mut warnings = Vec::new();
    let mut results = Vec::new();

    for agent in selected_agents.iter().copied() {
        let result = install_mcp_for_agent(agent, scope, local_root.as_deref(), &home);
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
            results,
            warnings,
            notes: vec!["Configured MCP only (`bt setup mcp`).".to_string()],
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .context("failed to serialize MCP setup report")?
        );
    } else {
        print_mcp_human_report(scope, &selected_agents, &detected, &results, &warnings);
    }

    if installed_count == 0 {
        bail!("no MCP configurations were installed successfully");
    }

    Ok(())
}

fn resolve_setup_selection(args: &AgentsSetupArgs, home: &Path) -> Result<SetupSelection> {
    let mut scope = initial_setup_scope(args);
    let interactive = std::io::stdin().is_terminal() && !args.yes;
    let mut prompted_agents: Option<Vec<Agent>> = None;
    let mut prompted_workflows: Option<Vec<WorkflowArg>> = if args.no_fetch_docs {
        Some(Vec::new())
    } else {
        None
    };

    if interactive {
        #[derive(Clone, Copy)]
        enum SetupWizardStep {
            Scope,
            Agents,
            Workflows,
        }

        let mut steps = Vec::new();
        if scope.is_none() {
            steps.push(SetupWizardStep::Scope);
        }
        if args.agents.is_empty() {
            steps.push(SetupWizardStep::Agents);
        }
        if !args.no_fetch_docs && args.workflows.is_empty() {
            steps.push(SetupWizardStep::Workflows);
        }

        let mut idx = 0usize;
        while idx < steps.len() {
            match steps[idx] {
                SetupWizardStep::Scope => match prompt_scope_selection("Select install scope")? {
                    Some(selected) => {
                        scope = Some(selected);
                        idx += 1;
                    }
                    None => {
                        if idx == 0 {
                            bail!("setup cancelled by user");
                        }
                        idx -= 1;
                    }
                },
                SetupWizardStep::Agents => {
                    let current_scope = scope.ok_or_else(|| anyhow!("scope not selected"))?;
                    let local_root = resolve_local_root_for_scope(current_scope)?;
                    let detected = detect_agents(local_root.as_deref(), home);
                    let defaults = resolve_selected_agents(&[], &detected);
                    match prompt_agents_selection(&defaults)? {
                        Some(selected) => {
                            prompted_agents = Some(selected);
                            idx += 1;
                        }
                        None => {
                            if idx == 0 {
                                bail!("setup cancelled by user");
                            }
                            idx -= 1;
                        }
                    }
                }
                SetupWizardStep::Workflows => {
                    let defaults = resolve_workflow_selection(&[]);
                    match prompt_workflows_selection(&defaults)? {
                        Some(selected) => {
                            prompted_workflows = Some(selected);
                            idx += 1;
                        }
                        None => {
                            if idx == 0 {
                                bail!("setup cancelled by user");
                            }
                            idx -= 1;
                        }
                    }
                }
            }
        }
    }

    let scope = match scope {
        Some(value) => value,
        None => resolve_scope(args)?,
    };
    let local_root = resolve_local_root_for_scope(scope)?;
    let detected = detect_agents(local_root.as_deref(), home);
    let selected_agents = match prompted_agents {
        Some(value) => value,
        None => resolve_selected_agents(&args.agents, &detected),
    };
    if selected_agents.is_empty() {
        bail!("no agents selected for installation");
    }

    let selected_workflows = if args.no_fetch_docs {
        Vec::new()
    } else if let Some(value) = prompted_workflows {
        value
    } else {
        resolve_workflow_selection(&args.workflows)
    };

    Ok(SetupSelection {
        scope,
        local_root,
        detected,
        selected_agents,
        selected_workflows,
    })
}

fn resolve_mcp_selection(args: &AgentsMcpSetupArgs, home: &Path) -> Result<McpSelection> {
    let mut scope = initial_mcp_scope(args);
    let interactive = std::io::stdin().is_terminal() && !args.yes;
    let mut prompted_agents: Option<Vec<Agent>> = None;

    if interactive {
        #[derive(Clone, Copy)]
        enum McpWizardStep {
            Scope,
            Agents,
        }

        let mut steps = Vec::new();
        if scope.is_none() {
            steps.push(McpWizardStep::Scope);
        }
        if args.agents.is_empty() {
            steps.push(McpWizardStep::Agents);
        }

        let mut idx = 0usize;
        while idx < steps.len() {
            match steps[idx] {
                McpWizardStep::Scope => match prompt_scope_selection("Select MCP setup scope")? {
                    Some(selected) => {
                        scope = Some(selected);
                        idx += 1;
                    }
                    None => {
                        if idx == 0 {
                            bail!("MCP setup cancelled by user");
                        }
                        idx -= 1;
                    }
                },
                McpWizardStep::Agents => {
                    let current_scope = scope.ok_or_else(|| anyhow!("scope not selected"))?;
                    let local_root = resolve_local_root_for_scope(current_scope)?;
                    let detected = detect_agents(local_root.as_deref(), home);
                    let defaults = resolve_selected_agents(&[], &detected);
                    match prompt_agents_selection(&defaults)? {
                        Some(selected) => {
                            prompted_agents = Some(selected);
                            idx += 1;
                        }
                        None => {
                            if idx == 0 {
                                bail!("MCP setup cancelled by user");
                            }
                            idx -= 1;
                        }
                    }
                }
            }
        }
    }

    let scope = match scope {
        Some(value) => value,
        None => resolve_mcp_scope(args)?,
    };
    let local_root = resolve_local_root_for_scope(scope)?;
    let detected = detect_agents(local_root.as_deref(), home);
    let selected_agents = match prompted_agents {
        Some(value) => value,
        None => resolve_selected_agents(&args.agents, &detected),
    };
    if selected_agents.is_empty() {
        bail!("no agents selected for MCP setup");
    }

    Ok(McpSelection {
        scope,
        local_root,
        detected,
        selected_agents,
    })
}

fn run_doctor(base: BaseArgs, args: AgentsDoctorArgs) -> Result<()> {
    let (scope, local_root) = resolve_doctor_scope(&args)?;
    let home = home_dir().ok_or_else(|| anyhow!("failed to resolve HOME/USERPROFILE"))?;
    let docs_output_dir = setup_docs_output_dir(scope, local_root.as_deref(), &home)?;
    let detected = detect_agents(local_root.as_deref(), &home);

    let mut warnings = Vec::new();
    let agents = [Agent::Claude, Agent::Codex, Agent::Cursor, Agent::Opencode]
        .iter()
        .map(|agent| doctor_agent_status(*agent, scope, local_root.as_deref(), &home, &detected))
        .collect::<Vec<_>>();

    if matches!(scope, InstallScope::Global) {
        warnings.push("cursor is local-only in this setup flow".to_string());
    }

    if base.json {
        let report = DoctorJsonReport {
            scope: scope.as_str().to_string(),
            root: local_root.as_ref().map(|p| p.display().to_string()),
            docs_path: docs_output_dir.display().to_string(),
            docs_present: docs_output_dir.exists(),
            agents,
            warnings,
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&report).context("failed to serialize doctor report")?
        );
        return Ok(());
    }

    println!("Braintrust Setup Doctor");
    println!("Scope: {}", scope.as_str());
    if let Some(root) = &local_root {
        println!("Root: {}", root.display());
    }
    println!(
        "Docs cache: {} ({})",
        docs_output_dir.display(),
        if docs_output_dir.exists() {
            "present"
        } else {
            "missing"
        }
    );
    println!("Agents:");
    for status in &agents {
        println!(
            "  - {}: {}{}",
            status.agent.as_str(),
            if status.configured {
                "configured"
            } else {
                "not configured"
            },
            if status.detected { " (detected)" } else { "" }
        );
        if let Some(path) = &status.config_path {
            println!("      path: {path}");
        }
        for signal in &status.detected_signals {
            println!("      signal: {signal}");
        }
        for note in &status.notes {
            println!("      note: {note}");
        }
    }
    if !warnings.is_empty() {
        println!("Warnings:");
        for warning in &warnings {
            println!("  - {warning}");
        }
    }
    Ok(())
}

fn resolve_doctor_scope(args: &AgentsDoctorArgs) -> Result<(InstallScope, Option<PathBuf>)> {
    if args.local {
        let root = find_git_root().ok_or_else(|| {
            anyhow!(
                "--local requires running inside a git repository (could not find .git in parent chain)"
            )
        })?;
        return Ok((InstallScope::Local, Some(root)));
    }
    if args.global {
        return Ok((InstallScope::Global, None));
    }
    if let Some(root) = find_git_root() {
        return Ok((InstallScope::Local, Some(root)));
    }
    Ok((InstallScope::Global, None))
}

fn doctor_agent_status(
    agent: Agent,
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
    detected: &[DetectionSignal],
) -> DoctorAgentStatus {
    let detected_signals = detected
        .iter()
        .filter(|signal| signal.agent == agent)
        .map(|signal| signal.reason.clone())
        .collect::<Vec<_>>();
    let detected_any = !detected_signals.is_empty();

    let mut notes = Vec::new();
    let config_path = match (agent, scope) {
        (Agent::Claude, InstallScope::Local) => local_root
            .map(|root| root.join(".claude/skills/braintrust/SKILL.md"))
            .map(|p| p.display().to_string()),
        (Agent::Claude, InstallScope::Global) => Some(
            home.join(".claude/skills/braintrust/SKILL.md")
                .display()
                .to_string(),
        ),
        (Agent::Codex, InstallScope::Local) | (Agent::Opencode, InstallScope::Local) => local_root
            .map(|root| root.join(".agents/skills/braintrust/SKILL.md"))
            .map(|p| p.display().to_string()),
        (Agent::Codex, InstallScope::Global) | (Agent::Opencode, InstallScope::Global) => Some(
            home.join(".agents/skills/braintrust/SKILL.md")
                .display()
                .to_string(),
        ),
        (Agent::Cursor, InstallScope::Local) => local_root
            .map(|root| root.join(".cursor/rules/braintrust.mdc"))
            .map(|p| p.display().to_string()),
        (Agent::Cursor, InstallScope::Global) => {
            notes.push("cursor currently supports local-only setup in this flow".to_string());
            None
        }
    };

    let configured = config_path
        .as_deref()
        .map(|p| Path::new(p).exists())
        .unwrap_or(false);

    DoctorAgentStatus {
        agent,
        detected: detected_any,
        detected_signals,
        configured,
        config_path,
        notes,
    }
}

fn initial_setup_scope(args: &AgentsSetupArgs) -> Option<InstallScope> {
    if args.local {
        Some(InstallScope::Local)
    } else if args.global || args.yes {
        Some(InstallScope::Global)
    } else {
        None
    }
}

fn initial_mcp_scope(args: &AgentsMcpSetupArgs) -> Option<InstallScope> {
    if args.local {
        Some(InstallScope::Local)
    } else if args.global || args.yes {
        Some(InstallScope::Global)
    } else {
        None
    }
}

fn resolve_local_root_for_scope(scope: InstallScope) -> Result<Option<PathBuf>> {
    if matches!(scope, InstallScope::Local) {
        return Ok(Some(find_git_root().ok_or_else(|| {
            anyhow!(
                "--local requires running inside a git repository (could not find .git in parent chain)"
            )
        })?));
    }
    Ok(None)
}

fn prompt_scope_selection(prompt: &str) -> Result<Option<InstallScope>> {
    let choices = ["local (current git repo)", "global (user-wide)"];
    let idx = FuzzySelect::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(&choices)
        .default(0)
        .interact_opt()?;
    Ok(idx.map(|i| {
        if i == 0 {
            InstallScope::Local
        } else {
            InstallScope::Global
        }
    }))
}

fn prompt_agents_selection(defaults: &[Agent]) -> Result<Option<Vec<Agent>>> {
    let all = [Agent::Claude, Agent::Codex, Agent::Cursor, Agent::Opencode];
    let default_set: BTreeSet<Agent> = defaults.iter().copied().collect();
    let labels = all.iter().map(|agent| agent.as_str()).collect::<Vec<_>>();
    let default_flags = all
        .iter()
        .map(|agent| default_set.contains(agent))
        .collect::<Vec<_>>();

    let selected = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Select agents to configure (Esc: back)")
        .items(&labels)
        .defaults(&default_flags)
        .interact_opt()?;

    Ok(selected.map(|indexes| {
        indexes
            .into_iter()
            .map(|index| all[index])
            .collect::<Vec<_>>()
    }))
}

fn prompt_workflows_selection(defaults: &[WorkflowArg]) -> Result<Option<Vec<WorkflowArg>>> {
    let all = [
        WorkflowArg::Instrument,
        WorkflowArg::Observe,
        WorkflowArg::Annotate,
        WorkflowArg::Evaluate,
        WorkflowArg::Deploy,
    ];
    let default_set: BTreeSet<WorkflowArg> = defaults.iter().copied().collect();
    let labels = all
        .iter()
        .map(|workflow| workflow.as_str())
        .collect::<Vec<_>>();
    let default_flags = all
        .iter()
        .map(|workflow| default_set.contains(workflow))
        .collect::<Vec<_>>();

    let selected = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt(
            "Select the workflows you are interested in (will prefetch docs for them) (Esc: back)",
        )
        .items(&labels)
        .defaults(&default_flags)
        .interact_opt()?;

    Ok(selected.map(|indexes| {
        indexes
            .into_iter()
            .map(|index| all[index])
            .collect::<Vec<_>>()
    }))
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

fn resolve_mcp_scope(args: &AgentsMcpSetupArgs) -> Result<InstallScope> {
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
    let idx = crate::ui::fuzzy_select("Select MCP setup scope", &choices)?;
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
    top_lines.push("Generated by `bt docs fetch`.".to_string());
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
) -> Result<AgentInstallResult> {
    let root = scope_root(scope, local_root, home)?;
    let skill_path = root.join(".claude/skills/braintrust/SKILL.md");
    let skill_content = render_braintrust_skill();
    write_text_file(&skill_path, &skill_content)?;

    Ok(AgentInstallResult {
        agent: Agent::Claude,
        status: InstallStatus::Installed,
        message: "installed skill".to_string(),
        paths: vec![skill_path.display().to_string()],
    })
}

fn install_codex(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
) -> Result<AgentInstallResult> {
    let root = scope_root(scope, local_root, home)?;
    let skill_path = root.join(".agents/skills/braintrust/SKILL.md");
    let skill_content = render_braintrust_skill();
    write_text_file(&skill_path, &skill_content)?;

    Ok(AgentInstallResult {
        agent: Agent::Codex,
        status: InstallStatus::Installed,
        message: "installed skill".to_string(),
        paths: vec![skill_path.display().to_string()],
    })
}

fn install_opencode(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
) -> Result<AgentInstallResult> {
    let root = scope_root(scope, local_root, home)?;
    let skill_path = root.join(".agents/skills/braintrust/SKILL.md");
    let skill_content = render_braintrust_skill();
    write_text_file(&skill_path, &skill_content)?;

    Ok(AgentInstallResult {
        agent: Agent::Opencode,
        status: InstallStatus::Installed,
        message: "installed skill".to_string(),
        paths: vec![skill_path.display().to_string()],
    })
}

fn install_cursor(
    scope: InstallScope,
    local_root: Option<&Path>,
    _home: &Path,
) -> Result<AgentInstallResult> {
    if matches!(scope, InstallScope::Global) {
        return Ok(AgentInstallResult {
            agent: Agent::Cursor,
            status: InstallStatus::Skipped,
            message: "warning: cursor currently supports only --local in bt setup skills"
                .to_string(),
            paths: Vec::new(),
        });
    }

    let root = scope_root(scope, local_root, _home)?;
    let rule_path = root.join(".cursor/rules/braintrust.mdc");
    let cursor_rule = render_cursor_rule();
    write_text_file(&rule_path, &cursor_rule)?;

    Ok(AgentInstallResult {
        agent: Agent::Cursor,
        status: InstallStatus::Installed,
        message: "installed rule".to_string(),
        paths: vec![rule_path.display().to_string()],
    })
}

fn install_mcp_for_agent(
    agent: Agent,
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
) -> Result<AgentInstallResult> {
    let path = match agent {
        Agent::Cursor => {
            if matches!(scope, InstallScope::Global) {
                return Ok(AgentInstallResult {
                    agent,
                    status: InstallStatus::Skipped,
                    message: "warning: cursor currently supports only --local in bt setup mcp"
                        .to_string(),
                    paths: Vec::new(),
                });
            }
            let root = scope_root(scope, local_root, home)?;
            root.join(".cursor/mcp.json")
        }
        Agent::Claude | Agent::Codex | Agent::Opencode => {
            let root = scope_root(scope, local_root, home)?;
            match scope {
                InstallScope::Local => root.join(".mcp.json"),
                InstallScope::Global => home.join(".mcp.json"),
            }
        }
    };

    merge_mcp_config(&path)?;

    Ok(AgentInstallResult {
        agent,
        status: InstallStatus::Installed,
        message: "installed MCP config".to_string(),
        paths: vec![path.display().to_string()],
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
        "---\ndescription: Braintrust CLI workflow\nalwaysApply: false\n---\n\n## Purpose\n\n{}\n\n## Key Workflows\n\n{}\n\n## bt CLI Reference (Inlined README)\n\n{}",
        SHARED_SKILL_BODY.trim(),
        SHARED_WORKFLOW_GUIDE.trim(),
        BT_README.trim()
    )
}

fn setup_docs_output_dir(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
) -> Result<PathBuf> {
    match scope {
        InstallScope::Local => {
            let root = scope_root(scope, local_root, home)?;
            Ok(root.join("skills").join("docs"))
        }
        InstallScope::Global => Ok(global_bt_config_dir(home).join("skills").join("docs")),
    }
}

fn global_bt_config_dir(home: &Path) -> PathBuf {
    #[cfg(windows)]
    {
        if let Some(app_data) = std::env::var_os("APPDATA") {
            return PathBuf::from(app_data).join("bt");
        }
        home.join(".config").join("bt")
    }

    #[cfg(not(windows))]
    {
        if let Some(xdg_config_home) = std::env::var_os("XDG_CONFIG_HOME") {
            return PathBuf::from(xdg_config_home).join("bt");
        }
        home.join(".config").join("bt")
    }
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
    warnings: &[String],
    notes: &[String],
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

    if !notes.is_empty() {
        println!("Notes:");
        for note in notes {
            println!("  - {note}");
        }
    }

    if !warnings.is_empty() {
        println!("Warnings:");
        for warning in warnings {
            println!("  - {warning}");
        }
    }
}

fn print_mcp_human_report(
    scope: InstallScope,
    selected_agents: &[Agent],
    detected: &[DetectionSignal],
    results: &[AgentInstallResult],
    warnings: &[String],
) {
    println!("Configuring MCP for Braintrust");
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

    if !warnings.is_empty() {
        println!("Warnings:");
        for warning in warnings {
            println!("  - {warning}");
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

    #[test]
    fn resolve_setup_selection_honors_no_fetch_docs() {
        let args = AgentsSetupArgs {
            agents: vec![AgentArg::Codex],
            local: false,
            global: true,
            workflows: vec![WorkflowArg::Evaluate],
            yes: true,
            no_fetch_docs: true,
            refresh_docs: false,
        };
        let home = std::env::temp_dir();
        let selection = resolve_setup_selection(&args, &home).expect("resolve setup selection");
        assert!(selection.selected_workflows.is_empty());
    }

    #[test]
    fn resolve_workflow_selection_resolves_explicit_values() {
        let selected =
            resolve_workflow_selection(&[WorkflowArg::Evaluate, WorkflowArg::Instrument]);
        assert_eq!(
            selected,
            vec![WorkflowArg::Instrument, WorkflowArg::Evaluate]
        );
    }

    #[test]
    fn resolve_doctor_scope_respects_global_flag() {
        let args = AgentsDoctorArgs {
            local: false,
            global: true,
        };
        let (scope, root) = resolve_doctor_scope(&args).expect("resolve doctor scope");
        assert!(matches!(scope, InstallScope::Global));
        assert!(root.is_none());
    }

    #[test]
    fn doctor_agent_status_marks_cursor_global_as_local_only() {
        let home = std::env::temp_dir();
        let status = doctor_agent_status(Agent::Cursor, InstallScope::Global, None, &home, &[]);
        assert!(!status.configured);
        assert!(status.config_path.is_none());
        assert!(status.notes.iter().any(|note| note.contains("local-only")));
    }

    #[test]
    fn resolve_mcp_scope_respects_global_flag() {
        let args = AgentsMcpSetupArgs {
            agents: vec![],
            local: false,
            global: true,
            yes: false,
        };
        let scope = resolve_mcp_scope(&args).expect("resolve mcp scope");
        assert!(matches!(scope, InstallScope::Global));
    }

    #[test]
    fn install_mcp_for_agent_writes_local_mcp_file() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-agents-mcp-local-{unique}"));
        fs::create_dir_all(&root).expect("create temp root");
        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");

        let result = install_mcp_for_agent(Agent::Codex, InstallScope::Local, Some(&root), &home)
            .expect("install local mcp");
        assert!(matches!(result.status, InstallStatus::Installed));

        let mcp_path = root.join(".mcp.json");
        assert!(mcp_path.exists());
        let parsed: Value =
            serde_json::from_str(&fs::read_to_string(&mcp_path).expect("read mcp")).expect("json");
        let servers = parsed
            .get("mcpServers")
            .and_then(|v| v.as_object())
            .expect("servers object");
        assert!(servers.contains_key("braintrust"));
    }
}
