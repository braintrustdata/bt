use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use dialoguer::{theme::ColorfulTheme, FuzzySelect, MultiSelect};
use serde::Serialize;
use serde_json::{Map, Value};
use tokio::process::Command;

use crate::args::BaseArgs;
use crate::ui::with_spinner;

mod docs;

pub use docs::DocsArgs;

const SHARED_SKILL_BODY: &str = include_str!("../../skills/shared/braintrust-cli-body.md");
const SHARED_WORKFLOW_GUIDE: &str = include_str!("../../skills/shared/workflows.md");
const SHARED_SKILL_TEMPLATE: &str = include_str!("../../skills/shared/skill_template.md");
const SKILL_FRONTMATTER: &str = include_str!("../../skills/shared/skill_frontmatter.md");
const BT_README: &str = include_str!("../../README.md");
const README_AGENT_SECTION_MARKERS: &[&str] = &[
    "bt eval", "bt sql", "bt view", "bt login", "bt setup", "bt docs",
];
const ALL_AGENTS: [Agent; 4] = [Agent::Claude, Agent::Codex, Agent::Cursor, Agent::Opencode];
const ALL_WORKFLOWS: [WorkflowArg; 5] = [
    WorkflowArg::Instrument,
    WorkflowArg::Observe,
    WorkflowArg::Annotate,
    WorkflowArg::Evaluate,
    WorkflowArg::Deploy,
];

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
    /// Download instrumentation docs and run a coding agent to instrument this repo
    Instrument(InstrumentSetupArgs),
    /// Configure MCP server settings for coding agents
    Mcp(AgentsMcpSetupArgs),
    /// Diagnose coding-agent setup for Braintrust
    Doctor(AgentsDoctorArgs),
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

    /// Number of concurrent workers for docs prefetch/download.
    #[arg(long, default_value_t = crate::sync::default_workers())]
    workers: usize,
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
struct InstrumentSetupArgs {
    /// Agent to run for instrumentation
    #[arg(long = "agent", value_enum)]
    agent: Option<InstrumentAgentArg>,

    /// Command to run the selected agent (overrides built-in defaults)
    #[arg(long)]
    agent_cmd: Option<String>,

    /// Workflow docs to prefetch alongside instrument (repeatable; always includes instrument)
    #[arg(long = "workflow", value_enum)]
    workflows: Vec<WorkflowArg>,

    /// Skip confirmation prompts and use defaults
    #[arg(long, short = 'y')]
    yes: bool,

    /// Refresh prefetched docs by clearing existing output before download
    #[arg(long)]
    refresh_docs: bool,

    /// Number of concurrent workers for docs prefetch/download.
    #[arg(long, default_value_t = crate::sync::default_workers())]
    workers: usize,
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

#[derive(Debug, Clone, Copy)]
enum YesScopeDefault {
    Global,
    LocalIfGit,
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

struct SkillsSetupOutcome {
    scope: InstallScope,
    selected_agents: Vec<Agent>,
    detected_agents: Vec<DetectionSignal>,
    results: Vec<AgentInstallResult>,
    warnings: Vec<String>,
    notes: Vec<String>,
    successful_count: usize,
}

#[derive(Debug, Clone)]
struct SkillsAliasResult {
    changed: bool,
    path: PathBuf,
}

pub async fn run_setup_top(base: BaseArgs, args: SetupArgs) -> Result<()> {
    match args.command {
        Some(SetupSubcommand::Skills(setup)) => run_setup(base, setup).await,
        Some(SetupSubcommand::Instrument(instrument)) => {
            run_instrument_setup(base, instrument).await
        }
        Some(SetupSubcommand::Mcp(mcp)) => run_mcp_setup(base, mcp),
        Some(SetupSubcommand::Doctor(doctor)) => run_doctor(base, doctor),
        None => {
            if should_prompt_setup_action(&base, &args.agents) {
                match prompt_setup_action()? {
                    Some(SetupAction::Instrument) => {
                        run_instrument_setup(
                            base,
                            InstrumentSetupArgs {
                                agent: None,
                                agent_cmd: None,
                                workflows: Vec::new(),
                                yes: false,
                                refresh_docs: false,
                                workers: crate::sync::default_workers(),
                            },
                        )
                        .await
                    }
                    Some(SetupAction::Skills) => run_setup(base, args.agents).await,
                    Some(SetupAction::Mcp) => run_mcp_setup(
                        base,
                        AgentsMcpSetupArgs {
                            agents: Vec::new(),
                            local: false,
                            global: false,
                            yes: false,
                        },
                    ),
                    Some(SetupAction::Doctor) => run_doctor(
                        base,
                        AgentsDoctorArgs {
                            local: false,
                            global: false,
                        },
                    ),
                    None => bail!("setup cancelled by user"),
                }
            } else {
                run_setup(base, args.agents).await
            }
        }
    }
}

pub use docs::run_docs_top;

async fn run_setup(base: BaseArgs, args: AgentsSetupArgs) -> Result<()> {
    let outcome = execute_skills_setup(&base, &args).await?;
    if base.json {
        let report = SetupJsonReport {
            scope: outcome.scope.as_str().to_string(),
            selected_agents: outcome.selected_agents,
            detected_agents: outcome.detected_agents,
            results: outcome.results,
            warnings: outcome.warnings,
            notes: outcome.notes,
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&report).context("failed to serialize setup report")?
        );
    } else {
        print_human_report(
            false,
            outcome.scope,
            &outcome.selected_agents,
            &outcome.results,
            &outcome.warnings,
            &outcome.notes,
        );
    }

    if outcome.successful_count == 0 {
        bail!("no agents were configured successfully");
    }

    Ok(())
}

async fn execute_skills_setup(
    base: &BaseArgs,
    args: &AgentsSetupArgs,
) -> Result<SkillsSetupOutcome> {
    let home = home_dir().ok_or_else(|| anyhow!("failed to resolve HOME/USERPROFILE"))?;
    let selection = resolve_setup_selection(args, &home)?;
    let scope = selection.scope;
    let local_root = selection.local_root;
    let detected = selection.detected;
    let selected_agents = selection.selected_agents;
    let selected_workflows = selection.selected_workflows;
    let mut warnings = Vec::new();
    let mut notes = Vec::new();
    let mut results = Vec::new();
    let show_progress = !base.json;

    if show_progress {
        println!("Configuring coding agents for Braintrust");
    }

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

    let successful_count = results
        .iter()
        .filter(|r| !matches!(r.status, InstallStatus::Failed))
        .count();

    if successful_count == 0 {
        notes.push(
            "Skipped workflow docs prefetch (no agents configured successfully).".to_string(),
        );
    } else if args.no_fetch_docs {
        notes.push("Skipped workflow docs prefetch (`--no-fetch-docs`).".to_string());
    } else if selected_workflows.is_empty() {
        notes.push("Skipped workflow docs prefetch (no workflows selected).".to_string());
    } else {
        prefetch_workflow_docs(
            show_progress,
            scope,
            local_root.as_deref(),
            &home,
            &selected_workflows,
            args.refresh_docs,
            args.workers,
            &mut notes,
            &mut warnings,
        )
        .await?;
    }

    Ok(SkillsSetupOutcome {
        scope,
        selected_agents,
        detected_agents: detected,
        results,
        warnings,
        notes,
        successful_count,
    })
}

#[derive(Debug, Clone, Copy)]
enum SetupAction {
    Instrument,
    Skills,
    Mcp,
    Doctor,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum InstrumentAgentArg {
    Claude,
    Codex,
    Cursor,
    Opencode,
}

fn should_prompt_setup_action(base: &BaseArgs, args: &AgentsSetupArgs) -> bool {
    if base.json || !std::io::stdin().is_terminal() {
        return false;
    }
    args.agents.is_empty()
        && !args.local
        && !args.global
        && args.workflows.is_empty()
        && !args.yes
        && !args.no_fetch_docs
        && !args.refresh_docs
        && args.workers == crate::sync::default_workers()
}

fn prompt_setup_action() -> Result<Option<SetupAction>> {
    let choices = [
        "instrument (setup skills + use a coding agent to install Braintrust)",
        "skills (just setup skills)",
        "mcp (configure MCP)",
        "doctor (diagnose setup)",
    ];
    let idx = FuzzySelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Select setup action")
        .items(&choices)
        .default(0)
        .interact_opt()?;
    Ok(idx.map(|value| match value {
        0 => SetupAction::Instrument,
        1 => SetupAction::Skills,
        2 => SetupAction::Mcp,
        _ => SetupAction::Doctor,
    }))
}

async fn run_instrument_setup(base: BaseArgs, args: InstrumentSetupArgs) -> Result<()> {
    let home = home_dir().ok_or_else(|| anyhow!("failed to resolve HOME/USERPROFILE"))?;
    let root = find_git_root().ok_or_else(|| {
        anyhow!(
            "instrument setup requires running inside a git repository (could not find .git in parent chain)"
        )
    })?;
    let mut detected = detect_agents(Some(&root), &home);

    let mut selected = if let Some(agent_arg) = args.agent {
        map_instrument_agent_arg(agent_arg)
    } else {
        pick_agent_mode_target(&resolve_selected_agents(&[], &detected))
            .ok_or_else(|| anyhow!("no detected agents available for instrumentation"))?
    };

    if args.agent.is_none() && std::io::stdin().is_terminal() && !args.yes {
        selected = prompt_instrument_agent(selected)?;
    }

    let selected_workflows = resolve_instrument_workflow_selection(&args)?;
    let show_progress = !base.json;
    let mut warnings = Vec::new();
    let mut notes = Vec::new();
    let mut results = Vec::new();
    let skill_path = skill_config_path(selected, InstallScope::Local, Some(&root), &home)?;

    if skill_path.exists() {
        results.push(AgentInstallResult {
            agent: selected,
            status: InstallStatus::Skipped,
            message: "already configured".to_string(),
            paths: vec![skill_path.display().to_string()],
        });
        notes.push("Skipped skills setup (already configured).".to_string());
        prefetch_workflow_docs(
            show_progress,
            InstallScope::Local,
            Some(&root),
            &home,
            &selected_workflows,
            args.refresh_docs,
            args.workers,
            &mut notes,
            &mut warnings,
        )
        .await?;
    } else {
        let setup_args = AgentsSetupArgs {
            agents: vec![map_agent_to_agent_arg(selected)],
            local: true,
            global: false,
            workflows: selected_workflows.clone(),
            yes: true,
            no_fetch_docs: false,
            refresh_docs: args.refresh_docs,
            workers: args.workers,
        };
        let outcome = execute_skills_setup(&base, &setup_args).await?;
        detected = outcome.detected_agents;
        results.extend(outcome.results);
        warnings.extend(outcome.warnings);
        notes.extend(outcome.notes);
        if outcome.successful_count == 0 {
            bail!("failed to configure skills for instrumentation");
        }
    }

    let task_path = root
        .join(".bt")
        .join("skills")
        .join("AGENT_TASK.instrument.md");
    write_text_file(
        &task_path,
        &render_instrument_task(&root, &selected_workflows),
    )?;

    let invocation =
        resolve_instrument_invocation(selected, args.agent_cmd.as_deref(), &task_path)?;
    notes.push(format!(
        "Instrumentation task prompt written to {}.",
        task_path.display()
    ));

    let status = run_agent_invocation(&root, &invocation, !base.json).await?;
    if status.success() {
        results.push(AgentInstallResult {
            agent: selected,
            status: InstallStatus::Installed,
            message: "agent instrumentation command completed".to_string(),
            paths: vec![task_path.display().to_string()],
        });
    } else {
        results.push(AgentInstallResult {
            agent: selected,
            status: InstallStatus::Failed,
            message: format!("agent command exited with status {status}"),
            paths: vec![task_path.display().to_string()],
        });
    }

    if base.json {
        let report = SetupJsonReport {
            scope: InstallScope::Local.as_str().to_string(),
            selected_agents: vec![selected],
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
            true,
            InstallScope::Local,
            &[selected],
            &results,
            &warnings,
            &notes,
        );
    }

    if !status.success() {
        bail!("agent instrumentation command failed");
    }
    Ok(())
}

fn resolve_instrument_workflow_selection(args: &InstrumentSetupArgs) -> Result<Vec<WorkflowArg>> {
    if !args.workflows.is_empty() {
        let mut selected = resolve_workflow_selection(&args.workflows);
        if !selected.contains(&WorkflowArg::Instrument) {
            selected.push(WorkflowArg::Instrument);
            selected.sort();
            selected.dedup();
        }
        return Ok(selected);
    }

    if std::io::stdin().is_terminal() && !args.yes {
        let Some(selected) = prompt_instrument_workflow_selection()? else {
            bail!("instrument setup cancelled by user");
        };
        return Ok(selected);
    }

    Ok(vec![WorkflowArg::Instrument])
}

fn prompt_instrument_workflow_selection() -> Result<Option<Vec<WorkflowArg>>> {
    let choices = ["observe", "evaluate"];
    let defaults = [true, false];
    let selected = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Select additional workflow docs to prefetch (instrument is always included)")
        .items(&choices)
        .defaults(&defaults)
        .interact_opt()?;
    Ok(selected.map(|indexes| {
        let mut workflows = vec![WorkflowArg::Instrument];
        for index in indexes {
            match index {
                0 => workflows.push(WorkflowArg::Observe),
                1 => workflows.push(WorkflowArg::Evaluate),
                _ => {}
            }
        }
        workflows
    }))
}

fn map_agent_to_agent_arg(agent: Agent) -> AgentArg {
    match agent {
        Agent::Claude => AgentArg::Claude,
        Agent::Codex => AgentArg::Codex,
        Agent::Cursor => AgentArg::Cursor,
        Agent::Opencode => AgentArg::Opencode,
    }
}

fn skill_config_path(
    agent: Agent,
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
) -> Result<PathBuf> {
    let root = scope_root(scope, local_root, home)?;
    let path = match agent {
        Agent::Claude => root.join(".claude/skills/braintrust/SKILL.md"),
        Agent::Codex | Agent::Opencode => root.join(".agents/skills/braintrust/SKILL.md"),
        Agent::Cursor => root.join(".cursor/rules/braintrust.mdc"),
    };
    Ok(path)
}

async fn prefetch_workflow_docs(
    show_progress: bool,
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
    selected_workflows: &[WorkflowArg],
    refresh_docs: bool,
    workers: usize,
    notes: &mut Vec<String>,
    warnings: &mut Vec<String>,
) -> Result<()> {
    let docs_output_dir = setup_docs_output_dir(scope, local_root, home)?;
    if !refresh_docs && docs_cache_has_required_files(&docs_output_dir, selected_workflows) {
        notes.push(format!(
            "Skipped workflow docs prefetch (already present at {}; use --refresh-docs to refresh).",
            docs_output_dir.display()
        ));
        return Ok(());
    }

    let docs_args = docs::DocsFetchArgs {
        llms_url: docs::DEFAULT_DOCS_LLMS_URL.to_string(),
        output_dir: docs_output_dir.clone(),
        workflows: selected_workflows.to_vec(),
        dry_run: false,
        strict: false,
        refresh: refresh_docs,
        workers,
    };
    let fetch_result = if show_progress {
        with_spinner(
            "Prefetching workflow docs...",
            docs::fetch_docs_pages(&docs_args, selected_workflows),
        )
        .await
    } else {
        docs::fetch_docs_pages(&docs_args, selected_workflows).await
    };
    match fetch_result {
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
        Err(err) => warnings.push(format!("workflow docs prefetch failed: {err}")),
    }

    Ok(())
}

fn prompt_instrument_agent(default_agent: Agent) -> Result<Agent> {
    let choices = ALL_AGENTS
        .iter()
        .map(|agent| agent.as_str())
        .collect::<Vec<_>>();
    let default_index = ALL_AGENTS
        .iter()
        .position(|agent| *agent == default_agent)
        .unwrap_or(0);
    let selection = FuzzySelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Select agent to instrument this repo")
        .items(&choices)
        .default(default_index)
        .interact_opt()?;
    let Some(index) = selection else {
        bail!("instrument setup cancelled by user");
    };
    Ok(ALL_AGENTS[index])
}

enum InstrumentInvocation {
    Program {
        program: String,
        args: Vec<String>,
        stdin_file: Option<PathBuf>,
        prompt_file_arg: Option<PathBuf>,
    },
    Shell(String),
}

fn resolve_instrument_invocation(
    agent: Agent,
    agent_cmd: Option<&str>,
    task_path: &Path,
) -> Result<InstrumentInvocation> {
    if let Some(command) = agent_cmd {
        let trimmed = command.trim();
        if trimmed.is_empty() {
            bail!("`--agent-cmd` cannot be empty");
        }
        return Ok(InstrumentInvocation::Shell(trimmed.to_string()));
    }

    let invocation = match agent {
        Agent::Codex => InstrumentInvocation::Program {
            program: "codex".to_string(),
            args: vec!["exec".to_string(), "-".to_string()],
            stdin_file: Some(task_path.to_path_buf()),
            prompt_file_arg: None,
        },
        Agent::Claude => InstrumentInvocation::Program {
            program: "claude".to_string(),
            args: vec![
                "-p".to_string(),
                "--permission-mode".to_string(),
                "acceptEdits".to_string(),
                "--verbose".to_string(),
                "--output-format".to_string(),
                "stream-json".to_string(),
                "--include-partial-messages".to_string(),
            ],
            stdin_file: Some(task_path.to_path_buf()),
            prompt_file_arg: None,
        },
        Agent::Opencode => InstrumentInvocation::Program {
            program: "opencode".to_string(),
            args: vec!["run".to_string()],
            stdin_file: None,
            prompt_file_arg: Some(task_path.to_path_buf()),
        },
        Agent::Cursor => InstrumentInvocation::Program {
            program: "cursor-agent".to_string(),
            args: vec![
                "-p".to_string(),
                "-f".to_string(),
                "--output-format".to_string(),
                "stream-json".to_string(),
                "--stream-partial-output".to_string(),
            ],
            stdin_file: None,
            prompt_file_arg: Some(task_path.to_path_buf()),
        },
    };
    Ok(invocation)
}

async fn run_agent_invocation(
    root: &Path,
    invocation: &InstrumentInvocation,
    show_output: bool,
) -> Result<std::process::ExitStatus> {
    match invocation {
        InstrumentInvocation::Shell(command_text) => {
            let mut command = Command::new("bash");
            command.arg("-lc").arg(command_text);
            command.current_dir(root);
            if !show_output {
                command.stdout(Stdio::null()).stderr(Stdio::null());
            }
            command
                .status()
                .await
                .with_context(|| format!("failed to run agent command in {}", root.display()))
        }
        InstrumentInvocation::Program {
            program,
            args,
            stdin_file,
            prompt_file_arg,
        } => {
            let mut command = Command::new(program);
            command.args(args).current_dir(root);
            if let Some(path) = prompt_file_arg {
                let prompt = fs::read_to_string(path).with_context(|| {
                    format!("failed to read task prompt file {}", path.display())
                })?;
                let prompt = prompt.trim();
                if prompt.is_empty() {
                    bail!("task prompt file is empty: {}", path.display());
                }
                command.arg(prompt);
            }
            if let Some(path) = stdin_file {
                let file = fs::File::open(path).with_context(|| {
                    format!("failed to open task prompt file {}", path.display())
                })?;
                command.stdin(Stdio::from(file));
            }

            if !show_output {
                command.stdout(Stdio::null()).stderr(Stdio::null());
            }
            command
                .status()
                .await
                .with_context(|| format!("failed to run agent command in {}", root.display()))
        }
    }
}

fn render_instrument_task(repo_root: &Path, workflows: &[WorkflowArg]) -> String {
    let docs_output_dir = repo_root.join(".bt").join("skills").join("docs");
    let workflow_list = workflows
        .iter()
        .map(|workflow| workflow.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        r#"Instrument this repository with Braintrust tracing.

Requirements:
1. Review the Braintrust instrumentation docs in `{}`.
2. Focus on these workflow docs: {}.
3. Use the installed Braintrust agent skills in this repo and prefer local `bt` CLI commands to verify setup.
4. Do not rely on the Braintrust MCP server for this setup flow.
5. Add tracing/instrumentation to the application code.
6. Keep behavior intact; avoid unrelated refactors.
7. If tests exist, run the smallest relevant tests after instrumentation.

Output:
- Updated source files with Braintrust instrumentation.
- A short summary of what was instrumented and why."#,
        docs_output_dir.display(),
        workflow_list
    )
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
        print_mcp_human_report(scope, &selected_agents, &results, &warnings);
    }

    if installed_count == 0 {
        bail!("no MCP configurations were installed successfully");
    }

    Ok(())
}

fn resolve_setup_selection(args: &AgentsSetupArgs, home: &Path) -> Result<SetupSelection> {
    let mut scope = initial_scope(
        args.local,
        args.global,
        args.yes,
        YesScopeDefault::LocalIfGit,
    );
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
        None => resolve_scope_from_flags(
            args.local,
            args.global,
            args.yes,
            "Select install scope",
            YesScopeDefault::LocalIfGit,
        )?,
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
    let mut scope = initial_scope(args.local, args.global, args.yes, YesScopeDefault::Global);
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
        None => resolve_scope_from_flags(
            args.local,
            args.global,
            args.yes,
            "Select MCP setup scope",
            YesScopeDefault::Global,
        )?,
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

    let warnings = Vec::new();
    let agents = [Agent::Claude, Agent::Codex, Agent::Cursor, Agent::Opencode]
        .iter()
        .map(|agent| doctor_agent_status(*agent, scope, local_root.as_deref(), &home, &detected))
        .collect::<Vec<_>>();

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
            .map(|root| root.join(".cursor/skills/braintrust/SKILL.md"))
            .map(|p| p.display().to_string()),
        (Agent::Cursor, InstallScope::Global) => Some(
            home.join(".cursor/skills/braintrust/SKILL.md")
                .display()
                .to_string(),
        ),
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
        notes: Vec::new(),
    }
}

fn initial_scope(
    local: bool,
    global: bool,
    yes: bool,
    yes_scope_default: YesScopeDefault,
) -> Option<InstallScope> {
    if local {
        Some(InstallScope::Local)
    } else if global {
        Some(InstallScope::Global)
    } else if yes {
        Some(resolve_yes_scope(yes_scope_default))
    } else {
        None
    }
}

fn resolve_yes_scope(default: YesScopeDefault) -> InstallScope {
    match default {
        YesScopeDefault::Global => InstallScope::Global,
        YesScopeDefault::LocalIfGit => {
            if find_git_root().is_some() {
                InstallScope::Local
            } else {
                InstallScope::Global
            }
        }
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
    let default_set: BTreeSet<Agent> = defaults.iter().copied().collect();
    let labels = ALL_AGENTS
        .iter()
        .map(|agent| agent.as_str())
        .collect::<Vec<_>>();
    let default_flags = ALL_AGENTS
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
            .map(|index| ALL_AGENTS[index])
            .collect::<Vec<_>>()
    }))
}

fn prompt_workflows_selection(defaults: &[WorkflowArg]) -> Result<Option<Vec<WorkflowArg>>> {
    let default_set: BTreeSet<WorkflowArg> = defaults.iter().copied().collect();
    let labels = ALL_WORKFLOWS
        .iter()
        .map(|workflow| workflow.as_str())
        .collect::<Vec<_>>();
    let default_flags = ALL_WORKFLOWS
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
            .map(|index| ALL_WORKFLOWS[index])
            .collect::<Vec<_>>()
    }))
}

fn resolve_scope_from_flags(
    local: bool,
    global: bool,
    yes: bool,
    prompt: &str,
    yes_scope_default: YesScopeDefault,
) -> Result<InstallScope> {
    if local {
        return Ok(InstallScope::Local);
    }
    if global {
        return Ok(InstallScope::Global);
    }
    if yes {
        return Ok(resolve_yes_scope(yes_scope_default));
    }

    if !std::io::stdin().is_terminal() {
        bail!("scope required in non-interactive mode: pass --local or --global");
    }

    let choices = ["local (current git repo)", "global (user-wide)"];
    let idx = crate::ui::fuzzy_select(prompt, &choices)?;
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
            return ALL_AGENTS.to_vec();
        }
        return inferred.into_iter().collect();
    }

    if requested.contains(&AgentArg::All) {
        return ALL_AGENTS.to_vec();
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

fn map_instrument_agent_arg(agent: InstrumentAgentArg) -> Agent {
    match agent {
        InstrumentAgentArg::Claude => Agent::Claude,
        InstrumentAgentArg::Codex => Agent::Codex,
        InstrumentAgentArg::Cursor => Agent::Cursor,
        InstrumentAgentArg::Opencode => Agent::Opencode,
    }
}

fn pick_agent_mode_target(candidates: &[Agent]) -> Option<Agent> {
    if candidates.is_empty() {
        return None;
    }
    // Prefer Codex for instrumentation defaults when multiple agents are detected.
    let priority = [Agent::Codex, Agent::Claude, Agent::Cursor, Agent::Opencode];
    for preferred in priority {
        if candidates.contains(&preferred) {
            return Some(preferred);
        }
    }
    candidates.first().copied()
}

fn resolve_workflow_selection(requested: &[WorkflowArg]) -> Vec<WorkflowArg> {
    if requested.is_empty() || requested.contains(&WorkflowArg::All) {
        return ALL_WORKFLOWS.to_vec();
    }

    let mut out = BTreeSet::new();
    for workflow in requested {
        if !matches!(workflow, WorkflowArg::All) {
            out.insert(*workflow);
        }
    }
    out.into_iter().collect()
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
    if home.join(".cursor").exists() {
        add_signal(&mut by_agent, Agent::Cursor, "~/.cursor exists");
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
    let skill_content = render_braintrust_skill();
    let (skill_changed, skill_path) = install_canonical_skill(root, &skill_content)?;
    let alias = ensure_agent_skills_alias(root, ".claude", &skill_content)?;
    let changed = skill_changed || alias.changed;

    Ok(AgentInstallResult {
        agent: Agent::Claude,
        status: if changed {
            InstallStatus::Installed
        } else {
            InstallStatus::Skipped
        },
        message: if changed {
            "installed skill".to_string()
        } else {
            "already configured".to_string()
        },
        paths: vec![
            skill_path.display().to_string(),
            alias.path.display().to_string(),
        ],
    })
}

fn install_codex(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
) -> Result<AgentInstallResult> {
    let root = scope_root(scope, local_root, home)?;
    let skill_content = render_braintrust_skill();
    let (changed, skill_path) = install_canonical_skill(root, &skill_content)?;

    Ok(AgentInstallResult {
        agent: Agent::Codex,
        status: if changed {
            InstallStatus::Installed
        } else {
            InstallStatus::Skipped
        },
        message: if changed {
            "installed skill".to_string()
        } else {
            "already configured".to_string()
        },
        paths: vec![skill_path.display().to_string()],
    })
}

fn install_opencode(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
) -> Result<AgentInstallResult> {
    let root = scope_root(scope, local_root, home)?;
    let skill_content = render_braintrust_skill();
    let (changed, skill_path) = install_canonical_skill(root, &skill_content)?;

    Ok(AgentInstallResult {
        agent: Agent::Opencode,
        status: if changed {
            InstallStatus::Installed
        } else {
            InstallStatus::Skipped
        },
        message: if changed {
            "installed skill".to_string()
        } else {
            "already configured".to_string()
        },
        paths: vec![skill_path.display().to_string()],
    })
}

fn install_cursor(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
) -> Result<AgentInstallResult> {
    let root = scope_root(scope, local_root, home)?;
    let skill_content = render_braintrust_skill();
    let (skill_changed, skill_path) = install_canonical_skill(root, &skill_content)?;
    let alias = ensure_agent_skills_alias(root, ".cursor", &skill_content)?;
    let changed = skill_changed || alias.changed;

    Ok(AgentInstallResult {
        agent: Agent::Cursor,
        status: if changed {
            InstallStatus::Installed
        } else {
            InstallStatus::Skipped
        },
        message: if changed {
            "installed skill".to_string()
        } else {
            "already configured".to_string()
        },
        paths: vec![
            skill_path.display().to_string(),
            alias.path.display().to_string(),
        ],
    })
}

fn install_canonical_skill(root: &Path, skill_content: &str) -> Result<(bool, PathBuf)> {
    let skill_path = root.join(".agents/skills/braintrust/SKILL.md");
    let changed = write_text_file_if_changed(&skill_path, skill_content)?;
    Ok((changed, skill_path))
}

fn ensure_agent_skills_alias(
    root: &Path,
    agent_dir: &str,
    skill_content: &str,
) -> Result<SkillsAliasResult> {
    let canonical_skills_dir = root.join(".agents/skills");
    fs::create_dir_all(&canonical_skills_dir).with_context(|| {
        format!(
            "failed to create canonical skills directory {}",
            canonical_skills_dir.display()
        )
    })?;

    let alias_path = root.join(agent_dir).join("skills");
    if let Some(parent) = alias_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    if let Ok(metadata) = fs::symlink_metadata(&alias_path) {
        if metadata.file_type().is_symlink() {
            if symlink_points_to(&alias_path, &canonical_skills_dir) {
                return Ok(SkillsAliasResult {
                    changed: false,
                    path: alias_path,
                });
            }
            fs::remove_file(&alias_path)
                .with_context(|| format!("failed to replace symlink {}", alias_path.display()))?;
        } else {
            let mirror_skill_path = alias_path.join("braintrust/SKILL.md");
            let changed = write_text_file_if_changed(&mirror_skill_path, skill_content)?;
            return Ok(SkillsAliasResult {
                changed,
                path: mirror_skill_path,
            });
        }
    }

    match create_dir_symlink(&canonical_skills_dir, &alias_path) {
        Ok(()) => Ok(SkillsAliasResult {
            changed: true,
            path: alias_path,
        }),
        Err(_) => {
            let mirror_skill_path = alias_path.join("braintrust/SKILL.md");
            let changed = write_text_file_if_changed(&mirror_skill_path, skill_content)?;
            Ok(SkillsAliasResult {
                changed,
                path: mirror_skill_path,
            })
        }
    }
}

fn symlink_points_to(link_path: &Path, target: &Path) -> bool {
    let Ok(link_target) = fs::read_link(link_path) else {
        return false;
    };
    let resolved_link_target = if link_target.is_absolute() {
        link_target
    } else {
        link_path
            .parent()
            .unwrap_or_else(|| Path::new("."))
            .join(link_target)
    };
    match (resolved_link_target.canonicalize(), target.canonicalize()) {
        (Ok(a), Ok(b)) => a == b,
        _ => false,
    }
}

#[cfg(unix)]
fn create_dir_symlink(target: &Path, link: &Path) -> Result<()> {
    std::os::unix::fs::symlink(target, link).with_context(|| {
        format!(
            "failed to create symlink {} -> {}",
            link.display(),
            target.display()
        )
    })?;
    Ok(())
}

#[cfg(windows)]
fn create_dir_symlink(target: &Path, link: &Path) -> Result<()> {
    std::os::windows::fs::symlink_dir(target, link).with_context(|| {
        format!(
            "failed to create symlink {} -> {}",
            link.display(),
            target.display()
        )
    })?;
    Ok(())
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

pub(super) fn write_text_file(path: &Path, content: &str) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }
    fs::write(path, format!("{}\n", content.trim_end()))
        .with_context(|| format!("failed to write {}", path.display()))
}

fn write_text_file_if_changed(path: &Path, content: &str) -> Result<bool> {
    let normalized = format!("{}\n", content.trim_end());
    if let Ok(existing) = fs::read_to_string(path) {
        if existing == normalized {
            return Ok(false);
        }
    }
    write_text_file(path, content)?;
    Ok(true)
}

fn render_braintrust_skill() -> String {
    render_skill_document(SKILL_FRONTMATTER)
}

fn render_skill_document(frontmatter: &str) -> String {
    let readme_excerpt = render_agent_readme_excerpt();
    let body = SHARED_SKILL_TEMPLATE
        .replace("{{purpose}}", SHARED_SKILL_BODY.trim())
        .replace("{{workflows}}", SHARED_WORKFLOW_GUIDE.trim())
        .replace("{{readme_excerpt}}", readme_excerpt.trim());
    format!("{}\n\n{}", frontmatter.trim(), body.trim())
}

fn render_agent_readme_excerpt() -> String {
    let sections = extract_readme_sections(BT_README);
    if sections.is_empty() {
        return "- No command reference sections were found in README; use `bt --help` and subcommand help directly.".to_string();
    }
    sections.join("\n\n")
}

fn extract_readme_sections(readme: &str) -> Vec<String> {
    let lines = readme.lines().collect::<Vec<_>>();
    let mut sections = Vec::new();
    let mut idx = 0usize;

    while idx < lines.len() {
        let line = lines[idx];
        if !line.starts_with("## ") {
            idx += 1;
            continue;
        }

        let heading = line.trim_start_matches("## ").trim().to_ascii_lowercase();
        let include = README_AGENT_SECTION_MARKERS
            .iter()
            .any(|marker| heading.contains(marker));

        let start = idx;
        idx += 1;
        while idx < lines.len() && !lines[idx].starts_with("## ") {
            idx += 1;
        }

        if !include {
            continue;
        }

        let mut section = lines[start..idx].join("\n");
        if let Some(rest) = section.strip_prefix("## ") {
            section = format!("### {rest}");
        }
        let section = section.trim().to_string();
        if !section.is_empty() {
            sections.push(section);
        }
    }

    sections
}

fn setup_docs_output_dir(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
) -> Result<PathBuf> {
    match scope {
        InstallScope::Local => {
            let root = scope_root(scope, local_root, home)?;
            Ok(root.join(".bt").join("skills").join("docs"))
        }
        InstallScope::Global => Ok(global_bt_config_dir(home).join("skills").join("docs")),
    }
}

fn docs_cache_has_required_files(output_dir: &Path, workflows: &[WorkflowArg]) -> bool {
    if !output_dir.join("README.md").exists() {
        return false;
    }
    for workflow in workflows {
        if !output_dir
            .join(workflow.as_str())
            .join("_index.md")
            .exists()
        {
            return false;
        }
    }
    output_dir.join("reference").join("sql.md").exists()
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
    include_header: bool,
    scope: InstallScope,
    selected_agents: &[Agent],
    results: &[AgentInstallResult],
    warnings: &[String],
    notes: &[String],
) {
    if include_header {
        println!("Configuring coding agents for Braintrust");
    }
    println!("Scope: {}", scope.as_str());

    let selected = selected_agents
        .iter()
        .map(|a| a.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    println!("Selected agents: {selected}");

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
    fn extract_readme_sections_includes_only_agent_command_sections() {
        let readme = r#"
# Title

## Install
- install details

## `bt sql`
- sql details

## `bt view`
- view details

## Roadmap
- future
"#;

        let sections = extract_readme_sections(readme);
        let rendered = sections.join("\n\n");
        assert!(rendered.contains("### `bt sql`"));
        assert!(rendered.contains("### `bt view`"));
        assert!(!rendered.contains("Install"));
        assert!(!rendered.contains("Roadmap"));
    }

    #[test]
    fn render_agent_readme_excerpt_contains_sql_and_setup_sections() {
        let excerpt = render_agent_readme_excerpt();
        assert!(excerpt.contains("`bt sql`"));
        assert!(excerpt.contains("`bt setup`"));
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
            workers: crate::sync::default_workers(),
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
    fn resolve_instrument_workflows_forces_instrument() {
        let args = InstrumentSetupArgs {
            agent: Some(InstrumentAgentArg::Codex),
            agent_cmd: None,
            workflows: vec![WorkflowArg::Evaluate],
            yes: true,
            refresh_docs: false,
            workers: crate::sync::default_workers(),
        };

        let selected =
            resolve_instrument_workflow_selection(&args).expect("resolve instrument workflows");
        assert_eq!(
            selected,
            vec![WorkflowArg::Instrument, WorkflowArg::Evaluate]
        );
    }

    #[test]
    fn resolve_instrument_workflows_default_to_instrument() {
        let args = InstrumentSetupArgs {
            agent: Some(InstrumentAgentArg::Codex),
            agent_cmd: None,
            workflows: Vec::new(),
            yes: true,
            refresh_docs: false,
            workers: crate::sync::default_workers(),
        };

        let selected =
            resolve_instrument_workflow_selection(&args).expect("resolve instrument workflows");
        assert_eq!(selected, vec![WorkflowArg::Instrument]);
    }

    #[test]
    fn render_instrument_task_includes_local_cli_and_no_mcp_guidance() {
        let root = PathBuf::from("/tmp/repo");
        let task = render_instrument_task(&root, &[WorkflowArg::Instrument, WorkflowArg::Observe]);
        assert!(task.contains("Use the installed Braintrust agent skills"));
        assert!(task.contains("prefer local `bt` CLI commands"));
        assert!(task.contains("Do not rely on the Braintrust MCP server"));
    }

    #[test]
    fn pick_agent_mode_target_prefers_codex() {
        let selected = pick_agent_mode_target(&[Agent::Claude, Agent::Codex, Agent::Cursor]);
        assert_eq!(selected, Some(Agent::Codex));
    }

    #[test]
    fn codex_instrument_invocation_uses_exec_with_stdin_prompt() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation = resolve_instrument_invocation(Agent::Codex, None, &task_path)
            .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
            } => {
                assert_eq!(program, "codex");
                assert_eq!(args, vec!["exec".to_string(), "-".to_string()]);
                assert_eq!(stdin_file, Some(task_path));
                assert_eq!(prompt_file_arg, None);
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn claude_instrument_invocation_uses_print_with_stdin_prompt() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation = resolve_instrument_invocation(Agent::Claude, None, &task_path)
            .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
            } => {
                assert_eq!(program, "claude");
                assert_eq!(
                    args,
                    vec![
                        "-p".to_string(),
                        "--permission-mode".to_string(),
                        "acceptEdits".to_string(),
                        "--verbose".to_string(),
                        "--output-format".to_string(),
                        "stream-json".to_string(),
                        "--include-partial-messages".to_string(),
                    ]
                );
                assert_eq!(stdin_file, Some(task_path));
                assert_eq!(prompt_file_arg, None);
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn opencode_instrument_invocation_uses_run_with_prompt_arg() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation = resolve_instrument_invocation(Agent::Opencode, None, &task_path)
            .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
            } => {
                assert_eq!(program, "opencode");
                assert_eq!(args, vec!["run".to_string()]);
                assert_eq!(stdin_file, None);
                assert_eq!(prompt_file_arg, Some(task_path));
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn cursor_instrument_invocation_uses_print_with_prompt_arg() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation = resolve_instrument_invocation(Agent::Cursor, None, &task_path)
            .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
            } => {
                assert_eq!(program, "cursor-agent");
                assert_eq!(
                    args,
                    vec![
                        "-p".to_string(),
                        "-f".to_string(),
                        "--output-format".to_string(),
                        "stream-json".to_string(),
                        "--stream-partial-output".to_string(),
                    ]
                );
                assert_eq!(stdin_file, None);
                assert_eq!(prompt_file_arg, Some(task_path));
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
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
    fn doctor_agent_status_reports_cursor_global_skill_path() {
        let home = std::env::temp_dir();
        let status = doctor_agent_status(Agent::Cursor, InstallScope::Global, None, &home, &[]);
        assert!(!status.configured);
        assert!(status.config_path.is_some());
        assert!(status.notes.is_empty());
    }

    #[test]
    fn resolve_scope_from_flags_respects_global_flag() {
        let scope =
            resolve_scope_from_flags(false, true, false, "ignored", YesScopeDefault::LocalIfGit)
                .expect("resolve scope from flags");
        assert!(matches!(scope, InstallScope::Global));
    }

    #[test]
    fn resolve_scope_from_flags_yes_defaults_to_local_in_git_repo() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-setup-yes-local-{unique}"));
        let nested = root.join("a/b/c");

        fs::create_dir_all(&nested).expect("create nested");
        fs::write(root.join(".git"), "gitdir: /tmp/fake").expect("write git file");

        let old = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&nested).expect("cd nested");
        let scope =
            resolve_scope_from_flags(false, false, true, "ignored", YesScopeDefault::LocalIfGit)
                .expect("resolve scope");
        std::env::set_current_dir(old).expect("restore cwd");

        assert!(matches!(scope, InstallScope::Local));
    }

    #[test]
    fn resolve_scope_from_flags_yes_falls_back_to_global_outside_git_repo() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-setup-yes-global-{unique}"));
        fs::create_dir_all(&root).expect("create root");

        let old = std::env::current_dir().expect("cwd");
        std::env::set_current_dir(&root).expect("cd root");
        let scope =
            resolve_scope_from_flags(false, false, true, "ignored", YesScopeDefault::LocalIfGit)
                .expect("resolve scope");
        std::env::set_current_dir(old).expect("restore cwd");

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

    #[test]
    fn install_codex_is_idempotent_when_skill_is_unchanged() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-agents-codex-idempotent-{unique}"));
        fs::create_dir_all(&root).expect("create temp root");
        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");

        let first = install_codex(InstallScope::Local, Some(&root), &home).expect("first install");
        assert!(matches!(first.status, InstallStatus::Installed));

        let second =
            install_codex(InstallScope::Local, Some(&root), &home).expect("second install");
        assert!(matches!(second.status, InstallStatus::Skipped));
        assert!(second.message.contains("already configured"));
    }

    #[test]
    fn install_cursor_uses_canonical_agents_skill_path() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-agents-cursor-skill-{unique}"));
        fs::create_dir_all(&root).expect("create temp root");
        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");

        let result =
            install_cursor(InstallScope::Local, Some(&root), &home).expect("install cursor");
        assert!(matches!(result.status, InstallStatus::Installed));
        assert!(root.join(".agents/skills/braintrust/SKILL.md").exists());
        assert!(root.join(".cursor/skills").exists());
    }

    #[test]
    fn docs_cache_has_required_files_checks_workflows_and_sql_reference() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-agents-docs-cache-{unique}"));
        fs::create_dir_all(root.join("evaluate")).expect("create evaluate dir");
        fs::create_dir_all(root.join("reference")).expect("create reference dir");
        fs::write(root.join("README.md"), "# docs\n").expect("write readme");
        fs::write(root.join("evaluate").join("_index.md"), "# evaluate\n").expect("write index");
        fs::write(root.join("reference").join("sql.md"), "# sql\n").expect("write sql");

        assert!(docs_cache_has_required_files(
            &root,
            &[WorkflowArg::Evaluate]
        ));
    }
}
