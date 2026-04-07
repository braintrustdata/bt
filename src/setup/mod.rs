use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use dialoguer::console::style;
use dialoguer::{theme::ColorfulTheme, Confirm, FuzzySelect, MultiSelect, Select};
use serde::Serialize;
use serde_json::{Map, Value};
use tokio::process::Command;

use crate::args::BaseArgs;
use crate::auth;
use crate::auth::LoginContext;
use crate::config;
use crate::http::ApiClient;
use crate::ui::{self, with_spinner};

mod agent_stream;
mod docs;
mod sdk_install_docs;

pub use docs::DocsArgs;

const INSTRUMENT_TASK_TEMPLATE: &str = include_str!("../../skills/sdk-install/instrument-task.md");
const SHARED_SKILL_BODY: &str = include_str!("../../skills/shared/braintrust-cli-body.md");
const SHARED_WORKFLOW_GUIDE: &str = include_str!("../../skills/shared/workflows.md");
const SHARED_SKILL_TEMPLATE: &str = include_str!("../../skills/shared/skill_template.md");
const SKILL_FRONTMATTER: &str = include_str!("../../skills/shared/skill_frontmatter.md");
const BT_README: &str = include_str!("../../README.md");
const README_AGENT_SECTION_MARKERS: &[&str] = &[
    "bt eval", "bt sql", "bt view", "bt auth", "bt setup", "bt docs",
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
#[command(after_help = "\
Examples:
  bt setup --agent cursor --workflow observe
  bt setup skills --agent codex --global
  bt setup mcp --agent codex
")]
pub struct SetupArgs {
    #[command(subcommand)]
    command: Option<SetupSubcommand>,

    /// Set up coding-agent skills (skips interactive selection in wizard)
    #[arg(long)]
    skills: bool,

    /// Set up MCP server (skips interactive selection in wizard)
    #[arg(long)]
    mcp: bool,

    /// Run instrumentation agent (skips interactive prompt in wizard)
    #[arg(long)]
    instrument: bool,

    /// Skip skills and MCP setup (skips interactive selection in wizard)
    #[arg(long, conflicts_with_all = ["skills", "mcp"])]
    no_mcp_skill: bool,

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

    /// Grant the agent full permissions and run it in the background without prompting.
    /// Equivalent to choosing "Background" with all tool restrictions lifted.
    #[arg(long)]
    yolo: bool,
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

    /// Suppress streaming agent output; show a spinner and print results at the end
    #[arg(long, short = 'q')]
    quiet: bool,

    /// Language(s) to instrument (repeatable; case-insensitive).
    /// When provided, the agent skips language auto-detection and instruments
    /// the specified language(s) directly.
    /// Accepted values: python, typescript, javascript, go, csharp, c#, java, ruby
    #[arg(long = "language", value_enum, ignore_case = true)]
    languages: Vec<LanguageArg>,

    /// Run the agent in interactive mode: inherits the terminal so the user can
    /// approve/deny tool uses directly. Conflicts with --quiet and --yolo.
    #[arg(long, short = 'i', conflicts_with_all = ["quiet", "yolo"])]
    interactive: bool,

    /// Grant the agent full permissions and run it in the background without prompting.
    /// Skips the run-mode selection question. Conflicts with --interactive.
    #[arg(long, conflicts_with = "interactive")]
    yolo: bool,
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

    fn display_name(self) -> &'static str {
        match self {
            Agent::Claude => "Claude",
            Agent::Codex => "Codex",
            Agent::Cursor => "Cursor",
            Agent::Opencode => "Opencode",
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
            run_instrument_setup(base, instrument, false).await
        }
        Some(SetupSubcommand::Mcp(mcp)) => run_mcp_setup(base, mcp),
        Some(SetupSubcommand::Doctor(doctor)) => run_doctor(base, doctor),
        None => {
            if should_prompt_setup_action(&base, &args.agents) {
                let wizard_flags = WizardFlags {
                    yolo: args.agents.yolo,
                    skills: args.skills,
                    mcp: args.mcp,
                    local: args.agents.local,
                    global: args.agents.global,
                    instrument: args.instrument,
                    agents: args.agents.agents,
                    no_mcp_skill: args.no_mcp_skill,
                    workflows: args.agents.workflows,
                };
                run_setup_wizard(base, wizard_flags).await
            } else {
                run_setup(base, args.agents).await
            }
        }
    }
}

pub use docs::run_docs_top;

struct WizardFlags {
    yolo: bool,
    skills: bool,
    mcp: bool,
    local: bool,
    global: bool,
    instrument: bool,
    agents: Vec<AgentArg>,
    no_mcp_skill: bool,
    workflows: Vec<WorkflowArg>,
}

async fn run_setup_wizard(mut base: BaseArgs, flags: WizardFlags) -> Result<()> {
    let WizardFlags {
        yolo,
        skills: flag_skills,
        mcp: flag_mcp,
        local: flag_local,
        global: flag_global,
        instrument: flag_instrument,
        agents: flag_agents,
        no_mcp_skill: flag_no_mcp_skill,
        workflows: flag_workflows,
    } = flags;
    let mut had_failures = false;
    let quiet = base.quiet;

    // ── Step 1: Auth ──
    if !quiet {
        print_wizard_step(1, "Auth");
    }
    let project_flag = base.project.clone();
    let login_ctx = ensure_auth(&mut base).await?;
    let client = ApiClient::new(&login_ctx)?;
    let org = client.org_name().to_string();
    if !quiet {
        eprintln!("   {} Using org '{}'", style("✓").green(), org);
    }

    // ── Step 2: Project ──
    if !quiet {
        print_wizard_step(2, "Project");
    }
    let project = select_project_with_skip(&client, project_flag.as_deref(), quiet).await?;
    if !quiet {
        if let Some(ref project) = project {
            if find_git_root().is_some() && maybe_init(&org, project)? {
                eprintln!(
                    "   {} Linked to {}/{}",
                    style("✓").green(),
                    org,
                    project.name
                );
            }
        } else {
            eprintln!("   {}", style("Skipped").dim());
        }
    } else if let Some(ref project) = project {
        if find_git_root().is_some() {
            let _ = maybe_init(&org, project)?;
        }
    }

    // ── Step 3: Agent tools (skills + MCP) ──
    if !quiet {
        print_wizard_step(3, "Agents");
    }
    let mut multiselect_hint_shown = false;
    let (wants_skills, wants_mcp) = if flag_no_mcp_skill {
        if !quiet {
            eprintln!(
                "{} What would you like to set up? · {}",
                style("✔").green(),
                style("(none)").dim()
            );
        }
        (false, false)
    } else if flag_skills || flag_mcp {
        if !quiet {
            let chosen: Vec<&str> = [("Skills", flag_skills), ("MCP", flag_mcp)]
                .iter()
                .filter(|(_, v)| *v)
                .map(|(s, _)| *s)
                .collect();
            let chosen_styled: Vec<String> = chosen
                .iter()
                .map(|s| style(s).green().to_string())
                .collect();
            eprintln!(
                "{} What would you like to set up? · {}",
                style("✔").green(),
                chosen_styled.join(", ")
            );
        }
        (flag_skills, flag_mcp)
    } else {
        if !quiet {
            eprintln!(
                "   {}",
                style("(Un)select option with Space, confirm selection with Enter.").dim()
            );
            multiselect_hint_shown = true;
        }
        let choices = ["Skills", "MCP"];
        let defaults = [true, true];
        let selected = MultiSelect::with_theme(&ColorfulTheme::default())
            .with_prompt("What would you like to set up?")
            .items(&choices)
            .defaults(&defaults)
            .interact()?;
        (selected.contains(&0), selected.contains(&1))
    };

    let setup_context = if wants_skills || wants_mcp {
        let scope = if flag_local {
            if !quiet {
                eprintln!(
                    "{} Select install scope · {}",
                    style("✔").green(),
                    style("local (current git repo)").green()
                );
            }
            InstallScope::Local
        } else if flag_global {
            if !quiet {
                eprintln!(
                    "{} Select install scope · {}",
                    style("✔").green(),
                    style("global (user-wide)").green()
                );
            }
            InstallScope::Global
        } else {
            prompt_scope_selection("Select install scope")?
                .ok_or_else(|| anyhow!("setup cancelled"))?
        };
        let home = home_dir().ok_or_else(|| anyhow!("failed to resolve HOME/USERPROFILE"))?;
        let local_root = resolve_local_root_for_scope(scope)?;
        let detected = detect_agents(local_root.as_deref(), &home);
        let agents = resolve_selected_agents(&flag_agents, &detected);
        if !quiet && !flag_agents.is_empty() {
            let agent_names: Vec<String> = agents
                .iter()
                .map(|a| style(a.as_str()).green().to_string())
                .collect();
            eprintln!(
                "{} Select agents to configure · {}",
                style("✔").green(),
                agent_names.join(", ")
            );
        }
        Some((scope, agents, home))
    } else {
        None
    };

    if wants_skills {
        if !quiet {
            eprintln!("   {}", style("Skills:").bold());
        }
        if let Some((scope, ref agents, _)) = setup_context {
            let agent_args: Vec<AgentArg> =
                agents.iter().map(|a| map_agent_to_agent_arg(*a)).collect();
            let args = AgentsSetupArgs {
                agents: agent_args,
                local: matches!(scope, InstallScope::Local),
                global: matches!(scope, InstallScope::Global),
                workflows: Vec::new(),
                yes: false,
                no_fetch_docs: true,
                refresh_docs: false,
                workers: crate::sync::default_workers(),
                yolo: false,
            };
            let outcome = execute_skills_setup(&base, &args, true).await?;
            for r in &outcome.results {
                if !quiet {
                    print_wizard_agent_result(r);
                }
                if matches!(r.status, InstallStatus::Failed) {
                    had_failures = true;
                }
            }
        }
    }

    if wants_mcp {
        if !quiet {
            eprintln!("   {}", style("MCP:").bold());
        }
        if let Some((scope, ref agents, ref home)) = setup_context {
            let local_root = resolve_local_root_for_scope(scope)?;
            let outcome = execute_mcp_install(scope, local_root.as_deref(), home, agents);
            for r in &outcome.results {
                if !quiet {
                    print_wizard_agent_result(r);
                }
                if matches!(r.status, InstallStatus::Failed) {
                    had_failures = true;
                }
            }
            if outcome.installed_count == 0 && !agents.is_empty() {
                had_failures = true;
            }
        }
    }

    if !wants_skills && !wants_mcp && !quiet {
        eprintln!("   {}", style("Skipped").dim());
    }

    // ── Step 4: Instrument ──
    if !quiet {
        print_wizard_step(4, "Instrument");
    }
    if find_git_root().is_some() {
        let instrument = if flag_instrument {
            if !quiet {
                eprintln!(
                    "Run instrumentation agent to set up tracing in this repo? {}",
                    style("yes").green()
                );
            }
            true
        } else {
            Confirm::with_theme(&ColorfulTheme::default())
                .with_prompt("Run instrumentation agent to set up tracing in this repo?")
                .default(false)
                .interact()?
        };
        if instrument {
            let instrument_agent = match flag_agents.as_slice() {
                [single] => match single {
                    AgentArg::Claude => Some(InstrumentAgentArg::Claude),
                    AgentArg::Codex => Some(InstrumentAgentArg::Codex),
                    AgentArg::Cursor => Some(InstrumentAgentArg::Cursor),
                    AgentArg::Opencode => Some(InstrumentAgentArg::Opencode),
                    AgentArg::All => None,
                },
                _ => None,
            };
            run_instrument_setup(
                base,
                InstrumentSetupArgs {
                    agent: instrument_agent,
                    agent_cmd: None,
                    workflows: flag_workflows,
                    yes: false,
                    refresh_docs: false,
                    workers: crate::sync::default_workers(),
                    quiet: false,
                    languages: Vec::new(),
                    interactive: false,
                    yolo,
                },
                !multiselect_hint_shown,
            )
            .await?;
        } else if !quiet {
            eprintln!("   {}", style("Skipped").dim());
        }
    } else if !quiet {
        eprintln!("   {}", style("Skipped").dim());
    }

    // ── Done ──
    if !quiet {
        print_wizard_done(had_failures);
    }
    if had_failures {
        bail!("setup completed with failures");
    }
    Ok(())
}

async fn ensure_auth(base: &mut BaseArgs) -> Result<LoginContext> {
    if base.api_key.is_some() {
        return auth::login(base).await;
    }

    let profiles = auth::list_profiles()?;
    match profiles.len() {
        0 => {
            eprintln!("No auth profiles found. Let's set one up.\n");
            auth::login_interactive(base).await?;
            auth::login(base).await
        }
        1 => {
            let p = &profiles[0];
            base.profile = Some(p.name.clone());
            auth::login(base).await
        }
        _ => {
            let name = auth::select_profile_interactive(None)?
                .ok_or_else(|| anyhow!("no profile selected"))?;
            base.profile = Some(name);
            auth::login(base).await
        }
    }
}

async fn select_project_with_skip(
    client: &ApiClient,
    project_name: Option<&str>,
    quiet: bool,
) -> Result<Option<crate::projects::api::Project>> {
    if let Some(name) = project_name {
        let project = with_spinner(
            "Loading project...",
            crate::projects::api::get_project_by_name(client, name),
        )
        .await?;
        match project {
            Some(p) => {
                if !quiet {
                    eprintln!("{} Select project · {}", style("✔").green(), p.name);
                }
                return Ok(Some(p));
            }
            None => bail!(
                "project '{}' not found in org '{}'",
                name,
                client.org_name()
            ),
        }
    }

    let mut projects = with_spinner(
        "Loading projects...",
        crate::projects::api::list_projects(client),
    )
    .await?;

    if projects.is_empty() {
        bail!("no projects found in org '{}'", client.org_name());
    }

    projects.sort_by(|a, b| a.name.cmp(&b.name));
    let mut labels: Vec<String> = projects.iter().map(|p| p.name.clone()).collect();
    labels.push("Skip (not recommended)".to_string());

    let selection = ui::fuzzy_select("Select project", &labels, 0)?;

    if selection == labels.len() - 1 {
        Ok(None)
    } else {
        Ok(Some(projects[selection].clone()))
    }
}

/// Returns `true` if config was written or already matched, `false` if user declined.
fn maybe_init(org: &str, project: &crate::projects::api::Project) -> Result<bool> {
    let config_path = std::env::current_dir()?.join(".bt").join("config.json");

    if config_path.exists() {
        let mut existing = config::load_file(&config_path);
        let matches = existing.org.as_deref() == Some(org)
            && existing.project.as_deref() == Some(project.name.as_str());
        if matches && existing.project_id.as_deref() == Some(project.id.as_str()) {
            return Ok(true);
        }
        if matches {
            existing.org = Some(org.to_string());
            existing.project = Some(project.name.clone());
            existing.project_id = Some(project.id.clone());
            config::save_local(&existing, true)?;
            return Ok(true);
        }
        let update = Confirm::new()
            .with_prompt(format!("Update .bt/config.json to {org}/{}?", project.name))
            .default(true)
            .interact()?;
        if !update {
            return Ok(false);
        }
    }

    let cfg = config::Config {
        org: Some(org.to_string()),
        project: Some(project.name.clone()),
        project_id: Some(project.id.clone()),
        ..Default::default()
    };
    config::save_local(&cfg, true)?;
    Ok(true)
}

async fn run_setup(base: BaseArgs, args: AgentsSetupArgs) -> Result<()> {
    let outcome = execute_skills_setup(&base, &args, false).await?;
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
    quiet: bool,
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
    let show_progress = !base.json && !quiet && !base.quiet;

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

#[derive(Debug, Clone, Copy, ValueEnum)]
enum InstrumentAgentArg {
    Claude,
    Codex,
    Cursor,
    Opencode,
}

/// Languages supported by `--language`.  Variants map to canonical display
/// names used in the agent task prompt.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, ValueEnum)]
enum LanguageArg {
    Python,
    /// TypeScript / JavaScript
    #[value(name = "typescript", alias = "javascript")]
    TypeScript,
    Go,
    /// C# / csharp
    #[value(name = "csharp", alias = "c#")]
    CSharp,
    Java,
    Ruby,
}

impl LanguageArg {
    fn display_name(self) -> &'static str {
        match self {
            LanguageArg::Python => "Python",
            LanguageArg::TypeScript => "TypeScript",
            LanguageArg::Go => "Go",
            LanguageArg::CSharp => "C#",
            LanguageArg::Java => "Java",
            LanguageArg::Ruby => "Ruby",
        }
    }
}

fn should_prompt_setup_action(base: &BaseArgs, args: &AgentsSetupArgs) -> bool {
    if base.json || !ui::is_interactive() {
        return false;
    }
    !args.yes
        && !args.no_fetch_docs
        && !args.refresh_docs
        && args.workers == crate::sync::default_workers()
}

async fn run_instrument_setup(
    base: BaseArgs,
    args: InstrumentSetupArgs,
    print_hint: bool,
) -> Result<()> {
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

    if args.agent.is_none() && ui::is_interactive() && !args.yes {
        selected = prompt_instrument_agent(selected)?;
    } else if args.agent.is_some() && !base.quiet {
        eprintln!(
            "{} Select agent to instrument this repo · {}",
            style("✔").green(),
            style(selected.as_str()).green()
        );
    }

    let mut hint_pending = print_hint && !base.quiet;
    let selected_workflows = resolve_instrument_workflow_selection(&args, &mut hint_pending)?;

    let selected_languages: Vec<LanguageArg> = if !args.languages.is_empty() {
        args.languages.clone()
    } else if ui::is_interactive() && !args.yes {
        if hint_pending {
            eprintln!(
                "   {}",
                style("(Un)select option with Space, confirm selection with Enter.").dim()
            );
        }
        let detected_langs = detect_languages_from_dir(&std::env::current_dir()?);
        let Some(langs) = prompt_instrument_language_selection(&detected_langs)? else {
            bail!("instrument setup cancelled by user");
        };
        langs
    } else {
        Vec::new()
    };

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
            yolo: false,
        };
        let outcome = execute_skills_setup(&base, &setup_args, false).await?;
        detected = outcome.detected_agents;
        results.extend(outcome.results);
        warnings.extend(outcome.warnings);
        notes.extend(outcome.notes);
        if outcome.successful_count == 0 {
            bail!("failed to configure skills for instrumentation");
        }
    }

    // Determine run mode: interactive TUI vs background (autonomous).
    // --yolo:        background, full bypassPermissions (no restrictions)
    // --interactive: interactive TUI
    // --yes or non-interactive terminal: background, restricted to language package managers
    // Otherwise: ask the user.
    let (run_interactive, bypass_permissions) = if args.interactive {
        (true, false)
    } else if args.yolo {
        (false, true)
    } else if args.yes || !ui::is_interactive() {
        (false, false)
    } else {
        let pkg_mgrs = package_manager_cmds_for_languages(&selected_languages).join(", ");
        let background_label = format!(
            "Background (automatic) — runs autonomously; \
             allowed package managers: {pkg_mgrs}"
        );
        let choices = [
            background_label.as_str(),
            "Interactive TUI — agent opens in its terminal UI; \
             you review and approve each tool use",
        ];
        let selection = Select::with_theme(&ColorfulTheme::default())
            .with_prompt("How do you want to run the agent?")
            .items(&choices)
            .default(0)
            .interact_opt()?;
        let Some(index) = selection else {
            bail!("instrument setup cancelled by user");
        };
        let interactive = index == 1;
        (interactive, false)
    };

    let docs_output_dir = root.join(".bt").join("skills").join("docs");
    sdk_install_docs::write_sdk_install_docs(&docs_output_dir)?;

    let task_path = root
        .join(".bt")
        .join("skills")
        .join("AGENT_TASK.instrument.md");
    write_text_file(
        &task_path,
        &render_instrument_task(
            &docs_output_dir,
            &selected_workflows,
            &selected_languages,
            run_interactive,
        ),
    )?;

    notes.push(format!(
        "Instrumentation task prompt written to {}.",
        task_path.display()
    ));

    let invocation = resolve_instrument_invocation(
        selected,
        args.agent_cmd.as_deref(),
        &task_path,
        run_interactive,
        bypass_permissions,
        &selected_languages,
    )?;

    if run_interactive {
        eprintln!();
        eprintln!("Claude Code is opening in interactive mode.");
        eprintln!("The instrumentation task is pre-loaded. Press Enter to begin.");
        eprintln!("Task file: {}", task_path.display());
        eprintln!();
    }

    let show_output = !base.json && !args.quiet;
    let status = if args.quiet && !base.json {
        with_spinner(
            "Running agent instrumentation…",
            run_agent_invocation(&root, &invocation, false),
        )
        .await?
    } else {
        run_agent_invocation(&root, &invocation, show_output).await?
    };
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
        eprintln!();
        for result in &results {
            print_wizard_agent_result(result);
        }
        for warning in &warnings {
            eprintln!("   {} {}", style("!").dim(), style(warning).dim());
        }
        print_wizard_done(!status.success());
    }

    if !status.success() {
        let _ = fs::remove_file(&task_path);
        bail!("agent instrumentation command failed");
    }
    Ok(())
}

fn resolve_instrument_workflow_selection(
    args: &InstrumentSetupArgs,
    hint_pending: &mut bool,
) -> Result<Vec<WorkflowArg>> {
    if !args.workflows.is_empty() {
        let mut selected = resolve_workflow_selection(&args.workflows);
        if !selected.contains(&WorkflowArg::Instrument) {
            selected.push(WorkflowArg::Instrument);
            selected.sort();
            selected.dedup();
        }
        return Ok(selected);
    }

    if ui::is_interactive() && !args.yes {
        if *hint_pending {
            eprintln!(
                "   {}",
                style("(Un)select option with Space, confirm selection with Enter.").dim()
            );
            *hint_pending = false;
        }
        let Some(selected) = prompt_instrument_workflow_selection()? else {
            bail!("instrument setup cancelled by user");
        };
        return Ok(selected);
    }

    Ok(vec![WorkflowArg::Instrument])
}

fn prompt_instrument_workflow_selection() -> Result<Option<Vec<WorkflowArg>>> {
    let choices = ["observe", "annotate", "evaluate", "deploy"];
    let defaults = [true, false, true, false];
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
                1 => workflows.push(WorkflowArg::Annotate),
                2 => workflows.push(WorkflowArg::Evaluate),
                3 => workflows.push(WorkflowArg::Deploy),
                _ => {}
            }
        }
        workflows
    }))
}

fn detect_languages_from_dir(dir: &std::path::Path) -> Vec<LanguageArg> {
    // (indicator filename suffix, language)
    let indicators: &[(&str, LanguageArg)] = &[
        ("pyproject.toml", LanguageArg::Python),
        ("setup.py", LanguageArg::Python),
        ("requirements.txt", LanguageArg::Python),
        ("package.json", LanguageArg::TypeScript),
        ("tsconfig.json", LanguageArg::TypeScript),
        ("go.mod", LanguageArg::Go),
        ("pom.xml", LanguageArg::Java),
        ("build.gradle", LanguageArg::Java),
        ("build.gradle.kts", LanguageArg::Java),
        ("Gemfile", LanguageArg::Ruby),
    ];
    // Glob-style suffix indicators (checked by extension)
    let ext_indicators: &[(&str, LanguageArg)] = &[
        ("csproj", LanguageArg::CSharp),
        ("sln", LanguageArg::CSharp),
        ("gemspec", LanguageArg::Ruby),
    ];

    let scan = |dir: &std::path::Path, found: &mut std::collections::BTreeSet<LanguageArg>| {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_file() {
                    if let Some(name) = path.file_name().and_then(|n| n.to_str()) {
                        for &(indicator, lang) in indicators {
                            if name.eq_ignore_ascii_case(indicator) {
                                found.insert(lang);
                            }
                        }
                        if let Some(ext) = path.extension().and_then(|e| e.to_str()) {
                            for &(indicator_ext, lang) in ext_indicators {
                                if ext.eq_ignore_ascii_case(indicator_ext) {
                                    found.insert(lang);
                                }
                            }
                        }
                    }
                }
            }
        }
    };

    let mut found = std::collections::BTreeSet::new();

    // Scan the current directory first.
    scan(dir, &mut found);

    // Only recurse into immediate subdirectories if nothing was found at the top level.
    if found.is_empty() {
        if let Ok(entries) = std::fs::read_dir(dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    scan(&path, &mut found);
                }
            }
        }
    }

    found.into_iter().collect()
}

fn prompt_instrument_language_selection(
    detected: &[LanguageArg],
) -> Result<Option<Vec<LanguageArg>>> {
    // Index 0 = "All / auto-detect".  Indices 1-6 map to specific languages.
    let choices = [
        "All languages (auto-detect)",
        "Python",
        "TypeScript / JavaScript",
        "Go",
        "Java",
        "Ruby",
        "C#",
    ];
    // Map detected languages to their choice indices (1-based)
    let lang_to_idx = |lang: LanguageArg| match lang {
        LanguageArg::Python => 1usize,
        LanguageArg::TypeScript => 2,
        LanguageArg::Go => 3,
        LanguageArg::Java => 4,
        LanguageArg::Ruby => 5,
        LanguageArg::CSharp => 6,
    };
    let defaults = if detected.len() != 1 {
        // Zero or multiple detected → pre-select "All languages (auto-detect)"
        [true, false, false, false, false, false, false]
    } else {
        let mut d = [false; 7];
        d[lang_to_idx(detected[0])] = true;
        d
    };
    let selected = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Which language(s) to instrument?")
        .items(&choices)
        .defaults(&defaults)
        .interact_opt()?;
    Ok(selected.map(|indices| {
        if indices.is_empty() || indices.contains(&0) {
            return Vec::new(); // auto-detect
        }
        indices
            .iter()
            .filter_map(|&i| match i {
                1 => Some(LanguageArg::Python),
                2 => Some(LanguageArg::TypeScript),
                3 => Some(LanguageArg::Go),
                4 => Some(LanguageArg::Java),
                5 => Some(LanguageArg::Ruby),
                6 => Some(LanguageArg::CSharp),
                _ => None,
            })
            .collect()
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

#[allow(clippy::too_many_arguments)]
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

/// Returns the package-manager command names allowed in background (non-yolo) mode.
///
/// When `languages` is empty (auto-detect), every supported package manager is included.
/// Otherwise only the managers for the specified languages are returned.
fn package_manager_cmds_for_languages(languages: &[LanguageArg]) -> Vec<&'static str> {
    use std::collections::BTreeSet;
    let unique: BTreeSet<LanguageArg> = languages.iter().copied().collect();

    if unique.is_empty() {
        return vec![
            "uv", "pip", "pip3", "poetry", "pipenv", "npm", "npx", "yarn", "pnpm", "bun", "deno",
            "go", "gradle", "gradlew", "mvn", "mvnw", "gem", "bundle", "dotnet",
        ];
    }

    let mut v: Vec<&'static str> = Vec::new();
    for lang in &unique {
        match lang {
            LanguageArg::Python => v.extend(["uv", "pip", "pip3", "poetry", "pipenv"]),
            LanguageArg::TypeScript => v.extend(["npm", "npx", "yarn", "pnpm", "bun", "deno"]),
            LanguageArg::Go => v.extend(["go"]),
            LanguageArg::Java => v.extend(["gradle", "gradlew", "mvn", "mvnw"]),
            LanguageArg::Ruby => v.extend(["gem", "bundle"]),
            LanguageArg::CSharp => v.extend(["dotnet"]),
        }
    }
    v
}

/// Returns the `--allowedTools` string for Claude from a language list.
fn allowed_bash_tools_for_languages(languages: &[LanguageArg]) -> String {
    package_manager_cmds_for_languages(languages)
        .iter()
        .map(|c| format!("Bash({c}:*)"))
        .collect::<Vec<_>>()
        .join(" ")
}

enum InstrumentInvocation {
    Program {
        program: String,
        args: Vec<String>,
        stdin_file: Option<PathBuf>,
        /// Path to a file whose content is passed as the initial user prompt (positional arg).
        prompt_file_arg: Option<PathBuf>,
        /// Hardcoded initial user prompt string (alternative to prompt_file_arg).
        initial_prompt: Option<String>,
        stream_json: bool,
        interactive: bool,
    },
    Shell(String),
}

fn resolve_instrument_invocation(
    agent: Agent,
    agent_cmd: Option<&str>,
    task_path: &Path,
    interactive: bool,
    bypass_permissions: bool,
    languages: &[LanguageArg],
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
            initial_prompt: None,
            stream_json: false,
            interactive,
        },
        Agent::Claude => {
            if interactive {
                // In interactive mode the full task goes into --append-system-prompt so
                // Claude already knows what to do.  A short initial user message is passed
                // as the positional arg so Claude immediately starts working — the user only
                // needs to press Enter once on a short, clear prompt rather than a wall of
                // raw task markdown.
                let task_content = std::fs::read_to_string(task_path)
                    .with_context(|| format!("failed to read task file {}", task_path.display()))?;
                InstrumentInvocation::Program {
                    program: "claude".to_string(),
                    args: vec![
                        "--append-system-prompt".to_string(),
                        task_content,
                        "--disallowedTools".to_string(),
                        "EnterPlanMode".to_string(),
                        "--name".to_string(),
                        "Braintrust: Instrument".to_string(),
                    ],
                    stdin_file: None,
                    prompt_file_arg: None,
                    initial_prompt: Some(
                        "Please begin the Braintrust instrumentation task.".to_string(),
                    ),
                    stream_json: false,
                    interactive: true,
                }
            } else {
                let mut claude_args = vec![
                    "-p".to_string(),
                    "--permission-mode".to_string(),
                    if bypass_permissions {
                        "bypassPermissions".to_string()
                    } else {
                        "acceptEdits".to_string()
                    },
                    "--verbose".to_string(),
                    "--output-format".to_string(),
                    "stream-json".to_string(),
                    "--include-partial-messages".to_string(),
                ];
                if !bypass_permissions {
                    let allowed = allowed_bash_tools_for_languages(languages);
                    claude_args.push("--allowedTools".to_string());
                    claude_args.push(allowed);
                }
                claude_args.push("--disallowedTools".to_string());
                claude_args.push("EnterPlanMode".to_string());
                InstrumentInvocation::Program {
                    program: "claude".to_string(),
                    args: claude_args,
                    stdin_file: Some(task_path.to_path_buf()),
                    prompt_file_arg: None,
                    initial_prompt: None,
                    stream_json: true,
                    interactive: false,
                }
            }
        }
        Agent::Opencode => InstrumentInvocation::Program {
            program: "opencode".to_string(),
            args: vec!["run".to_string()],
            stdin_file: None,
            prompt_file_arg: Some(task_path.to_path_buf()),
            initial_prompt: None,
            stream_json: false,
            interactive,
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
            initial_prompt: None,
            stream_json: true,
            interactive,
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
            initial_prompt,
            stream_json,
            interactive,
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
            if let Some(prompt) = initial_prompt {
                command.arg(prompt);
            }
            if let Some(path) = stdin_file {
                let file = fs::File::open(path).with_context(|| {
                    format!("failed to open task prompt file {}", path.display())
                })?;
                command.stdin(Stdio::from(file));
            }

            if *interactive {
                // Inherit all streams so the user can interact with the agent directly.
                return command
                    .status()
                    .await
                    .with_context(|| format!("failed to run agent command in {}", root.display()));
            }

            if !show_output {
                command.stdout(Stdio::null()).stderr(Stdio::null());
                return command
                    .status()
                    .await
                    .with_context(|| format!("failed to run agent command in {}", root.display()));
            }

            if *stream_json {
                command.stdout(Stdio::piped()).stderr(Stdio::piped());
                let child = command
                    .spawn()
                    .with_context(|| format!("failed to start {program}"))?;
                agent_stream::stream_agent_output(child, root).await
            } else {
                command
                    .status()
                    .await
                    .with_context(|| format!("failed to run agent command in {}", root.display()))
            }
        }
    }
}

fn render_instrument_task(
    docs_output_dir: &Path,
    workflows: &[WorkflowArg],
    languages: &[LanguageArg],
    interactive: bool,
) -> String {
    use std::collections::BTreeSet;
    let sdk_install_dir = docs_output_dir.join("sdk-install");

    // Deduplicate languages (TypeScript and JavaScript both map to the same variant).
    let unique_langs: BTreeSet<LanguageArg> = languages.iter().copied().collect();
    let language_context = if unique_langs.is_empty() {
        String::new()
    } else {
        let names: Vec<String> = unique_langs
            .iter()
            .map(|l| format!("**{}**", l.display_name()))
            .collect();
        let list = if names.len() == 1 {
            names[0].clone()
        } else {
            let (last, rest) = names.split_last().unwrap();
            format!("{} and {}", rest.join(", "), last)
        };
        format!(
            "### Language Override\n\n\
             Instrument {}. \
             Skip Step 2 (language auto-detection) and proceed directly to Step 3 \
             for the specified language(s).\n",
            list
        )
    };

    // When non-instrument workflows are selected the agent should use local
    // bt CLI skills rather than the MCP server.
    let workflow_context = if workflows
        .iter()
        .any(|w| !matches!(w, WorkflowArg::Instrument))
    {
        "## Agent Skills\n\n\
             Use the installed Braintrust agent skills from `.agents/skills/braintrust/`. \
             When verifying data in Braintrust, prefer local `bt` CLI commands over direct \
             API calls. Do not rely on the Braintrust MCP server for data queries.\n"
            .to_string()
    } else {
        String::new()
    };

    let run_mode_context = if interactive {
        "- **Interactive mode:** You can ask the user questions through the chat interface.\n"
    } else {
        "- **Non-interactive mode:** You cannot ask the user questions. \
         If a step requires user input (e.g., ambiguous language in a polyglot repo, \
         unknown run command), abort with a clear explanation of what is needed.\n"
    };

    INSTRUMENT_TASK_TEMPLATE
        .replace("{SDK_INSTALL_DIR}", &sdk_install_dir.display().to_string())
        .replace("{LANGUAGE_CONTEXT}", &language_context)
        .replace("{WORKFLOW_CONTEXT}", &workflow_context)
        .replace("{RUN_MODE_CONTEXT}", run_mode_context)
}

struct McpSetupOutcome {
    results: Vec<AgentInstallResult>,
    warnings: Vec<String>,
    installed_count: usize,
}

fn execute_mcp_install(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
    agents: &[Agent],
) -> McpSetupOutcome {
    let mut warnings = Vec::new();
    let mut results = Vec::new();

    for agent in agents.iter().copied() {
        let result = install_mcp_for_agent(agent, scope, local_root, home);
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

    McpSetupOutcome {
        results,
        warnings,
        installed_count,
    }
}

fn run_mcp_setup(base: BaseArgs, args: AgentsMcpSetupArgs) -> Result<()> {
    let home = home_dir().ok_or_else(|| anyhow!("failed to resolve HOME/USERPROFILE"))?;
    let selection = resolve_mcp_selection(&args, &home)?;
    let scope = selection.scope;
    let local_root = selection.local_root;
    let detected = selection.detected;
    let selected_agents = selection.selected_agents;

    let outcome = execute_mcp_install(scope, local_root.as_deref(), &home, &selected_agents);

    if base.json {
        let report = SetupJsonReport {
            scope: scope.as_str().to_string(),
            selected_agents,
            detected_agents: detected,
            results: outcome.results,
            warnings: outcome.warnings,
            notes: vec!["Configured MCP only (`bt setup mcp`).".to_string()],
        };
        println!(
            "{}",
            serde_json::to_string_pretty(&report)
                .context("failed to serialize MCP setup report")?
        );
    } else {
        print_mcp_human_report(scope, &selected_agents, &outcome.results, &outcome.warnings);
    }

    if outcome.installed_count == 0 {
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
    let interactive = ui::is_interactive() && !args.yes;
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
    let interactive = ui::is_interactive() && !args.yes;
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
    let idx = Select::with_theme(&ColorfulTheme::default())
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

    if !ui::is_interactive() {
        bail!("scope required in non-interactive mode: pass --local or --global");
    }

    let choices = ["local (current git repo)", "global (user-wide)"];
    let idx = crate::ui::fuzzy_select(prompt, &choices, 0)?;
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
    find_git_root_from(std::env::current_dir().ok()?)
}

fn find_git_root_from(mut current: PathBuf) -> Option<PathBuf> {
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

// ── Wizard output helpers ──

fn print_wizard_step(number: u8, label: &str) {
    eprintln!("\n{}. {}", style(number).bold(), style(label).bold());
}

fn print_wizard_agent_result(result: &AgentInstallResult) {
    let (indicator, status_text) = match result.status {
        InstallStatus::Installed => (style("✓").green(), "installed"),
        InstallStatus::Skipped => (style("—").dim(), "already configured"),
        InstallStatus::Failed => (style("✗").red(), "failed"),
    };
    eprintln!(
        "   {} {} — {}",
        indicator,
        result.agent.display_name(),
        status_text
    );
}

fn print_wizard_done(had_failures: bool) {
    if had_failures {
        eprintln!(
            "\n{} {}",
            style("!").dim(),
            style("Setup complete (with warnings)").bold()
        );
    } else {
        eprintln!(
            "\n{} {}",
            style("✓").green(),
            style("Setup complete").bold()
        );
    }
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

        let detected = find_git_root_from(nested)
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
            yolo: false,
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
            quiet: false,
            languages: Vec::new(),
            interactive: false,
            yolo: false,
        };

        let selected = resolve_instrument_workflow_selection(&args, &mut false)
            .expect("resolve instrument workflows");
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
            quiet: false,
            languages: Vec::new(),
            interactive: false,
            yolo: false,
        };

        let selected = resolve_instrument_workflow_selection(&args, &mut false)
            .expect("resolve instrument workflows");
        assert_eq!(selected, vec![WorkflowArg::Instrument]);
    }

    #[test]
    fn render_instrument_task_includes_local_cli_and_no_mcp_guidance() {
        let root = PathBuf::from("/tmp/repo");
        let task = render_instrument_task(
            &root,
            &[WorkflowArg::Instrument, WorkflowArg::Observe],
            &[],
            false,
        );
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
        let invocation =
            resolve_instrument_invocation(Agent::Codex, None, &task_path, false, false, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                stream_json,
                ..
            } => {
                assert_eq!(program, "codex");
                assert_eq!(args, vec!["exec".to_string(), "-".to_string()]);
                assert_eq!(stdin_file, Some(task_path));
                assert_eq!(prompt_file_arg, None);
                assert!(!stream_json);
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn claude_instrument_invocation_uses_print_with_bypass_permissions() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation =
            resolve_instrument_invocation(Agent::Claude, None, &task_path, false, true, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                stream_json,
                ..
            } => {
                assert_eq!(program, "claude");
                assert_eq!(
                    args,
                    vec![
                        "-p".to_string(),
                        "--permission-mode".to_string(),
                        "bypassPermissions".to_string(),
                        "--verbose".to_string(),
                        "--output-format".to_string(),
                        "stream-json".to_string(),
                        "--include-partial-messages".to_string(),
                        "--disallowedTools".to_string(),
                        "EnterPlanMode".to_string(),
                    ]
                );
                assert_eq!(stdin_file, Some(task_path));
                assert_eq!(prompt_file_arg, None);
                assert!(stream_json);
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn claude_background_invocation_uses_accept_edits_with_language_scoped_tools() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        // Python only → only Python package managers should be allowed.
        let invocation = resolve_instrument_invocation(
            Agent::Claude,
            None,
            &task_path,
            false,
            false,
            &[LanguageArg::Python],
        )
        .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program { program, args, .. } => {
                assert_eq!(program, "claude");
                let pm_idx = args.iter().position(|a| a == "--permission-mode").unwrap();
                assert_eq!(args[pm_idx + 1], "acceptEdits");

                let at_idx = args.iter().position(|a| a == "--allowedTools").unwrap();
                let allowed = &args[at_idx + 1];
                // Python managers present
                assert!(allowed.contains("Bash(uv:*)"), "uv should be allowed");
                assert!(allowed.contains("Bash(pip:*)"), "pip should be allowed");
                // Non-Python managers absent
                assert!(
                    !allowed.contains("Bash(npm:*)"),
                    "npm must not appear for Python-only"
                );
                assert!(
                    !allowed.contains("Bash(go:*)"),
                    "go must not appear for Python-only"
                );
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn claude_background_invocation_with_no_language_allows_all_package_managers() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation =
            resolve_instrument_invocation(Agent::Claude, None, &task_path, false, false, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program { args, .. } => {
                let at_idx = args.iter().position(|a| a == "--allowedTools").unwrap();
                let allowed = &args[at_idx + 1];
                assert!(allowed.contains("Bash(uv:*)"));
                assert!(allowed.contains("Bash(npm:*)"));
                assert!(allowed.contains("Bash(go:*)"));
                assert!(allowed.contains("Bash(gradle:*)"));
                assert!(allowed.contains("Bash(gem:*)"));
                assert!(allowed.contains("Bash(dotnet:*)"));
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn claude_interactive_instrument_invocation_uses_system_prompt_no_print_flag() {
        let dir = tempfile::tempdir().expect("tempdir");
        let task_path = dir.path().join("AGENT_TASK.instrument.md");
        std::fs::write(&task_path, "## Task\nInstrument this repo.").expect("write task");

        let invocation =
            resolve_instrument_invocation(Agent::Claude, None, &task_path, true, false, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                initial_prompt,
                stream_json,
                interactive,
            } => {
                assert_eq!(program, "claude");
                assert!(
                    !args.contains(&"-p".to_string()),
                    "interactive mode must not pass -p"
                );
                assert!(
                    args.contains(&"--append-system-prompt".to_string()),
                    "task should be in system prompt"
                );
                assert!(args.contains(&"--disallowedTools".to_string()));
                assert!(args.contains(&"--name".to_string()));
                assert_eq!(stdin_file, None);
                assert_eq!(
                    prompt_file_arg, None,
                    "task is in system prompt, not prompt_file_arg"
                );
                assert!(
                    initial_prompt.is_some(),
                    "short initial message must be set to trigger Claude"
                );
                assert!(!stream_json);
                assert!(interactive);
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn opencode_instrument_invocation_uses_run_with_prompt_arg() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation =
            resolve_instrument_invocation(Agent::Opencode, None, &task_path, false, false, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                stream_json,
                ..
            } => {
                assert_eq!(program, "opencode");
                assert_eq!(args, vec!["run".to_string()]);
                assert_eq!(stdin_file, None);
                assert_eq!(prompt_file_arg, Some(task_path));
                assert!(!stream_json);
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn cursor_instrument_invocation_uses_print_with_prompt_arg() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation =
            resolve_instrument_invocation(Agent::Cursor, None, &task_path, false, false, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                stream_json,
                ..
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
                assert!(stream_json);
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
    fn rendered_skill_frontmatter_name_matches_braintrust_directory() {
        let content = render_braintrust_skill();
        assert!(content.starts_with("---\nname: braintrust\n"));
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
