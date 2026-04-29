use std::collections::{BTreeMap, BTreeSet};
use std::ffi::OsStr;
use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use dialoguer::console::style;
use dialoguer::{theme::ColorfulTheme, Confirm, FuzzySelect, MultiSelect, Select};
use serde::Serialize;
use serde_json::{Map, Value};
use tokio::process::Command;
use toml::Value as TomlValue;

use crate::args::{ArgValueSource, BaseArgs, DEFAULT_API_URL, DEFAULT_APP_URL};
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
const ALL_AGENTS: [Agent; 7] = [
    Agent::Claude,
    Agent::Codex,
    Agent::Copilot,
    Agent::Cursor,
    Agent::Gemini,
    Agent::Opencode,
    Agent::Qwen,
];
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

    /// Also install reusable coding-agent skills (persistent, opt-in)
    #[arg(long, conflicts_with = "no_skills")]
    skills: bool,

    /// Do not set up reusable coding-agent skills
    #[arg(long, visible_alias = "no-skill", conflicts_with = "skills")]
    no_skills: bool,

    /// Set up MCP server
    #[arg(long, conflicts_with = "no_mcp")]
    mcp: bool,

    /// Do not set up MCP server [default]
    #[arg(long, conflicts_with = "mcp")]
    no_mcp: bool,

    /// Run instrumentation agent [default]
    #[arg(long)]
    instrument: bool,

    /// Do not run instrumentation agent (skills and MCP are still configured)
    #[arg(
        long,
        conflicts_with = "instrument",
        conflicts_with = "tui",
        conflicts_with = "background",
        conflicts_with = "yolo"
    )]
    no_instrument: bool,

    /// Run the agent in interactive TUI mode [default]
    #[arg(long, conflicts_with = "background", conflicts_with = "no_instrument")]
    tui: bool,

    /// Run the agent in background (non-interactive) mode. Use --verbose to see the agent output
    #[arg(long, conflicts_with = "tui", conflicts_with = "no_instrument")]
    background: bool,

    /// Language(s) to instrument (repeatable; case-insensitive).
    /// When provided, the agent skips language auto-detection and instruments
    /// the specified language(s) directly.
    #[arg(
        long = "language",
        value_enum,
        ignore_case = true,
        conflicts_with = "no_instrument"
    )]
    languages: Vec<LanguageArg>,

    /// Run the interactive setup wizard, prompting for every choice not already
    /// specified as a flag.
    #[arg(long, short = 'i')]
    interactive: bool,

    /// Deprecated: use --no-skills --no-mcp instead
    #[arg(long, hide = true, conflicts_with = "skills", conflicts_with = "mcp")]
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
    /// Agent to configure
    #[arg(long, alias = "agents", value_enum)]
    agent: Option<AgentArg>,

    /// Configure the current git repo root
    #[arg(long, conflicts_with = "global")]
    local: bool,

    /// Configure user-wide state [default]
    #[arg(long)]
    global: bool,

    /// Workflow docs to prefetch (repeatable) [default: all]
    #[arg(long = "workflow", value_enum)]
    workflows: Vec<WorkflowArg>,

    /// Do not fetch workflow docs during setup
    #[arg(long, conflicts_with = "workflows")]
    no_workflow: bool,

    #[arg(skip)]
    yes: bool,

    /// Refresh prefetched docs by clearing existing output before download
    #[arg(long, conflicts_with = "no_workflow")]
    refresh_docs: bool,

    /// Number of concurrent workers for docs prefetch/download.
    #[arg(long, default_value_t = crate::sync::default_workers())]
    workers: usize,

    #[command(flatten)]
    permissions: InstrumentPermissionArgs,
}

#[derive(Debug, Clone, Args)]
struct InstrumentPermissionArgs {
    /// Grant the agent full permissions (bypass permission prompts)
    #[arg(long)]
    yolo: bool,
}

#[derive(Debug, Clone, Args)]
struct AgentsMcpSetupArgs {
    /// Agent to configure MCP for
    #[arg(long, alias = "agents", value_enum)]
    agent: Option<AgentArg>,

    /// Configure MCP in the current git repo root
    #[arg(long, conflicts_with = "global")]
    local: bool,

    /// Configure MCP in user-wide state [default]
    #[arg(long)]
    global: bool,

    #[arg(skip)]
    yes: bool,
}

#[derive(Debug, Clone, Args)]
struct InstrumentSetupArgs {
    /// Agent to run for instrumentation
    #[arg(long = "agent", alias = "agents", value_enum)]
    agent: Option<InstrumentAgentArg>,

    /// Command to run the selected agent (overrides built-in defaults)
    #[arg(long)]
    agent_cmd: Option<String>,

    /// Latest workflow docs to provide to the instrumentation agent (repeatable; always includes instrument) [default: all]
    #[arg(long = "workflow", value_enum)]
    workflows: Vec<WorkflowArg>,

    #[arg(long = "no-workflow", conflicts_with = "workflows")]
    no_workflow: bool,

    #[arg(skip)]
    yes: bool,

    /// Deprecated: setup docs are always fetched fresh and are not cached
    #[arg(long)]
    refresh_docs: bool,

    /// Number of concurrent workers for docs prefetch/download.
    #[arg(long, default_value_t = crate::sync::default_workers())]
    workers: usize,

    /// Language(s) to instrument (repeatable; case-insensitive).
    /// When provided, the agent skips language auto-detection and instruments
    /// the specified language(s) directly.
    /// Accepted values: python, typescript, javascript, go, csharp, c#, java, ruby
    #[arg(long = "language", value_enum, ignore_case = true)]
    languages: Vec<LanguageArg>,

    /// Run the agent in interactive TUI mode [default]
    #[arg(long, conflicts_with = "background", alias = "interactive")]
    tui: bool,

    /// Run the agent in background (non-interactive) mode. Use --verbose to see the agent output
    #[arg(long, conflicts_with = "tui")]
    background: bool,

    #[command(flatten)]
    permissions: InstrumentPermissionArgs,

    #[arg(skip)]
    prompt_for_missing_options: bool,
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
    Copilot,
    Cursor,
    Gemini,
    Opencode,
    Qwen,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Ord, PartialOrd, Serialize)]
#[serde(rename_all = "lowercase")]
enum Agent {
    Claude,
    Codex,
    Copilot,
    Cursor,
    Gemini,
    Opencode,
    Qwen,
}

struct AgentMetadata {
    binary: &'static str,
    repo_marker: Option<&'static str>,
    home_markers: &'static [&'static str],
    skill_alias_dir: Option<&'static str>,
}

impl Agent {
    fn as_str(self) -> &'static str {
        match self {
            Agent::Claude => "claude",
            Agent::Codex => "codex",
            Agent::Copilot => "copilot",
            Agent::Cursor => "cursor",
            Agent::Gemini => "gemini",
            Agent::Opencode => "opencode",
            Agent::Qwen => "qwen",
        }
    }

    fn display_name(self) -> &'static str {
        match self {
            Agent::Claude => "Claude",
            Agent::Codex => "Codex",
            Agent::Copilot => "Copilot",
            Agent::Cursor => "Cursor",
            Agent::Gemini => "Gemini",
            Agent::Opencode => "Opencode",
            Agent::Qwen => "Qwen",
        }
    }

    fn metadata(self) -> AgentMetadata {
        match self {
            Agent::Claude => AgentMetadata {
                binary: "claude",
                repo_marker: Some(".claude"),
                home_markers: &[".claude"],
                skill_alias_dir: Some(".claude"),
            },
            Agent::Codex => AgentMetadata {
                binary: "codex",
                repo_marker: None,
                home_markers: &[".codex"],
                skill_alias_dir: None,
            },
            Agent::Copilot => AgentMetadata {
                binary: "copilot",
                repo_marker: Some(".copilot"),
                home_markers: &[".copilot"],
                skill_alias_dir: None,
            },
            Agent::Cursor => AgentMetadata {
                binary: "cursor-agent",
                repo_marker: Some(".cursor"),
                home_markers: &[".cursor"],
                skill_alias_dir: Some(".cursor"),
            },
            Agent::Gemini => AgentMetadata {
                binary: "gemini",
                repo_marker: Some(".gemini"),
                home_markers: &[".gemini"],
                skill_alias_dir: Some(".gemini"),
            },
            Agent::Opencode => AgentMetadata {
                binary: "opencode",
                repo_marker: Some(".opencode"),
                home_markers: &[".opencode", ".config/opencode"],
                skill_alias_dir: None,
            },
            Agent::Qwen => AgentMetadata {
                binary: "qwen",
                repo_marker: Some(".qwen"),
                home_markers: &[".qwen"],
                skill_alias_dir: Some(".qwen"),
            },
        }
    }

    fn install_skill(
        self,
        scope: InstallScope,
        local_root: Option<&Path>,
        home: &Path,
    ) -> Result<AgentInstallResult> {
        let alias_dir = self.metadata().skill_alias_dir;
        install_agent_skill(self, scope, local_root, home, alias_dir)
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
    #[serde(default)]
    on_path: bool,
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

struct SetupAuthContext {
    client: ApiClient,
    api_key: String,
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

pub async fn run_setup_top(base: BaseArgs, mut args: SetupArgs) -> Result<()> {
    // Deprecated flag: --no-mcp-skill is equivalent to --no-skills --no-mcp
    if args.no_mcp_skill {
        args.no_skills = true;
        args.no_mcp = true;
    }
    if base.json && args.instrument {
        bail!("--json conflicts with --instrument: JSON mode implies --no-instrument");
    }
    if base.json && args.tui {
        bail!(
            "--json conflicts with --tui: JSON output is not compatible with interactive TUI mode"
        );
    }
    if base.json {
        args.no_instrument = true;
    }
    match args.command {
        Some(SetupSubcommand::Skills(setup)) => run_setup(base, setup).await,
        Some(SetupSubcommand::Instrument(mut instrument)) => {
            instrument.prompt_for_missing_options = true;
            run_instrument_setup(base, instrument, false, true).await
        }
        Some(SetupSubcommand::Mcp(mcp)) => run_mcp_setup(base, mcp).await,
        Some(SetupSubcommand::Doctor(doctor)) => run_doctor(base, doctor),
        None => {
            let wizard_flags = WizardFlags {
                yolo: args.agents.permissions.yolo,
                skills: args.skills,
                no_skills: args.no_skills,
                mcp: args.mcp,
                no_mcp: args.no_mcp,
                local: args.agents.local,
                global: args.agents.global,
                instrument: args.instrument,
                no_instrument: args.no_instrument,
                tui: args.tui,
                background: args.background,
                agent: args.agents.agent,
                workflows: args.agents.workflows.clone(),
                no_workflow: args.agents.no_workflow,
                languages: args.languages.clone(),
            };
            if args.interactive {
                run_setup_wizard(base, wizard_flags).await
            } else {
                run_default_setup(base, args).await
            }
        }
    }
}

pub use docs::run_docs_top;

struct WizardFlags {
    yolo: bool,
    skills: bool,
    no_skills: bool,
    mcp: bool,
    no_mcp: bool,
    local: bool,
    global: bool,
    instrument: bool,
    no_instrument: bool,
    tui: bool,
    background: bool,
    agent: Option<AgentArg>,
    workflows: Vec<WorkflowArg>,
    no_workflow: bool,
    languages: Vec<LanguageArg>,
}

async fn run_setup_wizard(mut base: BaseArgs, flags: WizardFlags) -> Result<()> {
    let WizardFlags {
        yolo,
        skills: flag_skills,
        no_skills: flag_no_skills,
        mcp: flag_mcp,
        no_mcp: flag_no_mcp,
        local: flag_local,
        global: flag_global,
        instrument: flag_instrument,
        no_instrument: flag_no_instrument,
        tui: flag_tui,
        background: flag_background,
        agent: flag_agent,
        workflows: flag_workflows,
        no_workflow: flag_no_workflow,
        languages: flag_languages,
    } = flags;
    print_setup_banner(&base);
    eprintln!("Set up Braintrust SDK tracing");
    eprintln!(
        "Braintrust will use the coding agent you choose to add SDK tracing to this app and verify it works.\n"
    );

    let mut had_failures = false;
    let verbose = base.verbose;
    let home = home_dir().ok_or_else(|| anyhow!("failed to resolve HOME/USERPROFILE"))?;
    let git_root = find_git_root();
    let will_instrument = !flag_no_instrument;
    if git_root.is_none() && !base.json {
        eprintln!(
            "{} Not inside a git repository — the agent may edit files in the current directory.",
            style("!").yellow()
        );
    }

    // ── Step 1: Auth ──
    if verbose {
        print_wizard_step(1, "Auth");
    }
    let project_flag = will_instrument.then(|| base.project.clone()).flatten();
    let mut setup_auth = if will_instrument {
        Some(ensure_setup_auth(&mut base, !flag_no_instrument, !flag_no_instrument).await?)
    } else {
        None
    };
    let org = setup_auth
        .as_ref()
        .map(|auth| auth.client.org_name().to_string());

    if verbose {
        if let Some(org) = org.as_deref() {
            eprintln!("   {} Using org '{}'", style("✓").green(), org);
        } else {
            eprintln!("   {}", style("Skipped").dim());
        }
    }

    // ── Step 2: Project ──
    if verbose {
        print_wizard_step(2, "Project");
    }
    if project_flag.is_none() && ui::can_prompt() {
        eprintln!("First, select a project, or create a new one.");
        eprintln!("Projects organize AI features in your application. Each project contains logs, experiments, datasets, prompts, and other functions.");
    }
    let project = if let Some(auth) = setup_auth.as_ref() {
        select_project_with_skip(&auth.client, project_flag.as_deref(), !verbose).await?
    } else {
        None
    };
    if verbose {
        if let Some(ref project) = project {
            if let Some(org) = org.as_deref() {
                maybe_init(org, project)?;
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
        if let Some(org) = org.as_deref() {
            maybe_init(org, project)?;
        }
    }

    // ── Step 3: Agent tools (skills + MCP) ──
    if verbose {
        print_wizard_step(3, "Agents");
    }
    let multiselect_hint_shown = false;
    let (wants_skills, wants_mcp) = if flag_no_skills && flag_no_mcp {
        if verbose {
            eprintln!(
                "{} What would you like to set up? · {}",
                style("✔").green(),
                style("(none)").dim()
            );
        }
        (false, false)
    } else if flag_no_skills {
        (false, flag_mcp && !flag_no_mcp)
    } else if flag_no_mcp {
        (flag_skills && !flag_no_skills, false)
    } else if flag_skills || flag_mcp {
        if verbose {
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
        // Default setup is intentionally ephemeral: persistent skills/MCP are only
        // installed when the user asks for them explicitly (or accepts the
        // post-success skills prompt).
        (false, false)
    };

    let setup_context = if wants_skills || wants_mcp {
        let scope = if flag_local {
            if verbose {
                eprintln!(
                    "{} Select install scope · {}",
                    style("✔").green(),
                    style("local (current git repo)").green()
                );
            }
            InstallScope::Local
        } else if flag_global {
            if verbose {
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
        let local_root = resolve_local_root_for_scope(scope)?;
        let detected = detect_agents(local_root.as_deref(), &home);
        if !flag_no_instrument
            && should_print_agent_selection_intro(&base, flag_agent.is_some(), false)
        {
            print_coding_agent_selection_intro();
        }
        let selected_agent =
            resolve_default_agent_selection(flag_agent, &detected, "Select coding agent", true)?;
        if verbose && flag_agent.is_some() {
            eprintln!(
                "{} Select agent to configure · {}",
                style("✔").green(),
                style(selected_agent.as_str()).green()
            );
        }
        Some((scope, selected_agent, home.clone()))
    } else if !flag_no_instrument {
        let root = git_root
            .clone()
            .unwrap_or(std::env::current_dir().context("failed to get current directory")?);
        let detected = detect_agents(Some(&root), &home);
        if should_print_agent_selection_intro(&base, flag_agent.is_some(), false) {
            print_coding_agent_selection_intro();
        }
        let selected_agent =
            resolve_default_agent_selection(flag_agent, &detected, "Select coding agent", true)?;
        if verbose && flag_agent.is_some() {
            eprintln!(
                "{} Select coding agent · {}",
                style("✔").green(),
                style(selected_agent.as_str()).green()
            );
        }
        Some((InstallScope::Local, selected_agent, home.clone()))
    } else {
        None
    };

    if wants_skills {
        if verbose {
            eprintln!("   {}", style("Skills:").bold());
        }
        if let Some((scope, selected_agent, _)) = setup_context.as_ref() {
            let args = AgentsSetupArgs {
                agent: Some(map_agent_to_agent_arg(*selected_agent)),
                local: matches!(*scope, InstallScope::Local),
                global: matches!(*scope, InstallScope::Global),
                workflows: Vec::new(),
                no_workflow: true,
                yes: true,
                refresh_docs: false,
                workers: crate::sync::default_workers(),
                permissions: InstrumentPermissionArgs { yolo: false },
            };
            let outcome = execute_skills_setup(&base, &args, true).await?;
            for r in &outcome.results {
                if verbose {
                    print_wizard_agent_result(r);
                }
                if matches!(r.status, InstallStatus::Failed) {
                    had_failures = true;
                }
            }
        }
    }

    if wants_mcp {
        if verbose {
            eprintln!("   {}", style("MCP:").bold());
        }
        if setup_auth.is_none() {
            setup_auth = Some(ensure_setup_auth(&mut base, false, true).await?);
        }
        if let (Some((scope, selected_agent, home)), Some(auth)) =
            (setup_context.as_ref(), setup_auth.as_ref())
        {
            let local_root = resolve_local_root_for_scope(*scope)?;
            let api_url = auth.client.url("");
            let outcome = execute_mcp_install(
                *scope,
                local_root.as_deref(),
                home,
                &[*selected_agent],
                &auth.api_key,
                &mcp_url_from_api_url(&api_url),
            );
            for r in &outcome.results {
                if verbose {
                    print_wizard_agent_result(r);
                }
                if matches!(r.status, InstallStatus::Failed) {
                    had_failures = true;
                }
            }
            if outcome.installed_count == 0 {
                had_failures = true;
            }
        }
    }

    if !wants_skills && !wants_mcp && verbose {
        eprintln!("   {}", style("Skipped").dim());
    }

    // ── Step 4: Instrument ──
    if verbose {
        print_wizard_step(4, "Instrument");
    }
    {
        let instrument = if flag_no_instrument {
            if verbose {
                eprintln!(
                    "Run instrumentation agent to set up tracing in this repo? {}",
                    style("no").dim()
                );
            }
            false
        } else if flag_instrument {
            if verbose {
                eprintln!(
                    "Run instrumentation agent to set up tracing in this repo? {}",
                    style("yes").green()
                );
            }
            true
        } else {
            let term = ui::prompt_term().ok_or_else(|| anyhow!("interactive mode requires TTY"))?;
            Confirm::with_theme(&ColorfulTheme::default())
                .with_prompt("Run instrumentation agent to set up tracing in this repo?")
                .default(true)
                .interact_on(&term)?
        };
        if instrument {
            let instrument_agent = setup_context
                .as_ref()
                .map(|(_, agent, _)| map_agent_to_instrument_agent_arg(*agent))
                .or_else(|| {
                    flag_agent.map(|arg| map_agent_to_instrument_agent_arg(map_agent_arg(arg)))
                })
                .or_else(|| determine_wizard_instrument_agent(flag_agent));
            let selected_workflows = if flag_no_workflow {
                Vec::new()
            } else if !flag_workflows.is_empty() {
                resolve_prompted_instrument_workflows(flag_workflows.clone())
            } else if ui::can_prompt() {
                prompt_instrument_workflow_selection()?.ok_or_else(|| anyhow!("setup cancelled"))?
            } else {
                vec![WorkflowArg::Instrument, WorkflowArg::Observe]
            };
            let selected_languages = if !flag_languages.is_empty() {
                flag_languages.clone()
            } else if ui::can_prompt() {
                let defaults = detect_languages_from_dir(&std::env::current_dir()?);
                prompt_instrument_language_selection(&defaults)?
                    .ok_or_else(|| anyhow!("setup cancelled"))?
            } else {
                Vec::new()
            };
            let (run_tui, yolo_mode) = if flag_tui {
                (true, yolo)
            } else if flag_background {
                (false, yolo)
            } else if ui::can_prompt() {
                let term =
                    ui::prompt_term().ok_or_else(|| anyhow!("interactive mode requires TTY"))?;
                let run_tui = Select::with_theme(&ColorfulTheme::default())
                    .with_prompt("How do you want to run the agent?")
                    .items(&["Interactive (TUI)", "Background"])
                    .default(0)
                    .interact_on(&term)?
                    == 0;
                let yolo_mode = if yolo {
                    true
                } else {
                    Confirm::with_theme(&ColorfulTheme::default())
                        .with_prompt("Grant agent full permissions? (bypass permission prompts)")
                        .default(false)
                        .interact_on(&term)?
                };
                (run_tui, yolo_mode)
            } else {
                let (run_interactive, bypass_permissions) = resolve_instrument_run_mode(
                    &InstrumentSetupArgs {
                        agent: instrument_agent,
                        agent_cmd: None,
                        workflows: selected_workflows.clone(),
                        no_workflow: flag_no_workflow,
                        yes: false,
                        refresh_docs: false,
                        workers: crate::sync::default_workers(),
                        languages: selected_languages.clone(),
                        tui: false,
                        background: false,
                        permissions: InstrumentPermissionArgs { yolo },
                        prompt_for_missing_options: false,
                    },
                    ui::can_prompt(),
                );
                (run_interactive, bypass_permissions)
            };
            run_instrument_setup(
                base,
                InstrumentSetupArgs {
                    agent: instrument_agent,
                    agent_cmd: None,
                    workflows: selected_workflows,
                    no_workflow: flag_no_workflow,
                    yes: false,
                    refresh_docs: false,
                    workers: crate::sync::default_workers(),
                    languages: selected_languages,
                    tui: run_tui,
                    background: !run_tui,
                    permissions: InstrumentPermissionArgs { yolo: yolo_mode },
                    prompt_for_missing_options: false,
                },
                !multiselect_hint_shown,
                !wants_skills,
            )
            .await?;
        } else if verbose {
            eprintln!("   {}", style("Skipped").dim());
        }
    }

    // ── Done ──
    if verbose {
        print_wizard_done(had_failures);
    }
    if had_failures {
        bail!("setup completed with failures");
    }
    Ok(())
}

async fn run_default_setup(mut base: BaseArgs, args: SetupArgs) -> Result<()> {
    if !base.json {
        print_setup_banner(&base);
    }

    let will_instrument = !args.no_instrument;
    if will_instrument && find_git_root().is_none() && !base.json {
        eprintln!(
            "{} Not inside a git repository — the agent may edit files in the current directory.",
            style("!").yellow()
        );
    }
    let wants_skills = args.skills && !args.no_skills;
    let wants_mcp = args.mcp && !args.no_mcp;
    if will_instrument {
        let project_flag = base.project.clone();
        let auth = ensure_setup_auth(&mut base, false, true).await?;
        let org = auth.client.org_name().to_string();
        let project = select_project_with_skip(&auth.client, project_flag.as_deref(), true).await?;
        if let Some(ref project) = project {
            maybe_init(&org, project)?;
        }
    }

    if !wants_skills && !wants_mcp && !will_instrument {
        return Ok(());
    }

    let scope = default_setup_scope(&args.agents);
    let home = home_dir().ok_or_else(|| anyhow!("failed to resolve HOME/USERPROFILE"))?;
    let local_root = resolve_local_root_for_scope(scope)?;
    let detected = detect_agents(local_root.as_deref(), &home);
    if will_instrument
        && should_print_agent_selection_intro(&base, args.agents.agent.is_some(), false)
    {
        print_coding_agent_selection_intro();
    }
    let selected_agent = resolve_default_agent_selection(
        args.agents.agent,
        &detected,
        "Select coding agent",
        ui::can_prompt(),
    )?;

    if wants_skills {
        run_setup(
            base.clone(),
            AgentsSetupArgs {
                agent: Some(map_agent_to_agent_arg(selected_agent)),
                local: matches!(scope, InstallScope::Local),
                global: matches!(scope, InstallScope::Global),
                workflows: args.agents.workflows.clone(),
                no_workflow: args.agents.no_workflow,
                yes: true,
                refresh_docs: args.agents.refresh_docs,
                workers: args.agents.workers,
                permissions: args.agents.permissions.clone(),
            },
        )
        .await?;
    }

    if wants_mcp {
        run_mcp_setup(
            base.clone(),
            AgentsMcpSetupArgs {
                agent: Some(map_agent_to_agent_arg(selected_agent)),
                local: matches!(scope, InstallScope::Local),
                global: matches!(scope, InstallScope::Global),
                yes: true,
            },
        )
        .await?;
    }

    if will_instrument {
        run_instrument_setup(
            base,
            InstrumentSetupArgs {
                agent: Some(map_agent_to_instrument_agent_arg(selected_agent)),
                agent_cmd: None,
                workflows: args.agents.workflows,
                no_workflow: args.agents.no_workflow,
                yes: false,
                refresh_docs: args.agents.refresh_docs,
                workers: args.agents.workers,
                languages: args.languages,
                tui: args.tui,
                background: args.background,
                permissions: args.agents.permissions,
                prompt_for_missing_options: false,
            },
            false,
            !wants_skills,
        )
        .await?;
    }

    Ok(())
}

fn in_ci() -> bool {
    std::env::var_os("CI").is_some()
}

fn setup_banner_color_enabled(base: &BaseArgs) -> bool {
    if base.no_color || std::env::var_os("NO_COLOR").is_some() {
        return false;
    }

    !matches!(std::env::var_os("TERM"), Some(term) if term == OsStr::new("dumb"))
}

fn print_setup_banner(base: &BaseArgs) {
    let color_enabled = setup_banner_color_enabled(base);
    eprintln!(
        "{}",
        style(crate::BANNER)
            .for_stderr()
            .blue()
            .force_styling(color_enabled)
    );
    eprintln!(
        "{}",
        style("Braintrust")
            .for_stderr()
            .blue()
            .force_styling(color_enabled)
    );
    eprintln!();
}

fn print_coding_agent_selection_intro() {
    eprintln!(
        "Braintrust will ask a coding agent to add SDK tracing, run your app, and verify data reaches Braintrust."
    );
    eprintln!("Choose which agent to use for this one-time setup run.");
}

fn should_print_agent_selection_intro(
    base: &BaseArgs,
    agent_is_specified: bool,
    yes: bool,
) -> bool {
    !base.json && !agent_is_specified && !yes && ui::can_prompt()
}

fn apply_setup_config_fallbacks(base: &mut BaseArgs) {
    let cfg = config::load().unwrap_or_default();

    if base
        .org_name
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
    {
        base.org_name = cfg
            .org
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
    }

    if base
        .project
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
    {
        base.project = cfg
            .project
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty());
    }
}

fn setup_can_prompt(base: &BaseArgs) -> bool {
    !base.json && !base.no_input && !in_ci() && ui::can_prompt()
}

fn resolve_profile_name_for_setup(
    base: &BaseArgs,
    profiles: &[auth::ProfileInfo],
    prompt_for_choice: bool,
) -> Result<Option<String>> {
    if let Some(profile_name) = base
        .profile
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        if profiles.iter().any(|profile| profile.name == profile_name) {
            return Ok(Some(profile_name.to_string()));
        }
        bail!(
            "profile '{profile_name}' not found; run `bt auth profiles` to see available profiles"
        );
    }

    if let Some(org_name) = base.org_name.as_deref() {
        if let Some(profile_name) = profiles
            .iter()
            .find(|profile| profile.name == org_name)
            .map(|profile| profile.name.clone())
        {
            return Ok(Some(profile_name));
        }

        let mut matches = profiles
            .iter()
            .filter(|profile| profile.org_name.as_deref() == Some(org_name))
            .map(|profile| profile.name.clone())
            .collect::<Vec<_>>();
        matches.sort();

        return match matches.len() {
            0 => Ok(None),
            1 => Ok(Some(matches.remove(0))),
            _ if prompt_for_choice => auth::select_profile_interactive(Some(org_name))?
                .map(Some)
                .ok_or_else(|| anyhow!("no profile selected")),
            _ => bail!(
                "multiple profiles for org '{org_name}': {}. Use --profile to disambiguate.",
                matches.join(", ")
            ),
        };
    }

    if profiles.len() == 1 {
        return Ok(Some(profiles[0].name.clone()));
    }

    if prompt_for_choice && !profiles.is_empty() {
        auth::select_profile_interactive(None)?
            .map(Some)
            .ok_or_else(|| anyhow!("no profile selected"))
    } else {
        Ok(None)
    }
}

fn find_http_error(err: &anyhow::Error) -> Option<&crate::http::HttpError> {
    err.chain()
        .find_map(|source| source.downcast_ref::<crate::http::HttpError>())
}

async fn list_available_orgs_for_setup(
    api_key: &str,
    app_url: &str,
) -> Result<Vec<auth::AvailableOrg>> {
    match auth::list_available_orgs_for_api_key(api_key, app_url).await {
        Ok(orgs) => Ok(orgs),
        Err(err) => {
            if let Some(http_err) = find_http_error(&err) {
                if matches!(http_err.status.as_u16(), 401 | 403) {
                    return Ok(Vec::new());
                }
            }
            if err
                .to_string()
                .contains("no organizations found for this API key")
            {
                return Ok(Vec::new());
            }
            Err(err)
        }
    }
}

fn find_available_org<'a>(
    orgs: &'a [auth::AvailableOrg],
    org_name: &str,
) -> Option<&'a auth::AvailableOrg> {
    orgs.iter().find(|org| org.name == org_name).or_else(|| {
        let lowered = org_name.to_ascii_lowercase();
        orgs.iter()
            .find(|org| org.name.to_ascii_lowercase() == lowered)
    })
}

fn build_api_key_login_context(
    base: &BaseArgs,
    api_key: &str,
    org: &auth::AvailableOrg,
) -> LoginContext {
    let api_url = base
        .api_url
        .clone()
        .or_else(|| org.api_url.clone())
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());
    let app_url = base
        .app_url
        .clone()
        .unwrap_or_else(|| DEFAULT_APP_URL.to_string());
    let login = braintrust_sdk_rust::LoginState::new();
    let _ = login.set(
        api_key.to_string(),
        org.id.clone(),
        org.name.clone(),
        api_url.clone(),
        app_url.clone(),
    );

    LoginContext {
        login,
        api_url,
        app_url,
    }
}

async fn build_api_key_client(
    base: &BaseArgs,
    api_key: &str,
    org: &auth::AvailableOrg,
) -> Result<ApiClient> {
    ApiClient::new(&build_api_key_login_context(base, api_key, org))
}

async fn project_exists_in_org(client: &ApiClient, project_name: &str) -> Result<bool> {
    let projects = crate::projects::api::list_projects(client).await?;
    Ok(projects
        .into_iter()
        .any(|project| project.name == project_name))
}

async fn orgs_with_project(
    base: &BaseArgs,
    api_key: &str,
    orgs: &[auth::AvailableOrg],
    project_name: &str,
) -> Result<Vec<auth::AvailableOrg>> {
    let mut matches = Vec::new();
    for org in orgs {
        let client = build_api_key_client(base, api_key, org).await?;
        if project_exists_in_org(&client, project_name).await? {
            matches.push(org.clone());
        }
    }
    Ok(matches)
}

fn prompt_for_org_choice(prompt: &str, orgs: &[auth::AvailableOrg]) -> Result<auth::AvailableOrg> {
    let labels: Vec<&str> = orgs.iter().map(|org| org.name.as_str()).collect();
    let idx = FuzzySelect::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(&labels)
        .default(0)
        .interact_on(&ui::prompt_term().ok_or_else(|| anyhow!("interactive mode requires TTY"))?)?;
    Ok(orgs[idx].clone())
}

fn select_api_key_org_for_setup(
    base: &BaseArgs,
    orgs: &[auth::AvailableOrg],
    project_name: Option<&str>,
    preferred_org_names: &[String],
) -> Result<auth::AvailableOrg> {
    let mut candidates = if preferred_org_names.is_empty() {
        orgs.to_vec()
    } else {
        orgs.iter()
            .filter(|org| preferred_org_names.iter().any(|name| name == &org.name))
            .cloned()
            .collect::<Vec<_>>()
    };

    candidates.sort_by(|left, right| left.name.cmp(&right.name));
    candidates.dedup_by(|left, right| left.name == right.name);

    if candidates.len() == 1 {
        return Ok(candidates.remove(0));
    }

    if setup_can_prompt(base) {
        let prompt = if project_name.is_some() {
            "Select organization for the requested project"
        } else {
            "Select organization"
        };
        return prompt_for_org_choice(prompt, &candidates);
    }

    bail!("organization choice required in non-interactive mode; pass --org <NAME>")
}

fn matching_profile_org_names(
    profiles: &[auth::StoredProfileInfo],
    only_oauth: Option<bool>,
) -> Vec<String> {
    let mut org_names = profiles
        .iter()
        .filter(|profile| {
            only_oauth
                .map(|expected| profile.is_oauth == expected)
                .unwrap_or(true)
        })
        .filter_map(|profile| profile.org_name.clone())
        .collect::<Vec<_>>();
    org_names.sort();
    org_names.dedup();
    org_names
}

async fn ensure_profile_or_oauth_auth(
    base: &mut BaseArgs,
    prompt_for_profile_choice: bool,
) -> Result<(LoginContext, bool)> {
    let profiles = auth::list_profiles()?;
    let can_prompt = setup_can_prompt(base);
    let should_prompt_for_profile_choice = prompt_for_profile_choice && can_prompt;
    let selected_profile =
        resolve_profile_name_for_setup(base, &profiles, should_prompt_for_profile_choice)?;
    let mut auth_base = base.clone();
    auth_base.api_key = None;
    auth_base.api_key_source = None;

    if let Some(profile_name) = selected_profile {
        auth_base.profile = Some(profile_name.clone());

        match auth::login(&auth_base).await {
            Ok(ctx) => {
                base.profile = auth_base.profile.clone();
                let is_oauth = auth::resolve_auth(&auth_base).await?.is_oauth;
                return Ok((ctx, is_oauth));
            }
            Err(err) if auth::is_missing_credential_error(&err) => {
                if base.verbose {
                    eprintln!(
                        "   Profile '{}' credentials inaccessible ({}). Re-authenticating via OAuth...",
                        profile_name, err
                    );
                }
                if !can_prompt {
                    bail!(
                        "setup needs interactive OAuth re-authentication; rerun without --no-input/--json or pass a working API key"
                    );
                }
                auth::login_interactive_oauth(&mut auth_base).await?;
                base.profile = auth_base.profile.clone();
                return Ok((auth::login(&auth_base).await?, true));
            }
            Err(err) => return Err(err),
        }
    }

    if !can_prompt {
        if profiles.is_empty() {
            bail!(
                "setup needs interactive authentication; rerun without --no-input/--json or pass a valid API key/profile"
            );
        }
        bail!("profile selection required in non-interactive mode; pass --profile <NAME>");
    }

    if base.verbose {
        eprintln!("Starting OAuth login.\n");
    }
    auth::login_interactive_oauth(&mut auth_base).await?;
    base.profile = auth_base.profile.clone();
    Ok((auth::login(&auth_base).await?, true))
}

async fn ensure_setup_auth(
    base: &mut BaseArgs,
    prompt_for_profile_choice: bool,
    needs_api_key: bool,
) -> Result<SetupAuthContext> {
    let project_was_explicit = base
        .project
        .as_deref()
        .map(str::trim)
        .is_some_and(|value| !value.is_empty());
    apply_setup_config_fallbacks(base);

    let explicit_api_key = base
        .api_key
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let stored_profiles = auth::list_stored_profiles()?;
    let mut project_name = base
        .project
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let org_name = base
        .org_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);
    let profile_name = base
        .profile
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(str::to_string);

    if let Some(api_key) = explicit_api_key.as_deref() {
        let app_url = base
            .app_url
            .clone()
            .unwrap_or_else(|| DEFAULT_APP_URL.to_string());
        let available_orgs = list_available_orgs_for_setup(api_key, &app_url).await?;
        if project_name.is_some() && org_name.is_none() && profile_name.is_none() {
            let name = project_name
                .as_deref()
                .expect("project name exists when probing all orgs");
            let matched_orgs = orgs_with_project(base, api_key, &available_orgs, name).await?;
            if matched_orgs.is_empty() {
                clear_missing_fallback_setup_project(base, &mut project_name, project_was_explicit);
            }
        }

        if let Some(profile_name) = profile_name.as_deref() {
            let profile = stored_profiles
                .iter()
                .find(|profile| profile.name == profile_name)
                .ok_or_else(|| anyhow!("profile '{profile_name}' not found"))?;
            let target_org = match org_name.as_deref() {
                Some(org_name) => {
                    if profile.org_name.as_deref() != Some(org_name) {
                        bail!(
                            "profile '{profile_name}' belongs to org '{}' but '{}' was requested",
                            profile.org_name.as_deref().unwrap_or("(none)"),
                            org_name
                        );
                    }
                    org_name
                }
                None => profile.org_name.as_deref().ok_or_else(|| {
                    anyhow!("profile '{profile_name}' does not have a default org")
                })?,
            };

            if let Some(org) = find_available_org(&available_orgs, target_org) {
                let client = build_api_key_client(base, api_key, org).await?;
                ensure_selected_setup_project(
                    base,
                    &client,
                    &mut project_name,
                    project_was_explicit,
                    &org.name,
                )
                .await?;
                return build_setup_auth_context(base, client, false, needs_api_key).await;
            }

            let (login_ctx, is_oauth) =
                ensure_profile_or_oauth_auth(base, prompt_for_profile_choice).await?;
            let client = ApiClient::new(&login_ctx)?;
            let resolved_org = client.org_name().to_string();
            ensure_selected_setup_project(
                base,
                &client,
                &mut project_name,
                project_was_explicit,
                &resolved_org,
            )
            .await?;
            return build_setup_auth_context(base, client, is_oauth, needs_api_key).await;
        }

        if let Some(org_name) = org_name.as_deref() {
            let matching_profile_count = stored_profiles
                .iter()
                .filter(|profile| profile.org_name.as_deref() == Some(org_name))
                .count();
            if base.prefer_profile && matching_profile_count == 0 {
                bail!("no profile found for org '{org_name}'");
            }

            if let Some(org) = find_available_org(&available_orgs, org_name) {
                let client = build_api_key_client(base, api_key, org).await?;
                ensure_selected_setup_project(
                    base,
                    &client,
                    &mut project_name,
                    project_was_explicit,
                    org_name,
                )
                .await?;
                return build_setup_auth_context(base, client, false, needs_api_key).await;
            }

            let (login_ctx, is_oauth) =
                ensure_profile_or_oauth_auth(base, prompt_for_profile_choice).await?;
            let client = ApiClient::new(&login_ctx)?;
            let resolved_org = client.org_name().to_string();
            ensure_selected_setup_project(
                base,
                &client,
                &mut project_name,
                project_was_explicit,
                &resolved_org,
            )
            .await?;
            return build_setup_auth_context(base, client, is_oauth, needs_api_key).await;
        }

        if base.prefer_profile {
            if stored_profiles.is_empty() {
                let matched_orgs = match project_name.as_deref() {
                    Some(project_name) => {
                        orgs_with_project(base, api_key, &available_orgs, project_name).await?
                    }
                    None => available_orgs.clone(),
                };
                if matched_orgs.is_empty() {
                    let (login_ctx, is_oauth) =
                        ensure_profile_or_oauth_auth(base, prompt_for_profile_choice).await?;
                    let client = ApiClient::new(&login_ctx)?;
                    return build_setup_auth_context(base, client, is_oauth, needs_api_key).await;
                }
                let org = select_api_key_org_for_setup(
                    base,
                    &matched_orgs,
                    project_name.as_deref(),
                    &[],
                )?;
                let client = build_api_key_client(base, api_key, &org).await?;
                let api_url = base
                    .api_url
                    .clone()
                    .or_else(|| org.api_url.clone())
                    .unwrap_or_else(|| DEFAULT_API_URL.to_string());
                auth::commit_api_key_profile(
                    &org.name,
                    api_key,
                    api_url,
                    base.app_url.clone(),
                    Some(org.name.clone()),
                )?;
                return build_setup_auth_context(base, client, false, needs_api_key).await;
            }

            let preferred_org_names = matching_profile_org_names(&stored_profiles, None);
            let matching_orgs = available_orgs
                .iter()
                .filter(|org| preferred_org_names.iter().any(|name| name == &org.name))
                .cloned()
                .collect::<Vec<_>>();
            if matching_orgs.is_empty() {
                let (login_ctx, is_oauth) =
                    ensure_profile_or_oauth_auth(base, prompt_for_profile_choice).await?;
                let client = ApiClient::new(&login_ctx)?;
                return build_setup_auth_context(base, client, is_oauth, needs_api_key).await;
            }

            let candidate_orgs = match project_name.as_deref() {
                Some(project_name) => {
                    orgs_with_project(base, api_key, &matching_orgs, project_name).await?
                }
                None => matching_orgs,
            };
            if candidate_orgs.is_empty() {
                let (login_ctx, is_oauth) =
                    ensure_profile_or_oauth_auth(base, prompt_for_profile_choice).await?;
                let client = ApiClient::new(&login_ctx)?;
                return build_setup_auth_context(base, client, is_oauth, needs_api_key).await;
            }
            let org = select_api_key_org_for_setup(
                base,
                &candidate_orgs,
                project_name.as_deref(),
                &preferred_org_names,
            )?;
            let client = build_api_key_client(base, api_key, &org).await?;
            return build_setup_auth_context(base, client, false, needs_api_key).await;
        }

        let candidate_orgs = match project_name.as_deref() {
            Some(project_name) => {
                orgs_with_project(base, api_key, &available_orgs, project_name).await?
            }
            None => available_orgs.clone(),
        };
        if candidate_orgs.is_empty() {
            let (login_ctx, is_oauth) =
                ensure_profile_or_oauth_auth(base, prompt_for_profile_choice).await?;
            let client = ApiClient::new(&login_ctx)?;
            return build_setup_auth_context(base, client, is_oauth, needs_api_key).await;
        }
        let org =
            select_api_key_org_for_setup(base, &candidate_orgs, project_name.as_deref(), &[])?;
        let client = build_api_key_client(base, api_key, &org).await?;
        return build_setup_auth_context(base, client, false, needs_api_key).await;
    }

    let (login_ctx, is_oauth) =
        ensure_profile_or_oauth_auth(base, prompt_for_profile_choice).await?;
    let client = ApiClient::new(&login_ctx)?;
    build_setup_auth_context(base, client, is_oauth, needs_api_key).await
}

async fn ensure_selected_setup_project(
    base: &mut BaseArgs,
    client: &ApiClient,
    project_name: &mut Option<String>,
    project_was_explicit: bool,
    org_name: &str,
) -> Result<()> {
    let Some(requested_project) = project_name.as_deref() else {
        return Ok(());
    };

    if project_exists_in_org(client, requested_project).await? {
        return Ok(());
    }

    if project_was_explicit {
        bail!("project '{requested_project}' not found in org '{org_name}'");
    }

    clear_missing_fallback_setup_project(base, project_name, false);
    Ok(())
}

fn clear_missing_fallback_setup_project(
    base: &mut BaseArgs,
    project_name: &mut Option<String>,
    project_was_explicit: bool,
) {
    if project_was_explicit || project_name.is_none() {
        return;
    }

    base.project = None;
    *project_name = None;
}

fn sync_setup_api_key(base: &mut BaseArgs, api_key: &str) {
    let trimmed = api_key.trim();
    if trimmed.is_empty() {
        return;
    }

    base.api_key = Some(trimmed.to_string());
    std::env::set_var("BRAINTRUST_API_KEY", trimmed);
}

async fn build_setup_auth_context(
    base: &mut BaseArgs,
    client: ApiClient,
    is_oauth: bool,
    needs_api_key: bool,
) -> Result<SetupAuthContext> {
    let api_key = if should_create_api_key_for_setup(is_oauth, base, needs_api_key) {
        maybe_create_api_key_for_oauth(base, &client).await?
    } else {
        client.api_key().to_string()
    };

    if needs_api_key {
        sync_setup_api_key(base, &api_key);
    }

    Ok(SetupAuthContext { client, api_key })
}

fn should_create_api_key_for_setup(is_oauth: bool, base: &BaseArgs, needs_api_key: bool) -> bool {
    needs_api_key
        && is_oauth
        && !matches!(
            base.api_key_source,
            Some(ArgValueSource::CommandLine | ArgValueSource::EnvVariable)
        )
}

async fn maybe_create_api_key_for_oauth(base: &BaseArgs, client: &ApiClient) -> Result<String> {
    let username = std::process::Command::new("whoami")
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "user".to_string());
    let base_name = format!("{username}-created-by-bt-setup");

    #[derive(serde::Deserialize)]
    struct ApiKeyEntry {
        name: String,
    }
    #[derive(serde::Deserialize)]
    struct ApiKeyList {
        objects: Vec<ApiKeyEntry>,
    }
    #[derive(serde::Deserialize)]
    struct CreatedKey {
        key: String,
    }

    let existing: Vec<String> = client
        .get::<ApiKeyList>("/v1/api_key")
        .await
        .context("failed to list existing Braintrust API keys before creating one")?
        .objects
        .into_iter()
        .map(|k| k.name)
        .collect();

    let name = if !existing.iter().any(|n| n == &base_name) {
        base_name
    } else {
        (1u32..)
            .map(|i| format!("{base_name}{i}"))
            .find(|candidate| !existing.iter().any(|n| n == candidate))
            .expect("name sequence is infinite")
    };

    let body = serde_json::json!({ "name": name, "org_name": client.org_name() });
    let created: CreatedKey = client.post("/v1/api_key", &body).await?;

    let explicitly_quiet = base.quiet && base.quiet_source.is_some();
    if std::io::stderr().is_terminal() && !explicitly_quiet {
        eprintln!();
        eprintln!(
            "{} Created Braintrust API key '{}' for instrumentation and exported it to this setup process:",
            style("!").yellow().bold(),
            name,
        );
        eprintln!();
        eprintln!("   {}", style(&created.key).bold());
        eprintln!();
        eprintln!("   To reuse it later in your shell, run:");
        eprintln!(
            "   {}",
            style(format!("export BRAINTRUST_API_KEY={}", created.key)).dim()
        );
        eprintln!();
    }

    Ok(created.key)
}

async fn select_project_for_setup(
    client: &ApiClient,
    project_name: Option<&str>,
) -> Result<crate::projects::api::Project> {
    if let Some(name) = project_name {
        let projects = with_spinner(
            "Loading projects...",
            crate::projects::api::list_projects(client),
        )
        .await?;
        if let Some(project) = projects.into_iter().find(|project| project.name == name) {
            return Ok(project);
        }
        bail!(
            "project '{}' not found in org '{}'",
            name,
            client.org_name()
        )
    }

    if !ui::can_prompt() {
        bail!(
            "project choice required in non-interactive mode; pass --project <NAME> or set BRAINTRUST_DEFAULT_PROJECT"
        );
    }

    ui::select_project(
        client,
        None,
        Some("Select project"),
        ui::ProjectSelectMode::AllowCreate,
    )
    .await
}

async fn select_project_with_skip(
    client: &ApiClient,
    project_name: Option<&str>,
    quiet: bool,
) -> Result<Option<crate::projects::api::Project>> {
    let project = select_project_for_setup(client, project_name).await?;
    if !quiet {
        eprintln!("{} Select project · {}", style("✔").green(), project.name);
    }
    Ok(Some(project))
}

fn maybe_init(org: &str, project: &crate::projects::api::Project) -> Result<()> {
    let config_path = std::env::current_dir()?.join(".bt").join("config.json");
    let mut cfg = if config_path.exists() {
        let existing = config::load_file(&config_path);
        let matches = existing.org.as_deref() == Some(org)
            && existing.project.as_deref() == Some(project.name.as_str());
        if matches && existing.project_id.as_deref() == Some(project.id.as_str()) {
            return Ok(());
        }
        existing
    } else {
        config::Config::default()
    };

    cfg.org = Some(org.to_string());
    cfg.project = Some(project.name.clone());
    cfg.project_id = Some(project.id.clone());
    config::save_local(&cfg, true)?;
    Ok(())
}

fn default_setup_scope(args: &AgentsSetupArgs) -> InstallScope {
    if args.local {
        InstallScope::Local
    } else {
        InstallScope::Global
    }
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
    } else if base.verbose {
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
    let show_progress = !base.json && !quiet && base.verbose;

    if show_progress {
        println!("Configuring coding agents for Braintrust");
    }

    for agent in selected_agents.iter().copied() {
        let result = agent.install_skill(scope, local_root.as_deref(), &home);

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
    Copilot,
    Cursor,
    Gemini,
    Opencode,
    Qwen,
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

    fn doc_filename(self) -> &'static str {
        match self {
            LanguageArg::Python => "python.md",
            LanguageArg::TypeScript => "typescript.md",
            LanguageArg::Go => "go.md",
            LanguageArg::CSharp => "csharp.md",
            LanguageArg::Java => "java.md",
            LanguageArg::Ruby => "ruby.md",
        }
    }
}

async fn run_instrument_setup(
    base: BaseArgs,
    args: InstrumentSetupArgs,
    print_hint: bool,
    offer_skills_after_success: bool,
) -> Result<()> {
    let home = home_dir().ok_or_else(|| anyhow!("failed to resolve HOME/USERPROFILE"))?;
    let cwd = std::env::current_dir().context("failed to get current directory")?;
    let git_root = find_git_root();
    let root = git_root.clone().unwrap_or_else(|| cwd.clone());
    let detected = detect_agents(Some(&root), &home);

    if should_print_agent_selection_intro(&base, args.agent.is_some(), args.yes) {
        print_coding_agent_selection_intro();
    }

    let selected = resolve_default_agent_selection(
        args.agent.map(map_instrument_agent_arg_to_agent_arg),
        &detected,
        "Select agent to instrument this repo",
        ui::can_prompt() && !args.yes,
    )?;

    if args.agent.is_some() && base.verbose {
        eprintln!(
            "{} Select agent to instrument this repo · {}",
            style("✔").green(),
            style(selected.as_str()).green()
        );
    }

    let mut hint_pending = print_hint && base.verbose;
    let selected_workflows = resolve_instrument_workflow_selection(&args, &mut hint_pending)?;

    let selected_languages: Vec<LanguageArg> = if !args.languages.is_empty() {
        args.languages.clone()
    } else if args.prompt_for_missing_options && ui::can_prompt() && !args.yes && !base.json {
        if hint_pending {
            eprintln!(
                "   {}",
                style("(Un)select option with Space, confirm selection with Enter.").dim()
            );
        }
        let detected_langs = detect_languages_from_dir(&cwd);
        let Some(langs) = prompt_instrument_language_selection(&detected_langs)? else {
            bail!("instrument setup cancelled by user");
        };
        langs
    } else {
        detect_languages_from_dir(&cwd)
    };

    let show_progress = !base.json;
    let mut warnings = Vec::new();
    let mut notes = Vec::new();
    let mut results = Vec::new();

    if show_progress {
        eprintln!(
            "Braintrust will fetch the latest setup instructions and run your coding agent to install and validate the SDK."
        );
        eprintln!("No reusable Braintrust agent skills will be installed unless you ask for them.");
        eprintln!();
    }

    let setup_tempdir = tempfile::Builder::new()
        .prefix("bt-setup-")
        .tempdir()
        .context("failed to create temporary setup directory")?;
    let docs_output_dir = setup_tempdir.path().join("docs");
    let task_path = setup_tempdir.path().join("AGENT_TASK.instrument.md");
    let docs_workflows = if selected_workflows.is_empty() {
        vec![WorkflowArg::Instrument]
    } else {
        selected_workflows.clone()
    };

    let docs_args = docs::DocsFetchArgs {
        llms_url: docs::DEFAULT_DOCS_LLMS_URL.to_string(),
        output_dir: docs_output_dir.clone(),
        workflows: docs_workflows.clone(),
        dry_run: false,
        strict: true,
        refresh: true,
        workers: args.workers,
    };
    let docs_fetch_result = if show_progress {
        with_spinner(
            "Fetching latest Braintrust setup docs…",
            docs::fetch_docs_pages(&docs_args, &docs_workflows),
        )
        .await
    } else {
        docs::fetch_docs_pages(&docs_args, &docs_workflows).await
    }
    .map_err(|err| {
        anyhow!(
            "Couldn’t fetch the latest Braintrust setup docs. Check your network connection and try again.\n\n{err:#}"
        )
    })?;
    if docs_fetch_result.failed > 0 {
        bail!(
            "Couldn’t fetch the latest Braintrust setup docs. Check your network connection and try again."
        );
    }
    notes.push(format!(
        "Fetched latest Braintrust setup docs ({} page{}).",
        docs_fetch_result.written,
        if docs_fetch_result.written == 1 {
            ""
        } else {
            "s"
        }
    ));
    warnings.extend(docs_fetch_result.warnings);

    sdk_install_docs::write_sdk_install_docs(&docs_output_dir)?;

    // Determine run mode: interactive TUI vs background (autonomous).
    // Use prompt availability rather than stdin TTY state so `/dev/tty`
    // fallbacks still allow TUI launch when bt is invoked via a shell script.
    let (run_interactive, bypass_permissions) = if args.prompt_for_missing_options {
        prompt_instrument_run_mode(&args)?
    } else {
        resolve_instrument_run_mode(&args, ui::can_prompt())
    };

    write_text_file(
        &task_path,
        &render_instrument_task(
            &docs_output_dir,
            &selected_workflows,
            &selected_languages,
            run_interactive,
        ),
    )?;

    notes.push("Instrumentation task prompt prepared in a temporary directory.".to_string());

    let invocation = resolve_instrument_invocation(
        selected,
        args.agent_cmd.as_deref(),
        &task_path,
        run_interactive,
        bypass_permissions,
        &selected_languages,
    )?;
    if run_interactive && base.verbose {
        eprintln!();
        eprintln!("{} is opening in interactive mode.", selected.as_str());
        eprintln!("The instrumentation task is pre-loaded. Press Enter to begin.");
        eprintln!("Setup context is temporary and will be removed after the run.");
        eprintln!();
    }

    let show_output = !base.json && (run_interactive || base.verbose);
    let status = if !run_interactive && !base.json && !base.verbose {
        with_spinner(
            "Running agent instrumentation…",
            run_agent_invocation(&root, &invocation, false, &[]),
        )
        .await?
    } else {
        run_agent_invocation(&root, &invocation, show_output, &[]).await?
    };
    if status.success() {
        results.push(AgentInstallResult {
            agent: selected,
            status: InstallStatus::Installed,
            message: "agent instrumentation command completed".to_string(),
            paths: Vec::new(),
        });
    } else {
        results.push(AgentInstallResult {
            agent: selected,
            status: InstallStatus::Failed,
            message: format!("agent command exited with status {status}"),
            paths: Vec::new(),
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
    } else if base.verbose {
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
        if !base.json {
            eprintln!();
            eprintln!("Setup stopped during validation.");
            eprintln!("No Braintrust setup files were left in your repo.");
            eprintln!("Please fix the issue above and rerun `bt setup`.");
        }
        bail!("agent instrumentation command failed");
    }

    if !base.json {
        eprintln!();
        eprintln!("{} Braintrust SDK setup completed.", style("✓").green());
        if offer_skills_after_success && setup_can_prompt(&base) && !args.yes {
            let term = ui::prompt_term().ok_or_else(|| anyhow!("interactive mode requires TTY"))?;
            let install_skills = Confirm::with_theme(&ColorfulTheme::default())
                .with_prompt(
                    "Install reusable Braintrust coding-agent skills for future Braintrust work?",
                )
                .default(false)
                .interact_on(&term)?;
            if install_skills {
                install_reusable_skills_after_setup(&base, selected).await?;
            } else {
                eprintln!("You can install them later with `bt setup skills`.");
            }
        } else if offer_skills_after_success {
            eprintln!("Want reusable Braintrust coding-agent skills for future work? Run `bt setup skills`.");
        }
    }
    Ok(())
}

async fn install_reusable_skills_after_setup(base: &BaseArgs, selected: Agent) -> Result<()> {
    let args = AgentsSetupArgs {
        agent: Some(map_agent_to_agent_arg(selected)),
        local: false,
        global: true,
        workflows: Vec::new(),
        no_workflow: false,
        yes: true,
        refresh_docs: false,
        workers: crate::sync::default_workers(),
        permissions: InstrumentPermissionArgs { yolo: false },
    };
    let outcome = execute_skills_setup(base, &args, false).await?;
    if base.verbose {
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
        eprintln!(
            "{} Could not install reusable skills. You can retry with `bt setup skills`.",
            style("!").yellow()
        );
    } else if !base.verbose {
        eprintln!("Installed reusable Braintrust coding-agent skills.");
    }
    Ok(())
}

fn resolve_instrument_workflow_selection(
    args: &InstrumentSetupArgs,
    hint_pending: &mut bool,
) -> Result<Vec<WorkflowArg>> {
    if args.no_workflow {
        return Ok(Vec::new());
    }

    if !args.workflows.is_empty() {
        let mut selected = resolve_workflow_selection(&args.workflows);
        if !selected.contains(&WorkflowArg::Instrument) {
            selected.push(WorkflowArg::Instrument);
            selected.sort();
            selected.dedup();
        }
        return Ok(selected);
    }

    if args.prompt_for_missing_options && ui::can_prompt() && !args.yes {
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

    Ok(resolve_workflow_selection(&[]))
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

fn resolve_prompted_instrument_workflows(mut workflows: Vec<WorkflowArg>) -> Vec<WorkflowArg> {
    if workflows.is_empty() {
        return workflows;
    }
    if !workflows.contains(&WorkflowArg::Instrument) {
        workflows.push(WorkflowArg::Instrument);
    }
    workflows.sort();
    workflows.dedup();
    workflows
}

fn map_agent_to_agent_arg(agent: Agent) -> AgentArg {
    match agent {
        Agent::Claude => AgentArg::Claude,
        Agent::Codex => AgentArg::Codex,
        Agent::Copilot => AgentArg::Copilot,
        Agent::Cursor => AgentArg::Cursor,
        Agent::Gemini => AgentArg::Gemini,
        Agent::Opencode => AgentArg::Opencode,
        Agent::Qwen => AgentArg::Qwen,
    }
}

fn map_agent_to_instrument_agent_arg(agent: Agent) -> InstrumentAgentArg {
    match agent {
        Agent::Claude => InstrumentAgentArg::Claude,
        Agent::Codex => InstrumentAgentArg::Codex,
        Agent::Copilot => InstrumentAgentArg::Copilot,
        Agent::Cursor => InstrumentAgentArg::Cursor,
        Agent::Gemini => InstrumentAgentArg::Gemini,
        Agent::Opencode => InstrumentAgentArg::Opencode,
        Agent::Qwen => InstrumentAgentArg::Qwen,
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
        Agent::Codex
        | Agent::Copilot
        | Agent::Opencode
        | Agent::Cursor
        | Agent::Gemini
        | Agent::Qwen => root.join(".agents/skills/braintrust/SKILL.md"),
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
        Agent::Codex => {
            let mut codex_args = vec![];
            if bypass_permissions {
                codex_args.push("--dangerously-bypass-approvals-and-sandbox".to_string());
            }
            if interactive {
                InstrumentInvocation::Program {
                    program: "codex".to_string(),
                    args: codex_args,
                    stdin_file: None,
                    prompt_file_arg: Some(task_path.to_path_buf()),
                    initial_prompt: None,
                    stream_json: false,
                    interactive: true,
                }
            } else {
                codex_args.extend(["exec".to_string(), "-".to_string()]);
                InstrumentInvocation::Program {
                    program: "codex".to_string(),
                    args: codex_args,
                    stdin_file: Some(task_path.to_path_buf()),
                    prompt_file_arg: None,
                    initial_prompt: None,
                    stream_json: false,
                    interactive: false,
                }
            }
        }
        Agent::Claude => {
            if interactive {
                let permission_mode = if bypass_permissions {
                    "bypassPermissions"
                } else {
                    "acceptEdits"
                };
                InstrumentInvocation::Program {
                    program: "claude".to_string(),
                    args: vec![
                        "--permission-mode".to_string(),
                        permission_mode.to_string(),
                        "--settings".to_string(),
                        format!(r#"{{"defaultMode": "{permission_mode}"}}"#),
                        "--disallowedTools".to_string(),
                        "ExitPlanMode,EnterPlanMode".to_string(),
                        "--name".to_string(),
                        "Braintrust: Instrument".to_string(),
                    ],
                    stdin_file: None,
                    prompt_file_arg: Some(task_path.to_path_buf()),
                    initial_prompt: None,
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
        Agent::Gemini => {
            let mut gemini_args = vec![];
            if bypass_permissions {
                gemini_args.push("--yolo".to_string());
            }
            if interactive {
                InstrumentInvocation::Program {
                    program: "gemini".to_string(),
                    args: gemini_args,
                    stdin_file: None,
                    prompt_file_arg: Some(task_path.to_path_buf()),
                    initial_prompt: None,
                    stream_json: false,
                    interactive: true,
                }
            } else {
                gemini_args.extend([
                    "-p".to_string(),
                    String::new(),
                    "--output-format".to_string(),
                    "stream-json".to_string(),
                ]);
                InstrumentInvocation::Program {
                    program: "gemini".to_string(),
                    args: gemini_args,
                    stdin_file: Some(task_path.to_path_buf()),
                    prompt_file_arg: None,
                    initial_prompt: None,
                    stream_json: true,
                    interactive: false,
                }
            }
        }
        Agent::Cursor => {
            let mut cursor_args = vec![];
            if bypass_permissions {
                cursor_args.push("--yolo".to_string());
            }
            if interactive {
                InstrumentInvocation::Program {
                    program: "cursor-agent".to_string(),
                    args: cursor_args,
                    stdin_file: None,
                    prompt_file_arg: Some(task_path.to_path_buf()),
                    initial_prompt: None,
                    stream_json: false,
                    interactive: true,
                }
            } else {
                cursor_args.extend([
                    "-p".to_string(),
                    "--output-format".to_string(),
                    "stream-json".to_string(),
                ]);
                InstrumentInvocation::Program {
                    program: "cursor-agent".to_string(),
                    args: cursor_args,
                    stdin_file: None,
                    prompt_file_arg: Some(task_path.to_path_buf()),
                    initial_prompt: None,
                    stream_json: true,
                    interactive: false,
                }
            }
        }
        Agent::Qwen => {
            let mut qwen_args = vec![];
            if bypass_permissions {
                qwen_args.push("--yolo".to_string());
            }
            if interactive {
                qwen_args.push("-i".to_string());
                InstrumentInvocation::Program {
                    program: "qwen".to_string(),
                    args: qwen_args,
                    stdin_file: None,
                    prompt_file_arg: Some(task_path.to_path_buf()),
                    initial_prompt: None,
                    stream_json: false,
                    interactive: true,
                }
            } else {
                qwen_args.extend([
                    "-p".to_string(),
                    "--output-format".to_string(),
                    "stream-json".to_string(),
                    "--include-partial-messages".to_string(),
                ]);
                InstrumentInvocation::Program {
                    program: "qwen".to_string(),
                    args: qwen_args,
                    stdin_file: None,
                    prompt_file_arg: Some(task_path.to_path_buf()),
                    initial_prompt: None,
                    stream_json: true,
                    interactive: false,
                }
            }
        }
        Agent::Copilot => {
            let mut copilot_args = vec![];
            if bypass_permissions {
                copilot_args.push("--yolo".to_string());
            }
            if interactive {
                copilot_args.push("-i".to_string());
                InstrumentInvocation::Program {
                    program: "copilot".to_string(),
                    args: copilot_args,
                    stdin_file: None,
                    prompt_file_arg: Some(task_path.to_path_buf()),
                    initial_prompt: None,
                    stream_json: false,
                    interactive: true,
                }
            } else {
                copilot_args.extend([
                    "--no-ask-user".to_string(),
                    "--stream".to_string(),
                    "on".to_string(),
                    "-s".to_string(),
                    "-p".to_string(),
                ]);
                InstrumentInvocation::Program {
                    program: "copilot".to_string(),
                    args: copilot_args,
                    stdin_file: None,
                    prompt_file_arg: Some(task_path.to_path_buf()),
                    initial_prompt: None,
                    stream_json: false,
                    interactive: false,
                }
            }
        }
    };
    Ok(invocation)
}

async fn run_agent_invocation(
    root: &Path,
    invocation: &InstrumentInvocation,
    show_output: bool,
    extra_env: &[(String, String)],
) -> Result<std::process::ExitStatus> {
    match invocation {
        InstrumentInvocation::Shell(command_text) => {
            let mut command = Command::new("bash");
            command.arg("-lc").arg(command_text);
            command.current_dir(root);
            for (key, value) in extra_env {
                command.env(key, value);
            }
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
            for (key, value) in extra_env {
                command.env(key, value);
            }
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
                #[cfg(unix)]
                if !ui::is_interactive() {
                    if let Ok(tty) = fs::File::open("/dev/tty") {
                        command.stdin(Stdio::from(tty));
                    }
                }
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

fn resolve_instrument_run_mode(args: &InstrumentSetupArgs, prompt_available: bool) -> (bool, bool) {
    if args.tui {
        (true, args.permissions.yolo)
    } else if args.background || !prompt_available {
        (false, args.permissions.yolo)
    } else {
        (true, args.permissions.yolo)
    }
}

fn prompt_instrument_run_mode(args: &InstrumentSetupArgs) -> Result<(bool, bool)> {
    let term = ui::prompt_term().ok_or_else(|| anyhow!("interactive mode requires TTY"))?;
    let run_interactive = if args.tui {
        true
    } else if args.background {
        false
    } else {
        Select::with_theme(&ColorfulTheme::default())
            .with_prompt("How do you want to run the agent?")
            .items(&["Interactive (TUI)", "Background"])
            .default(0)
            .interact_on(&term)?
            == 0
    };
    let bypass_permissions = if args.permissions.yolo {
        true
    } else {
        Confirm::with_theme(&ColorfulTheme::default())
            .with_prompt("Grant agent full permissions? (bypass permission prompts)")
            .default(false)
            .interact_on(&term)?
    };
    Ok((run_interactive, bypass_permissions))
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
        "### 2. Detect Language\n\n\
         **Instrument exactly one language/service per install run.** Do not install Braintrust for multiple languages or multiple services in the same run, even if the repo contains more than one. If more than one candidate exists, stop and ask the user which single service to instrument before doing anything else.\n\n\
         Determine the project language using concrete signals:\n\n\
         - `package.json` -> TypeScript\n\
         - `requirements.txt`, `setup.py` or `pyproject.toml` -> Python\n\
         - `pom.xml` or `build.gradle` -> Java\n\
         - `go.mod` -> Go\n\
         - `Gemfile` -> Ruby\n\
         - `.csproj` -> C#\n\n\
         **If exactly one of these matches at the repo root and there is no ambiguity, proceed with that language.**\n\n\
         In every other case, **stop and ask the user** before continuing. Do not guess, do not pick the \"most likely\" language, and do not instrument more than one. Cases that require asking the user:\n\n\
         - **No standard build/dependency file is present.** Do not infer from loose hints (file extensions, a stray script, a README mention). Ask the user which language/service to instrument.\n\
         - **More than one of the above files is present** (polyglot repo, monorepo, or a mixed service). Ask the user which single service to instrument, and where it lives in the repo.\n\
         - **A workspace/monorepo with multiple sub-projects of the same language** (e.g., several `package.json` or several `go.mod` files). Ask the user which sub-project to instrument.\n\
         - **The inferred language is not in the supported list above.** Do not fall back to a different language or a generic OpenTelemetry setup -- **abort the install** and tell the user which languages are supported.\n\
         - **Any other ambiguity** about which app/service the user wants traced. Ask.\n\n\
         When you ask, be specific: list the candidate services/paths/languages you found and have the user pick exactly one. Then state the single strongest piece of evidence (the file they pointed at) before moving on.\n\n\
         Do not proceed to Step 3 until exactly one language and one service have been confirmed."
            .to_string()
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
            "### 2. Language\n\n\
             The target language has been specified: {}.",
            list
        )
    };
    let install_sdk_requirements = "- Install the latest Braintrust SDK via the language's package manager. Do not hard-pin the SDK version unless the user asks. Build-time dependencies called out by the language-specific resource (e.g. Orchestrion for Go) must still be pinned to an exact version.\n\
         - Modify only dependency files, a minimal application entry point (e.g., main/bootstrap), and any existing build/run scripts or checked-in env/config that must change to keep auto-instrumentation active in normal use. \
         Auto-instrument the app (except for Java and C# which don't support auto-instrumentation).\n\
         - Do not change unrelated code.";
    let install_sdk_context = if unique_langs.is_empty() {
        format!(
            "### 3. Install SDK (Language-Specific)\n\n\
             Read the install guide for the detected language from the local docs:\n\n\
             | Language   | Local doc                         |\n\
             | ---------- | --------------------------------- |\n\
             | Java       | `{{SDK_INSTALL_DIR}}/java.md`       |\n\
             | TypeScript | `{{SDK_INSTALL_DIR}}/typescript.md` |\n\
             | Python     | `{{SDK_INSTALL_DIR}}/python.md`     |\n\
             | Go         | `{{SDK_INSTALL_DIR}}/go.md`         |\n\
             | Ruby       | `{{SDK_INSTALL_DIR}}/ruby.md`       |\n\
             | C#         | `{{SDK_INSTALL_DIR}}/csharp.md`     |\n\n\
             Requirements:\n\n\
             {install_sdk_requirements}"
        )
    } else if unique_langs.len() == 1 {
        let lang = *unique_langs.iter().next().unwrap();
        format!(
            "### 3. Install SDK\n\n\
             Read the install guide from the local docs: `{{SDK_INSTALL_DIR}}/{}`\n\n\
             Requirements:\n\n\
             {install_sdk_requirements}",
            lang.doc_filename()
        )
    } else {
        let rows: String = unique_langs
            .iter()
            .map(|l| {
                format!(
                    "| {} | `{{SDK_INSTALL_DIR}}/{}` |\n",
                    l.display_name(),
                    l.doc_filename()
                )
            })
            .collect();
        format!(
            "### 3. Install SDK\n\n\
             Read the install guide for each language from the local docs:\n\n\
             | Language | Local doc |\n\
             | -------- | --------- |\n\
             {rows}\n\
             Requirements:\n\n\
             {install_sdk_requirements}"
        )
    };

    let workflow_context = if workflows
        .iter()
        .any(|w| !matches!(w, WorkflowArg::Instrument))
    {
        format!(
            "## Latest Braintrust Setup Docs\n\n\
             Latest Braintrust setup and workflow docs were fetched into `{}` for this one setup run. \
             Use those docs for Braintrust SDK, observe, annotate, evaluate, or deploy guidance. \
             When verifying data in Braintrust, prefer local `bt` CLI commands over direct \
             API calls. Do not rely on the Braintrust MCP server for data queries.\n",
            docs_output_dir.display()
        )
    } else {
        format!(
            "## Latest Braintrust Setup Docs\n\n\
             Latest Braintrust setup docs were fetched into `{}` for this one setup run. \
             Use them as the source of truth for Braintrust setup behavior.\n",
            docs_output_dir.display()
        )
    };

    let run_mode_context = if interactive {
        "- **Interactive mode:** You can ask the user questions through the chat interface.\n"
    } else {
        "- **Non-interactive mode:** You cannot ask the user questions. \
         If a step requires user input (e.g., ambiguous language in a polyglot repo, \
         unknown run command), abort with a clear explanation of what is needed.\n"
    };

    INSTRUMENT_TASK_TEMPLATE
        .replace("{LANGUAGE_CONTEXT}", &language_context)
        .replace("{INSTALL_SDK_CONTEXT}", &install_sdk_context)
        .replace("{WORKFLOW_CONTEXT}", &workflow_context)
        .replace("{RUN_MODE_CONTEXT}", run_mode_context)
        .replace("{SDK_INSTALL_DIR}", &sdk_install_dir.display().to_string())
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
    api_key: &str,
    mcp_url: &str,
) -> McpSetupOutcome {
    let mut warnings = Vec::new();
    let mut results = Vec::new();

    for agent in agents.iter().copied() {
        let result = install_mcp_for_agent(agent, scope, local_root, home, api_key, mcp_url);
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

async fn run_mcp_setup(mut base: BaseArgs, args: AgentsMcpSetupArgs) -> Result<()> {
    let home = home_dir().ok_or_else(|| anyhow!("failed to resolve HOME/USERPROFILE"))?;
    let selection = resolve_mcp_selection(&args, &home)?;
    let scope = selection.scope;
    let local_root = selection.local_root;
    let detected = selection.detected;
    let selected_agents = selection.selected_agents;

    let auth = ensure_setup_auth(&mut base, false, true).await?;
    let api_url = auth.client.url("");
    let outcome = execute_mcp_install(
        scope,
        local_root.as_deref(),
        &home,
        &selected_agents,
        &auth.api_key,
        &mcp_url_from_api_url(&api_url),
    );

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
    } else if base.verbose {
        print_mcp_human_report(scope, &selected_agents, &outcome.results, &outcome.warnings);
    }

    if outcome.installed_count == 0 {
        bail!("no MCP configurations were installed successfully");
    }

    Ok(())
}

fn resolve_setup_selection(args: &AgentsSetupArgs, home: &Path) -> Result<SetupSelection> {
    let mut scope = initial_scope(args.local, args.global, args.yes, YesScopeDefault::Global);
    let interactive = ui::can_prompt() && !args.yes;
    let mut prompted_workflows: Option<Vec<WorkflowArg>> = if args.no_workflow {
        Some(Vec::new())
    } else {
        None
    };

    if interactive {
        #[derive(Clone, Copy)]
        enum SetupWizardStep {
            Scope,
            Workflows,
        }

        let mut steps = Vec::new();
        if scope.is_none() {
            steps.push(SetupWizardStep::Scope);
        }
        if !args.no_workflow && args.workflows.is_empty() {
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
    let selected_agent = resolve_default_agent_selection(
        args.agent,
        &detected,
        "Select agent to configure",
        interactive,
    )?;
    let selected_agents = vec![selected_agent];

    let selected_workflows = if args.no_workflow {
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
    let interactive = ui::can_prompt() && !args.yes;

    if interactive {
        #[derive(Clone, Copy)]
        enum McpWizardStep {
            Scope,
        }

        let mut steps = Vec::new();
        if scope.is_none() {
            steps.push(McpWizardStep::Scope);
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
    let selected_agent = resolve_default_agent_selection(
        args.agent,
        &detected,
        "Select agent to configure MCP for",
        interactive,
    )?;
    let selected_agents = vec![selected_agent];

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
    let agents = [
        Agent::Claude,
        Agent::Codex,
        Agent::Copilot,
        Agent::Cursor,
        Agent::Gemini,
        Agent::Opencode,
        Agent::Qwen,
    ]
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

    let config_path = skill_config_path(agent, scope, local_root, home)
        .ok()
        .map(|path| path.display().to_string());

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
    let term = ui::prompt_term().ok_or_else(|| anyhow!("interactive mode requires TTY"))?;
    let idx = Select::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(&choices)
        .default(1)
        .interact_on_opt(&term)?;
    Ok(idx.map(|i| {
        if i == 0 {
            InstallScope::Local
        } else {
            InstallScope::Global
        }
    }))
}

fn prompt_agent_selection(
    prompt: &str,
    candidates: &[Agent],
    default: Agent,
) -> Result<Option<Agent>> {
    let labels = candidates
        .iter()
        .map(|agent| agent.as_str())
        .collect::<Vec<_>>();
    let default_index = candidates
        .iter()
        .position(|agent| *agent == default)
        .unwrap_or(0);
    let term = ui::prompt_term().ok_or_else(|| anyhow!("interactive mode requires TTY"))?;
    let selected = FuzzySelect::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(&labels)
        .default(default_index)
        .interact_on_opt(&term)?;
    Ok(selected.map(|index| candidates[index]))
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

    let term = ui::prompt_term().ok_or_else(|| anyhow!("interactive mode requires TTY"))?;
    let selected = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt(
            "Select the workflows you are interested in (will prefetch docs for them) (Esc: back)",
        )
        .items(&labels)
        .defaults(&default_flags)
        .interact_on_opt(&term)?;

    Ok(selected.map(|indexes| {
        indexes
            .into_iter()
            .map(|index| ALL_WORKFLOWS[index])
            .collect::<Vec<_>>()
    }))
}

fn prompt_instrument_workflow_selection() -> Result<Option<Vec<WorkflowArg>>> {
    let choices = ["observe", "annotate", "evaluate", "deploy"];
    let defaults = [true, false, true, false];
    let term = ui::prompt_term().ok_or_else(|| anyhow!("interactive mode requires TTY"))?;
    let selected = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Select additional workflow docs to prefetch (instrument is always included)")
        .items(&choices)
        .defaults(&defaults)
        .interact_on_opt(&term)?;

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

fn prompt_instrument_language_selection(
    defaults: &[LanguageArg],
) -> Result<Option<Vec<LanguageArg>>> {
    let choices = [
        "All languages (auto-detect)",
        "Python",
        "TypeScript / JavaScript",
        "Go",
        "Java",
        "Ruby",
        "C#",
    ];
    let lang_to_idx = |lang: LanguageArg| match lang {
        LanguageArg::Python => 1usize,
        LanguageArg::TypeScript => 2,
        LanguageArg::Go => 3,
        LanguageArg::Java => 4,
        LanguageArg::Ruby => 5,
        LanguageArg::CSharp => 6,
    };
    let defaults = if defaults.len() != 1 {
        [true, false, false, false, false, false, false]
    } else {
        let mut d = [false; 7];
        d[lang_to_idx(defaults[0])] = true;
        d
    };

    let term = ui::prompt_term().ok_or_else(|| anyhow!("interactive mode requires TTY"))?;
    let selected = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Which language(s) to instrument?")
        .items(&choices)
        .defaults(&defaults)
        .interact_on_opt(&term)?;

    Ok(selected.map(|indexes| {
        if indexes.is_empty() || indexes.contains(&0) {
            return Vec::new();
        }
        indexes
            .into_iter()
            .filter_map(|index| match index {
                1 => Some(LanguageArg::Python),
                2 => Some(LanguageArg::TypeScript),
                3 => Some(LanguageArg::Go),
                4 => Some(LanguageArg::Java),
                5 => Some(LanguageArg::Ruby),
                6 => Some(LanguageArg::CSharp),
                _ => None,
            })
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

    if !ui::can_prompt() {
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

fn resolve_default_agent_selection(
    requested: Option<AgentArg>,
    detected: &[DetectionSignal],
    prompt: &str,
    allow_prompt: bool,
) -> Result<Agent> {
    if let Some(arg) = requested {
        return Ok(map_agent_arg(arg));
    }

    let path_agents = detected_agents_on_path(detected);
    if path_agents.len() == 1 {
        return Ok(path_agents[0]);
    }

    if allow_prompt {
        let candidates = promptable_instrument_agents(&path_agents, detected);
        let default = pick_agent_mode_target(&candidates).unwrap_or(Agent::Codex);
        return prompt_agent_selection(prompt, &candidates, default)?
            .ok_or_else(|| anyhow!("setup cancelled by user"));
    }

    if path_agents.is_empty() {
        bail!("no coding agents detected on PATH; pass --agent <AGENT> to try anyway");
    }

    bail!("multiple coding agents available; pass --agent <AGENT> or re-run in an interactive terminal");
}

fn map_instrument_agent_arg_to_agent_arg(agent: InstrumentAgentArg) -> AgentArg {
    match agent {
        InstrumentAgentArg::Claude => AgentArg::Claude,
        InstrumentAgentArg::Codex => AgentArg::Codex,
        InstrumentAgentArg::Copilot => AgentArg::Copilot,
        InstrumentAgentArg::Cursor => AgentArg::Cursor,
        InstrumentAgentArg::Gemini => AgentArg::Gemini,
        InstrumentAgentArg::Opencode => AgentArg::Opencode,
        InstrumentAgentArg::Qwen => AgentArg::Qwen,
    }
}

fn map_agent_arg(agent: AgentArg) -> Agent {
    match agent {
        AgentArg::Claude => Agent::Claude,
        AgentArg::Codex => Agent::Codex,
        AgentArg::Copilot => Agent::Copilot,
        AgentArg::Cursor => Agent::Cursor,
        AgentArg::Gemini => Agent::Gemini,
        AgentArg::Opencode => Agent::Opencode,
        AgentArg::Qwen => Agent::Qwen,
    }
}

fn pick_agent_mode_target(candidates: &[Agent]) -> Option<Agent> {
    if candidates.is_empty() {
        return None;
    }
    // Prefer Codex for instrumentation defaults when multiple agents are detected.
    let priority = [
        Agent::Codex,
        Agent::Claude,
        Agent::Gemini,
        Agent::Qwen,
        Agent::Copilot,
        Agent::Cursor,
        Agent::Opencode,
    ];
    for preferred in priority {
        if candidates.contains(&preferred) {
            return Some(preferred);
        }
    }
    candidates.first().copied()
}

fn determine_wizard_instrument_agent(flag_agent: Option<AgentArg>) -> Option<InstrumentAgentArg> {
    if let Some(arg) = flag_agent {
        return Some(map_agent_to_instrument_agent_arg(map_agent_arg(arg)));
    }

    let runnable_agents = detect_runnable_agents();
    if runnable_agents.len() == 1 {
        return Some(map_agent_to_instrument_agent_arg(runnable_agents[0]));
    }
    None
}

fn promptable_instrument_agents(
    runnable_agents: &[Agent],
    detected: &[DetectionSignal],
) -> Vec<Agent> {
    if !runnable_agents.is_empty() {
        return runnable_agents.to_vec();
    }

    let detected_set: BTreeSet<Agent> = detected.iter().map(|signal| signal.agent).collect();
    if detected_set.is_empty() {
        return ALL_AGENTS.to_vec();
    }

    detected_set.into_iter().collect()
}

fn detected_agents_on_path(detected: &[DetectionSignal]) -> Vec<Agent> {
    let mut agents = BTreeSet::new();
    for signal in detected {
        if signal.on_path {
            agents.insert(signal.agent);
        }
    }
    agents.into_iter().collect()
}

fn detect_runnable_agents() -> Vec<Agent> {
    ALL_AGENTS
        .iter()
        .copied()
        .filter(|agent| command_exists(agent.metadata().binary))
        .collect()
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
    let mut by_agent: BTreeMap<Agent, BTreeSet<(bool, String)>> = BTreeMap::new();

    if let Some(root) = local_root {
        for agent in ALL_AGENTS {
            if let Some(marker) = agent.metadata().repo_marker {
                if root.join(marker).exists() {
                    add_signal(
                        &mut by_agent,
                        agent,
                        false,
                        &format!("{marker} exists in repo root"),
                    );
                }
            }
        }
        if root.join(".agents").exists() || root.join(".agents/skills").exists() {
            add_signal(
                &mut by_agent,
                Agent::Codex,
                false,
                ".agents/skills exists in repo root",
            );
            add_signal(
                &mut by_agent,
                Agent::Opencode,
                false,
                ".agents/skills exists in repo root",
            );
        }
        if root.join("AGENTS.md").exists() {
            add_signal(
                &mut by_agent,
                Agent::Codex,
                false,
                "AGENTS.md exists in repo root",
            );
        }
    }

    for agent in ALL_AGENTS {
        for marker in agent.metadata().home_markers {
            if home.join(marker).exists() {
                add_signal(&mut by_agent, agent, false, &format!("~/{} exists", marker));
            }
        }
    }
    if home.join(".agents/skills").exists() {
        add_signal(
            &mut by_agent,
            Agent::Codex,
            false,
            "~/.agents/skills exists",
        );
        add_signal(
            &mut by_agent,
            Agent::Opencode,
            false,
            "~/.agents/skills exists",
        );
    }
    if home.join(".opencode").exists() || home.join(".config/opencode").exists() {
        add_signal(
            &mut by_agent,
            Agent::Opencode,
            false,
            "opencode config directory exists",
        );
    }

    for agent in ALL_AGENTS {
        let binary = agent.metadata().binary;
        if command_exists(binary) {
            add_signal(
                &mut by_agent,
                agent,
                true,
                &format!("`{binary}` binary found in PATH"),
            );
        }
    }

    let mut out = Vec::new();
    for (agent, signals) in by_agent {
        for (on_path, reason) in signals {
            out.push(DetectionSignal {
                agent,
                on_path,
                reason,
            });
        }
    }
    out
}

fn add_signal(
    map: &mut BTreeMap<Agent, BTreeSet<(bool, String)>>,
    agent: Agent,
    on_path: bool,
    reason: &str,
) {
    map.entry(agent)
        .or_default()
        .insert((on_path, reason.to_string()));
}

fn install_agent_skill(
    agent: Agent,
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
    alias_dir: Option<&str>,
) -> Result<AgentInstallResult> {
    let root = scope_root(scope, local_root, home)?;
    let skill_content = render_braintrust_skill();
    let (canonical_changed, skill_path) = install_canonical_skill(root, &skill_content)?;
    let mut changed = canonical_changed;
    let mut paths = vec![skill_path.display().to_string()];

    if let Some(alias_dir) = alias_dir {
        let alias = ensure_agent_skills_alias(root, alias_dir, &skill_content)?;
        changed |= alias.changed;
        paths.push(alias.path.display().to_string());
    }

    Ok(AgentInstallResult {
        agent,
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
        paths,
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
    api_key: &str,
    mcp_url: &str,
) -> Result<AgentInstallResult> {
    match agent {
        Agent::Claude => install_mcp_for_claude(scope, local_root, home, mcp_url, api_key),
        Agent::Codex => match scope {
            InstallScope::Local => install_mcp_for_codex_local(local_root, home, api_key, mcp_url),
            InstallScope::Global => install_mcp_for_codex(mcp_url, api_key),
        },
        Agent::Cursor => install_mcp_for_cursor(scope, local_root, home, api_key, mcp_url),
        Agent::Copilot => install_mcp_for_copilot(scope, local_root, home, api_key, mcp_url),
        Agent::Gemini => install_mcp_for_gemini(scope, local_root, home, api_key, mcp_url),
        Agent::Opencode => install_mcp_for_opencode(scope, local_root, home, api_key, mcp_url),
        Agent::Qwen => install_mcp_for_qwen(scope, local_root, home, api_key, mcp_url),
    }
}

fn install_mcp_for_codex(mcp_url: &str, api_key: &str) -> Result<AgentInstallResult> {
    let status = std::process::Command::new("codex")
        .args([
            "mcp",
            "add",
            "braintrust",
            "--url",
            mcp_url,
            "--bearer-token-env-var",
            "BRAINTRUST_API_KEY",
        ])
        .env("BRAINTRUST_API_KEY", api_key)
        .stdout(Stdio::null())
        .status()
        .context("failed to run `codex mcp add`")?;

    if !status.success() {
        bail!("`codex mcp add` exited with status {status}");
    }

    Ok(AgentInstallResult {
        agent: Agent::Codex,
        status: InstallStatus::Installed,
        message: "installed MCP config".to_string(),
        paths: vec!["codex:braintrust".to_string()],
    })
}

fn install_mcp_for_codex_local(
    local_root: Option<&Path>,
    home: &Path,
    api_key: &str,
    mcp_url: &str,
) -> Result<AgentInstallResult> {
    let root = scope_root(InstallScope::Local, local_root, home)?;
    let path = root.join(".codex/config.toml");
    install_mcp_config_file(Agent::Codex, path, "installed MCP config", |path| {
        merge_codex_config(path, api_key, mcp_url)
    })
}

fn install_mcp_for_cursor(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
    api_key: &str,
    mcp_url: &str,
) -> Result<AgentInstallResult> {
    let path = match scope {
        InstallScope::Local => scope_root(scope, local_root, home)?.join(".cursor/mcp.json"),
        InstallScope::Global => home.join(".cursor/mcp.json"),
    };
    install_mcp_config_file(
        Agent::Cursor,
        path,
        "installed MCP config and enabled server",
        |path| {
            merge_mcp_config(path, api_key, mcp_url)?;
            enable_cursor_mcp(local_root)
        },
    )
}

fn install_mcp_config_file<F>(
    agent: Agent,
    path: PathBuf,
    message: &str,
    install: F,
) -> Result<AgentInstallResult>
where
    F: FnOnce(&Path) -> Result<()>,
{
    install(&path)?;

    Ok(AgentInstallResult {
        agent,
        status: InstallStatus::Installed,
        message: message.to_string(),
        paths: vec![path.display().to_string()],
    })
}

fn enable_cursor_mcp(local_root: Option<&Path>) -> Result<()> {
    let binary = if command_exists("cursor-agent") {
        "cursor-agent"
    } else {
        "cursor"
    };
    let cwd = match local_root {
        Some(root) => root.to_path_buf(),
        None => std::env::current_dir().context("failed to resolve current directory")?,
    };
    let status = std::process::Command::new(binary)
        .args(["mcp", "enable", "braintrust"])
        .current_dir(cwd)
        .stdout(Stdio::null())
        .status()
        .with_context(|| format!("failed to run `{binary} mcp enable braintrust`"))?;

    if !status.success() {
        bail!("`{binary} mcp enable braintrust` exited with status {status}");
    }
    Ok(())
}

fn install_mcp_for_claude(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
    mcp_url: &str,
    api_key: &str,
) -> Result<AgentInstallResult> {
    install_mcp_for_http_cli_agent(
        McpHttpCliAgentConfig {
            agent: Agent::Claude,
            binary: "claude",
            header_flag: "--header",
        },
        scope,
        local_root,
        home,
        api_key,
        mcp_url,
    )
}

fn install_mcp_for_gemini(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
    api_key: &str,
    mcp_url: &str,
) -> Result<AgentInstallResult> {
    install_mcp_for_http_cli_agent(
        McpHttpCliAgentConfig {
            agent: Agent::Gemini,
            binary: "gemini",
            header_flag: "-H",
        },
        scope,
        local_root,
        home,
        api_key,
        mcp_url,
    )
}

fn install_mcp_for_qwen(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
    api_key: &str,
    mcp_url: &str,
) -> Result<AgentInstallResult> {
    install_mcp_for_http_cli_agent(
        McpHttpCliAgentConfig {
            agent: Agent::Qwen,
            binary: "qwen",
            header_flag: "-H",
        },
        scope,
        local_root,
        home,
        api_key,
        mcp_url,
    )
}

fn install_mcp_for_copilot(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
    api_key: &str,
    mcp_url: &str,
) -> Result<AgentInstallResult> {
    let path = match scope {
        InstallScope::Local => scope_root(scope, local_root, home)?.join(".mcp.json"),
        InstallScope::Global => home.join(".copilot/mcp-config.json"),
    };
    install_mcp_config_file(Agent::Copilot, path, "installed MCP config", |path| {
        merge_mcp_config(path, api_key, mcp_url)
    })
}

struct McpHttpCliAgentConfig {
    agent: Agent,
    binary: &'static str,
    header_flag: &'static str,
}

fn install_mcp_for_http_cli_agent(
    config: McpHttpCliAgentConfig,
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
    api_key: &str,
    mcp_url: &str,
) -> Result<AgentInstallResult> {
    let (scope_name, cwd) = match scope {
        InstallScope::Local => (
            "project",
            scope_root(scope, local_root, home)?.to_path_buf(),
        ),
        InstallScope::Global => ("user", home.to_path_buf()),
    };

    let status = std::process::Command::new(config.binary)
        .args([
            "mcp",
            "add",
            "-s",
            scope_name,
            "--transport",
            "http",
            "braintrust",
            mcp_url,
            config.header_flag,
            &format!("Authorization: Bearer {api_key}"),
        ])
        .current_dir(&cwd)
        .stdout(Stdio::null())
        .status()
        .with_context(|| format!("failed to run `{} mcp add -s {scope_name}`", config.binary))?;

    if !status.success() {
        bail!(
            "`{} mcp add -s {scope_name}` exited with status {status}",
            config.binary
        );
    }

    Ok(AgentInstallResult {
        agent: config.agent,
        status: InstallStatus::Installed,
        message: "installed MCP config".to_string(),
        paths: vec![format!("{}:{scope_name}", config.agent.as_str())],
    })
}

fn mcp_url_from_api_url(api_url: &str) -> String {
    format!("{}/mcp", api_url.trim_end_matches('/'))
}

fn install_mcp_for_opencode(
    scope: InstallScope,
    local_root: Option<&Path>,
    home: &Path,
    api_key: &str,
    mcp_url: &str,
) -> Result<AgentInstallResult> {
    let path = match scope {
        InstallScope::Local => scope_root(scope, local_root, home)?.join("opencode.json"),
        InstallScope::Global => home.join(".config/opencode/opencode.json"),
    };
    install_mcp_config_file(Agent::Opencode, path, "installed MCP config", |path| {
        merge_opencode_config(path, api_key, mcp_url)
    })
}

fn merge_mcp_config(path: &Path, api_key: &str, mcp_url: &str) -> Result<()> {
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
            "url": mcp_url,
            "headers": {
                "Authorization": format!("Bearer {api_key}")
            }
        }),
    );

    write_json_object(path, &root)
}

fn merge_opencode_config(path: &Path, api_key: &str, mcp_url: &str) -> Result<()> {
    let mut root = load_json_object_or_default(path)?;
    root.entry("$schema".to_string())
        .or_insert_with(|| Value::String("https://opencode.ai/config.json".to_string()));

    let mcp_value = root
        .entry("mcp".to_string())
        .or_insert_with(|| Value::Object(Map::new()));
    let mcp = mcp_value
        .as_object_mut()
        .ok_or_else(|| anyhow!("field 'mcp' in {} must be a JSON object", path.display()))?;

    mcp.insert(
        "braintrust".to_string(),
        serde_json::json!({
            "type": "remote",
            "url": mcp_url,
            "headers": {
                "Authorization": format!("Bearer {api_key}")
            }
        }),
    );

    write_json_object(path, &root)
}

fn merge_codex_config(path: &Path, _api_key: &str, mcp_url: &str) -> Result<()> {
    let mut root = load_toml_table_or_default(path)?;
    let mcp_servers = root
        .entry("mcp_servers".to_string())
        .or_insert_with(|| TomlValue::Table(toml::map::Map::new()));
    let mcp_servers = mcp_servers.as_table_mut().ok_or_else(|| {
        anyhow!(
            "field 'mcp_servers' in {} must be a TOML table",
            path.display()
        )
    })?;

    let braintrust = mcp_servers
        .entry("braintrust".to_string())
        .or_insert_with(|| TomlValue::Table(toml::map::Map::new()));
    let braintrust = braintrust.as_table_mut().ok_or_else(|| {
        anyhow!(
            "field 'mcp_servers.braintrust' in {} must be a TOML table",
            path.display()
        )
    })?;

    braintrust.insert("url".to_string(), TomlValue::String(mcp_url.to_string()));
    braintrust.insert(
        "bearer_token_env_var".to_string(),
        TomlValue::String("BRAINTRUST_API_KEY".to_string()),
    );

    write_toml_table(path, &root)
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

fn load_toml_table_or_default(path: &Path) -> Result<toml::map::Map<String, TomlValue>> {
    if !path.exists() {
        return Ok(toml::map::Map::new());
    }

    let data = fs::read_to_string(path)
        .with_context(|| format!("failed to read TOML file {}", path.display()))?;
    let value: TomlValue = data
        .parse()
        .with_context(|| format!("failed to parse TOML file {}", path.display()))?;

    value
        .as_table()
        .cloned()
        .ok_or_else(|| anyhow!("{} must contain a TOML table", path.display()))
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

fn write_toml_table(path: &Path, table: &toml::map::Map<String, TomlValue>) -> Result<()> {
    let data = toml::to_string_pretty(&TomlValue::Table(table.clone()))
        .with_context(|| format!("failed to serialize TOML for {}", path.display()))?;

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    let tmp = path.with_extension("tmp");
    fs::write(&tmp, format!("{data}\n"))
        .with_context(|| format!("failed to finalize temp TOML file {}", tmp.display()))?;
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
    use std::env;
    use std::ffi::OsString;
    use std::sync::{Mutex, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::sync::Mutex as AsyncMutex;

    fn cwd_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn env_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn async_env_test_lock() -> &'static AsyncMutex<()> {
        static LOCK: OnceLock<AsyncMutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| AsyncMutex::new(()))
    }

    #[cfg(unix)]
    fn write_executable(path: &Path, content: &str) {
        use std::os::unix::fs::PermissionsExt;

        fs::write(path, content).expect("write executable");
        let mut perms = fs::metadata(path).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("chmod");
    }

    fn make_base_args() -> BaseArgs {
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
            ca_cert: None,
            env_file: None,
        }
    }

    fn restore_env_var(key: &str, previous: Option<OsString>) {
        match previous {
            Some(value) => env::set_var(key, value),
            None => env::remove_var(key),
        }
    }

    #[test]
    fn single_path_agent_is_selected_by_default() {
        let detected = vec![DetectionSignal {
            agent: Agent::Codex,
            on_path: true,
            reason: "`codex` binary found in PATH".to_string(),
        }];
        let resolved =
            resolve_default_agent_selection(None, &detected, "Select coding agent", false)
                .expect("resolve default agent");
        assert_eq!(resolved, Agent::Codex);
    }

    #[test]
    fn default_agent_selection_requires_prompt_when_path_is_ambiguous() {
        let detected = vec![
            DetectionSignal {
                agent: Agent::Codex,
                on_path: true,
                reason: "`codex` binary found in PATH".to_string(),
            },
            DetectionSignal {
                agent: Agent::Claude,
                on_path: true,
                reason: "`claude` binary found in PATH".to_string(),
            },
        ];
        let err = resolve_default_agent_selection(None, &detected, "Select coding agent", false)
            .expect_err("ambiguous path detection should fail");
        assert!(err.to_string().contains("pass --agent"));
    }

    #[test]
    fn default_agent_selection_uses_single_path_agent_without_prompt() {
        let detected = vec![DetectionSignal {
            agent: Agent::Cursor,
            on_path: true,
            reason: "`cursor-agent` binary found in PATH".to_string(),
        }];
        let resolved =
            resolve_default_agent_selection(None, &detected, "Select coding agent", true)
                .expect("resolve default agent");
        assert_eq!(resolved, Agent::Cursor);
    }

    #[test]
    fn resolve_profile_name_for_setup_requires_prompt_when_multiple_profiles_exist() {
        let base = make_base_args();
        let profiles = vec![
            auth::ProfileInfo {
                name: "zeta".to_string(),
                org_name: Some("Zeta Org".to_string()),
                user_name: None,
                email: None,
                api_key_hint: None,
            },
            auth::ProfileInfo {
                name: "alpha".to_string(),
                org_name: Some("Alpha Org".to_string()),
                user_name: None,
                email: None,
                api_key_hint: None,
            },
        ];

        let resolved =
            resolve_profile_name_for_setup(&base, &profiles, false).expect("resolve profile");
        assert_eq!(resolved, None);
    }

    #[test]
    fn resolve_profile_name_for_setup_allows_oauth_fallback_when_no_profiles_exist() {
        let base = make_base_args();
        let profiles = Vec::new();

        let resolved =
            resolve_profile_name_for_setup(&base, &profiles, true).expect("resolve profile");
        assert_eq!(resolved, None);
    }

    #[test]
    fn resolve_profile_name_for_setup_errors_for_unknown_explicit_profile() {
        let mut base = make_base_args();
        base.profile = Some("missing".to_string());
        let profiles = vec![auth::ProfileInfo {
            name: "work".to_string(),
            org_name: Some("Acme".to_string()),
            user_name: None,
            email: None,
            api_key_hint: None,
        }];

        let err =
            resolve_profile_name_for_setup(&base, &profiles, false).expect_err("missing profile");
        assert!(err.to_string().contains("profile 'missing' not found"));
    }

    #[test]
    fn oauth_instrumentation_creates_api_key_when_no_env_key_exists() {
        let base = make_base_args();
        assert!(should_create_api_key_for_setup(true, &base, true));
    }

    #[test]
    fn oauth_non_instrumentation_does_not_create_api_key() {
        let base = make_base_args();
        assert!(!should_create_api_key_for_setup(true, &base, false));
    }

    #[test]
    fn oauth_instrumentation_skips_api_key_creation_when_env_key_exists() {
        let mut base = make_base_args();
        base.api_key = Some("env-key".to_string());
        base.api_key_source = Some(ArgValueSource::EnvVariable);
        assert!(!should_create_api_key_for_setup(true, &base, true));
    }

    #[test]
    fn clear_missing_fallback_setup_project_clears_non_explicit_only() {
        let mut base = make_base_args();
        base.project = Some("stale-project".to_string());
        let mut project_name = Some("stale-project".to_string());

        clear_missing_fallback_setup_project(&mut base, &mut project_name, false);

        assert_eq!(base.project, None);
        assert_eq!(project_name, None);

        let mut base = make_base_args();
        base.project = Some("explicit-project".to_string());
        let mut project_name = Some("explicit-project".to_string());

        clear_missing_fallback_setup_project(&mut base, &mut project_name, true);

        assert_eq!(base.project, Some("explicit-project".to_string()));
        assert_eq!(project_name, Some("explicit-project".to_string()));
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

        merge_mcp_config(&path, "test-api-key", "https://api.braintrust.dev/mcp")
            .expect("merge mcp");

        let parsed: Value =
            serde_json::from_str(&fs::read_to_string(&path).expect("read mcp")).expect("json");
        let servers = parsed
            .get("mcpServers")
            .and_then(|v| v.as_object())
            .expect("servers object");
        assert!(servers.contains_key("existing"));
        assert!(servers.contains_key("braintrust"));
        assert_eq!(
            servers["braintrust"]["url"].as_str(),
            Some("https://api.braintrust.dev/mcp")
        );
        assert_eq!(
            servers["braintrust"]["headers"]["Authorization"].as_str(),
            Some("Bearer test-api-key")
        );
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
    fn resolve_setup_selection_honors_no_workflow() {
        let args = AgentsSetupArgs {
            agent: Some(AgentArg::Codex),
            local: false,
            global: true,
            workflows: vec![WorkflowArg::Evaluate],
            no_workflow: true,
            yes: true,
            refresh_docs: false,
            workers: crate::sync::default_workers(),
            permissions: InstrumentPermissionArgs { yolo: false },
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
            no_workflow: false,
            yes: true,
            refresh_docs: false,
            workers: crate::sync::default_workers(),
            languages: Vec::new(),
            tui: false,
            background: false,
            permissions: InstrumentPermissionArgs { yolo: false },
            prompt_for_missing_options: false,
        };

        let selected = resolve_instrument_workflow_selection(&args, &mut false)
            .expect("resolve instrument workflows");
        assert_eq!(
            selected,
            vec![WorkflowArg::Instrument, WorkflowArg::Evaluate]
        );
    }

    #[test]
    fn resolve_instrument_workflows_default_to_all() {
        let args = InstrumentSetupArgs {
            agent: Some(InstrumentAgentArg::Codex),
            agent_cmd: None,
            workflows: Vec::new(),
            no_workflow: false,
            yes: true,
            refresh_docs: false,
            workers: crate::sync::default_workers(),
            languages: Vec::new(),
            tui: false,
            background: false,
            permissions: InstrumentPermissionArgs { yolo: false },
            prompt_for_missing_options: false,
        };

        let selected = resolve_instrument_workflow_selection(&args, &mut false)
            .expect("resolve instrument workflows");
        assert_eq!(selected, resolve_workflow_selection(&[]));
    }

    #[test]
    fn resolve_instrument_workflows_honors_no_workflow() {
        let args = InstrumentSetupArgs {
            agent: Some(InstrumentAgentArg::Codex),
            agent_cmd: None,
            workflows: vec![WorkflowArg::Evaluate],
            no_workflow: true,
            yes: true,
            refresh_docs: false,
            workers: crate::sync::default_workers(),
            languages: Vec::new(),
            tui: false,
            background: false,
            permissions: InstrumentPermissionArgs { yolo: false },
            prompt_for_missing_options: false,
        };

        let selected = resolve_instrument_workflow_selection(&args, &mut false)
            .expect("resolve instrument workflows");
        assert!(selected.is_empty());
    }

    #[test]
    fn instrument_run_mode_prefers_tui_when_prompt_is_available() {
        let args = InstrumentSetupArgs {
            agent: Some(InstrumentAgentArg::Codex),
            agent_cmd: None,
            workflows: Vec::new(),
            no_workflow: false,
            yes: false,
            refresh_docs: false,
            workers: crate::sync::default_workers(),
            languages: Vec::new(),
            tui: false,
            background: false,
            permissions: InstrumentPermissionArgs { yolo: false },
            prompt_for_missing_options: false,
        };

        assert_eq!(resolve_instrument_run_mode(&args, true), (true, false));
    }

    #[test]
    fn instrument_run_mode_falls_back_to_background_without_prompt() {
        let args = InstrumentSetupArgs {
            agent: Some(InstrumentAgentArg::Codex),
            agent_cmd: None,
            workflows: Vec::new(),
            no_workflow: false,
            yes: false,
            refresh_docs: false,
            workers: crate::sync::default_workers(),
            languages: Vec::new(),
            tui: false,
            background: false,
            permissions: InstrumentPermissionArgs { yolo: false },
            prompt_for_missing_options: false,
        };

        assert_eq!(resolve_instrument_run_mode(&args, false), (false, false));
    }

    #[test]
    fn instrument_run_mode_keeps_tui_when_yolo_and_prompt_available() {
        let args = InstrumentSetupArgs {
            agent: Some(InstrumentAgentArg::Codex),
            agent_cmd: None,
            workflows: Vec::new(),
            no_workflow: false,
            yes: false,
            refresh_docs: false,
            workers: crate::sync::default_workers(),
            languages: Vec::new(),
            tui: false,
            background: false,
            permissions: InstrumentPermissionArgs { yolo: true },
            prompt_for_missing_options: false,
        };

        assert_eq!(resolve_instrument_run_mode(&args, true), (true, true));
    }

    #[test]
    fn instrument_run_mode_keeps_background_when_requested_with_yolo() {
        let args = InstrumentSetupArgs {
            agent: Some(InstrumentAgentArg::Codex),
            agent_cmd: None,
            workflows: Vec::new(),
            no_workflow: false,
            yes: false,
            refresh_docs: false,
            workers: crate::sync::default_workers(),
            languages: Vec::new(),
            tui: false,
            background: true,
            permissions: InstrumentPermissionArgs { yolo: true },
            prompt_for_missing_options: false,
        };

        assert_eq!(resolve_instrument_run_mode(&args, true), (false, true));
    }

    #[test]
    fn instrument_run_mode_prefers_tui_for_opencode_when_prompt_is_available() {
        let args = InstrumentSetupArgs {
            agent: Some(InstrumentAgentArg::Opencode),
            agent_cmd: None,
            workflows: Vec::new(),
            no_workflow: false,
            yes: false,
            refresh_docs: false,
            workers: crate::sync::default_workers(),
            languages: Vec::new(),
            tui: false,
            background: false,
            permissions: InstrumentPermissionArgs { yolo: false },
            prompt_for_missing_options: false,
        };

        assert_eq!(resolve_instrument_run_mode(&args, true), (true, false));
    }

    #[test]
    fn instrument_run_mode_keeps_opencode_background_when_requested() {
        let args = InstrumentSetupArgs {
            agent: Some(InstrumentAgentArg::Opencode),
            agent_cmd: None,
            workflows: Vec::new(),
            no_workflow: false,
            yes: false,
            refresh_docs: false,
            workers: crate::sync::default_workers(),
            languages: Vec::new(),
            tui: false,
            background: true,
            permissions: InstrumentPermissionArgs { yolo: false },
            prompt_for_missing_options: false,
        };

        assert_eq!(resolve_instrument_run_mode(&args, true), (false, false));
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
        assert!(task.contains("Latest Braintrust setup and workflow docs were fetched"));
        assert!(task.contains("prefer local `bt` CLI commands"));
        assert!(task.contains("Do not rely on the Braintrust MCP server"));
        assert!(!task.contains("Use the installed Braintrust agent skills"));
    }

    #[test]
    fn render_instrument_task_includes_language_detection_guidance() {
        let root = PathBuf::from("/tmp/repo");
        let task = render_instrument_task(&root, &[WorkflowArg::Instrument], &[], false);
        assert!(task.contains("### 2. Detect Language"));
        assert!(task.contains("`package.json` -> TypeScript"));
        assert!(task.contains("Latest Braintrust setup docs were fetched"));
    }

    #[test]
    fn render_instrument_task_includes_sdk_doc_paths_for_selected_languages() {
        let root = PathBuf::from("/tmp/repo");
        let task = render_instrument_task(
            &root,
            &[WorkflowArg::Instrument],
            &[LanguageArg::Python, LanguageArg::TypeScript],
            false,
        );
        assert!(task.contains("/tmp/repo/sdk-install/python.md"));
        assert!(task.contains("/tmp/repo/sdk-install/typescript.md"));
        assert!(!task.contains("{INSTALL_SDK_CONTEXT}"));
    }

    #[test]
    fn prompted_instrument_workflows_always_include_instrument_when_non_empty() {
        let resolved = resolve_prompted_instrument_workflows(vec![WorkflowArg::Evaluate]);
        assert_eq!(
            resolved,
            vec![WorkflowArg::Instrument, WorkflowArg::Evaluate]
        );
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
    fn codex_interactive_instrument_invocation_uses_tui_prompt_arg() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation =
            resolve_instrument_invocation(Agent::Codex, None, &task_path, true, false, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                stream_json,
                interactive,
                ..
            } => {
                assert_eq!(program, "codex");
                assert!(args.is_empty());
                assert_eq!(stdin_file, None);
                assert_eq!(prompt_file_arg, Some(task_path));
                assert!(!stream_json);
                assert!(interactive);
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
    fn claude_interactive_instrument_invocation_uses_prompt_arg_no_print_flag() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation =
            resolve_instrument_invocation(Agent::Claude, None, &task_path, true, false, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                stream_json,
                interactive,
                ..
            } => {
                assert_eq!(program, "claude");
                assert!(
                    !args.contains(&"-p".to_string()),
                    "interactive mode must not pass -p"
                );
                assert!(args.contains(&"--permission-mode".to_string()));
                assert!(args.contains(&"--settings".to_string()));
                assert!(args.contains(&"--disallowedTools".to_string()));
                assert!(args.contains(&"--name".to_string()));
                assert_eq!(stdin_file, None);
                assert_eq!(prompt_file_arg, Some(task_path));
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
    fn opencode_interactive_instrument_invocation_stays_interactive() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation =
            resolve_instrument_invocation(Agent::Opencode, None, &task_path, true, false, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                stream_json,
                interactive,
                ..
            } => {
                assert_eq!(program, "opencode");
                assert_eq!(args, vec!["run".to_string()]);
                assert_eq!(stdin_file, None);
                assert_eq!(prompt_file_arg, Some(task_path));
                assert!(!stream_json);
                assert!(interactive);
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn gemini_instrument_invocation_uses_headless_stream_json() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation =
            resolve_instrument_invocation(Agent::Gemini, None, &task_path, false, false, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                stream_json,
                interactive,
                ..
            } => {
                assert_eq!(program, "gemini");
                assert_eq!(
                    args,
                    vec![
                        "-p".to_string(),
                        String::new(),
                        "--output-format".to_string(),
                        "stream-json".to_string(),
                    ]
                );
                assert_eq!(stdin_file, Some(task_path));
                assert_eq!(prompt_file_arg, None);
                assert!(stream_json);
                assert!(!interactive);
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn gemini_interactive_instrument_invocation_uses_positional_prompt() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation =
            resolve_instrument_invocation(Agent::Gemini, None, &task_path, true, true, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                stream_json,
                interactive,
                ..
            } => {
                assert_eq!(program, "gemini");
                assert_eq!(args, vec!["--yolo".to_string()]);
                assert_eq!(stdin_file, None);
                assert_eq!(prompt_file_arg, Some(task_path));
                assert!(!stream_json);
                assert!(interactive);
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
                        "--output-format".to_string(),
                        "stream-json".to_string(),
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
    fn cursor_interactive_instrument_invocation_uses_tui_prompt_arg() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation =
            resolve_instrument_invocation(Agent::Cursor, None, &task_path, true, false, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                stream_json,
                interactive,
                ..
            } => {
                assert_eq!(program, "cursor-agent");
                assert!(args.is_empty());
                assert_eq!(stdin_file, None);
                assert_eq!(prompt_file_arg, Some(task_path));
                assert!(!stream_json);
                assert!(interactive);
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn qwen_instrument_invocation_uses_stream_json_with_prompt_arg() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation =
            resolve_instrument_invocation(Agent::Qwen, None, &task_path, false, false, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                stream_json,
                interactive,
                ..
            } => {
                assert_eq!(program, "qwen");
                assert_eq!(
                    args,
                    vec![
                        "-p".to_string(),
                        "--output-format".to_string(),
                        "stream-json".to_string(),
                        "--include-partial-messages".to_string(),
                    ]
                );
                assert_eq!(stdin_file, None);
                assert_eq!(prompt_file_arg, Some(task_path));
                assert!(stream_json);
                assert!(!interactive);
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn qwen_interactive_instrument_invocation_uses_prompt_interactive() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation =
            resolve_instrument_invocation(Agent::Qwen, None, &task_path, true, true, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                stream_json,
                interactive,
                ..
            } => {
                assert_eq!(program, "qwen");
                assert_eq!(args, vec!["--yolo".to_string(), "-i".to_string()]);
                assert_eq!(stdin_file, None);
                assert_eq!(prompt_file_arg, Some(task_path));
                assert!(!stream_json);
                assert!(interactive);
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn copilot_instrument_invocation_uses_json_output_with_prompt_arg() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation =
            resolve_instrument_invocation(Agent::Copilot, None, &task_path, false, false, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                stream_json,
                interactive,
                ..
            } => {
                assert_eq!(program, "copilot");
                assert_eq!(
                    args,
                    vec![
                        "--no-ask-user".to_string(),
                        "--stream".to_string(),
                        "on".to_string(),
                        "-s".to_string(),
                        "-p".to_string(),
                    ]
                );
                assert_eq!(stdin_file, None);
                assert_eq!(prompt_file_arg, Some(task_path));
                assert!(!stream_json);
                assert!(!interactive);
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn copilot_interactive_instrument_invocation_uses_interactive_flag() {
        let task_path = PathBuf::from("/tmp/AGENT_TASK.instrument.md");
        let invocation =
            resolve_instrument_invocation(Agent::Copilot, None, &task_path, true, true, &[])
                .expect("resolve instrument invocation");

        match invocation {
            InstrumentInvocation::Program {
                program,
                args,
                stdin_file,
                prompt_file_arg,
                stream_json,
                interactive,
                ..
            } => {
                assert_eq!(program, "copilot");
                assert_eq!(args, vec!["--yolo".to_string(), "-i".to_string()]);
                assert_eq!(stdin_file, None);
                assert_eq!(prompt_file_arg, Some(task_path));
                assert!(!stream_json);
                assert!(interactive);
            }
            InstrumentInvocation::Shell(_) => panic!("expected program invocation"),
        }
    }

    #[test]
    fn sync_setup_api_key_sets_base_and_process_env() {
        let _guard = env_test_lock().lock().expect("lock env test");
        let previous_api_key = env::var_os("BRAINTRUST_API_KEY");
        env::remove_var("BRAINTRUST_API_KEY");

        let mut base = make_base_args();
        sync_setup_api_key(&mut base, "cli-key");

        assert_eq!(base.api_key.as_deref(), Some("cli-key"));
        assert_eq!(
            env::var("BRAINTRUST_API_KEY").ok().as_deref(),
            Some("cli-key")
        );

        restore_env_var("BRAINTRUST_API_KEY", previous_api_key);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_agent_invocation_sets_extra_env_for_program_launches() {
        let _guard = async_env_test_lock().lock().await;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = env::temp_dir().join(format!("bt-run-agent-env-{unique}"));
        let bin_dir = root.join("bin");
        let task_path = root.join("AGENT_TASK.instrument.md");
        let log_path = root.join("codex.log");
        fs::create_dir_all(&bin_dir).expect("create bin dir");
        fs::write(&task_path, "instrument this repo").expect("write task");
        write_executable(
            &bin_dir.join("codex"),
            &format!(
                "#!/bin/sh\nprintf 'ENV=%s\\n' \"$BRAINTRUST_API_KEY\" > \"{}\"\ncat >/dev/null\nexit 0\n",
                log_path.display()
            ),
        );

        let old_path = env::var("PATH").unwrap_or_default();
        let previous_api_key = env::var_os("BRAINTRUST_API_KEY");
        env::set_var("PATH", format!("{}:{old_path}", bin_dir.display()));
        env::remove_var("BRAINTRUST_API_KEY");

        let invocation = InstrumentInvocation::Program {
            program: "codex".to_string(),
            args: vec!["exec".to_string(), "-".to_string()],
            stdin_file: Some(task_path),
            prompt_file_arg: None,
            initial_prompt: None,
            stream_json: false,
            interactive: false,
        };
        let status = run_agent_invocation(
            &root,
            &invocation,
            false,
            &[("BRAINTRUST_API_KEY".to_string(), "launch-key".to_string())],
        )
        .await
        .expect("run agent invocation");

        env::set_var("PATH", old_path);
        restore_env_var("BRAINTRUST_API_KEY", previous_api_key);

        assert!(status.success());
        let log = fs::read_to_string(&log_path).expect("read log");
        assert!(log.contains("ENV=launch-key"));
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn run_agent_invocation_inherits_process_api_key_env() {
        let _guard = async_env_test_lock().lock().await;
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = env::temp_dir().join(format!("bt-run-agent-inherit-env-{unique}"));
        let bin_dir = root.join("bin");
        let task_path = root.join("AGENT_TASK.instrument.md");
        let log_path = root.join("claude.log");
        fs::create_dir_all(&bin_dir).expect("create bin dir");
        fs::write(&task_path, "instrument this repo").expect("write task");
        write_executable(
            &bin_dir.join("claude"),
            &format!(
                "#!/bin/sh\nprintf 'ENV=%s\\n' \"$BRAINTRUST_API_KEY\" > \"{}\"\ncat >/dev/null\nexit 0\n",
                log_path.display()
            ),
        );

        let old_path = env::var("PATH").unwrap_or_default();
        let previous_api_key = env::var_os("BRAINTRUST_API_KEY");
        env::set_var("PATH", format!("{}:{old_path}", bin_dir.display()));
        env::set_var("BRAINTRUST_API_KEY", "inherited-key");

        let invocation = InstrumentInvocation::Program {
            program: "claude".to_string(),
            args: vec!["-p".to_string()],
            stdin_file: Some(task_path),
            prompt_file_arg: None,
            initial_prompt: None,
            stream_json: false,
            interactive: false,
        };
        let status = run_agent_invocation(&root, &invocation, false, &[])
            .await
            .expect("run agent invocation");

        env::set_var("PATH", old_path);
        restore_env_var("BRAINTRUST_API_KEY", previous_api_key);

        assert!(status.success());
        let log = fs::read_to_string(&log_path).expect("read log");
        assert!(log.contains("ENV=inherited-key"));
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
        assert_eq!(
            status.config_path,
            Some(
                home.join(".agents/skills/braintrust/SKILL.md")
                    .display()
                    .to_string()
            )
        );
        assert!(status.notes.is_empty());
    }

    #[test]
    fn doctor_agent_status_reports_gemini_global_skill_path() {
        let home = std::env::temp_dir();
        let status = doctor_agent_status(Agent::Gemini, InstallScope::Global, None, &home, &[]);
        assert!(!status.configured);
        assert_eq!(
            status.config_path,
            Some(
                home.join(".agents/skills/braintrust/SKILL.md")
                    .display()
                    .to_string()
            )
        );
        assert!(status.notes.is_empty());
    }

    #[test]
    fn doctor_agent_status_reports_qwen_global_skill_path() {
        let home = std::env::temp_dir();
        let status = doctor_agent_status(Agent::Qwen, InstallScope::Global, None, &home, &[]);
        assert!(!status.configured);
        assert_eq!(
            status.config_path,
            Some(
                home.join(".agents/skills/braintrust/SKILL.md")
                    .display()
                    .to_string()
            )
        );
        assert!(status.notes.is_empty());
    }

    #[test]
    fn doctor_agent_status_reports_copilot_global_skill_path() {
        let home = std::env::temp_dir();
        let status = doctor_agent_status(Agent::Copilot, InstallScope::Global, None, &home, &[]);
        assert!(!status.configured);
        assert_eq!(
            status.config_path,
            Some(
                home.join(".agents/skills/braintrust/SKILL.md")
                    .display()
                    .to_string()
            )
        );
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
        let _guard = cwd_test_lock().lock().expect("lock cwd test");
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
        let _guard = cwd_test_lock().lock().expect("lock cwd test");
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
    fn maybe_init_preserves_existing_extra_config_fields() {
        let _guard = cwd_test_lock().lock().expect("lock cwd test");
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = env::temp_dir().join(format!("bt-maybe-init-extra-{unique}"));
        let bt_dir = root.join(".bt");
        fs::create_dir_all(&bt_dir).expect("create .bt dir");
        fs::write(
            bt_dir.join("config.json"),
            r#"{
  "org": "old-org",
  "project": "old-project",
  "project_id": "old-project-id",
  "custom_flag": true,
  "nested": {
    "keep": "me"
  }
}
"#,
        )
        .expect("write config");

        let old = env::current_dir().expect("cwd");
        env::set_current_dir(&root).expect("cd root");

        let project = crate::projects::api::Project {
            id: "project-id".to_string(),
            name: "new-project".to_string(),
            org_id: "org-id".to_string(),
            description: None,
        };
        maybe_init("new-org", &project).expect("maybe init");

        env::set_current_dir(old).expect("restore cwd");

        let saved = config::load_file(&bt_dir.join("config.json"));
        assert_eq!(saved.org.as_deref(), Some("new-org"));
        assert_eq!(saved.project.as_deref(), Some("new-project"));
        assert_eq!(saved.project_id.as_deref(), Some("project-id"));
        assert_eq!(
            saved.extra.get("custom_flag"),
            Some(&serde_json::Value::Bool(true))
        );
        assert_eq!(
            saved.extra.get("nested"),
            Some(&serde_json::json!({ "keep": "me" }))
        );
    }

    #[test]
    fn install_mcp_for_agent_writes_opencode_local_config_file() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-agents-opencode-mcp-local-{unique}"));
        fs::create_dir_all(&root).expect("create temp root");
        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");

        let result = install_mcp_for_agent(
            Agent::Opencode,
            InstallScope::Local,
            Some(&root),
            &home,
            "embedded-api-key",
            "https://api.example.com/mcp",
        )
        .expect("install local mcp");
        assert!(matches!(result.status, InstallStatus::Installed));

        let mcp_path = root.join("opencode.json");
        assert!(mcp_path.exists());
        let parsed: Value =
            serde_json::from_str(&fs::read_to_string(&mcp_path).expect("read mcp")).expect("json");
        assert_eq!(
            parsed.get("$schema").and_then(|v| v.as_str()),
            Some("https://opencode.ai/config.json")
        );
        let servers = parsed
            .get("mcp")
            .and_then(|v| v.as_object())
            .expect("servers object");
        assert!(servers.contains_key("braintrust"));
        assert_eq!(
            servers["braintrust"]["url"].as_str(),
            Some("https://api.example.com/mcp")
        );
        assert_eq!(
            servers["braintrust"]["headers"]["Authorization"].as_str(),
            Some("Bearer embedded-api-key")
        );
        assert_eq!(servers["braintrust"]["type"].as_str(), Some("remote"));
    }

    #[cfg(unix)]
    #[test]
    fn install_mcp_for_agent_writes_codex_local_config_file() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-agents-codex-mcp-local-{unique}"));
        fs::create_dir_all(&root).expect("create temp root");
        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");

        let result = install_mcp_for_agent(
            Agent::Codex,
            InstallScope::Local,
            Some(&root),
            &home,
            "embedded-api-key",
            "https://api.example.com/mcp",
        )
        .expect("install local codex mcp");
        assert!(matches!(result.status, InstallStatus::Installed));

        let config_path = root.join(".codex/config.toml");
        assert!(config_path.exists());
        let parsed: TomlValue = fs::read_to_string(&config_path)
            .expect("read codex config")
            .parse()
            .expect("parse codex config");
        let servers = parsed
            .get("mcp_servers")
            .and_then(|v| v.as_table())
            .expect("mcp_servers table");
        let braintrust = servers
            .get("braintrust")
            .and_then(|v| v.as_table())
            .expect("braintrust table");
        assert_eq!(
            braintrust.get("url").and_then(|v| v.as_str()),
            Some("https://api.example.com/mcp")
        );
        assert_eq!(
            braintrust
                .get("bearer_token_env_var")
                .and_then(|v| v.as_str()),
            Some("BRAINTRUST_API_KEY")
        );
    }

    #[cfg(unix)]
    #[test]
    fn install_mcp_for_agent_updates_existing_codex_local_config() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-agents-codex-mcp-update-{unique}"));
        let codex_dir = root.join(".codex");
        fs::create_dir_all(&codex_dir).expect("create codex dir");
        fs::write(
            codex_dir.join("config.toml"),
            "[mcp_servers.braintrust]\nurl = \"https://old.example/mcp\"\nbearer_token_env_var = \"OLD_KEY\"\n",
        )
        .expect("seed codex config");
        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");

        install_mcp_for_agent(
            Agent::Codex,
            InstallScope::Local,
            Some(&root),
            &home,
            "embedded-api-key",
            "https://api.example.com/mcp",
        )
        .expect("update local codex mcp");

        let parsed: TomlValue = fs::read_to_string(codex_dir.join("config.toml"))
            .expect("read codex config")
            .parse()
            .expect("parse codex config");
        let braintrust = parsed
            .get("mcp_servers")
            .and_then(|v| v.get("braintrust"))
            .and_then(|v| v.as_table())
            .expect("braintrust table");
        assert_eq!(
            braintrust.get("url").and_then(|v| v.as_str()),
            Some("https://api.example.com/mcp")
        );
        assert_eq!(
            braintrust
                .get("bearer_token_env_var")
                .and_then(|v| v.as_str()),
            Some("BRAINTRUST_API_KEY")
        );
    }

    #[cfg(unix)]
    #[test]
    fn install_mcp_for_agent_invokes_codex_native_mcp_add_for_global() {
        let _guard = cwd_test_lock().lock().expect("lock cwd test");
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = env::temp_dir().join(format!("bt-agents-codex-mcp-{unique}"));
        let bin_dir = root.join("bin");
        let log_path = root.join("codex.log");
        fs::create_dir_all(&bin_dir).expect("create bin dir");

        write_executable(
            &bin_dir.join("codex"),
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"{}\"\nprintf 'ENV=%s\\n' \"$BRAINTRUST_API_KEY\" >> \"{}\"\nexit 0\n",
                log_path.display(),
                log_path.display()
            ),
        );

        let old_path = env::var("PATH").unwrap_or_default();
        env::set_var("PATH", format!("{}:{old_path}", bin_dir.display()));

        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");
        let result = install_mcp_for_agent(
            Agent::Codex,
            InstallScope::Global,
            None,
            &home,
            "embedded-api-key",
            "https://api.example.com/mcp",
        )
        .expect("install codex mcp");

        env::set_var("PATH", old_path);

        assert!(matches!(result.status, InstallStatus::Installed));
        let log = fs::read_to_string(&log_path).expect("read codex log");
        assert!(log.contains("mcp"));
        assert!(log.contains("add"));
        assert!(log.contains("braintrust"));
        assert!(log.contains("--url"));
        assert!(log.contains("https://api.example.com/mcp"));
        assert!(log.contains("--bearer-token-env-var"));
        assert!(log.contains("BRAINTRUST_API_KEY"));
        assert!(log.contains("ENV=embedded-api-key"));
    }

    #[cfg(unix)]
    #[test]
    fn install_mcp_for_agent_writes_cursor_global_config_and_enables_server() {
        let _guard = cwd_test_lock().lock().expect("lock cwd test");
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = env::temp_dir().join(format!("bt-agents-cursor-mcp-{unique}"));
        let bin_dir = root.join("bin");
        let log_path = root.join("cursor.log");
        fs::create_dir_all(&bin_dir).expect("create bin dir");

        write_executable(
            &bin_dir.join("cursor-agent"),
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"{}\"\nexit 0\n",
                log_path.display()
            ),
        );

        let old_path = env::var("PATH").unwrap_or_default();
        env::set_var("PATH", format!("{}:{old_path}", bin_dir.display()));

        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");
        let result = install_mcp_for_agent(
            Agent::Cursor,
            InstallScope::Global,
            None,
            &home,
            "cursor-api-key",
            "https://api.example.com/mcp",
        )
        .expect("install cursor mcp");

        env::set_var("PATH", old_path);

        assert!(matches!(result.status, InstallStatus::Installed));
        let mcp_path = home.join(".cursor/mcp.json");
        assert!(mcp_path.exists());
        let parsed: Value =
            serde_json::from_str(&fs::read_to_string(&mcp_path).expect("read mcp")).expect("json");
        let servers = parsed
            .get("mcpServers")
            .and_then(|v| v.as_object())
            .expect("servers object");
        assert_eq!(
            servers["braintrust"]["headers"]["Authorization"].as_str(),
            Some("Bearer cursor-api-key")
        );

        let log = fs::read_to_string(&log_path).expect("read cursor log");
        assert!(log.contains("mcp"));
        assert!(log.contains("enable"));
        assert!(log.contains("braintrust"));
    }

    #[cfg(unix)]
    #[test]
    fn install_mcp_for_agent_invokes_claude_project_scope_for_local() {
        let _guard = cwd_test_lock().lock().expect("lock cwd test");
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = env::temp_dir().join(format!("bt-agents-claude-mcp-{unique}"));
        let bin_dir = root.join("bin");
        let log_path = root.join("claude.log");
        fs::create_dir_all(&bin_dir).expect("create bin dir");
        fs::create_dir_all(&root).expect("create root");

        write_executable(
            &bin_dir.join("claude"),
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"{}\"\npwd >> \"{}\"\nexit 0\n",
                log_path.display(),
                log_path.display()
            ),
        );

        let old_path = env::var("PATH").unwrap_or_default();
        env::set_var("PATH", format!("{}:{old_path}", bin_dir.display()));

        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");
        let result = install_mcp_for_agent(
            Agent::Claude,
            InstallScope::Local,
            Some(&root),
            &home,
            "claude-api-key",
            "https://api.example.com/mcp",
        )
        .expect("install claude local mcp");

        env::set_var("PATH", old_path);

        assert!(matches!(result.status, InstallStatus::Installed));
        assert_eq!(result.paths, vec!["claude:project".to_string()]);
        let log = fs::read_to_string(&log_path).expect("read claude log");
        assert!(log.contains("mcp"));
        assert!(log.contains("add"));
        assert!(log.contains("-s"));
        assert!(log.contains("project"));
        assert!(log.contains("--transport"));
        assert!(log.contains("http"));
        assert!(log.contains("braintrust"));
        assert!(log.contains("https://api.example.com/mcp"));
        assert!(log.contains("Authorization: Bearer claude-api-key"));
        assert!(log.contains(&root.display().to_string()));
    }

    #[test]
    fn install_mcp_for_agent_invokes_gemini_project_scope_for_local() {
        let _guard = cwd_test_lock().lock().expect("lock cwd test");
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = env::temp_dir().join(format!("bt-agents-gemini-mcp-local-{unique}"));
        let bin_dir = root.join("bin");
        let log_path = root.join("gemini.log");
        fs::create_dir_all(&bin_dir).expect("create bin dir");
        fs::create_dir_all(&root).expect("create root");

        write_executable(
            &bin_dir.join("gemini"),
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"{}\"\npwd >> \"{}\"\nexit 0\n",
                log_path.display(),
                log_path.display()
            ),
        );

        let old_path = env::var("PATH").unwrap_or_default();
        env::set_var("PATH", format!("{}:{old_path}", bin_dir.display()));

        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");
        let result = install_mcp_for_agent(
            Agent::Gemini,
            InstallScope::Local,
            Some(&root),
            &home,
            "gemini-api-key",
            "https://api.example.com/mcp",
        )
        .expect("install gemini local mcp");

        env::set_var("PATH", old_path);

        assert!(matches!(result.status, InstallStatus::Installed));
        assert_eq!(result.paths, vec!["gemini:project".to_string()]);
        let log = fs::read_to_string(&log_path).expect("read gemini log");
        assert!(log.contains("mcp"));
        assert!(log.contains("add"));
        assert!(log.contains("-s"));
        assert!(log.contains("project"));
        assert!(log.contains("--transport"));
        assert!(log.contains("http"));
        assert!(log.contains("braintrust"));
        assert!(log.contains("https://api.example.com/mcp"));
        assert!(log.contains("Authorization: Bearer gemini-api-key"));
        assert!(log.contains(&root.display().to_string()));
    }

    #[test]
    fn install_mcp_for_agent_invokes_gemini_user_scope_for_global() {
        let _guard = cwd_test_lock().lock().expect("lock cwd test");
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = env::temp_dir().join(format!("bt-agents-gemini-mcp-global-{unique}"));
        let bin_dir = root.join("bin");
        let log_path = root.join("gemini-global.log");
        fs::create_dir_all(&bin_dir).expect("create bin dir");
        fs::create_dir_all(&root).expect("create root");

        write_executable(
            &bin_dir.join("gemini"),
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"{}\"\npwd >> \"{}\"\nexit 0\n",
                log_path.display(),
                log_path.display()
            ),
        );

        let old_path = env::var("PATH").unwrap_or_default();
        env::set_var("PATH", format!("{}:{old_path}", bin_dir.display()));

        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");
        let result = install_mcp_for_agent(
            Agent::Gemini,
            InstallScope::Global,
            None,
            &home,
            "gemini-api-key",
            "https://api.example.com/mcp",
        )
        .expect("install gemini global mcp");

        env::set_var("PATH", old_path);

        assert!(matches!(result.status, InstallStatus::Installed));
        assert_eq!(result.paths, vec!["gemini:user".to_string()]);
        let log = fs::read_to_string(&log_path).expect("read gemini log");
        assert!(log.contains("user"));
        assert!(log.contains(&home.display().to_string()));
    }

    #[test]
    fn install_mcp_for_agent_invokes_qwen_project_scope_for_local() {
        let _guard = cwd_test_lock().lock().expect("lock cwd test");
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = env::temp_dir().join(format!("bt-agents-qwen-mcp-local-{unique}"));
        let bin_dir = root.join("bin");
        let log_path = root.join("qwen.log");
        fs::create_dir_all(&bin_dir).expect("create bin dir");
        fs::create_dir_all(&root).expect("create root");

        write_executable(
            &bin_dir.join("qwen"),
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"{}\"\npwd >> \"{}\"\nexit 0\n",
                log_path.display(),
                log_path.display()
            ),
        );

        let old_path = env::var("PATH").unwrap_or_default();
        env::set_var("PATH", format!("{}:{old_path}", bin_dir.display()));

        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");
        let result = install_mcp_for_agent(
            Agent::Qwen,
            InstallScope::Local,
            Some(&root),
            &home,
            "qwen-api-key",
            "https://api.example.com/mcp",
        )
        .expect("install qwen local mcp");

        env::set_var("PATH", old_path);

        assert!(matches!(result.status, InstallStatus::Installed));
        assert_eq!(result.paths, vec!["qwen:project".to_string()]);
        let log = fs::read_to_string(&log_path).expect("read qwen log");
        assert!(log.contains("mcp"));
        assert!(log.contains("add"));
        assert!(log.contains("-s"));
        assert!(log.contains("project"));
        assert!(log.contains("--transport"));
        assert!(log.contains("http"));
        assert!(log.contains("braintrust"));
        assert!(log.contains("https://api.example.com/mcp"));
        assert!(log.contains("Authorization: Bearer qwen-api-key"));
        assert!(log.contains(&root.display().to_string()));
    }

    #[test]
    fn install_mcp_for_agent_invokes_qwen_user_scope_for_global() {
        let _guard = cwd_test_lock().lock().expect("lock cwd test");
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = env::temp_dir().join(format!("bt-agents-qwen-mcp-global-{unique}"));
        let bin_dir = root.join("bin");
        let log_path = root.join("qwen-global.log");
        fs::create_dir_all(&bin_dir).expect("create bin dir");
        fs::create_dir_all(&root).expect("create root");

        write_executable(
            &bin_dir.join("qwen"),
            &format!(
                "#!/bin/sh\nprintf '%s\\n' \"$@\" >> \"{}\"\npwd >> \"{}\"\nexit 0\n",
                log_path.display(),
                log_path.display()
            ),
        );

        let old_path = env::var("PATH").unwrap_or_default();
        env::set_var("PATH", format!("{}:{old_path}", bin_dir.display()));

        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");
        let result = install_mcp_for_agent(
            Agent::Qwen,
            InstallScope::Global,
            None,
            &home,
            "qwen-api-key",
            "https://api.example.com/mcp",
        )
        .expect("install qwen global mcp");

        env::set_var("PATH", old_path);

        assert!(matches!(result.status, InstallStatus::Installed));
        assert_eq!(result.paths, vec!["qwen:user".to_string()]);
        let log = fs::read_to_string(&log_path).expect("read qwen log");
        assert!(log.contains("user"));
        assert!(log.contains(&home.display().to_string()));
    }

    #[test]
    fn install_mcp_for_agent_writes_copilot_local_mcp_json() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-agents-copilot-mcp-local-{unique}"));
        fs::create_dir_all(&root).expect("create temp root");
        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");

        let result = install_mcp_for_agent(
            Agent::Copilot,
            InstallScope::Local,
            Some(&root),
            &home,
            "copilot-api-key",
            "https://api.example.com/mcp",
        )
        .expect("install copilot local mcp");
        assert!(matches!(result.status, InstallStatus::Installed));

        let mcp_path = root.join(".mcp.json");
        assert!(mcp_path.exists());
        let parsed: Value =
            serde_json::from_str(&fs::read_to_string(&mcp_path).expect("read mcp")).expect("json");
        let servers = parsed
            .get("mcpServers")
            .and_then(|v| v.as_object())
            .expect("mcpServers object");
        assert!(servers.contains_key("braintrust"));
        assert_eq!(
            servers["braintrust"]["url"].as_str(),
            Some("https://api.example.com/mcp")
        );
        assert_eq!(
            servers["braintrust"]["headers"]["Authorization"].as_str(),
            Some("Bearer copilot-api-key")
        );
    }

    #[test]
    fn install_mcp_for_agent_writes_copilot_global_mcp_config() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let home = std::env::temp_dir().join(format!("bt-agents-copilot-mcp-global-home-{unique}"));
        fs::create_dir_all(&home).expect("create temp home");

        let result = install_mcp_for_agent(
            Agent::Copilot,
            InstallScope::Global,
            None,
            &home,
            "copilot-api-key",
            "https://api.example.com/mcp",
        )
        .expect("install copilot global mcp");
        assert!(matches!(result.status, InstallStatus::Installed));

        let mcp_path = home.join(".copilot/mcp-config.json");
        assert!(mcp_path.exists());
        let parsed: Value =
            serde_json::from_str(&fs::read_to_string(&mcp_path).expect("read mcp")).expect("json");
        let servers = parsed
            .get("mcpServers")
            .and_then(|v| v.as_object())
            .expect("mcpServers object");
        assert!(servers.contains_key("braintrust"));
        assert_eq!(
            servers["braintrust"]["url"].as_str(),
            Some("https://api.example.com/mcp")
        );
        assert_eq!(
            servers["braintrust"]["headers"]["Authorization"].as_str(),
            Some("Bearer copilot-api-key")
        );
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

        let first = Agent::Codex
            .install_skill(InstallScope::Local, Some(&root), &home)
            .expect("first install");
        assert!(matches!(first.status, InstallStatus::Installed));

        let second = Agent::Codex
            .install_skill(InstallScope::Local, Some(&root), &home)
            .expect("second install");
        assert!(matches!(second.status, InstallStatus::Skipped));
        assert!(second.message.contains("already configured"));
    }

    #[test]
    fn install_gemini_uses_canonical_agents_skill_path() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-agents-gemini-skill-{unique}"));
        fs::create_dir_all(&root).expect("create temp root");
        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");

        let result = Agent::Gemini
            .install_skill(InstallScope::Local, Some(&root), &home)
            .expect("install gemini");
        assert!(matches!(result.status, InstallStatus::Installed));
        assert!(root.join(".agents/skills/braintrust/SKILL.md").exists());
        assert!(root.join(".gemini/skills").exists());
    }

    #[test]
    fn install_qwen_uses_canonical_agents_skill_path() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-agents-qwen-skill-{unique}"));
        fs::create_dir_all(&root).expect("create temp root");
        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");

        let result = Agent::Qwen
            .install_skill(InstallScope::Local, Some(&root), &home)
            .expect("install qwen");
        assert!(matches!(result.status, InstallStatus::Installed));
        assert!(root.join(".agents/skills/braintrust/SKILL.md").exists());
        assert!(root.join(".qwen/skills").exists());
    }

    #[test]
    fn install_copilot_uses_canonical_agents_skill_path() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-agents-copilot-skill-{unique}"));
        fs::create_dir_all(&root).expect("create temp root");
        let home = root.join("home");
        fs::create_dir_all(&home).expect("create temp home");

        let result = Agent::Copilot
            .install_skill(InstallScope::Local, Some(&root), &home)
            .expect("install copilot");
        assert!(matches!(result.status, InstallStatus::Installed));
        assert!(root.join(".agents/skills/braintrust/SKILL.md").exists());
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

        let result = Agent::Cursor
            .install_skill(InstallScope::Local, Some(&root), &home)
            .expect("install cursor");
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
