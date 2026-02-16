use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::IsTerminal;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use dialoguer::{theme::ColorfulTheme, MultiSelect};
use serde::Serialize;
use serde_json::{Map, Value};

use crate::args::BaseArgs;

const SHARED_SKILL_BODY: &str = include_str!("../skills/shared/braintrust-cli-body.md");

#[derive(Debug, Clone, Args)]
pub struct AgentsArgs {
    #[command(subcommand)]
    command: Option<AgentsSubcommand>,
}

#[derive(Debug, Clone, Subcommand)]
enum AgentsSubcommand {
    /// Configure coding agents to use Braintrust
    Setup(AgentsSetupArgs),
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

pub async fn run(base: BaseArgs, args: AgentsArgs) -> Result<()> {
    match args.command {
        Some(AgentsSubcommand::Setup(setup)) => run_setup(base, setup),
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
        "---\nname: braintrust-cli\nversion: 1.0.0\ndescription: Use the Braintrust `bt` CLI for projects, traces, prompts, and sync workflows.\n---\n\n{}",
        SHARED_SKILL_BODY.trim()
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
}
