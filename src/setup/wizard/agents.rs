// Vendored from https://github.com/vercel-labs/skills (MIT, v1.5.7), `src/agents.ts`
// via spark-raindrop's `packages/spark/src/skills-vendor/agents.ts`. License: MIT.

use std::path::{Path, PathBuf};

pub struct AgentConfig {
    pub name: &'static str,
    pub display_name: &'static str,
    pub skills_dir: &'static str,
    pub global_skills_dir: GlobalSkillsDir,
    pub detect: Detect,
}

#[allow(dead_code)]
pub enum GlobalSkillsDir {
    Join(&'static [DirPart]),
    OpenClaw,
    None,
}

pub enum DirPart {
    Home,
    ConfigHome,
    ClaudeHome,
    CodexHome,
    VibeHome,
    Sub(&'static str),
}

#[allow(dead_code)]
pub enum Detect {
    PathExists(&'static [DirPart]),
    OpenClaw,
    Codex,
    Cwd(&'static [&'static str]),
    HomeOrCwd {
        home: &'static [DirPart],
        cwd_segments: &'static [&'static str],
    },
    AnyHome(&'static [&'static [DirPart]]),
    Never,
}

#[derive(Clone)]
pub struct Resolved {
    pub home: PathBuf,
    pub config_home: PathBuf,
    pub claude_home: PathBuf,
    pub codex_home: PathBuf,
    pub vibe_home: PathBuf,
}

impl Resolved {
    pub fn new(home: PathBuf) -> Self {
        let config_home = std::env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| home.join(".config"));
        let claude_home = std::env::var_os("CLAUDE_CONFIG_DIR")
            .map(PathBuf::from)
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| home.join(".claude"));
        let codex_home = std::env::var_os("CODEX_HOME")
            .map(PathBuf::from)
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| home.join(".codex"));
        let vibe_home = std::env::var_os("VIBE_HOME")
            .map(PathBuf::from)
            .filter(|p| !p.as_os_str().is_empty())
            .unwrap_or_else(|| home.join(".vibe"));
        Self {
            home,
            config_home,
            claude_home,
            codex_home,
            vibe_home,
        }
    }

    fn resolve_parts(&self, parts: &[DirPart]) -> PathBuf {
        let mut out = PathBuf::new();
        for part in parts {
            match part {
                DirPart::Home => out.push(&self.home),
                DirPart::ConfigHome => out.push(&self.config_home),
                DirPart::ClaudeHome => out.push(&self.claude_home),
                DirPart::CodexHome => out.push(&self.codex_home),
                DirPart::VibeHome => out.push(&self.vibe_home),
                DirPart::Sub(s) => out.push(s),
            }
        }
        out
    }

    pub fn openclaw_global_skills_dir(&self) -> PathBuf {
        if self.home.join(".openclaw").exists() {
            return self.home.join(".openclaw/skills");
        }
        if self.home.join(".clawdbot").exists() {
            return self.home.join(".clawdbot/skills");
        }
        if self.home.join(".moltbot").exists() {
            return self.home.join(".moltbot/skills");
        }
        self.home.join(".openclaw/skills")
    }
}

impl AgentConfig {
    pub fn global_skills_dir(&self, resolved: &Resolved) -> Option<PathBuf> {
        match self.global_skills_dir {
            GlobalSkillsDir::Join(parts) => Some(resolved.resolve_parts(parts)),
            GlobalSkillsDir::OpenClaw => Some(resolved.openclaw_global_skills_dir()),
            GlobalSkillsDir::None => None,
        }
    }

    pub fn detect_installed(&self, resolved: &Resolved, cwd: &Path) -> bool {
        match self.detect {
            Detect::PathExists(parts) => resolved.resolve_parts(parts).exists(),
            Detect::OpenClaw => {
                resolved.home.join(".openclaw").exists()
                    || resolved.home.join(".clawdbot").exists()
                    || resolved.home.join(".moltbot").exists()
            }
            Detect::Codex => resolved.codex_home.exists() || Path::new("/etc/codex").exists(),
            Detect::Cwd(segments) => exists_in(cwd, segments),
            Detect::HomeOrCwd { home, cwd_segments } => {
                exists_in(cwd, cwd_segments) || resolved.resolve_parts(home).exists()
            }
            Detect::AnyHome(options) => options
                .iter()
                .any(|parts| resolved.resolve_parts(parts).exists()),
            Detect::Never => false,
        }
    }
}

fn exists_in(base: &Path, segments: &[&str]) -> bool {
    let mut path = base.to_path_buf();
    for s in segments {
        path.push(s);
    }
    path.exists()
}

pub fn detect_installed_agents(resolved: &Resolved, cwd: &Path) -> Vec<&'static AgentConfig> {
    AGENTS
        .iter()
        .filter(|a| a.detect_installed(resolved, cwd))
        .collect()
}

#[cfg(test)]
pub fn find_agent(name: &str) -> Option<&'static AgentConfig> {
    AGENTS.iter().find(|a| a.name == name)
}

macro_rules! parts {
    ($($e:expr),* $(,)?) => { &[$($e),*] };
}

pub static AGENTS: &[AgentConfig] = &[
    AgentConfig {
        name: "aider-desk",
        display_name: "AiderDesk",
        skills_dir: ".aider-desk/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".aider-desk/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".aider-desk")]),
    },
    AgentConfig {
        name: "amp",
        display_name: "Amp",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::ConfigHome,
            DirPart::Sub("agents/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::ConfigHome, DirPart::Sub("amp")]),
    },
    AgentConfig {
        name: "antigravity",
        display_name: "Antigravity",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".gemini/antigravity/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".gemini/antigravity")]),
    },
    AgentConfig {
        name: "augment",
        display_name: "Augment",
        skills_dir: ".augment/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".augment/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".augment")]),
    },
    AgentConfig {
        name: "bob",
        display_name: "IBM Bob",
        skills_dir: ".bob/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".bob/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".bob")]),
    },
    AgentConfig {
        name: "claude-code",
        display_name: "Claude Code",
        skills_dir: ".claude/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::ClaudeHome,
            DirPart::Sub("skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::ClaudeHome]),
    },
    AgentConfig {
        name: "openclaw",
        display_name: "OpenClaw",
        skills_dir: "skills",
        global_skills_dir: GlobalSkillsDir::OpenClaw,
        detect: Detect::OpenClaw,
    },
    AgentConfig {
        name: "cline",
        display_name: "Cline",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".agents/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".cline")]),
    },
    AgentConfig {
        name: "codearts-agent",
        display_name: "CodeArts Agent",
        skills_dir: ".codeartsdoer/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".codeartsdoer/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".codeartsdoer")]),
    },
    AgentConfig {
        name: "codebuddy",
        display_name: "CodeBuddy",
        skills_dir: ".codebuddy/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".codebuddy/skills")
        ]),
        detect: Detect::HomeOrCwd {
            home: parts![DirPart::Home, DirPart::Sub(".codebuddy")],
            cwd_segments: &[".codebuddy"],
        },
    },
    AgentConfig {
        name: "codemaker",
        display_name: "Codemaker",
        skills_dir: ".codemaker/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".codemaker/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".codemaker")]),
    },
    AgentConfig {
        name: "codestudio",
        display_name: "Code Studio",
        skills_dir: ".codestudio/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".codestudio/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".codestudio")]),
    },
    AgentConfig {
        name: "codex",
        display_name: "Codex",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::CodexHome,
            DirPart::Sub("skills")
        ]),
        detect: Detect::Codex,
    },
    AgentConfig {
        name: "command-code",
        display_name: "Command Code",
        skills_dir: ".commandcode/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".commandcode/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".commandcode")]),
    },
    AgentConfig {
        name: "continue",
        display_name: "Continue",
        skills_dir: ".continue/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".continue/skills")
        ]),
        detect: Detect::HomeOrCwd {
            home: parts![DirPart::Home, DirPart::Sub(".continue")],
            cwd_segments: &[".continue"],
        },
    },
    AgentConfig {
        name: "cortex",
        display_name: "Cortex Code",
        skills_dir: ".cortex/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".snowflake/cortex/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".snowflake/cortex")]),
    },
    AgentConfig {
        name: "crush",
        display_name: "Crush",
        skills_dir: ".crush/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".config/crush/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".config/crush")]),
    },
    AgentConfig {
        name: "cursor",
        display_name: "Cursor",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".cursor/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".cursor")]),
    },
    AgentConfig {
        name: "deepagents",
        display_name: "Deep Agents",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".deepagents/agent/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".deepagents")]),
    },
    AgentConfig {
        name: "devin",
        display_name: "Devin for Terminal",
        skills_dir: ".devin/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::ConfigHome,
            DirPart::Sub("devin/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::ConfigHome, DirPart::Sub("devin")]),
    },
    AgentConfig {
        name: "dexto",
        display_name: "Dexto",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".agents/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".dexto")]),
    },
    AgentConfig {
        name: "droid",
        display_name: "Droid",
        skills_dir: ".factory/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".factory/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".factory")]),
    },
    AgentConfig {
        name: "firebender",
        display_name: "Firebender",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".firebender/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".firebender")]),
    },
    AgentConfig {
        name: "forgecode",
        display_name: "ForgeCode",
        skills_dir: ".forge/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".forge/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".forge")]),
    },
    AgentConfig {
        name: "gemini-cli",
        display_name: "Gemini CLI",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".gemini/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".gemini")]),
    },
    AgentConfig {
        name: "github-copilot",
        display_name: "GitHub Copilot",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".copilot/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".copilot")]),
    },
    AgentConfig {
        name: "goose",
        display_name: "Goose",
        skills_dir: ".goose/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::ConfigHome,
            DirPart::Sub("goose/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::ConfigHome, DirPart::Sub("goose")]),
    },
    AgentConfig {
        name: "hermes-agent",
        display_name: "Hermes Agent",
        skills_dir: ".hermes/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".hermes/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".hermes")]),
    },
    AgentConfig {
        name: "junie",
        display_name: "Junie",
        skills_dir: ".junie/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".junie/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".junie")]),
    },
    AgentConfig {
        name: "iflow-cli",
        display_name: "iFlow CLI",
        skills_dir: ".iflow/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".iflow/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".iflow")]),
    },
    AgentConfig {
        name: "kilo",
        display_name: "Kilo Code",
        skills_dir: ".kilocode/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".kilocode/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".kilocode")]),
    },
    AgentConfig {
        name: "kimi-cli",
        display_name: "Kimi Code CLI",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".config/agents/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".kimi")]),
    },
    AgentConfig {
        name: "kiro-cli",
        display_name: "Kiro CLI",
        skills_dir: ".kiro/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".kiro/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".kiro")]),
    },
    AgentConfig {
        name: "kode",
        display_name: "Kode",
        skills_dir: ".kode/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".kode/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".kode")]),
    },
    AgentConfig {
        name: "mcpjam",
        display_name: "MCPJam",
        skills_dir: ".mcpjam/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".mcpjam/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".mcpjam")]),
    },
    AgentConfig {
        name: "mistral-vibe",
        display_name: "Mistral Vibe",
        skills_dir: ".vibe/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![DirPart::VibeHome, DirPart::Sub("skills")]),
        detect: Detect::PathExists(parts![DirPart::VibeHome]),
    },
    AgentConfig {
        name: "mux",
        display_name: "Mux",
        skills_dir: ".mux/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".mux/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".mux")]),
    },
    AgentConfig {
        name: "opencode",
        display_name: "OpenCode",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::ConfigHome,
            DirPart::Sub("opencode/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::ConfigHome, DirPart::Sub("opencode")]),
    },
    AgentConfig {
        name: "openhands",
        display_name: "OpenHands",
        skills_dir: ".openhands/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".openhands/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".openhands")]),
    },
    AgentConfig {
        name: "pi",
        display_name: "Pi",
        skills_dir: ".pi/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".pi/agent/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".pi/agent")]),
    },
    AgentConfig {
        name: "qoder",
        display_name: "Qoder",
        skills_dir: ".qoder/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".qoder/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".qoder")]),
    },
    AgentConfig {
        name: "qwen-code",
        display_name: "Qwen Code",
        skills_dir: ".qwen/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".qwen/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".qwen")]),
    },
    AgentConfig {
        name: "replit",
        display_name: "Replit",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::ConfigHome,
            DirPart::Sub("agents/skills")
        ]),
        detect: Detect::Cwd(&[".replit"]),
    },
    AgentConfig {
        name: "rovodev",
        display_name: "Rovo Dev",
        skills_dir: ".rovodev/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".rovodev/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".rovodev")]),
    },
    AgentConfig {
        name: "roo",
        display_name: "Roo Code",
        skills_dir: ".roo/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".roo/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".roo")]),
    },
    AgentConfig {
        name: "tabnine-cli",
        display_name: "Tabnine CLI",
        skills_dir: ".tabnine/agent/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".tabnine/agent/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".tabnine")]),
    },
    AgentConfig {
        name: "trae",
        display_name: "Trae",
        skills_dir: ".trae/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".trae/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".trae")]),
    },
    AgentConfig {
        name: "trae-cn",
        display_name: "Trae CN",
        skills_dir: ".trae/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".trae-cn/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".trae-cn")]),
    },
    AgentConfig {
        name: "warp",
        display_name: "Warp",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".agents/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".warp")]),
    },
    AgentConfig {
        name: "windsurf",
        display_name: "Windsurf",
        skills_dir: ".windsurf/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".codeium/windsurf/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".codeium/windsurf")]),
    },
    AgentConfig {
        name: "zencoder",
        display_name: "Zencoder",
        skills_dir: ".zencoder/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".zencoder/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".zencoder")]),
    },
    AgentConfig {
        name: "neovate",
        display_name: "Neovate",
        skills_dir: ".neovate/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".neovate/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".neovate")]),
    },
    AgentConfig {
        name: "pochi",
        display_name: "Pochi",
        skills_dir: ".pochi/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".pochi/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".pochi")]),
    },
    AgentConfig {
        name: "adal",
        display_name: "AdaL",
        skills_dir: ".adal/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::Home,
            DirPart::Sub(".adal/skills")
        ]),
        detect: Detect::PathExists(parts![DirPart::Home, DirPart::Sub(".adal")]),
    },
    AgentConfig {
        name: "universal",
        display_name: "Universal",
        skills_dir: ".agents/skills",
        global_skills_dir: GlobalSkillsDir::Join(parts![
            DirPart::ConfigHome,
            DirPart::Sub("agents/skills")
        ]),
        detect: Detect::Never,
    },
];

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn detect_claude_code_via_claude_home() {
        let home = tempdir().unwrap();
        std::fs::create_dir_all(home.path().join(".claude")).unwrap();
        let resolved = Resolved::new(home.path().to_path_buf());
        let cwd = tempdir().unwrap();
        let claude = find_agent("claude-code").unwrap();
        assert!(claude.detect_installed(&resolved, cwd.path()));
        let dir = claude.global_skills_dir(&resolved).unwrap();
        assert!(dir.ends_with(".claude/skills"));
    }

    #[test]
    fn detect_replit_via_cwd_marker() {
        let home = tempdir().unwrap();
        let cwd = tempdir().unwrap();
        std::fs::write(cwd.path().join(".replit"), "").unwrap();
        let resolved = Resolved::new(home.path().to_path_buf());
        let replit = find_agent("replit").unwrap();
        assert!(replit.detect_installed(&resolved, cwd.path()));
    }

    #[test]
    fn universal_never_detected() {
        let home = tempdir().unwrap();
        let cwd = tempdir().unwrap();
        let resolved = Resolved::new(home.path().to_path_buf());
        let universal = find_agent("universal").unwrap();
        assert!(!universal.detect_installed(&resolved, cwd.path()));
    }
}
