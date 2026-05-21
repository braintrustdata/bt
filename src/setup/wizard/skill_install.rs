use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use super::agents::{detect_installed_agents, AgentConfig, Resolved};
use super::language::DetectedLanguage;
use super::prompt::{render_skill_markdown, SKILL_NAME};

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
pub enum InstallScope {
    Global,
    Project,
}

#[derive(Debug, Clone)]
pub struct WrittenSkill {
    #[allow(dead_code)]
    pub agent: &'static str,
    pub display_name: &'static str,
    pub scope: InstallScope,
    #[allow(dead_code)]
    pub path: PathBuf,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[allow(dead_code)]
pub enum SkipReason {
    NoGlobalDir,
    SkippedByUser,
}

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SkippedSkill {
    pub agent: &'static str,
    pub display_name: &'static str,
    pub scope: InstallScope,
    pub reason: SkipReason,
}

#[derive(Debug, Default)]
pub struct InstallResult {
    pub written: Vec<WrittenSkill>,
    pub skipped: Vec<SkippedSkill>,
    pub detected_agents: Vec<&'static str>,
}

pub struct InstallOptions<'a> {
    pub cwd: &'a Path,
    pub home: PathBuf,
    pub languages: &'a [DetectedLanguage],
    pub skip_agents: &'a [String],
    pub override_dir: Option<&'a Path>,
}

pub fn install_instrument_code_skill(opts: InstallOptions<'_>) -> Result<InstallResult> {
    let mut result = InstallResult::default();

    if let Some(dir) = opts.override_dir {
        let target = dir.join(SKILL_NAME);
        let path = write_skill(&target, opts.languages)?;
        result.written.push(WrittenSkill {
            agent: "universal",
            display_name: "Universal",
            scope: InstallScope::Global,
            path,
        });
        return Ok(result);
    }

    let resolved = Resolved::new(opts.home);
    let detected: Vec<&'static AgentConfig> = detect_installed_agents(&resolved, opts.cwd);
    let skip: HashSet<&str> = opts.skip_agents.iter().map(String::as_str).collect();

    for cfg in &detected {
        result.detected_agents.push(cfg.name);
    }

    for cfg in detected {
        if skip.contains(cfg.name) {
            result.skipped.push(SkippedSkill {
                agent: cfg.name,
                display_name: cfg.display_name,
                scope: InstallScope::Global,
                reason: SkipReason::SkippedByUser,
            });
            continue;
        }

        // Global scope: language-agnostic body.
        match cfg.global_skills_dir(&resolved) {
            Some(global) => {
                let path = write_skill(&global.join(SKILL_NAME), &[])?;
                result.written.push(WrittenSkill {
                    agent: cfg.name,
                    display_name: cfg.display_name,
                    scope: InstallScope::Global,
                    path,
                });
            }
            None => {
                result.skipped.push(SkippedSkill {
                    agent: cfg.name,
                    display_name: cfg.display_name,
                    scope: InstallScope::Global,
                    reason: SkipReason::NoGlobalDir,
                });
            }
        }

        // Project scope: tailored body.
        let project_dir = opts.cwd.join(cfg.skills_dir).join(SKILL_NAME);
        let path = write_skill(&project_dir, opts.languages)?;
        result.written.push(WrittenSkill {
            agent: cfg.name,
            display_name: cfg.display_name,
            scope: InstallScope::Project,
            path,
        });
    }

    Ok(result)
}

fn write_skill(dir: &Path, languages: &[DetectedLanguage]) -> Result<PathBuf> {
    fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
    let path = dir.join("SKILL.md");
    fs::write(&path, render_skill_markdown(languages))
        .with_context(|| format!("writing {}", path.display()))?;
    Ok(path)
}

pub struct FallbackPrompt {
    pub path: PathBuf,
}

pub fn write_fallback_prompt(languages: &[DetectedLanguage]) -> Result<FallbackPrompt> {
    let dir = tempfile::Builder::new()
        .prefix("bt-setup-")
        .tempdir()
        .context("creating fallback prompt temp dir")?;
    let path = dir.path().join("instrument-code.md");
    fs::write(&path, render_skill_markdown(languages))
        .with_context(|| format!("writing {}", path.display()))?;
    // Persist the tempdir so the file survives this process; the user reads it after exit.
    let _ = dir.keep();
    Ok(FallbackPrompt { path })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn override_dir_writes_single_skill() {
        let dir = tempdir().unwrap();
        let cwd = tempdir().unwrap();
        let home = tempdir().unwrap();
        let result = install_instrument_code_skill(InstallOptions {
            cwd: cwd.path(),
            home: home.path().to_path_buf(),
            languages: &[],
            skip_agents: &[],
            override_dir: Some(dir.path()),
        })
        .unwrap();
        assert_eq!(result.written.len(), 1);
        assert!(result.written[0].path.exists());
    }
}
