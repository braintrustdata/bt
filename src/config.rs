use anyhow::{anyhow, bail, Context, Result};
use std::{
    env, fs, io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::ui::{print_command_status, CommandStatus};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub profile: Option<String>,
    pub org: Option<String>,
    pub project: Option<String>,
    pub project_id: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl Config {
    pub(crate) fn merge(&self, other: &Config) -> Config {
        let mut extra = self.extra.clone();
        extra.extend(other.extra.clone());
        let project = other.project.clone().or_else(|| self.project.clone());
        let project_id = if other.project.is_some() {
            other.project_id.clone()
        } else {
            self.project_id.clone()
        };
        Config {
            profile: other.profile.clone().or_else(|| self.profile.clone()),
            org: other.org.clone().or_else(|| self.org.clone()),
            project,
            project_id,
            extra,
        }
    }
}

pub fn global_config_dir() -> Result<PathBuf> {
    if let Some(xdg) = env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(xdg).join("bt"));
    }
    dirs::home_dir()
        .map(|path| path.join(".config").join("bt"))
        .ok_or_else(|| anyhow!("$HOME not configured."))
}

pub fn global_path() -> Result<PathBuf> {
    Ok(global_config_dir()?.join("config.json"))
}

pub fn load_file(path: &Path) -> Config {
    let file_contents = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Config::default(),
        Err(e) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Warning: could not read {}: {e}", path.display()),
            );
            return Config::default();
        }
    };

    let config: Config = match serde_json::from_str(&file_contents) {
        Ok(c) => c,
        Err(e) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Warning: could not read {}: {e}", path.display()),
            );
            return Config::default();
        }
    };

    for key in config.extra.keys() {
        print_command_status(
            CommandStatus::Error,
            &format!("Warning: unknown config key {} in {}", key, path.display()),
        );
    }

    config
}

pub fn load_global() -> Result<Config> {
    Ok(load_file(&global_path()?))
}

pub fn load() -> Result<Config> {
    let global = load_global().unwrap_or_default();
    let local = match local_path() {
        Some(p) => load_file(&p),
        None => Config::default(),
    };
    Ok(global.merge(&local))
}

pub fn configured_project_for_context(resolved_org: Option<&str>) -> Option<String> {
    load()
        .ok()
        .and_then(|cfg| project_from_config_for_context(&cfg, resolved_org))
}

pub fn configured_project_id() -> Option<String> {
    load()
        .ok()
        .and_then(|cfg| trimmed_option(cfg.project_id.as_deref()).map(str::to_string))
}

pub(crate) fn project_from_config_for_context(
    cfg: &Config,
    resolved_org: Option<&str>,
) -> Option<String> {
    config_matches_context(cfg, resolved_org)
        .then(|| trimmed_option(cfg.project.as_deref()).map(str::to_string))
        .flatten()
}

fn config_matches_context(cfg: &Config, resolved_org: Option<&str>) -> bool {
    let cfg_org = trimmed_option(cfg.org.as_deref());
    let resolved_org = trimmed_option(resolved_org);

    cfg_org
        .zip(resolved_org)
        .is_none_or(|(cfg, resolved)| cfg == resolved)
}

pub(crate) fn trimmed_option(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

pub fn save_file(path: &Path, config: &Config) -> Result<()> {
    crate::utils::write_json_atomic(path, config)
}

pub fn save_global(config: &Config) -> Result<()> {
    save_file(&global_path()?, config)
}

pub fn find_local_config_dir() -> Option<PathBuf> {
    let home = dirs::home_dir();
    let mut current_dir = std::env::current_dir().ok()?;

    loop {
        if current_dir.join(".bt").is_dir() {
            return Some(current_dir.join(".bt"));
        }
        if current_dir.join(".git").exists() {
            return None;
        }
        if Some(&current_dir) == home.as_ref() {
            return None;
        }
        if !current_dir.pop() {
            return None;
        }
    }
}

pub fn local_path() -> Option<PathBuf> {
    find_local_config_dir().map(|dir| dir.join("config.json"))
}

pub fn local_save_path() -> Result<PathBuf> {
    Ok(std::env::current_dir()?.join(".bt").join("config.json"))
}

/// Creates nothing, so an aborted command leaves no config directory behind.
pub fn preflight_config_write(path: &Path) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    // `save_file` creates missing parents, so permissions hinge on this one.
    let existing = nearest_existing_dir(parent);

    if !existing.is_dir() {
        bail!(
            "Could not create config directory {}: {} is not a directory",
            parent.display(),
            existing.display()
        );
    }

    let probe = tempfile::NamedTempFile::new_in(&existing)
        .with_context(|| format!("Could not write to config directory {}", existing.display()))?;
    probe
        .close()
        .with_context(|| format!("Could not clean up write test in {}", existing.display()))?;

    // Unix renames over a read-only target fine; Windows does not.
    #[cfg(windows)]
    if path
        .metadata()
        .is_ok_and(|meta| meta.permissions().readonly())
    {
        bail!(
            "Could not write config file {}: it is read-only",
            path.display()
        );
    }

    Ok(())
}

fn nearest_existing_dir(dir: &Path) -> PathBuf {
    let mut current = if dir.as_os_str().is_empty() {
        PathBuf::from(".")
    } else {
        dir.to_path_buf()
    };
    loop {
        if current.exists() {
            return current;
        }
        match current.parent() {
            Some(parent) if !parent.as_os_str().is_empty() => current = parent.to_path_buf(),
            _ => return PathBuf::from("."),
        }
    }
}

pub fn save_local(config: &Config, create_dir: bool) -> Result<PathBuf> {
    let path = local_save_path()?;
    let dir = path.parent().expect(".bt parent directory");
    if create_dir && !dir.exists() {
        fs::create_dir_all(dir)?;
    }
    save_file(&path, config)?;
    Ok(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn merge_other_takes_precedence() {
        let base = Config {
            org: Some("base-org".into()),
            project: Some("base-proj".into()),
            ..Default::default()
        };
        let other = Config {
            org: Some("other-org".into()),
            project: Some("other-proj".into()),
            ..Default::default()
        };
        let merged = base.merge(&other);
        assert_eq!(merged.org, Some("other-org".into()));
        assert_eq!(merged.project, Some("other-proj".into()));
    }

    #[test]
    fn merge_self_fills_when_other_none() {
        let base = Config {
            org: Some("base-org".into()),
            project: Some("base-proj".into()),
            ..Default::default()
        };
        let other = Config::default();
        let merged = base.merge(&other);
        assert_eq!(merged.org, Some("base-org".into()));
        assert_eq!(merged.project, Some("base-proj".into()));
    }

    #[test]
    fn merge_both_none_stays_none() {
        let base = Config::default();
        let other = Config::default();
        let merged = base.merge(&other);
        assert_eq!(merged.org, None);
        assert_eq!(merged.project, None);
    }

    #[test]
    fn merge_partial_fill() {
        let base = Config {
            org: Some("base-org".into()),
            project: None,
            ..Default::default()
        };
        let other = Config {
            org: None,
            project: Some("other-proj".into()),
            ..Default::default()
        };
        let merged = base.merge(&other);
        assert_eq!(merged.org, Some("base-org".into()));
        assert_eq!(merged.project, Some("other-proj".into()));
    }

    fn config(profile: Option<&str>, org: Option<&str>, project: Option<&str>) -> Config {
        Config {
            profile: profile.map(str::to_string),
            org: org.map(str::to_string),
            project: project.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn project_config_matches_org_independently_of_profile() {
        let cases = [
            (config(None, Some("acme"), Some("demo")), Some("demo")),
            (config(None, Some("other"), Some("demo")), None),
            (config(None, None, Some("demo")), Some("demo")),
            (
                config(Some("other"), Some("acme"), Some("demo")),
                Some("demo"),
            ),
            (
                config(Some("work"), Some("acme"), Some("demo")),
                Some("demo"),
            ),
        ];

        for (cfg, expected) in cases {
            assert_eq!(
                project_from_config_for_context(&cfg, Some("acme")).as_deref(),
                expected
            );
        }
    }

    #[test]
    fn load_missing_file_returns_default() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nonexistent.json");
        let config = load_file(&path);
        assert_eq!(config.org, None);
        assert_eq!(config.project, None);
    }

    #[test]
    fn load_invalid_json_returns_default() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("invalid.json");
        fs::write(&path, "not valid json {{{").unwrap();
        let config = load_file(&path);
        assert_eq!(config.org, None);
    }

    #[test]
    fn save_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("config.json");

        let original = Config {
            org: Some("test-org".into()),
            project: Some("test-project".into()),
            ..Default::default()
        };

        save_file(&path, &original).unwrap();
        let loaded = load_file(&path);

        assert_eq!(loaded.org, original.org);
        assert_eq!(loaded.project, original.project);
    }

    #[test]
    fn load_unknown_keys_still_returns_config() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("config.json");
        fs::write(
            &path,
            r#"{"org": "my-org", "unknown_field": "value", "another": 123}"#,
        )
        .unwrap();

        let config = load_file(&path);
        assert_eq!(config.org, Some("my-org".into()));
        assert!(config.extra.contains_key("unknown_field"));
        assert!(config.extra.contains_key("another"));
    }

    #[test]
    fn unknown_keys_roundtrip_through_save() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("config.json");
        fs::write(
            &path,
            r#"{"org": "my-org", "unknown_field": "value", "another": 123}"#,
        )
        .unwrap();

        let config = load_file(&path);
        save_file(&path, &config).unwrap();
        let reloaded = load_file(&path);

        assert_eq!(reloaded.org, Some("my-org".into()));
        assert!(reloaded.extra.contains_key("unknown_field"));
        assert!(reloaded.extra.contains_key("another"));
    }

    #[test]
    fn save_creates_parent_dirs() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nested").join("dir").join("config.json");

        let config = Config {
            org: Some("test".into()),
            ..Default::default()
        };

        save_file(&path, &config).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn preflight_config_write_checks_parent_without_creating_it() {
        let tmp = TempDir::new().unwrap();
        let missing = tmp.path().join(".bt");
        let existing = tmp.path().join("config.json");
        save_file(&existing, &Config::default()).unwrap();

        preflight_config_write(&missing.join("config.json")).unwrap();
        preflight_config_write(&existing).unwrap();
        assert!(!missing.exists());

        let file_parent = tmp.path().join("not-a-dir");
        fs::write(&file_parent, "").unwrap();
        let error = preflight_config_write(&file_parent.join("config.json")).unwrap_err();
        assert!(error
            .to_string()
            .contains("Could not create config directory"));
    }

    #[cfg(unix)]
    #[test]
    fn preflight_config_write_fails_when_parent_is_not_writable() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = TempDir::new().unwrap();
        let parent = tmp.path().join("locked");
        fs::create_dir(&parent).unwrap();
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o555)).unwrap();
        // Root ignores the mode bits, so the probe would succeed there.
        let permissions_enforced = fs::write(parent.join("root-check"), "").is_err();

        let result = preflight_config_write(&parent.join(".bt").join("config.json"));

        // Restore write access so cleanup can remove the temp dir.
        fs::set_permissions(&parent, fs::Permissions::from_mode(0o755)).unwrap();

        if permissions_enforced {
            assert!(result
                .unwrap_err()
                .to_string()
                .contains("Could not write to config directory"));
        }
    }
}
