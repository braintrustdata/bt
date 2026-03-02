use anyhow::{anyhow, bail, Result};
use clap::{Args, Subcommand};
use std::{
    env, fs,
    io::{self, Write as _},
    path::{Path, PathBuf},
    time::Duration,
};

use serde::{Deserialize, Serialize};

use crate::args::BaseArgs;
use crate::ui::{print_command_status, CommandStatus};

mod get;
mod list;
mod set;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub org: Option<String>,
    pub project: Option<String>,
    pub project_id: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

pub const KNOWN_KEYS: &[&str] = &["org", "project", "project_id"];

impl Config {
    pub fn get_field(&self, key: &str) -> Option<&str> {
        match key {
            "org" => self.org.as_deref(),
            "project" => self.project.as_deref(),
            "project_id" => self.project_id.as_deref(),
            _ => None,
        }
    }

    pub fn set_field(&mut self, key: &str, value: String) -> bool {
        match key {
            "org" => self.org = Some(value),
            "project" => {
                self.project = Some(value);
                self.project_id = None;
            }
            "project_id" => self.project_id = Some(value),
            _ => return false,
        }
        true
    }

    pub fn unset_field(&mut self, key: &str) -> bool {
        match key {
            "org" => self.org = None,
            "project" => {
                self.project = None;
                self.project_id = None;
            }
            "project_id" => self.project_id = None,
            _ => return false,
        }
        true
    }

    pub fn non_empty_fields(&self) -> Vec<(&str, &str)> {
        KNOWN_KEYS
            .iter()
            .filter_map(|&key| self.get_field(key).map(|v| (key, v)))
            .collect()
    }

    fn merge(&self, other: &Config) -> Config {
        let mut extra = self.extra.clone();
        extra.extend(other.extra.clone());
        let project = other.project.clone().or_else(|| self.project.clone());
        let project_id = if other.project.is_some() {
            other.project_id.clone()
        } else {
            self.project_id.clone()
        };
        Config {
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

pub fn save_file(path: &Path, config: &Config) -> Result<()> {
    let _lock = acquire_config_lock(path, CONFIG_LOCK_RETRIES, CONFIG_LOCK_RETRY_DELAY)?;
    save_file_unlocked(path, config)
}

pub fn update_file_with_lock<F>(path: &Path, mut updater: F) -> Result<bool>
where
    F: FnMut(&mut Config) -> bool,
{
    let _lock = acquire_config_lock(path, CONFIG_LOCK_RETRIES, CONFIG_LOCK_RETRY_DELAY)?;
    let mut config = load_file(path);
    if !updater(&mut config) {
        return Ok(false);
    }
    save_file_unlocked(path, &config)?;
    Ok(true)
}

const CONFIG_LOCK_RETRIES: u32 = 200;
const CONFIG_LOCK_RETRY_DELAY: Duration = Duration::from_millis(5);

fn save_file_unlocked(path: &Path, config: &Config) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let json = serde_json::to_string_pretty(config)?;
    let temp_path = path.with_extension("tmp");
    let mut file = fs::File::create(&temp_path)?;
    file.write_all(json.as_bytes())?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    fs::rename(&temp_path, path)?;

    Ok(())
}

struct ConfigLockGuard {
    path: PathBuf,
    _file: fs::File,
}

impl Drop for ConfigLockGuard {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

fn acquire_config_lock(
    path: &Path,
    max_retries: u32,
    retry_delay: Duration,
) -> Result<ConfigLockGuard> {
    let lock_path = lock_path_for(path);
    if let Some(parent) = lock_path.parent() {
        fs::create_dir_all(parent)?;
    }

    let mut attempts = 0u32;
    loop {
        match fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&lock_path)
        {
            Ok(file) => {
                return Ok(ConfigLockGuard {
                    path: lock_path,
                    _file: file,
                });
            }
            Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
                if attempts >= max_retries {
                    bail!("timed out waiting for config lock {}", lock_path.display());
                }
                attempts += 1;
                std::thread::sleep(retry_delay);
            }
            Err(err) => return Err(err.into()),
        }
    }
}

fn lock_path_for(path: &Path) -> PathBuf {
    if let Some(file_name) = path.file_name() {
        let mut lock_name = file_name.to_os_string();
        lock_name.push(".lock");
        path.with_file_name(lock_name)
    } else {
        path.with_extension("lock")
    }
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

pub enum WriteTarget {
    Global(PathBuf),
    Local(PathBuf),
}

pub fn write_target() -> Result<WriteTarget> {
    match local_path() {
        Some(p) => Ok(WriteTarget::Local(p)),
        None => Ok(WriteTarget::Global(global_path()?)),
    }
}

/// Resolve which config file to write based on --global/--local flags.
pub fn resolve_write_path(global: bool, local: bool) -> Result<PathBuf> {
    if global {
        global_path()
    } else if local {
        match local_path() {
            Some(p) => Ok(p),
            None => {
                bail!("No local .bt directory found. Use bt init to initialize this directory.")
            }
        }
    } else {
        match write_target()? {
            WriteTarget::Local(p) | WriteTarget::Global(p) => Ok(p),
        }
    }
}

pub fn save_local(config: &Config, create_dir: bool) -> Result<()> {
    let dir = std::env::current_dir()?.join(".bt");
    if create_dir && !dir.exists() {
        fs::create_dir_all(&dir)?;
    }
    save_file(&dir.join("config.json"), config)
}

// --- CLI commands ---

#[derive(Debug, Clone, Args)]
pub struct ScopeArgs {
    /// Apply to global config (~/.config/bt/config.json)
    #[arg(long, short = 'g', conflicts_with = "local")]
    global: bool,

    /// Apply to local config (.bt/config.json)
    #[arg(long, short = 'l')]
    local: bool,
}

#[derive(Debug, Clone, Args)]
pub struct ConfigArgs {
    #[command(subcommand)]
    command: Option<ConfigCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum ConfigCommands {
    /// List config values
    List {
        #[command(flatten)]
        scope: ScopeArgs,
        /// Show config values grouped by source
        #[arg(long)]
        verbose: bool,
    },
    /// Get a config value
    Get {
        /// Config key (org, project, project_id)
        key: String,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Set a config value
    Set {
        /// Config key (org, project, project_id)
        key: String,
        /// Value to set
        value: String,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Remove a config value
    Unset {
        /// Config key (org, project, project_id)
        key: String,
        #[command(flatten)]
        scope: ScopeArgs,
    },
}

fn validate_key(key: &str) -> Result<()> {
    if !KNOWN_KEYS.contains(&key) {
        bail!(
            "Unknown config key: {key}\nValid keys: {}",
            KNOWN_KEYS.join(", ")
        );
    }
    Ok(())
}

pub fn run(base: BaseArgs, args: ConfigArgs) -> Result<()> {
    match args.command {
        None => list::run(base, false, false, false),
        Some(ConfigCommands::List { scope, verbose }) => {
            list::run(base, scope.global, scope.local, verbose)
        }
        Some(ConfigCommands::Get { key, scope }) => {
            validate_key(&key)?;
            get::run(base, &key, scope.global, scope.local)
        }
        Some(ConfigCommands::Set { key, value, scope }) => {
            validate_key(&key)?;
            set::run(&key, &value, scope.global, scope.local)
        }
        Some(ConfigCommands::Unset { key, scope }) => {
            validate_key(&key)?;
            set::unset(&key, scope.global, scope.local)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::thread;
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
    fn update_file_with_lock_preserves_concurrent_updates() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("config.json");
        save_file(&path, &Config::default()).unwrap();

        let path_a = path.clone();
        let path_b = path.clone();
        let increments = 100;

        let thread_a = thread::spawn(move || {
            for _ in 0..increments {
                update_file_with_lock(&path_a, |cfg| {
                    let value = cfg.extra.get("a").and_then(|v| v.as_i64()).unwrap_or(0) + 1;
                    cfg.extra.insert("a".to_string(), json!(value));
                    true
                })
                .unwrap();
            }
        });

        let thread_b = thread::spawn(move || {
            for _ in 0..increments {
                update_file_with_lock(&path_b, |cfg| {
                    let value = cfg.extra.get("b").and_then(|v| v.as_i64()).unwrap_or(0) + 1;
                    cfg.extra.insert("b".to_string(), json!(value));
                    true
                })
                .unwrap();
            }
        });

        thread_a.join().unwrap();
        thread_b.join().unwrap();

        let final_config = load_file(&path);
        assert_eq!(
            final_config.extra.get("a").and_then(|v| v.as_i64()),
            Some(100)
        );
        assert_eq!(
            final_config.extra.get("b").and_then(|v| v.as_i64()),
            Some(100)
        );
    }

    #[test]
    fn acquire_config_lock_times_out_when_already_held() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("config.json");

        let _lock = acquire_config_lock(&path, 0, Duration::from_millis(0)).unwrap();
        match acquire_config_lock(&path, 0, Duration::from_millis(0)) {
            Ok(_) => panic!("expected lock acquisition to fail while lock is held"),
            Err(err) => assert!(err
                .to_string()
                .contains("timed out waiting for config lock")),
        }
    }
}
