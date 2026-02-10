use anyhow::{anyhow, Result};
use std::{
    fs, io,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::ui::{print_command_status, CommandStatus};

#[derive(Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub org: Option<String>,
    pub project: Option<String>,
    pub api_url: Option<String>,
    pub app_url: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

impl Config {
    fn merge(&self, other: &Config) -> Config {
        Config {
            org: other.org.clone().or_else(|| self.org.clone()),
            project: other.project.clone().or_else(|| self.project.clone()),
            api_url: other.api_url.clone().or_else(|| self.api_url.clone()),
            app_url: other.app_url.clone().or_else(|| self.app_url.clone()),
            extra: Default::default(), // Don't merge extras
        }
    }
}

pub fn global_path() -> Result<PathBuf> {
    let home = dirs::home_dir().ok_or_else(|| anyhow!("Could not determine home directory"))?;
    Ok(home.join(".bt").join("config.json"))
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
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let json = serde_json::to_string_pretty(config)?;
    fs::write(path, json)?;

    Ok(())
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

pub fn save_local(config: &Config, create_dir: bool) -> Result<()> {
    let dir = std::env::current_dir()?.join(".bt");
    if create_dir && !dir.exists() {
        fs::create_dir_all(&dir)?;
    }
    save_file(&dir.join("config.json"), config)
}
