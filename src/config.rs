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

    eprintln!("DEBUG: loaded config from {}", path.display());

    config
}

pub fn load_global() -> Result<Config> {
    Ok(load_file(&global_path()?))
}

#[allow(dead_code)]
pub fn save_file(path: &Path, config: &Config) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }

    let json = serde_json::to_string_pretty(config)?;
    fs::write(path, json)?;

    Ok(())
}

#[allow(dead_code)]
pub fn save_global(config: &Config) -> Result<()> {
    save_file(&global_path()?, config)
}
