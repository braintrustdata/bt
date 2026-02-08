use std::collections::HashMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

pub fn bootstrap_from_args(args: &[OsString]) -> Result<()> {
    let explicit_env_file = extract_env_file_arg(args);
    load_env(explicit_env_file.as_ref())
}

pub fn load_env(explicit_env_file: Option<&PathBuf>) -> Result<()> {
    let cwd = std::env::current_dir().context("failed to read current directory")?;
    let env_files = resolve_env_files(&cwd, explicit_env_file);
    let mut loaded = HashMap::new();

    for env_file in env_files {
        if !env_file.exists() && explicit_env_file.is_none() {
            continue;
        }

        let parsed = dotenvy::from_path_iter(&env_file)
            .with_context(|| format!("failed to read env file {}", env_file.display()))?;
        for item in parsed {
            let (key, value) =
                item.with_context(|| format!("failed to parse env file {}", env_file.display()))?;
            if std::env::var_os(&key).is_some() {
                continue;
            }
            // Env files are processed from lowest to highest precedence,
            // so later files intentionally override earlier file values.
            loaded.insert(key, value);
        }
    }

    let mut envs: Vec<(String, String)> = loaded.into_iter().collect();
    envs.sort_by(|a, b| a.0.cmp(&b.0));
    for (key, value) in envs {
        std::env::set_var(key, value);
    }
    Ok(())
}

fn extract_env_file_arg(args: &[OsString]) -> Option<PathBuf> {
    let mut explicit = None;
    let mut idx = 1usize;
    while idx < args.len() {
        let Some(arg) = args[idx].to_str() else {
            idx += 1;
            continue;
        };

        if arg == "--" {
            break;
        }

        if arg == "--env-file" {
            if let Some(next) = args.get(idx + 1) {
                explicit = Some(PathBuf::from(next));
            }
            idx += 2;
            continue;
        }

        if let Some(value) = arg.strip_prefix("--env-file=") {
            explicit = Some(PathBuf::from(value));
        }

        idx += 1;
    }
    explicit
}

fn resolve_env_files(cwd: &Path, explicit_env_file: Option<&PathBuf>) -> Vec<PathBuf> {
    if let Some(path) = explicit_env_file {
        let full_path = if path.is_absolute() {
            path.clone()
        } else {
            cwd.join(path)
        };
        return vec![full_path];
    }

    let node_env = std::env::var("NODE_ENV").unwrap_or_else(|_| "development".to_string());
    let mut files = vec![cwd.join(".env"), cwd.join(format!(".env.{node_env}"))];
    if node_env != "test" {
        files.push(cwd.join(".env.local"));
    }
    files.push(cwd.join(format!(".env.{node_env}.local")));
    files
}
