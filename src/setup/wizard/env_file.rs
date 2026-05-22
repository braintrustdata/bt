use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use ignore::gitignore::GitignoreBuilder;
use serde::Serialize;

const ENV_FILENAME: &str = ".env.braintrust";
const BT_DIR: &str = ".bt";
const BT_CONFIG_FILE: &str = "config.json";

pub struct EnvFileWriteResult {
    pub env_file_path: PathBuf,
    #[allow(dead_code)]
    pub gitignore_path: PathBuf,
    pub added_to_gitignore: bool,
    pub already_covered: bool,
}

pub fn write_env_braintrust(git_root: &Path, api_key: &str) -> Result<EnvFileWriteResult> {
    let env_file_path = git_root.join(ENV_FILENAME);
    write_file_mode_600(
        &env_file_path,
        format!("BRAINTRUST_API_KEY={api_key}\n").as_bytes(),
    )
    .with_context(|| format!("writing {}", env_file_path.display()))?;

    let gitignore_path = git_root.join(".gitignore");
    let existing = read_to_string_if_exists(&gitignore_path)?;

    if gitignore_covers(&existing, ENV_FILENAME) {
        return Ok(EnvFileWriteResult {
            env_file_path,
            gitignore_path,
            added_to_gitignore: false,
            already_covered: true,
        });
    }

    let sep = if existing.is_empty() || existing.ends_with('\n') {
        ""
    } else {
        "\n"
    };
    fs::write(&gitignore_path, format!("{existing}{sep}{ENV_FILENAME}\n"))
        .with_context(|| format!("writing {}", gitignore_path.display()))?;

    Ok(EnvFileWriteResult {
        env_file_path,
        gitignore_path,
        added_to_gitignore: true,
        already_covered: false,
    })
}

#[derive(Debug, Serialize)]
pub struct BtConfig<'a> {
    pub org: &'a str,
    pub project: &'a str,
    pub project_id: &'a str,
}

pub fn write_bt_config(git_root: &Path, config: &BtConfig<'_>) -> Result<()> {
    let dir = git_root.join(BT_DIR);
    fs::create_dir_all(&dir).with_context(|| format!("creating {}", dir.display()))?;
    let config_path = dir.join(BT_CONFIG_FILE);

    let payload = serde_json::json!({
        "profile": serde_json::Value::Null,
        "org": config.org,
        "project": config.project,
        "project_id": config.project_id,
    });
    let mut body =
        serde_json::to_string_pretty(&payload).context("serializing .bt/config.json payload")?;
    body.push('\n');
    fs::write(&config_path, body).with_context(|| format!("writing {}", config_path.display()))?;

    let gitignore_path = git_root.join(".gitignore");
    let existing = read_to_string_if_exists(&gitignore_path)?;
    let target = format!("{BT_DIR}/{BT_CONFIG_FILE}");
    if !gitignore_covers(&existing, &target) {
        let sep = if existing.is_empty() || existing.ends_with('\n') {
            ""
        } else {
            "\n"
        };
        fs::write(&gitignore_path, format!("{existing}{sep}{BT_DIR}/\n"))
            .with_context(|| format!("writing {}", gitignore_path.display()))?;
    }

    Ok(())
}

pub fn gitignore_covers(content: &str, filename: &str) -> bool {
    let mut builder = GitignoreBuilder::new("");
    for line in content.lines() {
        let _ = builder.add_line(None, line);
    }
    let Ok(gi) = builder.build() else {
        return false;
    };
    // matched_path_or_any_parents walks parent dirs so a directory pattern
    // like `.bt/` correctly covers `.bt/config.json`.
    gi.matched_path_or_any_parents(filename, false).is_ignore()
}

fn read_to_string_if_exists(path: &Path) -> Result<String> {
    match fs::read_to_string(path) {
        Ok(s) => Ok(s),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(String::new()),
        Err(e) => Err(e).with_context(|| format!("reading {}", path.display())),
    }
}

#[cfg(unix)]
fn write_file_mode_600(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    use std::os::unix::fs::OpenOptionsExt;
    let mut file = fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)
}

#[cfg(not(unix))]
fn write_file_mode_600(path: &Path, bytes: &[u8]) -> std::io::Result<()> {
    fs::write(path, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn write_env_braintrust_creates_file_and_appends_gitignore() {
        let dir = tempdir().unwrap();
        let result = write_env_braintrust(dir.path(), "sk-test").unwrap();
        let contents = fs::read_to_string(&result.env_file_path).unwrap();
        assert_eq!(contents, "BRAINTRUST_API_KEY=sk-test\n");
        let gi = fs::read_to_string(&result.gitignore_path).unwrap();
        assert!(gi.contains(".env.braintrust"));
        assert!(result.added_to_gitignore);
        assert!(!result.already_covered);
    }

    #[test]
    fn write_env_braintrust_respects_existing_gitignore_coverage() {
        let dir = tempdir().unwrap();
        fs::write(dir.path().join(".gitignore"), "*.braintrust\n").unwrap();
        let result = write_env_braintrust(dir.path(), "sk-test").unwrap();
        assert!(result.already_covered);
        assert!(!result.added_to_gitignore);
    }

    #[test]
    fn write_bt_config_writes_json_and_ignores_dir() {
        let dir = tempdir().unwrap();
        let cfg = BtConfig {
            org: "acme",
            project: "demo",
            project_id: "p_123",
        };
        write_bt_config(dir.path(), &cfg).unwrap();
        let body = fs::read_to_string(dir.path().join(".bt/config.json")).unwrap();
        let parsed: serde_json::Value = serde_json::from_str(&body).unwrap();
        assert_eq!(parsed["org"], "acme");
        assert_eq!(parsed["project"], "demo");
        assert_eq!(parsed["project_id"], "p_123");
        assert!(parsed["profile"].is_null());
        let gi = fs::read_to_string(dir.path().join(".gitignore")).unwrap();
        assert!(gi.contains(".bt/"));
    }

    #[test]
    fn write_bt_config_is_idempotent_against_gitignore() {
        let dir = tempdir().unwrap();
        let cfg = BtConfig {
            org: "acme",
            project: "demo",
            project_id: "p_123",
        };
        write_bt_config(dir.path(), &cfg).unwrap();
        write_bt_config(dir.path(), &cfg).unwrap();
        write_bt_config(dir.path(), &cfg).unwrap();
        let gi = fs::read_to_string(dir.path().join(".gitignore")).unwrap();
        let count = gi.matches(".bt/").count();
        assert_eq!(
            count, 1,
            "gitignore should contain .bt/ exactly once, got:\n{gi}"
        );
    }
}
