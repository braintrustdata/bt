use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct FixtureConfig {
    files: Vec<String>,
    runtime: Option<String>,
    runner: Option<String>,
    env: Option<BTreeMap<String, String>>,
}

#[test]
fn eval_fixtures() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixtures_root = root.join("tests").join("evals");
    if !fixtures_root.exists() {
        eprintln!("No eval fixtures found.");
        return;
    }

    let bt_path = match std::env::var("CARGO_BIN_EXE_bt") {
        Ok(path) => PathBuf::from(path),
        Err(_) => {
            let candidate = root.join("target").join("debug").join("bt");
            if !candidate.is_file() {
                build_bt_binary(&root);
            }
            candidate
        }
    };

    let mut fixture_dirs: Vec<PathBuf> = Vec::new();
    for runtime_dir in ["js", "py"] {
        let root_dir = fixtures_root.join(runtime_dir);
        if !root_dir.exists() {
            continue;
        }
        let mut dirs: Vec<PathBuf> = fs::read_dir(&root_dir)
            .expect("read fixtures dir")
            .filter_map(|entry| entry.ok())
            .map(|entry| entry.path())
            .filter(|path| path.is_dir())
            .collect();
        fixture_dirs.append(&mut dirs);
    }
    fixture_dirs.sort();

    let mut ran_any = false;
    for dir in fixture_dirs {
        let config_path = dir.join("fixture.json");
        if !config_path.exists() {
            continue;
        }
        ran_any = true;

        let config = read_fixture_config(&config_path);
        if config.files.is_empty() {
            panic!(
                "Fixture {} has no files configured.",
                dir.file_name().unwrap().to_string_lossy()
            );
        }

        let runtime = config.runtime.as_deref().unwrap_or("node");
        match runtime {
            "node" => {
                ensure_dependencies(&dir);
            }
            "bun" => {
                if !command_exists("bun") {
                    eprintln!(
                        "Skipping {} (bun not installed).",
                        dir.file_name().unwrap().to_string_lossy()
                    );
                    continue;
                }
                ensure_dependencies(&dir);
            }
            "python" => {
                let python = match find_python() {
                    Some(python) => python,
                    None => {
                        eprintln!(
                            "Skipping {} (python not installed).",
                            dir.file_name().unwrap().to_string_lossy()
                        );
                        continue;
                    }
                };
                if !python_can_import_braintrust(&python) {
                    eprintln!(
                        "Skipping {} (braintrust not installed in python).",
                        dir.file_name().unwrap().to_string_lossy()
                    );
                    continue;
                }
            }
            other => {
                panic!(
                    "Unsupported runtime for fixture {}: {other}",
                    dir.file_name().unwrap().to_string_lossy()
                );
            }
        }

        let mut cmd = Command::new(&bt_path);
        cmd.arg("eval");
        if let Some(runner) = config.runner.as_ref() {
            cmd.arg("--runner").arg(runner);
        }
        cmd.args(&config.files).current_dir(&dir);
        cmd.env("BT_EVAL_LOCAL", "1");
        cmd.env(
            "BRAINTRUST_API_KEY",
            std::env::var("BRAINTRUST_API_KEY").unwrap_or_else(|_| "local".to_string()),
        );
        if let Some(env) = config.env {
            for (key, value) in env {
                cmd.env(key, value);
            }
        }

        let tsx_path = dir.join("node_modules").join(".bin").join("tsx");
        if tsx_path.is_file() {
            cmd.env("BT_EVAL_JS_RUNNER", tsx_path);
        }

        if runtime == "python" {
            if let Some(python) = find_python() {
                cmd.env("BT_EVAL_PYTHON_RUNNER", python);
            }
        }

        let status = cmd.status().expect("run bt eval");
        assert!(
            status.success(),
            "Fixture {} failed with status {status}",
            dir.file_name().unwrap().to_string_lossy()
        );
    }

    if !ran_any {
        eprintln!("No eval fixtures with fixture.json found.");
    }
}

fn read_fixture_config(path: &Path) -> FixtureConfig {
    let raw = fs::read_to_string(path).expect("read fixture.json");
    serde_json::from_str(&raw).expect("parse fixture.json")
}

fn ensure_dependencies(dir: &Path) {
    let package_json = dir.join("package.json");
    if !package_json.exists() {
        return;
    }

    let node_modules = dir.join("node_modules");
    if node_modules.exists() {
        return;
    }

    if command_exists("pnpm") {
        let status = Command::new("pnpm")
            .args(["install", "--ignore-scripts", "--no-lockfile"])
            .current_dir(dir)
            .status()
            .expect("pnpm install");
        if !status.success() {
            panic!("pnpm install failed for {}", dir.display());
        }
        return;
    }

    let status = Command::new("npm")
        .args(["install", "--ignore-scripts", "--no-package-lock"])
        .current_dir(dir)
        .status()
        .expect("npm install");
    if !status.success() {
        panic!("npm install failed for {}", dir.display());
    }
}

fn build_bt_binary(root: &Path) {
    let status = Command::new("cargo")
        .args(["build", "--bin", "bt"])
        .current_dir(root)
        .status()
        .expect("cargo build --bin bt");
    if !status.success() {
        panic!("cargo build --bin bt failed");
    }
}

fn command_exists(command: &str) -> bool {
    let paths = match std::env::var_os("PATH") {
        Some(paths) => paths,
        None => return false,
    };

    for dir in std::env::split_paths(&paths) {
        let candidate = dir.join(command);
        if candidate.is_file() {
            return true;
        }
    }

    false
}

fn find_python() -> Option<String> {
    for candidate in ["python3", "python"] {
        if command_exists(candidate) {
            return Some(candidate.to_string());
        }
    }
    None
}

fn python_can_import_braintrust(python: &str) -> bool {
    Command::new(python)
        .args(["-c", "import braintrust"])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}
