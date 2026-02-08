use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
struct FixtureConfig {
    files: Vec<String>,
    runtime: Option<String>,
    runner: Option<String>,
    runners: Option<Vec<String>>,
    env: Option<BTreeMap<String, String>>,
    args: Option<Vec<String>>,
    expect_success: Option<bool>,
}

#[test]
fn eval_fixtures() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixtures_root = root.join("tests").join("evals");
    if !fixtures_root.exists() {
        eprintln!("No eval fixtures found.");
        return;
    }

    enforce_required_runtimes(&fixtures_root);

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
    let selected_runtimes = selected_fixture_runtimes();

    let mut ran_any = false;
    for dir in fixture_dirs {
        let config_path = dir.join("fixture.json");
        if !config_path.exists() {
            continue;
        }
        ran_any = true;

        let config = read_fixture_config(&config_path);
        let fixture_name = dir.file_name().unwrap().to_string_lossy().to_string();
        if config.files.is_empty() {
            panic!("Fixture {fixture_name} has no files configured.");
        }

        let runtime = config.runtime.as_deref().unwrap_or("node");
        if let Some(selected) = selected_runtimes.as_ref() {
            if !selected.contains(runtime) {
                eprintln!("Skipping {fixture_name} (runtime {runtime} filtered out).");
                continue;
            }
        }
        match runtime {
            "node" => ensure_dependencies(&dir),
            "bun" => ensure_dependencies(&dir),
            "python" => {}
            other => panic!("Unsupported runtime for fixture {fixture_name}: {other}"),
        }

        let python_runner = if runtime == "python" {
            match ensure_python_env(&fixtures_root.join("py")) {
                Some(python) => Some(python),
                None => {
                    if required_runtimes().contains("python") {
                        panic!(
                            "Python runtime is required but unavailable for fixture {fixture_name}"
                        );
                    }
                    eprintln!("Skipping {fixture_name} (uv/python not available).");
                    continue;
                }
            }
        } else {
            None
        };

        let runners = collect_runners(&config);
        let mut ran_variant = false;
        for runner in runners {
            if needs_bun(runtime, runner.as_deref()) && !command_exists("bun") {
                if required_runtimes().contains("bun") {
                    panic!("Bun runtime is required but unavailable for fixture {fixture_name}");
                }
                let label = runner.as_deref().unwrap_or("default");
                eprintln!("Skipping {fixture_name} [{label}] (bun not installed).");
                continue;
            }

            let mut cmd = Command::new(&bt_path);
            cmd.arg("eval");
            if let Some(args) = config.args.as_ref() {
                cmd.args(args);
            }
            if let Some(runner_cmd) =
                resolve_runner(&dir, runner.as_deref(), python_runner.as_ref())
            {
                cmd.arg("--runner").arg(runner_cmd);
            }
            cmd.args(&config.files).current_dir(&dir);
            cmd.env("BT_EVAL_LOCAL", "1");
            cmd.env(
                "BRAINTRUST_API_KEY",
                std::env::var("BRAINTRUST_API_KEY").unwrap_or_else(|_| "local".to_string()),
            );

            if let Some(env) = config.env.as_ref() {
                for (key, value) in env {
                    cmd.env(key, value);
                }
            }

            if let Some(tsx_path) = local_tsx_path(&dir) {
                cmd.env("BT_EVAL_RUNNER", tsx_path);
            }

            if let Some(python) = python_runner.as_ref() {
                cmd.env("BT_EVAL_PYTHON_RUNNER", python);
            }

            let expect_success = config.expect_success.unwrap_or(true);
            let output = cmd.output().expect("run bt eval");
            let status = output.status;
            if status.success() != expect_success {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stderr = String::from_utf8_lossy(&output.stderr);
                panic!(
                    "Fixture {fixture_name} [{}] had status {status} (expected success={expect_success})\nstdout:\n{stdout}\nstderr:\n{stderr}",
                    runner.as_deref().unwrap_or("default")
                );
            }
            ran_variant = true;
        }

        if !ran_variant {
            eprintln!("Skipping {fixture_name} (no runnable variants).")
        }
    }

    if !ran_any {
        eprintln!("No eval fixtures with fixture.json found.");
    }
}

fn read_fixture_config(path: &Path) -> FixtureConfig {
    let raw = fs::read_to_string(path).expect("read fixture.json");
    serde_json::from_str(&raw).expect("parse fixture.json")
}

fn collect_runners(config: &FixtureConfig) -> Vec<Option<String>> {
    if let Some(runners) = config.runners.as_ref() {
        return runners
            .iter()
            .map(|value| {
                if value == "default" {
                    None
                } else {
                    Some(value.clone())
                }
            })
            .collect();
    }

    vec![config.runner.clone()]
}

fn resolve_runner(dir: &Path, runner: Option<&str>, python: Option<&PathBuf>) -> Option<String> {
    let runner = runner?;

    if runner == "tsx" {
        if let Some(tsx_path) = local_tsx_path(dir) {
            return Some(tsx_path.to_string_lossy().to_string());
        }
    }

    if (runner == "python" || runner == "python3") && python.is_some() {
        return python.map(|path| path.to_string_lossy().to_string());
    }

    Some(runner.to_string())
}

fn local_tsx_path(dir: &Path) -> Option<PathBuf> {
    let tsx_path = dir.join("node_modules").join(".bin").join("tsx");
    tsx_path.is_file().then_some(tsx_path)
}

fn needs_bun(runtime: &str, runner: Option<&str>) -> bool {
    runtime == "bun" || runner == Some("bun")
}

fn required_runtimes() -> BTreeSet<String> {
    parse_runtime_list("BT_EVAL_REQUIRED_RUNTIMES")
}

fn selected_fixture_runtimes() -> Option<BTreeSet<String>> {
    let selected = parse_runtime_list("BT_EVAL_FIXTURE_RUNTIMES");
    (!selected.is_empty()).then_some(selected)
}

fn parse_runtime_list(env_var: &str) -> BTreeSet<String> {
    std::env::var(env_var)
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(|value| value.to_ascii_lowercase())
        .collect()
}

fn enforce_required_runtimes(fixtures_root: &Path) {
    let required = required_runtimes();

    if required.contains("node") && !command_exists("node") {
        panic!("node runtime is required but not installed");
    }

    if required.contains("bun") && !command_exists("bun") {
        panic!("bun runtime is required but not installed");
    }

    if required.contains("python") {
        let python = ensure_python_env(&fixtures_root.join("py"))
            .expect("python runtime is required but uv/python is unavailable");
        assert!(
            python_can_import_braintrust(python.to_string_lossy().as_ref()),
            "python runtime is required but braintrust package is unavailable"
        );
    }
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

fn ensure_python_env(fixtures_root: &Path) -> Option<PathBuf> {
    if !command_exists("uv") {
        return None;
    }

    let venv_dir = fixtures_root.join(".venv");
    let python = venv_python_path(&venv_dir);

    if !python.is_file() {
        let status = Command::new("uv")
            .args(["venv", venv_dir.to_string_lossy().as_ref()])
            .status()
            .ok()?;
        if !status.success() {
            return None;
        }
    }

    if !python_can_import_braintrust(python.to_string_lossy().as_ref()) {
        let status = Command::new("uv")
            .args([
                "pip",
                "install",
                "--python",
                python.to_string_lossy().as_ref(),
                "braintrust",
            ])
            .status()
            .ok()?;
        if !status.success() {
            return None;
        }
    }

    Some(python)
}

fn venv_python_path(venv: &Path) -> PathBuf {
    if cfg!(windows) {
        venv.join("Scripts").join("python.exe")
    } else {
        venv.join("bin").join("python")
    }
}

fn python_can_import_braintrust(python: &str) -> bool {
    Command::new(python)
        .args(["-c", "import braintrust"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}
