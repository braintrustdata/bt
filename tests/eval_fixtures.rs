use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

fn test_lock() -> MutexGuard<'static, ()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
        .lock()
        .unwrap_or_else(|err| err.into_inner())
}

#[test]
fn eval_fixtures() {
    let _guard = test_lock();
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
        match runtime {
            "node" => ensure_dependencies(&dir),
            "bun" => ensure_dependencies(&dir),
            "deno" => ensure_dependencies(&dir),
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
            if needs_deno(runtime, runner.as_deref()) && !command_exists("deno") {
                if required_runtimes().contains("deno") {
                    panic!("Deno runtime is required but unavailable for fixture {fixture_name}");
                }
                let label = runner.as_deref().unwrap_or("default");
                eprintln!("Skipping {fixture_name} [{label}] (deno not installed).");
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
            let status = cmd.status().expect("run bt eval");
            assert!(
                status.success() == expect_success,
                "Fixture {fixture_name} [{}] had status {status} (expected success={expect_success})",
                runner.as_deref().unwrap_or("default")
            );
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

#[test]
fn eval_watch_js_dependency_retriggers() {
    let _guard = test_lock();
    if !command_exists("node") {
        if required_runtimes().contains("node") {
            panic!("node runtime is required but unavailable for watch test");
        }
        eprintln!("Skipping eval_watch_js_dependency_retriggers (node not installed).");
        return;
    }

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixtures_root = root.join("tests").join("evals");
    let fixture_dir = fixtures_root.join("js").join("eval-ts-cjs");
    ensure_dependencies(&fixture_dir);

    let bt_path = bt_binary_path(&root);
    let runner = resolve_runner(&fixture_dir, Some("tsx"), None).expect("resolve js runner");

    assert_watch_detects_dependency_change(
        &bt_path,
        &fixture_dir,
        &runner,
        "tests/async-import.eval.ts",
        "tests/helper.js",
    );
}

#[test]
fn eval_watch_bun_dependency_retriggers() {
    let _guard = test_lock();
    if !command_exists("bun") {
        if required_runtimes().contains("bun") {
            panic!("bun runtime is required but unavailable for watch test");
        }
        eprintln!("Skipping eval_watch_bun_dependency_retriggers (bun not installed).");
        return;
    }

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixtures_root = root.join("tests").join("evals");
    let fixture_dir = fixtures_root.join("js").join("eval-ts-cjs");
    ensure_dependencies(&fixture_dir);

    let bt_path = bt_binary_path(&root);

    assert_watch_detects_dependency_change(
        &bt_path,
        &fixture_dir,
        "bun",
        "tests/async-import.eval.ts",
        "tests/helper.js",
    );
}

#[test]
fn eval_watch_deno_dependency_retriggers() {
    let _guard = test_lock();
    if !command_exists("deno") {
        if required_runtimes().contains("deno") {
            panic!("deno runtime is required but unavailable for watch test");
        }
        eprintln!("Skipping eval_watch_deno_dependency_retriggers (deno not installed).");
        return;
    }

    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixtures_root = root.join("tests").join("evals");
    let fixture_dir = fixtures_root.join("js").join("eval-deno");
    ensure_dependencies(&fixture_dir);

    let bt_path = bt_binary_path(&root);

    assert_watch_detects_dependency_change(
        &bt_path,
        &fixture_dir,
        "deno",
        "tests/basic.eval.ts",
        "tests/helper.ts",
    );
}

#[test]
fn eval_watch_python_dependency_retriggers() {
    let _guard = test_lock();
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixtures_root = root.join("tests").join("evals");
    let fixture_dir = fixtures_root.join("py").join("local_import");

    let python = ensure_python_env(&fixtures_root.join("py"))
        .expect("python runtime unavailable for watch dependency test");
    let bt_path = bt_binary_path(&root);

    assert_watch_detects_dependency_change(
        &bt_path,
        &fixture_dir,
        python.to_string_lossy().as_ref(),
        "eval_local_import.py",
        "helper.py",
    );
}

fn read_fixture_config(path: &Path) -> FixtureConfig {
    let raw = fs::read_to_string(path).expect("read fixture.json");
    serde_json::from_str(&raw).expect("parse fixture.json")
}

fn bt_binary_path(root: &Path) -> PathBuf {
    match std::env::var("CARGO_BIN_EXE_bt") {
        Ok(path) => PathBuf::from(path),
        Err(_) => {
            let candidate = root.join("target").join("debug").join("bt");
            if !candidate.is_file() {
                build_bt_binary(root);
            }
            candidate
        }
    }
}

struct FileRestoreGuard {
    path: PathBuf,
    original: Vec<u8>,
}

impl FileRestoreGuard {
    fn new(path: PathBuf) -> Self {
        let original = fs::read(&path).expect("read original file bytes");
        Self { path, original }
    }
}

impl Drop for FileRestoreGuard {
    fn drop(&mut self) {
        let _ = fs::write(&self.path, &self.original);
    }
}

fn assert_watch_detects_dependency_change(
    bt_path: &Path,
    fixture_dir: &Path,
    runner: &str,
    entry_file: &str,
    dependency_file: &str,
) {
    let dep_path = fixture_dir.join(dependency_file);
    let _restore_guard = FileRestoreGuard::new(dep_path.clone());

    let mut cmd = Command::new(bt_path);
    cmd.arg("eval")
        .arg("--watch")
        .arg("--no-send-logs")
        .arg("--runner")
        .arg(runner)
        .arg(entry_file)
        .current_dir(fixture_dir)
        .env("BT_EVAL_LOCAL", "1")
        .env(
            "BRAINTRUST_API_KEY",
            std::env::var("BRAINTRUST_API_KEY").unwrap_or_else(|_| "local".to_string()),
        )
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn bt eval --watch");
    let output = Arc::new(Mutex::new(String::new()));
    let mut threads = Vec::new();

    if let Some(stdout) = child.stdout.take() {
        threads.push(spawn_output_collector(stdout, Arc::clone(&output)));
    }
    if let Some(stderr) = child.stderr.take() {
        threads.push(spawn_output_collector(stderr, Arc::clone(&output)));
    }

    wait_for_output(
        &mut child,
        &output,
        "Waiting for changes...",
        Duration::from_secs(45),
    );

    let marker_prefix = if dep_path.extension().and_then(|ext| ext.to_str()) == Some("py") {
        "#"
    } else {
        "//"
    };
    let marker = format!(
        "\n{marker_prefix} bt-watch-test-{}\n",
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before epoch")
            .as_nanos()
    );
    let mut updated = fs::read_to_string(&dep_path).expect("read dependency file");
    updated.push_str(&marker);
    fs::write(&dep_path, updated).expect("modify dependency file");

    wait_for_output(
        &mut child,
        &output,
        "Detected changes in",
        Duration::from_secs(45),
    );
    let dep_name = dep_path
        .file_name()
        .and_then(|value| value.to_str())
        .expect("dependency file name");
    wait_for_output(&mut child, &output, dep_name, Duration::from_secs(45));
    wait_for_output(
        &mut child,
        &output,
        "Re-running evals.",
        Duration::from_secs(45),
    );

    let _ = child.kill();
    let _ = child.wait();
    for handle in threads {
        let _ = handle.join();
    }
}

fn spawn_output_collector<R>(reader: R, output: Arc<Mutex<String>>) -> thread::JoinHandle<()>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        let mut buffered = BufReader::new(reader);
        let mut line = String::new();
        loop {
            line.clear();
            match buffered.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let mut guard = output.lock().expect("output lock");
                    guard.push_str(&line);
                }
                Err(_) => break,
            }
        }
    })
}

fn wait_for_output(
    child: &mut Child,
    output: &Arc<Mutex<String>>,
    needle: &str,
    timeout: Duration,
) {
    let started = Instant::now();
    loop {
        if output.lock().expect("output lock").contains(needle) {
            return;
        }

        if let Some(status) = child.try_wait().expect("try_wait") {
            let captured = output.lock().expect("output lock").clone();
            panic!(
                "watch process exited early with status {status} while waiting for '{needle}'.\n{captured}"
            );
        }

        if started.elapsed() > timeout {
            let captured = output.lock().expect("output lock").clone();
            panic!("timed out waiting for '{needle}'.\n{captured}");
        }

        thread::sleep(Duration::from_millis(100));
    }
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

fn needs_deno(runtime: &str, runner: Option<&str>) -> bool {
    runtime == "deno" || runner == Some("deno")
}

fn required_runtimes() -> BTreeSet<String> {
    std::env::var("BT_EVAL_REQUIRED_RUNTIMES")
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

    if required.contains("deno") && !command_exists("deno") {
        panic!("deno runtime is required but not installed");
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
