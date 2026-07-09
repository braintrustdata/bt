use std::ffi::OsString;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use assert_cmd::cargo::CommandCargoExt;

#[derive(Clone, Copy)]
enum SpawnMode {
    Custom,
    Tsx,
}

struct Scenario {
    name: &'static str,
    mode: &'static str,
    flags: &'static [&'static str],
    spawn_mode: SpawnMode,
    expected_code: i32,
    profile: bool,
}

fn prepend_path(dir: &Path) -> OsString {
    let mut paths = vec![dir.to_path_buf()];
    paths.extend(std::env::split_paths(
        &std::env::var_os("PATH").unwrap_or_default(),
    ));
    std::env::join_paths(paths).expect("construct test PATH")
}

#[cfg(unix)]
fn make_executable(path: &Path) {
    use std::os::unix::fs::PermissionsExt;
    let mut permissions = fs::metadata(path).unwrap().permissions();
    permissions.set_mode(0o755);
    fs::set_permissions(path, permissions).unwrap();
}

#[cfg(not(unix))]
fn make_executable(_path: &Path) {}

fn run_scenario(scenario: &Scenario) -> (Vec<u8>, Vec<u8>, i32) {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixture_root = root.join("tests/fixtures/eval");
    let runner = fixture_root.join("fake_runner.py");
    let frames = fixture_root
        .join("scenarios")
        .join(format!("{}.jsonl", scenario.name));
    let temp = tempfile::tempdir().expect("create scenario temp dir");
    let eval_file = temp.path().join("fixture.eval.ts");
    fs::write(&eval_file, "// The scripted runner ignores this file.\n").unwrap();

    let attempt_file = temp.path().join("attempt");
    let config_dir = temp.path().join("config/bt");
    fs::create_dir_all(&config_dir).unwrap();
    fs::write(
        config_dir.join("auth.json"),
        r#"{"profiles":{"test-profile":{"auth_kind":"api_key","api_url":"https://api.example.test","app_url":"https://example.test","org_name":"test-org"}}}"#,
    )
    .unwrap();
    fs::write(
        config_dir.join("secrets.json"),
        r#"{"secrets":{"test-profile":"test-api-key"}}"#,
    )
    .unwrap();

    let mut command = Command::cargo_bin("bt").expect("locate bt binary");
    command.args(["eval", "--no-color", "--no-send-logs"]);
    if scenario.profile {
        command.args(["--profile", "test-profile"]);
    }
    if scenario.flags.contains(&"--verbose") {
        command.arg("--verbose");
    }
    for flag in scenario.flags {
        if *flag != "--verbose" {
            command.arg(flag);
        }
    }

    match scenario.spawn_mode {
        SpawnMode::Custom => {
            command.arg("--runner").arg(&runner);
        }
        SpawnMode::Tsx => {
            for binary in ["tsx", "vite-node"] {
                let destination = temp.path().join(binary);
                fs::copy(&runner, &destination).unwrap();
                make_executable(&destination);
            }
            command.env("PATH", prepend_path(temp.path()));
        }
    }

    let output = command
        .arg(&eval_file)
        .env("BT_TEST_FRAME_SCRIPT", frames)
        .env("BT_TEST_ATTEMPT_FILE", attempt_file)
        .env("XDG_CONFIG_HOME", temp.path().join("config"))
        .env_remove("BRAINTRUST_API_KEY")
        .env("NO_COLOR", "1")
        .output()
        .expect("run scripted eval");
    (
        output.stdout,
        output.stderr,
        output.status.code().unwrap_or(-1),
    )
}

fn assert_golden(scenario: &Scenario) {
    let (stdout, stderr, code) = run_scenario(scenario);
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let golden_root = root.join("tests/golden/eval");
    let stem = format!("{}--{}", scenario.name, scenario.mode);
    let stdout_path = golden_root.join(format!("{stem}.stdout"));
    let stderr_path = golden_root.join(format!("{stem}.stderr"));

    if std::env::var_os("UPDATE_GOLDENS").is_some() {
        fs::create_dir_all(&golden_root).unwrap();
        fs::write(&stdout_path, &stdout).unwrap();
        fs::write(&stderr_path, &stderr).unwrap();
    }

    assert_eq!(code, scenario.expected_code, "exit code for {stem}");
    assert_eq!(
        stdout,
        fs::read(&stdout_path).unwrap_or_else(|_| panic!(
            "missing {}; run UPDATE_GOLDENS=1 cargo test --test eval_golden",
            stdout_path.display()
        )),
        "stdout differed for {stem}"
    );
    assert_eq!(
        stderr,
        fs::read(&stderr_path).unwrap_or_else(|_| panic!(
            "missing {}; run UPDATE_GOLDENS=1 cargo test --test eval_golden",
            stderr_path.display()
        )),
        "stderr differed for {stem}"
    );
}

#[test]
fn eval_output_matches_characterization_goldens() {
    let scenarios = [
        Scenario {
            name: "happy",
            mode: "default",
            flags: &[],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "happy",
            mode: "jsonl",
            flags: &["--jsonl"],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "happy",
            mode: "verbose",
            flags: &["--verbose"],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "comparison",
            mode: "default",
            flags: &[],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: true,
        },
        Scenario {
            name: "comparison",
            mode: "jsonl",
            flags: &["--jsonl"],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: true,
        },
        Scenario {
            name: "minimal-summary",
            mode: "default",
            flags: &[],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "minimal-summary",
            mode: "jsonl",
            flags: &["--jsonl"],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "errors",
            mode: "default",
            flags: &[],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "errors",
            mode: "verbose",
            flags: &["--verbose"],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "api-key-error",
            mode: "default",
            flags: &[],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "api-key-error",
            mode: "verbose",
            flags: &["--verbose"],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "console",
            mode: "default",
            flags: &[],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "console",
            mode: "jsonl",
            flags: &["--jsonl"],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "console",
            mode: "list",
            flags: &["--list"],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "console",
            mode: "verbose",
            flags: &["--verbose"],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "console",
            mode: "silent",
            flags: &["--reporter=silent"],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "console",
            mode: "events",
            flags: &["--reporter=events"],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "console",
            mode: "two-stdout-reporters",
            flags: &["--reporter=events", "--reporter=jsonl"],
            spawn_mode: SpawnMode::Custom,
            expected_code: 1,
            profile: false,
        },
        Scenario {
            name: "console",
            mode: "unsupported-dot",
            flags: &["--reporter=dot"],
            spawn_mode: SpawnMode::Custom,
            expected_code: 1,
            profile: false,
        },
        Scenario {
            name: "console",
            mode: "tsx-default",
            flags: &[],
            spawn_mode: SpawnMode::Tsx,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "console",
            mode: "tsx-verbose",
            flags: &["--verbose"],
            spawn_mode: SpawnMode::Tsx,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "crash",
            mode: "default",
            flags: &[],
            spawn_mode: SpawnMode::Custom,
            expected_code: 1,
            profile: false,
        },
        Scenario {
            name: "error-exit-zero",
            mode: "default",
            flags: &[],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "empty",
            mode: "default",
            flags: &[],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "unknown",
            mode: "default",
            flags: &[],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "interleaved",
            mode: "default",
            flags: &[],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "interleaved",
            mode: "jsonl",
            flags: &["--jsonl"],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "progress-edge",
            mode: "default",
            flags: &[],
            spawn_mode: SpawnMode::Custom,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "esm-retry",
            mode: "tsx-default",
            flags: &[],
            spawn_mode: SpawnMode::Tsx,
            expected_code: 0,
            profile: false,
        },
        Scenario {
            name: "esm-retry",
            mode: "tsx-verbose",
            flags: &["--verbose"],
            spawn_mode: SpawnMode::Tsx,
            expected_code: 0,
            profile: false,
        },
    ];

    for scenario in scenarios {
        assert_golden(&scenario);
    }
}
