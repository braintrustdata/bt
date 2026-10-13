#![cfg(unix)]

use assert_cmd::Command;
use serde_json::{json, Value};
use std::fs;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::process::{Child, Stdio};
use std::time::{Duration, Instant};

struct TestDaemon(Child);

impl Drop for TestDaemon {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}

struct UpdateFixture {
    root: tempfile::TempDir,
    exe: PathBuf,
}

impl UpdateFixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let bin = root.path().join(".local/bin");
        fs::create_dir_all(&bin).unwrap();
        let exe = bin.join("bt");
        fs::copy(assert_cmd::cargo::cargo_bin!("bt"), &exe).unwrap();
        let fixture = Self { root, exe };
        // Replace the running executable as the real installer does. The
        // replacement is another copy of the actual bt under test.
        fixture.installer(
            "cp \"$BT_TEST_BINARY\" \"$HOME/.local/bin/bt.new\" && mv \"$HOME/.local/bin/bt.new\" \"$HOME/.local/bin/bt\"",
        );
        fixture
    }

    fn installer(&self, script: &str) {
        let curl = self.root.path().join(".local/bin/curl");
        fs::write(
            &curl,
            format!("#!/bin/sh\ncat <<'INSTALLER'\n{script}\nINSTALLER\n"),
        )
        .unwrap();
        fs::set_permissions(curl, fs::Permissions::from_mode(0o755)).unwrap();
    }

    fn write(&self, path: &str, content: &str) {
        let path = self.root.path().join(path);
        fs::create_dir_all(path.parent().unwrap()).unwrap();
        fs::write(path, content).unwrap();
    }

    fn command(&self) -> Command {
        let mut command = Command::new(&self.exe);
        command
            .env_clear()
            .env("HOME", self.root.path())
            .env("XDG_CONFIG_HOME", self.root.path().join(".config"))
            .env("BT_DAEMON_SOCKET", self.root.path().join("daemon.sock"))
            .env("BT_TEST_BINARY", assert_cmd::cargo::cargo_bin!("bt"))
            .env(
                "PATH",
                format!(
                    "{}:/usr/bin:/bin",
                    self.root.path().join(".local/bin").display()
                ),
            )
            .current_dir(self.root.path())
            .args(["update", "--channel", "canary", "--json", "--no-input"]);
        command
    }

    fn start_daemon(&self) -> TestDaemon {
        let socket = self.root.path().join("daemon.sock");
        let daemon = TestDaemon(
            std::process::Command::new(&self.exe)
                .env_clear()
                .env("HOME", self.root.path())
                .args(["trace", "daemon", "--idle-timeout-secs", "0", "--socket"])
                .arg(&socket)
                .arg("--data-dir")
                .arg(self.root.path().join("daemon-data"))
                .stdin(Stdio::null())
                .stdout(Stdio::null())
                .stderr(Stdio::null())
                .spawn()
                .unwrap(),
        );
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            tokio::time::timeout(Duration::from_secs(10), async {
                loop {
                    if bt_daemon::run_status(bt_daemon::StatusArgs {
                        socket: Some(socket.clone()),
                        session_id: None,
                    })
                    .await
                    .unwrap()
                    .is_some()
                    {
                        break;
                    }
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
            })
            .await
            .expect("daemon must become ready");
        });
        daemon
    }

    fn configure_opencode(&self) {
        self.write(".config/opencode/braintrust.json", ROUTE);
        self.write(
            ".config/opencode/opencode.json",
            &json!({
                "plugin": ["test-neighbor-plugin", "@braintrust/trace-opencode@0.0.0"],
                "model": "test-model"
            })
            .to_string(),
        );
    }

    fn opencode(&self) -> Value {
        serde_json::from_slice(
            &fs::read(self.root.path().join(".config/opencode/opencode.json")).unwrap(),
        )
        .unwrap()
    }

    fn assert_opencode_updated(&self) {
        let config = self.opencode();
        assert_eq!(config["model"], "test-model");
        let plugins = config["plugin"].as_array().unwrap();
        assert_eq!(plugins[0], "test-neighbor-plugin");
        let plugin = plugins[1].as_str().unwrap();
        assert!(plugin.starts_with("@braintrust/trace-opencode@"));
        assert_ne!(plugin, "@braintrust/trace-opencode@0.0.0");
        assert_eq!(
            fs::read_to_string(self.root.path().join(".config/opencode/braintrust.json")).unwrap(),
            ROUTE
        );
    }
}

// Disabled tracing is still an installed plugin; updating must not enable it
// or rewrite the saved destination, profile, metadata, or user transforms.
const ROUTE: &str = r#"{"trace_to_braintrust":false,"route":{"auth":{"profile":"test-profile"},"project_name":"test-project","additional_metadata":{"test":true},"plugins":["test-transform.js"]}}"#;

#[test]
fn update_refreshes_enabled_plugins_without_changing_routes_or_installing_other_agents() {
    let fixture = UpdateFixture::new();
    fixture.configure_opencode();
    let output = fixture.command().assert().success().get_output().clone();
    assert_eq!(
        serde_json::from_slice::<Value>(&output.stdout).unwrap(),
        json!({"channel": "canary", "status": "completed"})
    );
    fixture.assert_opencode_updated();
    for path in [".codex", ".claude", ".pi", ".grok", ".cursor", ".gemini"] {
        assert!(!fixture.root.path().join(path).exists());
    }
}

#[test]
fn update_attempts_remaining_plugins_after_one_fails() {
    let fixture = UpdateFixture::new();
    fixture.configure_opencode();
    fixture.write(".codex/braintrust.json", ROUTE);
    let output = fixture.command().assert().failure().get_output().clone();
    let error: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(error.to_string().contains("bt trace update codex"));
    fixture.assert_opencode_updated();
}

#[test]
fn failed_bt_install_does_not_update_plugins() {
    let fixture = UpdateFixture::new();
    fixture.configure_opencode();
    fixture.installer("exit 1");
    fixture.command().assert().failure();
    assert_eq!(
        fixture.opencode()["plugin"][1],
        "@braintrust/trace-opencode@0.0.0"
    );
}

#[test]
fn update_without_enabled_plugins_does_not_create_agent_configuration() {
    let fixture = UpdateFixture::new();
    fixture.command().assert().success();
    assert!(!fixture.root.path().join(".config").exists());
}

#[test]
fn update_stops_running_daemon_before_a_failed_install() {
    let fixture = UpdateFixture::new();
    let mut daemon = fixture.start_daemon();
    fixture.installer("exit 1");
    fixture.command().assert().failure();
    let deadline = Instant::now() + Duration::from_secs(5);
    loop {
        if let Some(status) = daemon.0.try_wait().unwrap() {
            assert!(status.success());
            break;
        }
        assert!(Instant::now() < deadline, "upgrade left the daemon running");
        std::thread::sleep(Duration::from_millis(20));
    }
}

#[test]
fn update_aborts_before_changes_when_daemon_stop_fails() {
    let fixture = UpdateFixture::new();
    fixture.configure_opencode();
    fixture.installer("touch \"$HOME/installer-started\"");
    let listener =
        std::os::unix::net::UnixListener::bind(fixture.root.path().join("daemon.sock")).unwrap();
    // A reachable but broken daemon must not be mistaken for an absent daemon.
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        stream.shutdown(std::net::Shutdown::Both).unwrap();
    });
    let output = fixture.command().assert().failure().get_output().clone();
    server.join().unwrap();
    let error: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert!(error.to_string().contains("bt trace stop"));
    assert!(!fixture.root.path().join("installer-started").exists());
    assert_eq!(
        fixture.opencode()["plugin"][1],
        "@braintrust/trace-opencode@0.0.0"
    );
}

#[test]
fn update_skips_plugins_when_settings_path_is_shared_by_all_agents() {
    let fixture = UpdateFixture::new();
    fixture.configure_opencode();
    // A settings override is not agent-specific, so it must not be read as
    // every agent being enabled.
    let output = fixture
        .command()
        .env(
            "BT_DAEMON_CONFIG",
            fixture.root.path().join(".config/opencode/braintrust.json"),
        )
        .assert()
        .success()
        .get_output()
        .clone();
    assert!(String::from_utf8_lossy(&output.stderr).contains("bt trace update opencode"));
    assert_eq!(
        fixture.opencode()["plugin"][1],
        "@braintrust/trace-opencode@0.0.0"
    );
    for path in [".codex", ".claude", ".pi", ".grok", ".cursor", ".gemini"] {
        assert!(!fixture.root.path().join(path).exists());
    }
}
