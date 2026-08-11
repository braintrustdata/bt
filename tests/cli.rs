use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use std::path::Path;

fn bt_command() -> Command {
    Command::cargo_bin("bt").expect("bt binary")
}

fn clear_braintrust_auth_env(cmd: &mut Command) {
    for key in [
        "BRAINTRUST_API_KEY",
        "BRAINTRUST_PROFILE",
        "BRAINTRUST_ORG_NAME",
        "BRAINTRUST_DEFAULT_PROJECT",
    ] {
        cmd.env_remove(key);
    }
}

fn write_executable(path: &Path) {
    fs::write(path, "#!/bin/sh\nexit 0\n").expect("write executable");
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = fs::metadata(path).expect("metadata").permissions();
        perms.set_mode(0o755);
        fs::set_permissions(path, perms).expect("chmod");
    }
}

#[cfg(unix)]
fn write_agent_cli(path: &Path, marketplace_json: &str, plugin_json: &str) {
    let script = format!(
        r#"#!/bin/sh
printf '%s\n' "$*" >> "$AGENT_SETUP_LOG"
case "$*" in
  "plugin marketplace list --json")
    printf '%s\n' '{marketplace_json}'
    ;;
  "plugin list --json")
    printf '%s\n' '{plugin_json}'
    ;;
esac
"#
    );
    fs::write(path, script).expect("write fake agent CLI");
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(path).expect("metadata").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("chmod");
}

#[cfg(unix)]
fn write_run_agent(path: &Path) {
    fs::write(
        path,
        "#!/bin/sh\nprintf '%s\\n' \"$*\" > \"$AGENT_RUN_LOG\"\nprintf '%s\\n' \"$BT_TRACE_INVOCATION_SETTINGS\" > \"$AGENT_RUN_SETTINGS\"\nif [ -n \"$AGENT_RUN_CONFIG\" ]; then printf '%s\\n' \"$OPENCODE_CONFIG_CONTENT\" > \"$AGENT_RUN_CONFIG\"; fi\n",
    )
    .expect("write fake run agent");
    use std::os::unix::fs::PermissionsExt;
    let mut perms = fs::metadata(path).expect("metadata").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("chmod");
}

fn make_git_repo() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    fs::write(dir.path().join(".git"), "gitdir: /tmp/fake").expect("write .git");
    dir
}

fn write_auth_store(config_home: &Path, profiles: &[(&str, &str)]) {
    let auth_dir = config_home.join("bt");
    fs::create_dir_all(&auth_dir).expect("create auth dir");

    let mut entries = Vec::new();
    for (profile, org) in profiles {
        entries.push(format!(
            "\"{profile}\":{{\"auth_kind\":\"api_key\",\"org_name\":\"{org}\"}}"
        ));
    }

    let body = format!("{{\"profiles\":{{{}}}}}", entries.join(","));
    fs::write(auth_dir.join("auth.json"), body).expect("write auth store");
}

#[test]
fn global_quiet_flag_still_parses_for_other_commands() {
    bt_command().args(["status", "--quiet"]).assert().success();
}

#[test]
fn quiet_flag_still_parses_for_setup_subcommands() {
    bt_command()
        .args(["setup", "skills", "--quiet", "--help"])
        .assert()
        .success();
}

#[test]
fn setup_instrument_quiet_no_longer_aliases_background() {
    bt_command()
        .args(["setup", "instrument", "--quiet", "--tui", "--help"])
        .assert()
        .success();
}

#[test]
fn setup_verbose_is_accepted_after_subcommand() {
    bt_command()
        .args(["setup", "skills", "--verbose", "--help"])
        .assert()
        .success();
}

#[test]
fn update_help_exposes_self_update_flags() {
    bt_command()
        .args(["update", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--check"))
        .stdout(predicate::str::contains("--channel"));
}

#[test]
fn self_update_remains_as_hidden_compatibility_path() {
    bt_command()
        .args(["self", "update", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--check"))
        .stdout(predicate::str::contains("--channel"));
}

#[test]
fn top_level_help_shows_update_not_self() {
    bt_command()
        .args(["--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("update       Update bt in-place"))
        .stdout(predicate::str::contains("self         Self-management commands").not());
}

#[test]
fn status_all_json_includes_profile_urls() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let auth_dir = config_home.path().join("bt");
    fs::create_dir_all(&auth_dir).expect("create auth dir");
    fs::write(
        auth_dir.join("auth.json"),
        r#"{"profiles":{"test-profile":{"auth_kind":"oauth","api_url":"https://oauth-api.test.example","app_url":"https://app.test.example","oauth_client_id":"bt_cli_test"}}}"#,
    )
    .expect("write auth store");

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args(["status", "--all", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "\"app_url\":\"https://app.test.example\"",
        ))
        .stdout(predicate::str::contains(
            "\"api_url\":\"https://oauth-api.test.example\"",
        ));
}

#[test]
fn trace_help_exposes_user_commands_and_hides_internal_commands() {
    bt_command().args(["daemon", "--help"]).assert().failure();
    bt_command().args(["agents", "--help"]).assert().failure();
    bt_command()
        .args(["trace", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("setup"))
        .stdout(predicate::str::contains("\n  import"))
        .stdout(predicate::str::contains("\n  run"))
        .stdout(predicate::str::contains("\n  daemon").not())
        .stdout(predicate::str::contains("serve").not())
        .stdout(predicate::str::contains("\n  hook").not())
        .stdout(predicate::str::contains("\n  status").not())
        .stdout(predicate::str::contains("\n  stop").not())
        .stdout(predicate::str::contains("\n  replay").not());

    bt_command()
        .args(["trace", "daemon", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Run the tracing daemon"))
        .stdout(predicate::str::contains("--socket"))
        .stdout(predicate::str::contains("--idle-timeout-secs"));

    bt_command()
        .args(["trace", "hook", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--source"))
        .stdout(predicate::str::contains("--flush-on-turn-end"))
        .stdout(predicate::str::contains("--profile"))
        .stdout(predicate::str::contains("--project"));

    bt_command()
        .args(["trace", "status", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--socket"));

    bt_command()
        .args(["trace", "stop", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--socket"));

    bt_command()
        .args(["trace", "import", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("<SOURCE>"))
        .stdout(predicate::str::contains("<SESSION_ID>"))
        .stdout(predicate::str::contains("codex"))
        .stdout(predicate::str::contains("claude"));

    bt_command()
        .args(["trace", "replay", "--help"])
        .assert()
        .failure();

    bt_command()
        .args(["trace", "setup", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("codex"))
        .stdout(predicate::str::contains("claude"))
        .stdout(predicate::str::contains("opencode"))
        .stdout(predicate::str::contains("pi"));

    bt_command()
        .args(["trace", "run", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("<SOURCE>"))
        .stdout(predicate::str::contains("codex"))
        .stdout(predicate::str::contains("claude"))
        .stdout(predicate::str::contains("opencode"))
        .stdout(predicate::str::contains("pi"));
}

#[test]
fn trace_commands_require_a_project_non_interactively() {
    for args in [
        vec!["trace", "setup", "codex", "--no-input"],
        vec!["trace", "run", "codex", "--no-input"],
        vec![
            "trace",
            "import",
            "codex",
            "00000000-0000-0000-0000-000000000000",
            "--no-input",
        ],
    ] {
        let home = tempfile::tempdir().expect("home tempdir");
        let config_home = tempfile::tempdir().expect("config tempdir");
        let mut cmd = bt_command();
        clear_braintrust_auth_env(&mut cmd);
        cmd.env("HOME", home.path())
            .env("XDG_CONFIG_HOME", config_home.path())
            .args(args)
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "project choice required in non-interactive mode",
            ))
            .stderr(predicate::str::contains("--project <NAME>"));
    }
}

#[cfg(unix)]
#[test]
fn trace_run_uses_the_invocation_project_without_changing_setup() {
    let home = tempfile::tempdir().expect("home tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let run_log = state_dir.path().join("run.log");
    let run_settings = state_dir.path().join("run-settings.json");
    let setup_settings = state_dir.path().join("setup-settings.json");
    write_run_agent(&bin_dir.path().join("codex"));

    bt_command()
        .env("HOME", home.path())
        .env("PATH", bin_dir.path())
        .env("AGENT_RUN_LOG", &run_log)
        .env("AGENT_RUN_SETTINGS", &run_settings)
        .env("BT_DAEMON_CONFIG", &setup_settings)
        .args([
            "trace",
            "run",
            "codex",
            "--project",
            "invocation-project",
            "--",
            "--version",
        ])
        .assert()
        .success();

    let args = fs::read_to_string(run_log).expect("read run args");
    assert!(args.contains("--version"));
    let settings: serde_json::Value =
        serde_json::from_slice(&fs::read(run_settings).expect("read invocation settings"))
            .expect("parse invocation settings");
    assert_eq!(
        settings["route"]["destination"]["project_name"],
        "invocation-project"
    );
    assert!(
        !setup_settings.exists(),
        "managed run must not change persistent setup settings"
    );
}

#[cfg(unix)]
#[test]
fn trace_run_opencode_injects_the_npm_plugin_without_changing_global_config() {
    let home = tempfile::tempdir().expect("home tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let run_log = state_dir.path().join("run.log");
    let run_settings = state_dir.path().join("run-settings.json");
    let run_config = state_dir.path().join("run-config.json");
    let global_config = home.path().join(".config/opencode/braintrust.json");
    fs::create_dir_all(global_config.parent().expect("config parent"))
        .expect("create config parent");
    fs::write(&global_config, r#"{"trace_to_braintrust":true}"#).expect("seed global config");
    write_run_agent(&bin_dir.path().join("opencode"));

    bt_command()
        .env("HOME", home.path())
        .env("OPENCODE_BIN", bin_dir.path().join("opencode"))
        .env("AGENT_RUN_LOG", &run_log)
        .env("AGENT_RUN_SETTINGS", &run_settings)
        .env("AGENT_RUN_CONFIG", &run_config)
        .args([
            "trace",
            "run",
            "opencode",
            "--project",
            "isolated-opencode",
            "--",
            "--version",
        ])
        .assert()
        .success();

    let inline: serde_json::Value =
        serde_json::from_slice(&fs::read(run_config).expect("read OpenCode inline config"))
            .expect("parse OpenCode inline config");
    assert_eq!(
        inline["plugin"],
        serde_json::json!(["@braintrust/trace-opencode@^1"])
    );
    let settings: serde_json::Value =
        serde_json::from_slice(&fs::read(run_settings).expect("read invocation settings"))
            .expect("parse invocation settings");
    assert_eq!(
        settings["route"]["destination"]["project_name"],
        "isolated-opencode"
    );
    assert_eq!(
        fs::read_to_string(global_config).expect("read global config"),
        r#"{"trace_to_braintrust":true}"#
    );
}

#[cfg(unix)]
#[test]
fn trace_run_pi_injects_the_npm_extension_for_only_that_process() {
    let home = tempfile::tempdir().expect("home tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let run_log = state_dir.path().join("run.log");
    let run_settings = state_dir.path().join("run-settings.json");
    let global_config = home.path().join(".pi/agent/braintrust.json");
    fs::create_dir_all(global_config.parent().expect("config parent"))
        .expect("create config parent");
    fs::write(&global_config, r#"{"trace_to_braintrust":true}"#).expect("seed global config");
    write_run_agent(&bin_dir.path().join("pi"));

    bt_command()
        .env("HOME", home.path())
        .env("PI_BIN", bin_dir.path().join("pi"))
        .env("AGENT_RUN_LOG", &run_log)
        .env("AGENT_RUN_SETTINGS", &run_settings)
        .args([
            "trace",
            "run",
            "pi",
            "--project",
            "isolated-pi",
            "--",
            "--version",
        ])
        .assert()
        .success();

    assert!(fs::read_to_string(run_log)
        .expect("read Pi arguments")
        .contains("-e npm:@braintrust/pi-extension@^1 --version"));
    let settings: serde_json::Value =
        serde_json::from_slice(&fs::read(run_settings).expect("read invocation settings"))
            .expect("parse invocation settings");
    assert_eq!(
        settings["route"]["destination"]["project_name"],
        "isolated-pi"
    );
    assert_eq!(
        fs::read_to_string(global_config).expect("read global config"),
        r#"{"trace_to_braintrust":true}"#
    );
}

#[cfg(unix)]
#[test]
fn trace_stop_gracefully_stops_an_isolated_daemon() {
    use std::process::Stdio;
    use std::thread;
    use std::time::Duration;

    let state = tempfile::tempdir().expect("state tempdir");
    let socket = state.path().join("daemon.sock");
    let bin = env!("CARGO_BIN_EXE_bt");
    let mut daemon = std::process::Command::new(bin)
        .args([
            "trace",
            "daemon",
            "--socket",
            socket.to_str().expect("UTF-8 socket path"),
            "--data-dir",
            state.path().to_str().expect("UTF-8 state path"),
            "--idle-timeout-secs",
            "0",
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn tracing daemon");

    for _ in 0..100 {
        if socket.exists() {
            break;
        }
        thread::sleep(Duration::from_millis(25));
    }
    if !socket.exists() {
        let _ = daemon.kill();
        panic!("tracing daemon did not create its socket");
    }

    bt_command()
        .args([
            "trace",
            "stop",
            "--socket",
            socket.to_str().expect("UTF-8 socket path"),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Tracing daemon stopped."));

    for _ in 0..100 {
        if let Some(status) = daemon.try_wait().expect("poll tracing daemon") {
            assert!(status.success(), "tracing daemon exited unsuccessfully");

            bt_command()
                .args([
                    "trace",
                    "stop",
                    "--socket",
                    socket.to_str().expect("UTF-8 socket path"),
                ])
                .assert()
                .success()
                .stdout(predicate::str::contains("No tracing daemon is running."));
            return;
        }
        thread::sleep(Duration::from_millis(25));
    }

    let _ = daemon.kill();
    panic!("tracing daemon did not stop");
}

#[test]
fn trace_status_and_stop_honor_global_json_when_daemon_is_absent() {
    let state = tempfile::tempdir().expect("state tempdir");
    #[cfg(unix)]
    let socket = state.path().join("missing.sock");
    #[cfg(windows)]
    let socket = std::path::PathBuf::from(format!(
        r"\\.\pipe\missing-bt-trace-{}",
        uuid::Uuid::new_v4()
    ));

    for (command, expected) in [
        (
            "status",
            serde_json::json!({"command":"status","running":false,"sessions":[]}),
        ),
        (
            "stop",
            serde_json::json!({"command":"stop","running":false,"stopped":false}),
        ),
    ] {
        let stdout = bt_command()
            .args([
                "trace",
                command,
                "--json",
                "--socket",
                socket.to_str().expect("UTF-8 socket path"),
            ])
            .assert()
            .success()
            .get_output()
            .stdout
            .clone();
        let output: serde_json::Value =
            serde_json::from_slice(&stdout).expect("trace command emits JSON");
        assert_eq!(output, expected);
    }
}

#[cfg(unix)]
#[test]
fn trace_setup_codex_installs_plugin_and_preserves_existing_settings() {
    let home = tempfile::tempdir().expect("home tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let log = state_dir.path().join("codex.log");
    let config = state_dir.path().join("config.json");
    write_agent_cli(
        &bin_dir.path().join("codex"),
        r#"{"marketplaces":[]}"#,
        r#"{"installed":[]}"#,
    );
    fs::write(
        &config,
        r#"{
          "flushOnTurnEnd": true,
          "additionalMetadata": {"team": "sdk"},
          "apiKey": "legacy-secret",
          "apiUrl": "https://legacy.example",
          "auth": {"type": "legacy"}
        }"#,
    )
    .expect("seed config");

    bt_command()
        .env("HOME", home.path())
        .env("PATH", bin_dir.path())
        .env("AGENT_SETUP_LOG", &log)
        .env("BT_DAEMON_CONFIG", &config)
        .args([
            "trace",
            "setup",
            "codex",
            "--profile",
            "test-profile",
            "--org",
            "test-org",
            "--project",
            "agent-traces",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "The Braintrust tracing plugin is installed for Codex",
        ));

    let calls = fs::read_to_string(log).expect("read fake CLI calls");
    assert!(calls.contains("plugin marketplace add braintrustdata/braintrust-codex-plugin"));
    assert!(calls.contains("plugin add trace-codex@braintrust-codex-plugins"));

    let settings: serde_json::Value =
        serde_json::from_slice(&fs::read(config).expect("read config")).expect("parse config");
    assert_eq!(settings["trace_to_braintrust"], true);
    assert!(settings.get("traceToBraintrust").is_none());
    assert_eq!(
        settings["route"]["destination"]["project_name"],
        "agent-traces"
    );
    assert_eq!(settings["route"]["auth"]["profile"], "test-profile");
    assert_eq!(settings["route"]["auth"]["org_name"], "test-org");
    assert_eq!(settings["flushOnTurnEnd"], true);
    assert_eq!(settings["additionalMetadata"]["team"], "sdk");
    assert_eq!(settings["apiKey"], "legacy-secret");
    assert_eq!(settings["apiUrl"], "https://legacy.example");
    assert_eq!(settings["auth"]["type"], "legacy");
}

#[cfg(unix)]
#[test]
fn trace_setup_claude_installs_plugin_and_writes_selected_project() {
    let home = tempfile::tempdir().expect("home tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let log = state_dir.path().join("claude.log");
    let config = state_dir.path().join("config.json");
    write_agent_cli(&bin_dir.path().join("claude"), "[]", "[]");

    bt_command()
        .env("HOME", home.path())
        .env("PATH", bin_dir.path())
        .env("AGENT_SETUP_LOG", &log)
        .env("BT_DAEMON_CONFIG", &config)
        .args(["trace", "setup", "claude", "--project", "coding-agents"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "The Braintrust tracing plugin is installed for Claude Code",
        ));

    let calls = fs::read_to_string(log).expect("read fake CLI calls");
    assert!(calls.contains("plugin marketplace add braintrustdata/braintrust-claude-plugin"));
    assert!(calls.contains("plugin install trace-claude-code@braintrust-claude-plugin"));

    let settings: serde_json::Value =
        serde_json::from_slice(&fs::read(config).expect("read config")).expect("parse config");
    assert_eq!(settings["trace_to_braintrust"], true);
    assert_eq!(
        settings["route"]["destination"]["project_name"],
        "coding-agents"
    );
}

#[cfg(unix)]
#[test]
fn trace_setup_claude_enables_an_existing_disabled_plugin() {
    let home = tempfile::tempdir().expect("home tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let log = state_dir.path().join("claude.log");
    write_agent_cli(
        &bin_dir.path().join("claude"),
        r#"[{"name":"braintrust-claude-plugin"}]"#,
        r#"[{"id":"trace-claude-code@braintrust-claude-plugin","enabled":false}]"#,
    );

    bt_command()
        .env("HOME", home.path())
        .env("PATH", bin_dir.path())
        .env("AGENT_SETUP_LOG", &log)
        .args(["trace", "setup", "claude", "--project", "coding-agents"])
        .assert()
        .success();

    let calls = fs::read_to_string(log).expect("read fake CLI calls");
    assert!(calls.contains("plugin enable trace-claude-code@braintrust-claude-plugin"));
    assert!(!calls.contains("plugin marketplace add"));
    assert!(!calls.contains("plugin install"));
}

#[test]
fn trace_setup_opencode_configures_the_npm_plugin_and_selected_route() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let opencode_dir = config_home.path().join("opencode");
    fs::create_dir_all(&opencode_dir).expect("create OpenCode config dir");
    fs::write(
        opencode_dir.join("opencode.json"),
        r#"{"plugin":["other-plugin","@braintrust/trace-opencode@0.9.0"],"model":"test/model"}"#,
    )
    .expect("seed OpenCode config");

    bt_command()
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args([
            "trace",
            "setup",
            "open-code",
            "--profile",
            "work",
            "--org",
            "acme",
            "--project",
            "agent-traces",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("installed for OpenCode"));

    let opencode: serde_json::Value = serde_json::from_slice(
        &fs::read(opencode_dir.join("opencode.json")).expect("read OpenCode config"),
    )
    .expect("parse OpenCode config");
    assert_eq!(opencode["model"], "test/model");
    assert_eq!(
        opencode["plugin"],
        serde_json::json!(["other-plugin", "@braintrust/trace-opencode@^1"])
    );

    let settings: serde_json::Value = serde_json::from_slice(
        &fs::read(opencode_dir.join("braintrust.json")).expect("read OpenCode settings"),
    )
    .expect("parse OpenCode settings");
    assert_eq!(settings["trace_to_braintrust"], true);
    assert_eq!(settings["route"]["auth"]["profile"], "work");
    assert_eq!(settings["route"]["auth"]["org_name"], "acme");
    assert_eq!(
        settings["route"]["destination"]["project_name"],
        "agent-traces"
    );
}

#[test]
fn trace_setup_honors_global_json() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let stdout = bt_command()
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args([
            "trace",
            "setup",
            "opencode",
            "--project",
            "agent-traces",
            "--json",
        ])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let output: serde_json::Value =
        serde_json::from_slice(&stdout).expect("trace setup emits JSON");
    assert_eq!(output["command"], "setup");
    assert_eq!(output["source"], "opencode");
    assert_eq!(output["display_name"], "OpenCode");
    assert_eq!(output["restart_required"], true);
    assert_eq!(
        output["settings_path"],
        config_home
            .path()
            .join("opencode/braintrust.json")
            .to_string_lossy()
            .as_ref()
    );
}

#[cfg(unix)]
#[test]
fn trace_setup_pi_installs_the_npm_extension_and_selected_route() {
    let home = tempfile::tempdir().expect("home tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let log = state_dir.path().join("pi.log");
    write_agent_cli(&bin_dir.path().join("pi"), "{}", "{}");

    bt_command()
        .env("HOME", home.path())
        .env("PATH", bin_dir.path())
        .env("AGENT_SETUP_LOG", &log)
        .args(["trace", "setup", "pi", "--project", "pi-traces"])
        .assert()
        .success()
        .stdout(predicate::str::contains("installed for Pi"));

    assert_eq!(
        fs::read_to_string(log).expect("read Pi calls").trim(),
        "install npm:@braintrust/pi-extension@^1"
    );
    let settings: serde_json::Value = serde_json::from_slice(
        &fs::read(home.path().join(".pi/agent/braintrust.json")).expect("read Pi settings"),
    )
    .expect("parse Pi settings");
    assert_eq!(settings["trace_to_braintrust"], true);
    assert_eq!(
        settings["route"]["destination"]["project_name"],
        "pi-traces"
    );
}

#[cfg(unix)]
#[test]
fn trace_setup_keeps_each_agents_persistent_selection_independent() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let log = tempfile::NamedTempFile::new().expect("setup log");
    write_agent_cli(
        &bin_dir.path().join("codex"),
        r#"{"marketplaces":[]}"#,
        r#"{"installed":[]}"#,
    );

    bt_command()
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .env("PATH", bin_dir.path())
        .env("AGENT_SETUP_LOG", log.path())
        .args(["trace", "setup", "codex", "--project", "codex-project"])
        .assert()
        .success();
    bt_command()
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args([
            "trace",
            "setup",
            "opencode",
            "--project",
            "opencode-project",
        ])
        .assert()
        .success();

    let codex: serde_json::Value = serde_json::from_slice(
        &fs::read(home.path().join(".codex/braintrust.json")).expect("read Codex settings"),
    )
    .expect("parse Codex settings");
    let opencode: serde_json::Value = serde_json::from_slice(
        &fs::read(config_home.path().join("opencode/braintrust.json"))
            .expect("read OpenCode settings"),
    )
    .expect("parse OpenCode settings");
    assert_eq!(
        codex["route"]["destination"]["project_name"],
        "codex-project"
    );
    assert_eq!(
        opencode["route"]["destination"]["project_name"],
        "opencode-project"
    );
}

#[test]
fn topics_report_help_accepts_global_org_short_conflict_free() {
    bt_command()
        .args([
            "topics",
            "report",
            "--profile",
            "test-profile",
            "--id",
            "fn_123",
            "--help",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("--id"))
        .stdout(predicate::str::contains("--output"));
}

#[test]
fn status_quiet_and_verbose_conflict() {
    bt_command()
        .args(["status", "--quiet", "--verbose"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn status_json_keeps_local_org_when_global_profile_has_different_org() {
    let repo = make_git_repo();
    fs::create_dir_all(repo.path().join(".bt")).expect("create local bt dir");
    fs::write(
        repo.path().join(".bt/config.json"),
        r#"{"profile":null,"org":"local-org","project":"local-project","project_id":null}"#,
    )
    .expect("write local config");

    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let global_bt_dir = config_home.path().join("bt");
    fs::create_dir_all(&global_bt_dir).expect("create global bt dir");
    fs::write(
        global_bt_dir.join("config.json"),
        r#"{"profile":"default-profile","org":"profile-org"}"#,
    )
    .expect("write global config");
    write_auth_store(config_home.path(), &[("default-profile", "profile-org")]);

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.current_dir(repo.path())
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args(["status", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains(r#""org":"local-org""#))
        .stdout(predicate::str::contains(r#""project":"local-project""#))
        .stdout(predicate::str::contains(r#""profile":"default-profile""#))
        .stdout(predicate::str::contains(r#""org":"profile-org""#).not());
}

#[test]
fn setup_quiet_and_verbose_conflict() {
    bt_command()
        .args([
            "setup",
            "--quiet",
            "--verbose",
            "--no-instrument",
            "--global",
            "--agent",
            "codex",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
}

#[test]
fn setup_instrument_accepts_no_workflow_flag() {
    bt_command()
        .args(["setup", "instrument", "--no-workflow", "--help"])
        .assert()
        .success();
}

#[test]
fn setup_instrument_accepts_deprecated_agents_alias() {
    bt_command()
        .args(["setup", "instrument", "--agents", "codex", "--help"])
        .assert()
        .success();
}

#[test]
fn util_version_to_time_accepts_pagination_key_with_utc() {
    bt_command()
        .args([
            "util",
            "version",
            "to-time",
            "p07639577379371417602",
            "--utc",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("2026-05-14T03:01:58Z"));
}

#[test]
fn util_version_from_time_can_output_pagination_key() {
    bt_command()
        .args([
            "util",
            "version",
            "from-time",
            "2026-05-14T08:00:09-07:00",
            "--pagination-key",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("p07639762451734462464"));
}

#[test]
fn setup_uses_codex_detected_on_path_without_explicit_agent() {
    let repo = make_git_repo();
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    write_executable(&bin_dir.path().join("codex"));

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.current_dir(repo.path())
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .env("PATH", bin_dir.path())
        .args(["setup", "skills", "--global", "--no-workflow", "--no-input"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Selected agents: codex").not());

    assert!(home
        .path()
        .join(".agents/skills/braintrust/SKILL.md")
        .exists());
}

#[test]
fn setup_uses_gemini_detected_on_path_without_explicit_agent() {
    let repo = make_git_repo();
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    write_executable(&bin_dir.path().join("gemini"));

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.current_dir(repo.path())
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .env("PATH", bin_dir.path())
        .args(["setup", "skills", "--global", "--no-workflow", "--no-input"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Selected agents: gemini").not());

    assert!(home
        .path()
        .join(".agents/skills/braintrust/SKILL.md")
        .exists());
}

#[test]
fn setup_uses_qwen_detected_on_path_without_explicit_agent() {
    let repo = make_git_repo();
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    write_executable(&bin_dir.path().join("qwen"));

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.current_dir(repo.path())
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .env("PATH", bin_dir.path())
        .args(["setup", "skills", "--global", "--no-workflow", "--no-input"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Selected agents: qwen").not());

    assert!(home
        .path()
        .join(".agents/skills/braintrust/SKILL.md")
        .exists());
}

#[test]
fn setup_uses_copilot_detected_on_path_without_explicit_agent() {
    let repo = make_git_repo();
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    write_executable(&bin_dir.path().join("copilot"));

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.current_dir(repo.path())
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .env("PATH", bin_dir.path())
        .args(["setup", "skills", "--global", "--no-workflow", "--no-input"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Selected agents: copilot").not());

    assert!(home
        .path()
        .join(".agents/skills/braintrust/SKILL.md")
        .exists());
    assert!(home.path().join(".copilot/skills").exists());
}

#[test]
fn setup_verbose_prints_agent_summary() {
    let repo = make_git_repo();
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    write_executable(&bin_dir.path().join("codex"));

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.current_dir(repo.path())
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .env("PATH", bin_dir.path())
        .args([
            "setup",
            "skills",
            "--verbose",
            "--global",
            "--no-workflow",
            "--no-input",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Selected agents: codex"));
}

#[test]
fn setup_no_instrument_does_not_require_auth_in_git_repo() {
    let repo = make_git_repo();
    let nested = repo.path().join("nested");
    fs::create_dir_all(&nested).expect("create nested");

    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    write_executable(&bin_dir.path().join("codex"));

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.current_dir(&nested)
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .env("PATH", bin_dir.path())
        .args([
            "setup",
            "--global",
            "--no-instrument",
            "--no-workflow",
            "--no-input",
        ])
        .assert()
        .success();
}

#[test]
fn setup_interactive_no_instrument_does_not_require_auth_in_git_repo() {
    let repo = make_git_repo();
    let nested = repo.path().join("nested");
    fs::create_dir_all(&nested).expect("create nested");

    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    write_executable(&bin_dir.path().join("codex"));

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.current_dir(&nested)
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .env("PATH", bin_dir.path())
        .args([
            "setup",
            "--interactive",
            "--global",
            "--agent",
            "codex",
            "--skills",
            "--no-mcp",
            "--no-instrument",
            "--no-input",
        ])
        .assert()
        .success();
}

#[test]
fn setup_accepts_no_skill_alias() {
    bt_command()
        .args(["setup", "--no-skill", "--help"])
        .assert()
        .success();
}

#[test]
fn setup_mcp_only_requires_auth_in_non_interactive_mode() {
    let repo = make_git_repo();
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    write_executable(&bin_dir.path().join("codex"));
    write_auth_store(
        config_home.path(),
        &[("alpha", "alpha-org"), ("beta", "beta-org")],
    );

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.current_dir(repo.path())
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .env("PATH", bin_dir.path())
        .args([
            "setup",
            "--global",
            "--mcp",
            "--no-skills",
            "--no-instrument",
            "--no-input",
        ])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "profile selection required in non-interactive mode",
        ));
}

#[test]
fn datasets_requires_profile_selection_when_multiple_profiles_exist() {
    let repo = make_git_repo();
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    write_auth_store(
        config_home.path(),
        &[("alpha", "alpha-org"), ("beta", "beta-org")],
    );

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.current_dir(repo.path())
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args(["datasets", "--no-input"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("multiple auth profiles available"))
        .stderr(predicate::str::contains("--profile <NAME>"))
        .stderr(predicate::str::contains("alpha"))
        .stderr(predicate::str::contains("beta"));
}
