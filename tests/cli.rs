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
fn trace_help_hides_internal_commands_but_keeps_them_callable() {
    bt_command().args(["daemon", "--help"]).assert().failure();
    bt_command().args(["agents", "--help"]).assert().failure();

    bt_command()
        .args(["trace", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("setup"))
        .stdout(predicate::str::contains("\n  daemon").not())
        .stdout(predicate::str::contains("serve").not())
        .stdout(predicate::str::contains("\n  hook").not())
        .stdout(predicate::str::contains("\n  status").not())
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
        .stdout(predicate::str::contains("--experiment-id"));

    bt_command()
        .args(["trace", "status", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--socket"));

    bt_command()
        .args(["trace", "replay", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("<FILE>"));

    bt_command()
        .args(["trace", "setup", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("codex"))
        .stdout(predicate::str::contains("claude"));
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
        .args(["trace", "setup", "codex", "--project", "agent-traces"])
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
    assert_eq!(settings["traceToBraintrust"], true);
    assert_eq!(settings["project"], "agent-traces");
    assert_eq!(settings["flushOnTurnEnd"], true);
    assert_eq!(settings["additionalMetadata"]["team"], "sdk");
    assert_eq!(settings["apiKey"], "legacy-secret");
    assert_eq!(settings["apiUrl"], "https://legacy.example");
    assert_eq!(settings["auth"]["type"], "legacy");
}

#[cfg(unix)]
#[test]
fn trace_setup_claude_installs_plugin_and_creates_default_settings() {
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
        .args(["trace", "setup", "claude"])
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
    assert_eq!(settings["traceToBraintrust"], true);
    assert_eq!(settings["project"], "coding-agents");
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
        .args(["trace", "setup", "claude"])
        .assert()
        .success();

    let calls = fs::read_to_string(log).expect("read fake CLI calls");
    assert!(calls.contains("plugin enable trace-claude-code@braintrust-claude-plugin"));
    assert!(!calls.contains("plugin marketplace add"));
    assert!(!calls.contains("plugin install"));
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
