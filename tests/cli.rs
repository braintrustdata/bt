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
fn status_quiet_and_verbose_conflict() {
    bt_command()
        .args(["status", "--quiet", "--verbose"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("cannot be used with"));
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
        .args([
            "setup",
            "--global",
            "--no-instrument",
            "--no-workflow",
            "--no-input",
        ])
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
        .args([
            "setup",
            "--global",
            "--no-instrument",
            "--no-workflow",
            "--no-input",
        ])
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
        .args([
            "setup",
            "--global",
            "--no-instrument",
            "--no-workflow",
            "--no-input",
        ])
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
        .args([
            "setup",
            "--global",
            "--no-instrument",
            "--no-workflow",
            "--no-input",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Selected agents: copilot").not());

    assert!(home
        .path()
        .join(".agents/skills/braintrust/SKILL.md")
        .exists());
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
            "--verbose",
            "--global",
            "--no-instrument",
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
