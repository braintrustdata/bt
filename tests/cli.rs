use assert_cmd::Command;
use predicates::prelude::*;
use std::fs;
use std::path::Path;

fn bt_command() -> Command {
    Command::cargo_bin("bt").expect("bt binary")
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
fn deprecated_global_quiet_flag_still_parses_for_other_commands() {
    bt_command().args(["status", "--quiet"]).assert().success();
}

#[test]
fn deprecated_global_quiet_flag_still_parses_for_setup_subcommands() {
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
fn setup_uses_codex_detected_on_path_without_explicit_agent() {
    let repo = make_git_repo();
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    write_executable(&bin_dir.path().join("codex"));

    bt_command()
        .current_dir(repo.path())
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
        .stdout(predicate::str::contains("Selected agents: codex"));

    assert!(home
        .path()
        .join(".agents/skills/braintrust/SKILL.md")
        .exists());
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

    bt_command()
        .current_dir(&nested)
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
fn setup_requires_profile_disambiguation_when_multiple_profiles_exist() {
    let repo = make_git_repo();
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    write_auth_store(
        config_home.path(),
        &[("alpha", "alpha-org"), ("beta", "beta-org")],
    );

    bt_command()
        .current_dir(repo.path())
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .env("PATH", bin_dir.path())
        .args(["setup", "--global", "--no-input"])
        .assert()
        .failure()
        .stderr(predicate::str::contains("multiple auth profiles found"))
        .stderr(predicate::str::contains("pass --profile"));
}
