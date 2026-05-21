use assert_cmd::prelude::*;
use predicates::prelude::*;
use predicates::str::contains;
use std::process::Command;

#[test]
fn setup_accepts_deprecated_quiet_flag() {
    let mut cmd = Command::cargo_bin("bt").expect("bt binary");
    cmd.args(["setup", "--quiet", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("Configure Braintrust setup flows"));
}

#[test]
fn setup_help_lists_subcommands() {
    let mut cmd = Command::cargo_bin("bt").expect("bt binary");
    cmd.args(["setup", "--help"]);
    cmd.assert()
        .success()
        .stdout(contains("skills"))
        .stdout(contains("instrument"))
        .stdout(contains("mcp"))
        .stdout(contains("doctor"));
}

#[test]
fn setup_with_no_input_requires_credentials() {
    let mut cmd = Command::cargo_bin("bt").expect("bt binary");
    cmd.args(["setup", "--no-input"]);
    cmd.assert()
        .failure()
        .stderr(contains("credentials required").or(contains("TTY required")));
}

#[test]
fn setup_with_json_is_rejected() {
    let mut cmd = Command::cargo_bin("bt").expect("bt binary");
    cmd.args(["setup", "--json"]);
    cmd.assert()
        .failure()
        .stderr(contains("interactive").and(contains("--json")));
}
