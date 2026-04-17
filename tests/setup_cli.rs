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
fn setup_prints_banner_without_interactive_flag() {
    let mut cmd = Command::cargo_bin("bt").expect("bt binary");
    cmd.env("TERM", "xterm-256color")
        .env_remove("NO_COLOR")
        .args([
            "setup",
            "--no-instrument",
            "--no-skills",
            "--no-mcp",
            "--agent",
            "codex",
        ]);
    cmd.assert()
        .success()
        .stderr(contains("Braintrust"))
        .stderr(contains("\u{1b}[34m"));
}

#[test]
fn setup_no_color_disables_banner_styling() {
    let mut cmd = Command::cargo_bin("bt").expect("bt binary");
    cmd.env("TERM", "xterm-256color")
        .env_remove("NO_COLOR")
        .args([
            "setup",
            "--no-color",
            "--no-instrument",
            "--no-skills",
            "--no-mcp",
            "--agent",
            "codex",
        ]);
    cmd.assert()
        .success()
        .stderr(contains("Braintrust"))
        .stderr(contains("\u{1b}[").not());
}
