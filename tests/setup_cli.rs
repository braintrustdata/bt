use assert_cmd::prelude::*;
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
