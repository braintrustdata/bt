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
        "BRAINTRUST_API_URL",
        "BRAINTRUST_APP_URL",
        "BRAINTRUST_PROFILE",
        "BRAINTRUST_ORG_NAME",
        "BRAINTRUST_DEFAULT_PROJECT",
    ] {
        cmd.env_remove(key);
    }
}

/// Setup, managed run, and import resolve a Braintrust credential and org
/// before writing a route, so those tests supply a synthetic one rather than
/// depending on whatever auth the ambient environment happens to carry.
fn bt_trace_command(config_home: &Path, profile: &str, org: &str) -> Command {
    write_auth_store(config_home, &[(profile, org)]);
    write_profile_secrets(config_home, &[profile]);
    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.env("XDG_CONFIG_HOME", config_home)
        .env("BRAINTRUST_PROFILE", profile)
        .env("BRAINTRUST_ORG_NAME", org);
    cmd
}

fn bt_trace_environment_command(config_home: &Path) -> Command {
    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.env("XDG_CONFIG_HOME", config_home)
        .env("BRAINTRUST_API_KEY", "test-api-key")
        .env("BRAINTRUST_ORG_NAME", "test-org");
    cmd
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
        "#!/bin/sh\nprintf '%s\\n' \"$*\" > \"$AGENT_RUN_LOG\"\nprintf '%s\\n' \"$BT_TRACE_INVOCATION_SETTINGS\" > \"$AGENT_RUN_SETTINGS\"\nif [ -n \"$AGENT_RUN_DAEMON_ENV\" ]; then printf '%s\\n%s\\n' \"$BT_DAEMON_SOCKET\" \"$BT_DAEMON_DATA_DIR\" > \"$AGENT_RUN_DAEMON_ENV\"; fi\nif [ -n \"$AGENT_RUN_CONFIG\" ]; then printf '%s\\n' \"$OPENCODE_CONFIG_CONTENT\" > \"$AGENT_RUN_CONFIG\"; fi\n",
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

/// Record a default org the way `bt init` and `bt switch` do.
#[cfg(unix)]
fn write_config_org(config_home: &Path, org: &str) {
    let config_dir = config_home.join("bt");
    fs::create_dir_all(&config_dir).expect("create config dir");
    fs::write(
        config_dir.join("config.json"),
        format!("{{\"org\":\"{org}\"}}"),
    )
    .expect("write config");
}

/// Store a synthetic credential for each profile so commands that resolve a
/// credential (rather than only listing profiles) can run offline.
fn write_profile_secrets(config_home: &Path, profiles: &[&str]) {
    let auth_dir = config_home.join("bt");
    fs::create_dir_all(&auth_dir).expect("create auth dir");

    let entries: Vec<String> = profiles
        .iter()
        .map(|profile| format!("\"{profile}\":\"test-api-key\""))
        .collect();
    let body = format!("{{\"secrets\":{{{}}}}}", entries.join(","));
    fs::write(auth_dir.join("secrets.json"), body).expect("write secret store");
}

#[cfg(unix)]
fn use_fake_credential_store(cmd: &mut Command, bin_dir: &Path) {
    fs::create_dir_all(bin_dir).expect("create fake credential bin dir");
    let security = bin_dir.join("security");
    write_executable(&security);
    fs::write(
        &security,
        "#!/bin/sh\ncase \"$1\" in\n  find-generic-password|delete-generic-password) exit 44 ;;\n  *) exit 1 ;;\nesac\n",
    )
    .expect("write fake security");

    let secret_tool = bin_dir.join("secret-tool");
    write_executable(&secret_tool);
    fs::write(
        &secret_tool,
        "#!/bin/sh\nif [ \"$1\" = store ]; then cat >/dev/null; fi\nexit 1\n",
    )
    .expect("write fake secret-tool");

    let mut paths = vec![bin_dir.to_path_buf()];
    if let Some(path) = std::env::var_os("PATH") {
        paths.extend(std::env::split_paths(&path));
    }
    cmd.env("PATH", std::env::join_paths(paths).expect("join PATH"));
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
        .stdout(predicate::str::contains("update"))
        .stdout(predicate::str::contains("Update bt in-place"))
        .stdout(predicate::str::contains("self         Self-management commands").not());
}

#[test]
fn top_level_help_shows_profiles() {
    bt_command()
        .args(["--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("profiles"));
    bt_command()
        .args(["--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "Manage saved Braintrust login profiles",
        ));
}

#[test]
fn profiles_list_json_reads_saved_profiles_without_login() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let auth_dir = config_home.path().join("bt");
    fs::create_dir_all(&auth_dir).expect("create auth dir");
    fs::write(
        auth_dir.join("auth.json"),
        r#"{"profiles":{"oauth-profile":{"auth_kind":"oauth","api_url":"https://oauth-api.test.example","app_url":"https://app.test.example","user_name":"Test User","email":"user@test.example"},"test-profile":{"auth_kind":"api_key","app_url":"https://app.test.example","org_name":"test-org","org_bound":true,"api_key_hint":"sk-****test"}}}"#,
    )
    .expect("write auth store");

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args(["profiles", "list", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"name\":\"oauth-profile\""))
        .stdout(predicate::str::contains("\"auth\":\"oauth\""))
        .stdout(predicate::str::contains("\"name\":\"test-profile\""))
        .stdout(predicate::str::contains("\"org\":\"test-org\""));
}

#[test]
fn profiles_list_uses_email_column() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let auth_dir = config_home.path().join("bt");
    fs::create_dir_all(&auth_dir).expect("create auth dir");
    fs::write(
        auth_dir.join("auth.json"),
        r#"{"profiles":{"test-profile":{"auth_kind":"oauth","app_url":"https://app.test.example","email":"user@test.example"}}}"#,
    )
    .expect("write auth store");

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.env("NO_COLOR", "1")
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args(["profiles", "list"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Email"))
        .stdout(predicate::str::contains("user@test.example"))
        .stdout(predicate::str::contains("Identity / org").not());
}

#[test]
fn status_verbose_explicitly_shows_unset_profile() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args(["status", "--verbose"])
        .assert()
        .success()
        .stdout(predicate::str::contains("profile: (unset)"));
}

#[test]
fn bare_status_does_not_render_the_all_profiles_report() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args(["status"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Saved login profiles").not())
        .stdout(predicate::str::contains("Credential precedence").not())
        .stdout(predicate::str::contains("Profile metadata").not())
        .stdout(predicate::str::contains("Secret storage").not());
}

#[test]
fn status_all_only_shows_precedence_for_an_active_override() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");

    let mut without_override = bt_command();
    clear_braintrust_auth_env(&mut without_override);
    without_override
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args(["status", "--all"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Saved login profiles"))
        .stdout(predicate::str::contains("Credential precedence").not());

    let mut with_override = bt_command();
    clear_braintrust_auth_env(&mut with_override);
    with_override
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .env("BRAINTRUST_API_KEY", "synthetic-api-key")
        .args(["status", "--all"])
        .assert()
        .success()
        .stdout(predicate::str::contains("Credential precedence"))
        .stdout(predicate::str::contains(
            "BRAINTRUST_API_KEY overrides saved profiles",
        ));
}

#[cfg(unix)]
#[test]
fn profiles_delete_removes_metadata_and_credentials() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let fake_bin = tempfile::tempdir().expect("fake bin tempdir");
    write_auth_store(
        config_home.path(),
        &[("test-profile", "test-org"), ("other-profile", "other-org")],
    );
    write_profile_secrets(config_home.path(), &["test-profile", "other-profile"]);
    fs::write(
        config_home.path().join("bt/config.json"),
        r#"{"profile":"test-profile","org":"test-org"}"#,
    )
    .expect("write global config");

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    use_fake_credential_store(&mut cmd, fake_bin.path());
    cmd.env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args(["profiles", "delete", "test-profile", "--force", "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            r#"{"name":"test-profile","status":"deleted"}"#,
        ));

    let auth: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(config_home.path().join("bt/auth.json")).expect("read auth store"),
    )
    .expect("parse auth store");
    assert!(auth["profiles"].get("test-profile").is_none());
    assert!(auth["profiles"].get("other-profile").is_some());

    let secrets: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(config_home.path().join("bt/secrets.json")).expect("read secret store"),
    )
    .expect("parse secret store");
    assert!(secrets["secrets"].get("test-profile").is_none());
    assert!(secrets["secrets"].get("other-profile").is_some());

    let config: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(config_home.path().join("bt/config.json")).expect("read config"),
    )
    .expect("parse config");
    assert!(config["profile"].is_null());
    assert_eq!(config["org"], "test-org");
}

#[cfg(unix)]
#[test]
fn logout_all_removes_every_saved_login_without_revoking_credentials() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let fake_bin = tempfile::tempdir().expect("fake bin tempdir");
    write_auth_store(
        config_home.path(),
        &[
            ("first-profile", "first-org"),
            ("second-profile", "second-org"),
        ],
    );
    write_profile_secrets(config_home.path(), &["first-profile", "second-profile"]);

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    use_fake_credential_store(&mut cmd, fake_bin.path());
    let output = cmd
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args(["logout", "--all", "--force", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let result: serde_json::Value =
        serde_json::from_slice(&output).expect("parse logout JSON output");
    assert_eq!(result["status"], "deleted");
    assert_eq!(result["results"].as_array().unwrap().len(), 2);
    assert!(result["results"]
        .as_array()
        .unwrap()
        .iter()
        .all(|entry| entry["revoked"] == false));

    let auth: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(config_home.path().join("bt/auth.json")).expect("read auth store"),
    )
    .expect("parse auth store");
    assert!(auth["profiles"].as_object().unwrap().is_empty());

    let secrets: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(config_home.path().join("bt/secrets.json")).expect("read secret store"),
    )
    .expect("parse secret store");
    assert!(secrets["secrets"].as_object().unwrap().is_empty());
}

#[test]
fn logout_all_requires_force_without_a_terminal() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    write_auth_store(config_home.path(), &[("test-profile", "test-org")]);

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args(["logout", "--all"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "rerun with --force in non-interactive mode",
        ));
}

#[cfg(unix)]
#[test]
fn profiles_rename_moves_credentials_and_updates_config() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let fake_bin = tempfile::tempdir().expect("fake bin tempdir");
    write_auth_store(config_home.path(), &[("old-profile", "test-org")]);
    write_profile_secrets(config_home.path(), &["old-profile"]);
    fs::write(
        config_home.path().join("bt/config.json"),
        r#"{"profile":"old-profile","org":"test-org"}"#,
    )
    .expect("write global config");

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    use_fake_credential_store(&mut cmd, fake_bin.path());
    cmd.env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args([
            "profiles",
            "rename",
            "old-profile",
            "renamed-profile",
            "--json",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            r#"{"name":"renamed-profile","previous_name":"old-profile","status":"renamed"}"#,
        ));

    let auth: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(config_home.path().join("bt/auth.json")).expect("read auth store"),
    )
    .expect("parse auth store");
    assert!(auth["profiles"].get("old-profile").is_none());
    assert!(auth["profiles"].get("renamed-profile").is_some());
    assert!(auth["profile_ids"].get("old-profile").is_none());
    assert!(uuid::Uuid::parse_str(
        auth["profile_ids"]["renamed-profile"]
            .as_str()
            .expect("stable profile ID"),
    )
    .is_ok());

    let secrets: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(config_home.path().join("bt/secrets.json")).expect("read secret store"),
    )
    .expect("parse secret store");
    assert!(secrets["secrets"].get("old-profile").is_none());
    assert_eq!(secrets["secrets"]["renamed-profile"], "test-api-key");

    let config: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(config_home.path().join("bt/config.json")).expect("read config"),
    )
    .expect("parse config");
    assert_eq!(config["profile"], "renamed-profile");
    assert_eq!(config["org"], "test-org");
}

#[cfg(unix)]
#[test]
fn profiles_rename_moves_oauth_credentials() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let fake_bin = tempfile::tempdir().expect("fake bin tempdir");
    let auth_dir = config_home.path().join("bt");
    fs::create_dir_all(&auth_dir).expect("create auth dir");
    fs::write(
        auth_dir.join("auth.json"),
        r#"{"profiles":{"old-oauth":{"auth_kind":"oauth","api_url":"https://oauth-api.test.example","app_url":"https://app.test.example","oauth_client_id":"bt_cli_test"}}}"#,
    )
    .expect("write auth store");
    fs::write(
        auth_dir.join("secrets.json"),
        r#"{"secrets":{"oauth_refresh::old-oauth":"test-refresh-token","oauth_access::old-oauth":"test-access-token"}}"#,
    )
    .expect("write secret store");

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    use_fake_credential_store(&mut cmd, fake_bin.path());
    cmd.env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .args(["profiles", "rename", "old-oauth", "renamed-oauth", "--json"])
        .assert()
        .success();

    let secrets: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(auth_dir.join("secrets.json")).expect("read secret store"),
    )
    .expect("parse secret store");
    assert!(secrets["secrets"].get("oauth_refresh::old-oauth").is_none());
    assert!(secrets["secrets"].get("oauth_access::old-oauth").is_none());
    assert_eq!(
        secrets["secrets"]["oauth_refresh::renamed-oauth"],
        "test-refresh-token"
    );
    assert_eq!(
        secrets["secrets"]["oauth_access::renamed-oauth"],
        "test-access-token"
    );
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
        .stdout(predicate::str::contains("\n  enable"))
        .stdout(predicate::str::contains("\n  doctor"))
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
        .stdout(predicate::str::contains("[SESSION_ID]..."))
        .stdout(predicate::str::contains("--all"))
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
        write_auth_store(config_home.path(), &[("test-profile", "test-org")]);
        write_profile_secrets(config_home.path(), &["test-profile"]);
        let mut cmd = bt_command();
        clear_braintrust_auth_env(&mut cmd);
        cmd.env("HOME", home.path())
            .env("XDG_CONFIG_HOME", config_home.path())
            .env("BRAINTRUST_PROFILE", "test-profile")
            .env("BRAINTRUST_ORG_NAME", "test-org")
            .args(args)
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "project choice required in non-interactive mode",
            ))
            .stderr(predicate::str::contains("--project <NAME>"));
    }
}

/// A default org comes from `--org`/`BRAINTRUST_ORG_NAME`, the config file
/// `bt init` and `bt switch` write, or an org-bound API key profile. A bare
/// API key in the environment has none of those, so tracing has to ask.
/// Non-interactively it says so instead of failing inside the trace runtime.
#[test]
fn trace_commands_require_an_org_when_the_credential_resolves_none() {
    for args in [
        vec![
            "trace",
            "setup",
            "codex",
            "--project",
            "test-project",
            "--no-input",
        ],
        vec![
            "trace",
            "run",
            "codex",
            "--project",
            "test-project",
            "--no-input",
        ],
    ] {
        let home = tempfile::tempdir().expect("home tempdir");
        let config_home = tempfile::tempdir().expect("config tempdir");
        let mut cmd = bt_command();
        clear_braintrust_auth_env(&mut cmd);
        if args[1] == "setup" {
            let auth_dir = config_home.path().join("bt");
            fs::create_dir_all(&auth_dir).expect("create auth dir");
            fs::write(
                auth_dir.join("auth.json"),
                r#"{"profiles":{"test-profile":{"auth_kind":"api_key"}}}"#,
            )
            .expect("write unbound profile");
            write_profile_secrets(config_home.path(), &["test-profile"]);
            cmd.env("BRAINTRUST_PROFILE", "test-profile");
        } else {
            cmd.env("BRAINTRUST_API_KEY", "test-api-key");
        }
        cmd.env("HOME", home.path())
            .env("XDG_CONFIG_HOME", config_home.path())
            .args(args)
            .assert()
            .failure()
            .stderr(predicate::str::contains(
                "organization choice required in non-interactive mode",
            ))
            .stderr(predicate::str::contains("--org <NAME>"));
    }
}

/// The other half: the org `bt init` and `bt switch` record in the config file
/// is adopted without asking. `--no-input` makes any attempt to prompt a
/// failure rather than a hang.
#[cfg(unix)]
#[test]
fn trace_setup_adopts_the_configured_org_without_prompting() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let config = state_dir.path().join("config.json");
    write_agent_cli(
        &bin_dir.path().join("codex"),
        r#"{"marketplaces":[]}"#,
        r#"{"installed":[]}"#,
    );
    write_config_org(config_home.path(), "test-org");
    write_auth_store(config_home.path(), &[("test-profile", "test-org")]);
    write_profile_secrets(config_home.path(), &["test-profile"]);

    let mut cmd = bt_command();
    clear_braintrust_auth_env(&mut cmd);
    cmd.env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .env("PATH", bin_dir.path())
        .env("BRAINTRUST_PROFILE", "test-profile")
        .env("AGENT_SETUP_LOG", state_dir.path().join("codex.log"))
        .env("BT_DAEMON_CONFIG", &config)
        .args([
            "trace",
            "setup",
            "codex",
            "--project",
            "test-project",
            "--no-input",
        ])
        .assert()
        .success();

    let settings: serde_json::Value =
        serde_json::from_slice(&fs::read(config).expect("read config")).expect("parse config");
    assert_eq!(settings["route"]["auth"]["org_name"], "test-org");
}

#[cfg(unix)]
#[test]
fn trace_run_uses_the_invocation_project_without_changing_setup() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let run_log = state_dir.path().join("run.log");
    let run_settings = state_dir.path().join("run-settings.json");
    let run_daemon_env = state_dir.path().join("run-daemon-env.txt");
    let setup_settings = state_dir.path().join("setup-settings.json");
    write_run_agent(&bin_dir.path().join("codex"));

    bt_trace_environment_command(config_home.path())
        .env("HOME", home.path())
        .env("PATH", bin_dir.path())
        .env("AGENT_RUN_LOG", &run_log)
        .env("AGENT_RUN_SETTINGS", &run_settings)
        .env("AGENT_RUN_DAEMON_ENV", &run_daemon_env)
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
    assert_eq!(settings["route"]["auth"]["source"], "environment");
    assert!(settings["route"]["auth"].get("profile").is_none());
    let daemon_env = fs::read_to_string(run_daemon_env).expect("read managed daemon environment");
    let mut daemon_env = daemon_env.lines();
    let socket = daemon_env.next().expect("managed daemon socket");
    let data_dir = daemon_env.next().expect("managed daemon data directory");
    assert!(socket.contains("bt-trace-run-"));
    assert!(data_dir.contains("bt-trace-run-"));
    assert!(
        !setup_settings.exists(),
        "managed run must not change persistent setup settings"
    );
}

#[cfg(unix)]
#[test]
fn trace_enable_requires_durable_saved_profile_auth() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    write_agent_cli(
        &bin_dir.path().join("codex"),
        r#"{"marketplaces":[]}"#,
        r#"{"installed":[]}"#,
    );

    bt_trace_environment_command(config_home.path())
        .env("HOME", home.path())
        .env("PATH", bin_dir.path())
        .env("AGENT_SETUP_LOG", state_dir.path().join("codex.log"))
        .args(["trace", "enable", "codex", "--project", "agent-traces"])
        .assert()
        .failure()
        .stderr(predicate::str::contains(
            "persistent coding-agent tracing requires a saved Braintrust profile",
        ))
        .stderr(predicate::str::contains(
            "bt login --profile <NAME> --save-env-api-key",
        ));
}

#[cfg(unix)]
#[test]
fn trace_run_opencode_injects_the_npm_plugin_without_changing_global_config() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
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

    bt_trace_command(config_home.path(), "test-profile", "test-org")
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
        serde_json::json!(["@braintrust/trace-opencode/tracing"])
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
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let run_log = state_dir.path().join("run.log");
    let run_settings = state_dir.path().join("run-settings.json");
    let global_config = home.path().join(".pi/agent/braintrust.json");
    fs::create_dir_all(global_config.parent().expect("config parent"))
        .expect("create config parent");
    fs::write(&global_config, r#"{"trace_to_braintrust":true}"#).expect("seed global config");
    write_run_agent(&bin_dir.path().join("pi"));

    bt_trace_command(config_home.path(), "test-profile", "test-org")
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
    #[cfg(unix)]
    let socket = {
        let state = tempfile::tempdir().expect("state tempdir");
        state.path().join("missing.sock")
    };
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
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    write_auth_store(config_home.path(), &[("test-profile", "test-org")]);
    write_profile_secrets(config_home.path(), &["test-profile"]);
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

    bt_trace_command(config_home.path(), "test-profile", "test-org")
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
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
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let log = state_dir.path().join("claude.log");
    let config = state_dir.path().join("config.json");
    write_agent_cli(&bin_dir.path().join("claude"), "[]", "[]");

    bt_trace_command(config_home.path(), "test-profile", "test-org")
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
    write_auth_store(config_home.path(), &[("work", "acme")]);
    write_profile_secrets(config_home.path(), &["work"]);

    bt_trace_command(config_home.path(), "work", "acme")
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
    let stdout = bt_trace_command(config_home.path(), "test-profile", "test-org")
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
    assert_eq!(output["command"], "enable");
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
fn trace_doctor_reports_saved_profile_provenance_without_credentials() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let log = tempfile::NamedTempFile::new().expect("setup log");
    write_agent_cli(
        &bin_dir.path().join("codex"),
        r#"{"marketplaces":[]}"#,
        r#"{"installed":[]}"#,
    );

    bt_trace_command(config_home.path(), "test-profile", "test-org")
        .env("HOME", home.path())
        .env("PATH", bin_dir.path())
        .env("AGENT_SETUP_LOG", log.path())
        .args(["trace", "enable", "codex", "--project", "agent-traces"])
        .assert()
        .success();

    let output = bt_trace_command(config_home.path(), "test-profile", "test-org")
        .env("HOME", home.path())
        .args(["trace", "doctor", "codex", "--json"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let doctor: serde_json::Value =
        serde_json::from_slice(&output).expect("trace doctor emits JSON");
    assert_eq!(doctor["command"], "doctor");
    assert_eq!(doctor["source"], "codex");
    assert_eq!(doctor["enabled"], true);
    assert_eq!(doctor["auth"]["status"], "ready");
    assert_eq!(doctor["auth"]["source"], "saved_profile");
    assert_eq!(doctor["auth"]["kind"], "api_key");
    assert_eq!(doctor["auth"]["profile"], "test-profile");
    assert!(!String::from_utf8(output)
        .expect("UTF-8 doctor output")
        .contains("test-api-key"));
}

#[cfg(unix)]
#[test]
fn trace_setup_pi_installs_the_npm_extension_and_selected_route() {
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let state_dir = tempfile::tempdir().expect("state tempdir");
    let log = state_dir.path().join("pi.log");
    write_agent_cli(&bin_dir.path().join("pi"), "{}", "{}");

    bt_trace_command(config_home.path(), "test-profile", "test-org")
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

    bt_trace_command(config_home.path(), "test-profile", "test-org")
        .env("HOME", home.path())
        .env("XDG_CONFIG_HOME", config_home.path())
        .env("PATH", bin_dir.path())
        .env("AGENT_SETUP_LOG", log.path())
        .args(["trace", "setup", "codex", "--project", "codex-project"])
        .assert()
        .success();
    bt_trace_command(config_home.path(), "test-profile", "test-org")
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
fn scorers_create_help_includes_llm_judge_configuration() {
    bt_command()
        .args(["scorers", "create", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--messages <SOURCE>"))
        .stdout(predicate::str::contains("--model"))
        .stdout(predicate::str::contains("--temperature"))
        .stdout(predicate::str::contains("--max-tokens"))
        .stdout(predicate::str::contains("--top-p"))
        .stdout(predicate::str::contains("--frequency-penalty"))
        .stdout(predicate::str::contains("--presence-penalty"))
        .stdout(predicate::str::contains("--stop-sequence"))
        .stdout(predicate::str::contains("--tool-choice"))
        .stdout(predicate::str::contains("--reasoning-effort"))
        .stdout(predicate::str::contains("--verbosity"))
        .stdout(predicate::str::contains("--use-cache"))
        .stdout(predicate::str::contains("--response-format"))
        .stdout(predicate::str::contains("--template-format"))
        .stdout(predicate::str::contains("--choice-scores"))
        .stdout(predicate::str::contains("--classifications"))
        .stdout(predicate::str::contains("--use-cot"))
        .stdout(predicate::str::contains("--pass-threshold"))
        .stdout(predicate::str::contains("--metadata"))
        .stdout(predicate::str::contains("--if-exists"))
        .stdout(predicate::str::contains("TypeScript: projects.create"))
        .stdout(predicate::str::contains("Python:     projects.create"))
        .stdout(predicate::str::contains("bt functions push scorer.ts"))
        .stdout(predicate::str::contains("bt functions push scorer.py"));
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
