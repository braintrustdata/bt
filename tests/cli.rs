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
fn views_help_accepts_push_trace_and_dataset_subcommands() {
    bt_command()
        .args(["views", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("push"))
        .stdout(predicate::str::contains("trace"))
        .stdout(predicate::str::contains("dataset"));
}

#[test]
fn views_trace_help_lists_bootstrap_and_preview() {
    bt_command()
        .args(["views", "trace", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("bootstrap"))
        .stdout(predicate::str::contains("preview"));
}

#[test]
fn views_dataset_help_lists_bootstrap_and_preview() {
    bt_command()
        .args(["views", "dataset", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("bootstrap"))
        .stdout(predicate::str::contains("preview"));
}

#[test]
fn views_push_help_lists_custom_view_flags() {
    bt_command()
        .args(["views", "push", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--if-exists"))
        .stdout(predicate::str::contains("--tsconfig"));
}

#[test]
fn views_trace_preview_help_lists_trace_selectors() {
    bt_command()
        .args(["views", "trace", "preview", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--trace-id"))
        .stdout(predicate::str::contains("--url"))
        .stdout(predicate::str::contains("--dataset").not())
        .stdout(predicate::str::contains("--row-index").not());
}

#[test]
fn views_dataset_preview_help_lists_dataset_selectors() {
    bt_command()
        .args(["views", "dataset", "preview", "--help"])
        .assert()
        .success()
        .stdout(predicate::str::contains("--dataset"))
        .stdout(predicate::str::contains("--trace-id").not())
        .stdout(predicate::str::contains("--url").not())
        .stdout(predicate::str::contains("--row-index"));
}

#[test]
fn views_trace_bootstrap_creates_starter_file() {
    let dir = tempfile::tempdir().expect("tempdir");

    bt_command()
        .current_dir(dir.path())
        .args(["views", "trace", "bootstrap", "Trace Review"])
        .assert()
        .success()
        .stdout(predicate::str::contains(
            "braintrust-custom-views/trace-review.trace-view.tsx",
        ))
        .stdout(predicate::str::contains(
            "braintrust-custom-views/tsconfig.json",
        ))
        .stdout(predicate::str::contains(
            "braintrust-custom-views/custom-view-env.d.ts",
        ));

    let contents = fs::read_to_string(
        dir.path()
            .join("braintrust-custom-views/trace-review.trace-view.tsx"),
    )
    .expect("read starter view");
    assert!(contents.contains("export default customTraceView"));
    assert!(contents.contains(r#"name: "Trace Review""#));
    assert!(contents.contains(r#"slug: "trace-review""#));
    assert!(contents.contains("({ trace, span, selectSpan }) => {"));
    assert!(contents.contains("onChange={(event) => selectSpan?.(event.target.value)}"));
    assert!(!contents.contains("function StarterTraceView"));
    assert!(!contents.contains("StarterTraceView,"));
    assert!(!contents.contains("trace: { spanOrder: string[] }"));
    assert!(!contents.contains("event:"));
    assert!(!contents.contains(r#"from "react""#));
    assert!(!contents.contains("component: StarterTraceView"));

    let tsconfig = fs::read_to_string(dir.path().join("braintrust-custom-views/tsconfig.json"))
        .expect("read custom view tsconfig");
    assert!(tsconfig.contains(r#""jsx": "react-jsx""#));
    assert!(tsconfig.contains(r#""moduleResolution": "Bundler""#));
    assert!(tsconfig.contains(r#""noEmit": true"#));
    assert!(tsconfig.contains(r#""allowJs": true"#));
    assert!(tsconfig.contains(r#""**/*.view.tsx""#));
    assert!(tsconfig.contains(r#""**/*.view.jsx""#));
    assert!(tsconfig.contains(r#""**/*-view.tsx""#));
    assert!(tsconfig.contains(r#""**/*-view.jsx""#));
    assert!(tsconfig.contains(r#""**/*.d.ts""#));

    let types = fs::read_to_string(
        dir.path()
            .join("braintrust-custom-views/custom-view-env.d.ts"),
    )
    .expect("read custom view type declarations");
    assert!(types.contains("declare namespace JSX"));
    assert!(types.contains("interface CustomViewIntrinsicElements"));
    assert!(types.contains("select: CustomViewSelectProps"));
    assert!(types.contains("option: CustomViewOptionProps"));
    assert!(types.contains("onChange?: (event: CustomViewSelectChangeEvent) => void"));
    assert!(types.contains("interface IntrinsicElements extends CustomViewIntrinsicElements"));
    assert!(types.contains(r#"declare module "react/jsx-runtime""#));
    assert!(types.contains(r#"declare module "react""#));
}

#[test]
fn views_dataset_bootstrap_creates_starter_file_with_dataset_name() {
    let dir = tempfile::tempdir().expect("tempdir");

    bt_command()
        .current_dir(dir.path())
        .args([
            "views",
            "dataset",
            "bootstrap",
            "Dataset Review",
            "--dataset",
            "test-dataset",
            "--file",
            "custom.dataset.view.tsx",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("custom.dataset.view.tsx"));

    let contents =
        fs::read_to_string(dir.path().join("custom.dataset.view.tsx")).expect("read starter view");
    assert!(contents.contains("export default customDatasetView"));
    assert!(contents.contains(r#"name: "Dataset Review""#));
    assert!(contents.contains(r#"slug: "dataset-review""#));
    assert!(contents.contains(r#"dataset: { name: "test-dataset" }"#));
    assert!(contents.contains("({ id, input, expected, metadata, tags = [] }) => {"));
    assert!(!contents.contains("function StarterDatasetView"));
    assert!(!contents.contains("StarterDatasetView,"));
    assert!(!contents.contains("id: string;"));
    assert!(!contents.contains(r#"from "react""#));
    assert!(!contents.contains("component: StarterDatasetView"));

    let tsconfig =
        fs::read_to_string(dir.path().join("tsconfig.json")).expect("read custom view tsconfig");
    assert!(tsconfig.contains(r#""jsx": "react-jsx""#));
    assert!(dir.path().join("custom-view-env.d.ts").exists());
}

#[test]
fn views_bootstrap_json_reports_tsconfig() {
    let dir = tempfile::tempdir().expect("tempdir");

    let output = bt_command()
        .current_dir(dir.path())
        .args(["views", "--json", "trace", "bootstrap", "Trace Review"])
        .assert()
        .success()
        .get_output()
        .stdout
        .clone();
    let payload: serde_json::Value =
        serde_json::from_slice(&output).expect("parse bootstrap json output");

    assert_eq!(
        payload["path"],
        "braintrust-custom-views/trace-review.trace-view.tsx"
    );
    assert_eq!(
        payload["tsconfig_path"],
        "braintrust-custom-views/tsconfig.json"
    );
    assert_eq!(payload["tsconfig_created"], true);
    assert_eq!(
        payload["types_path"],
        "braintrust-custom-views/custom-view-env.d.ts"
    );
    assert_eq!(payload["types_created"], true);
    assert_eq!(payload["view_type"], "trace");
}

#[test]
fn views_bootstrap_preserves_existing_tsconfig_without_force() {
    let dir = tempfile::tempdir().expect("tempdir");
    fs::create_dir_all(dir.path().join("custom")).expect("create custom view dir");
    fs::write(
        dir.path().join("custom/tsconfig.json"),
        "{ \"compilerOptions\": { \"jsx\": \"preserve\" } }\n",
    )
    .expect("write existing tsconfig");
    fs::write(
        dir.path().join("custom/custom-view-env.d.ts"),
        "declare const preserved: true;\n",
    )
    .expect("write existing types");

    bt_command()
        .current_dir(dir.path())
        .args([
            "views",
            "trace",
            "bootstrap",
            "Trace Review",
            "--file",
            "custom",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Reused TypeScript config"))
        .stdout(predicate::str::contains(
            "Reused custom view type declarations",
        ));

    let tsconfig =
        fs::read_to_string(dir.path().join("custom/tsconfig.json")).expect("read tsconfig");
    assert_eq!(
        tsconfig,
        "{ \"compilerOptions\": { \"jsx\": \"preserve\" } }\n"
    );
    assert!(dir
        .path()
        .join("custom/trace-review.trace-view.tsx")
        .exists());
    let types =
        fs::read_to_string(dir.path().join("custom/custom-view-env.d.ts")).expect("read types");
    assert_eq!(types, "declare const preserved: true;\n");
}

#[test]
fn views_bootstrap_force_overwrites_view_and_tsconfig() {
    let dir = tempfile::tempdir().expect("tempdir");
    fs::create_dir_all(dir.path().join("custom")).expect("create custom view dir");
    fs::write(
        dir.path().join("custom/trace-review.trace-view.tsx"),
        "old view\n",
    )
    .expect("write view");
    fs::write(
        dir.path().join("custom/tsconfig.json"),
        "{ \"compilerOptions\": { \"jsx\": \"preserve\" } }\n",
    )
    .expect("write existing tsconfig");
    fs::write(
        dir.path().join("custom/custom-view-env.d.ts"),
        "declare const preserved: true;\n",
    )
    .expect("write existing types");

    bt_command()
        .current_dir(dir.path())
        .args([
            "views",
            "trace",
            "bootstrap",
            "Trace Review",
            "--file",
            "custom",
            "--force",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("Updated TypeScript config"))
        .stdout(predicate::str::contains(
            "Updated custom view type declarations",
        ));

    let contents = fs::read_to_string(dir.path().join("custom/trace-review.trace-view.tsx"))
        .expect("read starter view");
    assert!(contents.contains("export default customTraceView"));

    let tsconfig =
        fs::read_to_string(dir.path().join("custom/tsconfig.json")).expect("read tsconfig");
    assert!(tsconfig.contains(r#""jsx": "react-jsx""#));
    assert!(!tsconfig.contains(r#""jsx": "preserve""#));
    let types =
        fs::read_to_string(dir.path().join("custom/custom-view-env.d.ts")).expect("read types");
    assert!(types.contains(r#"declare module "react/jsx-runtime""#));
    assert!(!types.contains("declare const preserved"));
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
