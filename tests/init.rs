use std::path::{Path, PathBuf};
use std::process::Command;

use tempfile::tempdir;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn bt_binary_path() -> PathBuf {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_bt") {
        return PathBuf::from(path);
    }

    let root = repo_root();
    let candidate = root.join("target").join("debug").join("bt");
    if !candidate.is_file() {
        let status = Command::new("cargo")
            .args(["build", "--bin", "bt"])
            .current_dir(&root)
            .status()
            .expect("cargo build --bin bt");
        assert!(status.success(), "cargo build --bin bt failed");
    }
    candidate
}

fn run_git(cwd: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("run git");
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "git command failed in {}: git {}\n{}",
            cwd.display(),
            args.join(" "),
            stderr.trim()
        );
    }
}

fn git_check_ignore(cwd: &Path, path: &str) -> bool {
    let status = Command::new("git")
        .args(["check-ignore", "-q", path])
        .current_dir(cwd)
        .status()
        .expect("git check-ignore");
    status.success()
}

#[test]
fn init_creates_config_and_bt_gitignore_with_expected_tracking() {
    let tmp = tempdir().expect("tempdir");
    run_git(tmp.path(), &["init"]);

    let output = Command::new(bt_binary_path())
        .args(["init", "--no-input", "--org", "acme", "--project", "my-app"])
        .current_dir(tmp.path())
        .output()
        .expect("run bt init");
    assert!(
        output.status.success(),
        "bt init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let config = tmp.path().join(".bt").join("config.json");
    assert!(config.is_file(), "expected {}", config.display());

    let gitignore = tmp.path().join(".bt").join(".gitignore");
    let gitignore_contents = std::fs::read_to_string(&gitignore).expect("read .bt/.gitignore");
    assert!(
        gitignore_contents.starts_with(
            "# BEGIN bt-managed\n*\n!config.json\n!.gitignore\n!skills/\n!skills/**\n# END bt-managed\n"
        ),
        "unexpected managed block:\n{}",
        gitignore_contents
    );

    let skills_doc = tmp.path().join(".bt/skills/docs/reference/sql.md");
    std::fs::create_dir_all(skills_doc.parent().unwrap()).expect("create skills docs dir");
    std::fs::write(&skills_doc, "docs").expect("write skills doc");

    let cache_file = tmp.path().join(".bt/cache/runners/v1/functions-runner.ts");
    std::fs::create_dir_all(cache_file.parent().unwrap()).expect("create cache dir");
    std::fs::write(&cache_file, "runner").expect("write cache file");

    assert!(!git_check_ignore(tmp.path(), ".bt/config.json"));
    assert!(!git_check_ignore(
        tmp.path(),
        ".bt/skills/docs/reference/sql.md"
    ));
    assert!(git_check_ignore(
        tmp.path(),
        ".bt/cache/runners/v1/functions-runner.ts"
    ));
}

#[test]
fn init_repairs_missing_bt_gitignore_when_config_exists() {
    let tmp = tempdir().expect("tempdir");
    std::fs::create_dir_all(tmp.path().join(".bt")).expect("create .bt");
    std::fs::write(
        tmp.path().join(".bt").join("config.json"),
        "{ \"org\": \"acme\", \"project\": \"my-app\" }\n",
    )
    .expect("write config.json");

    let output = Command::new(bt_binary_path())
        .args(["init", "--no-input"])
        .current_dir(tmp.path())
        .output()
        .expect("run bt init");
    assert!(
        output.status.success(),
        "bt init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        tmp.path().join(".bt").join(".gitignore").is_file(),
        "expected .bt/.gitignore to be created"
    );
}

#[test]
fn init_preserves_custom_bt_gitignore_rules_and_moves_managed_block_to_top() {
    let tmp = tempdir().expect("tempdir");
    std::fs::create_dir_all(tmp.path().join(".bt")).expect("create .bt");
    std::fs::write(
        tmp.path().join(".bt").join("config.json"),
        "{ \"org\": \"legacy-org\", \"project\": \"legacy-project\" }\n",
    )
    .expect("write config");

    let existing =
        "custom-before\n\n# BEGIN bt-managed\nold-rule\n# END bt-managed\n\ncustom-after\n";
    std::fs::write(tmp.path().join(".bt").join(".gitignore"), existing).expect("write .gitignore");

    let output = Command::new(bt_binary_path())
        .args(["init", "--no-input"])
        .current_dir(tmp.path())
        .output()
        .expect("run bt init");
    assert!(
        output.status.success(),
        "bt init failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );

    let gitignore = std::fs::read_to_string(tmp.path().join(".bt").join(".gitignore"))
        .expect("read .gitignore");
    assert!(
        gitignore.starts_with("# BEGIN bt-managed\n"),
        "managed block should be first:\n{gitignore}"
    );
    assert_eq!(gitignore.matches("# BEGIN bt-managed").count(), 1);
    assert_eq!(gitignore.matches("# END bt-managed").count(), 1);
    assert!(gitignore.contains("custom-before"));
    assert!(gitignore.contains("custom-after"));

    let end_pos = gitignore
        .find("# END bt-managed")
        .expect("managed block end");
    assert!(
        gitignore.find("custom-before").unwrap() > end_pos,
        "custom-before should be after managed block:\n{gitignore}"
    );
    assert!(
        gitignore.find("custom-after").unwrap() > end_pos,
        "custom-after should be after managed block:\n{gitignore}"
    );
}

#[test]
fn status_reads_local_config() {
    let tmp = tempdir().expect("tempdir");
    std::fs::create_dir_all(tmp.path().join(".bt")).expect("create .bt");
    std::fs::write(
        tmp.path().join(".bt").join("config.json"),
        "{ \"org\": \"test-org\", \"project\": \"test-project\" }\n",
    )
    .expect("write config");

    let output = Command::new(bt_binary_path())
        .args(["status", "--json"])
        .current_dir(tmp.path())
        .output()
        .expect("run bt status --json");
    assert!(
        output.status.success(),
        "bt status failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let json: serde_json::Value =
        serde_json::from_slice(&output.stdout).expect("parse status json");
    assert_eq!(json.get("org").and_then(|v| v.as_str()), Some("test-org"));
    assert_eq!(
        json.get("project").and_then(|v| v.as_str()),
        Some("test-project")
    );
}
