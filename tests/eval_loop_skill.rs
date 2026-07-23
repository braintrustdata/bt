use std::fs;
use std::path::Path;
use std::process::{Command, Output};

fn python3_available() -> bool {
    Command::new("python3")
        .arg("--version")
        .output()
        .map(|output| output.status.success())
        .unwrap_or(false)
}

fn run_script(script: &Path, args: &[&str]) -> Output {
    Command::new("python3")
        .arg(script)
        .args(args)
        .output()
        .expect("run eval loop log script")
}

#[test]
fn eval_loop_log_script_generates_and_validates_compact_jsonl() {
    if !python3_available() {
        eprintln!("Skipping eval loop skill test (python3 not installed).");
        return;
    }

    let root = tempfile::tempdir().expect("create tempdir");
    let eval_output = root.path().join("eval.jsonl");
    let log = root.path().join("log.jsonl");
    let script = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("skills/eval-improvement-loop/scripts/eval_loop_log.py");

    fs::write(
        &eval_output,
        concat!(
            "console output that is not JSON\n",
            r#"{"projectName":"test-project","projectId":"project-test-id","experimentName":"test-evaluator","experimentId":"experiment-test-id","experimentUrl":"https://www.example.test/test-experiment","scores":{"quality":{"name":"quality","score":0.75},"style":{"name":"style","score":0.8}},"metrics":{"duration":{"name":"duration","metric":1.2}},"isFinal":true,"runMode":"full"}"#,
            "\n"
        ),
    )
    .expect("write eval output");

    let append = run_script(
        &script,
        &[
            "append",
            "--log",
            log.to_str().expect("log path"),
            "--eval-output",
            eval_output.to_str().expect("eval output path"),
            "--run",
            "1",
            "--kind",
            "baseline",
            "--scope",
            "full",
            "--status",
            "keep",
            "--mode",
            "experiment",
            "--primary-name",
            "quality",
            "--direction",
            "higher",
            "--hypothesis",
            "unchanged baseline",
            "--timestamp",
            "2026-07-21T00:00:00Z",
        ],
    );
    assert!(
        append.status.success(),
        "append failed: {}",
        String::from_utf8_lossy(&append.stderr)
    );

    let validate = run_script(
        &script,
        &["validate", "--log", log.to_str().expect("log path")],
    );
    assert!(validate.status.success());
    assert_eq!(
        String::from_utf8_lossy(&validate.stdout).trim(),
        "valid: 1 record(s)"
    );

    let content = fs::read_to_string(&log).expect("read log");
    assert_eq!(content.lines().count(), 1);
    let record: serde_json::Value = serde_json::from_str(content.trim()).expect("parse record");
    assert_eq!(record["schema_version"], 1);
    assert_eq!(record["evaluator"], "test-evaluator");
    assert_eq!(record["primary"]["value"], 0.75);
    assert_eq!(record["scores"]["style"], 0.8);
    assert_eq!(record["metrics"]["duration"], 1.2);
    assert_eq!(record["experiment"]["experiment_id"], "experiment-test-id");

    let discard = run_script(
        &script,
        &[
            "append",
            "--log",
            log.to_str().expect("log path"),
            "--eval-output",
            eval_output.to_str().expect("eval output path"),
            "--run",
            "2",
            "--kind",
            "candidate",
            "--scope",
            "full",
            "--status",
            "discard",
            "--mode",
            "experiment",
            "--primary-name",
            "quality",
            "--direction",
            "higher",
            "--hypothesis",
            "test candidate",
            "--base-commit",
            "base-test-commit",
            "--commit",
            "candidate-test-commit",
            "--revert-commit",
            "revert-test-commit",
            "--changed-file",
            "src/z_test.rs",
            "--changed-file",
            "src/a_test.rs",
            "--changed-file",
            "src/z_test.rs",
            "--reason",
            "quality did not improve",
            "--timestamp",
            "2026-07-21T00:01:00Z",
        ],
    );
    assert!(
        discard.status.success(),
        "discard append failed: {}",
        String::from_utf8_lossy(&discard.stderr)
    );
    let records = fs::read_to_string(&log).expect("read two records");
    let discarded: serde_json::Value =
        serde_json::from_str(records.lines().nth(1).expect("second record"))
            .expect("parse discard record");
    assert_eq!(discarded["commit"], "candidate-test-commit");
    assert_eq!(discarded["revert_commit"], "revert-test-commit");
    assert_eq!(
        discarded["changed_files"],
        serde_json::json!(["src/a_test.rs", "src/z_test.rs"])
    );

    let invalid_keep = run_script(
        &script,
        &[
            "append",
            "--log",
            log.to_str().expect("log path"),
            "--eval-output",
            eval_output.to_str().expect("eval output path"),
            "--run",
            "3",
            "--kind",
            "candidate",
            "--scope",
            "full",
            "--status",
            "keep",
            "--mode",
            "experiment",
            "--primary-name",
            "quality",
            "--direction",
            "higher",
            "--hypothesis",
            "candidate without a commit",
            "--timestamp",
            "2026-07-21T00:01:00Z",
        ],
    );
    assert!(!invalid_keep.status.success());
    assert!(String::from_utf8_lossy(&invalid_keep.stderr)
        .contains("every candidate requires the evaluated commit"));
    assert_eq!(
        fs::read_to_string(&log)
            .expect("read unchanged log")
            .lines()
            .count(),
        2
    );
}
