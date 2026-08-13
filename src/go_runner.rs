//! Locating the Go toolchain, a user's prebuilt eval binary, and the packages
//! that hold their evals.
//!
//! Unlike JavaScript and Python, bt does not supply the program for Go. There
//! is no runtime loading in Go, so the user's compiled package *is* the runner
//! and nothing is materialized to disk on our side.
//!
//! Package layout is resolved by asking the Go toolchain (`go list -json`)
//! rather than by inspecting paths ourselves. `go list` is authoritative about
//! things bt cannot see from the filesystem: which files a build actually
//! includes (`//go:build` constraints, `GOOS`/`GOARCH`), where the enclosing
//! module really starts (`go.work` can move it), and whether a package is even
//! runnable (`package main`).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

/// Import path of the Go SDK package every eval runner links against.
///
/// This is what makes Go eval discovery convention-free: a package that imports
/// it is an eval runner, so no file-naming or directory-naming rule is needed.
pub const EVAL_RUNNER_IMPORT: &str = "github.com/braintrustdata/braintrust-sdk-go/evalrunner";

/// The subset of `go list -json` output bt uses.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GoPackage {
    /// Absolute directory holding the package's source.
    pub dir: PathBuf,
    pub import_path: String,
    /// Package name. `main` means the package is runnable.
    #[serde(default)]
    pub name: String,
    /// Go files the build includes, relative to `dir`, with build constraints
    /// already applied.
    #[serde(default)]
    pub go_files: Vec<String>,
    /// Transitive imports, used to spot packages that link the eval runner.
    #[serde(default)]
    pub deps: Vec<String>,
    #[serde(default)]
    pub module: Option<GoModule>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GoModule {
    /// Module root. Absent for packages outside any module.
    #[serde(default)]
    pub dir: Option<PathBuf>,
}

impl GoPackage {
    /// True when this package is a runnable eval runner.
    pub fn is_eval_runner(&self) -> bool {
        self.name == "main" && self.deps.iter().any(|dep| dep == EVAL_RUNNER_IMPORT)
    }

    /// The package's build inputs as absolute paths.
    pub fn absolute_go_files(&self) -> Vec<String> {
        self.go_files
            .iter()
            .map(|f| self.dir.join(f).to_string_lossy().into_owned())
            .collect()
    }
}

/// Resolves the Go toolchain to invoke.
///
/// Explicit environment overrides win, then `GOROOT`, then `PATH`. The override
/// matters for version managers (mise, asdf, gvm) that keep the real toolchain
/// off the default `PATH`.
pub fn resolve_go_toolchain(env_overrides: &[&str]) -> Option<PathBuf> {
    for env_name in env_overrides {
        if let Some(value) = std::env::var_os(env_name) {
            if !value.is_empty() {
                return Some(PathBuf::from(value));
            }
        }
    }

    if let Some(goroot) = std::env::var_os("GOROOT") {
        let candidate = PathBuf::from(goroot).join("bin").join("go");
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    crate::python_runner::find_binary_in_path(&["go"])
}

/// Resolves `--runner` for Go, which names a prebuilt eval binary rather than
/// an interpreter.
///
/// Unlike the Python interpreter override, this is validated: the Go runner
/// takes all its input from the environment and ignores argv, so a typo'd path
/// would otherwise surface only as an opaque spawn failure.
pub fn resolve_prebuilt_runner(explicit: &str) -> Result<PathBuf> {
    let path = Path::new(explicit);
    let looks_like_path = path.is_absolute()
        || explicit.contains('/')
        || explicit.contains('\\')
        || explicit.starts_with('.');

    if looks_like_path {
        if !path.is_file() {
            anyhow::bail!("--runner binary not found: {explicit}");
        }
        return Ok(path.to_path_buf());
    }

    // A bare name: prefer PATH, but fall through to the literal so the spawn
    // error names it, matching how the JS runner override behaves.
    Ok(crate::python_runner::find_binary_in_path(&[explicit])
        .unwrap_or_else(|| PathBuf::from(explicit)))
}

/// True when `dir` sits inside a Go module.
///
/// Checked before spawning `go list` so that a non-Go directory costs nothing
/// and keeps reporting the ordinary "no eval files found" error.
pub fn in_go_module(dir: &Path) -> bool {
    let mut current = Some(dir);
    while let Some(candidate) = current {
        if candidate.join("go.mod").is_file() {
            return true;
        }
        current = candidate.parent();
    }
    false
}

/// Runs `go list -json <patterns>` in `cwd` and returns the packages it reports.
///
/// Costs roughly what the `go run` that follows it costs, which is not worth
/// caching against: an eval run takes seconds, and a stale listing would be a
/// correctness question for no meaningful gain.
pub fn list_packages(go: &Path, patterns: &[String], cwd: &Path) -> Result<Vec<GoPackage>> {
    let mut command = std::process::Command::new(go);
    command
        .arg("list")
        .arg("-json")
        .args(patterns)
        .current_dir(cwd);

    let output = command
        .output()
        .with_context(|| format!("failed to run `{} list -json`", go.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        anyhow::bail!(
            "`go list` failed for {}: {}",
            patterns.join(" "),
            if stderr.is_empty() {
                "no output".to_string()
            } else {
                stderr
            }
        );
    }

    parse_go_list(&output.stdout)
}

/// Parses the stream of concatenated JSON objects `go list -json` emits.
///
/// The output is not a JSON array: one object follows another, so it has to be
/// read as a stream rather than deserialized in one shot.
pub fn parse_go_list(stdout: &[u8]) -> Result<Vec<GoPackage>> {
    serde_json::Deserializer::from_slice(stdout)
        .into_iter::<GoPackage>()
        .collect::<Result<Vec<_>, _>>()
        .context("failed to parse `go list -json` output")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("bt-go-runner-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        dir
    }

    #[test]
    fn in_go_module_finds_an_ancestor_go_mod() {
        let dir = make_temp_dir("in-module");
        let pkg = dir.join("cmd").join("evals");
        std::fs::create_dir_all(&pkg).expect("package dir should be created");
        std::fs::write(dir.join("go.mod"), "module example.test\n").expect("go.mod written");

        assert!(in_go_module(&pkg));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prebuilt_runner_rejects_a_missing_path() {
        let err = resolve_prebuilt_runner("./definitely/not/here")
            .expect_err("a missing path should fail");
        assert!(format!("{err:#}").contains("--runner binary not found"));
    }

    #[test]
    fn prebuilt_runner_accepts_an_existing_path() {
        let dir = make_temp_dir("prebuilt");
        let binary = dir.join("evals");
        std::fs::write(&binary, b"#!/bin/sh\n").expect("binary written");

        let resolved =
            resolve_prebuilt_runner(binary.to_str().expect("utf-8 path")).expect("should resolve");
        assert_eq!(resolved, binary);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // `go list -json` emits concatenated objects, not a JSON array, so the
    // parser has to read them as a stream.
    #[test]
    fn parse_go_list_reads_concatenated_objects() {
        let stdout = br#"{
            "Dir": "/repo/a",
            "ImportPath": "example.test/a",
            "Name": "main",
            "GoFiles": ["main.go"]
        }
        {
            "Dir": "/repo/b",
            "ImportPath": "example.test/b",
            "Name": "b"
        }"#;

        let packages = parse_go_list(stdout).expect("stream should parse");

        assert_eq!(packages.len(), 2);
        assert_eq!(packages[0].import_path, "example.test/a");
        assert_eq!(packages[1].name, "b");
    }

    // Discovery keys off the runner import, which is what lets Go evals live in
    // any file with any name.
    #[test]
    fn is_eval_runner_requires_main_and_the_runner_import() {
        let stdout = format!(
            r#"{{"Dir": "/repo/evals", "ImportPath": "example.test/evals", "Name": "main", "Deps": ["fmt", "{runner}"]}}
               {{"Dir": "/repo/cli", "ImportPath": "example.test/cli", "Name": "main", "Deps": ["fmt"]}}
               {{"Dir": "/repo/lib", "ImportPath": "example.test/lib", "Name": "lib", "Deps": ["{runner}"]}}"#,
            runner = EVAL_RUNNER_IMPORT
        );

        let packages = parse_go_list(stdout.as_bytes()).expect("stream should parse");

        assert!(packages[0].is_eval_runner(), "main + runner import");
        assert!(!packages[1].is_eval_runner(), "main without the import");
        assert!(
            !packages[2].is_eval_runner(),
            "library that imports the runner"
        );
    }

    #[test]
    fn absolute_go_files_joins_against_the_package_dir() {
        let stdout = br#"{"Dir": "/repo/evals", "ImportPath": "x", "Name": "main",
                          "GoFiles": ["main.go", "classifier.go"]}"#;

        let packages = parse_go_list(stdout).expect("stream should parse");

        assert_eq!(
            packages[0].absolute_go_files(),
            vec![
                "/repo/evals/main.go".to_string(),
                "/repo/evals/classifier.go".to_string()
            ]
        );
    }
}
