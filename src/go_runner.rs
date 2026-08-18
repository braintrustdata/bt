//! Locating the Go toolchain and a user's prebuilt eval binary.
//!
//! Unlike JavaScript and Python, bt does not supply the program for Go. There
//! is no runtime loading in Go, so the user's compiled package *is* the runner
//! and nothing is materialized to disk on our side. This module only answers
//! two questions: which `go` to invoke, and where the user's module root is.

use std::path::{Path, PathBuf};

use anyhow::Result;

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

/// Finds the nearest ancestor containing `go.mod`.
///
/// `go run <pkg>` requires the process working directory to sit inside the main
/// module, so bt may need to move into this directory before spawning.
pub fn module_root_for(package_dir: &Path) -> Option<PathBuf> {
    let mut current = Some(package_dir);
    while let Some(dir) = current {
        if dir.join("go.mod").is_file() {
            return Some(dir.to_path_buf());
        }
        current = dir.parent();
    }
    None
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
    fn module_root_finds_nearest_go_mod() {
        let dir = make_temp_dir("module-root");
        let pkg = dir.join("cmd").join("evals");
        std::fs::create_dir_all(&pkg).expect("package dir should be created");
        std::fs::write(dir.join("go.mod"), "module example.test\n").expect("go.mod written");

        assert_eq!(module_root_for(&pkg), Some(dir.clone()));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn module_root_is_none_without_go_mod() {
        let dir = make_temp_dir("no-module");
        let pkg = dir.join("cmd");
        std::fs::create_dir_all(&pkg).expect("package dir should be created");

        // A go.mod anywhere above the temp dir would make this flaky, so only
        // assert that the temp tree itself contributes nothing.
        let found = module_root_for(&pkg);
        assert!(found.is_none() || !found.unwrap().starts_with(&dir));

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
}
