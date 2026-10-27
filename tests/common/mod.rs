#![allow(dead_code)]

use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

pub fn in_ci() -> bool {
    std::env::var_os("CI").is_some_and(|value| value != "false" && value != "0")
}

// Cargo supplies the exact binary, including custom target directories and suffixes.
pub fn bt_binary_path(_root: &Path) -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_bt"))
}

pub fn command_exists(command: &str) -> bool {
    let found = std::env::var_os("PATH").is_some_and(|paths| {
        std::env::split_paths(&paths).any(|dir| {
            let path = dir.join(command);
            path.is_file()
                || (cfg!(windows)
                    && ["exe", "cmd", "bat"]
                        .iter()
                        .any(|ext| path.with_extension(ext).is_file()))
        })
    });
    assert!(
        found || !in_ci(),
        "required test runtime {command} is missing in CI"
    );
    found
}

pub fn ensure_python_env(fixtures_root: &Path) -> Option<PathBuf> {
    if !command_exists("uv") {
        assert!(
            !in_ci(),
            "uv is required in CI; install the repository toolchain"
        );
        return None;
    }

    let venv_dir = fixtures_root.join(".venv");
    let python = venv_python_path(&venv_dir);

    if !python.is_file() {
        let status = Command::new("uv")
            .args(["venv", venv_dir.to_string_lossy().as_ref()])
            .status()
            .expect("run uv");
        assert!(status.success(), "uv failed to prepare Python fixtures");
    }

    if !python_can_import_braintrust(python.to_string_lossy().as_ref()) {
        let status = Command::new("uv")
            .args([
                "pip",
                "install",
                "--python",
                python.to_string_lossy().as_ref(),
                "braintrust",
            ])
            .status()
            .expect("run uv");
        assert!(status.success(), "uv failed to prepare Python fixtures");
    }

    Some(python)
}

fn venv_python_path(venv: &Path) -> PathBuf {
    if cfg!(windows) {
        venv.join("Scripts").join("python.exe")
    } else {
        venv.join("bin").join("python")
    }
}

pub fn python_can_import_braintrust(python: &str) -> bool {
    Command::new(python)
        .args(["-c", "import braintrust"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

/// Ensure a failed assertion cannot leave a dev server running after its test.
pub struct TestChild(pub std::process::Child);

impl std::ops::Deref for TestChild {
    type Target = std::process::Child;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for TestChild {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Drop for TestChild {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
