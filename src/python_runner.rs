use std::path::{Path, PathBuf};

pub fn resolve_python_interpreter(
    explicit: Option<&str>,
    env_overrides: &[&str],
) -> Option<PathBuf> {
    if let Some(explicit) = explicit {
        return Some(PathBuf::from(explicit));
    }

    for env_name in env_overrides {
        if let Some(value) = std::env::var_os(env_name) {
            if !value.is_empty() {
                return Some(PathBuf::from(value));
            }
        }
    }

    // Process-internal interpreter discovery for active virtual environments.
    if let Some(venv) = find_virtual_env_python() {
        return Some(venv);
    }

    find_binary_in_path(&["python3", "python"])
}

fn find_virtual_env_python() -> Option<PathBuf> {
    let venv_root = std::env::var_os("VIRTUAL_ENV")?;
    let root = PathBuf::from(venv_root);

    let unix = root.join("bin").join("python");
    if unix.is_file() {
        return Some(unix);
    }

    let windows = root.join("Scripts").join("python.exe");
    if windows.is_file() {
        return Some(windows);
    }

    None
}

pub fn find_binary_in_path(candidates: &[&str]) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&paths) {
        for candidate in candidates {
            let path = dir.join(candidate);
            if path.is_file() {
                return Some(path);
            }
            if cfg!(windows) {
                let exe = with_windows_extensions(&path);
                for candidate_path in exe {
                    if candidate_path.is_file() {
                        return Some(candidate_path);
                    }
                }
            }
        }
    }
    None
}

#[cfg(windows)]
fn with_windows_extensions(path: &Path) -> [PathBuf; 2] {
    [path.with_extension("exe"), path.with_extension("cmd")]
}

#[cfg(not(windows))]
fn with_windows_extensions(_path: &Path) -> [PathBuf; 0] {
    []
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn explicit_python_runner_wins() {
        let resolved = resolve_python_interpreter(Some("/tmp/python"), &["BT_UNUSED"]);
        assert_eq!(resolved, Some(PathBuf::from("/tmp/python")));
    }

    #[test]
    fn env_override_python_runner_is_used() {
        unsafe {
            std::env::set_var("BT_TEST_PYTHON_RUNNER", "/tmp/from-env-python");
        }
        let resolved = resolve_python_interpreter(None, &["BT_TEST_PYTHON_RUNNER"]);
        unsafe {
            std::env::remove_var("BT_TEST_PYTHON_RUNNER");
        }
        assert_eq!(resolved, Some(PathBuf::from("/tmp/from-env-python")));
    }
}
