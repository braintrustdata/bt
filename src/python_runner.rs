use std::collections::HashSet;
use std::path::{Path, PathBuf};

pub fn resolve_python_interpreter(
    explicit: Option<&str>,
    env_overrides: &[&str],
) -> Option<PathBuf> {
    resolve_python_interpreter_for_roots(explicit, env_overrides, &[])
}

pub fn resolve_python_interpreter_for_roots(
    explicit: Option<&str>,
    env_overrides: &[&str],
    search_roots: &[PathBuf],
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

    // uv and common venv workflows keep the interpreter under the project root.
    // Prefer that before PATH so evals use the dependencies next to the files.
    if let Some(venv) = find_project_virtual_env_python(search_roots) {
        return Some(venv);
    }

    find_binary_in_path(&["python3", "python"])
}

fn find_virtual_env_python() -> Option<PathBuf> {
    let venv_root = std::env::var_os("VIRTUAL_ENV")?;
    find_virtual_env_python_in(&PathBuf::from(venv_root))
}

fn find_project_virtual_env_python(search_roots: &[PathBuf]) -> Option<PathBuf> {
    let mut seen = HashSet::new();

    for root in search_roots {
        let mut current = Some(root.as_path());
        while let Some(dir) = current {
            if seen.insert(dir.to_path_buf()) {
                for env_dir in [".venv", "venv"] {
                    if let Some(python) = find_virtual_env_python_in(&dir.join(env_dir)) {
                        return Some(python);
                    }
                }
            }
            current = dir.parent();
        }
    }

    None
}

fn find_virtual_env_python_in(root: &Path) -> Option<PathBuf> {
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

    #[test]
    fn project_venv_is_discovered_from_search_root_ancestors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let nested = dir.path().join("tests").join("evals");
        std::fs::create_dir_all(&nested).expect("nested dirs should be created");

        let venv_bin = dir.path().join(".venv").join("bin");
        std::fs::create_dir_all(&venv_bin).expect("venv bin should be created");
        let python = venv_bin.join("python");
        std::fs::write(&python, "").expect("python file should be written");

        let resolved = find_project_virtual_env_python(&[nested]);
        assert_eq!(resolved, Some(python));
    }
}
