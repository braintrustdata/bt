use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

pub fn build_js_runner_command(
    runner_override: Option<&str>,
    runner_script: &Path,
    files: &[PathBuf],
) -> Command {
    if let Some(explicit) = runner_override {
        let resolved = resolve_js_runner_command(explicit, files);
        if is_deno_runner_path(&resolved) {
            return build_deno_command(resolved.as_os_str(), runner_script, files);
        }

        let mut command = Command::new(&resolved);
        command.arg(runner_script);
        for file in files {
            command.arg(file);
        }
        return command;
    }

    if let Some(auto_runner) = find_js_runner_binary(files) {
        if is_deno_runner_path(&auto_runner) {
            return build_deno_command(auto_runner.as_os_str(), runner_script, files);
        }

        let mut command = Command::new(&auto_runner);
        command.arg(runner_script);
        for file in files {
            command.arg(file);
        }
        return command;
    }

    let mut command = Command::new("npx");
    command.arg("--yes").arg("tsx").arg(runner_script);
    for file in files {
        command.arg(file);
    }
    command
}

pub fn find_js_runner_binary(files: &[PathBuf]) -> Option<PathBuf> {
    const CANDIDATES: &[&str] = &["tsx", "vite-node", "ts-node", "ts-node-esm", "deno"];

    for candidate in CANDIDATES {
        if let Some(path) = find_node_module_bin_for_files(candidate, files) {
            return Some(path);
        }
    }

    super::find_binary_in_path(CANDIDATES)
}

pub fn resolve_js_runner_command(runner: &str, files: &[PathBuf]) -> PathBuf {
    if is_path_like_runner(runner) {
        return PathBuf::from(runner);
    }

    find_node_module_bin_for_files(runner, files)
        .or_else(|| super::find_binary_in_path(&[runner]))
        .unwrap_or_else(|| PathBuf::from(runner))
}

fn build_deno_command(deno_runner: &OsStr, runner_script: &Path, files: &[PathBuf]) -> Command {
    let mut command = Command::new(deno_runner);
    command
        .arg("run")
        .arg("-A")
        .arg("--node-modules-dir=auto")
        .arg("--unstable-detect-cjs")
        .arg(runner_script);
    for file in files {
        command.arg(file);
    }
    command
}

fn is_path_like_runner(runner: &str) -> bool {
    let path = Path::new(runner);
    path.is_absolute() || runner.contains('/') || runner.contains('\\') || runner.starts_with('.')
}

fn is_deno_runner_path(runner: &Path) -> bool {
    runner
        .file_name()
        .and_then(|value| value.to_str())
        .map(|name| name.eq_ignore_ascii_case("deno") || name.eq_ignore_ascii_case("deno.exe"))
        .unwrap_or(false)
}

fn find_node_module_bin_for_files(binary: &str, files: &[PathBuf]) -> Option<PathBuf> {
    for root in js_runner_search_roots(files) {
        if let Some(path) = find_node_module_bin(binary, &root) {
            return Some(path);
        }
    }
    None
}

fn js_runner_search_roots(files: &[PathBuf]) -> Vec<PathBuf> {
    let mut roots = Vec::new();
    if let Ok(cwd) = std::env::current_dir() {
        roots.push(cwd.clone());
        for file in files {
            let absolute = if file.is_absolute() {
                file.clone()
            } else {
                cwd.join(file)
            };
            if let Some(parent) = absolute.parent() {
                roots.push(parent.to_path_buf());
            }
        }
    }
    roots
}

fn find_node_module_bin(binary: &str, start: &Path) -> Option<PathBuf> {
    let mut current = Some(start);
    while let Some(dir) = current {
        let base = dir.join("node_modules").join(".bin").join(binary);
        if base.is_file() {
            return Some(base);
        }
        if cfg!(windows) {
            for candidate in super::with_windows_extensions(&base) {
                if candidate.is_file() {
                    return Some(candidate);
                }
            }
        }
        current = dir.parent();
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn path_like_runner_detection() {
        assert!(is_path_like_runner("./tsx"));
        assert!(is_path_like_runner("bin/tsx"));
        assert!(!is_path_like_runner("tsx"));
    }
}
