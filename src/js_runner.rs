use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};

pub fn materialize_runner_script(
    cache_dir: &Path,
    file_name: &str,
    source: &str,
) -> Result<PathBuf> {
    std::fs::create_dir_all(cache_dir).with_context(|| {
        format!(
            "failed to create runner cache directory {}",
            cache_dir.display()
        )
    })?;
    ensure_not_symlink(cache_dir)?;

    let path = cache_dir.join(file_name);
    ensure_not_symlink(&path)?;
    let current = std::fs::read_to_string(&path).ok();
    if current.as_deref() != Some(source) {
        crate::utils::write_text_atomic(&path, source)
            .with_context(|| format!("failed to write runner script {}", path.display()))?;
    }
    Ok(path)
}

pub fn materialize_runner_script_in_cwd(
    cache_subdir: &str,
    file_name: &str,
    source: &str,
) -> Result<PathBuf> {
    let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
    let cache_dir = crate::bt_dir::cache_dir(&cwd)
        .join(cache_subdir)
        .join(env!("CARGO_PKG_VERSION"));
    ensure_descendant_components_not_symlinks(&cwd, &cache_dir)?;
    materialize_runner_script(&cache_dir, file_name, source)
}

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

    find_binary_in_path(CANDIDATES)
}

pub fn resolve_js_runner_command(runner: &str, files: &[PathBuf]) -> PathBuf {
    if is_path_like_runner(runner) {
        return PathBuf::from(runner);
    }

    find_node_module_bin_for_files(runner, files)
        .or_else(|| find_binary_in_path(&[runner]))
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
            for candidate in with_windows_extensions(&base) {
                if candidate.is_file() {
                    return Some(candidate);
                }
            }
        }
        current = dir.parent();
    }
    None
}

fn find_binary_in_path(candidates: &[&str]) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&paths) {
        for candidate in candidates {
            let path = dir.join(candidate);
            if path.is_file() {
                return Some(path);
            }
            if cfg!(windows) {
                for candidate_path in with_windows_extensions(&path) {
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

fn ensure_descendant_components_not_symlinks(base: &Path, descendant: &Path) -> Result<()> {
    let Ok(relative) = descendant.strip_prefix(base) else {
        return Ok(());
    };

    let mut current = base.to_path_buf();
    for component in relative.components() {
        current.push(component.as_os_str());
        let metadata = match std::fs::symlink_metadata(&current) {
            Ok(metadata) => metadata,
            Err(err) if err.kind() == std::io::ErrorKind::NotFound => break,
            Err(err) => {
                return Err(err).with_context(|| {
                    format!("failed to inspect path component {}", current.display())
                })
            }
        };
        if metadata.file_type().is_symlink() {
            bail!(
                "refusing to write runner script through symlink path component {}",
                current.display()
            );
        }
    }
    Ok(())
}

fn ensure_not_symlink(path: &Path) -> Result<()> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            if metadata.file_type().is_symlink() {
                bail!(
                    "refusing to write runner script via symlink {}",
                    path.display()
                );
            }
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
        Err(err) => {
            return Err(err)
                .with_context(|| format!("failed to inspect runner path {}", path.display()))
        }
    }
    Ok(())
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

    #[cfg(unix)]
    #[test]
    fn descendant_symlink_check_rejects_symlinked_component() {
        use std::os::unix::fs::symlink;

        let dir = tempfile::tempdir().expect("tempdir");
        let base = dir.path().join("base");
        let real = dir.path().join("real");
        std::fs::create_dir_all(&base).expect("create base directory");
        std::fs::create_dir_all(&real).expect("create real directory");
        let link = base.join("link");
        symlink(&real, &link).expect("create symlink");

        let err = ensure_descendant_components_not_symlinks(&base, &link.join("cache"))
            .expect_err("must reject symlink path");
        assert!(err.to_string().contains("symlink"));
    }
}
