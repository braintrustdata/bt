use std::ffi::{OsStr, OsString};
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
    let cache_dir = cwd
        .join(".bt")
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

    let mut command = Command::new(npx_program());
    command.arg("--yes").arg("tsx").arg(runner_script);
    for file in files {
        command.arg(file);
    }
    command
}

/// Windows installs `npx` as `npx.cmd`, which `Command::new("npx")` does not
/// resolve, so look it up on PATH first.
pub fn npx_program() -> PathBuf {
    find_binary_in_path(&["npx"]).unwrap_or_else(|| PathBuf::from("npx"))
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
        let path = Path::new(runner);
        return executable_candidates(path)
            .find(|candidate| candidate.is_file())
            .unwrap_or_else(|| path.to_path_buf());
    }

    find_node_module_bin_for_files(runner, files)
        .or_else(|| find_binary_in_path(&[runner]))
        .unwrap_or_else(|| PathBuf::from(runner))
}

fn build_deno_command(deno_runner: &OsStr, runner_script: &Path, files: &[PathBuf]) -> Command {
    let mut command = Command::new(deno_runner);
    command.args(deno_runner_args(runner_script, files));
    command
}

pub fn deno_runner_args(runner_script: &Path, files: &[PathBuf]) -> Vec<OsString> {
    let mut args = vec![
        OsString::from("run"),
        OsString::from("-A"),
        OsString::from("--node-modules-dir=auto"),
        OsString::from("--unstable-detect-cjs"),
        runner_script.as_os_str().to_os_string(),
    ];
    args.extend(files.iter().map(|file| file.as_os_str().to_os_string()));
    args
}

fn is_path_like_runner(runner: &str) -> bool {
    let path = Path::new(runner);
    path.is_absolute() || runner.contains('/') || runner.contains('\\') || runner.starts_with('.')
}

pub fn is_deno_runner_path(runner: &Path) -> bool {
    runner
        .file_name()
        .and_then(|value| value.to_str())
        .map(|name| name.eq_ignore_ascii_case("deno") || name.eq_ignore_ascii_case("deno.exe"))
        .unwrap_or(false)
}

pub fn find_node_module_bin_for_files(binary: &str, files: &[PathBuf]) -> Option<PathBuf> {
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
        if let Some(found) = executable_candidates(&base).find(|path| path.is_file()) {
            return Some(found);
        }
        current = dir.parent();
    }
    None
}

pub fn find_binary_in_path(candidates: &[&str]) -> Option<PathBuf> {
    let paths = std::env::var_os("PATH")?;
    for dir in std::env::split_paths(&paths) {
        for candidate in candidates {
            let path = dir.join(candidate);
            if let Some(found) = executable_candidates(&path).find(|path| path.is_file()) {
                return Some(found);
            }
        }
    }
    None
}

pub fn runner_bin_name(runner_command: &Path) -> Option<String> {
    let name = runner_command.file_name()?.to_str()?.to_ascii_lowercase();
    let name = [".cmd", ".exe"]
        .iter()
        .find_map(|suffix| name.strip_suffix(suffix))
        .unwrap_or(&name);
    Some(name.to_string())
}

pub fn is_ts_node_runner_path(runner_command: &Path) -> bool {
    runner_bin_name(runner_command).is_some_and(|name| name == "ts-node" || name == "ts-node-esm")
}

/// npm and pnpm install an extensionless POSIX shell shim next to `.cmd` on
/// Windows, which `CreateProcess` cannot run, so Windows tries launchers first.
pub(crate) fn executable_candidates(path: &Path) -> impl Iterator<Item = PathBuf> {
    let has_launcher_extension = path
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| {
            ["exe", "cmd", "bat"]
                .iter()
                .any(|launcher| extension.eq_ignore_ascii_case(launcher))
        });
    let windows_launchers = if cfg!(windows) && !has_launcher_extension {
        [".exe", ".cmd"]
            .iter()
            .map(|suffix| {
                let mut candidate = path.as_os_str().to_os_string();
                candidate.push(suffix);
                PathBuf::from(candidate)
            })
            .collect()
    } else {
        Vec::new()
    };
    windows_launchers
        .into_iter()
        .chain(std::iter::once(path.to_path_buf()))
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

    #[test]
    fn executable_candidates_prefer_windows_launchers() {
        let base = Path::new("bin/python3.12");
        let candidates: Vec<PathBuf> = executable_candidates(base).collect();
        let expected: Vec<PathBuf> = if cfg!(windows) {
            vec![
                PathBuf::from("bin/python3.12.exe"),
                PathBuf::from("bin/python3.12.cmd"),
                base.into(),
            ]
        } else {
            vec![base.into()]
        };
        assert_eq!(candidates, expected);
    }

    #[test]
    fn explicit_launcher_extension_is_kept() {
        let dir = tempfile::tempdir().expect("tempdir");
        let cmd = dir.path().join("runner.cmd");
        std::fs::write(&cmd, "").expect("write runner.cmd");
        std::fs::write(dir.path().join("runner.exe"), "").expect("write runner.exe");

        assert_eq!(
            executable_candidates(&cmd).collect::<Vec<_>>(),
            vec![cmd.clone()]
        );
        let resolved = resolve_js_runner_command(cmd.to_str().expect("utf-8 path"), &[]);
        assert_eq!(resolved, cmd);
    }

    #[test]
    fn runner_bin_name_strips_windows_extensions() {
        for (path, expected) in [
            ("bun.exe", "bun"),
            ("TSX.CMD", "tsx"),
            ("vite-node", "vite-node"),
        ] {
            assert_eq!(runner_bin_name(Path::new(path)).as_deref(), Some(expected));
        }
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
