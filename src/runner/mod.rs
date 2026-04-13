use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

pub mod js;
pub mod py;

pub(crate) fn materialize_runner_script_in_cwd(file_name: &str, source: &str) -> Result<PathBuf> {
    let cwd = std::env::current_dir().context("failed to resolve current working directory")?;
    materialize_runner_script_in_root(&cwd, file_name, source)
}

pub(crate) fn materialize_runner_script_in_root(
    root: &Path,
    file_name: &str,
    source: &str,
) -> Result<PathBuf> {
    let cache_dir = crate::bt_dir::runners_cache_dir(root);
    ensure_descendant_components_not_symlinks(root, &cache_dir)?;
    materialize_runner_script(&cache_dir, file_name, source)
}

fn materialize_runner_script(cache_dir: &Path, file_name: &str, source: &str) -> Result<PathBuf> {
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

pub(crate) fn find_binary_in_path(candidates: &[&str]) -> Option<PathBuf> {
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

#[cfg(windows)]
pub(crate) fn with_windows_extensions(path: &Path) -> [PathBuf; 2] {
    [path.with_extension("exe"), path.with_extension("cmd")]
}

#[cfg(not(windows))]
pub(crate) fn with_windows_extensions(_path: &Path) -> [PathBuf; 0] {
    []
}

#[cfg(test)]
mod tests {
    use super::*;

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
