use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};

#[derive(Debug, Clone)]
pub struct GitRepo {
    root: PathBuf,
}

impl GitRepo {
    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn is_dirty_or_untracked(&self, path: &Path) -> Result<bool> {
        let output = Command::new("git")
            .arg("-C")
            .arg(&self.root)
            .arg("status")
            .arg("--porcelain")
            .arg("--")
            .arg(path)
            .output()
            .with_context(|| {
                format!(
                    "failed to check git status for {} in {}",
                    path.display(),
                    self.root.display()
                )
            })?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            anyhow::bail!(
                "git status failed for {} in {}: {}",
                path.display(),
                self.root.display(),
                stderr.trim()
            );
        }

        Ok(!String::from_utf8_lossy(&output.stdout).trim().is_empty())
    }

    pub fn discover_from(path: &Path) -> Option<Self> {
        find_repo_root_from(path).map(|root| Self { root })
    }
}

pub fn find_repo_root_from(start: &Path) -> Option<PathBuf> {
    let mut current = start.to_path_buf();
    if current.is_file() {
        current = current.parent()?.to_path_buf();
    }

    loop {
        if current.join(".git").exists() {
            return Some(current);
        }
        if !current.pop() {
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    #[test]
    fn find_repo_root_detects_git_dir() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-git-root-{unique}"));
        let nested = root.join("a").join("b");
        fs::create_dir_all(&nested).expect("create nested dirs");
        fs::create_dir_all(root.join(".git")).expect("create .git dir");

        let found = find_repo_root_from(&nested).expect("should find root");
        assert_eq!(found, root);

        let _ = fs::remove_dir_all(found);
    }

    #[test]
    fn find_repo_root_detects_git_file() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-git-file-{unique}"));
        let nested = root.join("x").join("y");
        fs::create_dir_all(&nested).expect("create nested dirs");
        fs::write(root.join(".git"), "gitdir: /tmp/mock").expect("write .git file");

        let found = find_repo_root_from(&nested).expect("should find root");
        assert_eq!(found, root);

        let _ = fs::remove_dir_all(found);
    }
}
