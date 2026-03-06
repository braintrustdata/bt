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

        Ok(has_tracked_changes(&String::from_utf8_lossy(
            &output.stdout,
        )))
    }

    pub fn discover_from(path: &Path) -> Option<Self> {
        find_repo_root_from(path).map(|root| Self { root })
    }
}

fn has_tracked_changes(porcelain: &str) -> bool {
    porcelain
        .lines()
        .map(str::trim_end)
        .filter(|line| !line.is_empty())
        .any(|line| !line.starts_with("?? ") && !line.starts_with("!! "))
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
    use std::process::Command;

    use super::*;

    fn run_git(cwd: &Path, args: &[&str]) {
        let status = Command::new("git")
            .args(args)
            .current_dir(cwd)
            .status()
            .expect("run git");
        assert!(
            status.success(),
            "git command failed: git {}",
            args.join(" ")
        );
    }

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

    #[test]
    fn tracked_modifications_are_reported_dirty() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-git-dirty-{unique}"));
        fs::create_dir_all(&root).expect("create repo root");

        run_git(&root, &["init"]);
        run_git(&root, &["config", "user.email", "tests@example.com"]);
        run_git(&root, &["config", "user.name", "BT Tests"]);

        let file = root.join("tracked.txt");
        fs::write(&file, "v1\n").expect("write tracked file");
        run_git(&root, &["add", "tracked.txt"]);
        run_git(&root, &["commit", "-m", "init"]);

        fs::write(&file, "v2\n").expect("modify tracked file");

        let repo = GitRepo { root: root.clone() };
        assert!(repo.is_dirty_or_untracked(&file).expect("git status"));

        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn untracked_file_is_not_treated_as_dirty_for_pull_compat() {
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root = std::env::temp_dir().join(format!("bt-git-untracked-{unique}"));
        fs::create_dir_all(&root).expect("create repo root");

        run_git(&root, &["init"]);
        run_git(&root, &["config", "user.email", "tests@example.com"]);
        run_git(&root, &["config", "user.name", "BT Tests"]);

        let tracked = root.join("tracked.txt");
        fs::write(&tracked, "v1\n").expect("write tracked file");
        run_git(&root, &["add", "tracked.txt"]);
        run_git(&root, &["commit", "-m", "init"]);

        let untracked = root.join("untracked.txt");
        fs::write(&untracked, "local-only\n").expect("write untracked file");

        let repo = GitRepo { root: root.clone() };
        assert!(!repo.is_dirty_or_untracked(&untracked).expect("git status"));

        let _ = fs::remove_dir_all(root);
    }
}
