use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::utils::write_text_atomic;

pub const BT_DIR: &str = ".bt";
pub const CONFIG_FILE: &str = "config.json";
pub const GITIGNORE_FILE: &str = ".gitignore";

const MANAGED_START: &str = "# BEGIN bt-managed";
const MANAGED_END: &str = "# END bt-managed";
const MANAGED_BODY: &str = "*\n!config.json\n!.gitignore\n!skills/\n!skills/**\n";

pub fn bt_dir(root: &Path) -> PathBuf {
    root.join(BT_DIR)
}

pub fn config_path(root: &Path) -> PathBuf {
    bt_dir(root).join(CONFIG_FILE)
}

pub fn gitignore_path(root: &Path) -> PathBuf {
    bt_dir(root).join(GITIGNORE_FILE)
}

pub fn cache_dir(root: &Path) -> PathBuf {
    bt_dir(root).join("cache")
}

pub fn state_dir(root: &Path) -> PathBuf {
    bt_dir(root).join("state")
}

pub fn skills_dir(root: &Path) -> PathBuf {
    bt_dir(root).join("skills")
}

pub fn skills_docs_dir(root: &Path) -> PathBuf {
    skills_dir(root).join("docs")
}

pub fn ensure_repo_layout(root: &Path) -> Result<()> {
    std::fs::create_dir_all(bt_dir(root))?;
    ensure_bt_gitignore(root)
}

pub fn ensure_bt_gitignore(root: &Path) -> Result<()> {
    let path = gitignore_path(root);
    let existing = match std::fs::read_to_string(&path) {
        Ok(contents) => contents,
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => String::new(),
        Err(err) => return Err(err.into()),
    };
    let updated = upsert_managed_block(&existing);
    if updated != existing {
        write_text_atomic(&path, &updated)?;
    }
    Ok(())
}

fn managed_block() -> String {
    format!("{MANAGED_START}\n{MANAGED_BODY}{MANAGED_END}\n")
}

fn upsert_managed_block(existing: &str) -> String {
    let managed = managed_block();
    let user_tail = strip_managed_block(existing);

    if user_tail.is_empty() {
        managed
    } else {
        format!("{managed}\n{user_tail}\n")
    }
}

fn strip_managed_block(existing: &str) -> String {
    let Some(start) = existing.find(MANAGED_START) else {
        return existing.to_string();
    };
    let Some(end_marker) = existing[start..].find(MANAGED_END) else {
        return existing.to_string();
    };

    let mut end = start + end_marker + MANAGED_END.len();
    while end < existing.len() && existing.as_bytes()[end] == b'\n' {
        end += 1;
    }

    let mut out = String::new();
    out.push_str(&existing[..start]);
    out.push_str(&existing[end..]);
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn upsert_adds_managed_block_for_empty_file() {
        let updated = upsert_managed_block("");
        assert!(updated.starts_with(MANAGED_START));
        assert!(updated.contains("!config.json"));
        assert!(updated.contains("!skills/**"));
    }

    #[test]
    fn upsert_places_managed_block_first_and_preserves_custom_rules() {
        let existing =
            "custom-before\n\n# BEGIN bt-managed\nold\n# END bt-managed\n\ncustom-after\n";
        let updated = upsert_managed_block(existing);
        assert!(updated.starts_with("# BEGIN bt-managed\n"));
        assert!(updated.contains("custom-before"));
        assert!(updated.contains("custom-after"));
        assert_eq!(updated.matches(MANAGED_START).count(), 1);
        assert_eq!(updated.matches(MANAGED_END).count(), 1);
        let end_pos = updated.find(MANAGED_END).unwrap();
        let before_pos = updated.find("custom-before").unwrap();
        let after_pos = updated.find("custom-after").unwrap();
        assert!(before_pos > end_pos);
        assert!(after_pos > end_pos);
    }

    #[test]
    fn strip_managed_block_returns_input_when_markers_incomplete() {
        let existing = "# BEGIN bt-managed\nno-end";
        assert_eq!(strip_managed_block(existing), existing);
    }
}
