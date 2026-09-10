use std::io::Write as _;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;
use tempfile::Builder;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Visibility {
    Default,
    Private,
}

#[cfg(unix)]
impl Visibility {
    fn mode(self) -> u32 {
        match self {
            Visibility::Default => 0o644,
            Visibility::Private => 0o600,
        }
    }
}

pub fn write_text_atomic(path: &Path, contents: &str) -> Result<()> {
    write_bytes_atomic(path, contents.as_bytes())
}

pub fn write_json_atomic<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    write_json(path, value, Visibility::Default)
}

pub fn write_json_atomic_private<T: Serialize>(path: &Path, value: &T) -> Result<()> {
    write_json(path, value, Visibility::Private)
}

fn write_json<T: Serialize>(path: &Path, value: &T, visibility: Visibility) -> Result<()> {
    let mut json = serde_json::to_string_pretty(value)
        .with_context(|| format!("failed to serialize {}", path.display()))?;
    json.push('\n');
    write_atomic(path, json.as_bytes(), visibility)
}

pub fn write_bytes_atomic(path: &Path, contents: &[u8]) -> Result<()> {
    write_atomic(path, contents, Visibility::Default)
}

fn write_atomic(path: &Path, contents: &[u8], visibility: Visibility) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        anyhow::anyhow!(
            "cannot atomically write {} because it has no parent directory",
            path.display()
        )
    })?;

    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create parent directory {}", parent.display()))?;

    #[cfg(unix)]
    let builder = {
        use std::os::unix::fs::PermissionsExt;
        let mut builder = Builder::new();
        builder.permissions(std::fs::Permissions::from_mode(visibility.mode()));
        builder
    };
    #[cfg(not(unix))]
    let builder = {
        let _ = visibility;
        Builder::new()
    };

    let mut file = builder
        .tempfile_in(parent)
        .with_context(|| format!("failed to create temporary file in {}", parent.display()))?;

    file.write_all(contents)
        .with_context(|| format!("failed to write temporary file for {}", path.display()))?;

    file.persist(path)
        .map_err(|err| err.error)
        .with_context(|| format!("failed to replace {}", path.display()))?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn write_text_atomic_creates_file() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("nested").join("out.txt");

        write_text_atomic(&path, "hello").expect("write");

        assert_eq!(std::fs::read_to_string(&path).expect("read"), "hello");
    }

    #[test]
    fn write_text_atomic_overwrites_existing_file() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("out.txt");
        std::fs::write(&path, "old").expect("seed file");

        write_text_atomic(&path, "new").expect("write");

        assert_eq!(std::fs::read_to_string(&path).expect("read"), "new");
    }

    #[cfg(unix)]
    #[test]
    fn write_json_atomic_private_tightens_existing_permissions() {
        use std::os::unix::fs::PermissionsExt;

        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("secrets.json");
        std::fs::write(&path, "{}").expect("seed file");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .expect("seed permissions");

        write_json_atomic_private(&path, &serde_json::json!({"secrets": {}})).expect("write");

        let mode = std::fs::metadata(&path)
            .expect("metadata")
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        assert_eq!(
            std::fs::read_to_string(&path).expect("read"),
            "{\n  \"secrets\": {}\n}\n"
        );
    }
}
