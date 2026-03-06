use std::path::Path;

use anyhow::{Context, Result};

pub fn write_text_atomic(path: &Path, contents: &str) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        anyhow::anyhow!(
            "cannot atomically write {} because it has no parent directory",
            path.display()
        )
    })?;

    std::fs::create_dir_all(parent)
        .with_context(|| format!("failed to create parent directory {}", parent.display()))?;

    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .context("failed to read system time for atomic write")?
        .as_nanos();
    let pid = std::process::id();

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow::anyhow!("invalid target file name: {}", path.display()))?;

    let tmp = parent.join(format!(".{file_name}.tmp.{pid}.{nonce}"));

    std::fs::write(&tmp, contents)
        .with_context(|| format!("failed to write temporary file {}", tmp.display()))?;

    std::fs::rename(&tmp, path).with_context(|| {
        format!(
            "failed to replace {} with temporary file {}",
            path.display(),
            tmp.display()
        )
    })?;

    Ok(())
}
