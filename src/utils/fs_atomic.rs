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

    replace_file_atomic(&tmp, path)?;

    Ok(())
}

#[cfg(not(windows))]
fn replace_file_atomic(tmp: &Path, path: &Path) -> Result<()> {
    std::fs::rename(tmp, path).with_context(|| {
        format!(
            "failed to replace {} with temporary file {}",
            path.display(),
            tmp.display()
        )
    })?;
    Ok(())
}

#[cfg(windows)]
fn replace_file_atomic(tmp: &Path, path: &Path) -> Result<()> {
    if path.exists() {
        replace_existing_file_windows(tmp, path)?;
        return Ok(());
    }

    let rename_attempt = std::fs::rename(tmp, path);
    if rename_attempt.is_ok() {
        return Ok(());
    }

    if path.exists() {
        replace_existing_file_windows(tmp, path)?;
        return Ok(());
    }

    rename_attempt.with_context(|| {
        format!(
            "failed to replace {} with temporary file {}",
            path.display(),
            tmp.display()
        )
    })?;
    Ok(())
}

#[cfg(windows)]
fn replace_existing_file_windows(tmp: &Path, path: &Path) -> Result<()> {
    use std::iter;
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::ReplaceFileW;

    let target = path
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect::<Vec<u16>>();
    let replacement = tmp
        .as_os_str()
        .encode_wide()
        .chain(iter::once(0))
        .collect::<Vec<u16>>();

    // SAFETY: Both paths are null-terminated UTF-16 strings with stable backing
    // storage for the duration of the call, and optional pointers are null.
    let replaced = unsafe {
        ReplaceFileW(
            target.as_ptr(),
            replacement.as_ptr(),
            std::ptr::null(),
            0,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if replaced == 0 {
        return Err(std::io::Error::last_os_error()).with_context(|| {
            format!(
                "failed to replace {} with temporary file {}",
                path.display(),
                tmp.display()
            )
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn write_text_atomic_creates_file() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("file.txt");

        write_text_atomic(&path, "hello").expect("write");
        let contents = std::fs::read_to_string(&path).expect("read");
        assert_eq!(contents, "hello");
    }

    #[test]
    fn write_text_atomic_overwrites_existing_file() {
        let tmp = tempdir().expect("tempdir");
        let path = tmp.path().join("file.txt");
        std::fs::write(&path, "old").expect("seed file");

        write_text_atomic(&path, "new").expect("overwrite");
        let contents = std::fs::read_to_string(&path).expect("read");
        assert_eq!(contents, "new");
    }
}
