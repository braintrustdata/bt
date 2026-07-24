use std::io::Read;

use anyhow::{bail, Context, Result};

/// Resolve inline text, an `@PATH` file reference, or `-` for stdin.
///
/// A leading literal `@` can be escaped as `@@`.
pub(crate) fn read_text_source(value: &str, label: &str) -> Result<String> {
    if value == "-" {
        let mut content = String::new();
        std::io::stdin()
            .read_to_string(&mut content)
            .with_context(|| format!("failed to read {label} from stdin"))?;
        return Ok(content);
    }

    if let Some(literal) = value.strip_prefix("@@") {
        return Ok(format!("@{literal}"));
    }

    if let Some(path) = value.strip_prefix('@') {
        if path.is_empty() {
            bail!("{label} file path cannot be empty after '@'");
        }
        return std::fs::read_to_string(path)
            .with_context(|| format!("failed to read {label} file {path}"));
    }

    Ok(value.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn returns_inline_text() {
        assert_eq!(
            read_text_source("Judge the answer.", "prompt").expect("inline prompt"),
            "Judge the answer."
        );
    }

    #[test]
    fn reads_at_prefixed_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("judge.md");
        std::fs::write(&path, "Judge from a file.\n").expect("write prompt");

        let source = format!("@{}", path.display());
        assert_eq!(
            read_text_source(&source, "prompt").expect("file prompt"),
            "Judge from a file.\n"
        );
    }

    #[test]
    fn double_at_escapes_literal_at() {
        assert_eq!(
            read_text_source("@@mention", "prompt").expect("literal prompt"),
            "@mention"
        );
    }

    #[test]
    fn rejects_empty_file_reference() {
        let error = read_text_source("@", "prompt").expect_err("empty path should fail");
        assert!(error.to_string().contains("cannot be empty"));
    }
}
