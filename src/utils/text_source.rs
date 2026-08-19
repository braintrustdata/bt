use std::io::Read;
use std::sync::Mutex;

use anyhow::{bail, Context, Result};

/// Which label drained stdin, so a second `-` fails loudly instead of reading "".
static STDIN_READER: Mutex<Option<String>> = Mutex::new(None);

/// Resolve inline text, an `@PATH` file reference, or `-` for stdin.
///
/// A leading literal `@` can be escaped as `@@`. Only one source per invocation
/// may read from stdin.
pub(crate) fn read_text_source(value: &str, label: &str) -> Result<String> {
    read_text_source_with_stdin_guard(value, label, &STDIN_READER)
}

fn read_text_source_with_stdin_guard(
    value: &str,
    label: &str,
    stdin_reader: &Mutex<Option<String>>,
) -> Result<String> {
    if value == "-" {
        // The second reader would otherwise see "" and call it malformed input.
        let mut reader = stdin_reader
            .lock()
            .map_err(|_| anyhow::anyhow!("stdin guard poisoned"))?;
        if let Some(previous) = reader.as_deref() {
            bail!("stdin was already read for {previous}; only one source can be '-'");
        }
        *reader = Some(label.to_string());
        drop(reader);

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

    #[test]
    fn rejects_a_second_stdin_source() {
        // A local guard avoids draining the suite's shared stdin; the rejection
        // happens before any read.
        let guard = Mutex::new(Some("metadata".to_string()));

        let error = read_text_source_with_stdin_guard("-", "patch", &guard)
            .expect_err("second stdin source should fail");
        assert_eq!(
            error.to_string(),
            "stdin was already read for metadata; only one source can be '-'"
        );
    }
}
