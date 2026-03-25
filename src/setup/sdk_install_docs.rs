use std::path::Path;

use anyhow::Result;

use super::write_text_file;

const PYTHON_DOCS: &str = include_str!("../../skills/sdk-install/python.md");
const TYPESCRIPT_DOCS: &str = include_str!("../../skills/sdk-install/typescript.md");
const GO_DOCS: &str = include_str!("../../skills/sdk-install/go.md");
const RUBY_DOCS: &str = include_str!("../../skills/sdk-install/ruby.md");
const JAVA_DOCS: &str = include_str!("../../skills/sdk-install/java.md");
const CSHARP_DOCS: &str = include_str!("../../skills/sdk-install/csharp.md");

const INDEX: &str = "# SDK Install Docs

Per-language SDK installation guides. Read the file for the detected language.

- [Python](python.md)
- [TypeScript](typescript.md)
- [Go](go.md)
- [Ruby](ruby.md)
- [Java](java.md)
- [C#](csharp.md)
";

/// Write all SDK install docs to `<output_dir>/sdk-install/`.
pub fn write_sdk_install_docs(output_dir: &Path) -> Result<()> {
    let dir = output_dir.join("sdk-install");
    write_text_file(&dir.join("python.md"), PYTHON_DOCS)?;
    write_text_file(&dir.join("typescript.md"), TYPESCRIPT_DOCS)?;
    write_text_file(&dir.join("go.md"), GO_DOCS)?;
    write_text_file(&dir.join("ruby.md"), RUBY_DOCS)?;
    write_text_file(&dir.join("java.md"), JAVA_DOCS)?;
    write_text_file(&dir.join("csharp.md"), CSHARP_DOCS)?;
    write_text_file(&dir.join("_index.md"), INDEX)?;
    Ok(())
}
