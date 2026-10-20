//! Locating the Go toolchain, a user's prebuilt eval binary, and the packages
//! that hold their evals.
//!
//! Unlike JavaScript and Python, bt does not supply the program for Go. There
//! is no runtime loading in Go, so the user's compiled package *is* the runner
//! and nothing is materialized to disk on our side.
//!
//! Package layout is resolved by asking the Go toolchain (`go list -json`)
//! rather than by inspecting paths ourselves. `go list` is authoritative about
//! things bt cannot see from the filesystem: which files a build actually
//! includes (`//go:build` constraints, `GOOS`/`GOARCH`), where the enclosing
//! module really starts (`go.work` can move it), and whether a package is even
//! runnable (`package main`).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

/// Import path of the Go SDK package every eval runner links against.
///
/// This is what makes Go eval discovery convention-free: a package that imports
/// it is an eval runner, so no file-naming or directory-naming rule is needed.
pub const EVAL_RUNNER_IMPORT: &str = "github.com/braintrustdata/braintrust-sdk-go/evalrunner";

/// The subset of `go list -json` output bt uses.
#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GoPackage {
    /// Absolute directory holding the package's source.
    pub dir: PathBuf,
    pub import_path: String,
    /// Package name. `main` means the package is runnable.
    #[serde(default)]
    pub name: String,
    /// Go files the build includes, relative to `dir`, with build constraints
    /// already applied.
    #[serde(default)]
    pub go_files: Vec<String>,
    /// Transitive imports, used to spot packages that link the eval runner.
    #[serde(default)]
    pub deps: Vec<String>,
    #[serde(default)]
    pub module: Option<GoModule>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct GoModule {
    /// Module root. Absent for packages outside any module.
    #[serde(default)]
    pub dir: Option<PathBuf>,
}

impl GoPackage {
    /// True when this package is a runnable eval runner.
    pub fn is_eval_runner(&self) -> bool {
        self.name == "main" && self.deps.iter().any(|dep| dep == EVAL_RUNNER_IMPORT)
    }

    /// The package's build inputs as absolute paths.
    pub fn absolute_go_files(&self) -> Vec<String> {
        self.go_files
            .iter()
            .map(|f| self.dir.join(f).to_string_lossy().into_owned())
            .collect()
    }
}

/// Resolves the Go toolchain to invoke.
///
/// Explicit environment overrides win, then `GOROOT`, then `PATH`. The override
/// matters for version managers (mise, asdf, gvm) that keep the real toolchain
/// off the default `PATH`.
pub fn resolve_go_toolchain(env_overrides: &[&str]) -> Option<PathBuf> {
    for env_name in env_overrides {
        if let Some(value) = std::env::var_os(env_name) {
            if !value.is_empty() {
                return Some(PathBuf::from(value));
            }
        }
    }

    if let Some(goroot) = std::env::var_os("GOROOT") {
        let candidate = PathBuf::from(goroot).join("bin").join("go");
        if candidate.is_file() {
            return Some(candidate);
        }
    }

    crate::python_runner::find_binary_in_path(&["go"])
}

/// Resolves `--runner` for Go, which names a prebuilt eval binary rather than
/// an interpreter.
///
/// Unlike the Python interpreter override, this is validated: the Go runner
/// takes all its input from the environment and ignores argv, so a typo'd path
/// would otherwise surface only as an opaque spawn failure.
pub fn resolve_prebuilt_runner(explicit: &str) -> Result<PathBuf> {
    let path = Path::new(explicit);
    if runner_looks_like_path(explicit) {
        if !path.is_file() {
            anyhow::bail!("--runner binary not found: {explicit}");
        }
        return Ok(path.to_path_buf());
    }

    // A bare name: prefer PATH, but fall through to the literal so the spawn
    // error names it, matching how the JS runner override behaves.
    Ok(crate::python_runner::find_binary_in_path(&[explicit])
        .unwrap_or_else(|| PathBuf::from(explicit)))
}

/// True when `--runner` names a file by path rather than a command on PATH.
pub fn runner_looks_like_path(runner: &str) -> bool {
    Path::new(runner).is_absolute()
        || runner.contains('/')
        || runner.contains('\\')
        || runner.starts_with('.')
}

/// Magic that opens the build-info blob the Go linker embeds in every binary
/// (what `go version <file>` reads). It survives `-ldflags="-s -w"`.
const GO_BUILDINFO_MAGIC: &[u8] = b"\xff Go buildinf:";

/// The blob is 16-byte aligned, and so is its file offset in practice.
const GO_BUILDINFO_ALIGN: usize = 16;

/// True when `path` is a Go executable that links the SDK's eval runner.
///
/// Lets `bt eval --runner ./bin/evals` recognize a prebuilt Go eval binary
/// without a Go toolchain or `--language go`. Both marks are required: the Go
/// linker's build info says the file is a Go binary, and [`EVAL_RUNNER_IMPORT`]
/// (kept in function names even when stripped) says it is an eval runner -- the
/// same rule discovery applies to packages. A Go-built wrapper that launches a
/// JS or Python runner therefore does not match. Scripts and shims (`tsx`, a
/// virtualenv's `python`) are rejected by their first bytes, before the rest of
/// the file is read.
pub fn is_go_eval_runner_binary(path: &Path) -> bool {
    use std::io::Read;

    const EXECUTABLE_MAGICS: &[&[u8]] = &[
        b"\x7fELF",
        b"MZ",
        b"\xcf\xfa\xed\xfe",
        b"\xce\xfa\xed\xfe",
        b"\xca\xfe\xba\xbe",
    ];
    let runner_import = EVAL_RUNNER_IMPORT.as_bytes();

    let Ok(mut file) = std::fs::File::open(path) else {
        return false;
    };
    let mut buf = vec![0u8; 1 << 20];
    let mut first = true;
    let mut has_build_info = false;
    let mut links_runner = false;
    // End of the previous chunk, so an import path split across two chunks is
    // still found.
    let mut tail: Vec<u8> = Vec::new();
    loop {
        // Fill whole chunks so each starts on an aligned offset; an aligned
        // blob then never straddles two chunks.
        let mut filled = 0;
        while filled < buf.len() {
            match file.read(&mut buf[filled..]) {
                Ok(0) => break,
                Ok(read) => filled += read,
                Err(_) => return false,
            }
        }
        let chunk = &buf[..filled];
        if first
            && !EXECUTABLE_MAGICS
                .iter()
                .any(|magic| chunk.starts_with(magic))
        {
            return false;
        }
        first = false;

        has_build_info = has_build_info
            || chunk
                .chunks_exact(GO_BUILDINFO_ALIGN)
                .any(is_go_buildinfo_header);
        if !links_runner {
            let keep = runner_import.len() - 1;
            tail.extend_from_slice(&chunk[..chunk.len().min(keep)]);
            links_runner = contains(&tail, runner_import) || contains(chunk, runner_import);
            tail.clear();
            tail.extend_from_slice(&chunk[chunk.len().saturating_sub(keep)..]);
        }
        if has_build_info && links_runner {
            return true;
        }
        if filled < buf.len() {
            return false;
        }
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// Matches the blob header as `debug/buildinfo` does: magic, then a pointer
/// size of 4 or 8, then flags with the inline-strings bit every Go 1.18+ binary
/// sets. Checking past the magic keeps a binary that merely contains the magic
/// as a string (bt itself does) from counting.
fn is_go_buildinfo_header(header: &[u8]) -> bool {
    header.starts_with(GO_BUILDINFO_MAGIC)
        && matches!(header[GO_BUILDINFO_MAGIC.len()], 4 | 8)
        && header[GO_BUILDINFO_MAGIC.len() + 1] & 0x2 != 0
}

/// True when `dir` sits inside a Go module.
///
/// Checked before spawning `go list` so that a non-Go directory costs nothing
/// and keeps reporting the ordinary "no eval files found" error.
pub fn in_go_module(dir: &Path) -> bool {
    in_go_module_from(dir, &std::env::current_dir().unwrap_or_default())
}

/// Like [`in_go_module`], resolving a relative `dir` against `cwd` first.
///
/// `Path::parent` is lexical, so climbing `.` or `cmd` as typed stops at the
/// cwd instead of reaching its real ancestors.
fn in_go_module_from(dir: &Path, cwd: &Path) -> bool {
    cwd.join(dir)
        .ancestors()
        .any(|candidate| candidate.join("go.mod").is_file())
}

/// Runs `go list -json <patterns>` in `cwd` and returns the packages it reports.
///
/// Costs roughly what the `go run` that follows it costs, which is not worth
/// caching against: an eval run takes seconds, and a stale listing would be a
/// correctness question for no meaningful gain.
pub fn list_packages(go: &Path, patterns: &[String], cwd: &Path) -> Result<Vec<GoPackage>> {
    let mut command = std::process::Command::new(go);
    command
        .arg("list")
        .arg("-json")
        .args(patterns)
        .current_dir(cwd);

    let output = command
        .output()
        .with_context(|| format!("failed to run `{} list -json`", go.display()))?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr).trim().to_string();
        anyhow::bail!(
            "`go list` failed for {}: {}",
            patterns.join(" "),
            if stderr.is_empty() {
                "no output".to_string()
            } else {
                stderr
            }
        );
    }

    parse_go_list(&output.stdout)
}

/// Parses the stream of concatenated JSON objects `go list -json` emits.
///
/// The output is not a JSON array: one object follows another, so it has to be
/// read as a stream rather than deserialized in one shot.
pub fn parse_go_list(stdout: &[u8]) -> Result<Vec<GoPackage>> {
    serde_json::Deserializer::from_slice(stdout)
        .into_iter::<GoPackage>()
        .collect::<Result<Vec<_>, _>>()
        .context("failed to parse `go list -json` output")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_temp_dir(label: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!("bt-go-runner-{label}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("temp dir should be created");
        dir
    }

    // `bt eval` passes the path as typed (`.` when bare), and `Path::parent`
    // on a relative path never reaches the real ancestors of the cwd.
    #[test]
    fn in_go_module_finds_an_ancestor_go_mod() {
        let dir = make_temp_dir("in-module");
        let cmd = dir.join("cmd");
        let pkg = cmd.join("a");
        std::fs::create_dir_all(&pkg).expect("package dir should be created");
        std::fs::write(dir.join("go.mod"), "module example.test\n").expect("go.mod written");

        assert!(in_go_module(&pkg));
        assert!(in_go_module_from(Path::new("."), &cmd));
        assert!(in_go_module_from(Path::new("a"), &cmd));
        assert!(!in_go_module_from(Path::new("."), &std::env::temp_dir()));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prebuilt_runner_rejects_a_missing_path() {
        let err = resolve_prebuilt_runner("./definitely/not/here")
            .expect_err("a missing path should fail");
        assert!(format!("{err:#}").contains("--runner binary not found"));
    }

    #[test]
    fn prebuilt_runner_accepts_an_existing_path() {
        let dir = make_temp_dir("prebuilt");
        let binary = dir.join("evals");
        std::fs::write(&binary, b"#!/bin/sh\n").expect("binary written");

        let resolved =
            resolve_prebuilt_runner(binary.to_str().expect("utf-8 path")).expect("should resolve");
        assert_eq!(resolved, binary);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // `go list -json` emits concatenated objects, not a JSON array, so the
    // parser has to read them as a stream.
    #[test]
    fn parse_go_list_reads_concatenated_objects() {
        let stdout = br#"{
            "Dir": "/repo/a",
            "ImportPath": "example.test/a",
            "Name": "main",
            "GoFiles": ["main.go", "classifier.go"]
        }
        {
            "Dir": "/repo/b",
            "ImportPath": "example.test/b",
            "Name": "b"
        }"#;

        let packages = parse_go_list(stdout).expect("stream should parse");

        assert_eq!(packages.len(), 2);
        assert_eq!(packages[0].import_path, "example.test/a");
        assert_eq!(packages[1].name, "b");
        assert_eq!(
            packages[0].absolute_go_files(),
            vec![
                "/repo/a/main.go".to_string(),
                "/repo/a/classifier.go".to_string()
            ]
        );
    }

    // Discovery keys off the runner import, which is what lets Go evals live in
    // any file with any name.
    #[test]
    fn is_eval_runner_requires_main_and_the_runner_import() {
        let stdout = format!(
            r#"{{"Dir": "/repo/evals", "ImportPath": "example.test/evals", "Name": "main", "Deps": ["fmt", "{runner}"]}}
               {{"Dir": "/repo/cli", "ImportPath": "example.test/cli", "Name": "main", "Deps": ["fmt"]}}
               {{"Dir": "/repo/lib", "ImportPath": "example.test/lib", "Name": "lib", "Deps": ["{runner}"]}}"#,
            runner = EVAL_RUNNER_IMPORT
        );

        let packages = parse_go_list(stdout.as_bytes()).expect("stream should parse");

        assert!(packages[0].is_eval_runner(), "main + runner import");
        assert!(!packages[1].is_eval_runner(), "main without the import");
        assert!(
            !packages[2].is_eval_runner(),
            "library that imports the runner"
        );
    }

    /// Bytes `is_go_eval_runner_binary` looks for: an ELF header, the Go
    /// build-info blob at `build_info_at`, and the eval runner import path at
    /// `import_at` (when given). Built from parts so the test binary itself
    /// never holds a matching blob.
    fn fake_go_binary(len: usize, build_info_at: usize, import_at: Option<usize>) -> Vec<u8> {
        let mut bytes = vec![0u8; len];
        bytes[..4].copy_from_slice(b"\x7fELF");
        let header = build_info_at..build_info_at + GO_BUILDINFO_MAGIC.len();
        bytes[header.clone()].copy_from_slice(GO_BUILDINFO_MAGIC);
        bytes[header.end] = 8;
        bytes[header.end + 1] = 2;
        if let Some(at) = import_at {
            bytes[at..at + EVAL_RUNNER_IMPORT.len()].copy_from_slice(EVAL_RUNNER_IMPORT.as_bytes());
        }
        bytes
    }

    #[test]
    fn is_go_eval_runner_binary_finds_marks_past_the_first_chunk() {
        let dir = make_temp_dir("go-eval-runner-binary");
        let binary = dir.join("evals");
        // The import path straddles the 1 MiB chunk boundary.
        let chunk = 1 << 20;
        std::fs::write(
            &binary,
            fake_go_binary(chunk + 4096, chunk + 1024, Some(chunk - 10)),
        )
        .expect("binary written");

        assert!(is_go_eval_runner_binary(&binary));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn is_go_eval_runner_binary_rejects_other_executables() {
        let dir = make_temp_dir("not-go-eval-runner-binary");

        let script = dir.join("tsx");
        let mut bytes = b"#!/bin/sh\n# ".to_vec();
        bytes.extend_from_slice(&fake_go_binary(256, 64, Some(128)));
        std::fs::write(&script, bytes).expect("script written");
        assert!(
            !is_go_eval_runner_binary(&script),
            "not an executable format"
        );

        // A Go binary that never links the runner, e.g. a Go-built JS wrapper.
        let wrapper = dir.join("wrapper");
        std::fs::write(&wrapper, fake_go_binary(256, 64, None)).expect("wrapper written");
        assert!(!is_go_eval_runner_binary(&wrapper), "Go, but no evalrunner");

        // The magic alone, unaligned or without the header fields, is not
        // enough, even next to the import path.
        let mut bytes = b"\x7fELF".to_vec();
        bytes.resize(33, 0);
        bytes.extend_from_slice(GO_BUILDINFO_MAGIC);
        bytes.extend_from_slice(&[8, 2]);
        bytes.extend_from_slice(EVAL_RUNNER_IMPORT.as_bytes());
        let bare_magic = dir.join("bare-magic");
        std::fs::write(&bare_magic, bytes).expect("binary written");
        assert!(
            !is_go_eval_runner_binary(&bare_magic),
            "magic without a valid header"
        );

        assert!(!is_go_eval_runner_binary(&dir.join("missing")));
        #[cfg(unix)]
        assert!(
            !is_go_eval_runner_binary(Path::new("/bin/sh")),
            "a native non-Go binary"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }
}
