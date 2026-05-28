use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn non_empty_env(name: &str) -> Option<String> {
    env::var(name)
        .ok()
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn git_short_sha() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--short=12", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if sha.is_empty() {
        return None;
    }
    Some(sha)
}

fn git_head_path() -> Option<String> {
    let output = Command::new("git")
        .args(["rev-parse", "--git-path", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let path = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if path.is_empty() {
        return None;
    }
    Some(path)
}

fn compute_default_version() -> String {
    let pkg_version = env::var("CARGO_PKG_VERSION").unwrap_or_else(|_| "0.0.0".to_string());
    match git_short_sha() {
        Some(sha) => format!("{pkg_version}-canary.{sha}"),
        None => format!("{pkg_version}-canary.dev"),
    }
}

fn main() {
    let version = non_empty_env("BT_VERSION_STRING").unwrap_or_else(compute_default_version);
    println!("cargo:rustc-env=BT_VERSION_STRING={version}");

    let channel = non_empty_env("BT_UPDATE_CHANNEL").unwrap_or_else(|| {
        if version.contains("-canary") {
            "canary".to_string()
        } else {
            "stable".to_string()
        }
    });
    println!("cargo:rustc-env=BT_UPDATE_CHANNEL={channel}");

    println!("cargo:rerun-if-env-changed=BT_VERSION_STRING");
    println!("cargo:rerun-if-env-changed=BT_UPDATE_CHANNEL");
    if let Some(path) = git_head_path() {
        println!("cargo:rerun-if-changed={path}");
    } else {
        println!("cargo:rerun-if-changed=.git/HEAD");
    }

    stage_spark_assets();
}

fn stage_spark_assets() {
    println!("cargo:rerun-if-env-changed=BT_SPARK_DIR");
    println!("cargo:rerun-if-env-changed=BT_SPARK_EMBED");

    let out_dir = PathBuf::from(env::var_os("OUT_DIR").expect("OUT_DIR is set by cargo"));
    let spark_out = out_dir.join("spark");
    fs::create_dir_all(&spark_out).expect("create OUT_DIR/spark");

    let cli_dest = spark_out.join("cli.mjs");
    let harness_dest = spark_out.join("harness.tgz");
    let hash_dest = spark_out.join("asset_hash");
    let embedded_marker = spark_out.join("embedded");

    let embed_enabled = non_empty_env("BT_SPARK_EMBED")
        .map(|value| !matches!(value.as_str(), "0" | "false" | "no" | "off"))
        .unwrap_or(true);

    if !embed_enabled {
        write_placeholder_assets(&cli_dest, &harness_dest, &hash_dest, &embedded_marker);
        println!("cargo:warning=BT_SPARK_EMBED disabled; `bt spark` will be a stub");
        return;
    }

    let manifest_dir = PathBuf::from(
        env::var_os("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by cargo"),
    );
    let spark_dir = non_empty_env("BT_SPARK_DIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| manifest_dir.join("..").join("spark"));

    let cli_src = spark_dir.join("packages/spark/dist/cli.mjs");
    let harness_src = spark_dir.join("dist-sea-build/harness.tgz");

    if !cli_src.exists() || !harness_src.exists() {
        write_placeholder_assets(&cli_dest, &harness_dest, &hash_dest, &embedded_marker);
        println!(
            "cargo:warning=spark assets not found under {}; build spark with `pnpm build:sea` to embed (looked for {} and {})",
            spark_dir.display(),
            cli_src.display(),
            harness_src.display(),
        );
        return;
    }

    println!("cargo:rerun-if-changed={}", cli_src.display());
    println!("cargo:rerun-if-changed={}", harness_src.display());

    fs::copy(&cli_src, &cli_dest).unwrap_or_else(|err| panic!("copy spark cli.mjs: {err}"));
    fs::copy(&harness_src, &harness_dest)
        .unwrap_or_else(|err| panic!("copy spark harness.tgz: {err}"));

    let hash = hash_files(&[cli_dest.as_path(), harness_dest.as_path()]);
    fs::write(&hash_dest, &hash).expect("write asset_hash");
    fs::write(&embedded_marker, b"1").expect("write embedded marker");
}

fn write_placeholder_assets(cli: &Path, harness: &Path, hash: &Path, marker: &Path) {
    fs::write(cli, b"").expect("write empty cli.mjs placeholder");
    fs::write(harness, b"").expect("write empty harness.tgz placeholder");
    fs::write(hash, b"unembedded").expect("write asset_hash placeholder");
    if marker.exists() {
        fs::remove_file(marker).expect("remove stale embedded marker");
    }
}

fn hash_files(paths: &[&Path]) -> String {
    use std::io::Read;

    // Tiny FNV-1a 64-bit, ample for cache-key purposes (no crypto needs here).
    let mut hash: u64 = 0xcbf29ce484222325;
    for path in paths {
        let mut file = fs::File::open(path).expect("open asset for hashing");
        let mut buf = [0u8; 8192];
        loop {
            let n = file.read(&mut buf).expect("read asset for hashing");
            if n == 0 {
                break;
            }
            for byte in &buf[..n] {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x100000001b3);
            }
        }
    }
    format!("{hash:016x}")
}
