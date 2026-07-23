use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::Command;

const PREVIEW_ASSETS: &[(&str, &str)] = &[
    (
        "node_modules/react/umd/react.development.js",
        "views-preview-react.js",
    ),
    (
        "node_modules/react-dom/umd/react-dom.development.js",
        "views-preview-react-dom.js",
    ),
    (
        "node_modules/@tailwindcss/browser/dist/index.global.js",
        "views-preview-tailwindcss-browser.js",
    ),
];

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

fn copy_preview_assets() {
    let manifest_dir =
        PathBuf::from(env::var("CARGO_MANIFEST_DIR").expect("CARGO_MANIFEST_DIR is set by cargo"));
    let out_dir = PathBuf::from(env::var("OUT_DIR").expect("OUT_DIR is set by cargo"));

    println!("cargo:rerun-if-changed=package.json");
    println!("cargo:rerun-if-changed=pnpm-lock.yaml");
    for (source, output) in PREVIEW_ASSETS {
        let source_path = manifest_dir.join(source);
        println!("cargo:rerun-if-changed={}", source_path.display());
        if !source_path.is_file() {
            panic!(
                "missing preview asset {}; run `pnpm install --ignore-scripts` before building bt",
                source_path.display()
            );
        }
        let output_path = out_dir.join(output);
        fs::copy(&source_path, &output_path).unwrap_or_else(|err| {
            panic!(
                "failed to copy preview asset {} to {}: {err}",
                source_path.display(),
                output_path.display()
            )
        });
    }
}

fn main() {
    copy_preview_assets();

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
}
