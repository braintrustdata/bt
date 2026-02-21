use std::env;
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
}
