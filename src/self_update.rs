use std::env;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use reqwest::Client;
use serde::Deserialize;

#[derive(Debug, Clone, Args)]
pub struct SelfArgs {
    #[command(subcommand)]
    pub command: SelfSubcommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum SelfSubcommand {
    /// Update bt in-place (installer-managed installs only)
    Update(UpdateArgs),
}

#[derive(Debug, Clone, Args)]
pub struct UpdateArgs {
    /// Check for updates without installing
    #[arg(long)]
    pub check: bool,

    /// Update channel (defaults to the build channel)
    #[arg(long, value_enum)]
    pub channel: Option<UpdateChannel>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum UpdateChannel {
    Stable,
    Canary,
}

impl UpdateChannel {
    fn installer_url(self) -> &'static str {
        match self {
            UpdateChannel::Stable => {
                "https://github.com/braintrustdata/bt/releases/latest/download/bt-installer.sh"
            }
            UpdateChannel::Canary => {
                "https://github.com/braintrustdata/bt/releases/download/canary/bt-installer.sh"
            }
        }
    }

    fn github_release_api_url(self) -> &'static str {
        match self {
            UpdateChannel::Stable => {
                "https://api.github.com/repos/braintrustdata/bt/releases/latest"
            }
            UpdateChannel::Canary => {
                "https://api.github.com/repos/braintrustdata/bt/releases/tags/canary"
            }
        }
    }

    fn name(self) -> &'static str {
        match self {
            UpdateChannel::Stable => "stable",
            UpdateChannel::Canary => "canary",
        }
    }
}

const BUILD_UPDATE_CHANNEL: Option<&str> = option_env!("BT_UPDATE_CHANNEL");

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
}

pub async fn run(args: SelfArgs) -> Result<()> {
    match args.command {
        SelfSubcommand::Update(args) => run_update(args).await,
    }
}

async fn run_update(args: UpdateArgs) -> Result<()> {
    ensure_installer_managed_install()?;
    let channel = args
        .channel
        .or_else(|| parse_update_channel(BUILD_UPDATE_CHANNEL))
        .unwrap_or(UpdateChannel::Stable);

    if args.check {
        check_for_update(channel).await?;
        return Ok(());
    }

    if channel == UpdateChannel::Stable {
        match fetch_release(channel).await {
            Ok(release) => {
                let current = env!("CARGO_PKG_VERSION");
                if stable_is_up_to_date(current, &release.tag_name) {
                    println!("{}", stable_check_message(current, &release.tag_name));
                    return Ok(());
                }
            }
            Err(err) => {
                eprintln!(
                    "warning: failed to pre-check stable version ({err}); continuing with update"
                );
            }
        }
    }

    run_installer(channel)?;
    Ok(())
}

fn ensure_installer_managed_install() -> Result<()> {
    let exe = env::current_exe().context("failed to resolve current executable path")?;

    let receipt_exists = receipt_path().as_ref().is_some_and(|path| path.exists());
    if is_installer_managed_install(&exe, receipt_exists, cargo_home_bin_path().as_deref()) {
        return Ok(());
    }

    anyhow::bail!(
        "self-update is only supported for installer-based installs.\ncurrent executable: {}\nif this was installed with Homebrew/apt/choco/etc, update with that package manager",
        exe.display()
    );
}

async fn check_for_update(channel: UpdateChannel) -> Result<()> {
    let release = fetch_release(channel).await?;
    let current = env!("CARGO_PKG_VERSION");

    match channel {
        UpdateChannel::Stable => {
            println!("{}", stable_check_message(current, &release.tag_name));
        }
        UpdateChannel::Canary => {
            println!("{}", canary_check_message(&release.tag_name));
        }
    }

    Ok(())
}

async fn fetch_release(channel: UpdateChannel) -> Result<GitHubRelease> {
    let client = Client::builder()
        .user_agent("bt-self-update")
        .build()
        .context("failed to initialize HTTP client")?;

    let mut request = client
        .get(channel.github_release_api_url())
        .header("Accept", "application/vnd.github+json");
    if let Ok(token) = env::var("GITHUB_TOKEN") {
        let token = token.trim();
        if !token.is_empty() {
            request = request.bearer_auth(token);
        }
    }
    let release = request
        .send()
        .await
        .context("failed to query GitHub releases")?;

    if !release.status().is_success() {
        let status = release.status();
        let body = release.text().await.unwrap_or_default();
        anyhow::bail!("failed to check for updates ({status}): {body}");
    }

    release
        .json()
        .await
        .context("failed to parse GitHub release response")
}

fn run_installer(channel: UpdateChannel) -> Result<()> {
    #[cfg(not(windows))]
    {
        let installer_url = channel.installer_url();
        println!("updating bt from {} channel...", channel.name());
        let cmd = format!("curl -fsSL '{installer_url}' | sh");
        let status = Command::new("sh")
            .arg("-c")
            .arg(cmd)
            .status()
            .context("failed to execute installer")?;

        if !status.success() {
            anyhow::bail!("installer exited with status {status}");
        }

        println!("update completed");
        Ok(())
    }

    #[cfg(windows)]
    {
        let installer_url = match channel {
            UpdateChannel::Stable => {
                "https://github.com/braintrustdata/bt/releases/latest/download/bt-installer.ps1"
            }
            UpdateChannel::Canary => {
                "https://github.com/braintrustdata/bt/releases/download/canary/bt-installer.ps1"
            }
        };
        let script = format!("irm {installer_url} | iex");
        let status = Command::new("powershell")
            .args([
                "-NoProfile",
                "-ExecutionPolicy",
                "Bypass",
                "-Command",
                &script,
            ])
            .status()
            .context("failed to execute PowerShell installer")?;
        if !status.success() {
            anyhow::bail!("installer exited with status {status}");
        }

        println!("update completed");
        return Ok(());
    }
}

fn receipt_path() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        env::var_os("APPDATA")
            .map(PathBuf::from)
            .map(|path| path.join("bt").join("bt-receipt.json"))
    }
    #[cfg(not(windows))]
    {
        if let Some(xdg) = env::var_os("XDG_CONFIG_HOME") {
            return Some(PathBuf::from(xdg).join("bt").join("bt-receipt.json"));
        }
        env::var_os("HOME")
            .map(PathBuf::from)
            .map(|path| path.join(".config").join("bt").join("bt-receipt.json"))
    }
}

fn cargo_home_bin_path() -> Option<PathBuf> {
    if let Some(cargo_home) = env::var_os("CARGO_HOME") {
        return Some(PathBuf::from(cargo_home).join("bin"));
    }

    #[cfg(windows)]
    {
        env::var_os("USERPROFILE")
            .map(PathBuf::from)
            .map(|path| path.join(".cargo").join("bin"))
    }
    #[cfg(not(windows))]
    {
        env::var_os("HOME")
            .map(PathBuf::from)
            .map(|path| path.join(".cargo").join("bin"))
    }
}

fn binary_name() -> &'static str {
    #[cfg(windows)]
    {
        "bt.exe"
    }
    #[cfg(not(windows))]
    {
        "bt"
    }
}

fn paths_equal(a: &Path, b: &Path) -> bool {
    let left = a.canonicalize().unwrap_or_else(|_| a.to_path_buf());
    let right = b.canonicalize().unwrap_or_else(|_| b.to_path_buf());
    left == right
}

fn is_installer_managed_install(
    exe: &Path,
    receipt_exists: bool,
    cargo_home_bin: Option<&Path>,
) -> bool {
    if receipt_exists {
        return true;
    }

    cargo_home_bin
        .map(|bin| paths_equal(exe, &bin.join(binary_name())))
        .unwrap_or(false)
}

fn stable_check_message(current: &str, release_tag: &str) -> String {
    if stable_is_up_to_date(current, release_tag) {
        return format!("bt {current} is up to date on the stable channel ({release_tag})");
    }
    format!("update available on stable channel: current={current}, latest={release_tag}")
}

fn stable_is_up_to_date(current: &str, release_tag: &str) -> bool {
    let latest = release_tag.trim_start_matches('v');
    latest == current
}

fn canary_check_message(release_tag: &str) -> String {
    format!(
        "latest canary release tag: {release_tag}\nrun `bt self update --channel canary` to install it"
    )
}

fn parse_update_channel(raw: Option<&str>) -> Option<UpdateChannel> {
    match raw {
        Some(channel) if channel.eq_ignore_ascii_case("stable") => Some(UpdateChannel::Stable),
        Some(channel) if channel.eq_ignore_ascii_case("canary") => Some(UpdateChannel::Canary),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn channel_urls_are_expected() {
        assert_eq!(
            UpdateChannel::Stable.installer_url(),
            "https://github.com/braintrustdata/bt/releases/latest/download/bt-installer.sh"
        );
        assert_eq!(
            UpdateChannel::Canary.installer_url(),
            "https://github.com/braintrustdata/bt/releases/download/canary/bt-installer.sh"
        );
        assert_eq!(
            UpdateChannel::Stable.github_release_api_url(),
            "https://api.github.com/repos/braintrustdata/bt/releases/latest"
        );
        assert_eq!(
            UpdateChannel::Canary.github_release_api_url(),
            "https://api.github.com/repos/braintrustdata/bt/releases/tags/canary"
        );
    }

    #[test]
    fn installer_detection_accepts_receipt() {
        let exe = Path::new("/tmp/not-in-cargo-home/bt");
        assert!(is_installer_managed_install(exe, true, None));
    }

    #[test]
    fn installer_detection_accepts_cargo_home_bin_path() {
        let cargo_home_bin = Path::new("/tmp/cargo/bin");
        let exe = cargo_home_bin.join(binary_name());
        assert!(is_installer_managed_install(
            &exe,
            false,
            Some(cargo_home_bin)
        ));
    }

    #[test]
    fn installer_detection_rejects_non_installer_location() {
        let cargo_home_bin = Path::new("/tmp/cargo/bin");
        let exe = Path::new("/usr/local/bin/bt");
        assert!(!is_installer_managed_install(
            exe,
            false,
            Some(cargo_home_bin)
        ));
    }

    #[test]
    fn stable_check_message_reports_up_to_date() {
        let msg = stable_check_message("0.1.0", "v0.1.0");
        assert!(msg.contains("up to date"));
        assert!(msg.contains("v0.1.0"));
    }

    #[test]
    fn stable_check_message_reports_update_available() {
        let msg = stable_check_message("0.1.0", "v0.2.0");
        assert!(msg.contains("update available"));
        assert!(msg.contains("current=0.1.0"));
        assert!(msg.contains("latest=v0.2.0"));
    }

    #[test]
    fn canary_check_message_contains_guidance() {
        let msg = canary_check_message("canary-deadbeef");
        assert!(msg.contains("canary-deadbeef"));
        assert!(msg.contains("bt self update --channel canary"));
    }

    #[test]
    fn parse_update_channel_handles_expected_values() {
        assert_eq!(
            parse_update_channel(Some("stable")),
            Some(UpdateChannel::Stable)
        );
        assert_eq!(
            parse_update_channel(Some("canary")),
            Some(UpdateChannel::Canary)
        );
        assert_eq!(
            parse_update_channel(Some("CANARY")),
            Some(UpdateChannel::Canary)
        );
    }

    #[test]
    fn parse_update_channel_rejects_unknown_values() {
        assert_eq!(parse_update_channel(Some("nightly")), None);
        assert_eq!(parse_update_channel(None), None);
    }
}
