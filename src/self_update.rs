use std::env;
use std::io;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};

#[cfg(windows)]
use std::fs;
#[cfg(windows)]
use std::io::Read as _;
#[cfg(windows)]
use std::os::windows::process::CommandExt;

use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueEnum};
use reqwest::Client;
use serde::Deserialize;
#[cfg(windows)]
use sha2::{Digest, Sha256};

use crate::args::BaseArgs;
use crate::http::DEFAULT_HTTP_TIMEOUT;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt update
  bt update --check
  bt update --channel canary
")]
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

    #[cfg(windows)]
    #[arg(long, hide = true, requires = "windows_update_parent_pid")]
    pub windows_update_worker: bool,

    #[cfg(windows)]
    #[arg(long, hide = true, requires = "windows_update_worker")]
    pub windows_update_parent_pid: Option<u32>,
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, ValueEnum)]
pub enum UpdateChannel {
    Stable,
    Canary,
}

impl UpdateChannel {
    #[cfg(not(windows))]
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
    #[serde(default)]
    target_commitish: Option<String>,
}

#[cfg(windows)]
#[derive(Debug)]
pub(crate) struct UpdateWorkerError {
    message: String,
    exit_code: Option<i32>,
}

#[cfg(windows)]
impl UpdateWorkerError {
    pub(crate) fn exit_code(&self) -> Option<i32> {
        self.exit_code
    }
}

#[cfg(windows)]
impl std::fmt::Display for UpdateWorkerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

#[cfg(windows)]
impl std::error::Error for UpdateWorkerError {}

pub async fn run(base: BaseArgs, args: SelfArgs) -> Result<()> {
    match args.command {
        SelfSubcommand::Update(args) => run_update(&base, args).await,
    }
}

async fn run_update(base: &BaseArgs, args: UpdateArgs) -> Result<()> {
    let channel = args
        .channel
        .unwrap_or_else(|| inferred_update_channel(BUILD_UPDATE_CHANNEL));

    #[cfg(windows)]
    if args.windows_update_worker {
        let parent_pid = args
            .windows_update_parent_pid
            .context("Windows update worker is missing its parent process ID")?;
        return run_windows_update_worker(base, channel, parent_pid).await;
    }

    ensure_installer_managed_install()?;

    if args.check {
        check_for_update(base, channel).await?;
        return Ok(());
    }

    if channel == UpdateChannel::Stable {
        match fetch_release(base, channel).await {
            Ok(release) => {
                if stable_is_up_to_date(env!("CARGO_PKG_VERSION"), &release.tag_name) {
                    print_check(base, channel, &release)?;
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

    #[cfg(windows)]
    {
        launch_windows_update_worker(base, channel)
    }

    #[cfg(not(windows))]
    {
        run_installer(base, channel).await?;
        print_update_completed(base, channel)
    }
}

fn print_update_completed(base: &BaseArgs, channel: UpdateChannel) -> Result<()> {
    if base.json {
        let payload = serde_json::json!({
            "channel": channel.name(),
            "status": "completed",
        });
        println!("{}", serde_json::to_string(&payload)?);
    }
    Ok(())
}

fn ensure_installer_managed_install() -> Result<()> {
    let exe = env::current_exe().context("failed to resolve current executable path")?;

    let receipt_exists = receipt_path().as_ref().is_some_and(|path| path.exists());
    let installer_bin_paths = installer_bin_paths();
    if is_installer_managed_install(&exe, receipt_exists, &installer_bin_paths) {
        return Ok(());
    }

    anyhow::bail!(
        "update is only supported for official installer installs.\ncurrent executable: {}\nif this was installed with npm, update with npm; otherwise reinstall with the official installer",
        exe.display()
    );
}

async fn check_for_update(base: &BaseArgs, channel: UpdateChannel) -> Result<()> {
    let release = fetch_release(base, channel).await?;
    print_check(base, channel, &release)
}

fn print_check(base: &BaseArgs, channel: UpdateChannel, release: &GitHubRelease) -> Result<()> {
    let (current, latest, up_to_date, message) = match channel {
        UpdateChannel::Stable => {
            let current = env!("CARGO_PKG_VERSION").to_string();
            let latest = release.tag_name.clone();
            let up_to_date = stable_is_up_to_date(&current, &latest);
            let message = stable_check_message(&current, &latest);
            (current, latest, up_to_date, message)
        }
        UpdateChannel::Canary => {
            // `current` is built by build.rs as `{CARGO_PKG_VERSION}-canary.{short_sha}`.
            // Construct `latest` in the same shape so the two are comparable.
            // The canary tag itself is always literally "canary"; the meaningful
            // identifier is the commit it points at (target_commitish).
            let current = crate::CLI_VERSION.to_string();
            let latest = format_canary_version(release.target_commitish.as_deref());
            let up_to_date = current == latest;
            let message = canary_check_message(&latest);
            (current, latest, up_to_date, message)
        }
    };

    if base.json {
        let payload = serde_json::json!({
            "channel": channel.name(),
            "current": current,
            "latest": latest,
            "up_to_date": up_to_date,
        });
        println!("{}", serde_json::to_string(&payload)?);
    } else {
        println!("{message}");
    }
    Ok(())
}

async fn fetch_release(_base: &BaseArgs, channel: UpdateChannel) -> Result<GitHubRelease> {
    let client = crate::http::build_http_client_from_builder(
        Client::builder().timeout(DEFAULT_HTTP_TIMEOUT),
    )
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

#[cfg(windows)]
fn launch_windows_update_worker(base: &BaseArgs, channel: UpdateChannel) -> Result<()> {
    let exe = env::current_exe().context("failed to resolve current executable path")?;
    let original_digest = file_sha256(&exe)
        .with_context(|| format!("failed to hash current executable {}", exe.display()))?;
    let parent = exe
        .parent()
        .context("current executable has no parent directory")?;
    let parent_pid = std::process::id();
    let worker = parent.join(format!("bt-update-{parent_pid}.exe"));

    cleanup_stale_windows_update_workers(parent, &worker);

    if worker.exists() {
        fs::remove_file(&worker).with_context(|| {
            format!(
                "failed to remove stale Windows update helper {}",
                worker.display()
            )
        })?;
    }
    // Renaming frees the install path while this process waits for the helper.
    fs::rename(&exe, &worker).with_context(|| {
        format!(
            "failed to move bt to Windows update helper {}",
            worker.display()
        )
    })?;
    // Preserve a usable bt.exe if the update is interrupted.
    if let Err(copy_err) = fs::copy(&worker, &exe) {
        if let Err(restore_err) = fs::rename(&worker, &exe) {
            anyhow::bail!(
                "failed to create update fallback at {}: {copy_err}; also failed to restore the original executable: {restore_err}",
                exe.display()
            );
        }
        return Err(copy_err)
            .with_context(|| format!("failed to create update fallback at {}", exe.display()));
    }

    let parent_pid_arg = parent_pid.to_string();
    let mut command = Command::new(&worker);
    command.args([
        "update",
        "--channel",
        channel.name(),
        "--windows-update-worker",
        "--windows-update-parent-pid",
        &parent_pid_arg,
    ]);
    // Always capture the worker's JSON response so only this parent process
    // renders the final success or error to the user's stdout.
    command.arg("--json");
    if base.quiet {
        command.arg("--quiet");
    }
    command.stdout(Stdio::piped()).stderr(Stdio::inherit());

    let output = match command.output() {
        Ok(output) => output,
        Err(spawn_err) => {
            if let Err(err) = schedule_windows_worker_cleanup(&worker) {
                eprintln!(
                    "warning: failed to schedule cleanup of Windows update helper {}: {err}",
                    worker.display()
                );
            }
            return Err(spawn_err).with_context(|| {
                format!(
                    "failed to launch Windows update helper {}",
                    worker.display()
                )
            });
        }
    };

    if !output.status.success() {
        restore_windows_executable(&worker, &exe).with_context(|| {
            format!(
                "Windows update helper exited with status {}; failed to restore the original executable from {}",
                output.status,
                worker.display()
            )
        })?;
        schedule_windows_worker_cleanup_with_warning(&worker);
        return Err(anyhow::Error::new(UpdateWorkerError {
            message: windows_worker_error_message(&output.stdout, &output.status),
            exit_code: output.status.code(),
        }));
    }

    if !exe.exists() {
        restore_windows_executable(&worker, &exe).with_context(|| {
            format!(
                "Windows update did not install an executable; failed to restore the original executable from {}",
                worker.display()
            )
        })?;
        schedule_windows_worker_cleanup_with_warning(&worker);
        anyhow::bail!(
            "Windows update did not install an executable; restored the original executable"
        );
    }

    let installed_digest = match file_sha256(&exe)
        .with_context(|| format!("failed to verify updated executable {}", exe.display()))
    {
        Ok(digest) => digest,
        Err(err) => {
            schedule_windows_worker_cleanup_with_warning(&worker);
            return Err(err);
        }
    };
    if installed_digest == original_digest {
        schedule_windows_worker_cleanup_with_warning(&worker);
        anyhow::bail!(
            "Windows installer completed without replacing {}; the original executable remains installed",
            exe.display()
        );
    }

    schedule_windows_worker_cleanup_with_warning(&worker);
    print_update_completed(base, channel)
}

#[cfg(windows)]
fn windows_worker_error_message(stdout: &[u8], status: &ExitStatus) -> String {
    serde_json::from_slice::<serde_json::Value>(stdout)
        .ok()
        .and_then(|value| {
            value
                .pointer("/error/message")
                .and_then(|message| message.as_str())
                .map(str::to_string)
        })
        .unwrap_or_else(|| format!("Windows update helper exited with status {status}"))
}

#[cfg(windows)]
fn file_sha256(path: &Path) -> Result<[u8; 32]> {
    let mut file = fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buffer = [0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize().into())
}

#[cfg(windows)]
fn cleanup_stale_windows_update_workers(parent: &Path, active_worker: &Path) {
    let entries = match fs::read_dir(parent) {
        Ok(entries) => entries,
        Err(err) => {
            eprintln!(
                "warning: failed to scan for stale Windows update helpers in {}: {err}",
                parent.display()
            );
            return;
        }
    };

    for entry in entries.flatten() {
        let path = entry.path();
        if path == active_worker || !is_windows_update_worker_path(&path) {
            continue;
        }
        if let Err(err) = fs::remove_file(&path) {
            if err.kind() != std::io::ErrorKind::NotFound {
                eprintln!(
                    "warning: failed to remove stale Windows update helper {}: {err}",
                    path.display()
                );
            }
        }
    }
}

#[cfg(windows)]
fn is_windows_update_worker_path(path: &Path) -> bool {
    let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
        return false;
    };
    let Some(pid) = name
        .strip_prefix("bt-update-")
        .and_then(|name| name.strip_suffix(".exe"))
    else {
        return false;
    };
    !pid.is_empty() && pid.bytes().all(|byte| byte.is_ascii_digit())
}

#[cfg(windows)]
fn restore_windows_executable(worker: &Path, exe: &Path) -> Result<()> {
    fs::copy(worker, exe).with_context(|| {
        format!(
            "failed to copy the original executable to {}",
            exe.display()
        )
    })?;
    Ok(())
}

#[cfg(windows)]
fn schedule_windows_worker_cleanup_with_warning(worker: &Path) {
    if let Err(err) = schedule_windows_worker_cleanup(worker) {
        eprintln!(
            "warning: failed to schedule cleanup of Windows update helper {}: {err}",
            worker.display()
        );
    }
}

#[cfg(windows)]
async fn run_windows_update_worker(
    base: &BaseArgs,
    channel: UpdateChannel,
    parent_pid: u32,
) -> Result<()> {
    let worker = env::current_exe().context("failed to resolve Windows update helper path")?;
    let expected_name = format!("bt-update-{parent_pid}.exe");
    let is_update_helper = worker
        .file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.eq_ignore_ascii_case(&expected_name));
    if !is_update_helper {
        anyhow::bail!("refusing to run the Windows update worker from an unexpected path");
    }

    run_installer(base, channel).await?;
    print_update_completed(base, channel)
}

#[cfg(windows)]
fn schedule_windows_worker_cleanup(worker: &Path) -> Result<()> {
    const CREATE_NO_WINDOW: u32 = 0x0800_0000;
    let worker_pid = std::process::id();
    let escaped_path = worker.to_string_lossy().replace('\'', "''");
    let cleanup_script = format!(
        "Wait-Process -Id {worker_pid} -ErrorAction SilentlyContinue; Remove-Item -LiteralPath '{escaped_path}' -Force -ErrorAction SilentlyContinue"
    );
    Command::new("powershell")
        .args([
            "-NoProfile",
            "-ExecutionPolicy",
            "Bypass",
            "-Command",
            &cleanup_script,
        ])
        .creation_flags(CREATE_NO_WINDOW)
        .spawn()
        .context("failed to launch Windows update-helper cleanup")?;
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InstallerStdout {
    Inherit,
    Stderr,
    Suppress,
}

fn installer_stdout(base: &BaseArgs) -> InstallerStdout {
    if base.quiet {
        InstallerStdout::Suppress
    } else if base.json {
        InstallerStdout::Stderr
    } else {
        InstallerStdout::Inherit
    }
}

async fn run_installer(base: &BaseArgs, channel: UpdateChannel) -> Result<()> {
    let status_line = |msg: &str| {
        if !base.quiet {
            eprintln!("{msg}");
        }
    };

    #[cfg(not(windows))]
    {
        let installer_url = channel.installer_url();
        status_line(&format!("updating bt from {} channel...", channel.name()));
        let cmd = format!("curl -fsSL '{installer_url}' | sh");
        let mut command = Command::new("sh");
        command.arg("-c").arg(cmd);
        let status = run_installer_command(&mut command, installer_stdout(base))
            .context("failed to execute installer")?;

        if !status.success() {
            anyhow::bail!("installer exited with status {status}");
        }

        status_line("update completed");
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
        status_line(&format!("updating bt from {} channel...", channel.name()));

        // Avoid the `irm ... | iex` pattern flagged by Windows security tools.
        let client = crate::http::build_http_client_from_builder(
            Client::builder().timeout(DEFAULT_HTTP_TIMEOUT),
        )
        .context("failed to initialize installer HTTP client")?;
        let response = client
            .get(installer_url)
            .send()
            .await
            .context("failed to download PowerShell installer")?
            .error_for_status()
            .context("failed to download PowerShell installer")?;
        let script = response
            .bytes()
            .await
            .context("failed to read PowerShell installer")?;
        let temp_dir = tempfile::tempdir().context("failed to create installer directory")?;
        let script_path = temp_dir.path().join("bt-installer.ps1");
        fs::write(&script_path, script).context("failed to save PowerShell installer")?;

        let mut command = Command::new("powershell");
        command.args(["-NoProfile", "-ExecutionPolicy", "Bypass", "-File"]);
        command.arg(&script_path);
        let status = run_installer_command(&mut command, installer_stdout(base))
            .context("failed to execute PowerShell installer")?;
        if !status.success() {
            anyhow::bail!("installer exited with status {status}");
        }

        status_line("update completed");
        Ok(())
    }
}

fn run_installer_command(command: &mut Command, stdout: InstallerStdout) -> Result<ExitStatus> {
    match stdout {
        InstallerStdout::Inherit => command.status().context("failed to start installer"),
        InstallerStdout::Suppress => command
            .stdout(Stdio::null())
            .status()
            .context("failed to start installer"),
        InstallerStdout::Stderr => {
            command.stdout(Stdio::piped());
            let mut child = command.spawn().context("failed to start installer")?;
            let mut stdout = child
                .stdout
                .take()
                .context("failed to capture installer stdout")?;
            let mut stderr = io::stderr().lock();
            io::copy(&mut stdout, &mut stderr).context("failed to relay installer output")?;
            child.wait().context("failed to wait for installer")
        }
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

    user_home_dir().map(|path| path.join(".cargo").join("bin"))
}

fn user_home_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        env::var_os("USERPROFILE").map(PathBuf::from)
    }
    #[cfg(not(windows))]
    {
        env::var_os("HOME").map(PathBuf::from)
    }
}

fn installer_bin_paths() -> Vec<PathBuf> {
    let mut paths = Vec::new();

    if let Some(path) = cargo_home_bin_path() {
        paths.push(path);
    }
    // These environment lookups match cargo-dist installer path conventions.
    // They are internal install-detection plumbing, not user-facing runtime config.
    if let Some(path) = env::var_os("XDG_BIN_HOME") {
        paths.push(PathBuf::from(path));
    }
    if let Some(path) = env::var_os("XDG_DATA_HOME") {
        paths.push(PathBuf::from(path).join("..").join("bin"));
    }
    if let Some(path) = user_home_dir() {
        paths.push(path.join(".local").join("bin"));
    }

    paths
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
    installer_bin_paths: &[PathBuf],
) -> bool {
    if receipt_exists {
        return true;
    }

    installer_bin_paths
        .iter()
        .any(|bin| paths_equal(exe, &bin.join(binary_name())))
}

fn stable_check_message(current: &str, release_tag: &str) -> String {
    if stable_is_up_to_date(current, release_tag) {
        return format!("bt {current} is up to date on the stable channel ({release_tag})");
    }
    format!("update available on stable channel: current={current}, latest={release_tag}")
}

/// Format a canary version string to match the shape that `build.rs` bakes into
/// `CLI_VERSION`: `{CARGO_PKG_VERSION}-canary.{short_sha}`. Falls back to a
/// `dev`-style suffix when `target_commitish` is missing so an unparseable
/// release never accidentally compares equal to a local canary build.
fn format_canary_version(target_commitish: Option<&str>) -> String {
    let short_sha = target_commitish
        .map(|sha| &sha[..sha.len().min(12)])
        .unwrap_or("unknown");
    format!("{}-canary.{}", env!("CARGO_PKG_VERSION"), short_sha)
}

fn stable_is_up_to_date(current: &str, release_tag: &str) -> bool {
    let latest = release_tag.trim_start_matches('v');
    latest == current
}

fn canary_check_message(latest: &str) -> String {
    format!("latest canary release: {latest}\nrun `bt update --channel canary` to install it")
}

fn parse_update_channel(raw: Option<&str>) -> Option<UpdateChannel> {
    match raw {
        Some(channel) if channel.eq_ignore_ascii_case("stable") => Some(UpdateChannel::Stable),
        Some(channel) if channel.eq_ignore_ascii_case("canary") => Some(UpdateChannel::Canary),
        _ => None,
    }
}

fn inferred_update_channel(raw: Option<&str>) -> UpdateChannel {
    parse_update_channel(raw).unwrap_or(UpdateChannel::Canary)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn channel_urls_are_expected() {
        #[cfg(not(windows))]
        {
            assert_eq!(
                UpdateChannel::Stable.installer_url(),
                "https://github.com/braintrustdata/bt/releases/latest/download/bt-installer.sh"
            );
            assert_eq!(
                UpdateChannel::Canary.installer_url(),
                "https://github.com/braintrustdata/bt/releases/download/canary/bt-installer.sh"
            );
        }
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
        assert!(is_installer_managed_install(exe, true, &[]));
    }

    #[test]
    fn installer_detection_accepts_cargo_home_bin_path() {
        let cargo_home_bin = Path::new("/tmp/cargo/bin");
        let exe = cargo_home_bin.join(binary_name());
        assert!(is_installer_managed_install(
            &exe,
            false,
            &[cargo_home_bin.to_path_buf()]
        ));
    }

    #[test]
    fn installer_detection_accepts_local_bin_path() {
        let local_bin = Path::new("/tmp/home/.local/bin");
        let exe = local_bin.join(binary_name());
        assert!(is_installer_managed_install(
            &exe,
            false,
            &[local_bin.to_path_buf()]
        ));
    }

    #[test]
    fn installer_detection_rejects_non_installer_location() {
        let cargo_home_bin = Path::new("/tmp/cargo/bin");
        let local_bin = Path::new("/tmp/home/.local/bin");
        let exe = Path::new("/usr/local/bin/bt");
        assert!(!is_installer_managed_install(
            exe,
            false,
            &[cargo_home_bin.to_path_buf(), local_bin.to_path_buf()]
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
    fn format_canary_version_matches_cli_version_shape() {
        let pkg = env!("CARGO_PKG_VERSION");
        let formatted = format_canary_version(Some("abc123def456789012345678901234567890aaaa"));
        assert_eq!(formatted, format!("{pkg}-canary.abc123def456"));
    }

    #[test]
    fn format_canary_version_falls_back_when_commitish_missing() {
        let pkg = env!("CARGO_PKG_VERSION");
        assert_eq!(format_canary_version(None), format!("{pkg}-canary.unknown"));
    }

    #[test]
    fn canary_check_message_contains_guidance() {
        let msg = canary_check_message("0.10.0-canary.abc123def456");
        assert!(msg.contains("0.10.0-canary.abc123def456"));
        assert!(msg.contains("bt update --channel canary"));
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

    #[test]
    fn inferred_update_channel_defaults_to_canary() {
        assert_eq!(inferred_update_channel(None), UpdateChannel::Canary);
        assert_eq!(
            inferred_update_channel(Some("nightly")),
            UpdateChannel::Canary
        );
    }

    #[test]
    fn inferred_update_channel_accepts_stable_and_canary() {
        assert_eq!(
            inferred_update_channel(Some("stable")),
            UpdateChannel::Stable
        );
        assert_eq!(
            inferred_update_channel(Some("canary")),
            UpdateChannel::Canary
        );
    }

    #[test]
    fn installer_stdout_respects_json_and_quiet() {
        let mut base = BaseArgs::default();
        assert_eq!(installer_stdout(&base), InstallerStdout::Inherit);

        base.json = true;
        assert_eq!(installer_stdout(&base), InstallerStdout::Stderr);

        base.quiet = true;
        assert_eq!(installer_stdout(&base), InstallerStdout::Suppress);
    }
}
