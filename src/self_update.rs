use std::env;
use std::fs::{self, File};
use std::io;
#[cfg(unix)]
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result};
use clap::{Args, Subcommand, ValueEnum};
#[cfg(not(windows))]
use flate2::read::GzDecoder;
use reqwest::{Client, RequestBuilder, Url};
use serde::de::DeserializeOwned;
use serde::Deserialize;
#[cfg(not(windows))]
use tar::Archive;
use zip::ZipArchive;

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

    /// Install from a PR reference (run URL, PR URL, #PR, or PR number)
    #[arg(long, value_name = "REF", conflicts_with_all = ["check", "channel"])]
    pub pr: Option<String>,
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
const GITHUB_API_ACCEPT: &str = "application/vnd.github+json";
const DEFAULT_GITHUB_OWNER: &str = "braintrustdata";
const DEFAULT_GITHUB_REPO: &str = "bt";
const RELEASE_CANARY_WORKFLOW_FILE: &str = "release-canary.yml";

#[derive(Debug, Deserialize)]
struct GitHubRelease {
    tag_name: String,
}

#[derive(Debug, Deserialize)]
struct GitHubPullRequest {
    head: GitHubPullRequestHead,
}

#[derive(Debug, Deserialize)]
struct GitHubPullRequestHead {
    sha: String,
}

#[derive(Debug, Deserialize)]
struct GitHubWorkflowRunsResponse {
    workflow_runs: Vec<GitHubWorkflowRun>,
}

#[derive(Debug, Deserialize)]
struct GitHubWorkflowRun {
    id: u64,
    html_url: String,
    head_sha: String,
    conclusion: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GitHubWorkflowArtifactsResponse {
    artifacts: Vec<GitHubArtifact>,
}

#[derive(Debug, Clone, Deserialize)]
struct GitHubArtifact {
    name: String,
    expired: bool,
    archive_download_url: String,
}

#[derive(Debug, Clone, Eq, PartialEq)]
struct GitHubRepo {
    owner: String,
    name: String,
}

impl GitHubRepo {
    fn new(owner: impl Into<String>, name: impl Into<String>) -> Self {
        Self {
            owner: owner.into(),
            name: name.into(),
        }
    }

    fn api_base_url(&self) -> String {
        format!("https://api.github.com/repos/{}/{}", self.owner, self.name)
    }

    fn display_name(&self) -> String {
        format!("{}/{}", self.owner, self.name)
    }
}

#[derive(Debug, Clone, Eq, PartialEq)]
enum PrSpecifier {
    PullRequest { repo: GitHubRepo, number: u64 },
    WorkflowRun { repo: GitHubRepo, run_id: u64 },
}

#[derive(Debug)]
struct ResolvedWorkflowRun {
    repo: GitHubRepo,
    run_id: u64,
    run_url: String,
}

pub async fn run(args: SelfArgs) -> Result<()> {
    match args.command {
        SelfSubcommand::Update(args) => run_update(args).await,
    }
}

async fn run_update(args: UpdateArgs) -> Result<()> {
    ensure_installer_managed_install()?;

    if let Some(pr_ref) = args.pr.as_deref() {
        install_from_pr_ref(pr_ref).await?;
        return Ok(());
    }

    let channel = args
        .channel
        .unwrap_or_else(|| inferred_update_channel(BUILD_UPDATE_CHANNEL));

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

async fn install_from_pr_ref(pr_ref: &str) -> Result<()> {
    #[cfg(windows)]
    {
        let _ = pr_ref;
        anyhow::bail!(
            "`bt self update --pr` is not supported on Windows yet. Use `gh run download` for the PR run artifacts."
        );
    }

    #[cfg(not(windows))]
    {
        let specifier = parse_pr_specifier(pr_ref)?;
        let target = host_target_triple().ok_or_else(|| {
            anyhow::anyhow!(
                "unsupported host target for PR self-update: os={} arch={}",
                env::consts::OS,
                env::consts::ARCH
            )
        })?;

        let client = github_client()?;
        let run = resolve_workflow_run_from_pr_specifier(&client, specifier).await?;
        let artifact = fetch_platform_artifact(&client, &run.repo, run.run_id, target).await?;

        println!(
            "installing bt from {} (run {}): {}",
            run.repo.display_name(),
            run.run_id,
            run.run_url
        );

        let temp_dir = create_temp_dir("bt-self-update-pr")?;
        let artifact_zip_path = temp_dir.join("run-artifact.zip");
        let package_path = temp_dir.join(package_archive_name_for_target(target));
        let new_binary_path = temp_dir.join(binary_name());

        let artifact_bytes = github_get_bytes(
            &client,
            &artifact.archive_download_url,
            "failed to download workflow artifact archive",
        )
        .await?;
        fs::write(&artifact_zip_path, artifact_bytes)
            .with_context(|| format!("failed to write {}", artifact_zip_path.display()))?;

        extract_named_file_from_zip(
            &artifact_zip_path,
            &package_archive_name_for_target(target),
            &package_path,
        )?;

        extract_binary_from_package_archive(&package_path, &new_binary_path)?;
        replace_current_binary(&new_binary_path)?;

        let _ = fs::remove_dir_all(&temp_dir);
        println!("update completed");
        Ok(())
    }
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
    let client = github_client()?;
    github_get_json(
        &client,
        channel.github_release_api_url(),
        "failed to query GitHub releases",
    )
    .await
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

fn github_client() -> Result<Client> {
    Client::builder()
        .user_agent("bt-self-update")
        .build()
        .context("failed to initialize HTTP client")
}

fn github_api_request(client: &Client, url: &str) -> RequestBuilder {
    let mut request = client.get(url).header("Accept", GITHUB_API_ACCEPT);
    if let Ok(token) = env::var("GITHUB_TOKEN") {
        let token = token.trim();
        if !token.is_empty() {
            request = request.bearer_auth(token);
        }
    }
    request
}

fn github_download_request(client: &Client, url: &str) -> RequestBuilder {
    let mut request = client.get(url).header("Accept", "application/octet-stream");
    if let Ok(token) = env::var("GITHUB_TOKEN") {
        let token = token.trim();
        if !token.is_empty() {
            request = request.bearer_auth(token);
        }
    }
    request
}

async fn github_get_json<T: DeserializeOwned>(
    client: &Client,
    url: &str,
    context: &str,
) -> Result<T> {
    let response = github_api_request(client, url)
        .send()
        .await
        .with_context(|| context.to_string())?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("GitHub API request failed ({status}): {body}");
    }

    response
        .json::<T>()
        .await
        .context("failed to parse GitHub API response")
}

async fn github_get_bytes(client: &Client, url: &str, context: &str) -> Result<Vec<u8>> {
    let response = github_download_request(client, url)
        .send()
        .await
        .with_context(|| context.to_string())?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        anyhow::bail!("GitHub download failed ({status}): {body}");
    }

    response
        .bytes()
        .await
        .context("failed to read GitHub response bytes")
        .map(|bytes| bytes.to_vec())
}

fn parse_pr_specifier(raw: &str) -> Result<PrSpecifier> {
    let value = raw.trim();
    if value.is_empty() {
        anyhow::bail!("--pr cannot be empty");
    }

    if let Some(specifier) = parse_pr_specifier_from_url(value)? {
        return Ok(specifier);
    }

    if let Some(stripped) = value.strip_prefix('#') {
        let number = stripped
            .parse::<u64>()
            .with_context(|| format!("invalid PR number in --pr value: {value}"))?;
        return Ok(PrSpecifier::PullRequest {
            repo: GitHubRepo::new(DEFAULT_GITHUB_OWNER, DEFAULT_GITHUB_REPO),
            number,
        });
    }

    if let Ok(number) = value.parse::<u64>() {
        return Ok(PrSpecifier::PullRequest {
            repo: GitHubRepo::new(DEFAULT_GITHUB_OWNER, DEFAULT_GITHUB_REPO),
            number,
        });
    }

    anyhow::bail!(
        "unsupported --pr value: {value}\nexpected a workflow run URL, PR URL, #PR number, or PR number"
    )
}

fn parse_pr_specifier_from_url(raw: &str) -> Result<Option<PrSpecifier>> {
    let url = match Url::parse(raw) {
        Ok(url) => url,
        Err(_) => return Ok(None),
    };

    if url.host_str() != Some("github.com") {
        anyhow::bail!(
            "unsupported --pr URL host: {} (expected github.com)",
            url.host_str().unwrap_or("(none)")
        );
    }

    let segments = url
        .path_segments()
        .map(|parts| parts.filter(|part| !part.is_empty()).collect::<Vec<_>>())
        .unwrap_or_default();

    if segments.len() >= 5 && segments[2] == "actions" && segments[3] == "runs" {
        let run_id = segments[4]
            .parse::<u64>()
            .with_context(|| format!("invalid workflow run id in URL: {raw}"))?;
        return Ok(Some(PrSpecifier::WorkflowRun {
            repo: GitHubRepo::new(segments[0], segments[1]),
            run_id,
        }));
    }

    if segments.len() >= 4 && segments[2] == "pull" {
        let number = segments[3]
            .parse::<u64>()
            .with_context(|| format!("invalid PR number in URL: {raw}"))?;
        return Ok(Some(PrSpecifier::PullRequest {
            repo: GitHubRepo::new(segments[0], segments[1]),
            number,
        }));
    }

    Ok(None)
}

async fn resolve_workflow_run_from_pr_specifier(
    client: &Client,
    specifier: PrSpecifier,
) -> Result<ResolvedWorkflowRun> {
    match specifier {
        PrSpecifier::WorkflowRun { repo, run_id } => Ok(ResolvedWorkflowRun {
            run_url: format!(
                "https://github.com/{}/{}/actions/runs/{run_id}",
                repo.owner, repo.name
            ),
            repo,
            run_id,
        }),
        PrSpecifier::PullRequest { repo, number } => {
            let pr_url = format!("{}/pulls/{number}", repo.api_base_url());
            let pr: GitHubPullRequest =
                github_get_json(client, &pr_url, "failed to load pull request metadata").await?;

            let runs_url = format!(
                "{}/actions/workflows/{RELEASE_CANARY_WORKFLOW_FILE}/runs?head_sha={}&per_page=30",
                repo.api_base_url(),
                pr.head.sha
            );
            let runs: GitHubWorkflowRunsResponse =
                github_get_json(client, &runs_url, "failed to load workflow runs for PR").await?;

            if let Some(run) = runs
                .workflow_runs
                .iter()
                .find(|run| run.conclusion.as_deref() == Some("success"))
            {
                return Ok(ResolvedWorkflowRun {
                    repo,
                    run_id: run.id,
                    run_url: run.html_url.clone(),
                });
            }

            if let Some(latest) = runs.workflow_runs.first() {
                anyhow::bail!(
                    "no successful {RELEASE_CANARY_WORKFLOW_FILE} run found for PR #{number}. latest run: {} (head_sha={})",
                    latest.html_url,
                    latest.head_sha
                );
            }

            anyhow::bail!(
                "no {RELEASE_CANARY_WORKFLOW_FILE} runs found for PR #{number}. wait for CI to run and retry"
            );
        }
    }
}

async fn fetch_platform_artifact(
    client: &Client,
    repo: &GitHubRepo,
    run_id: u64,
    target_triple: &str,
) -> Result<GitHubArtifact> {
    let artifacts_url = format!(
        "{}/actions/runs/{run_id}/artifacts?per_page=100",
        repo.api_base_url()
    );
    let artifacts: GitHubWorkflowArtifactsResponse = github_get_json(
        client,
        &artifacts_url,
        "failed to load workflow artifacts for run",
    )
    .await?;

    let expected_name = format!("artifacts-build-local-{target_triple}");

    if let Some(artifact) = artifacts
        .artifacts
        .iter()
        .find(|artifact| artifact.name == expected_name && !artifact.expired)
        .cloned()
    {
        return Ok(artifact);
    }

    if artifacts
        .artifacts
        .iter()
        .any(|artifact| artifact.name == expected_name && artifact.expired)
    {
        anyhow::bail!(
            "workflow artifact `{expected_name}` has expired for run {run_id}; re-run CI for this PR"
        );
    }

    let available = artifacts
        .artifacts
        .iter()
        .map(|artifact| artifact.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");

    anyhow::bail!(
        "artifact `{expected_name}` not found for run {run_id}. available artifacts: {available}"
    )
}

fn package_archive_name_for_target(target: &str) -> String {
    #[cfg(windows)]
    {
        format!("bt-{target}.zip")
    }
    #[cfg(not(windows))]
    {
        format!("bt-{target}.tar.gz")
    }
}

#[allow(unreachable_code)]
fn host_target_triple() -> Option<&'static str> {
    #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
    {
        return Some("aarch64-apple-darwin");
    }
    #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
    {
        return Some("x86_64-apple-darwin");
    }
    #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
    {
        return Some("aarch64-unknown-linux-gnu");
    }
    #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
    {
        return Some("x86_64-unknown-linux-gnu");
    }
    #[cfg(all(target_os = "windows", target_arch = "x86_64"))]
    {
        return Some("x86_64-pc-windows-msvc");
    }

    None
}

fn create_temp_dir(prefix: &str) -> Result<PathBuf> {
    let millis = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .context("system clock error")?
        .as_millis();
    let path = env::temp_dir().join(format!("{prefix}-{}-{millis}", std::process::id()));
    fs::create_dir_all(&path).with_context(|| format!("failed to create {}", path.display()))?;
    Ok(path)
}

fn extract_named_file_from_zip(zip_path: &Path, wanted_name: &str, out_path: &Path) -> Result<()> {
    let file =
        File::open(zip_path).with_context(|| format!("failed to open {}", zip_path.display()))?;
    let mut archive = ZipArchive::new(file)
        .with_context(|| format!("failed to parse zip archive {}", zip_path.display()))?;

    for index in 0..archive.len() {
        let mut entry = archive
            .by_index(index)
            .context("failed to read zip entry from archive")?;
        let Some(file_name) = Path::new(entry.name())
            .file_name()
            .and_then(|name| name.to_str())
        else {
            continue;
        };
        if file_name != wanted_name {
            continue;
        }

        let mut out = File::create(out_path)
            .with_context(|| format!("failed to create {}", out_path.display()))?;
        io::copy(&mut entry, &mut out)
            .with_context(|| format!("failed to extract {wanted_name} from zip archive"))?;
        return Ok(());
    }

    anyhow::bail!(
        "file `{wanted_name}` not found in artifact archive {}",
        zip_path.display()
    )
}

fn extract_binary_from_package_archive(package_path: &Path, out_path: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        let package_file = File::open(package_path)
            .with_context(|| format!("failed to open {}", package_path.display()))?;
        let mut archive = ZipArchive::new(package_file)
            .with_context(|| format!("failed to parse {} as zip", package_path.display()))?;

        for index in 0..archive.len() {
            let mut entry = archive
                .by_index(index)
                .context("failed to read zip entry")?;
            let Some(file_name) = Path::new(entry.name())
                .file_name()
                .and_then(|name| name.to_str())
            else {
                continue;
            };
            if file_name != binary_name() {
                continue;
            }

            let mut out = File::create(out_path)
                .with_context(|| format!("failed to create {}", out_path.display()))?;
            io::copy(&mut entry, &mut out)
                .with_context(|| format!("failed to extract {} from package", binary_name()))?;
            return Ok(());
        }

        anyhow::bail!(
            "binary {} not found in package archive {}",
            binary_name(),
            package_path.display()
        );
    }

    #[cfg(not(windows))]
    {
        let package_file = File::open(package_path)
            .with_context(|| format!("failed to open {}", package_path.display()))?;
        let decoder = GzDecoder::new(package_file);
        let mut archive = Archive::new(decoder);

        for entry in archive.entries().context("failed to read tar entries")? {
            let mut entry = entry.context("failed to load tar entry")?;
            let path = entry.path().context("failed to read tar path")?;
            let Some(file_name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if file_name != binary_name() {
                continue;
            }

            let mut out = File::create(out_path)
                .with_context(|| format!("failed to create {}", out_path.display()))?;
            io::copy(&mut entry, &mut out)
                .with_context(|| format!("failed to extract {} from package", binary_name()))?;
            return Ok(());
        }

        anyhow::bail!(
            "binary {} not found in package archive {}",
            binary_name(),
            package_path.display()
        );
    }
}

fn replace_current_binary(new_binary_path: &Path) -> Result<()> {
    #[cfg(windows)]
    {
        let _ = new_binary_path;
        anyhow::bail!(
            "replacing the running binary is not currently supported on Windows for `bt self update --pr`"
        );
    }

    #[cfg(not(windows))]
    {
        let exe = env::current_exe().context("failed to determine current executable path")?;
        let parent = exe.parent().with_context(|| {
            format!("missing parent directory for executable {}", exe.display())
        })?;
        let staged_path = parent.join(format!("{}.new", binary_name()));

        fs::copy(new_binary_path, &staged_path).with_context(|| {
            format!(
                "failed to stage new binary from {} to {}",
                new_binary_path.display(),
                staged_path.display()
            )
        })?;

        let existing_mode = fs::metadata(&exe)
            .context("failed to read current executable permissions")?
            .permissions()
            .mode();
        fs::set_permissions(&staged_path, fs::Permissions::from_mode(existing_mode)).with_context(
            || {
                format!(
                    "failed to set executable permissions on {}",
                    staged_path.display()
                )
            },
        )?;

        fs::rename(&staged_path, &exe).with_context(|| {
            format!(
                "failed to replace executable {} with {}",
                exe.display(),
                staged_path.display()
            )
        })?;

        Ok(())
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

fn inferred_update_channel(raw: Option<&str>) -> UpdateChannel {
    parse_update_channel(raw).unwrap_or(UpdateChannel::Canary)
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
    fn parse_pr_specifier_accepts_pr_url() {
        let parsed = parse_pr_specifier("https://github.com/braintrustdata/bt/pull/24")
            .expect("PR URL should parse");
        assert_eq!(
            parsed,
            PrSpecifier::PullRequest {
                repo: GitHubRepo::new("braintrustdata", "bt"),
                number: 24,
            }
        );
    }

    #[test]
    fn parse_pr_specifier_accepts_run_url() {
        let parsed =
            parse_pr_specifier("https://github.com/braintrustdata/bt/actions/runs/22047433355")
                .expect("run URL should parse");
        assert_eq!(
            parsed,
            PrSpecifier::WorkflowRun {
                repo: GitHubRepo::new("braintrustdata", "bt"),
                run_id: 22_047_433_355,
            }
        );
    }

    #[test]
    fn parse_pr_specifier_accepts_bare_pr_number() {
        let parsed = parse_pr_specifier("24").expect("bare PR number should parse");
        assert_eq!(
            parsed,
            PrSpecifier::PullRequest {
                repo: GitHubRepo::new("braintrustdata", "bt"),
                number: 24,
            }
        );
    }

    #[test]
    fn parse_pr_specifier_rejects_unknown_format() {
        let err = parse_pr_specifier("not-a-ref").expect_err("invalid ref should fail");
        assert!(err.to_string().contains("unsupported --pr value"));
    }
}
