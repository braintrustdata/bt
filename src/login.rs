use std::collections::BTreeMap;
use std::fs;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{bail, Context, Result};
use braintrust_sdk_rust::{BraintrustClient, LoginState};
use clap::{Args, Subcommand};
use dialoguer::{Confirm, Password, Select};
use reqwest::Client;
use serde::{Deserialize, Serialize};

use crate::args::BaseArgs;

const DEFAULT_API_URL: &str = "https://api.braintrust.dev";
const DEFAULT_APP_URL: &str = "https://www.braintrust.dev";
const KEYCHAIN_SERVICE: &str = "com.braintrust.bt.cli";

pub struct LoginContext {
    pub login: LoginState,
    pub api_url: String,
    pub app_url: String,
}

#[derive(Debug, Clone)]
pub struct ResolvedAuth {
    pub api_key: Option<String>,
    pub api_url: Option<String>,
    pub app_url: Option<String>,
    pub org_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AuthStore {
    #[serde(default)]
    active_profile: Option<String>,
    #[serde(default)]
    profiles: BTreeMap<String, AuthProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct AuthProfile {
    #[serde(default)]
    api_url: Option<String>,
    #[serde(default)]
    app_url: Option<String>,
    #[serde(default)]
    org_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
struct ApiKeyLoginResponse {
    org_info: Vec<LoginOrgInfo>,
}

#[derive(Debug, Clone, Deserialize)]
struct LoginOrgInfo {
    id: String,
    name: String,
    #[serde(default)]
    api_url: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct LoginArgs {
    #[command(subcommand)]
    command: Option<LoginCommand>,
}

#[derive(Debug, Clone, Subcommand)]
enum LoginCommand {
    /// Save credentials to a profile and make it active
    Set(LoginSetArgs),
    /// List saved login profiles
    List,
    /// Switch active profile
    Use(LoginUseArgs),
    /// Delete a saved profile
    Delete(LoginDeleteArgs),
    /// Clear the active profile
    Logout,
    /// Show current login status
    Status,
}

#[derive(Debug, Clone, Args)]
struct LoginSetArgs {
    /// API key to store
    #[arg(long, value_name = "KEY")]
    api_key: Option<String>,

    /// Profile name
    #[arg(long, short = 'n', default_value = "default")]
    profile: String,

    /// API URL for this profile
    #[arg(long, value_name = "URL")]
    api_url: Option<String>,

    /// App URL for this profile
    #[arg(long, value_name = "URL")]
    app_url: Option<String>,

    /// Org name for multi-org API keys
    #[arg(long, value_name = "ORG")]
    org_name: Option<String>,
}

#[derive(Debug, Clone, Args)]
struct LoginUseArgs {
    /// Profile name
    profile: String,
}

#[derive(Debug, Clone, Args)]
struct LoginDeleteArgs {
    /// Profile name
    profile: String,

    /// Skip confirmation prompt
    #[arg(long, short = 'f')]
    force: bool,
}

pub async fn run(args: LoginArgs) -> Result<()> {
    match args.command {
        None => {
            run_login_set(LoginSetArgs {
                api_key: None,
                profile: "default".to_string(),
                api_url: None,
                app_url: None,
                org_name: None,
            })
            .await
        }
        Some(LoginCommand::Set(set_args)) => run_login_set(set_args).await,
        Some(LoginCommand::List) => run_login_list(),
        Some(LoginCommand::Use(use_args)) => run_login_use(&use_args.profile),
        Some(LoginCommand::Delete(delete_args)) => {
            run_login_delete(&delete_args.profile, delete_args.force)
        }
        Some(LoginCommand::Logout) => run_login_logout(),
        Some(LoginCommand::Status) => run_login_status(),
    }
}

pub async fn login(base: &BaseArgs) -> Result<LoginContext> {
    let auth = resolve_auth(base)?;
    let api_key = auth.api_key.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "no login credentials found; set BRAINTRUST_API_KEY, pass --api-key, or run `bt login set`"
        )
    })?;

    let mut builder = BraintrustClient::builder()
        .blocking_login(true)
        .api_key(api_key);

    if let Some(api_url) = &auth.api_url {
        builder = builder.api_url(api_url);
    }
    if let Some(app_url) = &auth.app_url {
        builder = builder.app_url(app_url);
    }
    if let Some(org_name) = &auth.org_name {
        builder = builder.org_name(org_name);
    }
    if let Some(project) = &base.project {
        builder = builder.default_project(project);
    }

    let client = builder.build().await?;
    let login = client.wait_for_login().await?;

    let api_url = login
        .api_url
        .clone()
        .or(auth.api_url.clone())
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());

    let app_url = auth
        .app_url
        .clone()
        .unwrap_or_else(|| DEFAULT_APP_URL.to_string());

    Ok(LoginContext {
        login,
        api_url,
        app_url,
    })
}

pub fn resolve_auth(base: &BaseArgs) -> Result<ResolvedAuth> {
    let store = load_auth_store()?;
    resolve_auth_from_store(base, &store)
}

pub fn resolved_auth_env(base: &BaseArgs) -> Result<Vec<(String, String)>> {
    let auth = resolve_auth(base)?;
    let mut envs = Vec::new();

    if let Some(api_key) = auth.api_key {
        envs.push(("BRAINTRUST_API_KEY".to_string(), api_key));
    }
    if let Some(api_url) = auth.api_url {
        envs.push(("BRAINTRUST_API_URL".to_string(), api_url));
    }
    if let Some(app_url) = auth.app_url {
        envs.push(("BRAINTRUST_APP_URL".to_string(), app_url));
    }
    if let Some(org_name) = auth.org_name {
        envs.push(("BRAINTRUST_ORG_NAME".to_string(), org_name));
    }

    Ok(envs)
}

fn resolve_auth_from_store(base: &BaseArgs, store: &AuthStore) -> Result<ResolvedAuth> {
    resolve_auth_from_store_with_secret_lookup(base, store, load_profile_secret)
}

fn resolve_auth_from_store_with_secret_lookup<F>(
    base: &BaseArgs,
    store: &AuthStore,
    load_secret: F,
) -> Result<ResolvedAuth>
where
    F: Fn(&str) -> Result<Option<String>>,
{
    if let Some(api_key) = base.api_key.clone() {
        return Ok(ResolvedAuth {
            api_key: Some(api_key),
            api_url: base.api_url.clone(),
            app_url: base.app_url.clone(),
            org_name: base.org_name.clone(),
        });
    }

    let requested_profile = base
        .profile
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());
    let selected_profile_name = requested_profile.or(store.active_profile.as_deref());

    if let Some(profile_name) = selected_profile_name {
        let profile = store.profiles.get(profile_name).ok_or_else(|| {
            anyhow::anyhow!(
                "profile '{}' not found; run `bt login list` or `bt login set --profile {}`",
                profile_name,
                profile_name
            )
        })?;
        let api_key = load_secret(profile_name)?.ok_or_else(|| {
            anyhow::anyhow!(
                "no keychain credential found for profile '{}'; re-run `bt login set --profile {}`",
                profile_name,
                profile_name
            )
        })?;

        return Ok(ResolvedAuth {
            api_key: Some(api_key),
            api_url: base.api_url.clone().or_else(|| profile.api_url.clone()),
            app_url: base.app_url.clone().or_else(|| profile.app_url.clone()),
            org_name: base.org_name.clone().or_else(|| profile.org_name.clone()),
        });
    }

    Ok(ResolvedAuth {
        api_key: None,
        api_url: base.api_url.clone(),
        app_url: base.app_url.clone(),
        org_name: base.org_name.clone(),
    })
}

async fn run_login_set(args: LoginSetArgs) -> Result<()> {
    let profile_name = args.profile.trim();
    if profile_name.is_empty() {
        bail!("profile name cannot be empty");
    }
    let interactive = std::io::stdin().is_terminal();

    let api_key = match args.api_key {
        Some(value) if !value.trim().is_empty() => value,
        Some(_) => bail!("api key cannot be empty"),
        None => prompt_api_key()?,
    };

    let login_app_url = args
        .app_url
        .clone()
        .unwrap_or_else(|| DEFAULT_APP_URL.to_string());
    let login_orgs = fetch_login_orgs(&api_key, &login_app_url).await?;
    let selected_org = select_login_org(login_orgs, args.org_name.as_deref(), interactive)?;

    let selected_api_url = args
        .api_url
        .clone()
        .or_else(|| selected_org.api_url.clone())
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());

    if interactive {
        println!("Selected org: {}", selected_org.name);
        println!("Resolved API URL: {selected_api_url}");
        let confirmed = Confirm::new()
            .with_prompt("Use this API URL?")
            .default(true)
            .interact()?;
        if !confirmed {
            bail!("login cancelled");
        }
    }

    save_profile_secret(profile_name, &api_key)?;

    let mut store = load_auth_store()?;
    let stored_api_url = Some(selected_api_url.clone());
    let stored_app_url = args.app_url;

    store.profiles.insert(
        profile_name.to_string(),
        AuthProfile {
            api_url: stored_api_url,
            app_url: stored_app_url,
            org_name: Some(selected_org.name.clone()),
        },
    );
    store.active_profile = Some(profile_name.to_string());
    save_auth_store(&store)?;

    println!(
        "Logged in to org '{}' with profile '{}'",
        selected_org.name, profile_name
    );
    println!(
        "Saved profile metadata at {} and credential in OS keychain",
        auth_store_path()?.display()
    );
    Ok(())
}

fn run_login_list() -> Result<()> {
    let store = load_auth_store()?;
    if store.profiles.is_empty() {
        println!("No saved login profiles. Run `bt login set` to create one.");
        return Ok(());
    }

    println!("Saved login profiles:");
    for (name, profile) in &store.profiles {
        let active_marker = if store.active_profile.as_deref() == Some(name.as_str()) {
            "*"
        } else {
            " "
        };
        let org = profile
            .org_name
            .as_deref()
            .map(|value| format!(" org={value}"))
            .unwrap_or_default();
        let api_url = profile
            .api_url
            .as_deref()
            .map(|value| format!(" api_url={value}"))
            .unwrap_or_default();
        println!("{active_marker} {name}{org}{api_url}");
    }
    Ok(())
}

fn run_login_use(profile_name: &str) -> Result<()> {
    let profile_name = profile_name.trim();
    if profile_name.is_empty() {
        bail!("profile name cannot be empty");
    }

    let mut store = load_auth_store()?;
    if !store.profiles.contains_key(profile_name) {
        bail!(
            "profile '{}' not found; run `bt login list` to see available profiles",
            profile_name
        );
    }

    store.active_profile = Some(profile_name.to_string());
    save_auth_store(&store)?;
    println!("Switched active profile to '{profile_name}'");
    Ok(())
}

fn run_login_delete(profile_name: &str, force: bool) -> Result<()> {
    let profile_name = profile_name.trim();
    if profile_name.is_empty() {
        bail!("profile name cannot be empty");
    }

    let mut store = load_auth_store()?;
    if !store.profiles.contains_key(profile_name) {
        bail!(
            "profile '{}' not found; run `bt login list` to see available profiles",
            profile_name
        );
    }

    if !force && std::io::stdin().is_terminal() {
        let confirmed = Confirm::new()
            .with_prompt(format!("Delete profile '{profile_name}'?"))
            .default(false)
            .interact()?;
        if !confirmed {
            println!("Cancelled");
            return Ok(());
        }
    }

    store.profiles.remove(profile_name);
    if store.active_profile.as_deref() == Some(profile_name) {
        store.active_profile = None;
    }
    save_auth_store(&store)?;
    if let Err(err) = delete_profile_secret(profile_name) {
        eprintln!("warning: failed to delete keychain credential for '{profile_name}': {err}");
    }

    println!("Deleted profile '{profile_name}'");
    Ok(())
}

fn run_login_logout() -> Result<()> {
    let mut store = load_auth_store()?;
    if store.active_profile.is_none() {
        println!("No active profile.");
        return Ok(());
    }

    store.active_profile = None;
    save_auth_store(&store)?;
    println!("Cleared active profile.");
    Ok(())
}

fn run_login_status() -> Result<()> {
    let store = load_auth_store()?;
    let env_api_key = std::env::var("BRAINTRUST_API_KEY")
        .ok()
        .map(|v| !v.trim().is_empty())
        .unwrap_or(false);

    if env_api_key {
        println!("Auth source: BRAINTRUST_API_KEY environment variable");
        if let Ok(profile_name) = std::env::var("BRAINTRUST_PROFILE") {
            println!("Requested profile via BRAINTRUST_PROFILE={profile_name}");
        } else if let Some(active_profile) = store.active_profile.as_deref() {
            println!("Active saved profile (ignored while env key is set): {active_profile}");
        }
        return Ok(());
    }

    if let Some(active_profile) = store.active_profile.as_deref() {
        println!("Active profile: {active_profile}");
        if let Some(profile) = store.profiles.get(active_profile) {
            if let Some(org_name) = &profile.org_name {
                println!("Org: {org_name}");
            }
            if let Some(api_url) = &profile.api_url {
                println!("API URL: {api_url}");
            }
        }
        println!("Credentials file: {}", auth_store_path()?.display());
        return Ok(());
    }

    println!("No active profile.");
    println!("Run `bt login set` or set BRAINTRUST_API_KEY.");
    Ok(())
}

async fn fetch_login_orgs(api_key: &str, app_url: &str) -> Result<Vec<LoginOrgInfo>> {
    let login_url = format!("{}/api/apikey/login", app_url.trim_end_matches('/'));
    let client = Client::builder()
        .build()
        .context("failed to initialize HTTP client")?;
    let response = client
        .post(&login_url)
        .bearer_auth(api_key)
        .header("Content-Type", "application/json")
        .send()
        .await
        .with_context(|| format!("failed to call login endpoint {}", login_url))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("login failed ({status}): {body}");
    }

    let payload: ApiKeyLoginResponse = response
        .json()
        .await
        .context("failed to parse login response")?;
    if payload.org_info.is_empty() {
        bail!("no organizations found for this API key");
    }

    Ok(payload.org_info)
}

fn select_login_org(
    orgs: Vec<LoginOrgInfo>,
    requested_org_name: Option<&str>,
    interactive: bool,
) -> Result<LoginOrgInfo> {
    if orgs.is_empty() {
        bail!("no organizations found for this API key");
    }

    if let Some(name) = requested_org_name {
        let selected = orgs
            .iter()
            .find(|org| org.name == name)
            .or_else(|| {
                let lowered = name.to_ascii_lowercase();
                orgs.iter()
                    .find(|org| org.name.to_ascii_lowercase() == lowered)
            })
            .cloned();
        return selected.ok_or_else(|| {
            let available = orgs
                .iter()
                .map(|org| org.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            anyhow::anyhow!(
                "organization '{}' not found for this API key. Available organizations: {}",
                name,
                available
            )
        });
    }

    if orgs.len() == 1 {
        return Ok(orgs.into_iter().next().expect("org exists"));
    }

    if !interactive {
        let available = orgs
            .iter()
            .map(|org| org.name.as_str())
            .collect::<Vec<_>>()
            .join(", ");
        bail!(
            "multiple organizations found for this API key; pass --org-name. Available organizations: {}",
            available
        );
    }

    let labels: Vec<String> = orgs
        .iter()
        .map(|org| {
            let api_url = org.api_url.as_deref().unwrap_or(DEFAULT_API_URL);
            format!("{} [{}] ({})", org.name, org.id, api_url)
        })
        .collect();
    let selection = Select::new()
        .with_prompt("Multiple organizations found. Select one")
        .items(&labels)
        .default(0)
        .interact()?;

    Ok(orgs
        .into_iter()
        .nth(selection)
        .expect("selected index should be in range"))
}

fn prompt_api_key() -> Result<String> {
    if !std::io::stdin().is_terminal() {
        bail!("--api-key is required in non-interactive mode");
    }

    let api_key = Password::new()
        .with_prompt("Braintrust API key")
        .allow_empty_password(false)
        .interact()
        .context("failed to read API key")?;

    if api_key.trim().is_empty() {
        bail!("api key cannot be empty");
    }

    Ok(api_key)
}

fn save_profile_secret(profile_name: &str, api_key: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("security")
            .args([
                "add-generic-password",
                "-a",
                profile_name,
                "-s",
                KEYCHAIN_SERVICE,
                "-w",
                api_key,
                "-U",
            ])
            .output()
            .context("failed to execute macOS keychain utility `security`")?;

        if output.status.success() {
            return Ok(());
        }

        bail!(
            "failed to store credential in macOS keychain: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    #[cfg(target_os = "linux")]
    {
        let mut child = Command::new("secret-tool")
            .args([
                "store",
                "--label=Braintrust bt profile credential",
                "service",
                KEYCHAIN_SERVICE,
                "profile",
                profile_name,
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("failed to execute Linux keychain utility `secret-tool`")?;

        if let Some(stdin) = child.stdin.as_mut() {
            stdin
                .write_all(api_key.as_bytes())
                .context("failed to write credential to secret-tool")?;
            stdin
                .write_all(b"\n")
                .context("failed to write credential newline to secret-tool")?;
        }

        let output = child
            .wait_with_output()
            .context("failed while waiting for secret-tool")?;
        if output.status.success() {
            return Ok(());
        }

        bail!(
            "failed to store credential in Linux keychain: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = profile_name;
        let _ = api_key;
        bail!("OS keychain credential storage is not implemented on this platform");
    }
}

fn load_profile_secret(profile_name: &str) -> Result<Option<String>> {
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("security")
            .args([
                "find-generic-password",
                "-a",
                profile_name,
                "-s",
                KEYCHAIN_SERVICE,
                "-w",
            ])
            .output()
            .context("failed to execute macOS keychain utility `security`")?;

        if output.status.success() {
            let secret = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if secret.is_empty() {
                return Ok(None);
            }
            return Ok(Some(secret));
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        let not_found = output.status.code() == Some(44) || stderr.contains("could not be found");
        if not_found {
            return Ok(None);
        }

        bail!(
            "failed to load credential from macOS keychain: {}",
            stderr.trim()
        );
    }

    #[cfg(target_os = "linux")]
    {
        let output = Command::new("secret-tool")
            .args([
                "lookup",
                "service",
                KEYCHAIN_SERVICE,
                "profile",
                profile_name,
            ])
            .output()
            .context("failed to execute Linux keychain utility `secret-tool`")?;

        if output.status.success() {
            let secret = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if secret.is_empty() {
                return Ok(None);
            }
            return Ok(Some(secret));
        }

        if output.status.code() == Some(1) {
            return Ok(None);
        }

        bail!(
            "failed to load credential from Linux keychain: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = profile_name;
        bail!("OS keychain credential retrieval is not implemented on this platform");
    }
}

fn delete_profile_secret(profile_name: &str) -> Result<()> {
    #[cfg(target_os = "macos")]
    {
        let output = Command::new("security")
            .args([
                "delete-generic-password",
                "-a",
                profile_name,
                "-s",
                KEYCHAIN_SERVICE,
            ])
            .output()
            .context("failed to execute macOS keychain utility `security`")?;

        if output.status.success() {
            return Ok(());
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        let not_found = output.status.code() == Some(44) || stderr.contains("could not be found");
        if not_found {
            return Ok(());
        }

        bail!(
            "failed to delete credential from macOS keychain: {}",
            stderr.trim()
        );
    }

    #[cfg(target_os = "linux")]
    {
        let output = Command::new("secret-tool")
            .args([
                "clear",
                "service",
                KEYCHAIN_SERVICE,
                "profile",
                profile_name,
            ])
            .output()
            .context("failed to execute Linux keychain utility `secret-tool`")?;

        if output.status.success() || output.status.code() == Some(1) {
            return Ok(());
        }

        bail!(
            "failed to delete credential from Linux keychain: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = profile_name;
        bail!("OS keychain credential deletion is not implemented on this platform");
    }
}

fn load_auth_store() -> Result<AuthStore> {
    let path = auth_store_path()?;
    load_auth_store_from_path(&path)
}

fn load_auth_store_from_path(path: &Path) -> Result<AuthStore> {
    if !path.exists() {
        return Ok(AuthStore::default());
    }

    let data = fs::read_to_string(path)
        .with_context(|| format!("failed to read auth config {}", path.display()))?;
    serde_json::from_str(&data)
        .with_context(|| format!("failed to parse auth config {}", path.display()))
}

fn save_auth_store(store: &AuthStore) -> Result<()> {
    let path = auth_store_path()?;
    save_auth_store_to_path(&path, store)
}

fn save_auth_store_to_path(path: &Path, store: &AuthStore) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    let data = serde_json::to_string_pretty(store).context("failed to serialize auth config")?;
    let temp_path = path.with_extension("tmp");
    let mut file = fs::File::create(&temp_path)
        .with_context(|| format!("failed to write temp auth config {}", temp_path.display()))?;
    file.write_all(data.as_bytes())
        .with_context(|| format!("failed to write temp auth config {}", temp_path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("failed to write temp auth config {}", temp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to flush temp auth config {}", temp_path.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temp_path, fs::Permissions::from_mode(0o600)).with_context(|| {
            format!(
                "failed to set permissions on temp auth config {}",
                temp_path.display()
            )
        })?;
    }

    fs::rename(&temp_path, path).with_context(|| {
        format!(
            "failed to move temp auth config {} to {}",
            temp_path.display(),
            path.display()
        )
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o600)).with_context(|| {
            format!(
                "failed to set permissions on auth config {}",
                path.display()
            )
        })?;
    }

    Ok(())
}

fn auth_store_path() -> Result<PathBuf> {
    #[cfg(windows)]
    {
        let app_data =
            std::env::var_os("APPDATA").ok_or_else(|| anyhow::anyhow!("APPDATA is not set"))?;
        return Ok(PathBuf::from(app_data).join("bt").join("auth.json"));
    }

    #[cfg(not(windows))]
    {
        if let Some(xdg_config_home) = std::env::var_os("XDG_CONFIG_HOME") {
            return Ok(PathBuf::from(xdg_config_home).join("bt").join("auth.json"));
        }

        let home = std::env::var_os("HOME")
            .ok_or_else(|| anyhow::anyhow!("HOME is not set and XDG_CONFIG_HOME is unset"))?;
        Ok(PathBuf::from(home)
            .join(".config")
            .join("bt")
            .join("auth.json"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn make_base() -> BaseArgs {
        BaseArgs {
            json: false,
            profile: None,
            project: None,
            org_name: None,
            api_key: None,
            api_url: None,
            app_url: None,
            env_file: None,
        }
    }

    #[test]
    fn default_app_url_is_www() {
        assert_eq!(DEFAULT_APP_URL, "https://www.braintrust.dev");
    }

    #[test]
    fn save_and_load_auth_store_round_trip() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("bt-auth-store-test-{unique}"));
        fs::create_dir_all(&dir).expect("create dir");
        let path = dir.join("auth.json");

        let mut store = AuthStore::default();
        store.active_profile = Some("work".to_string());
        store.profiles.insert(
            "work".to_string(),
            AuthProfile {
                api_url: Some("https://api.example.com".to_string()),
                app_url: Some("https://www.example.com".to_string()),
                org_name: Some("Example Org".to_string()),
            },
        );

        save_auth_store_to_path(&path, &store).expect("save");
        let loaded = load_auth_store_from_path(&path).expect("load");

        assert_eq!(loaded.active_profile, Some("work".to_string()));
        assert!(loaded.profiles.contains_key("work"));

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn resolve_auth_uses_profile_when_no_api_key_override() {
        let mut base = make_base();
        base.profile = Some("work".to_string());

        let mut store = AuthStore::default();
        store.profiles.insert(
            "work".to_string(),
            AuthProfile {
                api_url: Some("https://api.example.com".to_string()),
                app_url: Some("https://www.example.com".to_string()),
                org_name: Some("Example Org".to_string()),
            },
        );

        let resolved = resolve_auth_from_store_with_secret_lookup(&base, &store, |_| {
            Ok(Some("profile-key".to_string()))
        })
        .expect("resolve");
        assert_eq!(resolved.api_key.as_deref(), Some("profile-key"));
        assert_eq!(resolved.api_url.as_deref(), Some("https://api.example.com"));
        assert_eq!(resolved.org_name.as_deref(), Some("Example Org"));
    }

    #[test]
    fn resolve_auth_prefers_explicit_api_key() {
        let mut base = make_base();
        base.api_key = Some("explicit-key".to_string());
        base.api_url = Some("https://override.example.com".to_string());

        let mut store = AuthStore::default();
        store.active_profile = Some("work".to_string());
        store.profiles.insert(
            "work".to_string(),
            AuthProfile {
                api_url: Some("https://api.example.com".to_string()),
                app_url: None,
                org_name: None,
            },
        );

        let resolved = resolve_auth_from_store(&base, &store).expect("resolve");
        assert_eq!(resolved.api_key.as_deref(), Some("explicit-key"));
        assert_eq!(
            resolved.api_url.as_deref(),
            Some("https://override.example.com")
        );
    }
}
