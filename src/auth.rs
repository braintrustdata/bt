use std::collections::BTreeMap;
use std::error::Error as StdError;
use std::fs;
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use base64::Engine as _;
use braintrust_sdk_rust::{BraintrustClient, LoginState};
use clap::{Args, Subcommand};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use dialoguer::{Confirm, Input, Password};
use oauth2::basic::BasicClient;
use oauth2::{
    AuthUrl, ClientId, CsrfToken, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope, TokenUrl,
};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::{
    args::{BaseArgs, DEFAULT_API_URL, DEFAULT_APP_URL},
    http::{build_http_client, build_http_client_from_builder},
    ui,
};

const KEYCHAIN_SERVICE: &str = "com.braintrust.bt.cli";
const OAUTH_SCOPE: &str = "mcp";
const OAUTH_CALLBACK_TIMEOUT: Duration = Duration::from_secs(300);
const OAUTH_REFRESH_SAFETY_WINDOW_SECONDS: u64 = 60;
static SECRET_STORE_FALLBACK_WARNED: AtomicBool = AtomicBool::new(false);

#[derive(Clone)]
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
    pub is_oauth: bool,
}

#[derive(Debug, Clone)]
pub struct ProfileInfo {
    pub name: String,
    pub org_name: Option<String>,
    pub user_name: Option<String>,
    pub email: Option<String>,
    pub api_key_hint: Option<String>,
}

#[derive(Debug, Clone)]
pub(crate) struct StoredProfileInfo {
    pub name: String,
    pub is_oauth: bool,
    pub org_name: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailableOrg {
    pub id: String,
    pub name: String,
    pub api_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoverableAuthErrorKind {
    OauthProfileSelection,
    OauthClientId,
    OauthRefreshToken,
    StoredCredential,
}

#[derive(Debug)]
struct RecoverableAuthError {
    kind: RecoverableAuthErrorKind,
    message: String,
}

impl std::fmt::Display for RecoverableAuthError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

impl StdError for RecoverableAuthError {}

fn recoverable_auth_error(kind: RecoverableAuthErrorKind, message: String) -> anyhow::Error {
    anyhow::Error::new(RecoverableAuthError { kind, message })
}

pub fn is_missing_credential_error(err: &anyhow::Error) -> bool {
    err.chain().any(|source| {
        source
            .downcast_ref::<RecoverableAuthError>()
            .is_some_and(|err| {
                matches!(
                    err.kind,
                    RecoverableAuthErrorKind::OauthProfileSelection
                        | RecoverableAuthErrorKind::OauthClientId
                        | RecoverableAuthErrorKind::OauthRefreshToken
                        | RecoverableAuthErrorKind::StoredCredential
                )
            })
    })
}

pub fn list_profiles() -> Result<Vec<ProfileInfo>> {
    let store = load_auth_store()?;
    Ok(store
        .profiles
        .iter()
        .map(|(name, p)| ProfileInfo {
            name: name.clone(),
            org_name: p.org_name.clone(),
            user_name: p.user_name.clone(),
            email: p.email.clone(),
            api_key_hint: p.api_key_hint.clone(),
        })
        .collect())
}

pub(crate) fn list_stored_profiles() -> Result<Vec<StoredProfileInfo>> {
    let store = load_auth_store()?;
    Ok(store
        .profiles
        .iter()
        .map(|(name, profile)| StoredProfileInfo {
            name: name.clone(),
            is_oauth: profile.auth_kind == AuthKind::Oauth,
            org_name: profile.org_name.clone(),
        })
        .collect())
}

pub fn resolve_org_to_profile(identifier: &str, profiles: &[ProfileInfo]) -> Result<String> {
    if profiles.is_empty() {
        bail!("no auth profiles found. Run `bt auth login` to create one.");
    }

    if let Some(p) = profiles.iter().find(|p| p.name == identifier) {
        return Ok(p.name.clone());
    }

    let matches: Vec<&ProfileInfo> = profiles
        .iter()
        .filter(|p| p.org_name.as_deref() == Some(identifier))
        .collect();

    match matches.len() {
        0 => {
            let available: Vec<String> = profiles
                .iter()
                .filter_map(|p| {
                    p.org_name
                        .as_ref()
                        .map(|org| format!("  {} (profile: {})", org, p.name))
                })
                .collect();
            bail!(
                "no profile found for '{identifier}'.\nAvailable:\n{}",
                available.join("\n")
            );
        }
        1 => Ok(matches[0].name.clone()),
        _ => {
            if !ui::can_prompt() {
                bail!(
                    "multiple profiles for org '{identifier}': {}. Use --profile to disambiguate.",
                    matches
                        .iter()
                        .map(|p| p.name.as_str())
                        .collect::<Vec<_>>()
                        .join(", ")
                );
            }
            let names: Vec<&str> = matches.iter().map(|p| p.name.as_str()).collect();
            let idx = crate::ui::fuzzy_select(
                &format!("Multiple profiles for '{identifier}'. Select one"),
                &names,
                0,
            )?;
            Ok(matches[idx].name.clone())
        }
    }
}

pub fn select_profile_interactive(current: Option<&str>) -> Result<Option<String>> {
    let profiles = list_profiles()?;
    if profiles.is_empty() {
        bail!("no auth profiles found. Run `bt auth login` to create one.");
    }
    if profiles.len() == 1 {
        return Ok(Some(profiles[0].name.clone()));
    }

    let labels: Vec<String> = profiles
        .iter()
        .map(|p| match &p.org_name {
            Some(org) if org != &p.name => format!("{} (profile: {})", org, p.name),
            _ => p.name.clone(),
        })
        .collect();

    let default = current
        .and_then(|c| {
            profiles
                .iter()
                .position(|p| p.name == c || p.org_name.as_deref() == Some(c))
        })
        .unwrap_or(0);
    let idx = crate::ui::fuzzy_select("Select org", &labels, default)?;
    Ok(Some(profiles[idx].name.clone()))
}

pub async fn list_available_orgs(base: &BaseArgs) -> Result<Vec<AvailableOrg>> {
    let resolved = resolve_auth(base).await?;
    let app_url = resolved
        .app_url
        .unwrap_or_else(|| DEFAULT_APP_URL.to_string());
    let api_key = match resolved.api_key {
        Some(api_key) => api_key,
        None => login(base)
            .await?
            .login
            .api_key()
            .context("login state missing API key")?,
    };

    let mut orgs = fetch_login_orgs(&api_key, &app_url).await?;
    orgs.sort_by(|a, b| {
        a.name
            .to_ascii_lowercase()
            .cmp(&b.name.to_ascii_lowercase())
            .then_with(|| a.name.cmp(&b.name))
    });

    Ok(orgs
        .into_iter()
        .map(|org| AvailableOrg {
            id: org.id,
            name: org.name,
            api_url: org.api_url,
        })
        .collect())
}

pub(crate) async fn list_available_orgs_for_api_key(
    api_key: &str,
    app_url: &str,
) -> Result<Vec<AvailableOrg>> {
    let mut orgs = fetch_login_orgs(api_key, app_url).await?;
    orgs.sort_by(|a, b| {
        a.name
            .to_ascii_lowercase()
            .cmp(&b.name.to_ascii_lowercase())
            .then_with(|| a.name.cmp(&b.name))
    });

    Ok(orgs
        .into_iter()
        .map(|org| AvailableOrg {
            id: org.id,
            name: org.name,
            api_url: org.api_url,
        })
        .collect())
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AuthStore {
    #[serde(default)]
    profiles: BTreeMap<String, AuthProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SecretStore {
    #[serde(default)]
    secrets: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AuthProfile {
    #[serde(default)]
    auth_kind: AuthKind,
    #[serde(default)]
    api_url: Option<String>,
    #[serde(default)]
    app_url: Option<String>,
    #[serde(default)]
    org_name: Option<String>,
    #[serde(default)]
    oauth_client_id: Option<String>,
    #[serde(default)]
    oauth_access_expires_at: Option<u64>,
    #[serde(default)]
    user_name: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    api_key_hint: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
enum AuthKind {
    #[default]
    ApiKey,
    Oauth,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ApiKeyOrgMismatchAction {
    UseApiKey,
    UseOauth,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RequestedOrgResolution {
    NoRequestedOrg,
    UseRequestedOrg,
    IgnoreRequestedOrg,
    SwitchToOauth,
}

#[derive(Debug, Clone, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
struct OAuthErrorResponse {
    #[serde(default)]
    error: Option<String>,
    #[serde(default)]
    error_description: Option<String>,
}

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt auth login
  bt auth profiles
  bt auth refresh
  bt auth logout --profile work
")]
pub struct AuthArgs {
    #[command(subcommand)]
    command: AuthCommand,
}

#[derive(Debug, Clone, Subcommand)]
enum AuthCommand {
    /// Authenticate with Braintrust (OAuth or API key)
    Login(AuthLoginArgs),
    /// Force-refresh OAuth access token for a profile
    Refresh,
    /// List auth profiles and check connection status
    Profiles(AuthProfilesArgs),
    /// Log out by removing a saved profile
    Logout(AuthLogoutArgs),
}

#[derive(Debug, Clone, Args)]
struct AuthProfilesArgs {}

#[derive(Debug, Clone, Args)]
struct AuthLoginArgs {
    /// Use OAuth login instead of API key login
    #[arg(long)]
    oauth: bool,

    /// OAuth client id (defaults to bt_cli_<profile>)
    #[arg(long, value_name = "CLIENT_ID")]
    client_id: Option<String>,

    /// Do not try to open a browser automatically
    #[arg(long)]
    no_browser: bool,
}

#[derive(Debug, Clone, Args)]
struct AuthLogoutArgs {
    /// Profile name to log out of (interactive picker if omitted)
    #[arg(long)]
    profile: Option<String>,

    /// Skip confirmation prompt
    #[arg(long, short = 'f')]
    force: bool,
}

pub async fn run(base: BaseArgs, args: AuthArgs) -> Result<()> {
    match args.command {
        AuthCommand::Login(login_args) => run_login_set(&base, login_args).await,
        AuthCommand::Refresh => run_login_refresh(&base).await,
        AuthCommand::Profiles(profile_args) => run_profiles(&base, profile_args).await,
        AuthCommand::Logout(logout_args) => run_login_logout(base, logout_args),
    }
}

pub async fn login_read_only(base: &BaseArgs) -> Result<LoginContext> {
    if !has_cached_project_id(base) {
        return login(base).await;
    }

    let ctx = fast_login(base).await?;
    if ctx.login.org_name().unwrap_or_default().trim().is_empty() {
        login(base).await
    } else {
        Ok(ctx)
    }
}

/// Build login context from stored auth without forcing a login validation request.
/// Use for read-oriented flows where downstream API calls can surface auth errors.
pub async fn fast_login(base: &BaseArgs) -> Result<LoginContext> {
    maybe_warn_api_key_override(base);
    let auth = resolve_auth(base).await?;
    let api_key = auth.api_key.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "no login credentials found; set BRAINTRUST_API_KEY, pass --api-key, or run `bt auth login`"
        )
    })?;
    let org_name = auth.org_name.clone().unwrap_or_default();
    let api_url = auth
        .api_url
        .clone()
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());
    let app_url = auth
        .app_url
        .clone()
        .unwrap_or_else(|| DEFAULT_APP_URL.to_string());

    let login = LoginState::new();
    login.set(
        api_key,
        String::new(),
        org_name,
        api_url.clone(),
        app_url.clone(),
    );

    Ok(LoginContext {
        login,
        api_url,
        app_url,
    })
}

pub async fn login(base: &BaseArgs) -> Result<LoginContext> {
    maybe_warn_api_key_override(base);
    let auth = resolve_auth(base).await?;
    let api_key = auth.api_key.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "no login credentials found; set BRAINTRUST_API_KEY, pass --api-key, or run `bt auth login`"
        )
    })?;

    let mut builder = BraintrustClient::builder()
        .blocking_login(true)
        .api_key(api_key.clone());

    if let Some(api_url) = &auth.api_url {
        builder = builder.api_url(api_url);
    }
    if let Some(app_url) = &auth.app_url {
        builder = builder.app_url(app_url);
    }
    if let Some(org_name) = &auth.org_name {
        builder = builder.org_name(org_name);
    }
    let project = base
        .project
        .clone()
        .or_else(|| crate::config::configured_project_for_context(base, auth.org_name.as_deref()));
    if let Some(project) = &project {
        builder = builder.default_project(project);
    }
    let login = match builder.build().await {
        Ok(client) => client.wait_for_login().await?,
        Err(err) if auth.is_oauth => {
            let org_name = auth
                .org_name
                .clone()
                .ok_or_else(|| anyhow::anyhow!("oauth profile is missing org_name: {err}"))?;
            let login = LoginState::new();
            login.set(
                api_key.clone(),
                String::new(),
                org_name,
                auth.api_url
                    .clone()
                    .unwrap_or_else(|| DEFAULT_API_URL.to_string()),
                auth.app_url
                    .clone()
                    .unwrap_or_else(|| DEFAULT_APP_URL.to_string()),
            );
            login
        }
        Err(err) => return Err(err.into()),
    };

    let api_url = login
        .api_url()
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

fn has_cached_project_id(base: &BaseArgs) -> bool {
    crate::config::configured_project_id_for_base(base)
        .is_some_and(|project_id| !project_id.trim().is_empty())
}

fn maybe_warn_api_key_override(base: &BaseArgs) {
    if base.json || !std::io::stderr().is_terminal() {
        return;
    }
    if resolve_api_key_override(base).is_none() {
        return;
    }

    let ignored_profile = base
        .profile
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty());

    if let Some(profile_name) = ignored_profile {
        eprintln!(
            "Info: using --api-key/BRAINTRUST_API_KEY credentials; selected profile '{profile_name}' is ignored for this command. Use --prefer-profile or unset BRAINTRUST_API_KEY to use a profile with OAuth login.",
        );
    }
}

fn has_explicit_profile_selection(base: &BaseArgs) -> bool {
    base.profile_explicit
        && base
            .profile
            .as_deref()
            .is_some_and(|value| !value.trim().is_empty())
}

fn resolve_api_key_override(base: &BaseArgs) -> Option<String> {
    if (base.prefer_profile || has_explicit_profile_selection(base))
        && !matches!(
            base.api_key_source,
            Some(crate::args::ArgValueSource::CommandLine)
        )
    {
        return None;
    }
    let value = base.api_key.as_deref()?.trim();
    if value.is_empty() {
        return None;
    }
    Some(value.to_string())
}

pub async fn resolve_auth(base: &BaseArgs) -> Result<ResolvedAuth> {
    let mut store = load_auth_store()?;
    let mut auth_base = base.clone();
    let cfg_org = if trimmed(base.profile.as_deref()).is_none()
        && trimmed(base.org_name.as_deref()).is_none()
    {
        let cfg = crate::config::load().unwrap_or_default();
        auth_base.profile = cfg.profile.and_then(|value| {
            let trimmed = value.trim();
            (!trimmed.is_empty()).then(|| trimmed.to_string())
        });
        auth_base.profile.is_none().then_some(cfg.org).flatten()
    } else {
        None
    };

    let mut auth = resolve_auth_from_store_with_secret_lookup(
        &auth_base,
        &store,
        load_profile_secret,
        &cfg_org,
    )?;
    if !auth.is_oauth {
        return Ok(auth);
    }

    let effective_org = auth_base.org_name.as_deref().or(cfg_org.as_deref());
    let profile_name = auth_base
        .profile
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .or_else(|| effective_org.and_then(|org| resolve_profile_for_org(org, &store)))
        .or_else(|| {
            (store.profiles.len() == 1).then(|| store.profiles.keys().next().unwrap().as_str())
        })
        .ok_or_else(|| {
            recoverable_auth_error(
                RecoverableAuthErrorKind::OauthProfileSelection,
                "oauth profile requested but none selected".to_string(),
            )
        })?
        .to_string();
    let profile = store
        .profiles
        .get(profile_name.as_str())
        .ok_or_else(|| anyhow::anyhow!("profile '{profile_name}' not found"))?;
    let client_id = profile.oauth_client_id.as_deref().ok_or_else(|| {
        recoverable_auth_error(
            RecoverableAuthErrorKind::OauthClientId,
            format!(
                "oauth profile '{profile_name}' is missing client_id; re-run `bt auth login --oauth --profile {profile_name}`"
            ),
        )
    })?;
    let cached_expires_at = profile.oauth_access_expires_at;
    let api_url = auth
        .api_url
        .clone()
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());

    if let Some(cached_access_token) =
        load_valid_cached_oauth_access_token(&profile_name, cached_expires_at)?
    {
        auth.api_key = Some(cached_access_token);
        return Ok(auth);
    }

    let refresh_token = load_profile_oauth_refresh_token(&profile_name)?.ok_or_else(|| {
        recoverable_auth_error(
            RecoverableAuthErrorKind::OauthRefreshToken,
            format!(
                "oauth refresh token missing for profile '{profile_name}'; re-run `bt auth login --oauth --profile {profile_name}`"
            ),
        )
    })?;
    let refreshed =
        refresh_oauth_access_token(&api_url, &refresh_token, client_id, &profile_name).await?;
    save_profile_oauth_access_token(&profile_name, &refreshed.access_token)?;
    if let Some(next_refresh_token) = refreshed.refresh_token.as_ref() {
        if next_refresh_token != &refresh_token {
            save_profile_oauth_refresh_token(&profile_name, next_refresh_token)?;
        }
    }
    if let Some(profile) = store.profiles.get_mut(&profile_name) {
        profile.oauth_access_expires_at = determine_oauth_access_expiry_epoch(&refreshed);
    }
    save_auth_store(&store)?;
    auth.api_key = Some(refreshed.access_token);
    Ok(auth)
}

pub async fn resolved_auth_env(base: &BaseArgs) -> Result<Vec<(String, String)>> {
    let auth = resolve_auth(base).await?;
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

fn resolve_profile_for_org<'a>(org: &str, store: &'a AuthStore) -> Option<&'a str> {
    if store.profiles.contains_key(org) {
        return Some(
            store
                .profiles
                .keys()
                .find(|k| k.as_str() == org)
                .map(|k| k.as_str())
                .unwrap(),
        );
    }

    let matches: Vec<&str> = store
        .profiles
        .iter()
        .filter(|(_, p)| p.org_name.as_deref() == Some(org))
        .map(|(name, _)| name.as_str())
        .collect();

    match matches.len() {
        0 => None,
        1 => Some(matches[0]),
        _ => None,
    }
}

fn resolve_auth_from_store_with_secret_lookup<F>(
    base: &BaseArgs,
    store: &AuthStore,
    load_secret: F,
    cfg_org: &Option<String>,
) -> Result<ResolvedAuth>
where
    F: Fn(&str) -> Result<Option<String>>,
{
    if let Some(api_key) = resolve_api_key_override(base) {
        return Ok(ResolvedAuth {
            api_key: Some(api_key),
            api_url: base.api_url.clone(),
            app_url: base.app_url.clone(),
            org_name: base.org_name.clone(),
            is_oauth: false,
        });
    }

    let requested_profile = base
        .profile
        .as_ref()
        .map(|s| s.trim())
        .filter(|s| !s.is_empty());

    let effective_org = base.org_name.as_deref().or(cfg_org.as_deref());

    let selected_profile_name = if let Some(profile) = requested_profile {
        Some(profile)
    } else if let Some(org) = effective_org {
        resolve_profile_for_org(org, store)
    } else if store.profiles.len() == 1 {
        store.profiles.keys().next().map(|k| k.as_str())
    } else {
        None
    };

    if let Some(profile_name) = selected_profile_name {
        let profile = store.profiles.get(profile_name).ok_or_else(|| {
            anyhow::anyhow!(
                "profile '{profile_name}' not found; run `bt auth profiles` or `bt auth login --profile {profile_name}`"
            )
        })?;
        let is_oauth = profile.auth_kind == AuthKind::Oauth;
        let api_key = if is_oauth {
            None
        } else {
            Some(load_secret(profile_name)?.ok_or_else(|| {
                recoverable_auth_error(
                    RecoverableAuthErrorKind::StoredCredential,
                    format!(
                        "no keychain credential found for profile '{profile_name}'; re-run `bt auth login --profile {profile_name}`"
                    ),
                )
            })?)
        };

        return Ok(ResolvedAuth {
            api_key,
            api_url: base.api_url.clone().or_else(|| profile.api_url.clone()),
            app_url: base.app_url.clone().or_else(|| profile.app_url.clone()),
            org_name: base.org_name.clone().or_else(|| profile.org_name.clone()),
            is_oauth,
        });
    }

    Ok(ResolvedAuth {
        api_key: None,
        api_url: base.api_url.clone(),
        app_url: base.app_url.clone(),
        org_name: base.org_name.clone().or_else(|| cfg_org.clone()),
        is_oauth: false,
    })
}

fn trimmed(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

async fn run_login_set(base: &BaseArgs, args: AuthLoginArgs) -> Result<()> {
    if args.oauth {
        return run_login_oauth(base, args).await;
    }

    let has_explicit_api_key = base.api_key.as_ref().is_some_and(|k| !k.trim().is_empty());
    if !has_explicit_api_key && ui::can_prompt() {
        let methods = ["OAuth (browser)", "API key"];
        let selected = ui::fuzzy_select("Select login method", &methods, 0)?;
        if selected == 0 {
            return run_login_oauth(base, args).await;
        }
    }

    let interactive = ui::can_prompt();

    let api_key = match base.api_key.clone() {
        Some(value) if !value.trim().is_empty() => value,
        Some(_) => bail!("api key cannot be empty"),
        None => prompt_api_key()?,
    };

    let login_app_url = base
        .app_url
        .clone()
        .unwrap_or_else(|| DEFAULT_APP_URL.to_string());
    let login_orgs = fetch_login_orgs(&api_key, &login_app_url).await?;
    let requested_org_resolution = resolve_requested_org_for_api_key_login(
        &login_orgs,
        base.org_name.as_deref(),
        ui::can_prompt(),
        prompt_for_auth_method_for_missing_requested_org,
    )?;
    if requested_org_resolution == RequestedOrgResolution::SwitchToOauth {
        return run_login_oauth(base, args).await;
    }
    let selected_org = select_login_org(
        login_orgs.clone(),
        match requested_org_resolution {
            RequestedOrgResolution::UseRequestedOrg => base.org_name.as_deref(),
            RequestedOrgResolution::NoRequestedOrg | RequestedOrgResolution::IgnoreRequestedOrg => {
                None
            }
            RequestedOrgResolution::SwitchToOauth => unreachable!("handled above"),
        },
        interactive,
        base.verbose,
        true,
        explicitly_quiet(base),
    )?;
    let selected_api_url =
        resolve_profile_api_url(base.api_url.clone(), selected_org.as_ref(), &login_orgs)?;
    let store = load_auth_store()?;
    let (profile_name, should_confirm_overwrite) = resolve_api_key_login_profile_name(
        base.profile.as_deref(),
        selected_org.as_ref().map(|org| org.name.as_str()),
        &selected_api_url,
        &store,
    )?;
    if should_confirm_overwrite {
        confirm_profile_overwrite(&profile_name)?;
    }

    commit_api_key_profile(
        &profile_name,
        &api_key,
        selected_api_url.clone(),
        base.app_url.clone(),
        selected_org.as_ref().map(|org| org.name.clone()),
    )?;

    ui::print_command_status(
        ui::CommandStatus::Success,
        &format_login_success(&selected_org, &profile_name, &selected_api_url),
    );
    Ok(())
}

async fn run_login_oauth(base: &BaseArgs, args: AuthLoginArgs) -> Result<()> {
    let api_url = base
        .api_url
        .clone()
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());
    let app_url = base
        .app_url
        .clone()
        .unwrap_or_else(|| DEFAULT_APP_URL.to_string());
    let provisional_profile = base
        .profile
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or("default");
    let client_id = args
        .client_id
        .clone()
        .unwrap_or_else(|| default_oauth_client_id(provisional_profile));

    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let state = generate_random_token(32)?;

    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .context("failed to bind oauth callback listener")?;
    let callback_port = listener
        .local_addr()
        .context("failed to read callback listener address")?
        .port();
    let redirect_uri = format!("http://127.0.0.1:{callback_port}/callback");
    let oauth_client = build_oauth_client(&api_url, &client_id, Some(&redirect_uri))?;
    let (authorize_url, _) = oauth_client
        .authorize_url(|| CsrfToken::new(state.clone()))
        .add_scope(Scope::new(OAUTH_SCOPE.to_string()))
        .set_pkce_challenge(pkce_challenge)
        .url();
    let authorize_url = authorize_url.to_string();

    eprintln!("Opening browser for OAuth authorization...");
    eprintln!("If it does not open, visit:\n{authorize_url}");
    if !args.no_browser {
        if let Err(err) = open::that(&authorize_url) {
            eprintln!("warning: failed to open browser automatically: {err}");
        }
    }

    let callback = collect_oauth_callback(
        listener,
        args.no_browser || is_ssh_session(),
        explicitly_quiet(base),
    )
    .await?;
    if let Some(error) = callback.error {
        bail!("oauth authorization failed: {error}");
    }
    let auth_code = callback
        .code
        .ok_or_else(|| anyhow::anyhow!("no authorization code received"))?;
    if callback.state.is_none() {
        bail!("oauth callback missing state; paste the full callback URL (or code=...&state=...)");
    }
    if callback.state.as_deref() != Some(state.as_str()) {
        bail!("oauth state mismatch; please try again");
    }

    let oauth_tokens = exchange_oauth_authorization_code(
        &api_url,
        &client_id,
        &redirect_uri,
        &auth_code,
        pkce_verifier,
    )
    .await?;
    let login_orgs = fetch_login_orgs(&oauth_tokens.access_token, &app_url).await?;
    let selected_org = select_login_org(
        login_orgs.clone(),
        base.org_name.as_deref(),
        ui::can_prompt(),
        base.verbose,
        true,
        explicitly_quiet(base),
    )?;
    let selected_api_url =
        resolve_profile_api_url(base.api_url.clone(), selected_org.as_ref(), &login_orgs)?;
    let store = load_auth_store()?;
    let jwt_id = decode_jwt_identity(&oauth_tokens.access_token);
    let (profile_name, should_confirm_overwrite) = resolve_oauth_login_profile_name(
        base.profile.as_deref(),
        selected_org.as_ref().map(|org| org.name.as_str()),
        &selected_api_url,
        &app_url,
        &jwt_id,
        &store,
    )?;
    if should_confirm_overwrite {
        confirm_profile_overwrite(&profile_name)?;
    }

    commit_oauth_profile(
        &profile_name,
        &oauth_tokens,
        selected_api_url.clone(),
        app_url.clone(),
        client_id.clone(),
        selected_org.as_ref().map(|org| org.name.clone()),
    )?;

    ui::print_command_status(
        ui::CommandStatus::Success,
        &format_login_success(&selected_org, &profile_name, &selected_api_url),
    );

    Ok(())
}

pub(crate) async fn login_interactive_oauth(base: &mut BaseArgs) -> Result<String> {
    let api_url = base
        .api_url
        .clone()
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());
    let app_url = base
        .app_url
        .clone()
        .unwrap_or_else(|| DEFAULT_APP_URL.to_string());
    let provisional_profile = base
        .profile
        .as_deref()
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or("default");
    let client_id = default_oauth_client_id(provisional_profile);

    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let state = generate_random_token(32)?;

    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .context("failed to bind oauth callback listener")?;
    let callback_port = listener
        .local_addr()
        .context("failed to read callback listener address")?
        .port();
    let redirect_uri = format!("http://127.0.0.1:{callback_port}/callback");
    let oauth_client = build_oauth_client(&api_url, &client_id, Some(&redirect_uri))?;
    let (authorize_url, _) = oauth_client
        .authorize_url(|| CsrfToken::new(state.clone()))
        .add_scope(Scope::new(OAUTH_SCOPE.to_string()))
        .set_pkce_challenge(pkce_challenge)
        .url();
    let authorize_url = authorize_url.to_string();

    let _ = open::that(&authorize_url);
    eprintln!("Complete authorization in your browser.");
    eprintln!();
    eprintln!("{}", dialoguer::console::style(&authorize_url).dim());
    eprintln!();

    let callback =
        collect_oauth_callback(listener, is_ssh_session(), explicitly_quiet(base)).await?;
    if let Some(error) = callback.error {
        bail!("oauth authorization failed: {error}");
    }
    let auth_code = callback
        .code
        .ok_or_else(|| anyhow::anyhow!("no authorization code received"))?;
    if callback.state.is_none() {
        bail!("oauth callback missing state; paste the full callback URL (or code=...&state=...)");
    }
    if callback.state.as_deref() != Some(state.as_str()) {
        bail!("oauth state mismatch; please try again");
    }

    let oauth_tokens = exchange_oauth_authorization_code(
        &api_url,
        &client_id,
        &redirect_uri,
        &auth_code,
        pkce_verifier,
    )
    .await?;

    let login_orgs = fetch_login_orgs(&oauth_tokens.access_token, &app_url).await?;
    let selected_org = select_login_org(
        login_orgs.clone(),
        base.org_name.as_deref(),
        ui::can_prompt(),
        false,
        false,
        explicitly_quiet(base),
    )?;
    let selected_api_url =
        resolve_profile_api_url(base.api_url.clone(), selected_org.as_ref(), &login_orgs)?;
    let store = load_auth_store()?;
    let jwt_id = decode_jwt_identity(&oauth_tokens.access_token);
    let (profile_name, should_confirm_overwrite) = resolve_oauth_login_profile_name(
        base.profile.as_deref(),
        selected_org.as_ref().map(|org| org.name.as_str()),
        &selected_api_url,
        &app_url,
        &jwt_id,
        &store,
    )?;
    if should_confirm_overwrite {
        confirm_profile_overwrite(&profile_name)?;
    }

    commit_oauth_profile(
        &profile_name,
        &oauth_tokens,
        selected_api_url,
        app_url,
        client_id,
        selected_org.as_ref().map(|org| org.name.clone()),
    )?;

    base.profile = Some(profile_name.clone());
    Ok(profile_name)
}

pub(crate) fn commit_api_key_profile(
    profile_name: &str,
    api_key: &str,
    api_url: String,
    app_url: Option<String>,
    org_name: Option<String>,
) -> Result<()> {
    save_profile_secret(profile_name, api_key)?;
    let _ = delete_profile_oauth_refresh_token(profile_name);
    let _ = delete_profile_oauth_access_token(profile_name);

    let mut store = load_auth_store()?;
    store.profiles.insert(
        profile_name.to_string(),
        AuthProfile {
            auth_kind: AuthKind::ApiKey,
            api_url: Some(api_url),
            app_url,
            org_name,
            oauth_client_id: None,
            oauth_access_expires_at: None,
            user_name: None,
            email: None,
            api_key_hint: Some(obscure_api_key(api_key)),
        },
    );
    save_auth_store(&store)
}

fn commit_oauth_profile(
    profile_name: &str,
    tokens: &OAuthTokenResponse,
    api_url: String,
    app_url: String,
    client_id: String,
    org_name: Option<String>,
) -> Result<()> {
    let refresh_token = tokens.refresh_token.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "oauth token response did not include a refresh_token; cannot create persistent oauth profile"
        )
    })?;
    save_profile_oauth_refresh_token(profile_name, refresh_token)?;
    save_profile_oauth_access_token(profile_name, &tokens.access_token)?;
    let _ = delete_profile_secret(profile_name);

    let oauth_access_expires_at = determine_oauth_access_expiry_epoch(tokens);
    let jwt_id = decode_jwt_identity(&tokens.access_token);

    let mut store = load_auth_store()?;
    store.profiles.insert(
        profile_name.to_string(),
        AuthProfile {
            auth_kind: AuthKind::Oauth,
            api_url: Some(api_url),
            app_url: Some(app_url),
            org_name,
            oauth_client_id: Some(client_id),
            oauth_access_expires_at,
            user_name: jwt_id.name,
            email: jwt_id.email,
            api_key_hint: None,
        },
    );
    save_auth_store(&store)
}

async fn run_login_refresh(base: &BaseArgs) -> Result<()> {
    let mut store = load_auth_store()?;
    let (profile_name, source) = resolve_selected_profile_name_for_debug(base, &store)?;
    let profile = store.profiles.get(profile_name.as_str()).ok_or_else(|| {
        anyhow::anyhow!(
            "profile '{profile_name}' not found; run `bt auth profiles` to see available profiles"
        )
    })?;
    if profile.auth_kind != AuthKind::Oauth {
        bail!(
            "profile '{profile_name}' uses api key auth; `bt auth refresh` only applies to oauth profiles"
        );
    }

    let api_url = profile
        .api_url
        .clone()
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());
    let client_id = profile.oauth_client_id.clone().ok_or_else(|| {
        anyhow::anyhow!(
            "oauth profile '{profile_name}' is missing client_id; re-run `bt auth login --oauth --profile {profile_name}`"
        )
    })?;
    let previous_expires_at = profile.oauth_access_expires_at;
    let refresh_token = load_profile_oauth_refresh_token(profile_name.as_str())?.ok_or_else(|| {
        anyhow::anyhow!(
            "oauth refresh token missing for profile '{profile_name}'; re-run `bt auth login --oauth --profile {profile_name}`"
        )
    })?;

    println!(
        "Refreshing OAuth token for profile '{profile_name}' (source: {source}, api_url: {api_url})"
    );
    if let Some(expires_at) = previous_expires_at {
        let now = current_unix_timestamp();
        let remaining = expires_at.saturating_sub(now);
        println!(
            "Cached access token expiry before refresh: {expires_at} (about {remaining}s remaining)"
        );
    } else {
        println!("Cached access token expiry before refresh: unknown");
    }

    let refreshed =
        refresh_oauth_access_token(&api_url, &refresh_token, &client_id, profile_name.as_str())
            .await?;
    save_profile_oauth_access_token(profile_name.as_str(), &refreshed.access_token)?;
    let mut refresh_rotated = false;
    if let Some(next_refresh_token) = refreshed.refresh_token.as_ref() {
        if next_refresh_token != &refresh_token {
            save_profile_oauth_refresh_token(profile_name.as_str(), next_refresh_token)?;
            refresh_rotated = true;
        }
    }

    let new_expires_at = determine_oauth_access_expiry_epoch(&refreshed);
    if let Some(profile) = store.profiles.get_mut(profile_name.as_str()) {
        profile.oauth_access_expires_at = new_expires_at;
    }
    save_auth_store(&store)?;

    if let Some(expires_at) = new_expires_at {
        let now = current_unix_timestamp();
        let remaining = expires_at.saturating_sub(now);
        println!("New access token expiry: {expires_at} (about {remaining}s remaining)");
    } else {
        println!("New access token expiry: unknown");
    }
    if refresh_rotated {
        println!("Refresh token rotation: yes");
    } else {
        println!("Refresh token rotation: no");
    }
    println!("OAuth refresh complete.");

    Ok(())
}

fn resolve_selected_profile_name_for_debug(
    base: &BaseArgs,
    store: &AuthStore,
) -> Result<(String, &'static str)> {
    if let Some(profile_name) = base.profile.as_deref() {
        let profile_name = profile_name.trim();
        if !profile_name.is_empty() {
            return Ok((profile_name.to_string(), "--profile/BRAINTRUST_PROFILE"));
        }
    }

    if let Some(org) = base.org_name.as_deref() {
        if let Some(profile_name) = resolve_profile_for_org(org, store) {
            return Ok((profile_name.to_string(), "org-based resolution"));
        }
    }

    if store.profiles.len() == 1 {
        let name = store.profiles.keys().next().unwrap().clone();
        return Ok((name, "only profile"));
    }

    if store.profiles.len() > 1 && ui::can_prompt() {
        if let Some(name) = select_profile_interactive(None)? {
            return Ok((name, "interactive selection"));
        }
    }

    bail!("no profile selected; pass --profile <NAME>, set BRAINTRUST_PROFILE, or configure an org")
}

fn resolve_profile_name(
    explicit_profile: Option<&str>,
    suggested_org_name: Option<&str>,
) -> Result<String> {
    if let Some(profile) = explicit_profile {
        let profile = profile.trim();
        if profile.is_empty() {
            bail!("profile name cannot be empty");
        }
        return Ok(profile.to_string());
    }

    Ok(suggested_org_name
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or("profile")
        .to_string())
}

fn default_profile_name(suggested_org_name: Option<&str>) -> String {
    suggested_org_name
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or("profile")
        .to_string()
}

fn next_available_profile_name(base_name: &str, store: &AuthStore) -> String {
    if !store.profiles.contains_key(base_name) {
        return base_name.to_string();
    }

    (2u32..)
        .map(|idx| format!("{base_name}-{idx}"))
        .find(|candidate| !store.profiles.contains_key(candidate))
        .expect("profile name sequence is infinite")
}

fn resolve_api_key_login_profile_name(
    explicit_profile: Option<&str>,
    suggested_org_name: Option<&str>,
    selected_api_url: &str,
    store: &AuthStore,
) -> Result<(String, bool)> {
    if let Some(profile_name) = explicit_profile {
        let profile_name = resolve_profile_name(Some(profile_name), suggested_org_name)?;
        return Ok((
            profile_name.clone(),
            store.profiles.contains_key(&profile_name),
        ));
    }

    let default_name = default_profile_name(suggested_org_name);
    let has_matching_api_key_profile = store.profiles.values().any(|profile| {
        profile.auth_kind == AuthKind::ApiKey
            && profile.api_url.as_deref() == Some(selected_api_url)
            && profile.org_name.as_deref() == suggested_org_name
    });

    if has_matching_api_key_profile {
        return Ok((next_available_profile_name(&default_name, store), false));
    }

    Ok((
        default_name.clone(),
        store.profiles.contains_key(&default_name),
    ))
}

fn resolve_oauth_login_profile_name(
    explicit_profile: Option<&str>,
    suggested_org_name: Option<&str>,
    selected_api_url: &str,
    app_url: &str,
    jwt_id: &JwtIdentity,
    store: &AuthStore,
) -> Result<(String, bool)> {
    if let Some(profile_name) = explicit_profile {
        let profile_name = resolve_profile_name(Some(profile_name), suggested_org_name)?;
        return Ok((
            profile_name.clone(),
            store.profiles.contains_key(&profile_name),
        ));
    }

    let matched_profile = store
        .profiles
        .iter()
        .filter(|(_, profile)| {
            profile.auth_kind == AuthKind::Oauth
                && profile.api_url.as_deref() == Some(selected_api_url)
                && profile.app_url.as_deref() == Some(app_url)
                && profile.org_name.as_deref() == suggested_org_name
                && profile.user_name == jwt_id.name
                && profile.email == jwt_id.email
        })
        .max_by(|(left_name, left), (right_name, right)| {
            left.oauth_access_expires_at
                .unwrap_or_default()
                .cmp(&right.oauth_access_expires_at.unwrap_or_default())
                .then_with(|| left_name.cmp(right_name))
        })
        .map(|(name, _)| name.clone());

    if let Some(profile_name) = matched_profile {
        return Ok((profile_name, false));
    }

    let default_name = default_profile_name(suggested_org_name);
    Ok((
        default_name.clone(),
        store.profiles.contains_key(&default_name),
    ))
}

fn confirm_profile_overwrite(profile_name: &str) -> Result<()> {
    let store = load_auth_store()?;
    if !store.profiles.contains_key(profile_name) {
        return Ok(());
    }
    let Some(term) = ui::prompt_term() else {
        return Ok(());
    };
    let confirmed = Confirm::new()
        .with_prompt(format!(
            "Profile '{profile_name}' already exists. Overwrite?"
        ))
        .default(false)
        .interact_on(&term)?;
    if !confirmed {
        bail!("login cancelled");
    }
    Ok(())
}

fn format_login_success(
    selected_org: &Option<LoginOrgInfo>,
    profile_name: &str,
    api_url: &str,
) -> String {
    match selected_org.as_ref() {
        Some(org) => format!(
            "Logged in as {} (profile: {profile_name}, api: {api_url})",
            org.name
        ),
        None => format!("Logged in (cross-org, profile: {profile_name}, api: {api_url})"),
    }
}

async fn run_profiles(base: &BaseArgs, _args: AuthProfilesArgs) -> Result<()> {
    let store = load_auth_store()?;
    if store.profiles.is_empty() {
        println!("No saved profiles. Run `bt auth login` to create one.");
        return Ok(());
    }

    let verifications = verify_all_profiles_from_store(&store).await;
    let all_network_errors = verifications
        .iter()
        .all(|v| v.status == "error" && !v.error.as_deref().unwrap_or("").contains("invalid"));
    if all_network_errors {
        eprintln!("Could not reach Braintrust API. Showing saved profiles:");
        print_saved_profiles(&store, base.json)?;
        return Ok(());
    }

    if base.json {
        println!("{}", serde_json::to_string(&verifications)?);
        return Ok(());
    }

    for v in &verifications {
        let cmd_status = match v.status.as_str() {
            "ok" => crate::ui::CommandStatus::Success,
            "expired" => crate::ui::CommandStatus::Warning,
            _ => crate::ui::CommandStatus::Error,
        };
        crate::ui::print_command_status(cmd_status, &format_verification_line(v));
    }

    if base.verbose {
        if let Ok(path) = auth_store_path() {
            eprintln!("\nCredentials: {}", path.display());
        }
    }

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
            "profile '{profile_name}' not found; run `bt auth profiles` to see available profiles"
        );
    }

    if !force {
        if let Some(term) = ui::prompt_term() {
            let confirmed = Confirm::new()
                .with_prompt(format!("Delete profile '{profile_name}'?"))
                .default(false)
                .interact_on(&term)?;
            if !confirmed {
                eprintln!("Cancelled");
                return Ok(());
            }
        }
    }

    store.profiles.remove(profile_name);
    save_auth_store(&store)?;
    if let Err(err) = delete_profile_secret(profile_name) {
        eprintln!("warning: failed to delete keychain credential for '{profile_name}': {err}");
    }
    if let Err(err) = delete_profile_oauth_refresh_token(profile_name) {
        eprintln!("warning: failed to delete oauth refresh token for '{profile_name}': {err}");
    }
    if let Err(err) = delete_profile_oauth_access_token(profile_name) {
        eprintln!("warning: failed to delete oauth access token for '{profile_name}': {err}");
    }

    ui::print_command_status(
        ui::CommandStatus::Success,
        &format!("Deleted profile '{profile_name}'"),
    );
    Ok(())
}

fn run_login_logout(base: BaseArgs, args: AuthLogoutArgs) -> Result<()> {
    let store = load_auth_store()?;
    if store.profiles.is_empty() {
        println!("No saved profiles.");
        return Ok(());
    }

    let profile_name = if let Some(p) = args.profile.or(base.profile) {
        let p = p.trim().to_string();
        if !store.profiles.contains_key(&p) {
            bail!("profile '{p}' not found; run `bt auth profiles` to see available profiles");
        }
        p
    } else if store.profiles.len() == 1 {
        store.profiles.keys().next().unwrap().clone()
    } else if ui::can_prompt() {
        let names: Vec<&str> = store.profiles.keys().map(|k| k.as_str()).collect();
        let idx = crate::ui::fuzzy_select("Select profile to log out", &names, 0)?;
        names[idx].to_string()
    } else {
        bail!("multiple profiles exist. Use --profile <NAME> to specify which one.");
    };

    run_login_delete(&profile_name, args.force)
}

enum ProfileStatus {
    Ok,
    Expired,
    Missing,
    Error(String),
}

enum CredentialLoad {
    Found(String),
    Missing,
    Expired,
    Error(String),
}

fn load_credential_for_profile(name: &str, profile: &AuthProfile) -> CredentialLoad {
    match profile.auth_kind {
        AuthKind::ApiKey => match load_profile_secret(name) {
            Ok(Some(k)) => CredentialLoad::Found(k),
            Ok(None) => CredentialLoad::Missing,
            Err(e) => CredentialLoad::Error(e.to_string()),
        },
        AuthKind::Oauth => {
            if let Some(ts) = profile.oauth_access_expires_at {
                if !oauth_access_token_is_fresh(ts) {
                    return CredentialLoad::Expired;
                }
            }
            match load_profile_oauth_access_token(name) {
                Ok(Some(k)) => CredentialLoad::Found(k),
                Ok(None) => CredentialLoad::Missing,
                Err(e) => CredentialLoad::Error(e.to_string()),
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileVerification {
    pub name: String,
    pub auth: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_email: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub api_key_hint: Option<String>,
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

fn build_verification(
    name: &str,
    auth_kind: &str,
    org: Option<String>,
    jwt_id: Option<JwtIdentity>,
    api_key_hint: Option<String>,
    status: ProfileStatus,
) -> ProfileVerification {
    let (status_str, error) = match &status {
        ProfileStatus::Ok => ("ok", None),
        ProfileStatus::Expired => ("expired", None),
        ProfileStatus::Missing => ("missing", None),
        ProfileStatus::Error(msg) => ("error", Some(msg.clone())),
    };
    ProfileVerification {
        name: name.to_string(),
        auth: auth_kind.to_string(),
        org,
        user_name: jwt_id.as_ref().and_then(|j| j.name.clone()),
        user_email: jwt_id.as_ref().and_then(|j| j.email.clone()),
        api_key_hint,
        status: status_str.to_string(),
        error,
    }
}

async fn verify_profile_full(name: &str, profile: &AuthProfile) -> ProfileVerification {
    let app_url = profile.app_url.as_deref().unwrap_or(DEFAULT_APP_URL);
    let auth_kind = match profile.auth_kind {
        AuthKind::ApiKey => "api_key",
        AuthKind::Oauth => "oauth",
    };
    let mk = |status, jwt_id: Option<JwtIdentity>, hint: Option<String>| {
        build_verification(
            name,
            auth_kind,
            profile.org_name.clone(),
            jwt_id,
            hint,
            status,
        )
    };

    let credential = match load_credential_for_profile(name, profile) {
        CredentialLoad::Found(k) => k,
        CredentialLoad::Missing => return mk(ProfileStatus::Missing, None, None),
        CredentialLoad::Expired => return mk(ProfileStatus::Expired, None, None),
        CredentialLoad::Error(e) => return mk(ProfileStatus::Error(e), None, None),
    };

    let (jwt_id, hint) = match profile.auth_kind {
        AuthKind::Oauth => (Some(decode_jwt_identity(&credential)), None),
        AuthKind::ApiKey => (None, profile.api_key_hint.clone()),
    };

    match fetch_login_orgs(&credential, app_url).await {
        Ok(_) => mk(ProfileStatus::Ok, jwt_id, hint),
        Err(e) => {
            let msg = e.to_string();
            let status = if msg.contains("401") || msg.contains("Unauthorized") {
                if profile.auth_kind == AuthKind::Oauth {
                    ProfileStatus::Expired
                } else {
                    ProfileStatus::Error("invalid API key".to_string())
                }
            } else {
                ProfileStatus::Error(msg)
            };
            mk(status, None, None)
        }
    }
}

async fn verify_all_profiles_from_store(store: &AuthStore) -> Vec<ProfileVerification> {
    let mut set = tokio::task::JoinSet::new();
    for (name, profile) in store.profiles.iter() {
        let name = name.clone();
        let profile = profile.clone();
        set.spawn(async move { verify_profile_full(&name, &profile).await });
    }

    let mut results = Vec::new();
    while let Some(res) = set.join_next().await {
        if let Ok(v) = res {
            results.push(v);
        }
    }
    results.sort_by(|a, b| a.name.cmp(&b.name));
    results
}

fn format_verification_line(v: &ProfileVerification) -> String {
    let mut parts = vec![v.name.clone(), v.auth.clone()];
    if let Some(ref org) = v.org {
        parts.push(format!("org: {org}"));
    }
    match v.status.as_str() {
        "ok" => {
            let id = match (&v.user_name, &v.user_email) {
                (Some(name), Some(email)) => Some(format!("{name} ({email})")),
                (None, Some(email)) => Some(email.clone()),
                _ => v.api_key_hint.clone(),
            };
            if let Some(id) = id {
                parts.push(id);
            }
        }
        "expired" => parts.push("token expired".into()),
        "missing" => parts.push("credential missing".into()),
        _ => {
            if let Some(ref e) = v.error {
                parts.push(e.clone());
            }
        }
    }
    parts.join(" — ")
}

fn print_saved_profiles(store: &AuthStore, json: bool) -> Result<()> {
    if json {
        let output: Vec<serde_json::Value> = store
            .profiles
            .iter()
            .map(|(name, p)| {
                serde_json::json!({
                    "name": name,
                    "auth": match p.auth_kind { AuthKind::ApiKey => "api_key", AuthKind::Oauth => "oauth" },
                    "org": p.org_name,
                    "user_name": p.user_name,
                    "user_email": p.email,
                    "api_key_hint": p.api_key_hint,
                    "status": "unchecked"
                })
            })
            .collect();
        println!("{}", serde_json::to_string(&output)?);
    } else {
        for (name, profile) in &store.profiles {
            let kind = match profile.auth_kind {
                AuthKind::ApiKey => "api_key",
                AuthKind::Oauth => "oauth",
            };
            let org = profile
                .org_name
                .as_deref()
                .map(|o| format!(" org={o}"))
                .unwrap_or_default();
            let id = match (profile.user_name.as_deref(), profile.email.as_deref()) {
                (Some(n), Some(e)) => format!(" {n} ({e})"),
                (None, Some(e)) => format!(" {e}"),
                _ => profile
                    .api_key_hint
                    .as_deref()
                    .map(|h| format!(" {h}"))
                    .unwrap_or_default(),
            };
            println!("  {name} {kind}{org}{id}");
        }
    }
    Ok(())
}

async fn fetch_login_orgs(api_key: &str, app_url: &str) -> Result<Vec<LoginOrgInfo>> {
    let login_url = format!("{}/api/apikey/login", app_url.trim_end_matches('/'));
    let client = build_http_client(crate::http::DEFAULT_HTTP_TIMEOUT)
        .context("failed to initialize HTTP client")?;
    let response = client
        .post(&login_url)
        .bearer_auth(api_key)
        .header("Content-Type", "application/json")
        .send()
        .await
        .with_context(|| format!("failed to call login endpoint {login_url}"))?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(crate::http::HttpError { status, body }.into());
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
    mut orgs: Vec<LoginOrgInfo>,
    requested_org_name: Option<&str>,
    interactive: bool,
    verbose: bool,
    allow_cross_org: bool,
    quiet_requested: bool,
) -> Result<Option<LoginOrgInfo>> {
    if orgs.is_empty() {
        bail!("no organizations found for this credential");
    }
    orgs.sort_by(|a, b| {
        a.name
            .to_ascii_lowercase()
            .cmp(&b.name.to_ascii_lowercase())
            .then_with(|| a.name.cmp(&b.name))
    });

    if let Some(name) = requested_org_name {
        return find_login_org(&orgs, name)
            .cloned()
            .map(Some)
            .ok_or_else(|| missing_requested_org_error(&orgs, name));
    }

    if orgs.len() == 1 {
        return Ok(Some(orgs.into_iter().next().expect("org exists")));
    }

    if !interactive {
        return Ok(None);
    }

    let offset = if allow_cross_org { 1 } else { 0 };
    let mut labels: Vec<String> = Vec::new();
    if allow_cross_org {
        labels.push(
            "No default org (cross-org mode; pass --org or BRAINTRUST_ORG_NAME when needed)"
                .to_string(),
        );
    }
    labels.extend(orgs.iter().map(|org| {
        if verbose {
            let api_url = org.api_url.as_deref().unwrap_or(DEFAULT_API_URL);
            format!("{} [{}] ({})", org.name, org.id, api_url)
        } else {
            org.name.clone()
        }
    }));
    let label_refs: Vec<&str> = labels.iter().map(String::as_str).collect();
    if !quiet_requested {
        eprintln!("\n\nA Braintrust organization is usually a team or a company.");
    }
    let selection = ui::fuzzy_select("Select organization", &label_refs, 0)?;
    if allow_cross_org && selection == 0 {
        return Ok(None);
    }

    Ok(Some(
        orgs.into_iter()
            .nth(selection - offset)
            .expect("selected index should be in range"),
    ))
}

fn find_login_org<'a>(
    orgs: &'a [LoginOrgInfo],
    requested_org_name: &str,
) -> Option<&'a LoginOrgInfo> {
    orgs.iter()
        .find(|org| org.name == requested_org_name)
        .or_else(|| {
            let lowered = requested_org_name.to_ascii_lowercase();
            orgs.iter()
                .find(|org| org.name.to_ascii_lowercase() == lowered)
        })
}

fn missing_requested_org_error(orgs: &[LoginOrgInfo], requested_org_name: &str) -> anyhow::Error {
    let available = orgs
        .iter()
        .map(|org| org.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    anyhow::anyhow!("org '{requested_org_name}' not found. Available: {available}")
}

fn resolve_requested_org_for_api_key_login<F>(
    orgs: &[LoginOrgInfo],
    requested_org_name: Option<&str>,
    can_prompt: bool,
    choose_auth_method: F,
) -> Result<RequestedOrgResolution>
where
    F: FnOnce(&str, &[LoginOrgInfo]) -> Result<ApiKeyOrgMismatchAction>,
{
    let Some(requested_org_name) = requested_org_name else {
        return Ok(RequestedOrgResolution::NoRequestedOrg);
    };

    if find_login_org(orgs, requested_org_name).is_some() {
        return Ok(RequestedOrgResolution::UseRequestedOrg);
    }

    if !can_prompt {
        return Err(missing_requested_org_error(orgs, requested_org_name));
    }

    match choose_auth_method(requested_org_name, orgs)? {
        ApiKeyOrgMismatchAction::UseApiKey => Ok(RequestedOrgResolution::IgnoreRequestedOrg),
        ApiKeyOrgMismatchAction::UseOauth => Ok(RequestedOrgResolution::SwitchToOauth),
    }
}

fn prompt_for_auth_method_for_missing_requested_org(
    requested_org_name: &str,
    orgs: &[LoginOrgInfo],
) -> Result<ApiKeyOrgMismatchAction> {
    let api_key_label = if orgs.len() == 1 {
        format!("API key ({})", orgs[0].name)
    } else {
        "API key (use available org)".to_string()
    };
    let methods = ["OAuth (browser)".to_string(), api_key_label];
    let method_refs: Vec<&str> = methods.iter().map(String::as_str).collect();
    let selection = ui::fuzzy_select(
        &format!("Org '{requested_org_name}' is not available for this API key. Continue with"),
        &method_refs,
        0,
    )?;
    Ok(match selection {
        0 => ApiKeyOrgMismatchAction::UseOauth,
        1 => ApiKeyOrgMismatchAction::UseApiKey,
        _ => unreachable!("fuzzy_select returned out-of-range index"),
    })
}

fn resolve_profile_api_url(
    explicit_api_url: Option<String>,
    selected_org: Option<&LoginOrgInfo>,
    orgs: &[LoginOrgInfo],
) -> Result<String> {
    if let Some(api_url) = explicit_api_url {
        return Ok(api_url);
    }
    if let Some(api_url) = selected_org.and_then(|org| org.api_url.clone()) {
        return Ok(api_url);
    }

    let mut api_urls = orgs
        .iter()
        .filter_map(|org| org.api_url.clone())
        .collect::<Vec<_>>();
    api_urls.sort();
    api_urls.dedup();

    if api_urls.len() <= 1 {
        return Ok(api_urls
            .into_iter()
            .next()
            .unwrap_or_else(|| DEFAULT_API_URL.to_string()));
    }

    bail!(
        "multiple organizations expose different API URLs; choose an organization or pass --api-url explicitly"
    )
}

fn default_oauth_client_id(profile_name: &str) -> String {
    let sanitized = profile_name
        .chars()
        .map(|ch| {
            if ch.is_ascii_alphanumeric() || ch == '-' || ch == '_' {
                ch
            } else {
                '-'
            }
        })
        .collect::<String>();
    let trimmed = sanitized.trim_matches('-');
    if trimmed.is_empty() {
        "bt_cli_default".to_string()
    } else {
        format!("bt_cli_{trimmed}")
    }
}

fn generate_random_token(num_bytes: usize) -> Result<String> {
    let mut bytes = vec![0u8; num_bytes];
    getrandom::fill(&mut bytes)
        .map_err(|err| anyhow::anyhow!("failed to generate secure random bytes: {err}"))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

#[derive(Debug)]
struct OAuthCallbackParams {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OAuthCallbackMode {
    ListenerOnly,
    ListenerOrStdin,
    PromptThenListener,
}

fn oauth_callback_mode(prefer_manual: bool) -> OAuthCallbackMode {
    if prefer_manual {
        if ui::can_prompt() {
            OAuthCallbackMode::PromptThenListener
        } else {
            OAuthCallbackMode::ListenerOnly
        }
    } else if ui::is_interactive() {
        OAuthCallbackMode::ListenerOrStdin
    } else if ui::can_prompt() {
        OAuthCallbackMode::PromptThenListener
    } else {
        OAuthCallbackMode::ListenerOnly
    }
}

async fn wait_for_oauth_callback(listener: TcpListener) -> Result<OAuthCallbackParams> {
    let (mut stream, _) = tokio::time::timeout(OAUTH_CALLBACK_TIMEOUT, listener.accept())
        .await
        .context("timed out waiting for oauth callback")?
        .context("failed to accept oauth callback connection")?;

    let mut buffer = vec![0u8; 16 * 1024];
    let bytes_read = stream
        .read(&mut buffer)
        .await
        .context("failed reading oauth callback request")?;
    let request = String::from_utf8_lossy(&buffer[..bytes_read]);
    let first_line = request
        .lines()
        .next()
        .ok_or_else(|| anyhow::anyhow!("oauth callback request was empty"))?;
    let mut parts = first_line.split_whitespace();
    let method = parts.next().unwrap_or("");
    let path = parts.next().unwrap_or("");
    if method != "GET" {
        bail!("unexpected oauth callback method: {method}");
    }

    let query = path.split_once('?').map(|(_, query)| query).unwrap_or("");
    let params = parse_oauth_callback_query(query);

    let body = if params.error.is_some() {
        "<html><body><h1>Authorization Failed</h1><p>You can close this window.</p></body></html>"
    } else {
        "<html><body><h1>Authorization Successful</h1><p>You can close this window.</p></body></html>"
    };
    let response = format!(
        "HTTP/1.1 200 OK\r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream
        .write_all(response.as_bytes())
        .await
        .context("failed writing oauth callback response")?;

    Ok(params)
}

async fn collect_oauth_callback(
    listener: TcpListener,
    prefer_manual: bool,
    quiet_requested: bool,
) -> Result<OAuthCallbackParams> {
    match oauth_callback_mode(prefer_manual) {
        OAuthCallbackMode::ListenerOnly => {
            eprintln!("Waiting for browser authorization...");
            wait_for_oauth_callback(listener).await
        }
        OAuthCallbackMode::ListenerOrStdin => wait_for_oauth_callback_or_stdin(listener).await,
        OAuthCallbackMode::PromptThenListener => {
            let term = ui::prompt_term()
                .ok_or_else(|| anyhow::anyhow!("interactive mode requires TTY"))?;
            if !quiet_requested {
                println!("Remote/SSH OAuth flow: open the URL in a browser on your local machine.");
                println!(
                    "After approving access, your browser may show a localhost connection error on remote hosts."
                );
                println!(
                    "Copy the full URL from the browser address bar (or just code=...&state=...) and paste it below."
                );
            }
            let pasted = Input::<String>::new()
                .with_prompt("Callback URL/query/JSON (press Enter to wait for automatic callback)")
                .allow_empty(true)
                .report(false)
                .interact_text_on(&term)
                .context("failed to read callback URL")?;
            if pasted.trim().is_empty() {
                return wait_for_oauth_callback(listener).await;
            }
            parse_oauth_callback_input(&pasted)
        }
    }
}

fn explicitly_quiet(base: &BaseArgs) -> bool {
    base.quiet && base.quiet_source.is_some()
}

async fn wait_for_oauth_callback_or_stdin(listener: TcpListener) -> Result<OAuthCallbackParams> {
    eprintln!("Waiting for browser authorization...");
    eprintln!(
        "{}",
        dialoguer::console::style("Paste code=...&state=... if callback doesn't complete").dim()
    );

    let callback_fut = wait_for_oauth_callback(listener);
    tokio::pin!(callback_fut);
    let mut manual_buffer = String::new();

    loop {
        tokio::select! {
            callback = &mut callback_fut => return callback,
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if let Some(input) = poll_manual_oauth_input(&mut manual_buffer)? {
                    return parse_oauth_callback_input(&input);
                }
            }
        }
    }
}

fn poll_manual_oauth_input(buffer: &mut String) -> Result<Option<String>> {
    while event::poll(Duration::from_millis(0)).context("failed to poll stdin events")? {
        match event::read().context("failed reading stdin events")? {
            Event::Key(key) => {
                if key.kind != KeyEventKind::Press {
                    continue;
                }
                match key.code {
                    KeyCode::Enter => {
                        let input = buffer.trim().to_string();
                        buffer.clear();
                        if !input.is_empty() {
                            return Ok(Some(input));
                        }
                    }
                    KeyCode::Backspace => {
                        buffer.pop();
                    }
                    KeyCode::Char(ch) => {
                        buffer.push(ch);
                    }
                    _ => {}
                }
            }
            Event::Paste(text) => {
                buffer.push_str(text.as_str());
            }
            _ => {}
        }
    }

    Ok(None)
}

fn parse_oauth_callback_input(input: &str) -> Result<OAuthCallbackParams> {
    let trimmed = input.trim();
    if trimmed.is_empty() {
        bail!("callback URL cannot be empty");
    }

    if trimmed.starts_with('{') {
        return parse_oauth_callback_json(trimmed);
    }

    let parsed = if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        reqwest::Url::parse(trimmed).context("invalid callback URL")?
    } else if trimmed.starts_with('/') {
        reqwest::Url::parse(&format!("http://127.0.0.1{trimmed}"))
            .context("invalid callback path")?
    } else if trimmed.starts_with('?') {
        reqwest::Url::parse(&format!("http://127.0.0.1/callback{trimmed}"))
            .context("invalid callback query")?
    } else if trimmed.starts_with('#') {
        reqwest::Url::parse(&format!("http://127.0.0.1/callback{trimmed}"))
            .context("invalid callback fragment")?
    } else if trimmed.contains("code=") || trimmed.contains("error=") {
        reqwest::Url::parse(&format!("http://127.0.0.1/callback?{trimmed}"))
            .context("invalid callback query")?
    } else {
        bail!(
            "expected a callback URL, callback path, callback query, or callback JSON with code/state"
        );
    };

    let query_params = parse_oauth_callback_query(parsed.query().unwrap_or_default());
    if query_params.code.is_some() || query_params.error.is_some() {
        return Ok(query_params);
    }

    let fragment_params = parse_oauth_callback_query(parsed.fragment().unwrap_or_default());
    if fragment_params.code.is_some() || fragment_params.error.is_some() {
        return Ok(fragment_params);
    }

    bail!("callback input did not include code or error");
}

fn parse_oauth_callback_query(query: &str) -> OAuthCallbackParams {
    let mut code = None;
    let mut state = None;
    let mut error = None;
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
        let decoded = urlencoding::decode(value)
            .map(|value| value.into_owned())
            .unwrap_or_else(|_| value.to_string());
        match key {
            "code" => code = Some(decoded),
            "state" => state = Some(decoded),
            "error" => error = Some(decoded),
            _ => {}
        }
    }
    OAuthCallbackParams { code, state, error }
}

fn parse_oauth_callback_json(input: &str) -> Result<OAuthCallbackParams> {
    #[derive(Debug, Deserialize)]
    struct CallbackJson {
        code: Option<String>,
        state: Option<String>,
        error: Option<String>,
    }

    let payload: CallbackJson =
        serde_json::from_str(input).context("invalid callback JSON payload")?;
    let params = OAuthCallbackParams {
        code: payload.code.map(|value| value.trim().to_string()),
        state: payload.state.map(|value| value.trim().to_string()),
        error: payload.error.map(|value| value.trim().to_string()),
    };
    if params.code.as_deref().is_some_and(str::is_empty) {
        bail!("callback JSON contains an empty code");
    }
    if params.error.as_deref().is_some_and(str::is_empty) {
        bail!("callback JSON contains an empty error");
    }
    if params.code.is_none() && params.error.is_none() {
        bail!("callback JSON must include code or error");
    }
    Ok(params)
}

fn is_ssh_session() -> bool {
    std::env::var_os("SSH_CONNECTION").is_some() || std::env::var_os("SSH_TTY").is_some()
}

async fn exchange_oauth_authorization_code(
    api_url: &str,
    client_id: &str,
    redirect_uri: &str,
    code: &str,
    code_verifier: PkceCodeVerifier,
) -> Result<OAuthTokenResponse> {
    let http_client = build_http_client_from_builder(
        reqwest::Client::builder()
            .timeout(crate::http::DEFAULT_HTTP_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none()),
    )
    .context("failed to initialize oauth HTTP client")?;
    request_oauth_token(
        &http_client,
        api_url,
        &[
            ("grant_type", "authorization_code"),
            ("client_id", client_id),
            ("code", code),
            ("redirect_uri", redirect_uri),
            ("code_verifier", code_verifier.secret()),
        ],
    )
    .await
}

fn map_refresh_oauth_error(
    api_url: &str,
    profile_name: &str,
    status: reqwest::StatusCode,
    body: &str,
) -> anyhow::Error {
    if let Ok(server_err) = serde_json::from_str::<OAuthErrorResponse>(body) {
        if matches!(server_err.error.as_deref(), Some("invalid_grant")) {
            let mut message =
                format!("oauth refresh token expired or was rejected for profile '{profile_name}'");
            if let Some(description) = server_err.error_description.as_deref() {
                message.push_str(&format!(" ({description})"));
            }
            message.push_str(&format!(
                "; re-run `bt auth login --oauth --profile {profile_name}`"
            ));
            return recoverable_auth_error(RecoverableAuthErrorKind::OauthRefreshToken, message);
        }
    }

    anyhow::anyhow!("oauth token request failed ({status}): {body}").context(format!(
        "failed to call oauth token endpoint {}/oauth/token",
        api_url.trim_end_matches('/')
    ))
}

async fn refresh_oauth_access_token(
    api_url: &str,
    refresh_token: &str,
    client_id: &str,
    profile_name: &str,
) -> Result<OAuthTokenResponse> {
    let http_client = build_http_client_from_builder(
        reqwest::Client::builder()
            .timeout(crate::http::DEFAULT_HTTP_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none()),
    )
    .context("failed to initialize oauth HTTP client")?;
    let token_url = format!("{}/oauth/token", api_url.trim_end_matches('/'));
    let response = http_client
        .post(&token_url)
        .form(&[
            ("grant_type", "refresh_token"),
            ("client_id", client_id),
            ("refresh_token", refresh_token),
        ])
        .send()
        .await
        .with_context(|| format!("failed to call oauth token endpoint {token_url}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(map_refresh_oauth_error(
            api_url,
            profile_name,
            status,
            &body,
        ));
    }

    response
        .json()
        .await
        .context("failed to parse oauth token response")
}

async fn request_oauth_token(
    http_client: &reqwest::Client,
    api_url: &str,
    params: &[(&str, &str)],
) -> Result<OAuthTokenResponse> {
    let token_url = format!("{}/oauth/token", api_url.trim_end_matches('/'));
    let response = http_client
        .post(&token_url)
        .form(params)
        .send()
        .await
        .with_context(|| format!("failed to call oauth token endpoint {token_url}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("oauth token request failed ({status}): {body}");
    }

    response
        .json()
        .await
        .context("failed to parse oauth token response")
}

fn build_oauth_client(
    api_url: &str,
    client_id: &str,
    redirect_uri: Option<&str>,
) -> Result<BasicClient> {
    let api_url = api_url.trim_end_matches('/');
    let auth_url = AuthUrl::new(format!("{api_url}/oauth/authorize"))
        .context("failed to construct oauth authorize URL")?;
    let token_url = TokenUrl::new(format!("{api_url}/oauth/token"))
        .context("failed to construct oauth token URL")?;
    let client = BasicClient::new(
        ClientId::new(client_id.to_string()),
        None,
        auth_url,
        Some(token_url),
    );
    if let Some(redirect_uri) = redirect_uri {
        let redirect_url =
            RedirectUrl::new(redirect_uri.to_string()).context("invalid oauth redirect URI")?;
        Ok(client.set_redirect_uri(redirect_url))
    } else {
        Ok(client)
    }
}

fn prompt_api_key() -> Result<String> {
    let term = ui::prompt_term()
        .ok_or_else(|| anyhow::anyhow!("--api-key is required in non-interactive mode"))?;
    let api_key = Password::new()
        .with_prompt("Braintrust API key")
        .allow_empty_password(false)
        .interact_on(&term)
        .context("failed to read API key")?;

    if api_key.trim().is_empty() {
        bail!("api key cannot be empty");
    }

    Ok(api_key)
}

#[cfg(target_os = "linux")]
fn linux_secret_tool_exec_error(err: std::io::Error) -> anyhow::Error {
    if err.kind() == std::io::ErrorKind::NotFound {
        anyhow::anyhow!(
            "`secret-tool` is not installed. Install `libsecret-tools` (Debian/Ubuntu) or your distro's equivalent package, or use BRAINTRUST_API_KEY/--api-key for non-persistent auth."
        )
    } else {
        anyhow::anyhow!("failed to execute Linux keychain utility `secret-tool`: {err}")
    }
}

#[cfg(target_os = "linux")]
fn linux_secret_service_unavailable(stderr: &str) -> bool {
    stderr.contains("org.freedesktop.secrets was not provided by any .service files")
        || stderr.contains("Cannot autolaunch D-Bus without X11")
        || stderr.contains("org.freedesktop.Secret.Service")
}

#[cfg(target_os = "linux")]
fn linux_secret_service_error() -> anyhow::Error {
    anyhow::anyhow!(
        "no Secret Service provider is running. Start a Secret Service daemon (for example gnome-keyring or keepassxc with Secret Service enabled), or use BRAINTRUST_API_KEY/--api-key for non-persistent auth."
    )
}

fn save_profile_secret(profile_name: &str, api_key: &str) -> Result<()> {
    match save_profile_secret_keychain(profile_name, api_key) {
        Ok(()) => {
            let _ = delete_profile_secret_plaintext(profile_name);
            Ok(())
        }
        Err(err) => {
            save_profile_secret_plaintext(profile_name, api_key)?;
            warn_secret_store_plaintext_fallback(&err);
            Ok(())
        }
    }
}

fn load_profile_secret(profile_name: &str) -> Result<Option<String>> {
    match load_profile_secret_keychain(profile_name) {
        Ok(Some(secret)) => Ok(Some(secret)),
        Ok(None) => load_profile_secret_plaintext(profile_name),
        Err(_) => load_profile_secret_plaintext(profile_name),
    }
}

fn delete_profile_secret(profile_name: &str) -> Result<()> {
    let keychain_err = delete_profile_secret_keychain(profile_name).err();
    let plaintext_err = delete_profile_secret_plaintext(profile_name).err();
    if keychain_err.is_none() || plaintext_err.is_none() {
        return Ok(());
    }
    Err(keychain_err.expect("checked is_some"))
}

fn warn_secret_store_plaintext_fallback(err: &anyhow::Error) {
    if SECRET_STORE_FALLBACK_WARNED.swap(true, Ordering::SeqCst) {
        return;
    }

    match secret_store_path() {
        Ok(path) => eprintln!(
            "warning: secure credential store unavailable ({err}); falling back to plaintext credential file at {} (permissions: 0600).",
            path.display()
        ),
        Err(_) => eprintln!(
            "warning: secure credential store unavailable ({err}); falling back to plaintext credential storage."
        ),
    }
}

fn save_profile_secret_plaintext(profile_name: &str, api_key: &str) -> Result<()> {
    let mut store = load_secret_store()?;
    store
        .secrets
        .insert(profile_name.to_string(), api_key.to_string());
    save_secret_store(&store)
}

fn load_profile_secret_plaintext(profile_name: &str) -> Result<Option<String>> {
    let store = load_secret_store()?;
    Ok(store.secrets.get(profile_name).cloned())
}

fn delete_profile_secret_plaintext(profile_name: &str) -> Result<()> {
    let path = secret_store_path()?;
    if !path.exists() {
        return Ok(());
    }

    let mut store = load_secret_store()?;
    store.secrets.remove(profile_name);
    save_secret_store(&store)
}

fn load_secret_store() -> Result<SecretStore> {
    let path = secret_store_path()?;
    if !path.exists() {
        return Ok(SecretStore::default());
    }

    let data = fs::read_to_string(&path)
        .with_context(|| format!("failed to read secret store {}", path.display()))?;
    serde_json::from_str(&data)
        .with_context(|| format!("failed to parse secret store {}", path.display()))
}

fn save_secret_store(store: &SecretStore) -> Result<()> {
    let path = secret_store_path()?;
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory {}", parent.display()))?;
    }

    let data = serde_json::to_string_pretty(store).context("failed to serialize secret store")?;
    let temp_path = path.with_extension("tmp");
    let mut file = fs::File::create(&temp_path)
        .with_context(|| format!("failed to write temp secret store {}", temp_path.display()))?;
    file.write_all(data.as_bytes())
        .with_context(|| format!("failed to write temp secret store {}", temp_path.display()))?;
    file.write_all(b"\n")
        .with_context(|| format!("failed to write temp secret store {}", temp_path.display()))?;
    file.sync_all()
        .with_context(|| format!("failed to flush temp secret store {}", temp_path.display()))?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&temp_path, fs::Permissions::from_mode(0o600)).with_context(|| {
            format!(
                "failed to set permissions on temp secret store {}",
                temp_path.display()
            )
        })?;
    }

    fs::rename(&temp_path, &path).with_context(|| {
        format!(
            "failed to move temp secret store {} to {}",
            temp_path.display(),
            path.display()
        )
    })?;

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).with_context(|| {
            format!(
                "failed to set permissions on secret store {}",
                path.display()
            )
        })?;
    }

    Ok(())
}

fn secret_store_path() -> Result<PathBuf> {
    let mut path = auth_store_path()?;
    path.set_file_name("secrets.json");
    Ok(path)
}

fn save_profile_secret_keychain(profile_name: &str, api_key: &str) -> Result<()> {
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
            .map_err(linux_secret_tool_exec_error)?;

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
        let stderr = String::from_utf8_lossy(&output.stderr);
        if linux_secret_service_unavailable(&stderr) {
            return Err(linux_secret_service_error());
        }

        bail!(
            "failed to store credential in Linux keychain: {}",
            stderr.trim()
        );
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = profile_name;
        let _ = api_key;
        bail!("OS keychain credential storage is not implemented on this platform");
    }
}

fn load_profile_secret_keychain(profile_name: &str) -> Result<Option<String>> {
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
            .map_err(linux_secret_tool_exec_error)?;

        if output.status.success() {
            let secret = String::from_utf8_lossy(&output.stdout).trim().to_string();
            if secret.is_empty() {
                return Ok(None);
            }
            return Ok(Some(secret));
        }

        let stderr = String::from_utf8_lossy(&output.stderr);
        if linux_secret_service_unavailable(&stderr) {
            return Err(linux_secret_service_error());
        }

        if output.status.code() == Some(1) {
            return Ok(None);
        }

        bail!(
            "failed to load credential from Linux keychain: {}",
            stderr.trim()
        );
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = profile_name;
        bail!("OS keychain credential retrieval is not implemented on this platform");
    }
}

fn delete_profile_secret_keychain(profile_name: &str) -> Result<()> {
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
            .map_err(linux_secret_tool_exec_error)?;

        let stderr = String::from_utf8_lossy(&output.stderr);
        if linux_secret_service_unavailable(&stderr) {
            return Err(linux_secret_service_error());
        }

        if output.status.success() || output.status.code() == Some(1) {
            return Ok(());
        }

        bail!(
            "failed to delete credential from Linux keychain: {}",
            stderr.trim()
        );
    }

    #[cfg(not(any(target_os = "macos", target_os = "linux")))]
    {
        let _ = profile_name;
        bail!("OS keychain credential deletion is not implemented on this platform");
    }
}

fn oauth_refresh_secret_key(profile_name: &str) -> String {
    format!("oauth_refresh::{profile_name}")
}

fn oauth_access_secret_key(profile_name: &str) -> String {
    format!("oauth_access::{profile_name}")
}

fn save_profile_oauth_refresh_token(profile_name: &str, refresh_token: &str) -> Result<()> {
    let key = oauth_refresh_secret_key(profile_name);
    save_profile_secret(&key, refresh_token)
}

fn load_profile_oauth_refresh_token(profile_name: &str) -> Result<Option<String>> {
    let key = oauth_refresh_secret_key(profile_name);
    load_profile_secret(&key)
}

fn delete_profile_oauth_refresh_token(profile_name: &str) -> Result<()> {
    let key = oauth_refresh_secret_key(profile_name);
    delete_profile_secret(&key)
}

fn save_profile_oauth_access_token(profile_name: &str, access_token: &str) -> Result<()> {
    let key = oauth_access_secret_key(profile_name);
    save_profile_secret(&key, access_token)
}

fn load_profile_oauth_access_token(profile_name: &str) -> Result<Option<String>> {
    let key = oauth_access_secret_key(profile_name);
    load_profile_secret(&key)
}

fn delete_profile_oauth_access_token(profile_name: &str) -> Result<()> {
    let key = oauth_access_secret_key(profile_name);
    delete_profile_secret(&key)
}

fn load_valid_cached_oauth_access_token(
    profile_name: &str,
    expires_at: Option<u64>,
) -> Result<Option<String>> {
    let Some(expires_at) = expires_at else {
        return Ok(None);
    };
    if !oauth_access_token_is_fresh(expires_at) {
        return Ok(None);
    }
    load_profile_oauth_access_token(profile_name)
}

fn oauth_access_token_is_fresh(expires_at: u64) -> bool {
    expires_at > current_unix_timestamp().saturating_add(OAUTH_REFRESH_SAFETY_WINDOW_SECONDS)
}

fn determine_oauth_access_expiry_epoch(tokens: &OAuthTokenResponse) -> Option<u64> {
    if let Some(expires_in) = tokens.expires_in {
        return Some(current_unix_timestamp().saturating_add(expires_in));
    }
    decode_jwt_exp_epoch(&tokens.access_token)
}

fn decode_jwt_exp_epoch(token: &str) -> Option<u64> {
    let payload = decode_jwt_payload(token)?;
    payload.get("exp").and_then(|value| value.as_u64())
}

fn decode_jwt_payload(token: &str) -> Option<serde_json::Value> {
    let mut parts = token.split('.');
    let _header = parts.next()?;
    let payload_b64 = parts.next()?;
    let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .ok()?;
    serde_json::from_slice(&payload_bytes).ok()
}

struct JwtIdentity {
    name: Option<String>,
    email: Option<String>,
}

fn decode_jwt_identity(token: &str) -> JwtIdentity {
    let extract = || -> Option<JwtIdentity> {
        let payload = decode_jwt_payload(token)?;
        Some(JwtIdentity {
            name: payload
                .get("name")
                .and_then(|v| v.as_str())
                .map(String::from),
            email: payload
                .get("email")
                .and_then(|v| v.as_str())
                .map(String::from),
        })
    };
    extract().unwrap_or(JwtIdentity {
        name: None,
        email: None,
    })
}

pub fn obscure_api_key(key: &str) -> String {
    if !key.is_ascii() || key.len() <= 8 {
        return "****".to_string();
    }
    let prefix_end = key.find('-').map(|i| i + 1).unwrap_or(0);
    let suffix_start = key.len().saturating_sub(5);
    format!("{}****{}", &key[..prefix_end], &key[suffix_start..])
}

fn current_unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
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
    use futures_util::lock::Mutex;
    use tempfile::TempDir;

    use super::*;
    use std::{
        env,
        ffi::OsString,
        fs,
        path::PathBuf,
        sync::OnceLock,
        time::{SystemTime, UNIX_EPOCH},
    };

    fn make_base() -> BaseArgs {
        BaseArgs {
            json: false,
            verbose: false,
            quiet: false,
            quiet_source: None,
            no_color: false,
            no_input: false,
            profile: None,
            profile_explicit: false,
            project: None,
            org_name: None,
            api_key: None,
            api_key_source: None,
            prefer_profile: false,
            api_url: None,
            app_url: None,
            ca_cert: None,
            env_file: None,
        }
    }

    fn env_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn setup_global_config(project_id: Option<&str>, org: Option<&str>) {
        let cfg = crate::config::Config {
            org: org.map(str::to_string),
            project_id: project_id.map(str::to_string),
            ..crate::config::Config::default()
        };

        crate::config::save_global(&cfg).expect("save global config");
    }

    fn setup_auth_store_profiles(profiles: &[(&str, &str, &str, &str)]) {
        let mut store = AuthStore::default();
        for (profile_name, org_name, api_url, app_url) in profiles {
            store.profiles.insert(
                (*profile_name).to_string(),
                AuthProfile {
                    auth_kind: AuthKind::ApiKey,
                    api_url: Some((*api_url).to_string()),
                    app_url: Some((*app_url).to_string()),
                    org_name: Some((*org_name).to_string()),
                    oauth_client_id: None,
                    oauth_access_expires_at: None,
                    user_name: None,
                    email: None,
                    api_key_hint: None,
                },
            );
        }

        save_auth_store(&store).expect("save auth store");
    }

    fn assert_err_contains<T>(result: Result<T>, expected_substring: &str) {
        match result {
            Ok(_) => panic!("expected error containing '{expected_substring}'"),
            Err(err) => {
                let msg = err.to_string();
                assert!(
                    msg.contains(expected_substring),
                    "expected error to contain '{expected_substring}', got '{msg}'",
                );
            }
        }
    }

    fn assert_invalid_api_url<T>(result: Result<T>) {
        assert_err_contains(result, "invalid api_url");
    }

    #[test]
    fn missing_credential_error_helper_detects_typed_auth_errors() {
        let err = recoverable_auth_error(
            RecoverableAuthErrorKind::OauthRefreshToken,
            "oauth refresh token missing".to_string(),
        );

        assert!(is_missing_credential_error(&err));
    }

    #[test]
    fn missing_credential_error_helper_ignores_unrelated_errors() {
        let err = anyhow::anyhow!("some unrelated error");

        assert!(!is_missing_credential_error(&err));
    }

    #[test]
    fn missing_credential_error_helper_detects_errors_through_context() {
        let err = recoverable_auth_error(
            RecoverableAuthErrorKind::StoredCredential,
            "missing stored credential".to_string(),
        )
        .context("while resolving auth");

        assert!(is_missing_credential_error(&err));
    }

    #[test]
    fn invalid_grant_refresh_error_is_treated_as_recoverable() {
        let err = map_refresh_oauth_error(
            "https://api.example.com",
            "work",
            reqwest::StatusCode::BAD_REQUEST,
            r#"{"error":"invalid_grant","error_description":"refresh token expired"}"#,
        );

        assert!(is_missing_credential_error(&err));
        assert!(err.to_string().contains("refresh token expired"));
    }

    #[test]
    fn nonrecoverable_refresh_errors_remain_nonrecoverable() {
        let err = map_refresh_oauth_error(
            "https://api.example.com",
            "work",
            reqwest::StatusCode::BAD_REQUEST,
            "unexpected response",
        );

        assert!(!is_missing_credential_error(&err));
        assert!(err
            .to_string()
            .contains("failed to call oauth token endpoint"));
    }

    fn restore_env_var(key: &str, previous: Option<OsString>) {
        match previous {
            Some(value) => env::set_var(key, value),
            None => env::remove_var(key),
        }
    }

    fn base_args_for_path_probe(org_name: Option<&str>) -> BaseArgs {
        let mut base = make_base();
        base.api_key = Some("test-api-key".into());
        base.api_url = Some("not-a-valid-url".into());
        base.app_url = Some("https://app.example.test".into());
        base.org_name = org_name.map(|s| s.into());
        base
    }

    struct TestEnv {
        _guard: futures_util::lock::MutexGuard<'static, ()>,
        _config_dir: TempDir,
        _cwd_dir: TempDir,
        previous_cwd: PathBuf,
        previous_xdg_config_home: Option<OsString>,
        previous_appdata: Option<OsString>,
    }

    impl TestEnv {
        async fn new(project_id: Option<&str>, org: Option<&str>) -> Self {
            let guard = env_test_lock().lock().await;
            let previous_cwd = env::current_dir().expect("read current dir");
            let cwd_dir = TempDir::new().expect("create temp cwd");
            // Prevent config::local_path from traversing parent directories.
            fs::create_dir(cwd_dir.path().join(".git")).expect("create .git marker");
            env::set_current_dir(cwd_dir.path()).expect("set test current dir");

            let previous_xdg_config_home = env::var_os("XDG_CONFIG_HOME");
            let previous_appdata = env::var_os("APPDATA");
            let config_dir = TempDir::new().expect("create temp config dir");
            env::set_var("XDG_CONFIG_HOME", config_dir.path());
            env::set_var("APPDATA", config_dir.path());
            setup_global_config(project_id, org);
            Self {
                _guard: guard,
                _config_dir: config_dir,
                _cwd_dir: cwd_dir,
                previous_cwd,
                previous_xdg_config_home,
                previous_appdata,
            }
        }

        async fn login_read_only_with_base(&self, base: BaseArgs) -> Result<LoginContext> {
            login_read_only(&base).await
        }

        async fn login_read_only_probe(&self, org_name: Option<&str>) -> Result<LoginContext> {
            self.login_read_only_with_base(base_args_for_path_probe(org_name))
                .await
        }
    }

    impl Drop for TestEnv {
        fn drop(&mut self) {
            env::set_current_dir(&self.previous_cwd).expect("restore current dir");
            restore_env_var("XDG_CONFIG_HOME", self.previous_xdg_config_home.clone());
            restore_env_var("APPDATA", self.previous_appdata.clone());
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
        store.profiles.insert(
            "work".to_string(),
            AuthProfile {
                auth_kind: AuthKind::ApiKey,
                api_url: Some("https://api.example.com".to_string()),
                app_url: Some("https://www.example.com".to_string()),
                org_name: Some("Example Org".to_string()),
                oauth_client_id: None,
                oauth_access_expires_at: None,
                ..Default::default()
            },
        );

        save_auth_store_to_path(&path, &store).expect("save");
        let loaded = load_auth_store_from_path(&path).expect("load");

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
                auth_kind: AuthKind::ApiKey,
                api_url: Some("https://api.example.com".to_string()),
                app_url: Some("https://www.example.com".to_string()),
                org_name: Some("Example Org".to_string()),
                oauth_client_id: None,
                oauth_access_expires_at: None,
                ..Default::default()
            },
        );

        let resolved = resolve_auth_from_store_with_secret_lookup(
            &base,
            &store,
            |_| Ok(Some("profile-key".to_string())),
            &None,
        )
        .expect("resolve");
        assert_eq!(resolved.api_key.as_deref(), Some("profile-key"));
        assert_eq!(resolved.api_url.as_deref(), Some("https://api.example.com"));
        assert_eq!(resolved.org_name.as_deref(), Some("Example Org"));
        assert!(!resolved.is_oauth);
    }

    #[test]
    fn resolve_auth_prefers_explicit_api_key() {
        let mut base = make_base();
        base.api_key = Some("explicit-key".to_string());
        base.api_url = Some("https://override.example.com".to_string());

        let mut store = AuthStore::default();
        store.profiles.insert(
            "work".to_string(),
            AuthProfile {
                auth_kind: AuthKind::ApiKey,
                api_url: Some("https://api.example.com".to_string()),
                app_url: None,
                org_name: None,
                oauth_client_id: None,
                oauth_access_expires_at: None,
                ..Default::default()
            },
        );

        let resolved = resolve_auth_from_store_with_secret_lookup(
            &base,
            &store,
            |_| Ok(Some("profile-key".to_string())),
            &None,
        )
        .expect("resolve");
        assert_eq!(resolved.api_key.as_deref(), Some("explicit-key"));
        assert_eq!(
            resolved.api_url.as_deref(),
            Some("https://override.example.com")
        );
        assert!(!resolved.is_oauth);
    }

    #[test]
    fn resolve_auth_prefer_profile_ignores_api_key_override() {
        let mut base = make_base();
        base.api_key = Some("explicit-key".to_string());
        base.prefer_profile = true;
        base.profile = Some("work".to_string());

        let mut store = AuthStore::default();
        store.profiles.insert(
            "work".to_string(),
            AuthProfile {
                auth_kind: AuthKind::ApiKey,
                api_url: Some("https://api.example.com".to_string()),
                app_url: None,
                org_name: Some("Example Org".to_string()),
                oauth_client_id: None,
                oauth_access_expires_at: None,
                ..Default::default()
            },
        );

        let resolved = resolve_auth_from_store_with_secret_lookup(
            &base,
            &store,
            |_| Ok(Some("profile-key".to_string())),
            &None,
        )
        .expect("resolve");
        assert_eq!(resolved.api_key.as_deref(), Some("profile-key"));
        assert_eq!(resolved.org_name.as_deref(), Some("Example Org"));
    }

    #[test]
    fn resolve_auth_prefers_cli_api_key_even_with_prefer_profile() {
        let mut base = make_base();
        base.api_key = Some("explicit-key".to_string());
        base.api_key_source = Some(crate::args::ArgValueSource::CommandLine);
        base.prefer_profile = true;
        base.profile = Some("work".to_string());

        let mut store = AuthStore::default();
        store.profiles.insert(
            "work".to_string(),
            AuthProfile {
                auth_kind: AuthKind::ApiKey,
                api_url: Some("https://api.example.com".to_string()),
                app_url: None,
                org_name: Some("Example Org".to_string()),
                oauth_client_id: None,
                oauth_access_expires_at: None,
                ..Default::default()
            },
        );

        let resolved = resolve_auth_from_store_with_secret_lookup(
            &base,
            &store,
            |_| Ok(Some("profile-key".to_string())),
            &None,
        )
        .expect("resolve");
        assert_eq!(resolved.api_key.as_deref(), Some("explicit-key"));
        assert_eq!(resolved.org_name, None);
    }

    #[test]
    fn resolve_auth_explicit_profile_ignores_env_api_key_override() {
        let mut base = make_base();
        base.api_key = Some("explicit-key".to_string());
        base.profile = Some("work".to_string());
        base.profile_explicit = true;

        let mut store = AuthStore::default();
        store.profiles.insert(
            "work".to_string(),
            AuthProfile {
                auth_kind: AuthKind::ApiKey,
                api_url: Some("https://api.example.com".to_string()),
                app_url: None,
                org_name: Some("Example Org".to_string()),
                oauth_client_id: None,
                oauth_access_expires_at: None,
                ..Default::default()
            },
        );

        let resolved = resolve_auth_from_store_with_secret_lookup(
            &base,
            &store,
            |_| Ok(Some("profile-key".to_string())),
            &None,
        )
        .expect("resolve");
        assert_eq!(resolved.api_key.as_deref(), Some("profile-key"));
        assert_eq!(resolved.org_name.as_deref(), Some("Example Org"));
    }

    #[test]
    fn resolve_auth_marks_oauth_profiles() {
        let mut base = make_base();
        base.profile = Some("work".to_string());

        let mut store = AuthStore::default();
        store.profiles.insert(
            "work".to_string(),
            AuthProfile {
                auth_kind: AuthKind::Oauth,
                api_url: Some("https://api.example.com".to_string()),
                app_url: Some("https://www.example.com".to_string()),
                org_name: Some("Example Org".to_string()),
                oauth_client_id: Some("bt_cli_work".to_string()),
                oauth_access_expires_at: None,
                ..Default::default()
            },
        );

        let resolved = resolve_auth_from_store_with_secret_lookup(
            &base,
            &store,
            |_| Ok(Some("should-not-be-used".to_string())),
            &None,
        )
        .expect("resolve");

        assert!(resolved.is_oauth);
        assert_eq!(resolved.api_key, None);
        assert_eq!(resolved.org_name.as_deref(), Some("Example Org"));
    }

    #[test]
    fn refresh_profile_selector_prefers_explicit_profile() {
        let mut base = make_base();
        base.profile = Some(" work ".to_string());
        let store = AuthStore::default();
        let (profile_name, source) =
            resolve_selected_profile_name_for_debug(&base, &store).expect("resolve");
        assert_eq!(profile_name, "work");
        assert_eq!(source, "--profile/BRAINTRUST_PROFILE");
    }

    #[test]
    fn parse_oauth_callback_input_accepts_json_payload() {
        let parsed =
            parse_oauth_callback_input(r#"{"code":"abc123","state":"state123","error":null}"#)
                .expect("parse");
        assert_eq!(parsed.code.as_deref(), Some("abc123"));
        assert_eq!(parsed.state.as_deref(), Some("state123"));
        assert_eq!(parsed.error, None);
    }

    #[test]
    fn parse_oauth_callback_input_accepts_fragment_payload() {
        let parsed = parse_oauth_callback_input("#code=abc123&state=state123").expect("parse");
        assert_eq!(parsed.code.as_deref(), Some("abc123"));
        assert_eq!(parsed.state.as_deref(), Some("state123"));
        assert_eq!(parsed.error, None);
    }

    #[test]
    fn parse_oauth_callback_input_requires_code_or_error() {
        let err = parse_oauth_callback_input("https://localhost/callback?state=only-state")
            .expect_err("should fail");
        assert!(
            err.to_string().contains("did not include code or error"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn resolve_profile_for_org_exact_profile_name() {
        let mut store = AuthStore::default();
        store.profiles.insert(
            "acme".into(),
            AuthProfile {
                org_name: Some("acme-corp".into()),
                ..Default::default()
            },
        );
        assert_eq!(resolve_profile_for_org("acme", &store), Some("acme"));
    }

    #[test]
    fn resolve_profile_for_org_by_org_name() {
        let mut store = AuthStore::default();
        store.profiles.insert(
            "work".into(),
            AuthProfile {
                org_name: Some("acme-corp".into()),
                ..Default::default()
            },
        );
        assert_eq!(resolve_profile_for_org("acme-corp", &store), Some("work"));
    }

    #[test]
    fn resolve_profile_for_org_no_match() {
        let mut store = AuthStore::default();
        store.profiles.insert(
            "work".into(),
            AuthProfile {
                org_name: Some("acme-corp".into()),
                ..Default::default()
            },
        );
        assert_eq!(resolve_profile_for_org("unknown", &store), None);
    }

    #[test]
    fn resolve_profile_for_org_multiple_returns_none() {
        let mut store = AuthStore::default();
        store.profiles.insert(
            "work-1".into(),
            AuthProfile {
                org_name: Some("acme".into()),
                ..Default::default()
            },
        );
        store.profiles.insert(
            "work-2".into(),
            AuthProfile {
                org_name: Some("acme".into()),
                ..Default::default()
            },
        );
        assert_eq!(resolve_profile_for_org("acme", &store), None);
    }

    #[test]
    fn resolve_auth_uses_org_to_find_profile() {
        let mut base = make_base();
        base.org_name = Some("acme-corp".into());

        let mut store = AuthStore::default();
        store.profiles.insert(
            "work".into(),
            AuthProfile {
                org_name: Some("acme-corp".into()),
                api_url: Some("https://api.acme.com".into()),
                ..Default::default()
            },
        );

        let resolved = resolve_auth_from_store_with_secret_lookup(
            &base,
            &store,
            |_| Ok(Some("profile-key".into())),
            &None,
        )
        .expect("resolve");
        assert_eq!(resolved.api_key.as_deref(), Some("profile-key"));
        assert_eq!(resolved.org_name.as_deref(), Some("acme-corp"));
    }

    #[test]
    fn resolve_auth_explicit_profile_overrides_org_resolution() {
        let mut base = make_base();
        base.profile = Some("other".into());
        base.org_name = Some("acme-corp".into());

        let mut store = AuthStore::default();
        store.profiles.insert(
            "work".into(),
            AuthProfile {
                org_name: Some("acme-corp".into()),
                ..Default::default()
            },
        );
        store.profiles.insert(
            "other".into(),
            AuthProfile {
                org_name: Some("other-org".into()),
                api_url: Some("https://api.other.com".into()),
                ..Default::default()
            },
        );

        let resolved = resolve_auth_from_store_with_secret_lookup(
            &base,
            &store,
            |_| Ok(Some("other-key".into())),
            &None,
        )
        .expect("resolve");
        assert_eq!(resolved.api_key.as_deref(), Some("other-key"));
        assert_eq!(resolved.org_name.as_deref(), Some("acme-corp"));
    }

    #[test]
    fn resolve_api_key_login_profile_name_creates_new_profile_for_matching_org() {
        let mut store = AuthStore::default();
        store.profiles.insert(
            "acme".into(),
            AuthProfile {
                auth_kind: AuthKind::ApiKey,
                api_url: Some("https://api.acme.example".into()),
                org_name: Some("acme".into()),
                ..Default::default()
            },
        );

        let (profile_name, should_confirm) = resolve_api_key_login_profile_name(
            None,
            Some("acme"),
            "https://api.acme.example",
            &store,
        )
        .expect("resolve");

        assert_eq!(profile_name, "acme-2");
        assert!(!should_confirm);
    }

    #[test]
    fn resolve_oauth_login_profile_name_reuses_most_recent_matching_profile() {
        let mut store = AuthStore::default();
        store.profiles.insert(
            "older".into(),
            AuthProfile {
                auth_kind: AuthKind::Oauth,
                api_url: Some("https://api.acme.example".into()),
                app_url: Some("https://www.acme.example".into()),
                org_name: Some("acme".into()),
                oauth_access_expires_at: Some(100),
                user_name: Some("Alice".into()),
                email: Some("alice@example.com".into()),
                ..Default::default()
            },
        );
        store.profiles.insert(
            "newer".into(),
            AuthProfile {
                auth_kind: AuthKind::Oauth,
                api_url: Some("https://api.acme.example".into()),
                app_url: Some("https://www.acme.example".into()),
                org_name: Some("acme".into()),
                oauth_access_expires_at: Some(200),
                user_name: Some("Alice".into()),
                email: Some("alice@example.com".into()),
                ..Default::default()
            },
        );

        let jwt_id = JwtIdentity {
            name: Some("Alice".into()),
            email: Some("alice@example.com".into()),
        };
        let (profile_name, should_confirm) = resolve_oauth_login_profile_name(
            None,
            Some("acme"),
            "https://api.acme.example",
            "https://www.acme.example",
            &jwt_id,
            &store,
        )
        .expect("resolve");

        assert_eq!(profile_name, "newer");
        assert!(!should_confirm);
    }

    fn login_org(id: &str, name: &str) -> LoginOrgInfo {
        LoginOrgInfo {
            id: id.to_string(),
            name: name.to_string(),
            api_url: None,
        }
    }

    #[test]
    fn resolve_requested_org_for_api_key_login_keeps_matching_requested_org() {
        let orgs = vec![login_org("org_1", "acme")];

        let resolution =
            resolve_requested_org_for_api_key_login(&orgs, Some("acme"), false, |_, _| {
                panic!("prompt should not be called")
            })
            .expect("resolve");

        assert_eq!(resolution, RequestedOrgResolution::UseRequestedOrg);
    }

    #[test]
    fn resolve_requested_org_for_api_key_login_errors_without_prompt() {
        let orgs = vec![login_org("org_1", "braintrustdata.com")];

        let err =
            resolve_requested_org_for_api_key_login(&orgs, Some("ced-test-1"), false, |_, _| {
                panic!("prompt should not be called")
            })
            .expect_err("should fail");

        assert!(err
            .to_string()
            .contains("org 'ced-test-1' not found. Available: braintrustdata.com"));
    }

    #[test]
    fn resolve_requested_org_for_api_key_login_can_switch_to_oauth() {
        let orgs = vec![login_org("org_1", "braintrustdata.com")];

        let resolution = resolve_requested_org_for_api_key_login(
            &orgs,
            Some("ced-test-1"),
            true,
            |requested_org_name, available_orgs| {
                assert_eq!(requested_org_name, "ced-test-1");
                assert_eq!(available_orgs.len(), 1);
                Ok(ApiKeyOrgMismatchAction::UseOauth)
            },
        )
        .expect("resolve");

        assert_eq!(resolution, RequestedOrgResolution::SwitchToOauth);
    }

    #[test]
    fn resolve_requested_org_for_api_key_login_can_continue_with_api_key() {
        let orgs = vec![login_org("org_1", "braintrustdata.com")];

        let resolution = resolve_requested_org_for_api_key_login(
            &orgs,
            Some("ced-test-1"),
            true,
            |requested_org_name, available_orgs| {
                assert_eq!(requested_org_name, "ced-test-1");
                assert_eq!(available_orgs.len(), 1);
                Ok(ApiKeyOrgMismatchAction::UseApiKey)
            },
        )
        .expect("resolve");

        assert_eq!(resolution, RequestedOrgResolution::IgnoreRequestedOrg);
    }

    #[test]
    fn obscure_api_key_standard() {
        assert_eq!(obscure_api_key("sk-LumEdp0BbLRzhJwO"), "sk-****zhJwO");
    }

    #[test]
    fn obscure_api_key_short() {
        assert_eq!(obscure_api_key("abc"), "****");
    }

    #[test]
    fn obscure_api_key_no_dash() {
        assert_eq!(obscure_api_key("abcdefghijklm"), "****ijklm");
    }

    #[test]
    fn obscure_api_key_non_ascii() {
        assert_eq!(obscure_api_key("sk-café-résumé-key"), "****");
    }

    #[test]
    fn decode_jwt_identity_extracts_claims() {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"name":"Alice","email":"alice@example.com"}"#);
        let token = format!("{header}.{payload}.sig");
        let id = decode_jwt_identity(&token);
        assert_eq!(id.name.as_deref(), Some("Alice"));
        assert_eq!(id.email.as_deref(), Some("alice@example.com"));
    }

    #[test]
    fn decode_jwt_identity_handles_missing_claims() {
        let header = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"RS256"}"#);
        let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"sub":"123"}"#);
        let token = format!("{header}.{payload}.sig");
        let id = decode_jwt_identity(&token);
        assert_eq!(id.name, None);
        assert_eq!(id.email, None);
    }

    #[test]
    fn decode_jwt_identity_handles_garbage() {
        let id = decode_jwt_identity("not-a-jwt");
        assert_eq!(id.name, None);
        assert_eq!(id.email, None);
    }

    #[test]
    fn format_verification_line_ok_with_identity() {
        let v = ProfileVerification {
            name: "work".into(),
            auth: "oauth".into(),
            org: Some("acme".into()),
            user_name: Some("Alice".into()),
            user_email: Some("alice@example.com".into()),
            api_key_hint: None,
            status: "ok".into(),
            error: None,
        };
        assert_eq!(
            format_verification_line(&v),
            "work — oauth — org: acme — Alice (alice@example.com)"
        );
    }

    #[test]
    fn format_verification_line_ok_with_api_key_hint() {
        let v = ProfileVerification {
            name: "work".into(),
            auth: "api_key".into(),
            org: Some("acme".into()),
            user_name: None,
            user_email: None,
            api_key_hint: Some("sk-****zhJwO".into()),
            status: "ok".into(),
            error: None,
        };
        assert_eq!(
            format_verification_line(&v),
            "work — api_key — org: acme — sk-****zhJwO"
        );
    }

    #[test]
    fn format_verification_line_expired() {
        let v = ProfileVerification {
            name: "old".into(),
            auth: "oauth".into(),
            org: None,
            user_name: None,
            user_email: None,
            api_key_hint: None,
            status: "expired".into(),
            error: None,
        };
        assert_eq!(format_verification_line(&v), "old — oauth — token expired");
    }

    #[test]
    fn format_verification_line_error() {
        let v = ProfileVerification {
            name: "bad".into(),
            auth: "api_key".into(),
            org: Some("corp".into()),
            user_name: None,
            user_email: None,
            api_key_hint: None,
            status: "error".into(),
            error: Some("invalid API key".into()),
        };
        assert_eq!(
            format_verification_line(&v),
            "bad — api_key — org: corp — invalid API key"
        );
    }

    #[tokio::test]
    async fn oauth_callback_mode_uses_listener_only_when_input_is_disabled() {
        let _guard = env_test_lock().lock().await;
        ui::set_no_input(true);
        assert_eq!(oauth_callback_mode(false), OAuthCallbackMode::ListenerOnly);
        assert_eq!(oauth_callback_mode(true), OAuthCallbackMode::ListenerOnly);
        ui::set_no_input(false);
    }

    #[test]
    fn oauth_callback_mode_prefers_manual_prompt_when_interactive() {
        ui::set_no_input(false);

        if ui::is_interactive() {
            assert_eq!(
                oauth_callback_mode(true),
                OAuthCallbackMode::PromptThenListener
            );
            assert_eq!(
                oauth_callback_mode(false),
                OAuthCallbackMode::ListenerOrStdin
            );
        }
    }

    #[tokio::test]
    async fn login_read_only_no_cached_project_id_uses_validated_login_path() {
        let env = TestEnv::new(None, None).await;
        assert_invalid_api_url(env.login_read_only_probe(Some("acme")).await);
    }

    #[tokio::test]
    async fn login_read_only_cached_project_id_and_org_uses_fast_path() {
        let env = TestEnv::new(Some("proj_123"), None).await;
        let ctx = env
            .login_read_only_probe(Some("acme"))
            .await
            .expect("fast path should succeed");

        assert_eq!(ctx.login.org_name().as_deref(), Some("acme"));
        assert_eq!(ctx.login.org_id().as_deref(), Some(""));
        assert_eq!(ctx.api_url, "not-a-valid-url");
    }

    #[tokio::test]
    async fn login_read_only_cached_project_id_but_missing_org_falls_back_to_login() {
        let env = TestEnv::new(Some("proj_123"), None).await;
        assert_invalid_api_url(env.login_read_only_probe(None).await);
    }

    #[tokio::test]
    async fn login_read_only_cached_project_id_but_whitespace_org_falls_back_to_login() {
        let env = TestEnv::new(Some("proj_123"), None).await;
        assert_invalid_api_url(env.login_read_only_probe(Some("     ")).await);
    }

    #[tokio::test]
    async fn login_read_only_whitespace_project_id_is_treated_as_not_cached() {
        let env = TestEnv::new(Some("     "), None).await; // has_cached_project_id => false
        assert_invalid_api_url(env.login_read_only_probe(Some("acme")).await);
    }

    #[tokio::test]
    async fn login_read_only_cached_project_id_and_config_org_uses_fast_path() {
        let env = TestEnv::new(Some("proj_123"), Some("acme-org")).await;
        setup_auth_store_profiles(&[
            (
                "acme-profile",
                "acme-org",
                "https://api.acme.example",
                "https://www.acme.example",
            ),
            (
                "other-profile",
                "other-org",
                "https://api.other.example",
                "https://www.other.example",
            ),
        ]);
        save_profile_secret_plaintext("acme-profile", "acme-secret").expect("save acme secret");
        save_profile_secret_plaintext("other-profile", "other-secret").expect("save other secret");

        let ctx = env
            .login_read_only_with_base(make_base())
            .await
            .expect("fast path should succeed with cfg org");

        assert_eq!(ctx.login.api_key().as_deref(), Some("acme-secret"));
        assert_eq!(ctx.login.org_name().as_deref(), Some("acme-org"));
        assert_eq!(ctx.api_url, "https://api.acme.example");
        assert_eq!(ctx.app_url, "https://www.acme.example");
    }

    #[tokio::test]
    async fn login_read_only_cached_project_id_and_org_uses_default_urls() {
        let env = TestEnv::new(Some("proj_123"), None).await;
        let mut base = make_base();
        base.api_key = Some("test-api-key".into());
        base.org_name = Some("acme".into());

        let ctx = env
            .login_read_only_with_base(base)
            .await
            .expect("fast path should succeed");

        assert_eq!(ctx.login.org_name().as_deref(), Some("acme"));
        assert_eq!(ctx.api_url, DEFAULT_API_URL);
        assert_eq!(ctx.app_url, DEFAULT_APP_URL);
    }

    #[tokio::test]
    async fn login_read_only_cached_project_id_missing_api_key_returns_helpful_error() {
        let env = TestEnv::new(Some("proj_123"), None).await;
        let mut base = make_base();
        base.org_name = Some("acme".into());

        let result = env.login_read_only_with_base(base).await;
        assert_err_contains(result, "no login credentials found");
    }
}
