use std::collections::BTreeMap;
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
use oauth2::basic::{BasicClient, BasicTokenType};
use oauth2::reqwest::async_http_client;
use oauth2::{
    AuthUrl, AuthorizationCode, ClientId, CsrfToken, EmptyExtraTokenFields, PkceCodeChallenge,
    PkceCodeVerifier, RedirectUrl, RefreshToken, Scope, StandardTokenResponse, TokenResponse,
    TokenUrl,
};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;

use crate::{
    args::{BaseArgs, DEFAULT_API_URL, DEFAULT_APP_URL},
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

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct AuthStore {
    #[serde(default)]
    active_profile: Option<String>,
    #[serde(default)]
    profiles: BTreeMap<String, AuthProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SecretStore {
    #[serde(default)]
    secrets: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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

#[derive(Debug, Clone, Deserialize)]
struct OAuthTokenResponse {
    access_token: String,
    #[serde(default)]
    refresh_token: Option<String>,
    #[serde(default)]
    expires_in: Option<u64>,
}

#[derive(Debug, Clone, Args)]
pub struct AuthArgs {
    #[command(subcommand)]
    command: Option<AuthCommand>,
}

#[derive(Debug, Clone, Subcommand)]
enum AuthCommand {
    /// Authenticate with Braintrust (OAuth or API key)
    Login(AuthLoginArgs),
    /// Force-refresh OAuth access token for a profile
    Refresh,
    /// List saved auth profiles
    List,
    /// Switch active profile
    Use(AuthUseArgs),
    /// Delete a saved profile
    Delete(AuthDeleteArgs),
    /// Clear the active profile
    Logout,
    /// Show current auth status
    Status,
}

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
struct AuthUseArgs {
    /// Profile name
    profile: String,

    /// Save as the active profile for this project (.bt/config.json)
    #[arg(long, short = 'l', conflicts_with = "global")]
    local: bool,

    /// Save as the global default profile (~/.config/bt/config.json)
    #[arg(long, short = 'g')]
    global: bool,
}

#[derive(Debug, Clone, Args)]
struct AuthDeleteArgs {
    /// Profile name
    profile: String,

    /// Skip confirmation prompt
    #[arg(long, short = 'f')]
    force: bool,
}

pub async fn run(base: BaseArgs, args: AuthArgs) -> Result<()> {
    match args.command {
        None => {
            let has_api_key = resolve_api_key_override(&base).is_some();
            let has_profile = load_auth_store()
                .ok()
                .and_then(|s| s.active_profile)
                .is_some();

            if has_api_key || has_profile {
                run_login_status(&base)?;
                println!("\nRun `bt auth login` to re-authenticate or `bt auth --help` for all commands.");
                Ok(())
            } else {
                run_login_interactive_default(&base).await
            }
        }
        Some(AuthCommand::Login(login_args)) => run_login_set(&base, login_args).await,
        Some(AuthCommand::Refresh) => run_login_refresh(&base).await,
        Some(AuthCommand::List) => run_login_list(),
        Some(AuthCommand::Use(use_args)) => {
            run_login_use(&use_args.profile, use_args.local, use_args.global)
        }
        Some(AuthCommand::Delete(delete_args)) => {
            run_login_delete(&delete_args.profile, delete_args.force)
        }
        Some(AuthCommand::Logout) => run_login_logout(),
        Some(AuthCommand::Status) => run_login_status(&base),
    }
}

fn default_login_set_args() -> AuthLoginArgs {
    AuthLoginArgs {
        oauth: false,
        client_id: None,
        no_browser: false,
    }
}

async fn run_login_interactive_default(base: &BaseArgs) -> Result<()> {
    if !std::io::stdin().is_terminal() {
        bail!(
            "`bt auth login` requires an interactive terminal. Use `bt auth login --api-key <KEY>` or `bt auth login --oauth`"
        );
    }

    let methods = ["OAuth (browser)", "API key"];
    let selected = ui::fuzzy_select("Select login method", &methods)?;
    let mut args = default_login_set_args();
    args.oauth = selected == 0;
    run_login_set(base, args).await
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
    if let Some(project) = &base.project {
        builder = builder.default_project(project);
    }

    let login = match builder.build().await {
        Ok(client) => client.wait_for_login().await?,
        Err(err) if auth.is_oauth => {
            let org_name = auth
                .org_name
                .clone()
                .ok_or_else(|| anyhow::anyhow!("oauth profile is missing org_name: {err}"))?;
            LoginState {
                api_key: api_key.clone(),
                org_id: String::new(),
                org_name,
                api_url: auth.api_url.clone(),
            }
        }
        Err(err) => return Err(err.into()),
    };

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
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
        .or_else(|| load_selected_profile_from_config().ok().flatten())
        .or_else(|| {
            load_auth_store()
                .ok()
                .and_then(|store| store.active_profile)
        });

    if let Some(profile_name) = ignored_profile {
        eprintln!(
            "Warning: using --api-key/BRAINTRUST_API_KEY credentials; selected profile '{profile_name}' is ignored for this command. Use --prefer-profile or unset BRAINTRUST_API_KEY.",
        );
    } else {
        eprintln!(
            "Warning: using --api-key/BRAINTRUST_API_KEY credentials for this command. Use --prefer-profile or unset BRAINTRUST_API_KEY."
        );
    }
}

fn resolve_api_key_override(base: &BaseArgs) -> Option<String> {
    if base.prefer_profile {
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
    let mut lookup_base = base.clone();
    if lookup_base.profile.is_none() {
        lookup_base.profile = load_selected_profile_from_config()?;
    }
    let mut auth = resolve_auth_from_store(&lookup_base, &store)?;
    if !auth.is_oauth {
        return Ok(auth);
    }

    let profile_name = lookup_base
        .profile
        .as_deref()
        .filter(|value| !value.trim().is_empty())
        .or(store.active_profile.as_deref())
        .ok_or_else(|| anyhow::anyhow!("oauth profile requested but none selected"))?;
    let profile = store
        .profiles
        .get(profile_name)
        .ok_or_else(|| anyhow::anyhow!("profile '{profile_name}' not found"))?;
    let client_id = profile.oauth_client_id.as_deref().ok_or_else(|| {
        anyhow::anyhow!(
            "oauth profile '{profile_name}' is missing client_id; re-run `bt auth login --oauth --profile {profile_name}`"
        )
    })?;
    let cached_expires_at = profile.oauth_access_expires_at;
    let api_url = auth
        .api_url
        .clone()
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());

    if let Some(cached_access_token) =
        load_valid_cached_oauth_access_token(profile_name, cached_expires_at)?
    {
        auth.api_key = Some(cached_access_token);
        return Ok(auth);
    }

    let refresh_token = load_profile_oauth_refresh_token(profile_name)?.ok_or_else(|| {
        anyhow::anyhow!(
            "oauth refresh token missing for profile '{profile_name}'; re-run `bt auth login --oauth --profile {profile_name}`"
        )
    })?;
    let refreshed = refresh_oauth_access_token(&api_url, &refresh_token, client_id).await?;
    save_profile_oauth_access_token(profile_name, &refreshed.access_token)?;
    if let Some(next_refresh_token) = refreshed.refresh_token.as_ref() {
        if next_refresh_token != &refresh_token {
            save_profile_oauth_refresh_token(profile_name, next_refresh_token)?;
        }
    }
    if let Some(profile) = store.profiles.get_mut(profile_name) {
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
    let selected_profile_name = requested_profile.or(store.active_profile.as_deref());

    if let Some(profile_name) = selected_profile_name {
        let profile = store.profiles.get(profile_name).ok_or_else(|| {
            anyhow::anyhow!(
                "profile '{profile_name}' not found; run `bt auth list` or `bt auth login --profile {profile_name}`"
            )
        })?;
        let is_oauth = profile.auth_kind == AuthKind::Oauth;
        let api_key = if is_oauth {
            None
        } else {
            Some(load_secret(profile_name)?.ok_or_else(|| {
                anyhow::anyhow!(
                    "no keychain credential found for profile '{profile_name}'; re-run `bt auth login --profile {profile_name}`"
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
        org_name: base.org_name.clone(),
        is_oauth: false,
    })
}

async fn run_login_set(base: &BaseArgs, args: AuthLoginArgs) -> Result<()> {
    if args.oauth {
        return run_login_oauth(base, args).await;
    }
    let interactive = std::io::stdin().is_terminal();

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
    let selected_org = select_login_org(login_orgs.clone(), base.org_name.as_deref(), interactive)?;
    let selected_api_url =
        resolve_profile_api_url(base.api_url.clone(), selected_org.as_ref(), &login_orgs)?;
    let mut store = load_auth_store()?;
    let profile_name = resolve_profile_name(
        base.profile.as_deref(),
        selected_org.as_ref().map(|org| org.name.as_str()),
        interactive,
    )?;

    if interactive {
        match selected_org.as_ref() {
            Some(org) => println!("Selected org: {}", org.name),
            None => println!("Selected org: (none, cross-org mode)"),
        }
        println!("Resolved API URL: {selected_api_url}");
        let confirmed = Confirm::new()
            .with_prompt("Use this API URL?")
            .default(true)
            .interact()?;
        if !confirmed {
            bail!("login cancelled");
        }
    }

    save_profile_secret(&profile_name, &api_key)?;
    let _ = delete_profile_oauth_refresh_token(&profile_name);
    let _ = delete_profile_oauth_access_token(&profile_name);

    let stored_api_url = Some(selected_api_url.clone());
    let stored_app_url = base.app_url.clone();

    store.profiles.insert(
        profile_name.to_string(),
        AuthProfile {
            auth_kind: AuthKind::ApiKey,
            api_url: stored_api_url,
            app_url: stored_app_url,
            org_name: selected_org.as_ref().map(|org| org.name.clone()),
            oauth_client_id: None,
            oauth_access_expires_at: None,
        },
    );
    let made_default = update_default_profile_after_save(&mut store, &profile_name, interactive)?;
    save_auth_store(&store)?;

    if let Some(org) = selected_org.as_ref() {
        println!(
            "Logged in to org '{}' with profile '{}'",
            org.name, profile_name
        );
    } else {
        println!("Logged in with no default org (cross-org) using profile '{profile_name}'");
    }
    println!(
        "Saved profile metadata at {} and credential in secure store (OS keychain when available; plaintext fallback otherwise)",
        auth_store_path()?.display()
    );
    if made_default {
        println!("Default profile is now '{profile_name}'.");
    } else if let Some(active) = store.active_profile.as_deref() {
        println!("Default profile remains '{active}'.");
    }
    Ok(())
}

async fn run_login_oauth(base: &BaseArgs, args: AuthLoginArgs) -> Result<()> {
    if !std::io::stdin().is_terminal() {
        bail!("oauth login requires an interactive terminal");
    }
    if let Some(warning) = oauth_ignored_api_key_warning(base) {
        eprintln!("{warning}");
    }

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

    println!("Opening browser for OAuth authorization...");
    println!("If it does not open, visit:\n{authorize_url}");
    if !args.no_browser {
        if let Err(err) = open::that(&authorize_url) {
            eprintln!("warning: failed to open browser automatically: {err}");
        }
    }

    let callback = collect_oauth_callback(listener, args.no_browser || is_ssh_session()).await?;
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
    if let Some(expires_in) = oauth_tokens.expires_in {
        println!("Received OAuth access token (expires in {expires_in}s).");
    }

    let login_orgs = fetch_login_orgs(&oauth_tokens.access_token, &app_url).await?;
    let selected_org = select_login_org(login_orgs.clone(), base.org_name.as_deref(), true)?;
    let selected_api_url =
        resolve_profile_api_url(base.api_url.clone(), selected_org.as_ref(), &login_orgs)?;
    let mut store = load_auth_store()?;
    let profile_name = resolve_profile_name(
        base.profile.as_deref(),
        selected_org.as_ref().map(|org| org.name.as_str()),
        true,
    )?;

    match selected_org.as_ref() {
        Some(org) => println!("Selected org: {}", org.name),
        None => println!("Selected org: (none, cross-org mode)"),
    }
    println!("Resolved API URL: {selected_api_url}");
    let confirmed = Confirm::new()
        .with_prompt("Use this API URL?")
        .default(true)
        .interact()?;
    if !confirmed {
        bail!("login cancelled");
    }

    let refresh_token = oauth_tokens.refresh_token.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "oauth token response did not include a refresh_token; cannot create persistent oauth profile"
        )
    })?;
    save_profile_oauth_refresh_token(&profile_name, refresh_token)?;
    save_profile_oauth_access_token(&profile_name, &oauth_tokens.access_token)?;
    let _ = delete_profile_secret(&profile_name);
    let oauth_access_expires_at = determine_oauth_access_expiry_epoch(&oauth_tokens);

    store.profiles.insert(
        profile_name.to_string(),
        AuthProfile {
            auth_kind: AuthKind::Oauth,
            api_url: Some(selected_api_url.clone()),
            app_url: Some(app_url.clone()),
            org_name: selected_org.as_ref().map(|org| org.name.clone()),
            oauth_client_id: Some(client_id.clone()),
            oauth_access_expires_at,
        },
    );
    let made_default = update_default_profile_after_save(&mut store, &profile_name, true)?;
    save_auth_store(&store)?;

    if let Some(org) = selected_org.as_ref() {
        println!(
            "Logged in with OAuth to org '{}' with profile '{}'",
            org.name, profile_name
        );
    } else {
        println!(
            "Logged in with OAuth and no default org (cross-org) using profile '{profile_name}'"
        );
    }
    println!(
        "Saved profile metadata at {} and refresh token in secure store (OS keychain when available; plaintext fallback otherwise)",
        auth_store_path()?.display()
    );
    if made_default {
        println!("Default profile is now '{profile_name}'.");
    } else if let Some(active) = store.active_profile.as_deref() {
        println!("Default profile remains '{active}'.");
    }

    Ok(())
}

fn oauth_ignored_api_key_warning(base: &BaseArgs) -> Option<String> {
    let api_key = base.api_key.as_deref()?.trim();
    if api_key.is_empty() {
        return None;
    }

    Some(
        "warning: --api-key/BRAINTRUST_API_KEY is set; ignoring it because --oauth was requested"
            .to_string(),
    )
}

async fn run_login_refresh(base: &BaseArgs) -> Result<()> {
    let mut store = load_auth_store()?;
    let (profile_name, source) = resolve_selected_profile_name_for_debug(base, &store)?;
    let profile = store.profiles.get(profile_name.as_str()).ok_or_else(|| {
        anyhow::anyhow!(
            "profile '{profile_name}' not found; run `bt auth list` to see available profiles"
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

    let refreshed = refresh_oauth_access_token(&api_url, &refresh_token, &client_id).await?;
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

    if let Some(profile_name) = load_selected_profile_from_config()? {
        return Ok((profile_name, "config file"));
    }

    if let Some(profile_name) = store.active_profile.clone() {
        return Ok((profile_name, "saved active profile"));
    }

    bail!(
        "no profile selected; pass --profile <NAME>, set BRAINTRUST_PROFILE, or run `bt auth use <NAME>`"
    )
}

fn update_default_profile_after_save(
    store: &mut AuthStore,
    profile_name: &str,
    interactive: bool,
) -> Result<bool> {
    if store.active_profile.as_deref() == Some(profile_name) {
        return Ok(false);
    }

    let profile_count = store.profiles.len();
    if profile_count <= 1 {
        store.active_profile = Some(profile_name.to_string());
        return Ok(true);
    }

    if !interactive {
        if store.active_profile.is_none() {
            store.active_profile = Some(profile_name.to_string());
            return Ok(true);
        }
        return Ok(false);
    }

    let set_default = Confirm::new()
        .with_prompt(format!("Set '{profile_name}' as the default profile?"))
        .default(false)
        .interact()?;
    if set_default {
        store.active_profile = Some(profile_name.to_string());
        Ok(true)
    } else {
        Ok(false)
    }
}

fn resolve_profile_name(
    explicit_profile: Option<&str>,
    suggested_org_name: Option<&str>,
    interactive: bool,
) -> Result<String> {
    if let Some(profile) = explicit_profile {
        let profile = profile.trim();
        if profile.is_empty() {
            bail!("profile name cannot be empty");
        }
        return Ok(profile.to_string());
    }

    let suggested = suggested_org_name
        .map(str::trim)
        .filter(|name| !name.is_empty())
        .unwrap_or("profile")
        .to_string();

    if !interactive {
        return Ok(suggested);
    }

    let profile_name = Input::<String>::new()
        .with_prompt("Profile name")
        .default(suggested)
        .interact_text()?;
    let profile_name = profile_name.trim();
    if profile_name.is_empty() {
        bail!("profile name cannot be empty");
    }
    Ok(profile_name.to_string())
}

fn run_login_list() -> Result<()> {
    let store = load_auth_store()?;
    if store.profiles.is_empty() {
        println!("No saved login profiles. Run `bt auth login` to create one.");
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
        let auth = match profile.auth_kind {
            AuthKind::ApiKey => " auth=api_key",
            AuthKind::Oauth => " auth=oauth",
        };
        println!("{active_marker} {name}{auth}{org}{api_url}");
    }
    Ok(())
}

fn run_login_use(profile_name: &str, local: bool, global: bool) -> Result<()> {
    let profile_name = profile_name.trim();
    if profile_name.is_empty() {
        bail!("profile name cannot be empty");
    }

    let mut store = load_auth_store()?;
    if !store.profiles.contains_key(profile_name) {
        bail!("profile '{profile_name}' not found; run `bt auth list` to see available profiles");
    }

    store.active_profile = Some(profile_name.to_string());
    save_auth_store(&store)?;

    if local || global {
        let config_path = resolve_profile_config_write_path(local, global)?;
        write_profile_selection_to_config(&config_path, Some(profile_name))?;
        println!(
            "Switched active profile to '{profile_name}' and wrote profile selection to {}",
            config_path.display()
        );
        return Ok(());
    }

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
        bail!("profile '{profile_name}' not found; run `bt auth list` to see available profiles");
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
    if let Err(err) = delete_profile_oauth_refresh_token(profile_name) {
        eprintln!("warning: failed to delete oauth refresh token for '{profile_name}': {err}");
    }
    if let Err(err) = delete_profile_oauth_access_token(profile_name) {
        eprintln!("warning: failed to delete oauth access token for '{profile_name}': {err}");
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

fn run_login_status(base: &BaseArgs) -> Result<()> {
    let store = load_auth_store()?;
    if resolve_api_key_override(base).is_some() {
        println!("Auth source: --api-key/BRAINTRUST_API_KEY override");
        if let Some(profile_name) = base.profile.as_deref() {
            let profile_name = profile_name.trim();
            if !profile_name.is_empty() {
                println!("Requested profile via --profile/BRAINTRUST_PROFILE={profile_name}");
            }
        } else if let Some(active_profile) = store.active_profile.as_deref() {
            println!(
                "Active saved profile (ignored while API key override is set): {active_profile}"
            );
        }
        println!("Tip: pass --prefer-profile or unset BRAINTRUST_API_KEY.");
        return Ok(());
    }

    let selected_profile = base
        .profile
        .as_ref()
        .map(|value| value.trim())
        .filter(|value| !value.is_empty())
        .map(|profile| {
            (
                profile.to_string(),
                "--profile/BRAINTRUST_PROFILE".to_string(),
            )
        })
        .or_else(|| {
            load_selected_profile_from_config()
                .ok()
                .flatten()
                .map(|profile| (profile, "config file".to_string()))
        })
        .or_else(|| {
            store
                .active_profile
                .as_ref()
                .map(|profile| (profile.clone(), "saved active profile".to_string()))
        });

    if let Some((profile_name, source)) = selected_profile {
        println!("Selected profile: {profile_name} (source: {source})");
        if let Some(profile) = store.profiles.get(profile_name.as_str()) {
            let auth_method = match profile.auth_kind {
                AuthKind::ApiKey => "api_key",
                AuthKind::Oauth => "oauth",
            };
            println!("Auth method: {auth_method}");
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
    println!("Run `bt auth login`, `bt auth login --oauth`, or set BRAINTRUST_API_KEY.");
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
        .with_context(|| format!("failed to call login endpoint {login_url}"))?;

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
    mut orgs: Vec<LoginOrgInfo>,
    requested_org_name: Option<&str>,
    interactive: bool,
) -> Result<Option<LoginOrgInfo>> {
    if orgs.is_empty() {
        bail!("no organizations found for this API key");
    }
    orgs.sort_by(|a, b| {
        a.name
            .to_ascii_lowercase()
            .cmp(&b.name.to_ascii_lowercase())
            .then_with(|| a.name.cmp(&b.name))
    });

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
        return selected.map(Some).ok_or_else(|| {
            let available = orgs
                .iter()
                .map(|org| org.name.as_str())
                .collect::<Vec<_>>()
                .join(", ");
            anyhow::anyhow!(
                "organization '{name}' not found for this API key. Available organizations: {available}"
            )
        });
    }

    if orgs.len() == 1 {
        return Ok(Some(orgs.into_iter().next().expect("org exists")));
    }

    if !interactive {
        return Ok(None);
    }

    let mut labels = vec![
        "No default org (cross-org mode; pass --org-name or BRAINTRUST_ORG_NAME when needed)"
            .to_string(),
    ];
    labels.extend(orgs.iter().map(|org| {
        let api_url = org.api_url.as_deref().unwrap_or(DEFAULT_API_URL);
        format!("{} [{}] ({})", org.name, org.id, api_url)
    }));
    let label_refs: Vec<&str> = labels.iter().map(String::as_str).collect();
    let selection = ui::fuzzy_select("Select organization", &label_refs)?;
    if selection == 0 {
        return Ok(None);
    }

    Ok(Some(
        orgs.into_iter()
            .nth(selection - 1)
            .expect("selected index should be in range"),
    ))
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
) -> Result<OAuthCallbackParams> {
    if !prefer_manual {
        return wait_for_oauth_callback_or_stdin(listener).await;
    }

    println!("Remote/SSH OAuth flow: open the URL in a browser on your local machine.");
    println!(
        "After approving access, your browser may show a localhost connection error on remote hosts."
    );
    println!(
        "Copy the full URL from the browser address bar (or just code=...&state=...) and paste it below."
    );
    let pasted = Input::<String>::new()
        .with_prompt("Callback URL/query/JSON (press Enter to wait for automatic callback)")
        .allow_empty(true)
        .interact_text()
        .context("failed to read callback URL")?;
    if pasted.trim().is_empty() {
        return wait_for_oauth_callback(listener).await;
    }
    parse_oauth_callback_input(&pasted)
}

async fn wait_for_oauth_callback_or_stdin(listener: TcpListener) -> Result<OAuthCallbackParams> {
    println!("Waiting for OAuth callback...");
    println!("If localhost callback does not complete, paste code=...&state=... and press Enter.");

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
    let oauth_client = build_oauth_client(api_url, client_id, Some(redirect_uri))?;
    let token_response = oauth_client
        .exchange_code(AuthorizationCode::new(code.to_string()))
        .set_pkce_verifier(code_verifier)
        .request_async(async_http_client)
        .await
        .with_context(|| {
            format!(
                "failed to call oauth token endpoint {}/oauth/token",
                api_url.trim_end_matches('/')
            )
        })?;
    Ok(to_oauth_token_response(token_response))
}

async fn refresh_oauth_access_token(
    api_url: &str,
    refresh_token: &str,
    client_id: &str,
) -> Result<OAuthTokenResponse> {
    let oauth_client = build_oauth_client(api_url, client_id, None)?;
    let token_response = oauth_client
        .exchange_refresh_token(&RefreshToken::new(refresh_token.to_string()))
        .request_async(async_http_client)
        .await
        .with_context(|| {
            format!(
                "failed to call oauth token endpoint {}/oauth/token",
                api_url.trim_end_matches('/')
            )
        })?;
    Ok(to_oauth_token_response(token_response))
}

type OAuth2StdTokenResponse = StandardTokenResponse<EmptyExtraTokenFields, BasicTokenType>;

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

fn to_oauth_token_response(tokens: OAuth2StdTokenResponse) -> OAuthTokenResponse {
    OAuthTokenResponse {
        access_token: tokens.access_token().secret().to_string(),
        refresh_token: tokens
            .refresh_token()
            .map(|token| token.secret().to_string()),
        expires_in: tokens.expires_in().map(|duration| duration.as_secs()),
    }
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
    let mut parts = token.split('.');
    let _header = parts.next()?;
    let payload_b64 = parts.next()?;
    let payload_bytes = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload_b64)
        .ok()?;
    let payload: serde_json::Value = serde_json::from_slice(&payload_bytes).ok()?;
    payload.get("exp").and_then(|value| value.as_u64())
}

fn current_unix_timestamp() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_secs())
        .unwrap_or(0)
}

fn load_selected_profile_from_config() -> Result<Option<String>> {
    let global_profile = read_profile_selection_from_config(&global_config_path()?)?;
    let local_profile = match local_config_path() {
        Some(path) => read_profile_selection_from_config(&path)?,
        None => None,
    };
    Ok(local_profile.or(global_profile))
}

fn read_profile_selection_from_config(path: &Path) -> Result<Option<String>> {
    if !path.exists() {
        return Ok(None);
    }

    let data = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&data)
        .with_context(|| format!("failed to parse config file {}", path.display()))?;
    Ok(value
        .as_object()
        .and_then(|obj| obj.get("profile"))
        .and_then(|v| v.as_str())
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .map(str::to_string))
}

fn write_profile_selection_to_config(path: &Path, profile: Option<&str>) -> Result<()> {
    let mut object = load_config_object(path)?;
    if let Some(profile) = profile {
        object.insert(
            "profile".to_string(),
            serde_json::Value::String(profile.to_string()),
        );
    } else {
        object.remove("profile");
    }
    save_config_object(path, &object)
}

fn load_config_object(path: &Path) -> Result<serde_json::Map<String, serde_json::Value>> {
    if !path.exists() {
        return Ok(serde_json::Map::new());
    }

    let data = fs::read_to_string(path)
        .with_context(|| format!("failed to read config file {}", path.display()))?;
    let value: serde_json::Value = serde_json::from_str(&data)
        .with_context(|| format!("failed to parse config file {}", path.display()))?;
    let object = value.as_object().cloned().ok_or_else(|| {
        anyhow::anyhow!("config file {} must contain a JSON object", path.display())
    })?;
    Ok(object)
}

fn save_config_object(
    path: &Path,
    object: &serde_json::Map<String, serde_json::Value>,
) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create config directory {}", parent.display()))?;
    }
    let json = serde_json::to_string_pretty(&serde_json::Value::Object(object.clone()))
        .with_context(|| format!("failed to serialize config file {}", path.display()))?;
    fs::write(path, format!("{json}\n"))
        .with_context(|| format!("failed to write config file {}", path.display()))?;
    Ok(())
}

fn resolve_profile_config_write_path(local: bool, global: bool) -> Result<PathBuf> {
    if local && global {
        bail!("--local and --global cannot be used together");
    }

    if global {
        return global_config_path();
    }
    if local {
        return local_config_path().ok_or_else(|| {
            anyhow::anyhow!(
                "no local .bt directory found. Create one first (for example by running `bt init` when available)"
            )
        });
    }

    if let Some(path) = local_config_path() {
        return Ok(path);
    }
    global_config_path()
}

fn global_config_path() -> Result<PathBuf> {
    #[cfg(windows)]
    {
        let app_data =
            std::env::var_os("APPDATA").ok_or_else(|| anyhow::anyhow!("APPDATA is not set"))?;
        return Ok(PathBuf::from(app_data).join("bt").join("config.json"));
    }

    #[cfg(not(windows))]
    {
        if let Some(xdg_config_home) = std::env::var_os("XDG_CONFIG_HOME") {
            return Ok(PathBuf::from(xdg_config_home)
                .join("bt")
                .join("config.json"));
        }

        let home = home_dir().ok_or_else(|| anyhow::anyhow!("HOME is not set"))?;
        Ok(home.join(".config").join("bt").join("config.json"))
    }
}

fn local_config_path() -> Option<PathBuf> {
    find_local_bt_dir().map(|dir| dir.join("config.json"))
}

fn find_local_bt_dir() -> Option<PathBuf> {
    let home = home_dir();
    let mut current_dir = std::env::current_dir().ok()?;

    loop {
        if current_dir.join(".bt").is_dir() {
            return Some(current_dir.join(".bt"));
        }
        if current_dir.join(".git").exists() {
            return None;
        }
        if Some(&current_dir) == home.as_ref() {
            return None;
        }
        if !current_dir.pop() {
            return None;
        }
    }
}

fn home_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        std::env::var_os("USERPROFILE").map(PathBuf::from)
    }

    #[cfg(not(windows))]
    {
        std::env::var_os("HOME").map(PathBuf::from)
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
            prefer_profile: false,
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
                auth_kind: AuthKind::ApiKey,
                api_url: Some("https://api.example.com".to_string()),
                app_url: Some("https://www.example.com".to_string()),
                org_name: Some("Example Org".to_string()),
                oauth_client_id: None,
                oauth_access_expires_at: None,
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
                auth_kind: AuthKind::ApiKey,
                api_url: Some("https://api.example.com".to_string()),
                app_url: Some("https://www.example.com".to_string()),
                org_name: Some("Example Org".to_string()),
                oauth_client_id: None,
                oauth_access_expires_at: None,
            },
        );

        let resolved = resolve_auth_from_store_with_secret_lookup(&base, &store, |_| {
            Ok(Some("profile-key".to_string()))
        })
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
        store.active_profile = Some("work".to_string());
        store.profiles.insert(
            "work".to_string(),
            AuthProfile {
                auth_kind: AuthKind::ApiKey,
                api_url: Some("https://api.example.com".to_string()),
                app_url: None,
                org_name: None,
                oauth_client_id: None,
                oauth_access_expires_at: None,
            },
        );

        let resolved = resolve_auth_from_store(&base, &store).expect("resolve");
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
            },
        );

        let resolved = resolve_auth_from_store_with_secret_lookup(&base, &store, |_| {
            Ok(Some("profile-key".to_string()))
        })
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
            },
        );

        let resolved = resolve_auth_from_store_with_secret_lookup(&base, &store, |_| {
            Ok(Some("should-not-be-used".to_string()))
        })
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

    fn oauth_ignored_api_key_warning_is_none_without_api_key() {
        let base = make_base();
        assert_eq!(oauth_ignored_api_key_warning(&base), None);
    }

    #[test]
    fn oauth_ignored_api_key_warning_is_some_with_api_key() {
        let mut base = make_base();
        base.api_key = Some("secret".to_string());
        let warning = oauth_ignored_api_key_warning(&base).expect("warning");
        assert!(warning.contains("ignoring it"));
        assert!(warning.contains("--oauth"));
    }
}
