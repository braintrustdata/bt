use std::collections::{BTreeMap, BTreeSet};
use std::error::Error as StdError;
use std::fs;
use std::io::Write;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Mutex,
};
use std::time::Duration;

use actix_web::{dev::ServerHandle, web, App, HttpResponse, HttpServer};
use anyhow::{bail, Context, Result};
use base64::Engine as _;
use braintrust_sdk_rust::{BraintrustClient, LoginState};
use chrono::{DateTime, Months, Utc};
use clap::{Args, Subcommand};
use crossterm::event::{self, Event, KeyCode, KeyEventKind};
use dialoguer::{Confirm, Input, Password};
use oauth2::basic::BasicClient;
use oauth2::{
    AuthUrl, ClientId, CsrfToken, PkceCodeChallenge, PkceCodeVerifier, RedirectUrl, Scope, TokenUrl,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::oneshot;

use crate::{
    args::{BaseArgs, DEFAULT_API_URL, DEFAULT_APP_URL},
    config,
    http::{build_http_client, build_http_client_from_builder, ApiClient},
    projects::api,
    ui,
    utils::shell_quote_arg,
};

const KEYCHAIN_SERVICE: &str = "com.braintrust.bt.cli";
const OAUTH_CLIENT_ID: &str = "bt_cli";
const OAUTH_SCOPE: &str = "mcp";
const OAUTH_CALLBACK_TIMEOUT: Duration = Duration::from_secs(300);
const OAUTH_REFRESH_SAFETY_WINDOW_SECONDS: u64 = 60;
const AI_PROVIDER_KEY_STALENESS_CHECK_INTERVAL_SECONDS: i64 = 24 * 60 * 60;
static SECRET_STORE_FALLBACK_WARNED: AtomicBool = AtomicBool::new(false);
static AI_PROVIDER_KEY_STALENESS_WARNED: AtomicBool = AtomicBool::new(false);

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
    slot_key: Option<String>,
}

#[derive(Debug, Clone)]
pub struct ProfileInfo {
    pub auth_method: String,
    pub org_name: Option<String>,
    pub user_name: Option<String>,
    pub email: Option<String>,
    pub api_key_hint: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AvailableOrg {
    pub id: String,
    pub name: String,
    pub api_url: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RecoverableAuthErrorKind {
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
                    RecoverableAuthErrorKind::OauthRefreshToken
                        | RecoverableAuthErrorKind::StoredCredential
                )
            })
    })
}

pub fn list_profiles() -> Result<Vec<ProfileInfo>> {
    let store = load_auth_store()?;
    Ok(store
        .profiles
        .values()
        .map(profile_info_from_store_entry)
        .collect())
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

    available_orgs(&api_key, &app_url).await
}

pub(crate) async fn list_available_orgs_for_api_key(
    api_key: &str,
    app_url: &str,
) -> Result<Vec<AvailableOrg>> {
    available_orgs(api_key, app_url).await
}

async fn available_orgs(api_key: &str, app_url: &str) -> Result<Vec<AvailableOrg>> {
    let mut orgs = fetch_login_orgs(api_key, app_url).await?;
    sort_login_orgs(&mut orgs);
    Ok(orgs
        .into_iter()
        .map(|org| AvailableOrg {
            id: org.id,
            name: org.name,
            api_url: org.api_url,
        })
        .collect())
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
struct AuthStore {
    #[serde(default)]
    profiles: BTreeMap<String, AuthProfile>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct SecretStore {
    #[serde(default)]
    secrets: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq, Eq)]
struct AuthProfile {
    #[serde(default)]
    auth_kind: AuthKind,
    #[serde(default)]
    api_url: Option<String>,
    #[serde(default)]
    app_url: Option<String>,
    #[serde(default)]
    org_id: Option<String>,
    #[serde(default)]
    org_name: Option<String>,
    #[serde(default)]
    oauth_access_expires_at: Option<u64>,
    #[serde(default)]
    user_name: Option<String>,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    api_key_hash: Option<String>,
    #[serde(default)]
    api_key_hint: Option<String>,
    #[serde(default)]
    legacy_secret_key: Option<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
enum AuthKind {
    #[default]
    ApiKey,
    Oauth,
}

fn auth_kind_label(kind: AuthKind) -> &'static str {
    match kind {
        AuthKind::ApiKey => "api_key",
        AuthKind::Oauth => "oauth",
    }
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
  bt auth login --global
  bt auth login --oauth --org test-org --local
  bt auth logins --org test-org --prefer-api-key
  bt auth refresh --org test-org
  bt auth logout
  bt auth logout --org test-org --oauth
")]
pub struct AuthArgs {
    #[command(subcommand)]
    command: AuthCommand,
}

#[derive(Debug, Clone, Subcommand)]
enum AuthCommand {
    /// Authenticate with Braintrust (OAuth or API key)
    Login(AuthLoginArgs),
    /// Force-refresh OAuth access token for the selected org
    Refresh,
    /// List saved auth logins and check connection status
    Logins(AuthLoginsArgs),
    /// Log out by removing a saved auth login
    Logout(AuthLogoutArgs),
}

#[derive(Debug, Clone, Args)]
struct AuthLoginsArgs {}

#[derive(Debug, Clone, Args)]
struct AuthLoginArgs {
    /// Use OAuth login instead of API key login
    #[arg(long)]
    oauth: bool,

    /// Do not try to open a browser automatically
    #[arg(long)]
    no_browser: bool,

    #[command(flatten)]
    scope: config::ScopeArgs,
}

#[derive(Debug, Clone, Args)]
struct AuthLogoutArgs {
    /// Only consider OAuth logins
    #[arg(long, conflicts_with = "api_key_hint")]
    oauth: bool,

    /// API key hint to log out of when multiple API keys exist for an org
    #[arg(long = "api-key-hint", value_name = "HINT")]
    api_key_hint: Option<String>,

    /// Skip confirmation prompt
    #[arg(long, short = 'f')]
    force: bool,
}

struct PostLoginContextUpdate {
    display: String,
    path: PathBuf,
}

pub async fn run(base: BaseArgs, args: AuthArgs) -> Result<()> {
    match args.command {
        AuthCommand::Login(login_args) => {
            login_args.scope.preflight(ui::can_prompt())?;
            run_login_set(&base, login_args).await
        }
        AuthCommand::Refresh => run_login_refresh(&base).await,
        AuthCommand::Logins(logins_args) => run_logins(&base, logins_args).await,
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
        Ok(client) => match client.wait_for_login().await {
            Ok(login) => login,
            Err(err) => {
                let err: anyhow::Error = err.into();
                if !auth.is_oauth && is_unauthorized_auth_error(&err) {
                    return Err(err.context("API key is not valid"));
                }
                return Err(err);
            }
        },
        Err(_err) if auth.is_oauth => {
            let org_name = auth.org_name.clone().unwrap_or_default();
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
        Err(err) => {
            let err: anyhow::Error = err.into();
            if is_unauthorized_auth_error(&err) {
                return Err(err.context("API key is not valid"));
            }
            return Err(err);
        }
    };

    reconcile_resolved_auth_slot(&auth, &login)?;

    let api_url = login
        .api_url()
        .or(auth.api_url.clone())
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());

    let app_url = auth
        .app_url
        .clone()
        .unwrap_or_else(|| DEFAULT_APP_URL.to_string());

    let ctx = LoginContext {
        login,
        api_url,
        app_url,
    };
    maybe_warn_ai_provider_key_staleness(base, &ctx).await;
    Ok(ctx)
}

#[derive(Debug, Deserialize)]
struct AiProviderSecret {
    #[serde(default)]
    id: Option<String>,
    name: String,
    #[serde(default)]
    r#type: Option<String>,
    #[serde(default)]
    preview_secret: Option<String>,
    #[serde(default)]
    secret_updated_at: Option<String>,
    #[serde(default)]
    updated_at: Option<String>,
    #[serde(default)]
    created: Option<String>,
}

#[derive(Debug, Default, Deserialize, Serialize)]
struct AiProviderKeyStalenessWarningState {
    #[serde(default)]
    warned: BTreeSet<String>,
    #[serde(default)]
    last_checked_at: BTreeMap<String, String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct StaleAiProviderSecret {
    name: String,
    warning_key: String,
}

fn parse_ai_provider_secret_timestamp(value: Option<&str>) -> Option<DateTime<Utc>> {
    let value = value?.trim();
    if value.is_empty() {
        return None;
    }
    DateTime::parse_from_rfc3339(value)
        .ok()
        .map(|parsed| parsed.with_timezone(&Utc))
}

fn ai_provider_warning_state_path() -> Result<PathBuf> {
    Ok(crate::config::global_config_dir()?.join("ai_provider_key_warnings.json"))
}

fn load_ai_provider_warning_state() -> AiProviderKeyStalenessWarningState {
    let Ok(path) = ai_provider_warning_state_path() else {
        return AiProviderKeyStalenessWarningState::default();
    };
    let Ok(contents) = fs::read_to_string(path) else {
        return AiProviderKeyStalenessWarningState::default();
    };
    serde_json::from_str(&contents).unwrap_or_default()
}

fn save_ai_provider_warning_state(state: &AiProviderKeyStalenessWarningState) -> Result<()> {
    let path = ai_provider_warning_state_path()?;
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;

    let json = serde_json::to_string_pretty(state)?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(json.as_bytes())?;
    file.write_all(b"\n")?;
    file.as_file().sync_all()?;
    file.persist(path)?;
    Ok(())
}

fn stale_ai_provider_secrets(
    org_id: &str,
    secrets: &[AiProviderSecret],
    now: DateTime<Utc>,
) -> Vec<StaleAiProviderSecret> {
    let Some(cutoff) = now.checked_sub_months(Months::new(6)) else {
        return Vec::new();
    };

    let mut stale = secrets
        .iter()
        .filter(|secret| {
            secret
                .preview_secret
                .as_deref()
                .is_some_and(|preview| !preview.trim().is_empty())
        })
        .filter_map(|secret| {
            let updated_at_raw = secret
                .secret_updated_at
                .as_deref()
                .or(secret.updated_at.as_deref())
                .or(secret.created.as_deref())?;
            let updated_at = parse_ai_provider_secret_timestamp(Some(updated_at_raw))?;
            if updated_at >= cutoff {
                return None;
            }
            let identity = secret
                .id
                .as_deref()
                .or(secret.r#type.as_deref())
                .unwrap_or(&secret.name);
            let name = secret.name.clone();
            let warning_key = format!("{org_id}:{identity}:{updated_at_raw}");
            Some(StaleAiProviderSecret { name, warning_key })
        })
        .collect::<Vec<_>>();
    stale.sort_by(|a, b| a.name.cmp(&b.name));
    stale
}

fn unwarned_stale_ai_provider_secrets(
    stale: Vec<StaleAiProviderSecret>,
    state: &AiProviderKeyStalenessWarningState,
) -> Vec<StaleAiProviderSecret> {
    stale
        .into_iter()
        .filter(|secret| !state.warned.contains(&secret.warning_key))
        .collect()
}

fn should_check_ai_provider_key_staleness(
    state: &AiProviderKeyStalenessWarningState,
    org_id: &str,
    now: DateTime<Utc>,
) -> bool {
    let Some(last_checked_at) = state
        .last_checked_at
        .get(org_id)
        .and_then(|value| parse_ai_provider_secret_timestamp(Some(value)))
    else {
        return true;
    };
    if last_checked_at > now {
        return true;
    }
    now.signed_duration_since(last_checked_at).num_seconds()
        >= AI_PROVIDER_KEY_STALENESS_CHECK_INTERVAL_SECONDS
}

fn record_ai_provider_key_staleness_check(
    state: &mut AiProviderKeyStalenessWarningState,
    org_id: &str,
    now: DateTime<Utc>,
) {
    state
        .last_checked_at
        .insert(org_id.to_string(), now.to_rfc3339());
}

fn ai_provider_key_staleness_warning_message(secret_name: &str) -> String {
    format!(
        "We recommend disabling and rotating AI provider secrets periodically. {secret_name} has not been rotated in over 6 months."
    )
}

async fn maybe_warn_ai_provider_key_staleness(base: &BaseArgs, ctx: &LoginContext) {
    if base.json
        || ui::is_quiet()
        || ctx
            .login
            .org_id()
            .is_none_or(|org_id| org_id.trim().is_empty())
    {
        return;
    }
    if AI_PROVIDER_KEY_STALENESS_WARNED.swap(true, Ordering::Relaxed) {
        return;
    }

    let _ = warn_ai_provider_key_staleness(ctx).await;
}

async fn warn_ai_provider_key_staleness(ctx: &LoginContext) -> Result<()> {
    let app_url = ctx.app_url.trim_end_matches('/');
    let url = format!("{app_url}/api/ai_secret/get");
    let api_key = ctx.login.api_key().context("login state missing API key")?;
    let org_id = ctx
        .login
        .org_id()
        .context("login state missing organization id")?;
    let now = Utc::now();
    let mut state = load_ai_provider_warning_state();
    if !should_check_ai_provider_key_staleness(&state, &org_id, now) {
        return Ok(());
    }
    record_ai_provider_key_staleness_check(&mut state, &org_id, now);
    let _ = save_ai_provider_warning_state(&state);

    let client = build_http_client(crate::http::DEFAULT_HTTP_TIMEOUT)?;
    let response = client
        .post(&url)
        .bearer_auth(api_key)
        .json(&json!({ "org_id": org_id }))
        .send()
        .await
        .with_context(|| format!("failed to call AI provider secrets endpoint {url}"))?;

    if !response.status().is_success() {
        return Ok(());
    }

    let secrets = response
        .json::<Vec<AiProviderSecret>>()
        .await
        .context("failed to parse AI provider secrets response")?;
    let stale = stale_ai_provider_secrets(&org_id, &secrets, now);
    if stale.is_empty() {
        return Ok(());
    }
    let to_warn = unwarned_stale_ai_provider_secrets(stale, &state);
    if to_warn.is_empty() {
        return Ok(());
    }

    for secret in &to_warn {
        ui::print_command_status(
            ui::CommandStatus::Warning,
            &ai_provider_key_staleness_warning_message(&secret.name),
        );
    }
    state
        .warned
        .extend(to_warn.into_iter().map(|secret| secret.warning_key));
    let _ = save_ai_provider_warning_state(&state);
    Ok(())
}

fn has_cached_project_id(base: &BaseArgs) -> bool {
    crate::config::configured_project_id_for_base(base)
        .is_some_and(|project_id| !project_id.trim().is_empty())
}

fn is_unauthorized_auth_error(err: &anyhow::Error) -> bool {
    err.chain().any(|source| {
        if let Some(http_error) = source.downcast_ref::<crate::http::HttpError>() {
            return matches!(http_error.status.as_u16(), 401 | 403);
        }
        if let Some(sdk_error) = source.downcast_ref::<braintrust_sdk_rust::BraintrustError>() {
            return matches!(
                sdk_error,
                braintrust_sdk_rust::BraintrustError::Api {
                    status: 401 | 403,
                    ..
                }
            );
        }
        false
    })
}

fn resolve_cli_api_key_override(base: &BaseArgs) -> Option<String> {
    if matches!(
        base.api_key_source,
        Some(crate::args::ArgValueSource::EnvVariable)
    ) {
        return None;
    }
    let value = base.api_key.as_deref()?.trim();
    if value.is_empty() {
        return None;
    }
    Some(value.to_string())
}

fn resolve_env_api_key(base: &BaseArgs) -> Option<String> {
    if !matches!(
        base.api_key_source,
        Some(crate::args::ArgValueSource::EnvVariable)
    ) {
        return None;
    }
    let value = base.api_key.as_deref()?.trim();
    if value.is_empty() {
        return None;
    }
    Some(value.to_string())
}

fn config_auth_context(base: &BaseArgs) -> Option<String> {
    let cfg = crate::config::load().unwrap_or_default();
    config_auth_context_from_config(base, &cfg)
}

fn config_auth_context_from_config(base: &BaseArgs, cfg: &crate::config::Config) -> Option<String> {
    if crate::config::org_option(base.org_name.as_deref()).is_none() {
        crate::config::org_option(cfg.org.as_deref()).map(str::to_string)
    } else {
        None
    }
}

fn effective_org_name<'a>(base: &'a BaseArgs, cfg_org: &'a Option<String>) -> Option<&'a str> {
    crate::config::org_option(base.org_name.as_deref())
        .or_else(|| crate::config::org_option(cfg_org.as_deref()))
}

/// The auth source selected by the precedence ladder, before any live
/// credential is fetched. `resolve_auth` turns this into a `ResolvedAuth`
/// (fetching/refreshing tokens); `active_auth_info` turns it into a
/// `ProfileInfo` for display. Both share [`resolve_auth_source`] so the
/// precedence order documented in the README lives in exactly one place.
#[derive(Debug, Clone, PartialEq, Eq)]
enum AuthSource {
    CliApiKey(String),
    EnvApiKey(String),
    Oauth(String),
    ApiKey(String),
    None,
}

/// Pure auth-source precedence ladder (README "Auth resolution order"):
/// 1. explicit `--api-key`
/// 2. `--prefer-api-key`: `BRAINTRUST_API_KEY` → stored API key → OAuth fallback
/// 3. stored OAuth login for the selected org
/// 4. `BRAINTRUST_API_KEY`
/// 5. stored API key login for the selected org
///
/// The slot selectors return `Ok(None)` when no candidate matches (the ladder
/// continues) and may return `Err` for an ambiguous selection that neither
/// caller can resolve without prompting (the ladder stops).
fn resolve_auth_source(
    prefer_api_key: bool,
    cli_api_key: Option<String>,
    env_api_key: impl Fn() -> Option<String>,
    select_oauth: impl Fn() -> Result<Option<String>>,
    select_api_key: impl Fn() -> Result<Option<String>>,
) -> Result<AuthSource> {
    if let Some(api_key) = cli_api_key {
        return Ok(AuthSource::CliApiKey(api_key));
    }

    if prefer_api_key {
        if let Some(api_key) = env_api_key() {
            return Ok(AuthSource::EnvApiKey(api_key));
        }
        if let Some(slot) = select_api_key()? {
            return Ok(AuthSource::ApiKey(slot));
        }
        if let Some(slot) = select_oauth()? {
            return Ok(AuthSource::Oauth(slot));
        }
        return Ok(AuthSource::None);
    }

    if let Some(slot) = select_oauth()? {
        return Ok(AuthSource::Oauth(slot));
    }
    if let Some(api_key) = env_api_key() {
        return Ok(AuthSource::EnvApiKey(api_key));
    }
    if let Some(slot) = select_api_key()? {
        return Ok(AuthSource::ApiKey(slot));
    }
    Ok(AuthSource::None)
}

pub async fn resolve_auth(base: &BaseArgs) -> Result<ResolvedAuth> {
    let mut store = load_auth_store()?;
    let cfg_org = config_auth_context(base);
    let can_prompt = ui::can_prompt();

    let effective_org = effective_org_name(base, &cfg_org);
    reject_cross_org_api_key_preference(base.prefer_api_key, effective_org, &store)?;

    if let Some(slot) = base.pinned_auth_slot.clone() {
        return resolve_saved_auth_slot(base, &mut store, &None, &slot).await;
    }

    let source = resolve_auth_source(
        base.prefer_api_key,
        resolve_cli_api_key_override(base),
        || resolve_env_api_key(base),
        || select_profile_for_auth(base, &store, &cfg_org, AuthKind::Oauth, can_prompt),
        || select_profile_for_auth(base, &store, &cfg_org, AuthKind::ApiKey, can_prompt),
    )?;

    match source {
        AuthSource::CliApiKey(api_key) | AuthSource::EnvApiKey(api_key) => {
            resolve_ad_hoc_api_key_auth(base, &cfg_org, api_key).await
        }
        AuthSource::Oauth(slot) | AuthSource::ApiKey(slot) => {
            resolve_saved_auth_slot(base, &mut store, &cfg_org, &slot).await
        }
        AuthSource::None => {
            if base.prefer_api_key {
                bail!("--prefer-api-key requires an API key or OAuth login for the selected org");
            }
            if effective_org.is_none() {
                if let Some(err) = missing_org_for_stored_logins_error(&store) {
                    return Err(err);
                }
            }
            Ok(ResolvedAuth {
                api_key: None,
                api_url: base.api_url.clone(),
                app_url: base.app_url.clone(),
                org_name: effective_org.map(str::to_string),
                is_oauth: false,
                slot_key: None,
            })
        }
    }
}

async fn resolve_ad_hoc_api_key_auth(
    base: &BaseArgs,
    cfg_org: &Option<String>,
    api_key: String,
) -> Result<ResolvedAuth> {
    let requested_org = effective_org_name(base, cfg_org);
    if requested_org == Some("") {
        bail!("API keys require a concrete org; rerun with --org <ORG>");
    }

    let mut resolved_org = requested_org.map(str::to_string);
    let mut resolved_api_url = base.api_url.clone();
    if let Some(requested_org) = requested_org {
        if crate::args::custom_api_without_app_url(base.api_url.as_deref(), base.app_url.as_deref())
        {
            bail!("API key organization validation with a custom API URL requires --app-url or BRAINTRUST_APP_URL");
        }
        let app_url = base.app_url.as_deref().unwrap_or(DEFAULT_APP_URL);
        let orgs = fetch_login_orgs(&api_key, app_url).await.map_err(|err| {
            if is_unauthorized_auth_error(&err) {
                anyhow::anyhow!("API key is not valid")
            } else {
                err.context("failed to validate API key organization membership")
            }
        })?;
        let selected_org = find_login_org(&orgs, requested_org).ok_or_else(|| {
            let available = login_org_names(&orgs);
            anyhow::anyhow!(
                "API key does not belong to requested org '{requested_org}'. Available orgs for this key: {available}"
            )
        })?;
        resolved_org = Some(selected_org.name.clone());
        resolved_api_url = resolved_api_url.or_else(|| selected_org.api_url.clone());
    }

    Ok(ResolvedAuth {
        api_key: Some(api_key),
        api_url: resolved_api_url,
        app_url: base.app_url.clone(),
        org_name: resolved_org,
        is_oauth: false,
        slot_key: None,
    })
}

async fn resolve_saved_auth_slot(
    base: &BaseArgs,
    store: &mut AuthStore,
    cfg_org: &Option<String>,
    slot: &str,
) -> Result<ResolvedAuth> {
    let kind = store
        .profiles
        .get(slot)
        .map(|profile| profile.auth_kind)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "saved auth login not found; run `bt auth logins` to see available logins"
            )
        })?;
    match kind {
        AuthKind::ApiKey => resolve_api_key_profile_auth(base, store, cfg_org, slot),
        AuthKind::Oauth => resolve_oauth_profile_auth(base, store, cfg_org, slot).await,
    }
}

fn resolve_api_key_profile_auth(
    base: &BaseArgs,
    store: &mut AuthStore,
    cfg_org: &Option<String>,
    profile_name: &str,
) -> Result<ResolvedAuth> {
    let profile = store
        .profiles
        .get(profile_name)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("saved auth login not found; run `bt auth logins`"))?;
    if let Some(requested_org) = effective_org_name(base, cfg_org) {
        if !profile_matches_org_identifier(&profile, requested_org) {
            bail!(
                "stored API key for '{}' does not belong to requested org '{requested_org}'",
                profile_org_label(&profile)
            );
        }
    }

    let api_key = load_profile_secret_with_legacy(
        profile_name,
        profile.legacy_secret_key.as_deref(),
    )?
    .ok_or_else(|| {
        recoverable_auth_error(
            RecoverableAuthErrorKind::StoredCredential,
            format!(
                "no keychain credential found for auth login '{}'; re-run `bt auth login --org <ORG> --api-key <KEY>`",
                auth_slot_label(&profile)
            ),
        )
    })?;

    let resolved = ResolvedAuth {
        api_key: Some(api_key.clone()),
        api_url: base.api_url.clone().or_else(|| profile.api_url.clone()),
        app_url: base.app_url.clone().or_else(|| profile.app_url.clone()),
        org_name: effective_org_name(base, cfg_org)
            .map(str::to_string)
            .or_else(|| profile.org_name.clone()),
        is_oauth: false,
        slot_key: Some(profile_name.to_string()),
    };

    maybe_rekey_api_key_profile_after_secret_load(store, profile_name, &api_key)?;
    Ok(resolved)
}

fn replace_with_canonical_auth_profile(
    store: &mut AuthStore,
    current_key: &str,
    mut profile: AuthProfile,
) -> bool {
    let canonical_key = canonical_profile_key(current_key, &profile);
    if canonical_key != current_key && profile.legacy_secret_key.is_none() {
        // Keep the old key as a lazy keychain fallback. Secrets are relocated
        // only when they are next saved, avoiding platform-specific migration
        // work while auth.json is being upgraded.
        profile.legacy_secret_key = Some(current_key.to_string());
    }

    let unchanged = canonical_key == current_key
        && store
            .profiles
            .get(current_key)
            .is_some_and(|existing| existing == &profile);
    if unchanged {
        return false;
    }

    if canonical_key != current_key {
        store.profiles.remove(current_key);
        if let Some(existing) = store.profiles.get(&canonical_key) {
            if !should_replace_migrated_profile(existing, &profile) {
                return true;
            }
        }
    }
    store.profiles.insert(canonical_key, profile);
    true
}

fn maybe_rekey_api_key_profile_after_secret_load(
    store: &mut AuthStore,
    profile_name: &str,
    api_key: &str,
) -> Result<()> {
    let Some(mut profile) = store.profiles.get(profile_name).cloned() else {
        return Ok(());
    };
    if profile.auth_kind != AuthKind::ApiKey
        || profile
            .org_id
            .as_deref()
            .map(str::trim)
            .is_none_or(str::is_empty)
    {
        return Ok(());
    }

    profile.api_key_hash = Some(api_key_hash(api_key));
    if replace_with_canonical_auth_profile(store, profile_name, profile) {
        save_auth_store(store)?;
    }
    Ok(())
}

async fn reconcile_oauth_slot_from_access_token(
    store: &mut AuthStore,
    slot_key: &str,
    access_token: &str,
    app_url: &str,
) -> Result<()> {
    let Some(mut profile) = store.profiles.get(slot_key).cloned() else {
        return Ok(());
    };
    if profile.auth_kind != AuthKind::Oauth || profile.org_id.is_some() {
        return Ok(());
    }

    let Some(org_name) = profile
        .org_name
        .as_deref()
        .map(str::trim)
        .filter(|org| !org.is_empty())
    else {
        profile.org_id = Some(String::new());
        if replace_with_canonical_auth_profile(store, slot_key, profile) {
            save_auth_store(store)?;
        }
        return Ok(());
    };

    // A legacy auth entry only cached the org name. Resolve the stable ID from
    // the newly refreshed token; a failed best-effort lookup must not turn a
    // successful token refresh into a failed command.
    let Ok(orgs) = fetch_login_orgs(access_token, app_url).await else {
        return Ok(());
    };
    let Some(org) = find_login_org(&orgs, org_name) else {
        return Ok(());
    };

    profile.org_id = Some(org.id.clone());
    profile.org_name = Some(org.name.clone());
    let identity = decode_jwt_identity(access_token);
    if identity.email.is_some() {
        profile.email = identity.email;
    }
    if identity.name.is_some() {
        profile.user_name = identity.name;
    }
    if replace_with_canonical_auth_profile(store, slot_key, profile) {
        save_auth_store(store)?;
    }
    Ok(())
}

fn reconcile_resolved_auth_slot(auth: &ResolvedAuth, login: &LoginState) -> Result<()> {
    let Some(slot_key) = auth.slot_key.as_deref() else {
        return Ok(());
    };
    let mut store = load_auth_store()?;
    let Some(mut profile) = store.profiles.get(slot_key).cloned() else {
        return Ok(());
    };

    let login_org_id = login.org_id().unwrap_or_default();
    let is_cross_org = profile.auth_kind == AuthKind::Oauth
        && auth
            .org_name
            .as_deref()
            .is_none_or(|org| org.trim().is_empty());
    if login_org_id.trim().is_empty() && !is_cross_org {
        // The SDK's OAuth compatibility path does not always return org
        // metadata. `bt auth logins` performs the same reconciliation after
        // its explicit credential verification request.
        return Ok(());
    }

    profile.org_id = Some(login_org_id);
    profile.org_name = if is_cross_org {
        None
    } else {
        login
            .org_name()
            .filter(|org| !org.trim().is_empty())
            .or_else(|| auth.org_name.clone())
    };
    match profile.auth_kind {
        AuthKind::ApiKey => {
            let Some(api_key) = auth.api_key.as_deref() else {
                return Ok(());
            };
            profile.api_key_hash = Some(api_key_hash(api_key));
            if profile.api_key_hint.is_none() {
                profile.api_key_hint = Some(obscure_api_key(api_key));
            }
        }
        AuthKind::Oauth if profile.email.as_deref().is_none_or(str::is_empty) => return Ok(()),
        AuthKind::Oauth => {}
    }

    if replace_with_canonical_auth_profile(&mut store, slot_key, profile) {
        save_auth_store(&store)?;
    }
    Ok(())
}

async fn resolve_oauth_profile_auth(
    base: &BaseArgs,
    store: &mut AuthStore,
    cfg_org: &Option<String>,
    profile_name: &str,
) -> Result<ResolvedAuth> {
    let profile = store
        .profiles
        .get(profile_name)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("saved OAuth login not found; run `bt auth logins`"))?;
    let api_url = base
        .api_url
        .clone()
        .or_else(|| profile.api_url.clone())
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());
    let app_url = base.app_url.clone().or_else(|| profile.app_url.clone());
    let org_name = effective_org_name(base, cfg_org)
        .map(str::to_string)
        .or_else(|| profile.org_name.clone());

    let mut auth = ResolvedAuth {
        api_key: None,
        api_url: Some(api_url.clone()),
        app_url,
        org_name,
        is_oauth: true,
        slot_key: Some(profile_name.to_string()),
    };

    if let Some(cached_access_token) = load_valid_cached_oauth_access_token(
        profile_name,
        &profile,
        profile.oauth_access_expires_at,
    )? {
        auth.api_key = Some(cached_access_token);
        return Ok(auth);
    }

    let refresh_token = load_profile_oauth_refresh_token_for_profile(profile_name, &profile)?
        .ok_or_else(|| {
            recoverable_auth_error(
                RecoverableAuthErrorKind::OauthRefreshToken,
                format!(
                    "oauth refresh token missing for '{}'; re-run `{}`",
                    auth_slot_label(&profile),
                    oauth_reauth_command(&profile)
                ),
            )
        })?;
    let refreshed = refresh_oauth_access_token(&api_url, &refresh_token, &profile).await?;
    save_profile_oauth_access_token(profile_name, &refreshed.access_token)?;
    let mut refresh_rotated = false;
    if let Some(next_refresh_token) = refreshed.refresh_token.as_ref() {
        if next_refresh_token != &refresh_token {
            save_profile_oauth_refresh_token(profile_name, next_refresh_token)?;
            refresh_rotated = true;
        }
    }
    if !refresh_rotated && profile.legacy_secret_key.is_some() {
        save_profile_oauth_refresh_token(profile_name, &refresh_token)?;
    }
    if let Some(profile) = store.profiles.get_mut(profile_name) {
        profile.oauth_access_expires_at = determine_oauth_access_expiry_epoch(&refreshed);
        if refresh_rotated || profile.legacy_secret_key.is_some() {
            delete_legacy_profile_secrets(profile);
            profile.legacy_secret_key = None;
        }
    }
    save_auth_store(store)?;
    reconcile_oauth_slot_from_access_token(
        store,
        profile_name,
        &refreshed.access_token,
        auth.app_url.as_deref().unwrap_or(DEFAULT_APP_URL),
    )
    .await?;
    auth.api_key = Some(refreshed.access_token);
    Ok(auth)
}

fn auth_env(auth: ResolvedAuth) -> Vec<(String, String)> {
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
    envs
}

pub async fn resolved_runner_env(base: &BaseArgs) -> Result<Vec<(String, String)>> {
    let auth = resolve_auth(base).await?;
    let resolved_org = auth.org_name.clone();
    let mut envs = auth_env(auth);
    let project = base
        .project
        .clone()
        .or_else(|| crate::config::configured_project_for_context(base, resolved_org.as_deref()));
    if let Some(project) = project {
        envs.push(("BRAINTRUST_DEFAULT_PROJECT".to_string(), project));
    }
    Ok(envs)
}

fn profile_matches_org_identifier(profile: &AuthProfile, org: &str) -> bool {
    profile.org_id.as_deref() == Some(org) || profile.org_name.as_deref() == Some(org)
}

fn profile_org(profile: &AuthProfile) -> &str {
    profile
        .org_name
        .as_deref()
        .filter(|org| !org.trim().is_empty())
        .or(profile
            .org_id
            .as_deref()
            .filter(|org| !org.trim().is_empty()))
        .unwrap_or("")
}

fn profile_org_label(profile: &AuthProfile) -> String {
    config::display_org(profile_org(profile)).to_string()
}

fn oauth_reauth_command(profile: &AuthProfile) -> String {
    format!(
        "bt auth login --oauth --org {}",
        shell_quote_arg(config::display_org(profile_org(profile)))
    )
}

pub(crate) fn identity_label(
    name: Option<&str>,
    email: Option<&str>,
    fallback: Option<&str>,
) -> Option<String> {
    match (name, email) {
        (Some(name), Some(email)) => Some(format!("{name} ({email})")),
        (Some(name), None) => Some(name.to_string()),
        (None, Some(email)) => Some(email.to_string()),
        (None, None) => fallback.map(str::to_string),
    }
}

fn profile_identity_label(profile: &AuthProfile) -> Option<String> {
    let fallback = (profile.auth_kind == AuthKind::ApiKey)
        .then_some(profile.api_key_hint.as_deref())
        .flatten();
    identity_label(
        profile.user_name.as_deref(),
        profile.email.as_deref(),
        fallback,
    )
}

fn auth_slot_label(profile: &AuthProfile) -> String {
    let mut parts = vec![profile_org_label(profile)];
    parts.push(auth_kind_label(profile.auth_kind).to_string());
    if let Some(identity) = profile_identity_label(profile) {
        parts.push(identity);
    }
    parts.join(" — ")
}

fn is_cross_org_oauth_profile(profile: &AuthProfile) -> bool {
    profile.auth_kind == AuthKind::Oauth && profile_org(profile).is_empty()
}

fn reject_cross_org_api_key_preference(
    prefer_api_key: bool,
    org: Option<&str>,
    store: &AuthStore,
) -> Result<()> {
    let cross_org = org == Some("")
        || (org.is_none() && store.profiles.values().any(is_cross_org_oauth_profile));
    if prefer_api_key && cross_org {
        bail!("--prefer-api-key cannot be used from cross-org context; rerun with --org <ORG>");
    }
    Ok(())
}

fn auth_profile_names_by_kind<'a>(
    store: &'a AuthStore,
    org: Option<&str>,
    kind: AuthKind,
) -> Vec<&'a str> {
    store
        .profiles
        .iter()
        .filter(|(_, profile)| profile.auth_kind == kind)
        .filter(|(_, profile)| match org {
            Some(org) => profile_matches_org_identifier(profile, org),
            None if kind == AuthKind::Oauth => is_cross_org_oauth_profile(profile),
            None => false,
        })
        .map(|(name, _)| name.as_str())
        .collect()
}

fn profile_info_from_store_entry(profile: &AuthProfile) -> ProfileInfo {
    ProfileInfo {
        auth_method: auth_kind_label(profile.auth_kind).to_string(),
        org_name: profile.org_name.clone(),
        user_name: profile.user_name.clone(),
        email: profile.email.clone(),
        api_key_hint: profile.api_key_hint.clone(),
    }
}

fn profile_info_for_candidate(store: &AuthStore, name: &str) -> Option<ProfileInfo> {
    store.profiles.get(name).map(profile_info_from_store_entry)
}

fn ad_hoc_api_key_profile(org: Option<&str>, api_key: &str) -> ProfileInfo {
    ProfileInfo {
        auth_method: auth_kind_label(AuthKind::ApiKey).to_string(),
        org_name: org.map(str::to_string),
        user_name: None,
        email: None,
        api_key_hint: Some(obscure_api_key(api_key)),
    }
}

pub(crate) fn active_auth_info(base: &BaseArgs, org: Option<&str>) -> Result<Option<ProfileInfo>> {
    let store = load_auth_store().unwrap_or_default();

    reject_cross_org_api_key_preference(base.prefer_api_key, org, &store)?;

    let select = |kind| match auth_profile_names_by_kind(&store, org, kind).as_slice() {
        [] => Ok(None),
        [name] => Ok(Some((*name).to_string())),
        _ => bail!("multiple {kind:?} logins"),
    };

    let source = match resolve_auth_source(
        base.prefer_api_key,
        resolve_cli_api_key_override(base),
        || resolve_env_api_key(base),
        || select(AuthKind::Oauth),
        || select(AuthKind::ApiKey),
    ) {
        Ok(source) => source,
        Err(_) => return Ok(None),
    };

    Ok(match source {
        AuthSource::CliApiKey(api_key) | AuthSource::EnvApiKey(api_key) => {
            Some(ad_hoc_api_key_profile(org, &api_key))
        }
        AuthSource::Oauth(slot) | AuthSource::ApiKey(slot) => {
            profile_info_for_candidate(&store, &slot)
        }
        AuthSource::None => None,
    })
}

fn missing_org_for_stored_logins_error(store: &AuthStore) -> Option<anyhow::Error> {
    let candidates = store
        .profiles
        .iter()
        .filter(|(_, profile)| {
            !is_cross_org_oauth_profile(profile) && !profile_org(profile).is_empty()
        })
        .collect::<Vec<_>>();
    if candidates.is_empty() {
        return None;
    }

    let labels = candidates
        .iter()
        .map(|(_, profile)| auth_slot_label(profile))
        .collect::<Vec<_>>()
        .join(", ");
    let all_api_key = candidates
        .iter()
        .all(|(_, profile)| profile.auth_kind == AuthKind::ApiKey);

    Some(if candidates.len() == 1 {
        anyhow::anyhow!(
            "auth org selection required; pass --org <ORG> to use saved auth login: {labels}"
        )
    } else if all_api_key {
        anyhow::anyhow!(
            "multiple API key logins available: {labels}. Pass --org <ORG> to disambiguate."
        )
    } else {
        anyhow::anyhow!(
            "multiple auth logins available: {labels}. Pass --org <ORG> to disambiguate."
        )
    })
}

fn profile_label_from_store(name: &str, store: &AuthStore) -> String {
    store
        .profiles
        .get(name)
        .map(auth_slot_label)
        .unwrap_or_else(|| "saved auth login".to_string())
}

fn select_profile_from_store(
    prompt: &str,
    names: &[&str],
    current: Option<&str>,
    store: &AuthStore,
) -> Result<String> {
    let labels: Vec<String> = names
        .iter()
        .map(|name| profile_label_from_store(name, store))
        .collect();
    let default = current
        .and_then(|current| {
            names.iter().position(|name| {
                *name == current
                    || store
                        .profiles
                        .get(*name)
                        .is_some_and(|profile| profile_matches_org_identifier(profile, current))
            })
        })
        .unwrap_or(0);
    let label_refs: Vec<&str> = labels.iter().map(String::as_str).collect();
    let idx = ui::fuzzy_select(prompt, &label_refs, default)?;
    Ok(names[idx].to_string())
}

fn saved_login_names(store: &AuthStore, include_cross_org: bool) -> Vec<&str> {
    let mut oauth_orgs = BTreeSet::new();
    store
        .profiles
        .iter()
        .filter(|(_, profile)| {
            profile.auth_kind == AuthKind::ApiKey
                || ((include_cross_org || !is_cross_org_oauth_profile(profile))
                    && oauth_orgs.insert(
                        profile
                            .org_id
                            .as_deref()
                            .filter(|id| !id.is_empty())
                            .unwrap_or_else(|| profile_org(profile))
                            .to_ascii_lowercase(),
                    ))
        })
        .map(|(name, _)| name.as_str())
        .collect()
}

pub(crate) fn select_saved_login(
    base: &mut BaseArgs,
    current_org: Option<&str>,
    include_cross_org: bool,
) -> Result<bool> {
    let store = load_auth_store()?;
    let names = saved_login_names(&store, include_cross_org);
    let selected = match names.as_slice() {
        [] => return Ok(false),
        [name] => (*name).to_string(),
        _ if ui::can_prompt() => {
            select_profile_from_store("Select login", &names, current_org, &store)?
        }
        _ => {
            bail!("multiple saved logins match; pass --org <ORG>, or rerun interactively to choose")
        }
    };
    let profile = &store.profiles[&selected];
    if profile.auth_kind == AuthKind::ApiKey {
        base.pinned_auth_slot = Some(selected);
    } else {
        base.org_name = Some(profile_org(profile).to_string());
    }
    Ok(true)
}

fn candidate_identities<'a>(names: &[&'a str], store: &'a AuthStore) -> Vec<String> {
    names
        .iter()
        .map(|name| {
            store
                .profiles
                .get(*name)
                .map(|profile| {
                    profile_identity_label(profile).unwrap_or_else(|| auth_slot_label(profile))
                })
                .unwrap_or_else(|| "saved auth login".to_string())
        })
        .collect()
}

fn select_profile_for_auth(
    base: &BaseArgs,
    store: &AuthStore,
    cfg_org: &Option<String>,
    kind: AuthKind,
    can_prompt: bool,
) -> Result<Option<String>> {
    let org = effective_org_name(base, cfg_org);
    let candidates = auth_profile_names_by_kind(store, org, kind);
    let label = match kind {
        AuthKind::Oauth => "OAuth login",
        AuthKind::ApiKey => "API key",
    };
    select_auth_profile_candidate(label, org, &candidates, store, can_prompt)
}

fn select_auth_profile_candidate(
    kind_label: &str,
    org: Option<&str>,
    candidates: &[&str],
    store: &AuthStore,
    can_prompt: bool,
) -> Result<Option<String>> {
    match candidates.len() {
        0 => Ok(None),
        1 => Ok(Some(candidates[0].to_string())),
        _ if can_prompt => {
            let prompt = org
                .map(|org| format!("Multiple {kind_label} logins for '{org}'. Select one"))
                .unwrap_or_else(|| format!("Select {kind_label} login"));
            select_profile_from_store(&prompt, candidates, org, store).map(Some)
        }
        _ => {
            let identities = candidate_identities(candidates, store).join(", ");
            if let Some(org) = org {
                bail!(
                    "multiple {kind_label} logins for org '{org}': {identities}. Rerun interactively or remove one with `bt auth logout`."
                );
            }
            bail!(
                "multiple cross-org {kind_label} logins available: {identities}. Rerun interactively or remove one with `bt auth logout`."
            );
        }
    }
}

async fn run_login_set(base: &BaseArgs, args: AuthLoginArgs) -> Result<()> {
    if args.oauth {
        return run_login_oauth(base, args).await;
    }
    if base.org_name.as_deref() == Some("") {
        bail!(
            "API-key login requires a concrete org; cross-org API keys do not exist. Use --oauth, or rerun with --org <ORG>"
        );
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
    let configured_org = config::load().ok().and_then(|cfg| cfg.org);
    let selected_org = select_login_org(
        login_orgs.clone(),
        match requested_org_resolution {
            RequestedOrgResolution::UseRequestedOrg => base.org_name.as_deref(),
            RequestedOrgResolution::NoRequestedOrg | RequestedOrgResolution::IgnoreRequestedOrg => {
                None
            }
            RequestedOrgResolution::SwitchToOauth => unreachable!("handled above"),
        },
        configured_org.as_deref(),
        interactive,
        base.verbose,
        false,
        explicitly_quiet(base),
    )?;
    let selected_org = selected_org.ok_or_else(|| {
        anyhow::anyhow!("API-key login requires an org; pass --org <ORG> or rerun interactively")
    })?;
    let selected_api_url =
        resolve_profile_api_url(base.api_url.clone(), Some(&selected_org), &login_orgs)?;

    commit_api_key_profile(
        &api_key,
        selected_api_url.clone(),
        base.app_url.clone(),
        selected_org.id.clone(),
        selected_org.name.clone(),
    )?;
    let context_update = persist_post_login_context(
        base,
        &api_key,
        &selected_api_url,
        &login_app_url,
        Some(&selected_org),
        &args.scope,
    )
    .await
    .context("login succeeded, but failed to update active context")?;

    let human = format_login_success(Some(&selected_org), &selected_api_url);
    emit_result(
        base.json,
        serde_json::json!({
            "auth": "api_key",
            "org": selected_org.name,
            "org_id": selected_org.id,
            "api_url": selected_api_url,
            "app_url": login_app_url,
            "api_key_hint": obscure_api_key(&api_key),
            "status": "ok",
        }),
        || {
            ui::print_command_status(ui::CommandStatus::Success, &human);
            ui::print_command_status(
                ui::CommandStatus::Success,
                &format!("Switched to {}", context_update.display),
            );
            if base.verbose {
                eprintln!("Wrote to {}", context_update.path.display());
            }
        },
    )
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
    let (pkce_challenge, pkce_verifier) = PkceCodeChallenge::new_random_sha256();
    let state = generate_random_token(32)?;

    let callback_server = bind_oauth_callback_server()?;
    let redirect_uri = callback_server.redirect_uri();
    let oauth_client = build_oauth_client(&api_url, Some(&redirect_uri))?;
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
        callback_server,
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

    let oauth_tokens =
        exchange_oauth_authorization_code(&api_url, &redirect_uri, &auth_code, pkce_verifier)
            .await?;
    let login_orgs = fetch_login_orgs(&oauth_tokens.access_token, &app_url).await?;
    let configured_org = config::load().ok().and_then(|cfg| cfg.org);
    let selected_org = select_login_org(
        login_orgs.clone(),
        base.org_name.as_deref(),
        configured_org.as_deref(),
        ui::can_prompt(),
        base.verbose,
        true,
        explicitly_quiet(base),
    )?;
    let selected_api_url =
        resolve_profile_api_url(base.api_url.clone(), selected_org.as_ref(), &login_orgs)?;

    commit_oauth_profile(
        &oauth_tokens,
        selected_api_url.clone(),
        app_url.clone(),
        selected_org.as_ref(),
    )?;
    let context_update = persist_post_login_context(
        base,
        &oauth_tokens.access_token,
        &selected_api_url,
        &app_url,
        selected_org.as_ref(),
        &args.scope,
    )
    .await
    .context("login succeeded, but failed to update active context")?;

    let human = format_login_success(selected_org.as_ref(), &selected_api_url);
    emit_result(
        base.json,
        serde_json::json!({
            "auth": "oauth",
            "org": selected_org.as_ref().map(|org| org.name.clone()),
            "org_id": selected_org.as_ref().map(|org| org.id.clone()),
            "cross_org": selected_org.is_none(),
            "api_url": selected_api_url,
            "app_url": app_url,
            "status": "ok",
        }),
        || {
            ui::print_command_status(ui::CommandStatus::Success, &human);
            ui::print_command_status(
                ui::CommandStatus::Success,
                &format!("Switched to {}", context_update.display),
            );
            if base.verbose {
                eprintln!("Wrote to {}", context_update.path.display());
            }
        },
    )
}

pub(crate) fn commit_api_key_profile(
    api_key: &str,
    api_url: String,
    app_url: Option<String>,
    org_id: String,
    org_name: String,
) -> Result<()> {
    let hash = api_key_hash(api_key);
    let slot_key = api_key_slot_key(&hash, &org_id);
    save_profile_secret(&slot_key, api_key)?;

    let mut store = load_auth_store()?;
    if let Some(old_profile) = store.profiles.get(&slot_key) {
        delete_legacy_profile_secrets(old_profile);
    }
    store.profiles.insert(
        slot_key,
        AuthProfile {
            auth_kind: AuthKind::ApiKey,
            api_url: Some(api_url),
            app_url,
            org_id: Some(org_id),
            org_name: Some(org_name),
            oauth_access_expires_at: None,
            user_name: None,
            email: None,
            api_key_hash: Some(hash),
            api_key_hint: Some(obscure_api_key(api_key)),
            legacy_secret_key: None,
        },
    );
    save_auth_store(&store)
}

fn commit_oauth_profile(
    tokens: &OAuthTokenResponse,
    api_url: String,
    app_url: String,
    selected_org: Option<&LoginOrgInfo>,
) -> Result<()> {
    let refresh_token = tokens.refresh_token.as_ref().ok_or_else(|| {
        anyhow::anyhow!(
            "oauth token response did not include a refresh_token; cannot create persistent oauth login"
        )
    })?;

    let oauth_access_expires_at = determine_oauth_access_expiry_epoch(tokens);
    let jwt_id = decode_jwt_identity(&tokens.access_token);
    let email = jwt_id
        .email
        .clone()
        .filter(|email| !email.trim().is_empty())
        .ok_or_else(|| {
            anyhow::anyhow!(
                "oauth token did not include an email; cannot create persistent oauth login"
            )
        })?;
    let org_id = selected_org.map(|org| org.id.clone()).unwrap_or_default();
    let slot_key = oauth_slot_key(&org_id, &email);

    save_profile_oauth_refresh_token(&slot_key, refresh_token)?;
    save_profile_oauth_access_token(&slot_key, &tokens.access_token)?;
    let _ = delete_profile_secret(&slot_key);

    let mut store = load_auth_store()?;
    if let Some(old_profile) = store.profiles.get(&slot_key) {
        delete_legacy_profile_secrets(old_profile);
    }
    store.profiles.insert(
        slot_key,
        AuthProfile {
            auth_kind: AuthKind::Oauth,
            api_url: Some(api_url),
            app_url: Some(app_url),
            org_id: Some(org_id),
            org_name: selected_org.map(|org| org.name.clone()),
            oauth_access_expires_at,
            user_name: jwt_id.name,
            email: jwt_id.email,
            api_key_hash: None,
            api_key_hint: None,
            legacy_secret_key: None,
        },
    );
    save_auth_store(&store)
}

async fn run_login_refresh(base: &BaseArgs) -> Result<()> {
    let mut store = load_auth_store()?;
    let cfg_org = config_auth_context(base);
    let profile_name = select_profile_for_auth(
        base,
        &store,
        &cfg_org,
        AuthKind::Oauth,
        ui::can_prompt(),
    )?
    .ok_or_else(|| {
            anyhow::anyhow!(
                "no OAuth login selected; pass --org <ORG> or run `bt auth logins` to see available logins"
            )
        })?;
    let profile = store
        .profiles
        .get(profile_name.as_str())
        .cloned()
        .ok_or_else(|| {
            anyhow::anyhow!("OAuth login not found; run `bt auth logins` to see available logins")
        })?;

    let api_url = profile
        .api_url
        .clone()
        .unwrap_or_else(|| DEFAULT_API_URL.to_string());
    let previous_expires_at = profile.oauth_access_expires_at;
    let refresh_token =
        load_profile_oauth_refresh_token_for_profile(profile_name.as_str(), &profile)?.ok_or_else(
            || {
                anyhow::anyhow!(
                    "OAuth refresh token missing for '{}'; re-run `{}`",
                    auth_slot_label(&profile),
                    oauth_reauth_command(&profile)
                )
            },
        )?;

    eprintln!(
        "Refreshing OAuth token for {} (api_url: {api_url})",
        auth_slot_label(&profile)
    );
    if let Some(expires_at) = previous_expires_at {
        let now = current_unix_timestamp();
        let remaining = expires_at.saturating_sub(now);
        eprintln!(
            "Cached access token expiry before refresh: {expires_at} (about {remaining}s remaining)"
        );
    } else {
        eprintln!("Cached access token expiry before refresh: unknown");
    }

    let refreshed = refresh_oauth_access_token(&api_url, &refresh_token, &profile).await?;
    save_profile_oauth_access_token(profile_name.as_str(), &refreshed.access_token)?;
    let mut refresh_rotated = false;
    if let Some(next_refresh_token) = refreshed.refresh_token.as_ref() {
        if next_refresh_token != &refresh_token {
            save_profile_oauth_refresh_token(profile_name.as_str(), next_refresh_token)?;
            refresh_rotated = true;
        }
    }
    if !refresh_rotated && profile.legacy_secret_key.is_some() {
        save_profile_oauth_refresh_token(profile_name.as_str(), &refresh_token)?;
    }

    let new_expires_at = determine_oauth_access_expiry_epoch(&refreshed);
    if let Some(profile) = store.profiles.get_mut(profile_name.as_str()) {
        profile.oauth_access_expires_at = new_expires_at;
        if refresh_rotated || profile.legacy_secret_key.is_some() {
            delete_legacy_profile_secrets(profile);
            profile.legacy_secret_key = None;
        }
    }
    save_auth_store(&store)?;
    reconcile_oauth_slot_from_access_token(
        &mut store,
        profile_name.as_str(),
        &refreshed.access_token,
        profile.app_url.as_deref().unwrap_or(DEFAULT_APP_URL),
    )
    .await?;

    if let Some(expires_at) = new_expires_at {
        let now = current_unix_timestamp();
        let remaining = expires_at.saturating_sub(now);
        eprintln!("New access token expiry: {expires_at} (about {remaining}s remaining)");
    } else {
        eprintln!("New access token expiry: unknown");
    }
    if refresh_rotated {
        eprintln!("Refresh token rotation: yes");
    } else {
        eprintln!("Refresh token rotation: no");
    }

    emit_result(
        base.json,
        serde_json::json!({
            "auth": "oauth",
            "org": profile.org_name,
            "org_id": profile.org_id.filter(|org_id| !org_id.trim().is_empty()),
            "user_email": profile.email,
            "access_expires_at": new_expires_at,
            "refresh_token_rotated": refresh_rotated,
            "status": "ok",
        }),
        || ui::print_command_status(ui::CommandStatus::Success, "OAuth refresh complete."),
    )
}

fn format_login_success(selected_org: Option<&LoginOrgInfo>, api_url: &str) -> String {
    match selected_org {
        Some(org) => format!("Logged in as {} (api: {api_url})", org.name),
        None => format!("Logged in (cross-org, api: {api_url})"),
    }
}

fn build_login_context_for_selected_org(
    credential: &str,
    api_url: &str,
    app_url: &str,
    selected_org: Option<&LoginOrgInfo>,
) -> LoginContext {
    let login = LoginState::new();
    let _ = login.set(
        credential.to_string(),
        selected_org.map(|org| org.id.clone()).unwrap_or_default(),
        selected_org.map(|org| org.name.clone()).unwrap_or_default(),
        api_url.to_string(),
        app_url.to_string(),
    );
    LoginContext {
        login,
        api_url: api_url.to_string(),
        app_url: app_url.to_string(),
    }
}

fn format_post_login_context(
    selected_org: Option<&LoginOrgInfo>,
    project: Option<&api::Project>,
) -> String {
    match (selected_org, project) {
        (Some(org), Some(project)) => format!("{}/{}", org.name, project.name),
        (Some(org), None) => org.name.clone(),
        (None, _) => "cross-org mode".to_string(),
    }
}

async fn resolve_post_login_project(
    base: &BaseArgs,
    credential: &str,
    api_url: &str,
    app_url: &str,
    selected_org: Option<&LoginOrgInfo>,
) -> Result<Option<api::Project>> {
    let Some(project_name) = config::trimmed_option(base.project.as_deref()) else {
        return Ok(None);
    };

    let selected_org = selected_org.ok_or_else(|| {
        anyhow::anyhow!(
            "cannot set a default project in cross-org mode; rerun `bt auth login --org <ORG> --project <PROJECT>`"
        )
    })?;
    let ctx =
        build_login_context_for_selected_org(credential, api_url, app_url, Some(selected_org));
    let client = ApiClient::new(&ctx)?;
    ui::select_or_create_project(&client, Some(project_name), None, None)
        .await
        .map(Some)
}

async fn persist_post_login_context(
    base: &BaseArgs,
    credential: &str,
    api_url: &str,
    app_url: &str,
    selected_org: Option<&LoginOrgInfo>,
    scope: &config::ScopeArgs,
) -> Result<PostLoginContextUpdate> {
    // Scope is prompted last, after org (during login) and project.
    let project =
        resolve_post_login_project(base, credential, api_url, app_url, selected_org).await?;
    let (path, _) = scope.resolve(ui::can_prompt(), "Where to use this login")?;
    let mut cfg = config::load_file(&path);
    let org = selected_org.map_or("", |org| org.name.as_str());
    let preserve_project = project.is_none()
        && selected_org.is_some()
        && config::org_option(cfg.org.as_deref()) == Some(org);
    if !preserve_project {
        cfg.set_context(
            Some(org),
            project
                .as_ref()
                .map(|project| (project.name.as_str(), project.id.as_str())),
        );
    }
    config::save_file(&path, &cfg)
        .with_context(|| format!("Could not save config to {}", path.display()))?;

    Ok(PostLoginContextUpdate {
        display: format_post_login_context(selected_org, project.as_ref()),
        path,
    })
}

/// Emit a machine-readable JSON payload on stdout when `--json` is set,
/// otherwise run the human-readable printer. Keeps stdout pure JSON.
fn emit_result(json: bool, payload: serde_json::Value, human: impl FnOnce()) -> Result<()> {
    if json {
        println!("{}", serde_json::to_string(&payload)?);
    } else {
        human();
    }
    Ok(())
}

fn filter_auth_store(
    store: &AuthStore,
    org: Option<&str>,
    kind: Option<AuthKind>,
    api_key_hint: Option<&str>,
) -> AuthStore {
    let mut filtered = store.clone();
    filtered.profiles.retain(|_, profile| {
        org.is_none_or(|org| profile_matches_org_identifier(profile, org))
            && kind.is_none_or(|kind| profile.auth_kind == kind)
            && api_key_hint.is_none_or(|hint| {
                profile.auth_kind == AuthKind::ApiKey
                    && profile.api_key_hint.as_deref() == Some(hint.trim())
            })
    });
    filtered
}

async fn run_logins(base: &BaseArgs, _args: AuthLoginsArgs) -> Result<()> {
    let mut store = load_auth_store()?;
    let has_filter = base.org_name.is_some() || base.prefer_api_key;
    let filtered = filter_auth_store(
        &store,
        base.org_name.as_deref(),
        base.prefer_api_key.then_some(AuthKind::ApiKey),
        None,
    );
    if filtered.profiles.is_empty() {
        return emit_result(base.json, serde_json::json!([]), || {
            if store.profiles.is_empty() && !has_filter {
                println!("No saved auth logins. Run `bt auth login` to create one.");
            }
        });
    }

    let verifications = verify_all_profiles_from_store(&filtered).await;
    reconcile_verified_auth_slots(&mut store, &verifications)?;
    let all_network_errors = verifications
        .iter()
        .all(|v| v.status == "error" && !v.error.as_deref().unwrap_or("").contains("invalid"));
    if all_network_errors {
        eprintln!("Could not reach Braintrust API. Showing saved auth logins:");
        print_saved_profiles(&filtered, base.json)?;
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

fn auth_profile_json(profile: &AuthProfile, status: &str) -> serde_json::Value {
    serde_json::json!({
        "auth": auth_kind_label(profile.auth_kind),
        "org": profile.org_name,
        "org_id": profile.org_id.as_deref().filter(|org_id| !org_id.trim().is_empty()),
        "user_name": profile.user_name,
        "user_email": profile.email,
        "api_key_hint": profile.api_key_hint,
        "status": status,
    })
}

fn run_login_delete(profile_name: &str, force: bool, base_json: bool) -> Result<()> {
    let profile_name = profile_name.trim();
    if profile_name.is_empty() {
        bail!("auth login key cannot be empty");
    }

    let mut store = load_auth_store()?;
    let profile = store.profiles.get(profile_name).cloned().ok_or_else(|| {
        anyhow::anyhow!("auth login not found; run `bt auth logins` to see available logins")
    })?;
    let label = auth_slot_label(&profile);

    if !force {
        let term = ui::prompt_term().ok_or_else(|| {
            anyhow::anyhow!(
                "logout confirmation requires an interactive terminal; rerun with --force"
            )
        })?;
        let confirmed = Confirm::new()
            .with_prompt(format!("Delete {label}?"))
            .default(false)
            .interact_on(&term)?;
        if !confirmed {
            return emit_result(base_json, auth_profile_json(&profile, "cancelled"), || {
                eprintln!("Cancelled")
            });
        }
    }

    store.profiles.remove(profile_name);
    save_auth_store(&store)?;
    if let Err(err) = delete_profile_secret(profile_name) {
        eprintln!("warning: failed to delete keychain credential for '{label}': {err}");
    }
    if let Err(err) = delete_profile_oauth_refresh_token(profile_name) {
        eprintln!("warning: failed to delete oauth refresh token for '{label}': {err}");
    }
    if let Err(err) = delete_profile_oauth_access_token(profile_name) {
        eprintln!("warning: failed to delete oauth access token for '{label}': {err}");
    }
    delete_legacy_profile_secrets(&profile);

    emit_result(base_json, auth_profile_json(&profile, "deleted"), || {
        ui::print_command_status(ui::CommandStatus::Success, &format!("Deleted {label}"));
    })
}

fn run_login_logout(base: BaseArgs, args: AuthLogoutArgs) -> Result<()> {
    let store = load_auth_store()?;
    if store.profiles.is_empty() {
        return emit_result(base.json, serde_json::json!({ "status": "empty" }), || {
            println!("No saved auth logins.")
        });
    }

    let requested_org = if matches!(
        base.org_name_source,
        Some(crate::args::ArgValueSource::CommandLine)
    ) {
        config::org_option(base.org_name.as_deref())
    } else {
        None
    };
    let filtered = filter_auth_store(
        &store,
        requested_org,
        args.oauth.then_some(AuthKind::Oauth),
        args.api_key_hint.as_deref(),
    );
    let candidates = filtered
        .profiles
        .keys()
        .map(String::as_str)
        .collect::<Vec<_>>();

    let cfg_org = config_auth_context(&base);
    let current_org = effective_org_name(&base, &cfg_org);
    let profile_name = match candidates.len() {
        0 => bail!("no matching auth login found; run `bt auth logins` to see available logins"),
        1 => candidates[0].to_string(),
        _ if ui::can_prompt() => select_profile_from_store(
            "Select auth login to log out",
            &candidates,
            current_org,
            &filtered,
        )?,
        _ => {
            let labels = candidate_identities(&candidates, &filtered).join(", ");
            bail!(
                "multiple auth logins match: {labels}. Rerun interactively, or use --org <ORG> with --oauth or --api-key-hint <HINT> to disambiguate."
            );
        }
    };

    run_login_delete(&profile_name, args.force, base.json)
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
        AuthKind::ApiKey => {
            match load_profile_secret_with_legacy(name, profile.legacy_secret_key.as_deref()) {
                Ok(Some(k)) => CredentialLoad::Found(k),
                Ok(None) => CredentialLoad::Missing,
                Err(e) => CredentialLoad::Error(e.to_string()),
            }
        }
        AuthKind::Oauth => {
            if let Some(ts) = profile.oauth_access_expires_at {
                if !oauth_access_token_is_fresh(ts) {
                    return CredentialLoad::Expired;
                }
            }
            match load_profile_oauth_access_token_for_profile(name, profile) {
                Ok(Some(k)) => CredentialLoad::Found(k),
                Ok(None) => CredentialLoad::Missing,
                Err(e) => CredentialLoad::Error(e.to_string()),
            }
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ProfileVerification {
    #[serde(skip_serializing)]
    pub name: String,
    #[serde(skip_serializing)]
    slot_hash: Option<String>,
    pub auth: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org_id: Option<String>,
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
    org_id: Option<String>,
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
        slot_hash: None,
        auth: auth_kind.to_string(),
        org,
        org_id,
        user_name: jwt_id.as_ref().and_then(|j| j.name.clone()),
        user_email: jwt_id.as_ref().and_then(|j| j.email.clone()),
        api_key_hint,
        status: status_str.to_string(),
        error,
    }
}

async fn verify_profile_full(name: &str, profile: &AuthProfile) -> ProfileVerification {
    let app_url = profile.app_url.as_deref().unwrap_or(DEFAULT_APP_URL);
    let auth_kind = auth_kind_label(profile.auth_kind);
    let mk = |status, jwt_id: Option<JwtIdentity>, hint: Option<String>| {
        build_verification(
            name,
            auth_kind,
            profile.org_name.clone(),
            profile
                .org_id
                .clone()
                .filter(|org_id| !org_id.trim().is_empty()),
            jwt_id,
            hint,
            status,
        )
    };

    let credential = match load_credential_for_profile(name, profile) {
        CredentialLoad::Found(k) => k,
        CredentialLoad::Missing => {
            return mk(ProfileStatus::Missing, None, profile.api_key_hint.clone())
        }
        CredentialLoad::Expired => {
            return mk(ProfileStatus::Expired, None, profile.api_key_hint.clone())
        }
        CredentialLoad::Error(e) => {
            return mk(ProfileStatus::Error(e), None, profile.api_key_hint.clone())
        }
    };

    let (jwt_id, hint) = match profile.auth_kind {
        AuthKind::Oauth => (Some(decode_jwt_identity(&credential)), None),
        AuthKind::ApiKey => (None, profile.api_key_hint.clone()),
    };

    match fetch_login_orgs(&credential, app_url).await {
        Ok(orgs) => {
            let mut verification = mk(ProfileStatus::Ok, jwt_id, hint);
            if !is_cross_org_oauth_profile(profile) {
                if let Some(org) = profile
                    .org_id
                    .as_deref()
                    .and_then(|id| find_login_org(&orgs, id))
                    .or_else(|| {
                        profile
                            .org_name
                            .as_deref()
                            .and_then(|name| find_login_org(&orgs, name))
                    })
                {
                    verification.org = Some(org.name.clone());
                    verification.org_id = Some(org.id.clone());
                }
            }
            if profile.auth_kind == AuthKind::ApiKey {
                verification.slot_hash = Some(api_key_hash(&credential));
            }
            verification
        }
        Err(e) => {
            let status = if is_unauthorized_auth_error(&e) {
                if profile.auth_kind == AuthKind::Oauth {
                    ProfileStatus::Expired
                } else {
                    ProfileStatus::Error("invalid API key".to_string())
                }
            } else {
                ProfileStatus::Error(e.to_string())
            };
            mk(status, None, hint)
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
    sort_profile_verifications(&mut results);
    results
}

fn sort_profile_verifications(verifications: &mut [ProfileVerification]) {
    verifications.sort_by(|a, b| {
        a.org
            .as_deref()
            .unwrap_or("")
            .cmp(b.org.as_deref().unwrap_or(""))
            .then_with(|| a.name.cmp(&b.name))
    });
}

fn reconcile_verified_auth_slots(
    store: &mut AuthStore,
    verifications: &[ProfileVerification],
) -> Result<()> {
    let mut changed = false;
    for verification in verifications
        .iter()
        .filter(|verification| verification.status == "ok")
    {
        let Some(mut profile) = store.profiles.get(&verification.name).cloned() else {
            continue;
        };

        if let Some(org_id) = verification.org_id.as_deref() {
            profile.org_id = Some(org_id.to_string());
            profile.org_name = verification.org.clone();
        } else if is_cross_org_oauth_profile(&profile) {
            profile.org_id = Some(String::new());
            profile.org_name = None;
        }

        match profile.auth_kind {
            AuthKind::ApiKey => {
                let Some(hash) = verification.slot_hash.as_deref() else {
                    continue;
                };
                if profile.org_id.as_deref().is_none_or(str::is_empty) {
                    continue;
                }
                profile.api_key_hash = Some(hash.to_string());
            }
            AuthKind::Oauth => {
                if let Some(email) = verification.user_email.as_deref() {
                    profile.email = Some(email.to_string());
                }
                if let Some(user_name) = verification.user_name.as_deref() {
                    profile.user_name = Some(user_name.to_string());
                }
                if profile.email.as_deref().is_none_or(str::is_empty) {
                    continue;
                }
            }
        }

        changed |= replace_with_canonical_auth_profile(store, verification.name.as_str(), profile);
    }

    if changed {
        save_auth_store(store)?;
    }
    Ok(())
}

fn format_verification_line(v: &ProfileVerification) -> String {
    let mut parts = vec![
        config::display_org(v.org.as_deref().unwrap_or("")).to_string(),
        v.auth.clone(),
    ];
    match v.status.as_str() {
        "ok" => {
            if let Some(id) = identity_label(
                v.user_name.as_deref(),
                v.user_email.as_deref(),
                v.api_key_hint.as_deref(),
            ) {
                parts.push(id);
            }
        }
        "expired" => parts.push("token expired".into()),
        "missing" => match v.api_key_hint.as_deref() {
            Some(hint) => parts.push(format!("{hint} credential missing")),
            None => parts.push("credential missing".into()),
        },
        _ => {
            if let Some(ref e) = v.error {
                parts.push(e.clone());
            }
        }
    }
    parts.join(" — ")
}

fn profiles_grouped_by_org(store: &AuthStore) -> Vec<(&str, &AuthProfile)> {
    let mut profiles = store
        .profiles
        .iter()
        .map(|(name, profile)| (name.as_str(), profile))
        .collect::<Vec<_>>();
    profiles.sort_by(|(a_name, a), (b_name, b)| {
        profile_org(a)
            .cmp(profile_org(b))
            .then_with(|| a_name.cmp(b_name))
    });
    profiles
}

fn print_saved_profiles(store: &AuthStore, json: bool) -> Result<()> {
    let profiles = profiles_grouped_by_org(store);
    if json {
        let output: Vec<serde_json::Value> = profiles
            .into_iter()
            .map(|(_, p)| {
                serde_json::json!({
                    "auth": auth_kind_label(p.auth_kind),
                    "org": p.org_name,
                    "org_id": p.org_id,
                    "user_name": p.user_name,
                    "user_email": p.email,
                    "api_key_hint": p.api_key_hint,
                    "status": "unchecked"
                })
            })
            .collect();
        println!("{}", serde_json::to_string(&output)?);
    } else {
        for (_, profile) in profiles {
            println!("  {}", auth_slot_label(profile));
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
    default_org_name: Option<&str>,
    interactive: bool,
    verbose: bool,
    allow_cross_org: bool,
    quiet_requested: bool,
) -> Result<Option<LoginOrgInfo>> {
    if orgs.is_empty() {
        bail!("no organizations found for this credential");
    }
    sort_login_orgs(&mut orgs);

    if requested_org_name == Some("") {
        return Ok(None);
    }

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
        if allow_cross_org {
            bail!(
                "organization selection required in non-interactive mode; pass --org <ORG> or rerun interactively to choose cross-org mode"
            );
        }
        return Ok(None);
    }

    let default_org_matched = move_default_login_org_first(&mut orgs, default_org_name);
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
    let default = if default_org_matched { offset } else { 0 };
    let selection = ui::fuzzy_select("Select organization", &label_refs, default)?;
    if allow_cross_org && selection == 0 {
        return Ok(None);
    }

    Ok(Some(
        orgs.into_iter()
            .nth(selection - offset)
            .expect("selected index should be in range"),
    ))
}

fn sort_login_orgs(orgs: &mut [LoginOrgInfo]) {
    orgs.sort_by(|a, b| {
        a.name
            .to_ascii_lowercase()
            .cmp(&b.name.to_ascii_lowercase())
            .then_with(|| a.name.cmp(&b.name))
    });
}

fn move_default_login_org_first(
    orgs: &mut Vec<LoginOrgInfo>,
    default_org_name: Option<&str>,
) -> bool {
    let Some(idx) = default_org_name.and_then(|name| find_login_org_index(orgs, name)) else {
        return false;
    };
    if idx != 0 {
        let org = orgs.remove(idx);
        orgs.insert(0, org);
    }
    true
}

fn find_login_org<'a>(
    orgs: &'a [LoginOrgInfo],
    requested_org_name: &str,
) -> Option<&'a LoginOrgInfo> {
    find_login_org_index(orgs, requested_org_name).map(|idx| &orgs[idx])
}

fn find_login_org_index(orgs: &[LoginOrgInfo], requested_org_name: &str) -> Option<usize> {
    orgs.iter()
        .position(|org| org.id == requested_org_name || org.name == requested_org_name)
        .or_else(|| {
            let lowered = requested_org_name.to_ascii_lowercase();
            orgs.iter()
                .position(|org| org.name.to_ascii_lowercase() == lowered)
        })
}

fn login_org_names(orgs: &[LoginOrgInfo]) -> String {
    orgs.iter()
        .map(|org| org.name.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

fn missing_requested_org_error(orgs: &[LoginOrgInfo], requested_org_name: &str) -> anyhow::Error {
    anyhow::anyhow!(
        "org '{requested_org_name}' not found. Available: {}",
        login_org_names(orgs)
    )
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

fn generate_random_token(num_bytes: usize) -> Result<String> {
    let mut bytes = vec![0u8; num_bytes];
    getrandom::fill(&mut bytes)
        .map_err(|err| anyhow::anyhow!("failed to generate secure random bytes: {err}"))?;
    Ok(base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(bytes))
}

#[derive(Debug, Clone)]
struct OAuthCallbackParams {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OAuthCallbackQuery {
    code: Option<String>,
    state: Option<String>,
    error: Option<String>,
}

struct OAuthCallbackState {
    sender: Mutex<Option<oneshot::Sender<OAuthCallbackParams>>>,
}

struct OAuthCallbackServer {
    port: u16,
    handle: ServerHandle,
    callback_rx: oneshot::Receiver<OAuthCallbackParams>,
}

impl OAuthCallbackServer {
    fn redirect_uri(&self) -> String {
        format!("http://127.0.0.1:{}/callback", self.port)
    }

    async fn stop(self) {
        self.handle.stop(false).await;
    }
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

fn bind_oauth_callback_server() -> Result<OAuthCallbackServer> {
    let listener =
        TcpListener::bind(("127.0.0.1", 0)).context("failed to bind oauth callback listener")?;
    let port = listener
        .local_addr()
        .context("failed to read callback listener address")?
        .port();
    let (sender, callback_rx) = oneshot::channel();
    let state = web::Data::new(OAuthCallbackState {
        sender: Mutex::new(Some(sender)),
    });

    let server = HttpServer::new(move || {
        App::new()
            .app_data(state.clone())
            .route("/callback", web::get().to(oauth_callback_handler))
    })
    .workers(1)
    .listen(listener)
    .context("failed to listen on oauth callback server")?
    .run();
    let handle = server.handle();
    tokio::spawn(server);

    Ok(OAuthCallbackServer {
        port,
        handle,
        callback_rx,
    })
}

async fn oauth_callback_handler(
    state: web::Data<OAuthCallbackState>,
    query: web::Query<OAuthCallbackQuery>,
) -> HttpResponse {
    let query = query.into_inner();
    let params = OAuthCallbackParams {
        code: query.code,
        state: query.state,
        error: query.error,
    };

    if let Some(sender) = state.sender.lock().expect("callback sender lock").take() {
        let _ = sender.send(params.clone());
    }

    oauth_callback_response(&params)
}

fn oauth_callback_response(params: &OAuthCallbackParams) -> HttpResponse {
    let body = if params.error.is_some() {
        "<html><body><h1>Authorization Failed</h1><p>You can close this window.</p></body></html>"
    } else {
        "<html><body><h1>Authorization Successful</h1><p>You can close this window.</p></body></html>"
    };

    HttpResponse::Ok()
        .insert_header(("Connection", "close"))
        .content_type("text/html; charset=utf-8")
        .body(body)
}

async fn wait_for_oauth_callback_result(
    callback_rx: oneshot::Receiver<OAuthCallbackParams>,
) -> Result<OAuthCallbackParams> {
    tokio::time::timeout(OAUTH_CALLBACK_TIMEOUT, callback_rx)
        .await
        .context("timed out waiting for oauth callback")?
        .context("oauth callback server stopped before receiving callback")
}

async fn wait_for_oauth_callback(server: OAuthCallbackServer) -> Result<OAuthCallbackParams> {
    let OAuthCallbackServer {
        handle,
        callback_rx,
        ..
    } = server;
    let result = wait_for_oauth_callback_result(callback_rx).await;
    handle.stop(false).await;
    result
}

async fn collect_oauth_callback(
    callback_server: OAuthCallbackServer,
    prefer_manual: bool,
    quiet_requested: bool,
) -> Result<OAuthCallbackParams> {
    match oauth_callback_mode(prefer_manual) {
        OAuthCallbackMode::ListenerOnly => {
            eprintln!("Waiting for browser authorization...");
            wait_for_oauth_callback(callback_server).await
        }
        OAuthCallbackMode::ListenerOrStdin => {
            wait_for_oauth_callback_or_stdin(callback_server).await
        }
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
                return wait_for_oauth_callback(callback_server).await;
            }
            callback_server.stop().await;
            parse_oauth_callback_input(&pasted)
        }
    }
}

fn explicitly_quiet(base: &BaseArgs) -> bool {
    base.quiet && base.quiet_source.is_some()
}

async fn wait_for_oauth_callback_or_stdin(
    callback_server: OAuthCallbackServer,
) -> Result<OAuthCallbackParams> {
    eprintln!("Waiting for browser authorization...");
    eprintln!(
        "{}",
        dialoguer::console::style("Paste code=...&state=... if callback doesn't complete").dim()
    );

    let OAuthCallbackServer {
        handle,
        callback_rx,
        ..
    } = callback_server;
    let callback_fut = wait_for_oauth_callback_result(callback_rx);
    tokio::pin!(callback_fut);
    let mut manual_buffer = String::new();

    loop {
        tokio::select! {
            callback = &mut callback_fut => {
                handle.stop(false).await;
                return callback;
            }
            _ = tokio::time::sleep(Duration::from_millis(50)) => {
                if let Some(input) = poll_manual_oauth_input(&mut manual_buffer)? {
                    handle.stop(false).await;
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
            ("client_id", OAUTH_CLIENT_ID),
            ("code", code),
            ("redirect_uri", redirect_uri),
            ("code_verifier", code_verifier.secret()),
        ],
    )
    .await
}

fn map_refresh_oauth_error(
    api_url: &str,
    profile: &AuthProfile,
    status: reqwest::StatusCode,
    body: &str,
) -> anyhow::Error {
    if let Ok(server_err) = serde_json::from_str::<OAuthErrorResponse>(body) {
        if matches!(server_err.error.as_deref(), Some("invalid_grant")) {
            let mut message = format!(
                "oauth refresh token expired or was rejected for auth login '{}'",
                auth_slot_label(profile)
            );
            if let Some(description) = server_err.error_description.as_deref() {
                message.push_str(&format!(" ({description})"));
            }
            message.push_str(&format!("; re-run `{}`", oauth_reauth_command(profile)));
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
    profile: &AuthProfile,
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
            ("client_id", OAUTH_CLIENT_ID),
            ("refresh_token", refresh_token),
        ])
        .send()
        .await
        .with_context(|| format!("failed to call oauth token endpoint {token_url}"))?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(map_refresh_oauth_error(api_url, profile, status, &body));
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

fn build_oauth_client(api_url: &str, redirect_uri: Option<&str>) -> Result<BasicClient> {
    let api_url = api_url.trim_end_matches('/');
    let auth_url = AuthUrl::new(format!("{api_url}/oauth/authorize"))
        .context("failed to construct oauth authorize URL")?;
    let token_url = TokenUrl::new(format!("{api_url}/oauth/token"))
        .context("failed to construct oauth token URL")?;
    let client = BasicClient::new(
        ClientId::new(OAUTH_CLIENT_ID.to_string()),
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

fn load_profile_secret_with_legacy(
    primary_key: &str,
    legacy_key: Option<&str>,
) -> Result<Option<String>> {
    if let Some(secret) = load_profile_secret(primary_key)? {
        return Ok(Some(secret));
    }

    let Some(legacy_key) = legacy_key
        .map(str::trim)
        .filter(|key| !key.is_empty() && *key != primary_key)
    else {
        return Ok(None);
    };

    let Some(secret) = load_profile_secret(legacy_key)? else {
        return Ok(None);
    };
    let _ = relocate_plaintext_secret_if_present(primary_key, legacy_key, &secret);
    Ok(Some(secret))
}

fn relocate_plaintext_secret_if_present(
    primary_key: &str,
    legacy_key: &str,
    secret: &str,
) -> Result<()> {
    let path = secret_store_path()?;
    if !path.exists() {
        return Ok(());
    }
    let mut store = load_secret_store()?;
    if store.secrets.remove(legacy_key).is_some() {
        store
            .secrets
            .insert(primary_key.to_string(), secret.to_string());
        save_secret_store(&store)?;
    }
    Ok(())
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

fn load_profile_oauth_refresh_token_for_profile(
    profile_name: &str,
    profile: &AuthProfile,
) -> Result<Option<String>> {
    let primary = oauth_refresh_secret_key(profile_name);
    let legacy = profile
        .legacy_secret_key
        .as_deref()
        .map(oauth_refresh_secret_key);
    load_profile_secret_with_legacy(&primary, legacy.as_deref())
}

fn delete_profile_oauth_refresh_token(profile_name: &str) -> Result<()> {
    let key = oauth_refresh_secret_key(profile_name);
    delete_profile_secret(&key)
}

fn save_profile_oauth_access_token(profile_name: &str, access_token: &str) -> Result<()> {
    let key = oauth_access_secret_key(profile_name);
    save_profile_secret(&key, access_token)
}

fn load_profile_oauth_access_token_for_profile(
    profile_name: &str,
    profile: &AuthProfile,
) -> Result<Option<String>> {
    let primary = oauth_access_secret_key(profile_name);
    let legacy = profile
        .legacy_secret_key
        .as_deref()
        .map(oauth_access_secret_key);
    load_profile_secret_with_legacy(&primary, legacy.as_deref())
}

fn delete_profile_oauth_access_token(profile_name: &str) -> Result<()> {
    let key = oauth_access_secret_key(profile_name);
    delete_profile_secret(&key)
}

fn delete_legacy_profile_secrets(profile: &AuthProfile) {
    let Some(legacy_key) = profile.legacy_secret_key.as_deref() else {
        return;
    };
    match profile.auth_kind {
        AuthKind::ApiKey => {
            let _ = delete_profile_secret(legacy_key);
        }
        AuthKind::Oauth => {
            let _ = delete_profile_oauth_refresh_token(legacy_key);
            let _ = delete_profile_oauth_access_token(legacy_key);
        }
    }
}

fn load_valid_cached_oauth_access_token(
    profile_name: &str,
    profile: &AuthProfile,
    expires_at: Option<u64>,
) -> Result<Option<String>> {
    let Some(expires_at) = expires_at else {
        return Ok(None);
    };
    if !oauth_access_token_is_fresh(expires_at) {
        return Ok(None);
    }
    load_profile_oauth_access_token_for_profile(profile_name, profile)
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

fn sha256_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    let digest = hasher.finalize();
    let mut out = String::with_capacity(digest.len() * 2);
    for byte in digest {
        use std::fmt::Write as _;
        let _ = write!(&mut out, "{byte:02x}");
    }
    out
}

fn api_key_hash(api_key: &str) -> String {
    sha256_hex(api_key)
}

fn oauth_slot_key(org_id: &str, email: &str) -> String {
    format!("{org_id}::{email}")
}

fn api_key_slot_key(api_key_hash: &str, org_id: &str) -> String {
    format!("{api_key_hash}::{org_id}")
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
    let store: AuthStore = serde_json::from_str(&data)
        .with_context(|| format!("failed to parse auth config {}", path.display()))?;
    let migrated = migrate_auth_store(store.clone());
    if migrated != store {
        save_auth_store_to_path(path, &migrated).with_context(|| {
            format!("failed to persist auth config migration {}", path.display())
        })?;
    }
    Ok(migrated)
}

fn migrate_auth_store(store: AuthStore) -> AuthStore {
    let mut migrated = AuthStore::default();
    for (old_key, mut profile) in store.profiles {
        normalize_profile_cached_fields_from_key(&old_key, &mut profile);
        if profile.auth_kind == AuthKind::Oauth
            && profile.org_id.is_none()
            && profile
                .org_name
                .as_deref()
                .is_none_or(|org| org.trim().is_empty())
        {
            profile.org_id = Some(String::new());
            profile.org_name = None;
        }
        let new_key = canonical_profile_key(&old_key, &profile);
        if new_key != old_key && profile.legacy_secret_key.is_none() {
            profile.legacy_secret_key = Some(old_key.clone());
        }
        if migrated
            .profiles
            .get(&new_key)
            .is_some_and(|existing| !should_replace_migrated_profile(existing, &profile))
        {
            continue;
        }
        migrated.profiles.insert(new_key, profile);
    }
    migrated
}

fn should_replace_migrated_profile(existing: &AuthProfile, candidate: &AuthProfile) -> bool {
    match (existing.auth_kind, candidate.auth_kind) {
        (AuthKind::Oauth, AuthKind::Oauth) => {
            candidate.oauth_access_expires_at.unwrap_or_default()
                > existing.oauth_access_expires_at.unwrap_or_default()
        }
        _ => false,
    }
}

fn looks_like_sha256_hex(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn normalize_profile_cached_fields_from_key(current_key: &str, profile: &mut AuthProfile) {
    let Some((left, right)) = current_key.split_once("::") else {
        return;
    };

    match profile.auth_kind {
        AuthKind::Oauth => {
            let key_matches_email = profile.email.as_deref() == Some(right);
            if key_matches_email || (profile.email.is_none() && right.contains('@')) {
                profile.org_id = Some(left.to_string());
                if profile.email.is_none() {
                    profile.email = Some(right.to_string());
                }
            }
        }
        AuthKind::ApiKey => {
            if looks_like_sha256_hex(left) && !right.trim().is_empty() {
                profile.api_key_hash = Some(left.to_string());
                profile.org_id = Some(right.to_string());
            }
        }
    }
}

fn canonical_profile_key(current_key: &str, profile: &AuthProfile) -> String {
    match profile.auth_kind {
        AuthKind::Oauth => {
            let Some(email) = profile.email.as_deref().filter(|value| !value.is_empty()) else {
                return current_key.to_string();
            };
            let org_id = profile.org_id.as_deref().unwrap_or_default();
            if profile.org_id.is_some() || profile.org_name.is_none() {
                oauth_slot_key(org_id, email)
            } else {
                current_key.to_string()
            }
        }
        AuthKind::ApiKey => match (
            profile
                .api_key_hash
                .as_deref()
                .filter(|value| !value.is_empty()),
            profile.org_id.as_deref().filter(|value| !value.is_empty()),
        ) {
            (Some(hash), Some(org_id)) => api_key_slot_key(hash, org_id),
            _ => current_key.to_string(),
        },
    }
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
        BaseArgs::default()
    }

    fn auth_config(org: Option<&str>) -> crate::config::Config {
        crate::config::Config {
            org: org.map(str::to_string),
            ..Default::default()
        }
    }

    fn dt(value: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(value)
            .expect("timestamp")
            .with_timezone(&Utc)
    }

    fn ai_provider_secret(
        id: Option<&str>,
        name: &str,
        secret_type: Option<&str>,
        preview_secret: Option<&str>,
        secret_updated_at: Option<&str>,
        updated_at: Option<&str>,
        created: Option<&str>,
    ) -> AiProviderSecret {
        AiProviderSecret {
            id: id.map(str::to_string),
            name: name.to_string(),
            r#type: secret_type.map(str::to_string),
            preview_secret: preview_secret.map(str::to_string),
            secret_updated_at: secret_updated_at.map(str::to_string),
            updated_at: updated_at.map(str::to_string),
            created: created.map(str::to_string),
        }
    }

    #[test]
    fn stale_ai_provider_secrets_returns_configured_keys_older_than_six_months() {
        let secrets = vec![
            ai_provider_secret(
                Some("openai-secret"),
                "OPENAI_API_KEY",
                Some("openai"),
                Some("********"),
                Some("2025-11-12T00:00:00Z"),
                None,
                None,
            ),
            ai_provider_secret(
                Some("anthropic-secret"),
                "ANTHROPIC_API_KEY",
                Some("anthropic"),
                Some("********"),
                Some("2025-11-14T00:00:00Z"),
                None,
                None,
            ),
            ai_provider_secret(
                Some("gemini-secret"),
                "GEMINI_API_KEY",
                Some("google"),
                None,
                Some("2025-01-01T00:00:00Z"),
                None,
                None,
            ),
        ];

        let stale = stale_ai_provider_secrets("org-id", &secrets, dt("2026-05-13T00:00:00Z"));

        assert_eq!(
            stale,
            vec![StaleAiProviderSecret {
                name: "OPENAI_API_KEY".to_string(),
                warning_key: "org-id:openai-secret:2025-11-12T00:00:00Z".to_string(),
            }]
        );
    }

    #[test]
    fn stale_ai_provider_secrets_uses_timestamp_fallbacks_and_ignores_invalid_dates() {
        let secrets = vec![
            ai_provider_secret(
                None,
                "OPENAI_API_KEY",
                Some("openai"),
                Some("********"),
                None,
                Some("2025-11-12T00:00:00Z"),
                None,
            ),
            ai_provider_secret(
                None,
                "ANTHROPIC_API_KEY",
                Some("anthropic"),
                Some("********"),
                None,
                Some("not-a-date"),
                Some("2025-01-01T00:00:00Z"),
            ),
            ai_provider_secret(
                None,
                "GEMINI_API_KEY",
                Some("google"),
                Some("********"),
                None,
                None,
                None,
            ),
        ];

        let stale = stale_ai_provider_secrets("org-id", &secrets, dt("2026-05-13T00:00:00Z"));

        assert_eq!(
            stale,
            vec![StaleAiProviderSecret {
                name: "OPENAI_API_KEY".to_string(),
                warning_key: "org-id:openai:2025-11-12T00:00:00Z".to_string(),
            }]
        );
    }

    #[test]
    fn stale_ai_provider_secrets_does_not_treat_exact_six_month_cutoff_as_stale() {
        let secrets = vec![ai_provider_secret(
            Some("openai-secret"),
            "OPENAI_API_KEY",
            Some("openai"),
            Some("********"),
            Some("2025-11-13T00:00:00Z"),
            None,
            None,
        )];

        let stale = stale_ai_provider_secrets("org-id", &secrets, dt("2026-05-13T00:00:00Z"));

        assert!(stale.is_empty());
    }

    #[test]
    fn unwarned_stale_ai_provider_secrets_skips_previously_warned_key_versions() {
        let already_warned = StaleAiProviderSecret {
            name: "openai".to_string(),
            warning_key: "org-id:secret-id:2025-11-12T00:00:00Z".to_string(),
        };
        let newly_stale = StaleAiProviderSecret {
            name: "openai".to_string(),
            warning_key: "org-id:secret-id:2026-05-13T00:00:00Z".to_string(),
        };
        let state = AiProviderKeyStalenessWarningState {
            warned: BTreeSet::from([already_warned.warning_key.clone()]),
            last_checked_at: BTreeMap::new(),
        };

        let unwarned =
            unwarned_stale_ai_provider_secrets(vec![already_warned, newly_stale.clone()], &state);

        assert_eq!(unwarned, vec![newly_stale]);
    }

    #[test]
    fn ai_provider_key_staleness_warning_message_includes_key_name() {
        assert_eq!(
            ai_provider_key_staleness_warning_message("OPENAI_API_KEY"),
            "We recommend disabling and rotating AI provider secrets periodically. OPENAI_API_KEY has not been rotated in over 6 months."
        );
    }

    #[test]
    fn should_check_ai_provider_key_staleness_at_most_once_per_day_per_org() {
        let state = AiProviderKeyStalenessWarningState {
            warned: BTreeSet::new(),
            last_checked_at: BTreeMap::from([
                (
                    "org-id".to_string(),
                    "2026-05-12T12:00:00+00:00".to_string(),
                ),
                (
                    "other-org-id".to_string(),
                    "2026-05-13T11:00:00+00:00".to_string(),
                ),
            ]),
        };

        assert!(!should_check_ai_provider_key_staleness(
            &state,
            "org-id",
            dt("2026-05-13T11:59:59Z")
        ));
        assert!(should_check_ai_provider_key_staleness(
            &state,
            "org-id",
            dt("2026-05-13T12:00:00Z")
        ));
        assert!(should_check_ai_provider_key_staleness(
            &state,
            "new-org-id",
            dt("2026-05-13T11:00:00Z")
        ));
    }

    #[test]
    fn record_ai_provider_key_staleness_check_updates_org_timestamp() {
        let mut state = AiProviderKeyStalenessWarningState {
            warned: BTreeSet::new(),
            last_checked_at: BTreeMap::from([(
                "other-org-id".to_string(),
                "2026-05-13T11:00:00+00:00".to_string(),
            )]),
        };

        record_ai_provider_key_staleness_check(&mut state, "org-id", dt("2026-05-13T12:00:00Z"));

        assert_eq!(
            state.last_checked_at.get("org-id"),
            Some(&"2026-05-13T12:00:00+00:00".to_string())
        );
        assert_eq!(
            state.last_checked_at.get("other-org-id"),
            Some(&"2026-05-13T11:00:00+00:00".to_string())
        );
    }

    #[tokio::test]
    async fn ai_provider_warning_state_round_trips_through_global_config_dir() {
        let _guard = env_test_lock().lock().await;
        let previous_xdg_config_home = env::var_os("XDG_CONFIG_HOME");
        let previous_appdata = env::var_os("APPDATA");
        let config_dir = TempDir::new().expect("create temp config dir");
        env::set_var("XDG_CONFIG_HOME", config_dir.path());
        env::set_var("APPDATA", config_dir.path());

        let state = AiProviderKeyStalenessWarningState {
            warned: BTreeSet::from([
                "org-id:openai-secret:2025-11-12T00:00:00Z".to_string(),
                "org-id:anthropic-secret:2025-11-10T00:00:00Z".to_string(),
            ]),
            last_checked_at: BTreeMap::from([(
                "org-id".to_string(),
                "2026-05-13T12:00:00+00:00".to_string(),
            )]),
        };

        save_ai_provider_warning_state(&state).expect("save warning state");
        let loaded = load_ai_provider_warning_state();

        restore_env_var("XDG_CONFIG_HOME", previous_xdg_config_home);
        restore_env_var("APPDATA", previous_appdata);

        assert_eq!(loaded.warned, state.warned);
        assert_eq!(loaded.last_checked_at, state.last_checked_at);
    }

    fn env_test_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn setup_global_config(project_id: Option<&str>, org: Option<&str>) {
        let cfg = crate::config::Config {
            org: org.map(str::to_string),
            project: project_id.map(|_| "test-project".to_string()),
            project_id: project_id.map(str::to_string),
            ..crate::config::Config::default()
        };

        crate::config::save_global(&cfg).expect("save global config");
    }

    fn org_profile(kind: AuthKind, org_id: &str, org_name: &str) -> AuthProfile {
        AuthProfile {
            auth_kind: kind,
            org_id: Some(org_id.into()),
            org_name: Some(org_name.into()),
            ..Default::default()
        }
    }

    fn setup_auth_store_profiles(profiles: &[(&str, &str, &str, &str)]) {
        let mut store = AuthStore::default();
        for (profile_name, org_name, api_url, app_url) in profiles {
            store.profiles.insert(
                (*profile_name).to_string(),
                AuthProfile {
                    api_url: Some((*api_url).to_string()),
                    app_url: Some((*app_url).to_string()),
                    org_name: Some((*org_name).to_string()),
                    ..Default::default()
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
        let profile = org_profile(AuthKind::Oauth, "org_test", "BT Staging");
        let command = oauth_reauth_command(&profile);
        assert_eq!(command, "bt auth login --oauth --org 'BT Staging'");
        let err = map_refresh_oauth_error(
            "https://api.example.com",
            &profile,
            reqwest::StatusCode::BAD_REQUEST,
            r#"{"error":"invalid_grant","error_description":"refresh token expired"}"#,
        );

        assert!(is_missing_credential_error(&err));
        assert!(err.to_string().contains("refresh token expired"));
        assert!(err.to_string().contains(&format!("re-run `{command}`")));
    }

    #[test]
    fn nonrecoverable_refresh_errors_remain_nonrecoverable() {
        let err = map_refresh_oauth_error(
            "https://api.example.com",
            &org_profile(AuthKind::Oauth, "org_test", "test-org"),
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
            let mut base = base_args_for_path_probe(org_name);
            if let Some(org) = org_name.filter(|org| !org.trim().is_empty()) {
                base.app_url = Some(spawn_api_key_login_server(org));
            }
            self.login_read_only_with_base(base).await
        }
    }

    impl Drop for TestEnv {
        fn drop(&mut self) {
            env::set_current_dir(&self.previous_cwd).expect("restore current dir");
            restore_env_var("XDG_CONFIG_HOME", self.previous_xdg_config_home.clone());
            restore_env_var("APPDATA", self.previous_appdata.clone());
        }
    }

    fn spawn_login_response_server(status: &str, body: String) -> String {
        use std::io::{Read as _, Write as _};

        let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind login server");
        let address = listener.local_addr().expect("login server address");
        let status = status.to_string();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept login request");
            let mut request = [0u8; 4096];
            let _ = stream.read(&mut request);
            write!(
                stream,
                "HTTP/1.1 {status}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                body.len(),
                body
            )
            .expect("write login response");
        });
        format!("http://{address}")
    }

    fn spawn_api_key_login_server(org_name: &str) -> String {
        spawn_login_response_server(
            "200 OK",
            serde_json::json!({
                "org_info": [{
                    "id": "org_test",
                    "name": org_name,
                    "api_url": "https://api.example.test"
                }]
            })
            .to_string(),
        )
    }

    #[test]
    fn default_app_url_is_www() {
        assert_eq!(DEFAULT_APP_URL, "https://www.braintrust.dev");
    }

    #[tokio::test]
    async fn active_auth_info_without_org_uses_cross_org_oauth_only() {
        let _env = TestEnv::new(None, None).await;
        let mut store = AuthStore::default();
        store.profiles.insert(
            api_key_slot_key(&api_key_hash("test-api-key"), "org_fake"),
            AuthProfile {
                api_key_hint: Some("sk-****abcde".to_string()),
                ..org_profile(AuthKind::ApiKey, "org_fake", "test-org")
            },
        );
        store.profiles.insert(
            oauth_slot_key("", "user@example.test"),
            AuthProfile {
                auth_kind: AuthKind::Oauth,
                org_id: Some(String::new()),
                org_name: None,
                user_name: Some("Test User".to_string()),
                email: Some("user@example.test".to_string()),
                ..Default::default()
            },
        );
        save_auth_store(&store).expect("save auth store");

        let info = active_auth_info(&make_base(), None)
            .expect("resolve active auth")
            .expect("active auth info");

        assert_eq!(info.auth_method, "oauth");
        assert_eq!(info.email.as_deref(), Some("user@example.test"));
        assert_eq!(info.org_name, None);

        let mut base = make_base();
        base.prefer_api_key = true;
        assert!(resolve_auth(&base)
            .await
            .unwrap_err()
            .to_string()
            .contains("cross-org"));
        assert!(active_auth_info(&base, None)
            .unwrap_err()
            .to_string()
            .contains("cross-org"));
    }

    fn save_cached_oauth_login(store: &mut AuthStore, org_id: &str, org_name: &str) -> String {
        let slot_key = oauth_slot_key(org_id, "user@example.test");
        store.profiles.insert(
            slot_key.clone(),
            AuthProfile {
                api_url: Some("https://api.example.test".to_string()),
                app_url: Some("https://www.example.test".to_string()),
                oauth_access_expires_at: Some(current_unix_timestamp() + 3600),
                user_name: Some("Test User".to_string()),
                email: Some("user@example.test".to_string()),
                ..org_profile(AuthKind::Oauth, org_id, org_name)
            },
        );
        save_profile_secret_plaintext(
            &oauth_access_secret_key(&slot_key),
            "cached-oauth-access-token",
        )
        .expect("save cached OAuth token");
        slot_key
    }

    #[tokio::test]
    async fn auth_precedence_keeps_env_api_key_below_oauth() {
        let _env = TestEnv::new(None, None).await;
        let mut store = AuthStore::default();
        save_cached_oauth_login(&mut store, "org_fake", "test-org");
        save_auth_store(&store).expect("save auth store");
        let mut base = make_base();
        base.org_name = Some("test-org".to_string());
        base.api_key = Some("environment-api-key".to_string());
        base.api_key_source = Some(crate::args::ArgValueSource::EnvVariable);

        let resolved = resolve_auth(&base).await.expect("resolve auth");

        assert!(resolved.is_oauth);
        assert_eq!(
            resolved.api_key.as_deref(),
            Some("cached-oauth-access-token")
        );
    }

    #[tokio::test]
    async fn auth_precedence_cli_api_key_overrides_oauth() {
        let _env = TestEnv::new(None, None).await;
        let mut store = AuthStore::default();
        save_cached_oauth_login(&mut store, "org_fake", "test-org");
        save_auth_store(&store).expect("save auth store");
        let mut base = make_base();
        base.org_name = Some("test-org".to_string());
        base.api_key = Some("command-line-api-key".to_string());
        base.api_key_source = Some(crate::args::ArgValueSource::CommandLine);
        base.app_url = Some(spawn_api_key_login_server("test-org"));

        let resolved = resolve_auth(&base).await.expect("resolve auth");

        assert!(!resolved.is_oauth);
        assert_eq!(resolved.api_key.as_deref(), Some("command-line-api-key"));
    }

    #[tokio::test]
    async fn ad_hoc_api_key_validation_errors() {
        for (app_url, expected) in [
            (
                spawn_api_key_login_server("different-org"),
                "does not belong",
            ),
            (
                spawn_login_response_server("401 Unauthorized", "{}".into()),
                "not valid",
            ),
        ] {
            let mut base = make_base();
            base.org_name = Some("requested-org".into());
            base.app_url = Some(app_url);
            assert!(
                resolve_ad_hoc_api_key_auth(&base, &None, "selected-key".into())
                    .await
                    .unwrap_err()
                    .to_string()
                    .contains(expected)
            );
        }
    }

    #[test]
    fn selected_stored_api_key_wrong_org_fails_before_secret_lookup() {
        let mut store = AuthStore::default();
        store.profiles.insert(
            "stored-slot".into(),
            org_profile(AuthKind::ApiKey, "org_actual", "actual-org"),
        );
        let mut base = make_base();
        base.org_name = Some("requested-org".into());

        let err = resolve_api_key_profile_auth(&base, &mut store, &None, "stored-slot")
            .expect_err("wrong-org stored key must fail locally");
        assert!(err.to_string().contains("does not belong"));
    }

    #[tokio::test]
    async fn auth_precedence_prefer_api_key_promotes_env_api_key() {
        let _env = TestEnv::new(None, None).await;
        let mut store = AuthStore::default();
        save_cached_oauth_login(&mut store, "org_fake", "test-org");
        save_auth_store(&store).expect("save auth store");
        let mut base = make_base();
        base.org_name = Some("test-org".to_string());
        base.api_key = Some("environment-api-key".to_string());
        base.api_key_source = Some(crate::args::ArgValueSource::EnvVariable);
        base.prefer_api_key = true;
        base.app_url = Some(spawn_api_key_login_server("test-org"));

        let resolved = resolve_auth(&base).await.expect("resolve auth");

        assert!(!resolved.is_oauth);
        assert_eq!(resolved.api_key.as_deref(), Some("environment-api-key"));
    }

    #[tokio::test]
    async fn auth_precedence_prefer_api_key_falls_back_to_oauth_without_key() {
        let _env = TestEnv::new(None, None).await;
        let mut store = AuthStore::default();
        save_cached_oauth_login(&mut store, "org_fake", "test-org");
        save_auth_store(&store).expect("save auth store");
        let mut base = make_base();
        base.org_name = Some("test-org".to_string());
        base.prefer_api_key = true;

        let resolved = resolve_auth(&base).await.expect("resolve auth");

        assert!(resolved.is_oauth);
        assert_eq!(
            resolved.api_key.as_deref(),
            Some("cached-oauth-access-token")
        );
    }

    #[tokio::test]
    async fn active_auth_info_hides_ambiguous_api_keys_instead_of_failing_status() {
        let _env = TestEnv::new(None, None).await;
        let mut store = AuthStore::default();
        for (slot, hint) in [("key-a", "sk-****aaaaa"), ("key-b", "sk-****bbbbb")] {
            store.profiles.insert(
                slot.into(),
                AuthProfile {
                    api_key_hint: Some(hint.into()),
                    ..org_profile(AuthKind::ApiKey, "org_test", "test-org")
                },
            );
        }
        save_auth_store(&store).expect("save auth store");
        let mut base = make_base();
        base.org_name = Some("test-org".into());

        assert!(active_auth_info(&base, Some("test-org"))
            .expect("status auth lookup")
            .is_none());
    }

    #[tokio::test]
    async fn active_auth_info_prefer_api_key_selects_stored_key_for_org() {
        let _env = TestEnv::new(None, None).await;
        let mut store = AuthStore::default();
        store.profiles.insert(
            oauth_slot_key("org_fake", "user@example.test"),
            AuthProfile {
                user_name: Some("Test User".to_string()),
                email: Some("user@example.test".to_string()),
                ..org_profile(AuthKind::Oauth, "org_fake", "test-org")
            },
        );
        store.profiles.insert(
            api_key_slot_key(&api_key_hash("test-api-key"), "org_fake"),
            AuthProfile {
                api_key_hint: Some("sk-****abcde".to_string()),
                ..org_profile(AuthKind::ApiKey, "org_fake", "test-org")
            },
        );
        save_auth_store(&store).expect("save auth store");
        let mut base = make_base();
        base.prefer_api_key = true;

        let info = active_auth_info(&base, Some("test-org"))
            .expect("resolve active auth")
            .expect("active auth info");

        assert_eq!(info.auth_method, "api_key");
        assert_eq!(info.api_key_hint.as_deref(), Some("sk-****abcde"));
    }

    #[tokio::test]
    async fn api_key_profile_rekeys_after_legacy_secret_load() {
        let _env = TestEnv::new(None, None).await;
        let mut store = AuthStore::default();
        store.profiles.insert(
            "work".to_string(),
            org_profile(AuthKind::ApiKey, "org_fake", "test-org"),
        );

        maybe_rekey_api_key_profile_after_secret_load(&mut store, "work", "test-api-key")
            .expect("rekey api key profile");

        let key = api_key_slot_key(&api_key_hash("test-api-key"), "org_fake");
        let profile = store.profiles.get(&key).expect("rekeyed profile");
        assert!(!store.profiles.contains_key("work"));
        assert_eq!(
            profile.api_key_hash.as_deref(),
            Some(api_key_hash("test-api-key").as_str())
        );
        assert_eq!(profile.legacy_secret_key.as_deref(), Some("work"));
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
    fn migrate_auth_store_rekeys_oauth_slots_and_preserves_legacy_secret_key() {
        let mut store = AuthStore::default();
        store.profiles.insert(
            "work".to_string(),
            AuthProfile {
                email: Some("user@example.test".to_string()),
                ..org_profile(AuthKind::Oauth, "org_fake", "test-org")
            },
        );

        let migrated = migrate_auth_store(store);
        let key = oauth_slot_key("org_fake", "user@example.test");
        let profile = migrated.profiles.get(&key).expect("migrated profile");

        assert_eq!(profile.legacy_secret_key.as_deref(), Some("work"));
    }

    #[test]
    fn migrate_auth_store_rekeys_cross_org_oauth_with_empty_org_id() {
        let mut store = AuthStore::default();
        store.profiles.insert(
            "legacy-cross-org".to_string(),
            AuthProfile {
                auth_kind: AuthKind::Oauth,
                org_name: None,
                email: Some("user@example.test".to_string()),
                ..Default::default()
            },
        );

        let migrated = migrate_auth_store(store);
        let profile = migrated
            .profiles
            .get(&oauth_slot_key("", "user@example.test"))
            .expect("cross-org OAuth slot");

        assert_eq!(profile.org_id.as_deref(), Some(""));
        assert_eq!(
            profile.legacy_secret_key.as_deref(),
            Some("legacy-cross-org")
        );
    }

    #[test]
    fn migrate_auth_store_rekeys_api_key_slots_by_hash_and_org() {
        let hash = api_key_hash("test-api-key");
        let mut store = AuthStore::default();
        store.profiles.insert(
            "work".to_string(),
            AuthProfile {
                auth_kind: AuthKind::ApiKey,
                org_id: Some("org_fake".to_string()),
                org_name: Some("test-org".to_string()),
                api_key_hash: Some(hash.clone()),
                api_key_hint: Some("test-****i-key".to_string()),
                ..Default::default()
            },
        );

        let migrated = migrate_auth_store(store);
        let key = api_key_slot_key(&hash, "org_fake");
        let profile = migrated.profiles.get(&key).expect("migrated profile");

        assert_eq!(profile.legacy_secret_key.as_deref(), Some("work"));
        assert_eq!(profile.api_key_hint.as_deref(), Some("test-****i-key"));
    }

    #[test]
    fn load_auth_store_persists_canonical_slot_migration() {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("bt-auth-migration-test-{unique}"));
        fs::create_dir_all(&dir).expect("create dir");
        let path = dir.join("auth.json");

        let mut store = AuthStore::default();
        store.profiles.insert(
            "legacy-login".to_string(),
            AuthProfile {
                auth_kind: AuthKind::Oauth,
                org_id: Some("org_fake".to_string()),
                org_name: Some("test-org".to_string()),
                email: Some("user@example.test".to_string()),
                ..Default::default()
            },
        );
        save_auth_store_to_path(&path, &store).expect("save legacy store");

        let loaded = load_auth_store_from_path(&path).expect("load and migrate");
        let persisted: AuthStore =
            serde_json::from_str(&fs::read_to_string(&path).expect("read migrated store"))
                .expect("parse migrated store");
        let slot_key = oauth_slot_key("org_fake", "user@example.test");

        for migrated in [&loaded, &persisted] {
            let profile = migrated
                .profiles
                .get(&slot_key)
                .expect("canonical OAuth slot");
            assert_eq!(profile.legacy_secret_key.as_deref(), Some("legacy-login"));
        }

        let _ = fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn verified_legacy_slots_gain_stable_keys() {
        let _env = TestEnv::new(None, None).await;
        let mut store = AuthStore::default();
        store.profiles.insert(
            "legacy-oauth".to_string(),
            AuthProfile {
                auth_kind: AuthKind::Oauth,
                org_name: Some("test-org".to_string()),
                ..Default::default()
            },
        );
        store.profiles.insert(
            "legacy-api-key".to_string(),
            AuthProfile {
                auth_kind: AuthKind::ApiKey,
                org_name: Some("test-org".to_string()),
                api_key_hint: Some("sk-****abcde".to_string()),
                ..Default::default()
            },
        );
        let hash = api_key_hash("test-api-key");
        let verifications = vec![
            ProfileVerification {
                name: "legacy-oauth".to_string(),
                slot_hash: None,
                auth: "oauth".to_string(),
                org: Some("test-org".to_string()),
                org_id: Some("org_fake".to_string()),
                user_name: Some("Test User".to_string()),
                user_email: Some("user@example.test".to_string()),
                api_key_hint: None,
                status: "ok".to_string(),
                error: None,
            },
            ProfileVerification {
                name: "legacy-api-key".to_string(),
                slot_hash: Some(hash.clone()),
                auth: "api_key".to_string(),
                org: Some("test-org".to_string()),
                org_id: Some("org_fake".to_string()),
                user_name: None,
                user_email: None,
                api_key_hint: Some("sk-****abcde".to_string()),
                status: "ok".to_string(),
                error: None,
            },
        ];

        reconcile_verified_auth_slots(&mut store, &verifications)
            .expect("reconcile verified slots");

        let oauth = store
            .profiles
            .get(&oauth_slot_key("org_fake", "user@example.test"))
            .expect("canonical OAuth slot");
        assert_eq!(oauth.legacy_secret_key.as_deref(), Some("legacy-oauth"));
        let api_key = store
            .profiles
            .get(&api_key_slot_key(&hash, "org_fake"))
            .expect("canonical API-key slot");
        assert_eq!(api_key.legacy_secret_key.as_deref(), Some("legacy-api-key"));
    }

    #[test]
    fn migrate_auth_store_dedupes_oauth_slots_by_latest_expiry() {
        let mut store = AuthStore::default();
        for (name, expires_at) in [("old", 10), ("new", 20)] {
            store.profiles.insert(
                name.to_string(),
                AuthProfile {
                    auth_kind: AuthKind::Oauth,
                    org_id: Some("org_fake".to_string()),
                    org_name: Some("test-org".to_string()),
                    email: Some("user@example.test".to_string()),
                    oauth_access_expires_at: Some(expires_at),
                    ..Default::default()
                },
            );
        }

        let migrated = migrate_auth_store(store);
        let key = oauth_slot_key("org_fake", "user@example.test");
        assert_eq!(migrated.profiles.len(), 1);
        let profile = migrated.profiles.get(&key).expect("migrated profile");
        assert_eq!(profile.legacy_secret_key.as_deref(), Some("new"));
    }

    #[test]
    fn config_auth_context_returns_config_org() {
        let base = make_base();
        let cfg = auth_config(Some("local-org"));

        let org = config_auth_context_from_config(&base, &cfg);

        assert_eq!(org.as_deref(), Some("local-org"));
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
    fn move_default_login_org_first_moves_matching_org() {
        let mut orgs = vec![
            login_org("org_1", "acme"),
            login_org("org_2", "beta"),
            login_org("org_3", "gamma"),
        ];

        assert!(move_default_login_org_first(&mut orgs, Some("beta")));
        assert_eq!(orgs[0].name, "beta");
        assert_eq!(orgs[1].name, "acme");
    }

    #[test]
    fn move_default_login_org_first_keeps_order_without_match() {
        let mut orgs = vec![login_org("org_1", "acme"), login_org("org_2", "beta")];

        assert!(!move_default_login_org_first(&mut orgs, Some("missing")));
        assert_eq!(orgs[0].name, "acme");
        assert_eq!(orgs[1].name, "beta");
    }

    fn login_org(id: &str, name: &str) -> LoginOrgInfo {
        LoginOrgInfo {
            id: id.to_string(),
            name: name.to_string(),
            api_url: None,
        }
    }

    fn auth_source(
        prefer_api_key: bool,
        cli: Option<&str>,
        env: Option<&str>,
        oauth: Option<&str>,
        api_key: Option<&str>,
    ) -> AuthSource {
        resolve_auth_source(
            prefer_api_key,
            cli.map(str::to_string),
            || env.map(str::to_string),
            || Ok(oauth.map(str::to_string)),
            || Ok(api_key.map(str::to_string)),
        )
        .expect("resolve auth source")
    }

    #[test]
    fn auth_source_cli_api_key_wins_over_everything() {
        assert_eq!(
            auth_source(false, Some("cli"), Some("env"), Some("oauth"), Some("ak")),
            AuthSource::CliApiKey("cli".into())
        );
        assert_eq!(
            auth_source(true, Some("cli"), Some("env"), Some("oauth"), Some("ak")),
            AuthSource::CliApiKey("cli".into())
        );
    }

    #[test]
    fn auth_source_default_order_is_oauth_then_env_then_stored_api_key() {
        assert_eq!(
            auth_source(false, None, Some("env"), Some("oauth"), Some("ak")),
            AuthSource::Oauth("oauth".into())
        );
        assert_eq!(
            auth_source(false, None, Some("env"), None, Some("ak")),
            AuthSource::EnvApiKey("env".into())
        );
        assert_eq!(
            auth_source(false, None, None, None, Some("ak")),
            AuthSource::ApiKey("ak".into())
        );
        assert_eq!(auth_source(false, None, None, None, None), AuthSource::None);
    }

    #[test]
    fn auth_source_prefer_api_key_order_is_env_then_api_key_then_oauth() {
        assert_eq!(
            auth_source(true, None, Some("env"), Some("oauth"), Some("ak")),
            AuthSource::EnvApiKey("env".into())
        );
        assert_eq!(
            auth_source(true, None, None, Some("oauth"), Some("ak")),
            AuthSource::ApiKey("ak".into())
        );
        // No env/stored API key, but OAuth for the org is available: fall back to it.
        assert_eq!(
            auth_source(true, None, None, Some("oauth"), None),
            AuthSource::Oauth("oauth".into())
        );
        assert_eq!(auth_source(true, None, None, None, None), AuthSource::None);
    }

    #[test]
    fn auth_source_ambiguous_selection_stops_the_ladder() {
        let err = resolve_auth_source(
            false,
            None,
            || None,
            || bail!("multiple oauth logins"),
            || Ok(Some("ak".to_string())),
        )
        .expect_err("ambiguous oauth should stop the ladder");
        assert!(err.to_string().contains("multiple oauth logins"));
    }

    fn login_filter_store() -> AuthStore {
        let mut store = AuthStore::default();
        for (slot, kind, suffix, hint) in [
            ("oauth-a", AuthKind::Oauth, "a", None),
            ("key-a", AuthKind::ApiKey, "a", Some("sk-****aaaaa")),
            ("key-b", AuthKind::ApiKey, "b", Some("sk-****bbbbb")),
        ] {
            store.profiles.insert(
                slot.into(),
                AuthProfile {
                    api_key_hint: hint.map(str::to_string),
                    ..org_profile(
                        kind,
                        &format!("org_test_{suffix}"),
                        &format!("test-org-{suffix}"),
                    )
                },
            );
        }
        store.profiles.insert(
            "cross".into(),
            AuthProfile {
                auth_kind: AuthKind::Oauth,
                org_id: Some(String::new()),
                ..Default::default()
            },
        );
        store
    }

    #[test]
    fn login_and_logout_filters_compose() {
        let store = login_filter_store();
        let matches = |org, kind, hint| {
            filter_auth_store(&store, org, kind, hint)
                .profiles
                .into_keys()
                .collect::<Vec<_>>()
        };
        assert_eq!(
            matches(Some("test-org-a"), None, None),
            ["key-a", "oauth-a"]
        );
        assert_eq!(matches(Some("org_test_b"), None, None), ["key-b"]);
        assert_eq!(
            matches(Some("test-org-a"), Some(AuthKind::ApiKey), None),
            ["key-a"]
        );
        assert_eq!(matches(Some(""), None, None), ["cross"]);
        assert!(matches(Some(""), Some(AuthKind::ApiKey), None).is_empty());
        assert_eq!(matches(None, None, None).len(), 4);
        assert_eq!(
            matches(Some("test-org-a"), Some(AuthKind::Oauth), None),
            ["oauth-a"]
        );
        assert_eq!(matches(None, None, Some("sk-****bbbbb")), ["key-b"]);

        let mut picker_store = store.clone();
        picker_store.profiles.insert(
            "oauth-a-duplicate".into(),
            picker_store.profiles["oauth-a"].clone(),
        );
        assert_eq!(saved_login_names(&picker_store, true).len(), 4);
        assert_eq!(saved_login_names(&picker_store, false).len(), 3);
    }

    #[tokio::test]
    async fn post_login_context_preserves_only_same_org_projects() {
        let _env = TestEnv::new(None, None).await;
        let save = |org: &str| {
            crate::config::save_global(&crate::config::Config {
                org: Some(org.into()),
                project: Some("test-project".into()),
                project_id: Some("proj_test".into()),
                ..Default::default()
            })
            .unwrap();
        };
        let persist = |org: Option<LoginOrgInfo>| async move {
            persist_post_login_context(
                &make_base(),
                "test-credential",
                "https://api.example.test",
                "https://www.example.test",
                org.as_ref(),
                &config::ScopeArgs {
                    global: true,
                    local: false,
                },
            )
            .await
            .unwrap();
            crate::config::load_global().unwrap()
        };

        save("old-org");
        let cfg = persist(Some(login_org("org_test", "test-org"))).await;
        assert_eq!((cfg.org.as_deref(), cfg.project), (Some("test-org"), None));

        save("test-org");
        let cfg = persist(Some(login_org("org_test", "test-org"))).await;
        assert_eq!(
            (cfg.project.as_deref(), cfg.project_id.as_deref()),
            (Some("test-project"), Some("proj_test"))
        );

        save("");
        let cfg = persist(None).await;
        assert_eq!(
            (cfg.org.as_deref(), cfg.project, cfg.project_id),
            (Some(""), None, None)
        );
    }

    #[tokio::test]
    async fn resolve_post_login_project_rejects_cross_org_default_project() {
        let mut base = make_base();
        base.project = Some("demo-project".to_string());

        let err = resolve_post_login_project(
            &base,
            "test-api-key",
            "https://api.example.test",
            "https://www.example.test",
            None,
        )
        .await
        .expect_err("cross-org project selection should fail");

        assert!(err
            .to_string()
            .contains("cannot set a default project in cross-org mode"));
    }

    #[test]
    fn requested_api_key_org_resolution() {
        fn no_prompt(_: &str, _: &[LoginOrgInfo]) -> Result<ApiKeyOrgMismatchAction> {
            panic!("prompt should not be called")
        }
        let orgs = vec![login_org("org_test", "test-org")];
        assert_eq!(
            resolve_requested_org_for_api_key_login(&orgs, Some("test-org"), false, no_prompt)
                .unwrap(),
            RequestedOrgResolution::UseRequestedOrg
        );
        assert!(resolve_requested_org_for_api_key_login(
            &orgs,
            Some("other-org"),
            false,
            no_prompt
        )
        .unwrap_err()
        .to_string()
        .contains("org 'other-org' not found. Available: test-org"));

        for (action, expected) in [
            (
                ApiKeyOrgMismatchAction::UseOauth,
                RequestedOrgResolution::SwitchToOauth,
            ),
            (
                ApiKeyOrgMismatchAction::UseApiKey,
                RequestedOrgResolution::IgnoreRequestedOrg,
            ),
        ] {
            let actual = resolve_requested_org_for_api_key_login(
                &orgs,
                Some("other-org"),
                true,
                |requested, available| {
                    assert_eq!(requested, "other-org");
                    assert_eq!(available.len(), 1);
                    Ok(action)
                },
            )
            .unwrap();
            assert_eq!(actual, expected);
        }
    }

    #[test]
    fn obscure_api_keys() {
        for (key, expected) in [
            ("sk-LumEdp0BbLRzhJwO", "sk-****zhJwO"),
            ("abc", "****"),
            ("abcdefghijklm", "****ijklm"),
            ("sk-café-résumé-key", "****"),
        ] {
            assert_eq!(obscure_api_key(key), expected);
        }
    }

    #[test]
    fn decode_jwt_identity_handles_claims_and_invalid_tokens() {
        let encode = |payload| base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(payload);
        let header = encode(r#"{"alg":"RS256"}"#);
        for (payload, expected) in [
            (
                r#"{"name":"Test User","email":"user@example.test"}"#,
                (Some("Test User"), Some("user@example.test")),
            ),
            (r#"{"sub":"123"}"#, (None, None)),
        ] {
            let id = decode_jwt_identity(&format!("{header}.{}.sig", encode(payload)));
            assert_eq!((id.name.as_deref(), id.email.as_deref()), expected);
        }
        let id = decode_jwt_identity("not-a-jwt");
        assert_eq!((id.name, id.email), (None, None));
    }

    #[test]
    fn auth_logins_are_grouped_by_org() {
        let verification = |name: &str, org: Option<&str>| ProfileVerification {
            name: name.into(),
            slot_hash: None,
            auth: "oauth".into(),
            org: org.map(str::to_string),
            org_id: None,
            user_name: None,
            user_email: None,
            api_key_hint: None,
            status: "ok".into(),
            error: None,
        };
        let mut verifications = vec![
            verification("profile-z", Some("test-org-a")),
            verification("profile-a", Some("test-org-b")),
            verification("profile-m", Some("test-org-a")),
            verification("profile-x", None),
        ];

        sort_profile_verifications(&mut verifications);

        let order = verifications
            .iter()
            .map(|v| (v.org.as_deref(), v.name.as_str()))
            .collect::<Vec<_>>();
        assert_eq!(
            order,
            vec![
                (None, "profile-x"),
                (Some("test-org-a"), "profile-m"),
                (Some("test-org-a"), "profile-z"),
                (Some("test-org-b"), "profile-a"),
            ]
        );
    }

    #[test]
    fn saved_auth_logins_are_grouped_by_org() {
        let mut store = AuthStore::default();
        for (name, org) in [
            ("profile-z", "test-org-a"),
            ("profile-a", "test-org-b"),
            ("profile-m", "test-org-a"),
        ] {
            store.profiles.insert(
                name.into(),
                AuthProfile {
                    org_name: Some(org.into()),
                    ..Default::default()
                },
            );
        }

        let order = profiles_grouped_by_org(&store)
            .into_iter()
            .map(|(name, profile)| (profile_org(profile), name))
            .collect::<Vec<_>>();
        assert_eq!(
            order,
            vec![
                ("test-org-a", "profile-m"),
                ("test-org-a", "profile-z"),
                ("test-org-b", "profile-a"),
            ]
        );
    }

    #[test]
    fn verification_line_formatting() {
        for (name, email, hint, expected) in [
            (
                Some("Test User"),
                Some("user@example.test"),
                None,
                Some("Test User (user@example.test)"),
            ),
            (
                None,
                Some("user@example.test"),
                None,
                Some("user@example.test"),
            ),
            (None, None, Some("sk-****abcde"), Some("sk-****abcde")),
            (None, None, None, None),
        ] {
            assert_eq!(identity_label(name, email, hint).as_deref(), expected);
        }

        let verification = |auth: &str,
                            org: Option<&str>,
                            identity: Option<&str>,
                            hint: Option<&str>,
                            status: &str,
                            error: Option<&str>| ProfileVerification {
            name: "test-profile".into(),
            slot_hash: None,
            auth: auth.into(),
            org: org.map(str::to_string),
            org_id: None,
            user_name: identity.map(str::to_string),
            user_email: identity.map(|_| "user@example.test".into()),
            api_key_hint: hint.map(str::to_string),
            status: status.into(),
            error: error.map(str::to_string),
        };
        let cases = [
            (
                verification(
                    "oauth",
                    Some("test-org"),
                    Some("Test User"),
                    None,
                    "ok",
                    None,
                ),
                "test-org — oauth — Test User (user@example.test)",
            ),
            (
                verification(
                    "api_key",
                    Some("test-org"),
                    None,
                    Some("sk-****abcde"),
                    "ok",
                    None,
                ),
                "test-org — api_key — sk-****abcde",
            ),
            (
                verification("oauth", None, None, None, "expired", None),
                "cross-org — oauth — token expired",
            ),
            (
                verification(
                    "api_key",
                    Some("test-org"),
                    None,
                    None,
                    "error",
                    Some("invalid API key"),
                ),
                "test-org — api_key — invalid API key",
            ),
        ];
        for (verification, expected) in cases {
            assert_eq!(format_verification_line(&verification), expected);
        }
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

    async fn assert_oauth_callback(stale_connection: bool, code: &str, state: &str) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        use tokio::net::TcpStream;

        let server = bind_oauth_callback_server().unwrap();
        let addr = format!("127.0.0.1:{}", server.port);
        let callback = tokio::spawn(wait_for_oauth_callback(server));
        if stale_connection {
            drop(TcpStream::connect(&addr).await.unwrap());
        }
        let mut stream = TcpStream::connect(addr).await.unwrap();
        let request =
            format!("GET /callback?code={code}&state={state} HTTP/1.1\r\nHost: 127.0.0.1\r\n\r\n");
        stream.write_all(request.as_bytes()).await.unwrap();

        let mut response = vec![0u8; 4096];
        let read = tokio::time::timeout(Duration::from_secs(1), stream.read(&mut response))
            .await
            .unwrap()
            .unwrap();
        let params = callback.await.unwrap().unwrap();
        assert_eq!(
            (params.code.as_deref(), params.state.as_deref()),
            (Some(code), Some(state))
        );
        let response = String::from_utf8_lossy(&response[..read]);
        assert!(response.starts_with("HTTP/1.1 200 OK"));
        assert!(response.contains("Authorization Successful"));
    }

    #[tokio::test]
    async fn oauth_callback_listener_responds_to_http_request() {
        assert_oauth_callback(false, "test-code", "test-state").await;
    }

    #[tokio::test]
    async fn oauth_callback_listener_ignores_empty_connection() {
        assert_oauth_callback(true, "next-code", "next-state").await;
    }

    #[tokio::test]
    async fn login_read_only_no_cached_project_id_uses_validated_login_path() {
        let env = TestEnv::new(None, None).await;
        assert_invalid_api_url(env.login_read_only_probe(Some("acme")).await);
    }

    #[tokio::test]
    async fn login_read_only_cached_project_id_and_org_uses_fast_path() {
        let env = TestEnv::new(Some("proj_123"), Some("test-org")).await;
        let ctx = env
            .login_read_only_probe(Some("test-org"))
            .await
            .expect("fast path should succeed");

        assert_eq!(ctx.login.org_name().as_deref(), Some("test-org"));
        assert_eq!(ctx.login.org_id().as_deref(), Some(""));
        assert_eq!(ctx.api_url, "not-a-valid-url");
    }

    #[tokio::test]
    async fn login_read_only_cached_project_id_but_missing_org_falls_back_to_login() {
        let env = TestEnv::new(Some("proj_123"), None).await;
        assert_invalid_api_url(env.login_read_only_probe(None).await);
    }

    #[tokio::test]
    async fn login_read_only_cached_project_id_but_whitespace_org_is_cross_org() {
        let env = TestEnv::new(Some("proj_123"), None).await;
        let err = match env.login_read_only_probe(Some("     ")).await {
            Ok(_) => panic!("whitespace org should be canonical cross-org"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("concrete org"));
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
        let env = TestEnv::new(Some("proj_123"), Some("test-org")).await;
        let mut base = make_base();
        base.api_key = Some("test-api-key".into());
        base.org_name = Some("test-org".into());
        let app_url = spawn_api_key_login_server("test-org");
        base.app_url = Some(app_url.clone());

        let ctx = env
            .login_read_only_with_base(base)
            .await
            .expect("fast path should succeed");

        assert_eq!(ctx.login.org_name().as_deref(), Some("test-org"));
        assert_eq!(ctx.api_url, "https://api.example.test");
        assert_eq!(ctx.app_url, app_url);
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
