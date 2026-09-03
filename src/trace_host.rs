//! Braintrust host services for the mounted coding-agent trace runtime.
//!
//! The plugin runtime owns all trace commands and agent behavior. This module
//! only adapts `bt`'s profile store and project picker to its host-service
//! interface.

use std::ffi::OsString;
use std::sync::Arc;

use async_trait::async_trait;
use bt_daemon::wire::{
    AuthSelection, AuthSource, BackendAuth, FlushMode, SessionRoute, TraceDestination,
};
use bt_daemon::{
    AuthDiagnostic, AuthLease, AuthResolveReason, OutputFormat, RouteRequirements, RunHookCommand,
    TraceHostContext, TraceHostServices,
};

use braintrust_sdk_rust::DEFAULT_APP_URL;
use std::collections::HashMap;
use tokio::sync::Mutex;

use crate::args::{ArgValueSource, BaseArgs};
use crate::auth::OrgDataPlane;

#[derive(Clone)]
struct BtTraceHost {
    base: BaseArgs,
    /// An organization's data plane is stable per (app URL, org name); cache
    /// lookups so lease renewals don't re-query the app URL every few minutes.
    org_data_plane_cache: Arc<Mutex<HashMap<(String, String), OrgDataPlane>>>,
}

impl BtTraceHost {
    fn new(base: BaseArgs) -> Self {
        Self {
            base,
            org_data_plane_cache: Arc::default(),
        }
    }

    async fn org_data_plane(
        &self,
        credential: &str,
        app_url: &str,
        org_name: Option<&str>,
    ) -> anyhow::Result<OrgDataPlane> {
        let key = (
            app_url.to_string(),
            org_name.unwrap_or_default().to_string(),
        );
        // Held across the fetch so concurrent leases on a new key resolve once.
        let mut cache = self.org_data_plane_cache.lock().await;
        if let Some(hit) = cache.get(&key) {
            return Ok(hit.clone());
        }
        let data_plane = crate::auth::resolve_org_data_plane(credential, app_url, org_name)
            .await
            .map_err(|error| anyhow::anyhow!("resolve organization data plane: {error}"))?;
        cache.insert(key, data_plane.clone());
        Ok(data_plane)
    }
}

fn has_usable_api_key(base: &BaseArgs) -> bool {
    base.api_key
        .as_deref()
        .is_some_and(|key| !key.trim().is_empty())
}

fn session_route(base: &BaseArgs) -> SessionRoute {
    let source = if base.profile.is_some() {
        AuthSource::SavedProfile
    } else if matches!(base.api_key_source, Some(ArgValueSource::EnvVariable))
        && has_usable_api_key(base)
    {
        AuthSource::Environment
    } else {
        AuthSource::Auto
    };
    SessionRoute {
        auth: AuthSelection {
            source,
            profile_id: None,
            profile: base.profile.clone(),
            org_name: base.org_name.clone(),
        },
        destination: base
            .project
            .clone()
            .map(|project_name| TraceDestination::ProjectLogs {
                project_id: None,
                project_name: Some(project_name),
            }),
        flush_mode: FlushMode::FireAndForget,
        additional_metadata: None,
    }
}

async fn resolve_persistent_trace_auth(mut base: BaseArgs) -> anyhow::Result<BaseArgs> {
    base.prefer_profile = true;
    let resolved = match crate::auth::resolve_auth(&base).await {
        Ok(resolved) if resolved.profile.is_some() => resolved,
        Ok(_) => crate::auth::ensure_saved_trace_profile(&base)
            .await
            .map_err(|error| anyhow::anyhow!("save tracing login: {error}"))?,
        Err(_)
            if crate::ui::can_prompt()
                || base
                    .api_key
                    .as_deref()
                    .is_some_and(|key| !key.trim().is_empty()) =>
        {
            crate::auth::ensure_saved_trace_profile(&base)
                .await
                .map_err(|error| anyhow::anyhow!("save tracing login: {error}"))?
        }
        Err(error) => return Err(anyhow::anyhow!("resolve saved auth: {error}")),
    };
    let profile = resolved
        .profile
        .expect("saved trace profile resolver always returns a profile");
    base.profile = Some(profile);
    base.profile_explicit = true;
    base.prefer_profile = true;
    if base.org_name.is_none() {
        base.org_name = resolved.org_name;
    }
    Ok(base)
}

async fn resolve_invocation_trace_auth(mut base: BaseArgs) -> anyhow::Result<BaseArgs> {
    match crate::auth::resolve_auth(&base).await {
        Ok(resolved) if resolved.api_key.is_some() => {
            if let Some(profile) = resolved.profile {
                base.profile = Some(profile);
                base.profile_explicit = true;
                base.prefer_profile = true;
            }
            if base.org_name.is_none() {
                base.org_name = resolved.org_name;
            }
            Ok(base)
        }
        Ok(_) | Err(_) if crate::ui::can_prompt() => {
            let resolved = crate::auth::ensure_saved_trace_profile(&base)
                .await
                .map_err(|error| anyhow::anyhow!("create tracing login: {error}"))?;
            base.profile = resolved.profile;
            base.profile_explicit = true;
            base.prefer_profile = true;
            if base.org_name.is_none() {
                base.org_name = resolved.org_name;
            }
            Ok(base)
        }
        Ok(_) => Ok(base),
        Err(error) => Err(anyhow::anyhow!("resolve auth: {error}")),
    }
}

fn profile_auth_diagnostic(
    verification: crate::auth::ProfileVerification,
    source: &str,
    selected_org: Option<String>,
) -> AuthDiagnostic {
    let expires_at_ms = verification
        .expires_at
        .and_then(|seconds| i64::try_from(seconds).ok())
        .and_then(|seconds| seconds.checked_mul(1000));
    let (status, error) = match verification.status.as_str() {
        "ok" => ("ready", None),
        "expired" => (
            "expired",
            Some(format!(
                "OAuth access token is expired; run `bt login --refresh --profile {}`",
                verification.name
            )),
        ),
        "missing" => (
            "error",
            Some(format!(
                "saved profile credential is missing; rerun `bt login --profile {}`",
                verification.name
            )),
        ),
        _ => (
            "error",
            Some(
                verification
                    .error
                    .unwrap_or_else(|| "saved profile is unusable".into()),
            ),
        ),
    };
    AuthDiagnostic {
        status: status.into(),
        source: source.into(),
        kind: Some(verification.auth),
        profile: Some(verification.name),
        org_name: selected_org.or(verification.org),
        expires_at_ms,
        error,
    }
}

fn unresolved_auth_diagnostic(
    source: &str,
    profile: Option<String>,
    org_name: Option<String>,
    error: impl Into<String>,
) -> AuthDiagnostic {
    AuthDiagnostic {
        status: "error".into(),
        source: source.into(),
        kind: None,
        profile,
        org_name,
        expires_at_ms: None,
        error: Some(error.into()),
    }
}

/// Ensure the route carries an organization, the way `resolve_trace_project`
/// ensures it carries a project. Tracing has no later opportunity to ask: the
/// org is baked into the route before the daemon sees a single event, so a
/// credential that resolves no default org has to be settled here or not at
/// all. That is a real gap rather than a corner case — a default org comes
/// from `--org`, the config file `bt init` and `bt switch` write, or an
/// org-bound API key profile, and an OAuth profile carries none of them.
///
/// Idempotent, so the project flow can call it before listing projects and
/// `resolve_route` can call it again for the paths that skip project selection.
async fn resolve_trace_org(mut base: BaseArgs) -> anyhow::Result<BaseArgs> {
    if base
        .org_name
        .as_deref()
        .is_some_and(|org| !org.trim().is_empty())
    {
        return Ok(base);
    }

    let auth = crate::auth::resolve_auth(&base)
        .await
        .map_err(|error| anyhow::anyhow!("resolve auth: {error}"))?;
    if let Some(profile) = auth.profile.clone() {
        base.profile = Some(profile);
        base.profile_explicit = true;
        base.prefer_profile = true;
    }
    if let Some(org_name) = auth.org_name.clone().filter(|org| !org.trim().is_empty()) {
        base.org_name = Some(org_name);
        return Ok(base);
    }

    if !crate::ui::is_interactive() {
        anyhow::bail!(
            "organization choice required in non-interactive mode; pass --org <NAME> or set BRAINTRUST_ORG_NAME"
        );
    }

    let (_, org) = crate::switch::select_org_for_switch(&base, None).await?;
    base.org_name = Some(org.name);
    Ok(base)
}

async fn resolve_trace_project(mut base: BaseArgs) -> anyhow::Result<BaseArgs> {
    if base
        .project
        .as_deref()
        .is_some_and(|project| !project.trim().is_empty())
    {
        return Ok(base);
    }

    if let Some(project) = crate::config::configured_project_for_context(base.org_name.as_deref()) {
        base.project = Some(project);
        return Ok(base);
    }

    if !crate::ui::is_interactive() {
        anyhow::bail!(
            "project choice required in non-interactive mode; pass --project <NAME> or set BRAINTRUST_DEFAULT_PROJECT"
        );
    }

    let auth = crate::auth::resolve_auth(&base)
        .await
        .map_err(|error| anyhow::anyhow!("resolve auth: {error}"))?;
    if let Some(profile) = auth.profile.clone() {
        base.profile = Some(profile);
        base.profile_explicit = true;
        base.prefer_profile = true;
    }
    if base.org_name.is_none() {
        base.org_name = auth.org_name.clone();
    }
    if let Some(project) = crate::config::configured_project_for_context(auth.org_name.as_deref()) {
        base.project = Some(project);
        return Ok(base);
    }

    // Settle the org before listing projects so the picker offers the projects
    // of the org the traces will actually land in.
    base = resolve_trace_org(base).await?;

    let login = crate::auth::login_read_only(&base).await?;
    let client = crate::http::ApiClient::new(&login)?;
    let project = crate::ui::select_project(
        &client,
        None,
        Some("Select a project for coding-agent traces"),
        crate::ui::ProjectSelectMode::ExistingOnly,
    )
    .await?;
    base.project = Some(project.name);
    Ok(base)
}

#[async_trait]
impl TraceHostServices for BtTraceHost {
    async fn resolve_route(&self, requirements: RouteRequirements) -> anyhow::Result<SessionRoute> {
        // Commands that run inside an agent's turn (hooks) leave this false so
        // no missing profile or org can block the turn on a prompt. bt gates
        // every prompt on this global, including the ones `resolve_auth`
        // reaches later in the same process.
        if !requirements.interactive_auth {
            crate::ui::set_no_input(true);
        }
        let base = if requirements.persistent_auth {
            resolve_persistent_trace_auth(self.base.clone()).await?
        } else if requirements.interactive_auth {
            resolve_invocation_trace_auth(self.base.clone()).await?
        } else {
            self.base.clone()
        };
        let mut base = if requirements.destination_required {
            resolve_trace_project(base).await?
        } else {
            base
        };
        // Hooks tolerate an unresolved org — the daemon accepts their events
        // without one — but setup, managed run, and import bake the org into a
        // stored route, so they have to settle it now rather than fail later.
        if requirements.interactive_auth {
            base = resolve_trace_org(base).await?;
        }
        Ok(session_route(&base))
    }

    async fn resolve_auth(
        &self,
        selection: &AuthSelection,
        _reason: AuthResolveReason,
    ) -> anyhow::Result<AuthLease> {
        let mut base = self.base.clone();
        base.no_input = true;
        let selection = selection.clone().canonicalized()?;
        match selection.source {
            AuthSource::SavedProfile => {
                let profile = if let Some(profile_id) = &selection.profile_id {
                    crate::auth::profile_name_for_id(profile_id)?.ok_or_else(|| {
                        anyhow::anyhow!(
                            "saved profile ID '{profile_id}' no longer exists; run `bt trace enable` to select a profile"
                        )
                    })?
                } else {
                    selection.profile.clone().ok_or_else(|| {
                        anyhow::anyhow!("saved-profile auth requires a profile ID or name")
                    })?
                };
                base.profile = Some(profile);
                base.profile_explicit = true;
                base.prefer_profile = true;
                // The route explicitly selects this durable credential. Do
                // not let any transient override displace it on lease renewal.
                base.api_key = None;
                base.api_key_source = None;
            }
            AuthSource::Environment => {
                if !matches!(base.api_key_source, Some(ArgValueSource::EnvVariable))
                    || base
                        .api_key
                        .as_deref()
                        .is_none_or(|key| key.trim().is_empty())
                {
                    anyhow::bail!(
                        "this trace route uses environment auth, but BRAINTRUST_API_KEY is not set"
                    );
                }
                base.profile = None;
                base.profile_explicit = false;
                base.prefer_profile = false;
            }
            AuthSource::Auto => {}
        }
        if let Some(org_name) = &selection.org_name {
            base.org_name = Some(org_name.clone());
        }
        let resolved = crate::auth::resolve_auth(&base)
            .await
            .map_err(|error| anyhow::anyhow!("resolve auth: {error}"))?;
        let token = resolved
            .api_key
            .ok_or_else(|| anyhow::anyhow!("selected Braintrust profile has no credential"))?;
        let canonical_selection = if let Some(profile) = resolved.profile {
            AuthSelection {
                source: AuthSource::SavedProfile,
                profile_id: resolved.profile_id.or_else(|| selection.profile_id.clone()),
                profile: Some(profile),
                org_name: resolved.org_name.clone(),
            }
        } else if matches!(base.api_key_source, Some(ArgValueSource::EnvVariable)) {
            AuthSelection {
                source: AuthSource::Environment,
                profile_id: None,
                profile: None,
                org_name: resolved.org_name.clone(),
            }
        } else {
            anyhow::bail!(
                "trace route resolved an unsupported transient credential; use BRAINTRUST_API_KEY or a saved profile"
            );
        };
        let expires_at_ms = resolved.is_oauth.then(|| {
            chrono::Utc::now()
                .timestamp_millis()
                .saturating_add(5 * 60 * 1000)
        });
        // An invocation-level --api-url / BRAINTRUST_API_URL override wins.
        // Otherwise resolve the selected organization's data-plane URL the way
        // login flows do — on hybrid deployments only the organization knows
        // it. Never guess: a failed lookup fails the lease and the daemon
        // retries, rather than silently routing traces to the wrong instance.
        let (api_url, org_id) = match resolved.api_url {
            Some(explicit) => (Some(explicit), None),
            None => {
                let app_url = resolved
                    .app_url
                    .clone()
                    .unwrap_or_else(|| DEFAULT_APP_URL.to_string());
                let data_plane = self
                    .org_data_plane(&token, &app_url, resolved.org_name.as_deref())
                    .await?;
                (Some(data_plane.api_url), data_plane.org_id)
            }
        };
        Ok(AuthLease {
            selection: canonical_selection,
            auth: BackendAuth {
                api_url,
                token,
                app_url: resolved.app_url,
                org_name: resolved.org_name,
                org_id,
            },
            expires_at_ms,
        })
    }

    async fn diagnose_auth(&self, selection: &AuthSelection) -> AuthDiagnostic {
        if selection.effective_source() == AuthSource::Environment {
            return if matches!(self.base.api_key_source, Some(ArgValueSource::EnvVariable))
                && self
                    .base
                    .api_key
                    .as_deref()
                    .is_some_and(|key| !key.trim().is_empty())
            {
                AuthDiagnostic {
                    status: "ready".into(),
                    source: "environment".into(),
                    kind: Some("api_key".into()),
                    profile: None,
                    org_name: selection.org_name.clone(),
                    expires_at_ms: None,
                    error: None,
                }
            } else {
                unresolved_auth_diagnostic(
                    "environment",
                    None,
                    selection.org_name.clone(),
                    "BRAINTRUST_API_KEY is not set in the current process",
                )
            };
        }

        let selected_profile = selection.profile.clone().or_else(|| {
            (selection.effective_source() == AuthSource::Auto)
                .then(|| self.base.profile.clone())
                .flatten()
        });
        if let Some(profile) = selected_profile {
            return match crate::auth::diagnose_stored_profile(&profile) {
                Ok(verification) => profile_auth_diagnostic(
                    verification,
                    "saved_profile",
                    selection.org_name.clone(),
                ),
                Err(error) => unresolved_auth_diagnostic(
                    "saved_profile",
                    Some(profile),
                    selection.org_name.clone(),
                    error.to_string(),
                ),
            };
        }

        if has_usable_api_key(&self.base) {
            let source = match self.base.api_key_source {
                Some(ArgValueSource::EnvVariable) => "environment_api_key",
                _ => "api_key_override",
            };
            return AuthDiagnostic {
                status: "ready".into(),
                source: source.into(),
                kind: Some("api_key".into()),
                profile: None,
                org_name: selection.org_name.clone(),
                expires_at_ms: None,
                error: None,
            };
        }

        match crate::auth::list_profiles() {
            Ok(profiles) if profiles.len() == 1 => {
                let profile = &profiles[0].name;
                match crate::auth::diagnose_stored_profile(profile) {
                    Ok(verification) => profile_auth_diagnostic(
                        verification,
                        "automatic_saved_profile",
                        selection.org_name.clone(),
                    ),
                    Err(error) => unresolved_auth_diagnostic(
                        "automatic_saved_profile",
                        Some(profile.clone()),
                        selection.org_name.clone(),
                        error.to_string(),
                    ),
                }
            }
            Ok(profiles) if profiles.is_empty() => unresolved_auth_diagnostic(
                "unresolved",
                None,
                selection.org_name.clone(),
                "no saved Braintrust profile; run `bt login --profile <NAME>`",
            ),
            Ok(_) => unresolved_auth_diagnostic(
                "unresolved",
                None,
                selection.org_name.clone(),
                "multiple saved profiles exist; pass --profile <NAME>",
            ),
            Err(error) => unresolved_auth_diagnostic(
                "saved_profile_store",
                None,
                selection.org_name.clone(),
                error.to_string(),
            ),
        }
    }
}

pub fn context(base: BaseArgs) -> TraceHostContext {
    let output_format = OutputFormat::from(base.login.json);
    let verbose = base.verbose;
    let executable = std::env::current_exe()
        .map(OsString::from)
        .unwrap_or_else(|_| OsString::from("bt"));
    TraceHostContext {
        version: crate::CLI_VERSION.to_string(),
        output_format,
        verbose,
        command: RunHookCommand {
            program: executable,
            args: vec![OsString::from("trace")],
        },
        services: Arc::new(BtTraceHost::new(base)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::args::LoginBaseArgs;
    use std::{env, ffi::OsString, fs};
    use tempfile::TempDir;

    struct TestOAuthProfile {
        name: &'static str,
        id: &'static str,
        org_name: &'static str,
        api_url: &'static str,
        app_url: String,
        access_token: &'static str,
    }

    impl TestOAuthProfile {
        // Represents the profile created by `bt login --oauth` for a
        // self-hosted Braintrust deployment whose control plane lives at
        // `app_url`.
        fn self_hosted(app_url: String) -> Self {
            Self {
                name: "bt-test-self-hosted-00000000-0000-4000-8000-000000000001",
                id: "00000000-0000-4000-8000-000000000002",
                org_name: "test-org",
                api_url: "https://api.self-hosted.example",
                app_url,
                access_token: "synthetic-access-token",
            }
        }
    }

    /// Serves the app URL's `/api/apikey/login` shape from a local socket so
    /// organization lookups stay off the network. Returns the app URL to use.
    async fn spawn_login_orgs_server(org_info: serde_json::Value) -> String {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind login mock");
        let app_url = format!(
            "http://127.0.0.1:{}",
            listener.local_addr().expect("mock addr").port()
        );
        let body = serde_json::json!({ "org_info": org_info }).to_string();
        let response = format!(
            "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
            body.len()
        );
        tokio::spawn(async move {
            while let Ok((mut socket, _)) = listener.accept().await {
                let mut request = [0u8; 4096];
                let _ = socket.read(&mut request).await;
                let _ = socket.write_all(response.as_bytes()).await;
            }
        });
        app_url
    }

    // Installs a fake profile in an isolated bt config directory. The test
    // never reads or changes the developer's real bt configuration.
    struct InstalledAuthProfile {
        _guard: futures_util::lock::MutexGuard<'static, ()>,
        _config_dir: TempDir,
        previous_xdg_config_home: Option<OsString>,
        previous_appdata: Option<OsString>,
    }

    impl InstalledAuthProfile {
        async fn install(profile: &TestOAuthProfile) -> Self {
            let guard = crate::auth::env_test_lock().lock().await;
            let previous_xdg_config_home = env::var_os("XDG_CONFIG_HOME");
            let previous_appdata = env::var_os("APPDATA");
            let config_dir = TempDir::new().expect("create temp config dir");
            env::set_var("XDG_CONFIG_HOME", config_dir.path());
            env::set_var("APPDATA", config_dir.path());

            let auth_dir = config_dir.path().join("bt");
            fs::create_dir_all(&auth_dir).expect("create auth dir");

            // `bt login` saves profile metadata, including the deployment URLs,
            // in auth.json. The coding agent stores only a reference to it.
            fs::write(
                auth_dir.join("auth.json"),
                serde_json::to_vec(&serde_json::json!({
                    "profiles": {
                        (profile.name): {
                            "auth_kind": "oauth",
                            "api_url": profile.api_url,
                            "app_url": profile.app_url,
                            "org_name": profile.org_name,
                            "oauth_client_id": "synthetic-client-id",
                            "oauth_access_expires_at": 4_102_444_800_u64
                        }
                    },
                    "profile_ids": {
                        (profile.name): profile.id
                    }
                }))
                .expect("serialize auth store"),
            )
            .expect("write auth store");

            // Use a valid cached token so the test never contacts an OAuth
            // endpoint. secrets.json is bt's fallback for the OS keychain.
            let access_secret_key = format!("oauth_access::{}", profile.name);
            fs::write(
                auth_dir.join("secrets.json"),
                serde_json::to_vec(&serde_json::json!({
                    "secrets": {
                        (access_secret_key): profile.access_token
                    }
                }))
                .expect("serialize secret store"),
            )
            .expect("write secret store");

            Self {
                _guard: guard,
                _config_dir: config_dir,
                previous_xdg_config_home,
                previous_appdata,
            }
        }
    }

    impl Drop for InstalledAuthProfile {
        fn drop(&mut self) {
            match &self.previous_xdg_config_home {
                Some(value) => env::set_var("XDG_CONFIG_HOME", value),
                None => env::remove_var("XDG_CONFIG_HOME"),
            }
            match &self.previous_appdata {
                Some(value) => env::set_var("APPDATA", value),
                None => env::remove_var("APPDATA"),
            }
        }
    }

    async fn resolve_trace_auth(
        profile: &TestOAuthProfile,
        api_url_override: Option<&str>,
    ) -> AuthLease {
        // This models the later `bt trace hook` process. Clap puts either an
        // explicit `--api-url` or BRAINTRUST_API_URL into this same field.
        let host = BtTraceHost::new(BaseArgs {
            login: LoginBaseArgs {
                api_url: api_url_override.map(str::to_string),
                ..LoginBaseArgs::default()
            },
            ..BaseArgs::default()
        });

        // This models the route that `bt trace setup` writes to the coding
        // agent's braintrust.json. It references the bt profile by name and ID.
        let saved_route = AuthSelection {
            source: AuthSource::SavedProfile,
            profile_id: Some(profile.id.into()),
            profile: Some(profile.name.into()),
            org_name: Some(profile.org_name.into()),
        };

        // This is the production handoff from bt's profile store to the daemon.
        host.resolve_auth(&saved_route, AuthResolveReason::Initial)
            .await
            .expect("resolve saved-profile auth")
    }

    #[test]
    fn profile_diagnostic_reports_expiry_without_credentials() {
        let diagnostic = profile_auth_diagnostic(
            crate::auth::ProfileVerification {
                name: "work".into(),
                auth: "oauth".into(),
                app_url: "https://www.braintrust.dev".into(),
                api_url: None,
                org: Some("acme".into()),
                user_name: None,
                user_email: None,
                api_key_hint: None,
                expires_at: Some(1_700_000_000),
                status: "expired".into(),
                error: None,
            },
            "saved_profile",
            None,
        );
        assert_eq!(diagnostic.status, "expired");
        assert_eq!(diagnostic.kind.as_deref(), Some("oauth"));
        assert_eq!(diagnostic.expires_at_ms, Some(1_700_000_000_000));
        assert!(diagnostic
            .error
            .as_deref()
            .unwrap()
            .contains("bt login --refresh --profile work"));
    }

    #[test]
    fn automatic_route_ignores_an_empty_environment_api_key() {
        let base = BaseArgs {
            login: LoginBaseArgs {
                api_key: Some("  ".into()),
                api_key_source: Some(ArgValueSource::EnvVariable),
                ..LoginBaseArgs::default()
            },
            ..BaseArgs::default()
        };

        assert!(!has_usable_api_key(&base));
        assert_eq!(session_route(&base).auth.source, AuthSource::Auto);
    }

    #[tokio::test]
    async fn invocation_environment_auth_returns_an_environment_lease() {
        // A SaaS-style organization that advertises no data-plane URL of its own.
        let app_url = spawn_login_orgs_server(serde_json::json!([
            { "id": "org_123", "name": "test-org" },
        ]))
        .await;
        let base = BaseArgs {
            login: LoginBaseArgs {
                api_key: Some("synthetic-api-key".into()),
                api_key_source: Some(ArgValueSource::EnvVariable),
                app_url: Some(app_url),
                ..LoginBaseArgs::default()
            },
            org_name: Some("test-org".into()),
            ..BaseArgs::default()
        };
        let host = BtTraceHost::new(base);
        let lease = host
            .resolve_auth(
                &AuthSelection {
                    source: AuthSource::Environment,
                    profile_id: None,
                    profile: None,
                    org_name: Some("test-org".into()),
                },
                AuthResolveReason::Initial,
            )
            .await
            .unwrap();
        assert_eq!(lease.selection.source, AuthSource::Environment);
        assert_eq!(lease.selection.profile, None);
        assert_eq!(lease.auth.token, "synthetic-api-key");
        assert_eq!(lease.auth.org_name.as_deref(), Some("test-org"));
    }

    #[tokio::test]
    async fn saved_profile_auth_resolves_the_orgs_api_url() {
        // Given: `bt login --oauth` saved a self-hosted profile, and the
        // organization reports the deployment's API URL, as every command
        // resolving a data plane relies on.
        let app_url = spawn_login_orgs_server(serde_json::json!([
            {
                "id": "org_123",
                "name": "test-org",
                "api_url": "https://api.self-hosted.example",
            },
        ]))
        .await;
        let profile = TestOAuthProfile::self_hosted(app_url.clone());
        let _installed_profile = InstalledAuthProfile::install(&profile).await;

        // When: a coding-agent trace resolves that profile without repeating
        // `--api-url` on the trace-hook invocation.
        let lease = resolve_trace_auth(&profile, None).await;

        // Then: the daemon receives the profile's credential and the
        // organization's deployment URLs.
        assert_eq!(lease.auth.token, profile.access_token);
        assert_eq!(
            lease.auth.api_url.as_deref(),
            Some("https://api.self-hosted.example")
        );
        assert_eq!(lease.auth.app_url.as_deref(), Some(app_url.as_str()));
        assert_eq!(lease.auth.org_id.as_deref(), Some("org_123"));
    }

    #[tokio::test]
    async fn saved_profile_auth_without_an_org_api_url_uses_the_default() {
        // Given: a hosted-style organization that reports no API URL of its own.
        let app_url = spawn_login_orgs_server(serde_json::json!([
            { "id": "org_123", "name": "test-org" },
        ]))
        .await;
        let profile = TestOAuthProfile::self_hosted(app_url);
        let _installed_profile = InstalledAuthProfile::install(&profile).await;

        // When: a coding-agent trace resolves that profile.
        let lease = resolve_trace_auth(&profile, None).await;

        // Then: resolution lands on the hosted default, exactly as
        // resolve_profile_api_url settles it for login, and the profile's
        // OAuth-refresh URL is not treated as a data plane.
        assert_eq!(lease.auth.token, profile.access_token);
        assert_eq!(
            lease.auth.api_url.as_deref(),
            Some(braintrust_sdk_rust::DEFAULT_API_URL)
        );
        assert_eq!(lease.auth.org_id.as_deref(), Some("org_123"));
    }

    #[tokio::test]
    async fn saved_profile_auth_prefers_the_selected_orgs_api_url() {
        // Given: a hybrid deployment — the control plane hosts several
        // organizations and the selected one runs its own data plane.
        let app_url = spawn_login_orgs_server(serde_json::json!([
            {
                "id": "org_123",
                "name": "test-org",
                "api_url": "https://api.hybrid-data-plane.example",
            },
            { "id": "org_456", "name": "other-org" },
        ]))
        .await;
        let profile = TestOAuthProfile::self_hosted(app_url);
        let _installed_profile = InstalledAuthProfile::install(&profile).await;

        // When: a coding-agent trace resolves that profile.
        let lease = resolve_trace_auth(&profile, None).await;

        // Then: the organization's data-plane URL wins over the profile's
        // control-plane URL, exactly as login-based commands resolve it.
        assert_eq!(lease.auth.token, profile.access_token);
        assert_eq!(
            lease.auth.api_url.as_deref(),
            Some("https://api.hybrid-data-plane.example")
        );
        assert_eq!(lease.auth.org_id.as_deref(), Some("org_123"));
    }

    #[tokio::test]
    async fn saved_profile_auth_uses_an_api_url_override() {
        // Given: `bt login --oauth` saved the same self-hosted profile. The
        // unreachable app URL proves an override skips the organization lookup.
        let profile = TestOAuthProfile::self_hosted("https://app.self-hosted.example".into());
        let _installed_profile = InstalledAuthProfile::install(&profile).await;

        // When: BRAINTRUST_API_URL or `--api-url` supplies an API URL to the
        // trace-hook invocation. Clap represents both inputs with the same value.
        let lease = resolve_trace_auth(&profile, Some("https://api.override.example")).await;

        // Then: the invocation-level override takes precedence over the profile URL.
        assert_eq!(lease.auth.token, profile.access_token);
        assert_eq!(
            lease.auth.api_url.as_deref(),
            Some("https://api.override.example")
        );
        assert_eq!(
            lease.auth.app_url.as_deref(),
            Some(profile.app_url.as_str())
        );
    }
}
