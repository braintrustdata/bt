//! Braintrust host services for the mounted coding-agent trace runtime.
//!
//! The plugin runtime owns all trace commands and agent behavior. This module
//! only adapts `bt`'s profile store and project picker to its host-service
//! interface.

use std::ffi::OsString;
use std::sync::Arc;

use async_trait::async_trait;
use bt_daemon::wire::{AuthSelection, BackendAuth, FlushMode, SessionRoute, TraceDestination};
use bt_daemon::{
    AuthLease, AuthResolveReason, OutputFormat, RouteRequirements, RunHookCommand,
    TraceHostContext, TraceHostServices,
};

use crate::args::BaseArgs;

#[derive(Clone)]
struct BtTraceHost {
    base: BaseArgs,
}

// AuthLease currently requires a canonical string even when the credential is
// supplied directly by BRAINTRUST_API_KEY rather than a saved profile. Keep a
// distinct sentinel for that auth source, and accept the old value so routes
// written by earlier bt versions continue to work.
const ENVIRONMENT_API_KEY_AUTH: &str = "environment:BRAINTRUST_API_KEY";
const LEGACY_ENVIRONMENT_API_KEY_AUTH: &str = "environment";

fn is_environment_api_key_auth(profile: &str) -> bool {
    matches!(
        profile,
        ENVIRONMENT_API_KEY_AUTH | LEGACY_ENVIRONMENT_API_KEY_AUTH
    )
}

fn session_route(base: &BaseArgs) -> SessionRoute {
    SessionRoute {
        auth: AuthSelection {
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
        let mut base = if requirements.destination_required {
            resolve_trace_project(self.base.clone()).await?
        } else {
            self.base.clone()
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
        let uses_environment_api_key = selection
            .profile
            .as_deref()
            .is_some_and(is_environment_api_key_auth);
        if uses_environment_api_key {
            if base
                .api_key
                .as_deref()
                .is_none_or(|api_key| api_key.trim().is_empty())
            {
                anyhow::bail!(
                    "route uses BRAINTRUST_API_KEY authentication, but BRAINTRUST_API_KEY is not available"
                );
            }
            // The sentinel identifies an auth source, not a saved profile.
            // Leaving profile selection unset allows resolve_auth to use the
            // API key already parsed by clap into BaseArgs.
            base.profile = None;
            base.profile_explicit = false;
            base.prefer_profile = false;
        } else if let Some(profile) = &selection.profile {
            base.profile = Some(profile.clone());
            base.profile_explicit = true;
            base.prefer_profile = true;
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
        let profile = resolved
            .profile
            .or_else(|| {
                selection
                    .profile
                    .clone()
                    .filter(|_| !uses_environment_api_key)
            })
            .unwrap_or_else(|| ENVIRONMENT_API_KEY_AUTH.into());
        let expires_at_ms = resolved.is_oauth.then(|| {
            chrono::Utc::now()
                .timestamp_millis()
                .saturating_add(5 * 60 * 1000)
        });
        Ok(AuthLease {
            profile,
            auth: BackendAuth {
                token,
                api_url: resolved.api_url,
                app_url: resolved.app_url,
                org_name: resolved.org_name,
                org_id: None,
            },
            expires_at_ms,
        })
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
        services: Arc::new(BtTraceHost { base }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn environment_auth_route_reuses_the_cli_api_key() {
        for sentinel in [ENVIRONMENT_API_KEY_AUTH, LEGACY_ENVIRONMENT_API_KEY_AUTH] {
            // A configured profile must not override the route's explicit
            // environment-key auth source.
            let base = BaseArgs {
                login: crate::args::LoginBaseArgs {
                    api_key: Some("sk-test-fake".into()),
                    profile: Some("test-profile".into()),
                    profile_explicit: true,
                    prefer_profile: true,
                    ..crate::args::LoginBaseArgs::default()
                },
                ..BaseArgs::default()
            };
            let host = BtTraceHost { base };

            let lease = host
                .resolve_auth(
                    &AuthSelection {
                        profile: Some(sentinel.into()),
                        org_name: Some("test-org".into()),
                    },
                    AuthResolveReason::Initial,
                )
                .await
                .expect("resolve environment auth");

            assert_eq!(lease.profile, ENVIRONMENT_API_KEY_AUTH);
            assert_eq!(lease.auth.token, "sk-test-fake");
            assert_eq!(lease.auth.org_name.as_deref(), Some("test-org"));
        }
    }

    #[tokio::test]
    async fn environment_auth_route_requires_the_cli_api_key() {
        let host = BtTraceHost {
            base: BaseArgs::default(),
        };
        let error = host
            .resolve_auth(
                &AuthSelection {
                    profile: Some(ENVIRONMENT_API_KEY_AUTH.into()),
                    org_name: None,
                },
                AuthResolveReason::Initial,
            )
            .await
            .expect_err("missing environment key should fail");

        assert!(error
            .to_string()
            .contains("BRAINTRUST_API_KEY is not available"));
    }
}
