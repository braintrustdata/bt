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

use crate::args::{ArgValueSource, BaseArgs};

#[derive(Clone)]
struct BtTraceHost {
    base: BaseArgs,
}

fn session_route(base: &BaseArgs) -> SessionRoute {
    SessionRoute {
        auth: AuthSelection {
            source: if base.profile.is_some() {
                AuthSource::SavedProfile
            } else {
                AuthSource::Environment
            },
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

fn require_saved_trace_profile(profile: Option<String>) -> anyhow::Result<String> {
    profile.ok_or_else(|| {
        anyhow::anyhow!(
            "coding-agent tracing requires a saved Braintrust profile; run `bt login --profile <NAME>`, then rerun this command with `--profile <NAME>`"
        )
    })
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
        if let Some(profile_id) = &selection.profile_id {
            let profile = crate::auth::profile_name_for_id(profile_id)?
                .ok_or_else(|| anyhow::anyhow!(
                    "saved profile ID '{profile_id}' no longer exists; run `bt trace enable` to select a profile"
                ))?;
            base.profile = Some(profile);
            base.profile_explicit = true;
            base.prefer_profile = true;
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
        let profile = require_saved_trace_profile(resolved.profile)?;
        let expires_at_ms = resolved.is_oauth.then(|| {
            chrono::Utc::now()
                .timestamp_millis()
                .saturating_add(5 * 60 * 1000)
        });
        Ok(AuthLease {
            selection: AuthSelection {
                source: if profile == "environment" {
                    AuthSource::Environment
                } else {
                    AuthSource::SavedProfile
                },
                profile_id: resolved.profile_id.or_else(|| selection.profile_id.clone()),
                profile: (profile != "environment").then_some(profile),
                org_name: resolved.org_name.clone(),
            },
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

    async fn diagnose_auth(&self, selection: &AuthSelection) -> AuthDiagnostic {
        let selected_profile = selection
            .profile
            .clone()
            .or_else(|| self.base.profile.clone());
        if let Some(profile) = selected_profile {
            if profile == "environment" {
                return unresolved_auth_diagnostic(
                    "legacy_environment_route",
                    Some(profile),
                    selection.org_name.clone(),
                    "profile `environment` is not a saved login; rerun setup with --profile <NAME>",
                );
            }
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

        if self.base.api_key.is_some() {
            let source = match self.base.api_key_source {
                Some(ArgValueSource::CommandLine) => "command_line_api_key",
                Some(ArgValueSource::EnvVariable) => "environment_api_key",
                None => "api_key_override",
            };
            return unresolved_auth_diagnostic(
                source,
                None,
                selection.org_name.clone(),
                "coding-agent tracing requires a saved Braintrust profile; run `bt login --profile <NAME>`",
            );
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
        services: Arc::new(BtTraceHost { base }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tracing_rejects_environment_only_auth() {
        let error = require_saved_trace_profile(None).unwrap_err();
        assert!(error
            .to_string()
            .contains("requires a saved Braintrust profile"));
        assert!(error.to_string().contains("bt login --profile <NAME>"));
    }

    #[test]
    fn tracing_keeps_the_resolved_saved_profile() {
        assert_eq!(
            require_saved_trace_profile(Some("test-profile".into())).unwrap(),
            "test-profile"
        );
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
}
