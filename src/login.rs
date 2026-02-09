use anyhow::{anyhow, Result};
use braintrust_sdk_rust::{BraintrustClient, LoginState};

use crate::args::BaseArgs;
use crate::auth;
use crate::config;

/// Resolved credentials and context for API calls
pub struct LoginContext {
    /// Either from SDK LoginState or from profile
    pub source: LoginSource,
    pub api_url: String,
    pub app_url: String,
    /// Resolved project (from --project, env, or profile)
    pub project: Option<String>,
}

pub enum LoginSource {
    /// SDK-based login (legacy flow)
    Sdk(LoginState),
    /// Profile-based login (new OAuth2/API key flow)
    Profile {
        api_key: String,
        org_name: Option<String>,
    },
}

impl LoginContext {
    /// Get the API key for authentication
    pub fn api_key(&self) -> &str {
        match &self.source {
            LoginSource::Sdk(login) => &login.api_key,
            LoginSource::Profile { api_key, .. } => api_key,
        }
    }

    /// Get the organization name if available
    pub fn org_name(&self) -> Option<&str> {
        match &self.source {
            LoginSource::Sdk(login) => Some(login.org_name.as_str()),
            LoginSource::Profile { org_name, .. } => org_name.as_deref(),
        }
    }
}

pub async fn login(base: &BaseArgs) -> Result<LoginContext> {
    // Priority 1: Explicit API key (--api-key or BRAINTRUST_API_KEY)
    if let Some(api_key) = &base.api_key {
        return login_with_explicit_key(base, api_key).await;
    }

    // Priority 2: Profile-based authentication
    if let Ok(Some(profile)) = config::get_profile(&base.profile) {
        // Refresh token if needed
        let profile = auth::refresh_token_if_needed(&base.profile, profile).await?;

        let api_url = profile.api_url.clone();
        let app_url = base.app_url.clone().unwrap_or_else(|| {
            api_url
                .replace("api.braintrust", "www.braintrust")
                .replace("api.braintrustdata", "www.braintrustdata")
        });

        // Resolve effective project: --project > BRAINTRUST_DEFAULT_PROJECT (in base.project) > profile.project
        let project = base.project.clone().or_else(|| profile.project.clone());

        return Ok(LoginContext {
            source: LoginSource::Profile {
                api_key: profile.access_token,
                org_name: profile.org_name,
            },
            api_url,
            app_url,
            project,
        });
    }

    // Priority 3: Fall back to SDK login (legacy behavior)
    // This will likely fail if no credentials are available
    login_with_sdk(base).await
}

async fn login_with_explicit_key(base: &BaseArgs, api_key: &str) -> Result<LoginContext> {
    let mut builder = BraintrustClient::builder()
        .blocking_login(true)
        .api_key(api_key);

    if let Some(api_url) = &base.api_url {
        builder = builder.api_url(api_url);
    }
    if let Some(project) = &base.project {
        builder = builder.default_project(project);
    }

    let client = builder.build().await?;
    let login = client.wait_for_login().await?;

    let api_url = login
        .api_url
        .clone()
        .or_else(|| base.api_url.clone())
        .unwrap_or_else(|| "https://api.braintrust.dev".to_string());

    let app_url = base.app_url.clone().unwrap_or_else(|| {
        api_url
            .replace("api.braintrust", "www.braintrust")
            .replace("api.braintrustdata", "www.braintrustdata")
    });

    Ok(LoginContext {
        source: LoginSource::Sdk(login),
        api_url,
        app_url,
        project: base.project.clone(),
    })
}

async fn login_with_sdk(base: &BaseArgs) -> Result<LoginContext> {
    let mut builder = BraintrustClient::builder().blocking_login(true);

    if let Some(api_url) = &base.api_url {
        builder = builder.api_url(api_url);
    }
    if let Some(project) = &base.project {
        builder = builder.default_project(project);
    }

    let client = builder.build().await;

    // Provide a better error message if SDK login fails
    let client = client.map_err(|e| {
        anyhow!(
            "Failed to authenticate: {}. \
            Try setting BRAINTRUST_API_KEY or run `bt auth login{}`",
            e,
            if base.profile != "DEFAULT" {
                format!(" --profile {}", base.profile)
            } else {
                String::new()
            }
        )
    })?;

    let login = client.wait_for_login().await?;

    let api_url = login
        .api_url
        .clone()
        .or_else(|| base.api_url.clone())
        .unwrap_or_else(|| "https://api.braintrust.dev".to_string());

    let app_url = base.app_url.clone().unwrap_or_else(|| {
        api_url
            .replace("api.braintrust", "www.braintrust")
            .replace("api.braintrustdata", "www.braintrustdata")
    });

    Ok(LoginContext {
        source: LoginSource::Sdk(login),
        api_url,
        app_url,
        project: base.project.clone(),
    })
}
