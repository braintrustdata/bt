//! API access and transport configuration for unrelated downloads.
use crate::auth::LoginContext;
use anyhow::{Context, Result};
pub use braintrust_api_client::backend::btql::{BtqlOptions, Pagination};
pub use braintrust_api_client::backend::{
    BtqlResponse, OperationOptions, ResponseParseError, RetryReason,
};
use reqwest::ClientBuilder;
use std::time::Duration;
pub const DEFAULT_HTTP_TIMEOUT: Duration = Duration::from_secs(30);
pub const BT_USER_AGENT: &str = concat!("bt-cli/", env!("CARGO_PKG_VERSION"));
pub fn build_http_client(timeout: std::time::Duration) -> Result<reqwest::Client> {
    build_http_client_from_builder(reqwest::Client::builder().timeout(timeout))
}

pub fn build_http_client_from_builder(builder: ClientBuilder) -> Result<reqwest::Client> {
    configured_http_builder(builder)
        .build()
        .context("failed to build HTTP client")
}

fn configured_http_builder(builder: ClientBuilder) -> ClientBuilder {
    // Prefer the platform/native root store so standard envs like SSL_CERT_FILE
    // are honored consistently across the CLI.
    let roots = rustls_native_certs::load_native_certs()
        .certs
        .into_iter()
        .filter_map(|cert| reqwest::Certificate::from_der(cert.as_ref()).ok());
    builder.user_agent(BT_USER_AGENT).tls_certs_only(roots)
}

#[derive(Clone)]
pub struct ApiClient(braintrust_api_client::backend::BackendClient);
impl std::ops::Deref for ApiClient {
    type Target = braintrust_api_client::backend::BackendClient;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
impl ApiClient {
    pub fn new(ctx: &LoginContext) -> Result<Self> {
        Ok(Self(braintrust_api_client::backend::BackendClient::new(
            &ctx.api_url,
            &ctx.app_url,
            &ctx.login.api_key().context("login state missing API key")?,
            &ctx.login.org_id().unwrap_or_default(),
            &ctx.login.org_name().unwrap_or_default(),
            BT_USER_AGENT,
        )?))
    }
    pub fn with_options(&self, options: OperationOptions) -> Self {
        Self(self.0.with_options(options))
    }
}

pub use braintrust_api_client::backend::{
    api_error, build_api_client, put_signed_url, put_signed_url_with_headers,
};

#[cfg(test)]
pub use braintrust_api_client::backend::HttpError;
