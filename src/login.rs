use anyhow::Result;
use braintrust_sdk_rust::{BraintrustClient, LoginState};

use crate::args::BaseArgs;

#[derive(Clone)]
pub struct LoginContext {
    pub login: LoginState,
    pub api_url: String,
    pub app_url: String,
}

pub async fn login(base: &BaseArgs) -> Result<LoginContext> {
    let mut builder = BraintrustClient::builder().blocking_login(true);
    if let Some(api_key) = &base.api_key {
        builder = builder.api_key(api_key);
    }
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

    // Derive app_url from api_url (api.braintrust.dev -> www.braintrust.dev)
    let app_url = base.app_url.clone().unwrap_or_else(|| {
        api_url
            .replace("api.braintrust", "www.braintrust")
            .replace("api.braintrustdata", "www.braintrustdata")
    });

    Ok(LoginContext {
        login,
        api_url,
        app_url,
    })
}
