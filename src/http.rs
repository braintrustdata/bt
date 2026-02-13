use anyhow::{Context, Result};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::login::LoginContext;

#[derive(Clone)]
pub struct ApiClient {
    http: Client,
    base_url: String,
    api_key: String,
    org_name: String,
}

impl ApiClient {
    pub fn new(ctx: &LoginContext) -> Result<Self> {
        let http = Client::builder()
            .build()
            .context("failed to build HTTP client")?;

        Ok(Self {
            http,
            base_url: ctx.api_url.trim_end_matches('/').to_string(),
            api_key: ctx.login.api_key.clone(),
            org_name: ctx.login.org_name.clone(),
        })
    }

    pub fn url(&self, path: &str) -> String {
        let path = path.trim_start_matches('/');
        format!("{}/{}", self.base_url, path)
    }

    pub fn org_name(&self) -> &str {
        &self.org_name
    }

    pub fn with_org_name(mut self, org: String) -> Self {
        self.org_name = org;
        self
    }

    pub async fn get<T: DeserializeOwned>(&self, path: &str) -> Result<T> {
        let url = self.url(path);
        let response = self
            .http
            .get(&url)
            .bearer_auth(&self.api_key)
            .send()
            .await
            .context("request failed")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("request failed ({status}): {body}");
        }

        response.json().await.context("failed to parse response")
    }

    pub async fn post<T: DeserializeOwned, B: Serialize>(&self, path: &str, body: &B) -> Result<T> {
        let url = self.url(path);
        let response = self
            .http
            .post(&url)
            .bearer_auth(&self.api_key)
            .json(body)
            .send()
            .await
            .context("request failed")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("request failed ({status}): {body}");
        }

        response.json().await.context("failed to parse response")
    }

    pub async fn post_with_headers<T, B>(
        &self,
        path: &str,
        body: &B,
        headers: &[(&str, &str)],
    ) -> Result<T>
    where
        T: DeserializeOwned,
        B: Serialize,
    {
        let url = self.url(path);
        let mut request = self.http.post(&url).bearer_auth(&self.api_key).json(body);

        for (key, value) in headers {
            request = request.header(*key, *value);
        }

        let response = request.send().await.context("request failed")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("request failed ({status}): {body}");
        }

        response.json().await.context("failed to parse response")
    }

    pub async fn delete(&self, path: &str) -> Result<()> {
        let url = self.url(path);
        let response = self
            .http
            .delete(&url)
            .bearer_auth(&self.api_key)
            .send()
            .await
            .context("request failed")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            anyhow::bail!("request failed ({status}): {body}");
        }

        Ok(())
    }
}
