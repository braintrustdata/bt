use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use reqwest::Client;
use serde::Deserialize;
use tokio::time::sleep;

const POLL_INTERVAL: Duration = Duration::from_secs(2);
const SLOW_DOWN_INCREMENT: Duration = Duration::from_secs(1);
const MAX_POLL_INTERVAL: Duration = Duration::from_secs(30);
const POLL_HARD_TIMEOUT: Duration = Duration::from_secs(3 * 60);
const CREATE_REQUEST_TIMEOUT: Duration = Duration::from_secs(15);
const POLL_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize)]
pub struct WizardSessionCreateResponse {
    pub session_token: String,
    pub poll_token: String,
    #[allow(dead_code)]
    pub expires_at: String,
    pub login_path: String,
    pub verification_code: String,
}

#[derive(Debug, Clone)]
pub struct WizardSessionComplete {
    pub api_key: String,
    #[allow(dead_code)]
    pub org_id: String,
    pub org_name: String,
    pub project_id: String,
    pub project_name: String,
}

pub struct WizardSessionAuthClient {
    http: Client,
    app_url: String,
}

impl WizardSessionAuthClient {
    pub fn new(http: Client, app_url: impl Into<String>) -> Self {
        let mut app_url = app_url.into();
        while app_url.ends_with('/') {
            app_url.pop();
        }
        Self { http, app_url }
    }

    pub async fn create_session(&self) -> Result<WizardSessionCreateResponse> {
        let url = format!("{}/api/cli/wizard-session/create", self.app_url);
        let res = self
            .http
            .post(&url)
            .header("Accept", "application/json")
            .timeout(CREATE_REQUEST_TIMEOUT)
            .send()
            .await
            .with_context(|| format!("POST {url}"))?;
        if !res.status().is_success() {
            let status = res.status();
            let body = res.text().await.unwrap_or_default();
            bail!("Wizard session create failed: {status} {body}");
        }
        res.json::<WizardSessionCreateResponse>()
            .await
            .context("parsing wizard session create response")
    }

    pub fn build_login_url(&self, session: &WizardSessionCreateResponse) -> String {
        let path = if session.login_path.starts_with('/') {
            session.login_path.clone()
        } else {
            format!("/{}", session.login_path)
        };
        format!("{}{}", self.app_url, path)
    }

    pub async fn poll_session(
        &self,
        session_token: &str,
        poll_token: &str,
    ) -> Result<WizardSessionComplete> {
        let url = format!(
            "{}/api/cli/wizard-session/poll?session_token={}",
            self.app_url,
            urlencoding::encode(session_token)
        );
        let deadline = std::time::Instant::now() + POLL_HARD_TIMEOUT;
        let mut interval = POLL_INTERVAL;

        while std::time::Instant::now() < deadline {
            sleep(interval).await;
            let res = self
                .http
                .get(&url)
                .header("Accept", "application/json")
                .header("Authorization", format!("Bearer {poll_token}"))
                .timeout(POLL_REQUEST_TIMEOUT)
                .send()
                .await
                .with_context(|| format!("GET {url}"))?;
            if res.status().as_u16() == 429 {
                interval = (interval + SLOW_DOWN_INCREMENT).min(MAX_POLL_INTERVAL);
                continue;
            }
            let status = res.status();
            let body = res.text().await.unwrap_or_default();
            if !status.is_success() {
                bail!("Wizard session poll failed: {status} {body}");
            }
            let json: serde_json::Value = serde_json::from_str(&body)
                .with_context(|| format!("parsing poll response: {body}"))?;
            match json.get("status").and_then(|v| v.as_str()) {
                Some("pending") => {}
                Some("expired") => bail!("Wizard session expired before approval."),
                Some("claimed") => bail!("Wizard session was already claimed by another client."),
                Some("complete") => return parse_complete(&json),
                other => bail!(
                    "Unexpected wizard session status: {} (body: {body})",
                    other.unwrap_or("<missing>")
                ),
            }
        }
        Err(anyhow!("Wizard session timed out."))
    }
}

fn parse_complete(json: &serde_json::Value) -> Result<WizardSessionComplete> {
    let api_key = require_string(json, "api_key")?;
    let org_id = require_string(json, "org_id")?;
    let org_name = require_string(json, "org_name")?;
    let project_id = require_string(json, "project_id")?;
    let project_name = require_string(json, "project_name")?;
    Ok(WizardSessionComplete {
        api_key,
        org_id,
        org_name,
        project_id,
        project_name,
    })
}

fn require_string(json: &serde_json::Value, key: &str) -> Result<String> {
    json.get(key)
        .and_then(|v| v.as_str())
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .ok_or_else(|| anyhow!("Wizard session complete response missing field `{key}`"))
}
