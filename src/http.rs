use anyhow::{Context, Result};
use reqwest::header::HeaderValue;
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::args::BaseArgs;
use crate::auth::LoginContext;

pub const DEFAULT_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

#[derive(Clone)]
pub struct ApiClient {
    http: Client,
    base_url: String,
    api_key: String,
    org_name: String,
}

#[derive(Debug)]
pub struct HttpError {
    pub status: reqwest::StatusCode,
    pub body: String,
}

impl std::fmt::Display for HttpError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "request failed ({}): {}", self.status, self.body)
    }
}

impl std::error::Error for HttpError {}

#[derive(Debug, Deserialize)]
pub struct BtqlResponse<T> {
    pub data: Vec<T>,
}

impl ApiClient {
    pub fn new(base: &BaseArgs, ctx: &LoginContext) -> Result<Self> {
        let http = client_builder(base, DEFAULT_HTTP_TIMEOUT)?
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

    pub fn api_key(&self) -> &str {
        &self.api_key
    }

    pub fn org_name(&self) -> &str {
        &self.org_name
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
            return Err(HttpError { status, body }.into());
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
            return Err(HttpError { status, body }.into());
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
        self.post_with_headers_timeout(path, body, headers, None)
            .await
    }

    pub async fn post_with_headers_timeout<T, B>(
        &self,
        path: &str,
        body: &B,
        headers: &[(&str, &str)],
        timeout: Option<std::time::Duration>,
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
        if let Some(t) = timeout {
            request = request.timeout(t);
        }

        let response = request.send().await.context("request failed")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(HttpError { status, body }.into());
        }

        response.json().await.context("failed to parse response")
    }

    pub async fn post_with_headers_raw<B>(
        &self,
        path: &str,
        body: &B,
        headers: &[(&str, &str)],
    ) -> Result<reqwest::Response>
    where
        B: Serialize,
    {
        let url = self.url(path);
        let mut request = self.http.post(&url).bearer_auth(&self.api_key).json(body);

        for (key, value) in headers {
            request = request.header(*key, *value);
        }

        request.send().await.context("request failed")
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
            return Err(HttpError { status, body }.into());
        }

        Ok(())
    }

    pub async fn btql<T: DeserializeOwned>(&self, query: &str) -> Result<BtqlResponse<T>> {
        let body = json!({
            "query": query,
            "fmt": "json",
        });

        let org_name = self.org_name();
        let headers = if !org_name.is_empty() {
            vec![("x-bt-org-name", org_name)]
        } else {
            Vec::new()
        };

        self.post_with_headers("/btql", &body, &headers).await
    }
}

const UPLOAD_HTTP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(120);

pub async fn put_signed_url(
    base: &BaseArgs,
    url: &str,
    body: Vec<u8>,
    content_encoding: Option<&str>,
) -> Result<()> {
    let client = client_builder(base, UPLOAD_HTTP_TIMEOUT)?
        .build()
        .context("failed to build signed-url HTTP client")?;

    let mut request = client.put(url).body(body);
    if let Some(encoding) = content_encoding {
        request = request.header("Content-Encoding", encoding);
    }
    if url.contains(".blob.core.windows.net") {
        request = request.header("x-ms-blob-type", HeaderValue::from_static("BlockBlob"));
    }

    let response = request
        .send()
        .await
        .context("signed-url upload request failed")?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        return Err(HttpError { status, body }.into());
    }
    Ok(())
}

pub fn resolved_ca_cert_path(base: &BaseArgs) -> Option<std::path::PathBuf> {
    base.ca_cert.clone().or_else(|| {
        // Compatibility exception: `SSL_CERT_FILE` is a widely-used TLS env var and needs to
        // remain available even though bt's primary configuration surface is clap args/env.
        std::env::var_os("SSL_CERT_FILE")
            .filter(|value| !value.is_empty())
            .map(std::path::PathBuf::from)
    })
}

pub fn client_builder(
    base: &BaseArgs,
    timeout: std::time::Duration,
) -> Result<reqwest::ClientBuilder> {
    let mut builder = Client::builder().timeout(timeout);

    if let Some(ca_cert) = resolved_ca_cert_path(base) {
        let pem = std::fs::read(&ca_cert)
            .with_context(|| format!("failed to read CA bundle {}", ca_cert.display()))?;
        let certs = reqwest::Certificate::from_pem_bundle(&pem).with_context(|| {
            format!(
                "failed to parse PEM certificates from CA bundle {}",
                ca_cert.display()
            )
        })?;
        if certs.is_empty() {
            anyhow::bail!(
                "CA bundle {} did not contain any PEM certificates",
                ca_cert.display()
            );
        }
        for cert in certs {
            builder = builder.add_root_certificate(cert);
        }
    }

    Ok(builder)
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, OnceLock};

    use super::*;

    const VALID_TEST_CERT_PEM: &str = r#"-----BEGIN CERTIFICATE-----
MIICpDCCAYwCCQDtlc4RX+IuODANBgkqhkiG9w0BAQsFADAUMRIwEAYDVQQDDAls
b2NhbGhvc3QwHhcNMjYwMzE3MTY1MzAyWhcNMjYwMzE4MTY1MzAyWjAUMRIwEAYD
VQQDDAlsb2NhbGhvc3QwggEiMA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDX
K/y/7AlhzPBkIbiEiCt/l1Qfa99h8FdblOe8BCeJkpoW4Fw10mZnWBgX6peZMF7j
p4rjtIJTWkfl8eNoTPdOkYfmi6B3AwAZzl7VQCMgE0gCFkIrgXrkeLqP+q231UxE
wKgilRG3DWFfELZCQeFtq0jSBcnyWybw+o9SgajaQ+SJg7lbgT6o+8AwHQ54HBo+
VVZJ2CybZvmijQXGiVCMpZ34nxJVW/i6AbsFwp+CLMHOFjrpLuZpv61EnZaGsqsF
RG/VPiNca769Dr8YG4RtPRBKvyDMnUqEDkGwYXrhAVxvI3kKlQq3MHppGCSsjnVl
oqhWm//sE7znMJtuzIf7AgMBAAEwDQYJKoZIhvcNAQELBQADggEBAD1zS7eOkfU2
IzxjW7MAJce5JrAcRWWe3L2ORx+y+PS4uI0ms1FM4AopZ2FxXdbSSXLf5bqC2f2i
qy+8YbVdZacFtFLmnZicCXP86Na5JUYxZERDyqKN4GFwSrfELwLsuv9TWpir+p/H
3XxQ/8/eJdTHOunNtl4BVUefjGp9PVNb6NFvLDkSkNN37KcjNpB9jPVK970uZ5lb
kOx6ulbMXpNH73h5rwzgs6FbVbcAavPJKYGr170rDRxidpfRz3ex+RBQvcfFQeRx
NP64Q8OosOHraKRn7bvST7bXvGFZUp06aIFrlwdmSQPXU/6o4zYNmkR4RVv4VvQ7
cb0bfZ7fHHs=
-----END CERTIFICATE-----
"#;

    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn base_args() -> BaseArgs {
        BaseArgs {
            json: false,
            quiet: false,
            no_color: false,
            profile: None,
            org_name: None,
            project: None,
            api_key: None,
            prefer_profile: false,
            no_input: false,
            api_url: None,
            app_url: None,
            ca_cert: None,
            env_file: None,
        }
    }

    #[test]
    fn resolved_ca_cert_prefers_base_arg_over_ssl_cert_file() {
        let _guard = env_lock().lock().expect("lock env");
        let ssl_path = std::env::temp_dir().join("ssl-cert-file.pem");
        std::env::set_var("SSL_CERT_FILE", &ssl_path);

        let explicit = std::env::temp_dir().join("explicit-ca.pem");
        let mut base = base_args();
        base.ca_cert = Some(explicit.clone());

        assert_eq!(resolved_ca_cert_path(&base), Some(explicit));
        std::env::remove_var("SSL_CERT_FILE");
    }

    #[test]
    fn resolved_ca_cert_uses_ssl_cert_file_when_flag_missing() {
        let _guard = env_lock().lock().expect("lock env");
        let ssl_path = std::env::temp_dir().join("ssl-cert-file-only.pem");
        std::env::set_var("SSL_CERT_FILE", &ssl_path);

        assert_eq!(resolved_ca_cert_path(&base_args()), Some(ssl_path));
        std::env::remove_var("SSL_CERT_FILE");
    }

    #[test]
    fn client_builder_accepts_valid_ca_bundle() {
        let path = std::env::temp_dir().join(format!("bt-ca-bundle-{}.pem", std::process::id()));
        std::fs::write(&path, VALID_TEST_CERT_PEM).expect("write temp bundle");

        let mut base = base_args();
        base.ca_cert = Some(path.clone());
        let client = client_builder(&base, DEFAULT_HTTP_TIMEOUT)
            .expect("build client builder")
            .build();

        std::fs::remove_file(&path).expect("remove temp bundle");
        assert!(client.is_ok());
    }
}
