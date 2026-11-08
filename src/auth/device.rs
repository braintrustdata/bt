use anyhow::{bail, Context, Result};
use oauth2::{PkceCodeChallenge, PkceCodeVerifier};
use reqwest::{Client, StatusCode, Url};
use serde::Deserialize;
use std::time::Duration;
use tokio::time::{sleep, timeout_at, Instant};

use super::{OAuthErrorResponse, OAuthTokenResponse};
use crate::http::{build_http_client_from_builder, DEFAULT_HTTP_TIMEOUT};

#[derive(Deserialize)]
struct DeviceAuthorization {
    device_code: String,
    user_code: String,
    verification_uri_complete: Url,
    expires_in: u64,
    #[serde(default = "default_poll_interval")]
    interval: u64,
}

fn default_poll_interval() -> u64 {
    5
}

pub(super) async fn login(
    app_url: &str,
    client_id: &str,
    challenge: &PkceCodeChallenge,
    verifier: &PkceCodeVerifier,
    no_browser: bool,
    quiet: bool,
) -> Result<Option<OAuthTokenResponse>> {
    let http = build_http_client_from_builder(
        Client::builder()
            .timeout(DEFAULT_HTTP_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none()),
    )
    .context("failed to initialize device login HTTP client")?;
    let Some(device) = request_authorization(&http, app_url, client_id, challenge).await? else {
        return Ok(None);
    };
    if !quiet {
        eprintln!(
            "Sign in to Braintrust\n  {}\n\nConfirmation code: {}\nWaiting for browser approval...",
            device.verification_uri_complete, device.user_code
        );
    }
    if !no_browser {
        if let Err(error) = open::that(device.verification_uri_complete.as_str()) {
            eprintln!("warning: failed to open browser automatically: {error}");
        }
    }
    let tokens = poll_authorization(&http, app_url, client_id, verifier, &device).await?;
    Ok(Some(tokens))
}

async fn request_authorization(
    http: &Client,
    app_url: &str,
    client_id: &str,
    challenge: &PkceCodeChallenge,
) -> Result<Option<DeviceAuthorization>> {
    let response = http
        .post(format!(
            "{}/api/mcp/oauth/device",
            app_url.trim_end_matches('/')
        ))
        .form(&[
            ("client_id", client_id),
            ("scope", "mcp"),
            ("code_challenge", challenge.as_str()),
            ("code_challenge_method", "S256"),
        ])
        .send()
        .await
        .context("failed to start device login")?;
    if matches!(
        response.status(),
        StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED
    ) {
        return Ok(None);
    }
    let response = response
        .error_for_status()
        .context("device login request failed")?;
    let device: DeviceAuthorization = response
        .json()
        .await
        .context("invalid device login response")?;
    let app = Url::parse(app_url).context("invalid Braintrust app URL")?;
    if device.verification_uri_complete.origin() != app.origin() {
        bail!("device login returned a verification URL outside the configured Braintrust app");
    }
    if device.expires_in == 0 || device.interval == 0 || device.device_code.is_empty() {
        bail!("device login returned invalid expiration, interval, or device code");
    }
    Ok(Some(device))
}

async fn poll_authorization(
    http: &Client,
    app_url: &str,
    client_id: &str,
    verifier: &PkceCodeVerifier,
    device: &DeviceAuthorization,
) -> Result<OAuthTokenResponse> {
    let expires_in = Duration::from_secs(device.expires_in);
    let mut interval = Duration::from_secs(device.interval);
    let deadline = Instant::now()
        .checked_add(expires_in)
        .context("invalid device login expiration")?;
    timeout_at(deadline, async {
        loop {
            sleep(interval).await;
            let response = http
                .post(format!(
                    "{}/api/mcp/oauth/token",
                    app_url.trim_end_matches('/')
                ))
                .form(&[
                    ("grant_type", "urn:ietf:params:oauth:grant-type:device_code"),
                    ("device_code", device.device_code.as_str()),
                    ("client_id", client_id),
                    ("code_verifier", verifier.secret()),
                ])
                .send()
                .await;
            let response = match response {
                Ok(response) => response,
                Err(error) if error.is_timeout() => {
                    interval = interval.saturating_mul(2);
                    continue;
                }
                Err(error) => return Err(error).context("failed to poll device login"),
            };
            if response.status().is_success() {
                return response
                    .json()
                    .await
                    .context("invalid device login token response");
            }
            let status = response.status();
            let error: OAuthErrorResponse = response
                .json()
                .await
                .context("invalid device login error response")?;
            match error.error.as_deref() {
                Some("authorization_pending") => continue,
                Some("slow_down") => {
                    interval = interval.saturating_add(Duration::from_secs(5));
                }
                Some("access_denied") => bail!("login was declined in the browser"),
                Some("expired_token") => bail!("login expired; run `bt login --oauth` again"),
                _ => bail!(
                    "device login failed ({status}): {}",
                    error.error_description.or(error.error).unwrap_or_default()
                ),
            }
        }
    })
    .await
    .context("login expired while waiting for browser approval; run `bt login --oauth` again")?
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::{dev::ServerHandle, web, App, HttpResponse, HttpServer};
    use serde_json::{json, Value};
    use std::collections::{BTreeMap, VecDeque};
    use std::net::TcpListener;
    use std::sync::Mutex;

    struct State {
        authorization: Mutex<(StatusCode, Value)>,
        tokens: Mutex<VecDeque<(StatusCode, Value)>>,
        polls: Mutex<Vec<Instant>>,
    }

    async fn server() -> (String, web::Data<State>, ServerHandle) {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind");
        let origin = format!("http://{}", listener.local_addr().expect("address"));
        let state = web::Data::new(State {
            authorization: Mutex::new((
                StatusCode::OK,
                json!({
                    "device_code": "test-device-code",
                    "user_code": "ABCD-2345",
                    "verification_uri_complete": format!("{origin}/device/ABCD-2345"),
                    "expires_in": 30,
                    "interval": 1,
                }),
            )),
            tokens: Mutex::new(VecDeque::new()),
            polls: Mutex::new(Vec::new()),
        });
        let app_state = state.clone();
        let server = HttpServer::new(move || {
            App::new()
                .app_data(app_state.clone())
                .route("/api/mcp/oauth/device", web::post().to(|state: web::Data<State>, form: web::Form<BTreeMap<String, String>>| async move {
                    assert_eq!(form.get("client_id").map(String::as_str), Some("test-client"));
                    assert_eq!(form.get("code_challenge_method").map(String::as_str), Some("S256"));
                    let (status, body) = &*state.authorization.lock().expect("authorization lock");
                    HttpResponse::build(actix_web::http::StatusCode::from_u16(status.as_u16()).expect("status")).json(body)
                }))
                .route("/api/mcp/oauth/token", web::post().to(|state: web::Data<State>, form: web::Form<BTreeMap<String, String>>| async move {
                    assert_eq!(form.get("grant_type").map(String::as_str), Some("urn:ietf:params:oauth:grant-type:device_code"));
                    assert_eq!(form.get("device_code").map(String::as_str), Some("test-device-code"));
                    assert_eq!(form.get("code_verifier").map(String::as_str), Some("test-verifier"));
                    state.polls.lock().expect("polls lock").push(Instant::now());
                    let (status, body) = state.tokens.lock().expect("tokens lock").pop_front().expect("unexpected token request");
                    HttpResponse::build(actix_web::http::StatusCode::from_u16(status.as_u16()).expect("status")).json(body)
                }))
        }).workers(1).listen(listener).expect("listen").run();
        let handle = server.handle();
        tokio::spawn(server);
        (origin, state, handle)
    }

    #[tokio::test]
    async fn polls_until_approved_and_backs_off_when_requested() {
        let (origin, state, handle) = server().await;
        state.tokens.lock().expect("tokens").extend([
            (
                StatusCode::BAD_REQUEST,
                json!({"error": "authorization_pending"}),
            ),
            (StatusCode::BAD_REQUEST, json!({"error": "slow_down"})),
            (
                StatusCode::OK,
                json!({"access_token": "test-access", "refresh_token": "test-refresh"}),
            ),
        ]);
        let (challenge, _) = PkceCodeChallenge::new_random_sha256();
        let tokens = login(
            &origin,
            "test-client",
            &challenge,
            &PkceCodeVerifier::new("test-verifier".into()),
            true,
            true,
        )
        .await
        .expect("login")
        .expect("tokens");
        assert_eq!(tokens.access_token, "test-access");
        assert_eq!(tokens.refresh_token.as_deref(), Some("test-refresh"));
        let polls = state.polls.lock().expect("polls");
        assert_eq!(polls.len(), 3);
        assert!(polls[2].duration_since(polls[1]) >= Duration::from_secs(6));
        drop(polls);
        handle.stop(false).await;
    }

    #[tokio::test]
    async fn falls_back_only_for_an_unsupported_endpoint_and_rejects_external_links() {
        let (origin, state, handle) = server().await;
        let http = Client::new();
        let (challenge, _) = PkceCodeChallenge::new_random_sha256();
        for status in [StatusCode::NOT_FOUND, StatusCode::METHOD_NOT_ALLOWED] {
            state.authorization.lock().expect("authorization").0 = status;
            assert!(
                request_authorization(&http, &origin, "test-client", &challenge)
                    .await
                    .expect("unsupported")
                    .is_none()
            );
        }
        state.authorization.lock().expect("authorization").0 = StatusCode::SERVICE_UNAVAILABLE;
        assert!(
            request_authorization(&http, &origin, "test-client", &challenge)
                .await
                .is_err()
        );
        {
            let mut authorization = state.authorization.lock().expect("authorization");
            authorization.0 = StatusCode::OK;
            authorization.1["verification_uri_complete"] =
                json!("https://other.example.com/device/ABCD-2345");
        }
        assert!(
            request_authorization(&http, &origin, "test-client", &challenge)
                .await
                .err()
                .expect("invalid URL")
                .to_string()
                .contains("outside")
        );
        handle.stop(false).await;
    }

    #[tokio::test]
    async fn stops_on_denial_and_on_the_authorization_deadline() {
        let (origin, state, handle) = server().await;
        state
            .tokens
            .lock()
            .expect("tokens")
            .push_back((StatusCode::BAD_REQUEST, json!({"error": "access_denied"})));
        let (challenge, verifier) = (
            PkceCodeChallenge::new_random_sha256().0,
            PkceCodeVerifier::new("test-verifier".into()),
        );
        let error = login(&origin, "test-client", &challenge, &verifier, true, true)
            .await
            .err()
            .expect("denied");
        assert!(error.to_string().contains("declined"));
        {
            let mut authorization = state.authorization.lock().expect("authorization");
            authorization.1["expires_in"] = json!(1);
            authorization.1["interval"] = json!(10);
        }
        let error = login(&origin, "test-client", &challenge, &verifier, true, true)
            .await
            .err()
            .expect("expired");
        assert!(error.to_string().contains("expired"));
        assert_eq!(state.polls.lock().expect("polls").len(), 1);
        handle.stop(false).await;
    }
}
