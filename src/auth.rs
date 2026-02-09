use anyhow::{anyhow, Context, Result};
use chrono::{Duration, Utc};
use clap::{Args, Subcommand};
use dialoguer::Input;
use serde::{Deserialize, Serialize};
use std::net::TcpListener;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener as TokioTcpListener;

use crate::config::{self, Profile};

#[derive(Debug, Clone, Args)]
pub struct AuthArgs {
    #[command(subcommand)]
    pub command: AuthSubcommand,
}

#[derive(Debug, Clone, Subcommand)]
pub enum AuthSubcommand {
    /// Log in to Braintrust using OAuth2 or API key
    Login(LoginArgs),
    /// Display current access token and TTL
    Token(TokenArgs),
    /// Log out (remove profile)
    Logout(LogoutArgs),
}

#[derive(Debug, Clone, Args)]
pub struct LoginArgs {
    /// Profile name to use
    #[arg(long, default_value = "DEFAULT")]
    pub profile: String,

    /// API URL (defaults to https://api.braintrust.dev)
    #[arg(long)]
    pub api_url: Option<String>,

    /// Use API key instead of OAuth2 (for headless/CI)
    #[arg(long)]
    pub api_key: bool,

    /// Optional default project for this profile
    #[arg(long)]
    pub project: Option<String>,
}

#[derive(Debug, Clone, Args)]
pub struct TokenArgs {
    /// Profile name to use
    #[arg(long, default_value = "DEFAULT")]
    pub profile: String,

    /// Output as JSON
    #[arg(short = 'j', long)]
    pub json: bool,

    /// Show full token (default: masked)
    #[arg(long)]
    pub show: bool,
}

#[derive(Debug, Clone, Args)]
pub struct LogoutArgs {
    /// Profile name to remove
    #[arg(long, default_value = "DEFAULT")]
    pub profile: String,
}

#[derive(Debug, Deserialize)]
struct OAuthDiscovery {
    authorization_endpoint: String,
    token_endpoint: String,
}

#[derive(Debug, Serialize)]
struct TokenRequest {
    grant_type: String,
    code: String,
    redirect_uri: String,
    client_id: String,
    code_verifier: String,
}

#[derive(Debug, Serialize)]
struct RefreshTokenRequest {
    grant_type: String,
    refresh_token: String,
    client_id: String,
}

#[derive(Debug, Deserialize)]
pub struct TokenResponse {
    pub access_token: String,
    #[serde(default)]
    pub refresh_token: Option<String>,
    #[serde(default)]
    pub expires_in: Option<i64>,
}

pub async fn run(args: AuthArgs) -> Result<()> {
    match args.command {
        AuthSubcommand::Login(args) => run_login(args).await,
        AuthSubcommand::Token(args) => run_token(args),
        AuthSubcommand::Logout(args) => run_logout(args),
    }
}

async fn run_login(args: LoginArgs) -> Result<()> {
    // Prompt for API URL if not provided
    let api_url = if let Some(url) = args.api_url {
        url
    } else {
        Input::<String>::new()
            .with_prompt("API URL")
            .default("https://api.braintrust.dev".to_string())
            .interact_text()?
    };

    let api_url = api_url.trim_end_matches('/').to_string();

    // Choose authentication method
    if args.api_key {
        login_with_api_key(&args.profile, &api_url, args.project.as_deref()).await?;
    } else {
        login_with_oauth2(&args.profile, &api_url, args.project.as_deref()).await?;
    }

    println!("✓ Successfully logged in to profile '{}'", args.profile);
    Ok(())
}

async fn login_with_api_key(
    profile_name: &str,
    api_url: &str,
    project: Option<&str>,
) -> Result<()> {
    let api_key: String = dialoguer::Password::new()
        .with_prompt("Enter your API key")
        .interact()?;

    let api_key = api_key.trim().to_string();

    if api_key.is_empty() {
        anyhow::bail!("API key cannot be empty");
    }

    // Try to fetch org info
    let org_name = fetch_org_name(api_url, &api_key).await.ok();

    let profile = Profile {
        api_url: api_url.to_string(),
        access_token: api_key,
        refresh_token: None,
        expires_at: None,
        org_name,
        project: project.map(|s| s.to_string()),
    };

    config::save_profile(profile_name, profile)?;
    Ok(())
}

async fn login_with_oauth2(profile_name: &str, api_url: &str, project: Option<&str>) -> Result<()> {
    // Discover OAuth endpoints
    let discovery = discover_oauth_endpoints(api_url).await?;

    // Generate PKCE challenge
    let code_verifier = generate_code_verifier();
    let code_challenge = generate_code_challenge(&code_verifier);

    // Start local server for callback
    let listener = TcpListener::bind("127.0.0.1:0")
        .context("failed to bind local server for OAuth callback")?;
    let port = listener.local_addr()?.port();
    let redirect_uri = format!("http://127.0.0.1:{}/callback", port);

    // Build authorization URL
    let client_id = "bt-cli"; // TODO: Use proper client ID from Braintrust
    let auth_url = format!(
        "{}?response_type=code&client_id={}&redirect_uri={}&code_challenge={}&code_challenge_method=S256&scope=openid%20profile%20email",
        discovery.authorization_endpoint,
        urlencoding::encode(client_id),
        urlencoding::encode(&redirect_uri),
        urlencoding::encode(&code_challenge)
    );

    println!("Opening browser for authentication...");
    println!("If browser doesn't open, visit: {}", auth_url);

    // Open browser
    if let Err(e) = open::that(&auth_url) {
        eprintln!("Warning: failed to open browser: {}", e);
    }

    // Wait for callback
    println!("Waiting for authentication callback...");
    let code = receive_callback(listener).await?;

    // Exchange code for tokens
    let token_response = exchange_code_for_token(
        &discovery.token_endpoint,
        &code,
        &redirect_uri,
        client_id,
        &code_verifier,
    )
    .await?;

    // Calculate expiry
    let expires_at = token_response
        .expires_in
        .map(|secs| Utc::now() + Duration::seconds(secs));

    // OAuth2 JWTs work without x-bt-org-name header, so we leave org_name as None
    let profile = Profile {
        api_url: api_url.to_string(),
        access_token: token_response.access_token,
        refresh_token: token_response.refresh_token,
        expires_at,
        org_name: None, // OAuth2 tokens don't need this
        project: project.map(|s| s.to_string()),
    };

    config::save_profile(profile_name, profile)?;
    Ok(())
}

async fn discover_oauth_endpoints(api_url: &str) -> Result<OAuthDiscovery> {
    let discovery_url = format!("{}/.well-known/oauth-authorization-server", api_url);

    let client = reqwest::Client::new();
    let response = client
        .get(&discovery_url)
        .send()
        .await
        .context("failed to discover OAuth endpoints")?;

    if !response.status().is_success() {
        anyhow::bail!(
            "OAuth discovery failed ({}): {}",
            response.status(),
            response.text().await.unwrap_or_default()
        );
    }

    response
        .json::<OAuthDiscovery>()
        .await
        .context("failed to parse OAuth discovery response")
}

fn generate_code_verifier() -> String {
    use rand::Rng;
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~";
    let mut rng = rand::thread_rng();
    (0..128)
        .map(|_| {
            let idx = rng.gen_range(0..CHARSET.len());
            CHARSET[idx] as char
        })
        .collect()
}

fn generate_code_challenge(verifier: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(verifier.as_bytes());
    let hash = hasher.finalize();
    base64_url_encode(&hash)
}

fn base64_url_encode(input: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(input)
}

async fn receive_callback(listener: TcpListener) -> Result<String> {
    listener.set_nonblocking(true)?;
    let listener = TokioTcpListener::from_std(listener)?;

    let (mut stream, _) = listener.accept().await?;

    let mut buffer = vec![0u8; 4096];
    let n = stream.read(&mut buffer).await?;
    let request = String::from_utf8_lossy(&buffer[..n]);

    // Parse the code from the request
    let code = parse_code_from_request(&request)
        .ok_or_else(|| anyhow!("failed to parse authorization code from callback"))?;

    // Send success response
    let response = "HTTP/1.1 200 OK\r\nContent-Type: text/html\r\n\r\n\
        <html><body><h1>Authentication successful!</h1>\
        <p>You can close this window and return to the terminal.</p></body></html>";
    stream.write_all(response.as_bytes()).await?;
    stream.flush().await?;

    Ok(code)
}

fn parse_code_from_request(request: &str) -> Option<String> {
    // Parse GET /callback?code=... HTTP/1.1
    let first_line = request.lines().next()?;
    let parts: Vec<&str> = first_line.split_whitespace().collect();
    if parts.len() < 2 {
        return None;
    }

    let path = parts[1];
    if let Some(query_start) = path.find('?') {
        let query = &path[query_start + 1..];
        for param in query.split('&') {
            if let Some((key, value)) = param.split_once('=') {
                if key == "code" {
                    return Some(urlencoding::decode(value).ok()?.into_owned());
                }
            }
        }
    }
    None
}

async fn exchange_code_for_token(
    token_endpoint: &str,
    code: &str,
    redirect_uri: &str,
    client_id: &str,
    code_verifier: &str,
) -> Result<TokenResponse> {
    let client = reqwest::Client::new();

    let params = TokenRequest {
        grant_type: "authorization_code".to_string(),
        code: code.to_string(),
        redirect_uri: redirect_uri.to_string(),
        client_id: client_id.to_string(),
        code_verifier: code_verifier.to_string(),
    };

    let response = client
        .post(token_endpoint)
        .form(&params)
        .send()
        .await
        .context("failed to exchange code for token")?;

    if !response.status().is_success() {
        anyhow::bail!(
            "token exchange failed ({}): {}",
            response.status(),
            response.text().await.unwrap_or_default()
        );
    }

    response
        .json::<TokenResponse>()
        .await
        .context("failed to parse token response")
}

pub async fn refresh_token_if_needed(profile_name: &str, mut profile: Profile) -> Result<Profile> {
    // Check if token is expired
    if let Some(expires_at) = profile.expires_at {
        if Utc::now() >= expires_at {
            // Token is expired, try to refresh
            if let Some(refresh_token) = &profile.refresh_token {
                println!("Access token expired, refreshing...");
                let token_response = refresh_access_token(&profile.api_url, refresh_token).await?;

                profile.access_token = token_response.access_token;
                if let Some(new_refresh) = token_response.refresh_token {
                    profile.refresh_token = Some(new_refresh);
                }
                if let Some(expires_in) = token_response.expires_in {
                    profile.expires_at = Some(Utc::now() + Duration::seconds(expires_in));
                }

                // Save updated profile
                config::save_profile(profile_name, profile.clone())?;
                println!("✓ Token refreshed successfully");
            } else {
                anyhow::bail!("Access token expired and no refresh token available. Please run `bt auth login --profile {}`", profile_name);
            }
        }
    }

    Ok(profile)
}

async fn refresh_access_token(api_url: &str, refresh_token: &str) -> Result<TokenResponse> {
    // Discover token endpoint
    let discovery = discover_oauth_endpoints(api_url).await?;

    let client = reqwest::Client::new();
    let client_id = "bt-cli"; // TODO: Use proper client ID

    let params = RefreshTokenRequest {
        grant_type: "refresh_token".to_string(),
        refresh_token: refresh_token.to_string(),
        client_id: client_id.to_string(),
    };

    let response = client
        .post(&discovery.token_endpoint)
        .form(&params)
        .send()
        .await
        .context("failed to refresh token")?;

    if !response.status().is_success() {
        anyhow::bail!(
            "token refresh failed ({}): {}",
            response.status(),
            response.text().await.unwrap_or_default()
        );
    }

    response
        .json::<TokenResponse>()
        .await
        .context("failed to parse refresh token response")
}

async fn fetch_org_name(api_url: &str, token: &str) -> Result<String> {
    // Try to discover the user's org name automatically
    if let Ok(org) = discover_user_org(api_url, token).await {
        println!("✓ Discovered organization: {}", org);
        return Ok(org);
    }

    // Try to get a list of available orgs
    let orgs = fetch_available_orgs(api_url, token).await;

    if !orgs.is_empty() {
        if orgs.len() == 1 {
            // Only one org found, use it automatically
            println!("✓ Found organization: {}", orgs[0]);
            return Ok(orgs[0].clone());
        }

        // Multiple orgs found, let user select
        println!("Found {} organizations:", orgs.len());
        let selection = dialoguer::Select::new()
            .with_prompt("Select your organization")
            .items(&orgs)
            .default(0)
            .interact()?;

        return Ok(orgs[selection].clone());
    }

    // Fallback: prompt user for org name with helpful guidance
    eprintln!("\nCould not automatically discover your organization.");
    eprintln!("You can find your org name in the Braintrust web app URL:");
    eprintln!("  https://www.braintrust.dev/app/YOUR-ORG-NAME/...");
    eprintln!();

    let org_name: String = dialoguer::Input::new()
        .with_prompt("Organization name")
        .allow_empty(false)
        .interact_text()?;

    Ok(org_name.trim().to_string())
}

async fn discover_user_org(api_url: &str, token: &str) -> Result<String> {
    let client = reqwest::Client::new();

    // Strategy 1: Fetch API keys to get org_id (works as org_name in headers!)
    let response = client
        .get(format!("{}/v1/api_key", api_url))
        .bearer_auth(token)
        .send()
        .await?;

    if response.status().is_success() {
        #[derive(Deserialize)]
        struct ApiKeyResponse {
            objects: Vec<ApiKeyObject>,
        }

        #[derive(Deserialize)]
        struct ApiKeyObject {
            org_id: String,
        }

        if let Ok(keys) = response.json::<ApiKeyResponse>().await {
            if let Some(key) = keys.objects.first() {
                // The org_id can be used in the x-bt-org-name header!
                return Ok(key.org_id.clone());
            }
        }
    }

    // Strategy 2: Try to fetch a project - error messages sometimes include [user_org=...]
    let response = client
        .get(format!(
            "{}/v1/organization/00000000-0000-0000-0000-000000000000",
            api_url
        ))
        .bearer_auth(token)
        .send()
        .await?;

    let error_text = response.text().await.unwrap_or_default();

    // Extract org name from error message like [user_org=Braintrust Demos]
    if let Some(org_start) = error_text.find("user_org=") {
        let org_part = &error_text[org_start + 9..];
        if let Some(org_end) = org_part.find(']') {
            return Ok(org_part[..org_end].to_string());
        }
    }

    anyhow::bail!("Could not extract org from API response")
}

async fn fetch_available_orgs(api_url: &str, token: &str) -> Vec<String> {
    let client = reqwest::Client::new();
    let mut orgs = std::collections::HashSet::new();

    // Try to fetch projects and extract org names
    let response = client
        .get(format!("{}/v1/project?limit=100", api_url))
        .bearer_auth(token)
        .send()
        .await;

    if let Ok(resp) = response {
        if resp.status().is_success() {
            #[derive(Deserialize)]
            struct ProjectResponse {
                objects: Vec<ProjectObject>,
            }

            #[derive(Deserialize)]
            struct ProjectObject {
                org_id: String,
            }

            if let Ok(projects) = resp.json::<ProjectResponse>().await {
                // For each unique org_id, try to get the org name
                let unique_org_ids: std::collections::HashSet<_> =
                    projects.objects.iter().map(|p| p.org_id.clone()).collect();

                for org_id in unique_org_ids {
                    // Try to fetch org - even if it fails, we might get org name from error
                    if let Ok(org_resp) = client
                        .get(format!("{}/v1/organization/{}", api_url, org_id))
                        .bearer_auth(token)
                        .send()
                        .await
                    {
                        if let Ok(error_text) = org_resp.text().await {
                            // Try to extract from error message
                            if let Some(org_start) = error_text.find("user_org=") {
                                let org_part = &error_text[org_start + 9..];
                                if let Some(org_end) = org_part.find(']') {
                                    orgs.insert(org_part[..org_end].to_string());
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    let mut org_list: Vec<String> = orgs.into_iter().collect();
    org_list.sort();
    org_list
}

fn run_token(args: TokenArgs) -> Result<()> {
    let profile = config::get_profile(&args.profile)?.ok_or_else(|| {
        anyhow!(
            "Profile '{}' not found. Run `bt auth login --profile {}`",
            args.profile,
            args.profile
        )
    })?;

    if args.json {
        let ttl_seconds = profile
            .expires_at
            .map(|exp| (exp - Utc::now()).num_seconds());

        let token_value = if args.show {
            profile.access_token.clone()
        } else {
            mask_token(&profile.access_token)
        };

        let output = serde_json::json!({
            "token": token_value,
            "expires_at": profile.expires_at,
            "ttl_seconds": ttl_seconds,
        });

        println!("{}", serde_json::to_string_pretty(&output)?);
    } else {
        let token_display = if args.show {
            &profile.access_token
        } else {
            &mask_token(&profile.access_token)
        };

        println!("Token: {}", token_display);

        if let Some(expires_at) = profile.expires_at {
            let ttl = expires_at - Utc::now();
            if ttl.num_seconds() > 0 {
                println!("Expires: {} (in {} seconds)", expires_at, ttl.num_seconds());
            } else {
                println!("Expires: {} (EXPIRED)", expires_at);
            }
        } else {
            println!("Expires: Never");
        }
    }

    Ok(())
}

fn run_logout(args: LogoutArgs) -> Result<()> {
    let removed = config::delete_profile(&args.profile)?;

    if removed {
        println!("✓ Logged out from profile '{}'", args.profile);
    } else {
        println!("Profile '{}' not found (already logged out)", args.profile);
    }

    Ok(())
}

fn mask_token(token: &str) -> String {
    if token.len() <= 8 {
        return "***".to_string();
    }
    format!("{}...{}", &token[..4], &token[token.len() - 4..])
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::time::{SystemTime, UNIX_EPOCH};

    #[test]
    fn test_generate_code_verifier() {
        let verifier = generate_code_verifier();
        // PKCE verifier should be 43-128 characters
        assert!(verifier.len() >= 43 && verifier.len() <= 128);
        // Should be URL-safe characters only (RFC 7636 allows: A-Z a-z 0-9 - . _ ~)
        assert!(verifier.chars().all(|c| c.is_alphanumeric()
            || c == '-'
            || c == '.'
            || c == '_'
            || c == '~'));
    }

    #[test]
    fn test_generate_code_verifier_randomness() {
        let v1 = generate_code_verifier();
        let v2 = generate_code_verifier();
        // Two calls should produce different verifiers
        assert_ne!(v1, v2);
    }

    #[test]
    fn test_generate_code_challenge() {
        let verifier = "test_verifier_string";
        let challenge = generate_code_challenge(verifier);

        // Should be base64url encoded (no padding)
        assert!(!challenge.contains('='));
        assert!(!challenge.contains('+'));
        assert!(!challenge.contains('/'));

        // SHA256 hash should produce 32 bytes -> 43 base64url chars (without padding)
        assert_eq!(challenge.len(), 43);
    }

    #[test]
    fn test_generate_code_challenge_deterministic() {
        let verifier = "same_verifier";
        let c1 = generate_code_challenge(verifier);
        let c2 = generate_code_challenge(verifier);
        // Same verifier should produce same challenge
        assert_eq!(c1, c2);
    }

    #[test]
    fn test_base64_url_encode() {
        let input = b"hello world";
        let encoded = base64_url_encode(input);

        // Should not contain standard base64 chars
        assert!(!encoded.contains('+'));
        assert!(!encoded.contains('/'));
        assert!(!encoded.contains('='));
    }

    #[test]
    fn test_base64_url_encode_known_value() {
        // SHA256 of "test" in base64url
        let input = b"\x9f\x86\xd0\x81\x88\x4c\x7d\x65\x9a\x2f\xea\xa0\xc5\x5a\xd0\x15\xa3\xbf\x4f\x1b\x2b\x0b\x82\x2c\xd1\x5d\x6c\x15\xb0\xf0\x0a\x08";
        let encoded = base64_url_encode(input);
        assert_eq!(encoded, "n4bQgYhMfWWaL-qgxVrQFaO_TxsrC4Is0V1sFbDwCgg");
    }

    #[test]
    fn test_parse_code_from_request_valid() {
        let request = "GET /?code=test_code_12345&state=xyz HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let code = parse_code_from_request(request);
        assert_eq!(code, Some("test_code_12345".to_string()));
    }

    #[test]
    fn test_parse_code_from_request_no_code() {
        let request = "GET /?state=xyz HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let code = parse_code_from_request(request);
        assert_eq!(code, None);
    }

    #[test]
    fn test_parse_code_from_request_with_multiple_params() {
        let request = "GET /?foo=bar&code=my_auth_code&state=abc HTTP/1.1\r\n";
        let code = parse_code_from_request(request);
        assert_eq!(code, Some("my_auth_code".to_string()));
    }

    #[test]
    fn test_parse_code_from_request_invalid() {
        let request = "POST / HTTP/1.1\r\nHost: localhost\r\n\r\n";
        let code = parse_code_from_request(request);
        assert_eq!(code, None);
    }

    #[test]
    fn test_mask_token_long() {
        let token = "brt_1234567890abcdef";
        let masked = mask_token(token);
        assert_eq!(masked, "brt_...cdef");
    }

    #[test]
    fn test_mask_token_short() {
        let token = "short";
        let masked = mask_token(token);
        assert_eq!(masked, "***");
    }

    #[test]
    fn test_mask_token_exact_8() {
        let token = "12345678";
        let masked = mask_token(token);
        assert_eq!(masked, "***");
    }

    #[test]
    fn test_mask_token_jwt() {
        let token = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.dozjgNryP4J3jVmNHl0w5N_XgL0n3I9PlFUP0THsR8U";
        let masked = mask_token(token);
        assert_eq!(masked, "eyJh...sR8U");
    }

    #[tokio::test]
    #[serial]
    async fn test_refresh_token_if_needed_not_expired() {
        // Create a temp config
        let config_path = std::env::temp_dir().join(format!(
            "bt-auth-test-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::env::set_var("BT_CONFIG", &config_path);

        let future_time = Utc::now() + Duration::hours(1);
        let profile = Profile {
            api_url: "https://api.test.com".to_string(),
            access_token: "valid_token".to_string(),
            refresh_token: Some("refresh".to_string()),
            expires_at: Some(future_time),
            org_name: None,
            project: None,
        };

        config::save_profile("test", profile.clone()).unwrap();

        // Should not refresh since token is still valid
        let result = refresh_token_if_needed("test", profile.clone()).await;
        assert!(result.is_ok());
        let refreshed = result.unwrap();

        // Token should be unchanged
        assert_eq!(refreshed.access_token, "valid_token");

        // Cleanup
        std::fs::remove_file(&config_path).ok();
        std::env::remove_var("BT_CONFIG");
    }

    #[tokio::test]
    #[serial]
    async fn test_refresh_token_if_needed_expired_no_refresh_token() {
        let config_path = std::env::temp_dir().join(format!(
            "bt-auth-test-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::env::set_var("BT_CONFIG", &config_path);

        let past_time = Utc::now() - Duration::hours(1);
        let profile = Profile {
            api_url: "https://api.test.com".to_string(),
            access_token: "expired_token".to_string(),
            refresh_token: None, // No refresh token (API key)
            expires_at: Some(past_time),
            org_name: Some("test-org".to_string()),
            project: None,
        };

        config::save_profile("test", profile.clone()).unwrap();

        // Should fail since token is expired and no refresh token
        let result = refresh_token_if_needed("test", profile).await;
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("expired"));

        // Cleanup
        std::fs::remove_file(&config_path).ok();
        std::env::remove_var("BT_CONFIG");
    }

    #[tokio::test]
    #[serial]
    async fn test_refresh_token_if_needed_no_expiry() {
        let config_path = std::env::temp_dir().join(format!(
            "bt-auth-test-{}-{}.json",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::env::set_var("BT_CONFIG", &config_path);

        let profile = Profile {
            api_url: "https://api.test.com".to_string(),
            access_token: "api_key_token".to_string(),
            refresh_token: None,
            expires_at: None, // API keys don't expire
            org_name: Some("test-org".to_string()),
            project: None,
        };

        config::save_profile("test", profile.clone()).unwrap();

        // Should pass through without refresh (API keys don't expire)
        let result = refresh_token_if_needed("test", profile.clone()).await;
        assert!(result.is_ok());
        let refreshed = result.unwrap();
        assert_eq!(refreshed.access_token, "api_key_token");

        // Cleanup
        std::fs::remove_file(&config_path).ok();
        std::env::remove_var("BT_CONFIG");
    }

    #[test]
    fn test_oauth_discovery_deserialization() {
        let json = r#"{
            "authorization_endpoint": "https://auth.example.com/authorize",
            "token_endpoint": "https://auth.example.com/token"
        }"#;

        let discovery: Result<OAuthDiscovery, _> = serde_json::from_str(json);
        assert!(discovery.is_ok());
        let discovery = discovery.unwrap();
        assert_eq!(
            discovery.authorization_endpoint,
            "https://auth.example.com/authorize"
        );
        assert_eq!(discovery.token_endpoint, "https://auth.example.com/token");
    }

    #[test]
    fn test_token_response_deserialization() {
        let json = r#"{
            "access_token": "at_12345",
            "refresh_token": "rt_67890",
            "expires_in": 3600
        }"#;

        let response: Result<TokenResponse, _> = serde_json::from_str(json);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.access_token, "at_12345");
        assert_eq!(response.refresh_token, Some("rt_67890".to_string()));
        assert_eq!(response.expires_in, Some(3600));
    }

    #[test]
    fn test_token_response_without_refresh() {
        let json = r#"{
            "access_token": "at_only"
        }"#;

        let response: Result<TokenResponse, _> = serde_json::from_str(json);
        assert!(response.is_ok());
        let response = response.unwrap();
        assert_eq!(response.access_token, "at_only");
        assert_eq!(response.refresh_token, None);
        assert_eq!(response.expires_in, None);
    }
}
