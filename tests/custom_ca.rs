use std::process::Command;
use std::sync::Arc;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio_rustls::rustls::{self, pki_types::PrivatePkcs8KeyDer};
use tokio_rustls::TlsAcceptor;

// This test exercises the real CLI and its SDK transports in separate processes,
// so their environment and trust stores cannot affect other tests.
#[tokio::test(flavor = "multi_thread")]
async fn custom_ca_is_used_for_login_and_generated_requests() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(
            vec![cert.cert.der().clone()],
            PrivatePkcs8KeyDer::from(cert.key_pair.serialize_der()).into(),
        )
        .unwrap();
    let acceptor = TlsAcceptor::from(Arc::new(config));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "https://localhost:{}",
        listener.local_addr().unwrap().port()
    );
    let server_url = url.clone();
    let server = tokio::spawn(async move {
        loop {
            let (socket, _) = listener.accept().await.unwrap();
            let acceptor = acceptor.clone();
            let url = server_url.clone();
            tokio::spawn(async move {
                let Ok(mut socket) = acceptor.accept(socket).await else {
                    return;
                };
                let mut request = Vec::new();
                let mut buf = [0; 4096];
                loop {
                    let Ok(n) = socket.read(&mut buf).await else {
                        return;
                    };
                    if n == 0 {
                        return;
                    }
                    request.extend_from_slice(&buf[..n]);
                    let Some(end) = request.windows(4).position(|w| w == b"\r\n\r\n") else {
                        continue;
                    };
                    let headers = String::from_utf8_lossy(&request[..end]);
                    let length = headers
                        .lines()
                        .find_map(|line| {
                            let (name, value) = line.split_once(':')?;
                            name.eq_ignore_ascii_case("content-length")
                                .then(|| value.trim().parse::<usize>().unwrap())
                        })
                        .unwrap_or(0);
                    if request.len() < end + 4 + length {
                        continue;
                    }
                    let first = headers.lines().next().unwrap();
                    let body = if first.contains("/api/apikey/login") {
                        serde_json::json!({"org_info":[{"id":"test-org-id","name":"test-org","api_url":url}]}).to_string()
                    } else if first.contains("/v1/project") {
                        r#"{"objects":[]}"#.into()
                    } else if first.contains("/oauth/token") {
                        r#"{"access_token":"test-token"}"#.into()
                    } else if first.contains("/mcp") {
                        r#"{"jsonrpc":"2.0","id":1,"result":{"protocolVersion":"2025-03-26"}}"#
                            .into()
                    } else {
                        "{}".into()
                    };
                    let response = format!("HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}", body.len(), body);
                    let _ = socket.write_all(response.as_bytes()).await;
                    return;
                }
            });
        }
    });
    let dir = tempfile::tempdir().unwrap();
    let ca = dir.path().join("ca.pem");
    std::fs::write(&ca, cert.cert.pem()).unwrap();
    let bad_ca = dir.path().join("wrong.pem");
    let wrong = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    std::fs::write(&bad_ca, wrong.cert.pem()).unwrap();

    for mode in ["flag", "braintrust_env", "ssl_env", "raw_pem", "combined"] {
        let mut command = Command::new(env!("CARGO_BIN_EXE_bt"));
        command.args([
            "projects",
            "list",
            "--json",
            "--app-url",
            &url,
            "--api-url",
            &url,
        ]);
        command
            .env("HOME", dir.path())
            .env("XDG_CONFIG_HOME", dir.path())
            .env("USERPROFILE", dir.path())
            .env("APPDATA", dir.path())
            .env("LOCALAPPDATA", dir.path())
            .env("NO_PROXY", "localhost,127.0.0.1")
            .env("no_proxy", "localhost,127.0.0.1")
            .env("BRAINTRUST_API_KEY", "test-key")
            .env("BRAINTRUST_ORG_NAME", "test-org")
            .env_remove("BRAINTRUST_PROFILE")
            .env_remove("BRAINTRUST_CUSTOM_CA_BUNDLE")
            .env_remove("BRAINTRUST_CA_CERT")
            .env_remove("SSL_CERT_DIR")
            .env_remove("SSL_CERT_FILE");
        match mode {
            "flag" => {
                command
                    .env("SSL_CERT_FILE", &bad_ca)
                    .arg("--ca-cert")
                    .arg(&ca);
            }
            "braintrust_env" => {
                command
                    .env("SSL_CERT_FILE", &bad_ca)
                    .env("BRAINTRUST_CA_CERT", &ca);
            }
            "raw_pem" => {
                command.env("BRAINTRUST_CUSTOM_CA_BUNDLE", cert.cert.pem());
            }
            "combined" => {
                command
                    .env("SSL_CERT_FILE", &bad_ca)
                    .env("BRAINTRUST_CUSTOM_CA_BUNDLE", cert.cert.pem());
            }
            _ => {
                command.env("SSL_CERT_FILE", &ca);
            }
        }
        let output = command.output().unwrap();
        assert!(
            output.status.success(),
            "{mode}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
        assert_eq!(String::from_utf8_lossy(&output.stdout).trim(), "[]");
    }

    let output = Command::new(env!("CARGO_BIN_EXE_bt"))
        .args([
            "projects",
            "list",
            "--json",
            "--app-url",
            &url,
            "--api-url",
            &url,
        ])
        .env("HOME", dir.path())
        .env("XDG_CONFIG_HOME", dir.path())
        .env("USERPROFILE", dir.path())
        .env("APPDATA", dir.path())
        .env("LOCALAPPDATA", dir.path())
        .env("NO_PROXY", "localhost,127.0.0.1")
        .env("no_proxy", "localhost,127.0.0.1")
        .env("BRAINTRUST_API_KEY", "test-key")
        .env("BRAINTRUST_ORG_NAME", "test-org")
        .env("SSL_CERT_FILE", &bad_ca)
        .env_remove("BRAINTRUST_PROFILE")
        .env_remove("BRAINTRUST_CA_CERT")
        .env_remove("BRAINTRUST_CUSTOM_CA_BUNDLE")
        .env_remove("SSL_CERT_DIR")
        .output()
        .unwrap();
    assert!(
        !output.status.success(),
        "an unrelated CA must not be trusted"
    );

    server.abort();
}
