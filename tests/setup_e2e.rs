#![cfg(unix)]

use assert_cmd::cargo::cargo_bin;
use portable_pty::{native_pty_system, CommandBuilder, PtySize};
use reqwest::Url;
use std::fs;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

const TIMEOUT: Duration = Duration::from_secs(20);
const WORKFLOWS: &[&str] = &["instrument", "observe", "annotate", "evaluate", "deploy"];
const BRAINTRUST_CLI_ENVS: &[&str] = &[
    "BRAINTRUST_VERBOSE",
    "BRAINTRUST_QUIET",
    "BRAINTRUST_NO_COLOR",
    "BRAINTRUST_NO_INPUT",
    "BRAINTRUST_PROFILE",
    "BRAINTRUST_ORG_NAME",
    "BRAINTRUST_DEFAULT_PROJECT",
    "BRAINTRUST_API_KEY",
    "BRAINTRUST_API_URL",
    "BRAINTRUST_APP_URL",
    "BRAINTRUST_ENV_FILE",
];

#[test]
fn bare_setup_supports_interactive_oauth_org_project_and_agent_selection() {
    let repo = make_git_repo();
    let home = tempfile::tempdir().expect("home tempdir");
    let config_home = tempfile::tempdir().expect("config tempdir");
    let bin_dir = tempfile::tempdir().expect("bin tempdir");
    let browser_log = home.path().join("opened-url.txt");
    let codex_log = repo.path().join("codex.log");
    let path_env = format!(
        "{}:{}",
        bin_dir.path().display(),
        std::env::var("PATH").unwrap_or_default()
    );

    write_executable(
        &bin_dir.path().join("codex"),
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\nexit 0\n",
            codex_log.display()
        ),
    );
    write_executable(&bin_dir.path().join("claude"), "#!/bin/sh\nexit 0\n");
    write_executable(
        &bin_dir.path().join("fake-browser"),
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$1\" > '{}'\nexit 0\n",
            browser_log.display()
        ),
    );
    write_executable(
        &bin_dir.path().join("xdg-open"),
        &format!(
            "#!/bin/sh\nprintf '%s\\n' \"$1\" > '{}'\nexit 0\n",
            browser_log.display()
        ),
    );
    write_executable(&bin_dir.path().join("secret-tool"), "#!/bin/sh\nexit 1\n");

    seed_docs_cache(&config_home.path().join("bt").join("skills").join("docs"));
    seed_docs_cache(&repo.path().join(".bt").join("skills").join("docs"));

    let server = FakeBtServer::start();
    let bt_bin = cargo_bin("bt");

    let mut pty = PtyProcess::spawn(
        bt_bin.as_path(),
        repo.path(),
        vec![
            ("HOME".to_string(), home.path().display().to_string()),
            (
                "XDG_CONFIG_HOME".to_string(),
                config_home.path().display().to_string(),
            ),
            ("PATH".to_string(), path_env),
            (
                "BROWSER".to_string(),
                bin_dir.path().join("fake-browser").display().to_string(),
            ),
            ("SSH_CONNECTION".to_string(), "test".to_string()),
        ],
        &[
            "setup",
            "--api-url",
            &server.base_url,
            "--app-url",
            &server.base_url,
        ],
    );

    let oauth_stage =
        pty.wait_for_any(&["Callback URL/query/JSON", "Select organization"], TIMEOUT);
    if oauth_stage == "Callback URL/query/JSON" {
        let authorize_url = wait_for_authorize_url(&pty, &browser_log, TIMEOUT);
        let state = query_value(
            &Url::parse(authorize_url.trim()).expect("authorize url"),
            "state",
        );
        pty.send(&format!("?code=test-oauth-code&state={state}\r"));
        pty.wait_for("Select organization", TIMEOUT);
    }

    pty.send("\u{1b}[B");
    pty.send("\u{1b}[A");
    pty.send("\u{1b}[B");
    pty.send("\r");

    pty.wait_for("Select project", TIMEOUT);
    pty.send("targetx");
    pty.send("\u{7f}");
    pty.send("\r");

    pty.wait_for("Select coding agent", TIMEOUT);
    pty.send("\r");

    let status = pty.wait(TIMEOUT);
    assert!(
        status.success(),
        "bt setup exited with {status:?}\n{}",
        pty.snapshot()
    );

    let config_path = repo.path().join(".bt").join("config.json");
    let config_text = fs::read_to_string(&config_path).expect("read config");
    assert!(
        config_text.contains("\"org\": \"Target Org\""),
        "{config_text}"
    );
    assert!(
        config_text.contains("\"project\": \"Target Project\""),
        "{config_text}"
    );
    assert!(
        config_text.contains("\"project_id\": \"project-target\""),
        "{config_text}"
    );

    let codex_args = fs::read_to_string(&codex_log).expect("read codex log");
    assert!(!codex_args.trim().is_empty(), "codex was not invoked");

    let requests = server.requests();
    assert!(
        requests
            .iter()
            .any(|request| request == "POST /oauth/token"),
        "missing oauth token request: {requests:?}"
    );
    assert!(
        requests
            .iter()
            .filter(|request| request.starts_with("POST /api/apikey/login"))
            .count()
            >= 1,
        "missing api login request: {requests:?}"
    );
    assert!(
        requests
            .iter()
            .any(|request| request.contains("GET /v1/project?org_name=Target%20Org")),
        "missing target org project request: {requests:?}"
    );

    assert!(home
        .path()
        .join(".agents/skills/braintrust/SKILL.md")
        .exists());
    assert!(repo
        .path()
        .join(".agents/skills/braintrust/SKILL.md")
        .exists());
}

fn make_git_repo() -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    fs::write(dir.path().join(".git"), "gitdir: /tmp/fake").expect("write .git");
    dir
}

fn write_executable(path: &Path, content: &str) {
    fs::write(path, content).expect("write executable");
    let mut perms = fs::metadata(path).expect("metadata").permissions();
    perms.set_mode(0o755);
    fs::set_permissions(path, perms).expect("chmod");
}

fn seed_docs_cache(output_dir: &Path) {
    fs::create_dir_all(output_dir.join("reference")).expect("create docs dir");
    fs::write(output_dir.join("README.md"), "# Docs\n").expect("write docs readme");
    fs::write(output_dir.join("reference").join("sql.md"), "# SQL\n").expect("write sql doc");
    for workflow in WORKFLOWS {
        let workflow_dir = output_dir.join(workflow);
        fs::create_dir_all(&workflow_dir).expect("create workflow dir");
        fs::write(workflow_dir.join("_index.md"), format!("# {workflow}\n"))
            .expect("write workflow doc");
    }
}

struct PtyProcess {
    child: Box<dyn portable_pty::Child + Send>,
    writer: Box<dyn Write + Send>,
    output: Arc<Mutex<Vec<u8>>>,
    reader_thread: Option<thread::JoinHandle<()>>,
}

impl PtyProcess {
    fn spawn(program: &Path, cwd: &Path, envs: Vec<(String, String)>, args: &[&str]) -> Self {
        let pty_system = native_pty_system();
        let pair = pty_system
            .openpty(PtySize {
                rows: 40,
                cols: 120,
                pixel_width: 0,
                pixel_height: 0,
            })
            .expect("open pty");
        let mut cmd = CommandBuilder::new(program);
        for arg in args {
            cmd.arg(arg);
        }
        cmd.cwd(cwd);
        for (key, value) in envs {
            cmd.env(key, value);
        }
        for key in BRAINTRUST_CLI_ENVS {
            cmd.env_remove(key);
        }
        cmd.env("TERM", "xterm-256color");
        cmd.env("NO_COLOR", "1");
        let child = pair.slave.spawn_command(cmd).expect("spawn bt");
        drop(pair.slave);

        let output = Arc::new(Mutex::new(Vec::new()));
        let mut reader = pair.master.try_clone_reader().expect("clone reader");
        let reader_output = Arc::clone(&output);
        let reader_thread = thread::spawn(move || {
            let mut buf = [0u8; 4096];
            loop {
                match reader.read(&mut buf) {
                    Ok(0) => break,
                    Ok(n) => reader_output
                        .lock()
                        .expect("lock output")
                        .extend_from_slice(&buf[..n]),
                    Err(_) => break,
                }
            }
        });
        let writer = pair.master.take_writer().expect("take writer");

        Self {
            child,
            writer,
            output,
            reader_thread: Some(reader_thread),
        }
    }

    fn send(&mut self, input: &str) {
        self.writer
            .write_all(input.as_bytes())
            .expect("write to pty");
        self.writer.flush().expect("flush pty");
    }

    fn wait_for(&self, needle: &str, timeout: Duration) {
        let deadline = Instant::now() + timeout;
        loop {
            if self.snapshot().contains(needle) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for '{needle}'\n{}",
                self.snapshot()
            );
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn wait_for_any<'a>(&self, needles: &[&'a str], timeout: Duration) -> &'a str {
        let deadline = Instant::now() + timeout;
        loop {
            let snapshot = self.snapshot();
            if let Some(found) = needles
                .iter()
                .copied()
                .find(|needle| snapshot.contains(needle))
            {
                return found;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for any of {:?}\n{}",
                needles,
                snapshot
            );
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn wait(&mut self, timeout: Duration) -> portable_pty::ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            if let Some(status) = self.child.try_wait().expect("poll child") {
                if let Some(thread) = self.reader_thread.take() {
                    thread.join().expect("join reader");
                }
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "timed out waiting for process exit\n{}",
                self.snapshot()
            );
            thread::sleep(Duration::from_millis(50));
        }
    }

    fn snapshot(&self) -> String {
        let bytes = self.output.lock().expect("lock output").clone();
        String::from_utf8_lossy(&strip_ansi_escapes::strip(&bytes)).into_owned()
    }
}

fn wait_for_authorize_url(pty: &PtyProcess, browser_log: &Path, timeout: Duration) -> String {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(contents) = fs::read_to_string(browser_log) {
            if let Some(url) = extract_authorize_url(&contents) {
                return url;
            }
        }
        if let Some(url) = extract_authorize_url(&pty.snapshot()) {
            return url;
        }
        assert!(
            Instant::now() < deadline,
            "timed out waiting for authorize url\n{}",
            pty.snapshot()
        );
        thread::sleep(Duration::from_millis(50));
    }
}

fn extract_authorize_url(text: &str) -> Option<String> {
    for token in text.split_whitespace() {
        if token.starts_with("http://") && token.contains("/oauth/authorize") {
            return Some(token.trim().to_string());
        }
    }
    None
}

struct FakeBtServer {
    base_url: String,
    requests: Arc<Mutex<Vec<String>>>,
    stop: Arc<AtomicBool>,
    thread: Option<thread::JoinHandle<()>>,
}

impl FakeBtServer {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind fake server");
        listener
            .set_nonblocking(true)
            .expect("set listener nonblocking");
        let addr = listener.local_addr().expect("server addr");
        let requests = Arc::new(Mutex::new(Vec::new()));
        let stop = Arc::new(AtomicBool::new(false));
        let stop_flag = Arc::clone(&stop);
        let requests_log = Arc::clone(&requests);
        let base_url = format!("http://{}", addr);

        let thread = thread::spawn(move || {
            while !stop_flag.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => handle_fake_bt_request(stream, addr, &requests_log),
                    Err(err) if err.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(20));
                    }
                    Err(err) => panic!("fake server accept failed: {err}"),
                }
            }
        });

        Self {
            base_url,
            requests,
            stop,
            thread: Some(thread),
        }
    }

    fn requests(&self) -> Vec<String> {
        self.requests.lock().expect("lock requests").clone()
    }
}

impl Drop for FakeBtServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        let _ = TcpStream::connect(self.base_url.trim_start_matches("http://"));
        if let Some(thread) = self.thread.take() {
            thread.join().expect("join fake server");
        }
    }
}

fn handle_fake_bt_request(
    mut stream: TcpStream,
    addr: SocketAddr,
    requests: &Arc<Mutex<Vec<String>>>,
) {
    let request = read_http_request(&mut stream);
    requests
        .lock()
        .expect("lock requests")
        .push(format!("{} {}", request.method, request.target));
    let url = Url::parse(&format!("http://{}{}", addr, request.target)).expect("parse url");
    let path = url.path();

    match (request.method.as_str(), path) {
        ("GET", "/oauth/authorize") => {
            let redirect_uri = query_value(&url, "redirect_uri");
            let state = query_value(&url, "state");
            let location = format!("{redirect_uri}?code=test-oauth-code&state={state}");
            write_response(
                &mut stream,
                302,
                &[("Location", &location), ("Content-Length", "0")],
                b"",
            );
        }
        ("POST", "/oauth/token") => {
            let body = r#"{"access_token":"not-a-jwt","token_type":"Bearer","refresh_token":"refresh-token","expires_in":3600}"#;
            write_json(&mut stream, 200, body);
        }
        ("POST", "/api/apikey/login") => {
            let api_url = format!("http://{}", addr);
            let body = format!(
                r#"{{"org_info":[{{"id":"org-alpha","name":"Alpha Org","api_url":"{api_url}"}},{{"id":"org-target","name":"Target Org","api_url":"{api_url}"}}]}}"#
            );
            write_json(&mut stream, 200, &body);
        }
        ("GET", "/v1/project") => {
            let org_name = query_value(&url, "org_name");
            let body = match org_name.as_str() {
                "Alpha Org" => {
                    r#"{"objects":[{"id":"project-alpha","name":"Alpha Project","org_id":"org-alpha"}]}"#
                }
                "Target Org" => {
                    r#"{"objects":[{"id":"project-alpha","name":"Alpha Project","org_id":"org-target"},{"id":"project-target","name":"Target Project","org_id":"org-target"}]}"#
                }
                other => panic!("unexpected org_name {other}"),
            };
            write_json(&mut stream, 200, body);
        }
        ("GET", "/v1/api_key") => {
            write_json(&mut stream, 200, r#"{"objects":[]}"#);
        }
        ("POST", "/v1/api_key") => {
            write_json(&mut stream, 200, r#"{"key":"generated-api-key"}"#);
        }
        _ => panic!(
            "unexpected fake server request: {} {}",
            request.method, request.target
        ),
    }
}

struct HttpRequest {
    method: String,
    target: String,
}

fn read_http_request(stream: &mut TcpStream) -> HttpRequest {
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .expect("set read timeout");

    let mut buffer = Vec::new();
    let mut temp = [0u8; 1024];
    let header_end = loop {
        let read = stream.read(&mut temp).expect("read request");
        assert!(read > 0, "connection closed before headers");
        buffer.extend_from_slice(&temp[..read]);
        if let Some(pos) = find_header_end(&buffer) {
            break pos;
        }
    };

    let headers = String::from_utf8_lossy(&buffer[..header_end]);
    let mut lines = headers.lines();
    let request_line = lines.next().expect("request line");
    let mut parts = request_line.split_whitespace();
    let method = parts.next().expect("method").to_string();
    let target = parts.next().expect("target").to_string();

    let content_length = lines
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().expect("content-length"))
        })
        .unwrap_or(0);
    let current_body_len = buffer.len() - (header_end + 4);
    let mut remaining = content_length.saturating_sub(current_body_len);
    while remaining > 0 {
        let read = stream.read(&mut temp).expect("read body");
        assert!(read > 0, "connection closed before body");
        buffer.extend_from_slice(&temp[..read]);
        remaining = remaining.saturating_sub(read);
    }

    HttpRequest { method, target }
}

fn find_header_end(buffer: &[u8]) -> Option<usize> {
    buffer.windows(4).position(|window| window == b"\r\n\r\n")
}

fn query_value(url: &Url, key: &str) -> String {
    url.query_pairs()
        .find_map(|(name, value)| (name == key).then(|| value.into_owned()))
        .unwrap_or_else(|| panic!("missing query parameter {key} in {url}"))
}

fn write_json(stream: &mut TcpStream, status: u16, body: &str) {
    write_response(
        stream,
        status,
        &[
            ("Content-Type", "application/json"),
            ("Content-Length", &body.len().to_string()),
        ],
        body.as_bytes(),
    );
}

fn write_response(stream: &mut TcpStream, status: u16, headers: &[(&str, &str)], body: &[u8]) {
    let reason = match status {
        200 => "OK",
        302 => "Found",
        _ => panic!("unexpected status {status}"),
    };
    let mut response = format!("HTTP/1.1 {status} {reason}\r\nConnection: close\r\n");
    for (name, value) in headers {
        response.push_str(name);
        response.push_str(": ");
        response.push_str(value);
        response.push_str("\r\n");
    }
    response.push_str("\r\n");

    stream
        .write_all(response.as_bytes())
        .expect("write response headers");
    stream.write_all(body).expect("write response body");
}
