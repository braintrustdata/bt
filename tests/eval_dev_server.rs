use std::io::{BufRead, BufReader, Read, Write};
use std::net::TcpListener;
use std::path::PathBuf;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Parsed SSE event from the streaming response.
#[derive(Debug)]
struct SseEvent {
    event: String,
    data: String,
}

/// Start a minimal HTTP server that always responds with a valid Braintrust
/// login payload so that the dev server's `authenticate_dev_request` succeeds.
fn start_mock_auth_server() -> (u16, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock auth server");
    let port = listener.local_addr().unwrap().port();
    listener
        .set_nonblocking(false)
        .expect("set mock listener blocking");

    let handle = thread::spawn(move || {
        let response_body = r#"{"org_info": [{"name": "test-org"}]}"#;
        let http_response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            response_body.len(),
            response_body,
        );
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => break,
            };
            let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
            let mut buf = [0u8; 4096];
            let _ = stream.read(&mut buf);
            let _ = stream.write_all(http_response.as_bytes());
            let _ = stream.flush();
        }
    });

    (port, handle)
}

/// Find an available TCP port.
fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind for free port");
    listener.local_addr().unwrap().port()
}

/// Parse SSE events from a text body (after stripping HTTP chunked encoding).
fn parse_sse_events(body: &str) -> Vec<SseEvent> {
    let mut events = Vec::new();
    let mut current_event = String::new();
    let mut current_data = Vec::<String>::new();

    for line in body.lines() {
        if line.starts_with("event: ") {
            current_event = line["event: ".len()..].to_string();
        } else if line.starts_with("data: ") {
            current_data.push(line["data: ".len()..].to_string());
        } else if line.is_empty() && !current_event.is_empty() {
            events.push(SseEvent {
                event: std::mem::take(&mut current_event),
                data: current_data.join("\n"),
            });
            current_data.clear();
        }
    }

    // Trailing event without a blank line terminator.
    if !current_event.is_empty() {
        events.push(SseEvent {
            event: current_event,
            data: current_data.join("\n"),
        });
    }

    events
}

fn spawn_output_collector<R: Read + Send + 'static>(
    reader: R,
    output: Arc<Mutex<String>>,
) -> thread::JoinHandle<()> {
    thread::spawn(move || {
        let mut buffered = BufReader::new(reader);
        let mut line = String::new();
        loop {
            line.clear();
            match buffered.read_line(&mut line) {
                Ok(0) => break,
                Ok(_) => {
                    let mut guard = output.lock().expect("output lock");
                    guard.push_str(&line);
                }
                Err(_) => break,
            }
        }
    })
}

fn wait_for_output(
    child: &mut Child,
    output: &Arc<Mutex<String>>,
    needle: &str,
    timeout: Duration,
) {
    let started = Instant::now();
    loop {
        if output.lock().expect("lock").contains(needle) {
            return;
        }
        if let Some(status) = child.try_wait().expect("try_wait") {
            let captured = output.lock().expect("lock").clone();
            panic!(
                "process exited early with status {status} while waiting for '{needle}'.\n{captured}"
            );
        }
        if started.elapsed() > timeout {
            let captured = output.lock().expect("lock").clone();
            panic!("timed out waiting for '{needle}'.\n{captured}");
        }
        thread::sleep(Duration::from_millis(100));
    }
}

/// Use curl to make an HTTP request and return the response body.
/// This handles chunked transfer encoding and other HTTP details.
fn curl_get(url: &str, headers: &[(&str, &str)]) -> String {
    let mut cmd = Command::new("curl");
    cmd.args(["-s", "--max-time", "60", url]);
    for (key, value) in headers {
        cmd.arg("-H").arg(format!("{key}: {value}"));
    }
    let output = cmd.output().expect("run curl");
    assert!(
        output.status.success(),
        "curl GET {url} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn curl_post(url: &str, headers: &[(&str, &str)], body: &str) -> String {
    let mut cmd = Command::new("curl");
    cmd.args(["-s", "--max-time", "60", "-X", "POST", "-d", body, url]);
    for (key, value) in headers {
        cmd.arg("-H").arg(format!("{key}: {value}"));
    }
    let output = cmd.output().expect("run curl");
    assert!(
        output.status.success(),
        "curl POST {url} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8_lossy(&output.stdout).to_string()
}

fn ensure_python_env(fixtures_py_root: &std::path::Path) -> Option<PathBuf> {
    if !command_exists("uv") {
        return None;
    }
    let venv_dir = fixtures_py_root.join(".venv");
    let python = venv_python_path(&venv_dir);
    if !python.is_file() {
        let status = Command::new("uv")
            .args(["venv", venv_dir.to_string_lossy().as_ref()])
            .status()
            .ok()?;
        if !status.success() {
            return None;
        }
    }
    if !python_can_import("braintrust", python.to_string_lossy().as_ref()) {
        let status = Command::new("uv")
            .args([
                "pip",
                "install",
                "--python",
                python.to_string_lossy().as_ref(),
                "braintrust",
            ])
            .status()
            .ok()?;
        if !status.success() {
            return None;
        }
    }
    Some(python)
}

fn venv_python_path(venv: &std::path::Path) -> PathBuf {
    if cfg!(windows) {
        venv.join("Scripts").join("python.exe")
    } else {
        venv.join("bin").join("python")
    }
}

fn python_can_import(module: &str, python: &str) -> bool {
    Command::new(python)
        .args(["-c", &format!("import {module}")])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn command_exists(command: &str) -> bool {
    std::env::var_os("PATH")
        .map(|paths| std::env::split_paths(&paths).any(|dir| dir.join(command).is_file()))
        .unwrap_or(false)
}

fn bt_binary_path(root: &std::path::Path) -> PathBuf {
    match std::env::var("CARGO_BIN_EXE_bt") {
        Ok(path) => PathBuf::from(path),
        Err(_) => {
            let candidate = root.join("target").join("debug").join("bt");
            if !candidate.is_file() {
                let status = Command::new("cargo")
                    .args(["build", "--bin", "bt"])
                    .current_dir(root)
                    .status()
                    .expect("cargo build");
                assert!(status.success(), "cargo build --bin bt failed");
            }
            candidate
        }
    }
}

#[test]
fn eval_dev_server_streams_python_events() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixtures_root = root.join("tests").join("evals");
    let fixture_dir = fixtures_root.join("py").join("streaming");

    if !fixture_dir.join("fixture.json").exists() {
        eprintln!("Skipping eval_dev_server_streams_python_events (streaming fixture missing).");
        return;
    }

    let python = match ensure_python_env(&fixtures_root.join("py")) {
        Some(python) => python,
        None => {
            eprintln!("Skipping eval_dev_server_streams_python_events (python/uv not available).");
            return;
        }
    };

    let bt_path = bt_binary_path(&root);

    // 1. Start mock auth server.
    let (mock_auth_port, _mock_handle) = start_mock_auth_server();

    // 2. Start bt eval --dev on the streaming fixture.
    let dev_port = free_port();
    let mut child = Command::new(&bt_path)
        .args([
            "eval",
            "--dev",
            "--dev-port",
            &dev_port.to_string(),
            "--no-send-logs",
            "eval_streaming.py",
        ])
        .current_dir(&fixture_dir)
        .env(
            "BRAINTRUST_APP_URL",
            format!("http://127.0.0.1:{mock_auth_port}"),
        )
        .env("BRAINTRUST_API_KEY", "test-key")
        .env("BT_EVAL_PYTHON_RUNNER", &python)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn bt eval --dev");

    let output = Arc::new(Mutex::new(String::new()));
    let mut threads = Vec::new();
    if let Some(stdout) = child.stdout.take() {
        threads.push(spawn_output_collector(stdout, Arc::clone(&output)));
    }
    if let Some(stderr) = child.stderr.take() {
        threads.push(spawn_output_collector(stderr, Arc::clone(&output)));
    }

    // Wait for the dev server to be ready.
    wait_for_output(
        &mut child,
        &output,
        "Starting eval dev server",
        Duration::from_secs(60),
    );
    // Give the server a moment to bind the port.
    thread::sleep(Duration::from_millis(500));

    let base_url = format!("http://127.0.0.1:{dev_port}");
    let auth_headers: Vec<(&str, &str)> = vec![
        ("x-bt-auth-token", "test-key"),
        ("x-bt-org-name", "test-org"),
    ];

    // 3. Health check.
    let health = curl_get(&base_url, &[]);
    assert!(
        health.contains("Hello"),
        "health check should return greeting, got: {health}"
    );

    // 4. List evaluators.
    let list_body = curl_get(&format!("{base_url}/list"), &auth_headers);
    let list_json: serde_json::Value =
        serde_json::from_str(&list_body).expect("parse /list JSON response");
    assert!(
        list_json.get("cli-streaming").is_some(),
        "/list should include 'cli-streaming' evaluator, got: {list_body}"
    );

    // 5. Streaming eval.
    let eval_body = serde_json::json!({
        "name": "cli-streaming",
        "data": {
            "data": [
                {"input": "hello", "expected": "hello"},
                {"input": "world", "expected": "world"}
            ]
        },
        "stream": true
    })
    .to_string();

    let mut post_headers = auth_headers.clone();
    post_headers.push(("Content-Type", "application/json"));
    let sse_response = curl_post(&format!("{base_url}/eval"), &post_headers, &eval_body);

    let events = parse_sse_events(&sse_response);

    // Collect event names.
    let event_names: Vec<&str> = events.iter().map(|e| e.event.as_str()).collect();

    // Should have one progress event per data row.
    let progress_events: Vec<&SseEvent> = events.iter().filter(|e| e.event == "progress").collect();
    assert_eq!(
        progress_events.len(),
        2,
        "expected one progress event per data row (2), got {}: {event_names:?}\nfull response:\n{sse_response}",
        progress_events.len()
    );

    // Each progress event should carry a json_delta payload.
    for pe in &progress_events {
        let parsed: serde_json::Value =
            serde_json::from_str(&pe.data).expect("parse progress event data");
        assert_eq!(
            parsed.get("event").and_then(|v| v.as_str()),
            Some("json_delta"),
            "progress event should have event=json_delta, got: {}",
            pe.data
        );
    }

    // Should have exactly one summary event.
    let summary_events: Vec<&SseEvent> = events.iter().filter(|e| e.event == "summary").collect();
    assert_eq!(
        summary_events.len(),
        1,
        "expected exactly one summary event, got events: {event_names:?}"
    );

    // Summary should contain scores.
    let summary: serde_json::Value =
        serde_json::from_str(&summary_events[0].data).expect("parse summary data");
    assert!(
        summary.get("scores").is_some(),
        "summary should contain scores, got: {}",
        summary_events[0].data
    );

    // Should have exactly one done event, and it should be last.
    let done_events: Vec<&SseEvent> = events.iter().filter(|e| e.event == "done").collect();
    assert_eq!(
        done_events.len(),
        1,
        "expected exactly one done event, got events: {event_names:?}"
    );
    assert_eq!(
        events.last().unwrap().event,
        "done",
        "done should be the last event, got events: {event_names:?}"
    );

    // Summary should come before done.
    let summary_idx = events.iter().position(|e| e.event == "summary").unwrap();
    let done_idx = events.iter().position(|e| e.event == "done").unwrap();
    assert!(
        summary_idx < done_idx,
        "summary should come before done, got events: {event_names:?}"
    );

    // 6. Clean up.
    let _ = child.kill();
    let _ = child.wait();
    for handle in threads {
        let _ = handle.join();
    }
}
