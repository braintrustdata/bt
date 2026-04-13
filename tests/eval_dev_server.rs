use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Parsed SSE event from the streaming response.
#[derive(Debug)]
struct SseEvent {
    event: String,
    data: String,
}

fn find_header_terminator(bytes: &[u8]) -> Option<usize> {
    bytes
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .map(|pos| pos + 4)
}

fn parse_content_length(headers: &str) -> usize {
    headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if !name.eq_ignore_ascii_case("content-length") {
                return None;
            }
            value.trim().parse::<usize>().ok()
        })
        .unwrap_or(0)
}

fn read_http_request(stream: &mut TcpStream) -> Option<String> {
    let mut request = Vec::new();
    let mut buf = [0u8; 8192];
    let mut expected_len: Option<usize> = None;

    loop {
        let n = stream.read(&mut buf).ok()?;
        if n == 0 {
            break;
        }
        request.extend_from_slice(&buf[..n]);

        if expected_len.is_none() {
            let header_end = find_header_terminator(&request)?;
            let headers = std::str::from_utf8(&request[..header_end]).ok()?;
            let content_length = parse_content_length(headers);
            expected_len = Some(header_end + content_length);
        }

        if request.len() >= expected_len.unwrap_or(0) {
            break;
        }
    }

    String::from_utf8(request).ok()
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
            let _ = read_http_request(&mut stream);
            let _ = stream.write_all(http_response.as_bytes());
            let _ = stream.flush();
        }
    });

    (port, handle)
}

/// Mock API server that handles auth, experiment registration, and log ingestion.
/// Counts the number of log rows received via the `/logs3` endpoint so that
/// tests can verify all SDK log events were flushed before a given point.
fn start_mock_api_server() -> (u16, Arc<AtomicUsize>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind mock api server");
    let port = listener.local_addr().unwrap().port();
    listener
        .set_nonblocking(false)
        .expect("set mock api listener blocking");

    let log_row_count = Arc::new(AtomicUsize::new(0));
    let count_clone = Arc::clone(&log_row_count);

    let handle = thread::spawn(move || {
        for stream in listener.incoming() {
            let mut stream = match stream {
                Ok(s) => s,
                Err(_) => break,
            };
            let _ = stream.set_read_timeout(Some(Duration::from_secs(5)));
            let request = match read_http_request(&mut stream) {
                Some(request) => request,
                None => continue,
            };

            // Extract first line to determine the method + path.
            let first_line = request.lines().next().unwrap_or("");
            let body = request
                .find("\r\n\r\n")
                .map(|pos| &request[pos + 4..])
                .unwrap_or("");

            let response_body = if first_line.contains("/logs3") {
                if let Ok(parsed) = serde_json::from_str::<serde_json::Value>(body) {
                    if let Some(arr) = parsed.get("rows").and_then(|r| r.as_array()) {
                        count_clone.fetch_add(arr.len(), Ordering::SeqCst);
                    }
                }
                r#"{}"#.to_string()
            } else if first_line.contains("/apikey") || first_line.contains("/login") {
                format!(
                    r#"{{"org_info": [{{"name": "test-org", "id": "org-test", "api_url": "http://127.0.0.1:{port}", "proxy_url": "http://127.0.0.1:{port}"}}]}}"#
                )
            } else if first_line.contains("/experiment/register") {
                r#"{"project": {"id": "proj-test", "name": "test"}, "experiment": {"id": "exp-test", "name": "test"}}"#.to_string()
            } else {
                r#"{}"#.to_string()
            };

            let http_response = format!(
                "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
                response_body.len(),
                response_body,
            );
            let _ = stream.write_all(http_response.as_bytes());
            let _ = stream.flush();
        }
    });

    (port, log_row_count, handle)
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

struct StreamingEvalResult {
    events: Vec<SseEvent>,
    marker_existed_at_done: bool,
    log_rows_at_done: usize,
}

fn streaming_eval_post(
    base_url: &str,
    body: &str,
    marker_path: &Path,
    log_row_count: &AtomicUsize,
) -> StreamingEvalResult {
    let mut curl = Command::new("curl")
        .args(["-s", "-N", "--max-time", "120", "-X", "POST"])
        .arg("-d")
        .arg(body)
        .arg("-H")
        .arg("Content-Type: application/json")
        .arg("-H")
        .arg("x-bt-auth-token: test-key")
        .arg("-H")
        .arg("x-bt-org-name: test-org")
        .arg(format!("{base_url}/eval"))
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn streaming curl");

    let stdout = curl.stdout.take().unwrap();
    let reader = BufReader::new(stdout);

    let mut events = Vec::new();
    let mut current_event = String::new();
    let mut current_data = Vec::<String>::new();
    let mut marker_existed_at_done = false;
    let mut log_rows_at_done = 0;

    for line_result in reader.lines() {
        let line = match line_result {
            Ok(l) => l,
            Err(_) => break,
        };
        if line.starts_with("event: ") {
            current_event = line["event: ".len()..].to_string();
        } else if line.starts_with("data: ") {
            current_data.push(line["data: ".len()..].to_string());
        } else if line.is_empty() && !current_event.is_empty() {
            let event = SseEvent {
                event: std::mem::take(&mut current_event),
                data: current_data.join("\n"),
            };
            if event.event == "done" {
                marker_existed_at_done = marker_path.exists();
                log_rows_at_done = log_row_count.load(Ordering::SeqCst);
            }
            events.push(event);
            current_data.clear();
        }
    }

    let _ = curl.wait();

    StreamingEvalResult {
        events,
        marker_existed_at_done,
        log_rows_at_done,
    }
}

/// Verify that the `done` SSE event is only delivered to the client after the
/// eval-runner process has fully exited (including atexit handlers) and all SDK
/// log events have been flushed to the API.
///
/// The eval fixture registers an atexit handler that sleeps 1 second then writes
/// a marker file. A streaming HTTP client checks for the marker at the instant
/// it receives the `done` event. Without the fix (deferring `done` until after
/// process exit), the marker will not yet exist.
#[test]
fn eval_dev_server_done_deferred_until_process_exit() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixtures_root = root.join("tests").join("evals");
    let fixture_dir = fixtures_root.join("py").join("atexit_flush");

    if !fixture_dir.join("fixture.json").exists() {
        eprintln!("Skipping (atexit_flush fixture missing).");
        return;
    }

    let python = match ensure_python_env(&fixtures_root.join("py")) {
        Some(python) => python,
        None => {
            eprintln!("Skipping (python/uv not available).");
            return;
        }
    };

    let bt_path = bt_binary_path(&root);
    let (mock_api_port, log_row_count, _mock_handle) = start_mock_api_server();

    let marker_dir = tempfile::tempdir().expect("create temp dir");
    let marker_path = marker_dir.path().join("atexit_marker");

    let dev_port = free_port();
    let mut child = Command::new(&bt_path)
        .args([
            "eval",
            "--dev",
            "--dev-port",
            &dev_port.to_string(),
            "eval_atexit.py",
        ])
        .current_dir(&fixture_dir)
        .env(
            "BRAINTRUST_APP_URL",
            format!("http://127.0.0.1:{mock_api_port}"),
        )
        .env("BRAINTRUST_API_KEY", "test-key")
        .env("BT_EVAL_PYTHON_RUNNER", &python)
        .env("ATEXIT_MARKER_FILE", marker_path.to_string_lossy().as_ref())
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

    wait_for_output(
        &mut child,
        &output,
        "Starting eval dev server",
        Duration::from_secs(60),
    );
    thread::sleep(Duration::from_millis(500));

    let base_url = format!("http://127.0.0.1:{dev_port}");

    let eval_body = serde_json::json!({
        "name": "cli-atexit-flush",
        "data": {
            "data": [
                {"input": "hello", "expected": "hello"},
                {"input": "world", "expected": "world"}
            ]
        },
        "stream": true
    })
    .to_string();

    let result = streaming_eval_post(&base_url, &eval_body, &marker_path, &log_row_count);

    let event_names: Vec<&str> = result.events.iter().map(|e| e.event.as_str()).collect();

    // The done event must only arrive after the process has fully exited,
    // which means the atexit handler (with its 1s sleep) has completed.
    assert!(
        result.marker_existed_at_done,
        "atexit marker should exist when done event arrives, proving done \
         is deferred until after process exit. Events: {event_names:?}"
    );

    // All SDK log rows must have been flushed to the mock API before done.
    let final_log_rows = log_row_count.load(Ordering::SeqCst);
    assert!(
        final_log_rows > 0,
        "mock API should have received log rows, got 0"
    );
    assert_eq!(
        result.log_rows_at_done, final_log_rows,
        "all log rows should be flushed before done event: \
         {} at done vs {} final",
        result.log_rows_at_done, final_log_rows,
    );

    // Verify all expected SSE events are present.
    assert!(
        result.events.iter().any(|e| e.event == "summary"),
        "expected at least one summary event, got: {event_names:?}"
    );
    assert_eq!(
        result.events.last().map(|e| e.event.as_str()),
        Some("done"),
        "done should be the last event, got: {event_names:?}"
    );
    assert_eq!(
        result.events.iter().filter(|e| e.event == "done").count(),
        1,
        "expected exactly one done event, got: {event_names:?}"
    );

    let _ = child.kill();
    let _ = child.wait();
    for handle in threads {
        let _ = handle.join();
    }
}

/// Verify that when a `parent` is included in the eval request, the eval
/// completes successfully and no error events are emitted. With --no-send-logs
/// we avoid needing a mock API server; the important thing is that the parent
/// reaches the SDK's parent_context() without crashing.
#[test]
fn eval_dev_server_parent_accepted() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixtures_root = root.join("tests").join("evals");
    let fixture_dir = fixtures_root.join("py").join("streaming");

    if !fixture_dir.join("fixture.json").exists() {
        eprintln!("Skipping eval_dev_server_parent_accepted (fixture missing).");
        return;
    }

    let python = match ensure_python_env(&fixtures_root.join("py")) {
        Some(python) => python,
        None => {
            eprintln!("Skipping eval_dev_server_parent_accepted (python/uv not available).");
            return;
        }
    };

    let bt_path = bt_binary_path(&root);
    let (mock_auth_port, _mock_handle) = start_mock_auth_server();

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

    wait_for_output(
        &mut child,
        &output,
        "Starting eval dev server",
        Duration::from_secs(60),
    );
    thread::sleep(Duration::from_millis(500));

    let base_url = format!("http://127.0.0.1:{dev_port}");

    // SpanComponentsV3: object_type=PLAYGROUND_LOGS, object_id=00000000-...-000042
    let parent_str = "AwMBAQAAAAAAAAAAAAAAAAAAAEI=";

    let eval_body = serde_json::json!({
        "name": "cli-streaming",
        "data": {
            "data": [
                {"input": "hello", "expected": "hello"},
                {"input": "world", "expected": "world"}
            ]
        },
        "parent": parent_str,
        "stream": true
    })
    .to_string();

    let post_headers = vec![
        ("x-bt-auth-token", "test-key"),
        ("x-bt-org-name", "test-org"),
        ("Content-Type", "application/json"),
    ];
    let sse_response = curl_post(&format!("{base_url}/eval"), &post_headers, &eval_body);
    let events = parse_sse_events(&sse_response);
    let event_names: Vec<&str> = events.iter().map(|e| e.event.as_str()).collect();

    assert!(
        !events.iter().any(|e| e.event == "error"),
        "expected no error events when parent is provided, got: {event_names:?}"
    );
    assert!(
        events.iter().any(|e| e.event == "summary"),
        "expected a summary event, got: {event_names:?}"
    );
    assert_eq!(
        events.last().map(|e| e.event.as_str()),
        Some("done"),
        "expected done as last event, got: {event_names:?}"
    );

    let _ = child.kill();
    let _ = child.wait();
    for handle in threads {
        let _ = handle.join();
    }
}

/// Verify that when `parent` is provided as a dict (the format the playground
/// sends), the eval runner correctly parses it via `parse_parent` and completes
/// successfully. Previously, non-string parents were silently discarded.
#[test]
fn eval_dev_server_parent_dict_accepted() {
    let root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let fixtures_root = root.join("tests").join("evals");
    let fixture_dir = fixtures_root.join("py").join("streaming");

    if !fixture_dir.join("fixture.json").exists() {
        eprintln!("Skipping eval_dev_server_parent_dict_accepted (fixture missing).");
        return;
    }

    let python = match ensure_python_env(&fixtures_root.join("py")) {
        Some(python) => python,
        None => {
            eprintln!("Skipping eval_dev_server_parent_dict_accepted (python/uv not available).");
            return;
        }
    };

    let bt_path = bt_binary_path(&root);
    let (mock_auth_port, _mock_handle) = start_mock_auth_server();

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

    wait_for_output(
        &mut child,
        &output,
        "Starting eval dev server",
        Duration::from_secs(60),
    );
    thread::sleep(Duration::from_millis(500));

    let base_url = format!("http://127.0.0.1:{dev_port}");

    // Send parent as a dict — this is the format the playground actually uses.
    let eval_body = serde_json::json!({
        "name": "cli-streaming",
        "data": {
            "data": [
                {"input": "hello", "expected": "hello"},
                {"input": "world", "expected": "world"}
            ]
        },
        "parent": {
            "object_type": "playground_logs",
            "object_id": "00000000-0000-0000-0000-000000000042",
            "row_ids": {
                "id": "00000000-0000-0000-0000-000000000001",
                "span_id": "00000000-0000-0000-0000-000000000002",
                "root_span_id": "00000000-0000-0000-0000-000000000003"
            }
        },
        "stream": true
    })
    .to_string();

    let post_headers = vec![
        ("x-bt-auth-token", "test-key"),
        ("x-bt-org-name", "test-org"),
        ("Content-Type", "application/json"),
    ];
    let sse_response = curl_post(&format!("{base_url}/eval"), &post_headers, &eval_body);
    let events = parse_sse_events(&sse_response);
    let event_names: Vec<&str> = events.iter().map(|e| e.event.as_str()).collect();

    assert!(
        !events.iter().any(|e| e.event == "error"),
        "expected no error events when dict parent is provided, got: {event_names:?}"
    );
    assert!(
        events.iter().any(|e| e.event == "summary"),
        "expected a summary event, got: {event_names:?}"
    );
    assert_eq!(
        events.last().map(|e| e.event.as_str()),
        Some("done"),
        "expected done as last event, got: {event_names:?}"
    );

    let _ = child.kill();
    let _ = child.wait();
    for handle in threads {
        let _ = handle.join();
    }
}
