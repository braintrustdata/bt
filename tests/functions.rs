use std::collections::BTreeMap;
use std::fs;
use std::io::Read;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::{Arc, Mutex};

use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use flate2::read::GzDecoder;
use serde::Deserialize;
use serde_json::Value;
use tempfile::tempdir;

#[derive(Debug, Deserialize)]
struct FixtureConfig {
    command: Vec<String>,
    #[serde(default)]
    env: BTreeMap<String, String>,
    #[serde(default = "default_expect_success")]
    expect_success: bool,
    #[serde(default)]
    stdout_contains: Vec<String>,
    #[serde(default)]
    stderr_contains: Vec<String>,
    #[serde(default)]
    stdout_not_contains: Vec<String>,
    #[serde(default)]
    stderr_not_contains: Vec<String>,
    #[serde(default)]
    live: bool,
    #[serde(default)]
    required_env: Vec<String>,
}

fn default_expect_success() -> bool {
    true
}

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

fn bt_binary_path() -> PathBuf {
    if let Ok(path) = std::env::var("CARGO_BIN_EXE_bt") {
        return PathBuf::from(path);
    }

    let root = repo_root();
    let candidate = root.join("target").join("debug").join("bt");
    if !candidate.is_file() {
        build_bt_binary(&root);
    }
    candidate
}

fn build_bt_binary(root: &Path) {
    let status = Command::new("cargo")
        .args(["build", "--bin", "bt"])
        .current_dir(root)
        .status()
        .expect("cargo build --bin bt");
    if !status.success() {
        panic!("cargo build --bin bt failed");
    }
}

fn find_python() -> Option<String> {
    for candidate in ["python3", "python"] {
        let Ok(status) = Command::new(candidate).arg("--version").status() else {
            continue;
        };
        if status.success() {
            return Some(candidate.to_string());
        }
    }
    None
}

fn command_exists(command: &str) -> bool {
    let Some(paths) = std::env::var_os("PATH") else {
        return false;
    };

    for dir in std::env::split_paths(&paths) {
        let candidate = dir.join(command);
        if candidate.is_file() {
            return true;
        }
        if cfg!(windows) {
            let exe = candidate.with_extension("exe");
            if exe.is_file() {
                return true;
            }
            let cmd = candidate.with_extension("cmd");
            if cmd.is_file() {
                return true;
            }
        }
    }

    false
}

fn run_git(cwd: &Path, args: &[&str]) {
    let output = Command::new("git")
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("run git");
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!(
            "git command failed in {}: git {}\n{}",
            cwd.display(),
            args.join(" "),
            stderr.trim()
        );
    }
}

fn find_tsc() -> Option<PathBuf> {
    let local = if cfg!(windows) {
        repo_root()
            .join("node_modules")
            .join(".bin")
            .join("tsc.cmd")
    } else {
        repo_root().join("node_modules").join(".bin").join("tsc")
    };
    if local.is_file() {
        return Some(local);
    }

    if command_exists("tsc") {
        return Some(PathBuf::from("tsc"));
    }

    None
}

fn decode_uploaded_bundle(bundle: &[u8]) -> String {
    if bundle.starts_with(&[0x1f, 0x8b]) {
        let mut decoder = GzDecoder::new(bundle);
        let mut out = String::new();
        decoder
            .read_to_string(&mut out)
            .expect("decompress uploaded bundle");
        out
    } else {
        String::from_utf8(bundle.to_vec()).expect("uploaded bundle utf8")
    }
}

fn read_fixture_config(path: &Path) -> FixtureConfig {
    let raw = fs::read_to_string(path).expect("read fixture.json");
    serde_json::from_str(&raw).expect("parse fixture.json")
}

fn env_flag(name: &str) -> bool {
    match std::env::var(name) {
        Ok(value) => matches!(
            value.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => false,
    }
}

fn sanitized_env_keys() -> &'static [&'static str] {
    &[
        "BT_FUNCTIONS_PUSH_FILES",
        "BT_FUNCTIONS_PUSH_IF_EXISTS",
        "BT_FUNCTIONS_PUSH_TERMINATE_ON_FAILURE",
        "BT_FUNCTIONS_PUSH_RUNNER",
        "BT_FUNCTIONS_PUSH_LANGUAGE",
        "BT_FUNCTIONS_PUSH_REQUIREMENTS",
        "BT_FUNCTIONS_PUSH_TSCONFIG",
        "BT_FUNCTIONS_PUSH_EXTERNAL_PACKAGES",
        "BT_FUNCTIONS_PULL_OUTPUT_DIR",
        "BT_FUNCTIONS_PULL_PROJECT_ID",
        "BT_FUNCTIONS_PULL_PROJECT_NAME",
        "BT_FUNCTIONS_PULL_ID",
        "BT_FUNCTIONS_PULL_SLUG",
        "BT_FUNCTIONS_PULL_VERSION",
        "BT_FUNCTIONS_PULL_FORCE",
        "BT_FUNCTIONS_PULL_LANGUAGE",
    ]
}

fn auth_profiles_command(cwd: &Path, config_dir: &Path) -> Command {
    let mut cmd = Command::new(bt_binary_path());
    cmd.arg("auth")
        .arg("profiles")
        .current_dir(cwd)
        .env("XDG_CONFIG_HOME", config_dir)
        .env("APPDATA", config_dir)
        .env("BRAINTRUST_NO_COLOR", "1")
        .env_remove("BRAINTRUST_PROFILE")
        .env_remove("BRAINTRUST_ORG_NAME")
        .env_remove("BRAINTRUST_API_URL")
        .env_remove("BRAINTRUST_APP_URL")
        .env_remove("BRAINTRUST_ENV_FILE");
    cmd
}

#[derive(Debug, Clone)]
struct MockProject {
    id: String,
    name: String,
    org_id: String,
}

#[derive(Default)]
struct MockServerState {
    requests: Mutex<Vec<String>>,
    projects: Mutex<Vec<MockProject>>,
    pull_rows: Mutex<Vec<Value>>,
    environment_objects: Mutex<BTreeMap<String, Vec<Value>>>,
    environment_upserts: Mutex<Vec<Value>>,
    uploaded_bundles: Mutex<Vec<Vec<u8>>>,
    inserted_functions: Mutex<Vec<Value>>,
    bundle_counter: Mutex<usize>,
}

struct MockServer {
    base_url: String,
    handle: actix_web::dev::ServerHandle,
}

impl MockServer {
    async fn start(state: Arc<MockServerState>) -> Self {
        let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind mock server");
        let addr = listener.local_addr().expect("mock server addr");
        let base_url = format!("http://{addr}");
        let data = web::Data::new(state.clone());

        let server = HttpServer::new(move || {
            App::new()
                .app_data(data.clone())
                .route("/api/apikey/login", web::post().to(mock_login))
                .route("/v1/project", web::get().to(mock_list_projects))
                .route("/v1/project", web::post().to(mock_create_project))
                .route("/function/code", web::post().to(mock_request_code_slot))
                .route("/upload/{bundle_id}", web::put().to(mock_upload_bundle))
                .route("/insert-functions", web::post().to(mock_insert_functions))
                .route("/v1/function", web::get().to(mock_list_functions))
                .route(
                    "/environment-object/{object_type}/{object_id}",
                    web::get().to(mock_list_environment_objects),
                )
                .route(
                    "/environment-object/{object_type}/{object_id}/{environment_slug}",
                    web::put().to(mock_upsert_environment_object),
                )
        })
        .workers(1)
        .listen(listener)
        .expect("listen mock server")
        .run();
        let handle = server.handle();
        tokio::spawn(server);

        Self { base_url, handle }
    }

    async fn stop(&self) {
        self.handle.stop(true).await;
    }
}

async fn mock_login(state: web::Data<Arc<MockServerState>>, req: HttpRequest) -> HttpResponse {
    log_request(&state, &req);
    let base = request_base_url(&req);
    HttpResponse::Ok().json(serde_json::json!({
        "org_info": [
            {
                "id": "org_mock",
                "name": "test-org",
                "api_url": base
            }
        ]
    }))
}

async fn mock_list_projects(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
) -> HttpResponse {
    log_request(&state, &req);
    let query = parse_query(req.query_string());
    let requested_name = query.get("project_name").cloned();
    let projects = state.projects.lock().expect("projects lock").clone();
    let objects = projects
        .into_iter()
        .filter(|project| {
            requested_name
                .as_deref()
                .is_none_or(|name| project.name == name)
        })
        .map(|project| {
            serde_json::json!({
                "id": project.id,
                "name": project.name,
                "org_id": project.org_id
            })
        })
        .collect::<Vec<_>>();
    HttpResponse::Ok().json(serde_json::json!({ "objects": objects }))
}

#[derive(Deserialize)]
struct CreateProjectRequest {
    name: String,
    org_name: String,
}

async fn mock_create_project(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
    body: web::Json<CreateProjectRequest>,
) -> HttpResponse {
    log_request(&state, &req);
    let mut projects = state.projects.lock().expect("projects lock");
    if let Some(existing) = projects.iter().find(|project| project.name == body.name) {
        return HttpResponse::Ok().json(serde_json::json!({
            "id": existing.id,
            "name": existing.name,
            "org_id": existing.org_id
        }));
    }

    let created = MockProject {
        id: format!("proj_created_{}", projects.len() + 1),
        name: body.name.clone(),
        org_id: body.org_name.clone(),
    };
    projects.push(created.clone());
    HttpResponse::Ok().json(serde_json::json!({
        "id": created.id,
        "name": created.name,
        "org_id": created.org_id
    }))
}

async fn mock_request_code_slot(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
) -> HttpResponse {
    log_request(&state, &req);
    let mut counter = state.bundle_counter.lock().expect("bundle counter lock");
    *counter += 1;
    let bundle_id = format!("bundle-{counter}");
    let base = request_base_url(&req);
    let upload_url = format!("{base}/upload/{bundle_id}");
    HttpResponse::Ok().json(serde_json::json!({
        "url": upload_url,
        "bundleId": bundle_id
    }))
}

async fn mock_upload_bundle(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
    body: web::Bytes,
) -> HttpResponse {
    log_request(&state, &req);
    state
        .uploaded_bundles
        .lock()
        .expect("uploaded bundles lock")
        .push(body.to_vec());
    HttpResponse::Ok().finish()
}

#[derive(Deserialize)]
struct InsertFunctionsRequest {
    functions: Vec<Value>,
}

async fn mock_insert_functions(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
    body: web::Json<InsertFunctionsRequest>,
) -> HttpResponse {
    log_request(&state, &req);
    let mut inserted = state
        .inserted_functions
        .lock()
        .expect("inserted functions lock");
    inserted.extend(body.functions.clone());
    let functions = body
        .functions
        .iter()
        .enumerate()
        .map(|(index, function)| {
            let slug = function
                .get("slug")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let project_id = function
                .get("project_id")
                .and_then(Value::as_str)
                .unwrap_or("proj_mock");
            serde_json::json!({
                "id": format!("fn_inserted_{index}"),
                "slug": slug,
                "project_id": project_id,
                "found_existing": false
            })
        })
        .collect::<Vec<_>>();

    HttpResponse::Ok().json(serde_json::json!({
        "ignored_count": 0,
        "xact_id": "0000000000000001",
        "functions": functions
    }))
}

async fn mock_list_functions(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
) -> HttpResponse {
    log_request(&state, &req);
    let query = parse_query(req.query_string());
    let id = query.get("ids").cloned();
    let slug = query.get("slug").cloned();
    let project_id = query.get("project_id").cloned();

    let rows = state.pull_rows.lock().expect("pull rows lock").clone();
    let filtered = rows
        .into_iter()
        .filter(|row| {
            id.as_deref()
                .is_none_or(|needle| row.get("id").and_then(Value::as_str) == Some(needle))
        })
        .filter(|row| {
            slug.as_deref()
                .is_none_or(|needle| row.get("slug").and_then(Value::as_str) == Some(needle))
        })
        .filter(|row| {
            project_id
                .as_deref()
                .is_none_or(|needle| row.get("project_id").and_then(Value::as_str) == Some(needle))
        })
        .collect::<Vec<_>>();

    HttpResponse::Ok().json(serde_json::json!({
        "objects": filtered
    }))
}

async fn mock_list_environment_objects(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
) -> HttpResponse {
    log_request(&state, &req);
    let object_type = req.match_info().get("object_type").unwrap_or_default();
    if object_type != "prompt" {
        return HttpResponse::Ok().json(serde_json::json!({ "objects": [] }));
    }
    let object_id = req.match_info().get("object_id").unwrap_or_default();
    let rows = state
        .environment_objects
        .lock()
        .expect("environment objects lock")
        .get(object_id)
        .cloned()
        .unwrap_or_default();

    HttpResponse::Ok().json(serde_json::json!({ "objects": rows }))
}

async fn mock_upsert_environment_object(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
    body: web::Json<Value>,
) -> HttpResponse {
    log_request(&state, &req);
    let object_type = req.match_info().get("object_type").unwrap_or_default();
    let object_id = req.match_info().get("object_id").unwrap_or_default();
    let environment_slug = req.match_info().get("environment_slug").unwrap_or_default();
    let object_version = body
        .get("object_version")
        .and_then(Value::as_str)
        .unwrap_or_default();
    let org_name = body
        .get("org_name")
        .and_then(Value::as_str)
        .unwrap_or_default();

    state
        .environment_upserts
        .lock()
        .expect("environment upserts lock")
        .push(serde_json::json!({
            "object_type": object_type,
            "object_id": object_id,
            "environment_slug": environment_slug,
            "object_version": object_version,
            "org_name": org_name
        }));

    if object_type == "prompt"
        && !object_id.trim().is_empty()
        && !environment_slug.trim().is_empty()
        && !object_version.trim().is_empty()
    {
        state
            .environment_objects
            .lock()
            .expect("environment objects lock")
            .entry(object_id.to_string())
            .or_default()
            .push(serde_json::json!({
                "environment_slug": environment_slug,
                "object_version": object_version
            }));
    }

    HttpResponse::Ok().json(serde_json::json!({}))
}

fn log_request(state: &Arc<MockServerState>, req: &HttpRequest) {
    let entry = if req.query_string().is_empty() {
        req.path().to_string()
    } else {
        format!("{}?{}", req.path(), req.query_string())
    };
    state.requests.lock().expect("requests lock").push(entry);
}

fn request_base_url(req: &HttpRequest) -> String {
    let info = req.connection_info();
    format!("{}://{}", info.scheme(), info.host())
}

fn parse_query(query: &str) -> BTreeMap<String, String> {
    let mut values = BTreeMap::new();
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let (raw_key, raw_value) = pair.split_once('=').unwrap_or((pair, ""));
        let key = urlencoding::decode(raw_key)
            .map(|value| value.into_owned())
            .unwrap_or_else(|_| raw_key.to_string());
        let value = urlencoding::decode(raw_value)
            .map(|value| value.into_owned())
            .unwrap_or_else(|_| raw_value.to_string());
        values.insert(key, value);
    }
    values
}

#[test]
fn functions_fixtures() {
    let root = repo_root();
    let fixtures_root = root.join("tests").join("functions-fixtures");
    if !fixtures_root.exists() {
        eprintln!("No functions fixtures found.");
        return;
    }

    let bt_path = bt_binary_path();
    let run_live = env_flag("BT_FUNCTIONS_FIXTURE_LIVE");

    let mut fixture_dirs: Vec<PathBuf> = fs::read_dir(&fixtures_root)
        .expect("read functions fixture root")
        .filter_map(|entry| entry.ok())
        .map(|entry| entry.path())
        .filter(|path| path.is_dir())
        .collect();
    fixture_dirs.sort();

    let mut ran_any = false;
    for dir in fixture_dirs {
        let config_path = dir.join("fixture.json");
        if !config_path.is_file() {
            continue;
        }
        ran_any = true;

        let fixture_name = dir
            .file_name()
            .map(|name| name.to_string_lossy().to_string())
            .expect("fixture directory name");
        let config = read_fixture_config(&config_path);
        if config.command.is_empty() {
            panic!("Fixture {fixture_name} has an empty command.");
        }

        if config.live && !run_live {
            eprintln!("Skipping {fixture_name} (live fixture; set BT_FUNCTIONS_FIXTURE_LIVE=1).");
            continue;
        }

        let missing_required: Vec<String> = config
            .required_env
            .iter()
            .filter(|key| std::env::var(key.as_str()).is_err())
            .cloned()
            .collect();
        if !missing_required.is_empty() {
            eprintln!(
                "Skipping {fixture_name} (missing required env: {}).",
                missing_required.join(", ")
            );
            continue;
        }

        let mut cmd = Command::new(&bt_path);
        cmd.args(&config.command).current_dir(&dir);
        for key in sanitized_env_keys() {
            cmd.env_remove(key);
        }
        for (key, value) in &config.env {
            cmd.env(key, value);
        }

        let output = cmd
            .output()
            .unwrap_or_else(|err| panic!("failed to run fixture {fixture_name}: {err}"));
        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if output.status.success() != config.expect_success {
            panic!(
                "Fixture {fixture_name} command {:?} had status {} (expected success={})\nstdout:\n{}\nstderr:\n{}",
                config.command,
                output.status,
                config.expect_success,
                stdout,
                stderr
            );
        }

        for expected in &config.stdout_contains {
            assert!(
                stdout.contains(expected),
                "Fixture {fixture_name}: stdout missing expected text: {expected}\nstdout:\n{stdout}"
            );
        }
        for expected in &config.stderr_contains {
            assert!(
                stderr.contains(expected),
                "Fixture {fixture_name}: stderr missing expected text: {expected}\nstderr:\n{stderr}"
            );
        }
        for unexpected in &config.stdout_not_contains {
            assert!(
                !stdout.contains(unexpected),
                "Fixture {fixture_name}: stdout unexpectedly contained text: {unexpected}\nstdout:\n{stdout}"
            );
        }
        for unexpected in &config.stderr_not_contains {
            assert!(
                !stderr.contains(unexpected),
                "Fixture {fixture_name}: stderr unexpectedly contained text: {unexpected}\nstderr:\n{stderr}"
            );
        }
    }

    if !ran_any {
        eprintln!("No functions fixtures with fixture.json found.");
    }
}

#[test]
fn functions_push_help_includes_expected_flags() {
    let output = Command::new(bt_binary_path())
        .arg("functions")
        .arg("push")
        .arg("--help")
        .output()
        .expect("run bt functions push --help");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--file"));
    assert!(stdout.contains("--if-exists"));
    assert!(stdout.contains("--terminate-on-failure"));
    assert!(stdout.contains("--create-missing-projects"));
    assert!(stdout.contains("--language"));
    assert!(stdout.contains("--requirements"));
    assert!(stdout.contains("--tsconfig"));
    assert!(stdout.contains("--external-packages"));
}

#[test]
fn functions_pull_help_includes_expected_flags() {
    let output = Command::new(bt_binary_path())
        .arg("functions")
        .arg("pull")
        .arg("--help")
        .output()
        .expect("run bt functions pull --help");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--output-dir"));
    assert!(stdout.contains("--project-id"));
    assert!(stdout.contains("--version"));
    assert!(stdout.contains("--language"));
}

#[test]
fn functions_pull_accepts_id_and_slug_together() {
    let output = Command::new(bt_binary_path())
        .arg("functions")
        .arg("pull")
        .arg("--id")
        .arg("abc")
        .arg("--slug")
        .arg("slug")
        .arg("--help")
        .output()
        .expect("run pull with id and slug");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("--id"));
    assert!(stdout.contains("--slug"));
}

#[test]
fn functions_push_rejects_type_flag() {
    let output = Command::new(bt_binary_path())
        .arg("functions")
        .arg("push")
        .arg("--type")
        .arg("tool")
        .output()
        .expect("run push with invalid --type");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--type"));
}

#[test]
fn functions_pull_rejects_invalid_language() {
    let output = Command::new(bt_binary_path())
        .arg("functions")
        .arg("pull")
        .arg("--language")
        .arg("ruby")
        .output()
        .expect("run pull with invalid language");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("ruby"));
}

#[test]
fn functions_push_rejects_invalid_language() {
    let output = Command::new(bt_binary_path())
        .arg("functions")
        .arg("push")
        .arg("--language")
        .arg("typescript")
        .output()
        .expect("run push with invalid language");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("typescript"));
}

#[test]
fn functions_push_requires_app_url_with_custom_api_url() {
    let output = Command::new(bt_binary_path())
        .arg("functions")
        .arg("--json")
        .arg("push")
        .env("BRAINTRUST_API_KEY", "test-key")
        .env("BRAINTRUST_API_URL", "http://127.0.0.1:1")
        .env_remove("BRAINTRUST_APP_URL")
        .env_remove("BRAINTRUST_ORG_NAME")
        .env_remove("BRAINTRUST_PROFILE")
        .output()
        .expect("run push with custom API URL and no app URL");

    assert!(!output.status.success());
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stderr.contains("--app-url or BRAINTRUST_APP_URL"));
    assert!(!stderr.contains("https://www.braintrust.dev/api/apikey/login"));
}

#[test]
fn functions_help_lists_push_and_pull() {
    let output = Command::new(bt_binary_path())
        .arg("functions")
        .arg("--help")
        .output()
        .expect("run bt functions --help");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(stdout.contains("push"));
    assert!(stdout.contains("pull"));
}

#[test]
fn auth_profiles_ignores_api_key_env_override() {
    let cwd = tempdir().expect("create temp cwd");
    let config_dir = tempdir().expect("create temp config dir");

    let output = auth_profiles_command(cwd.path(), config_dir.path())
        .env("BRAINTRUST_API_KEY", "test-key")
        .output()
        .expect("run bt auth profiles with api key env");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("No saved profiles. Run `bt auth login` to create one."));
    assert!(!stdout.contains("Auth source: --api-key/BRAINTRUST_API_KEY override"));
    assert!(!stderr.contains("pass --prefer-profile or unset BRAINTRUST_API_KEY"));
}

#[test]
fn auth_profiles_ignores_api_key_from_dotenv() {
    let cwd = tempdir().expect("create temp cwd");
    let config_dir = tempdir().expect("create temp config dir");
    fs::write(cwd.path().join(".env"), "BRAINTRUST_API_KEY=test-key\n").expect("write .env");

    let output = auth_profiles_command(cwd.path(), config_dir.path())
        .env_remove("BRAINTRUST_API_KEY")
        .output()
        .expect("run bt auth profiles with dotenv api key");

    assert!(output.status.success());
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("No saved profiles. Run `bt auth login` to create one."));
    assert!(!stdout.contains("Auth source: --api-key/BRAINTRUST_API_KEY override"));
    assert!(!stderr.contains("pass --prefer-profile or unset BRAINTRUST_API_KEY"));
}

#[test]
fn push_and_pull_help_are_machine_readable() {
    let push_help = Command::new(bt_binary_path())
        .arg("functions")
        .arg("push")
        .arg("--help")
        .output()
        .expect("run push help");
    assert!(push_help.status.success());

    let pull_help = Command::new(bt_binary_path())
        .arg("functions")
        .arg("pull")
        .arg("--help")
        .output()
        .expect("run pull help");
    assert!(pull_help.status.success());

    let push_stdout = String::from_utf8_lossy(&push_help.stdout);
    let pull_stdout = String::from_utf8_lossy(&pull_help.stdout);
    assert!(push_stdout.contains("BT_FUNCTIONS_PUSH_FILES"));
    assert!(push_stdout.contains("BT_FUNCTIONS_PUSH_LANGUAGE"));
    assert!(push_stdout.contains("BT_FUNCTIONS_PUSH_REQUIREMENTS"));
    assert!(push_stdout.contains("BT_FUNCTIONS_PUSH_TSCONFIG"));
    assert!(push_stdout.contains("BT_FUNCTIONS_PUSH_EXTERNAL_PACKAGES"));
    assert!(pull_stdout.contains("BT_FUNCTIONS_PULL_OUTPUT_DIR"));
    assert!(pull_stdout.contains("BT_FUNCTIONS_PULL_LANGUAGE"));
    assert!(pull_stdout.contains("BT_FUNCTIONS_PULL_VERSION"));
}

#[test]
fn functions_python_runner_scripts_compile_when_python_available() {
    let Some(python) = find_python() else {
        eprintln!(
            "Skipping functions_python_runner_scripts_compile_when_python_available (python not installed)."
        );
        return;
    };

    let root = repo_root();
    let output = Command::new(&python)
        .arg("-m")
        .arg("py_compile")
        .arg(root.join("scripts").join("functions-runner.py"))
        .arg(root.join("scripts").join("python_runner_common.py"))
        .output()
        .expect("run py_compile");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("Python runner scripts failed py_compile:\n{stderr}");
    }
}

#[test]
fn functions_python_runner_collects_function_type_from_type_() {
    let Some(python) = find_python() else {
        eprintln!(
            "Skipping functions_python_runner_collects_function_type_from_type_ (python not installed)."
        );
        return;
    };

    let root = repo_root();
    let scripts_dir = root.join("scripts");
    let runner_script = scripts_dir.join("functions-runner.py");
    let snippet = r#"
import importlib.util
import json
import pathlib
import sys

runner_path = pathlib.Path(sys.argv[1])
spec = importlib.util.spec_from_file_location("functions_runner", runner_path)
if spec is None or spec.loader is None:
    raise RuntimeError(f"failed to load {runner_path}")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class TypeEnum:
    value = "tool"

class Params:
    @staticmethod
    def model_json_schema():
        return {"type": "object", "properties": {}}

class Item:
    def __init__(self):
        self.name = "my-tool"
        self.slug = "my-tool"
        self.type_ = TypeEnum()
        self.parameters = Params
        self.preview = "def handler(x):\\n    return x"

entries = module.collect_code_entries([Item()])
print(json.dumps(entries))
"#;

    let output = Command::new(&python)
        .env("PYTHONPATH", &scripts_dir)
        .arg("-c")
        .arg(snippet)
        .arg(&runner_script)
        .output()
        .expect("run functions-runner collect_code_entries regression script");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("Python runner regression script failed:\n{stderr}");
    }

    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    let entries: Vec<Value> =
        serde_json::from_str(stdout.trim()).expect("parse entries JSON from regression script");
    let first = entries.first().expect("first entry");
    assert_eq!(
        first.get("function_type").and_then(Value::as_str),
        Some("tool")
    );
    assert_eq!(
        first.get("preview").and_then(Value::as_str),
        Some("def handler(x):\\n    return x")
    );
}

#[test]
fn functions_js_runner_emits_valid_manifest() {
    if !command_exists("node") {
        eprintln!("Skipping functions_js_runner_emits_valid_manifest (node not installed).");
        return;
    }
    let Some(tsc) = find_tsc() else {
        eprintln!("Skipping functions_js_runner_emits_valid_manifest (tsc not installed).");
        return;
    };

    let root = repo_root();
    let tmp = tempdir().expect("tempdir");
    let sample_path = tmp.path().join("sample.js");
    std::fs::write(
        &sample_path,
        r#"globalThis._evals ??= { functions: [], prompts: [], parameters: [], evaluators: {}, reporters: {} };
globalThis._evals.functions.push({
  name: "js-tool",
  slug: "js-tool",
  type: "tool",
  parameters: { type: "object", properties: {} },
  preview: "export function handler() { return 1; }"
});
"#,
    )
    .expect("write sample.js");

    let runner_dir = tmp.path().join("runner");
    let compile_output = Command::new(&tsc)
        .current_dir(&root)
        .args([
            "scripts/functions-runner.ts",
            "scripts/runner-common.ts",
            "--module",
            "esnext",
            "--target",
            "es2020",
            "--moduleResolution",
            "bundler",
            "--outDir",
        ])
        .arg(&runner_dir)
        .output()
        .expect("compile functions runner");
    if !compile_output.status.success() {
        let stdout = String::from_utf8_lossy(&compile_output.stdout);
        let stderr = String::from_utf8_lossy(&compile_output.stderr);
        panic!("tsc failed for functions runner:\nstdout:\n{stdout}\nstderr:\n{stderr}");
    }

    let runner_js = runner_dir.join("functions-runner.js");
    let runner_common_js = runner_dir.join("runner-common.js");
    assert!(runner_js.is_file(), "compiled functions-runner.js missing");
    assert!(
        runner_common_js.is_file(),
        "compiled runner-common.js missing"
    );

    let runner_code = std::fs::read_to_string(&runner_js).expect("read compiled runner");
    let patched_runner_code = runner_code
        .replace("\"./runner-common\"", "\"./runner-common.js\"")
        .replace("'./runner-common'", "'./runner-common.js'");
    assert_ne!(
        runner_code, patched_runner_code,
        "compiled runner import path did not contain ./runner-common"
    );
    std::fs::write(&runner_js, patched_runner_code).expect("write patched compiled runner");
    std::fs::write(runner_dir.join("package.json"), r#"{ "type": "module" }"#)
        .expect("write runner package.json");

    let output = Command::new("node")
        .arg(&runner_js)
        .arg(&sample_path)
        .output()
        .expect("run compiled functions runner");
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("compiled functions runner failed:\n{stderr}");
    }

    let manifest: Value = serde_json::from_slice(&output.stdout).expect("parse manifest JSON");
    assert_eq!(
        manifest["runtime_context"]["runtime"].as_str(),
        Some("node"),
        "runtime_context.runtime should be node"
    );
    assert!(
        manifest["runtime_context"]["version"]
            .as_str()
            .is_some_and(|value| !value.trim().is_empty()),
        "runtime_context.version should be present"
    );

    let files = manifest["files"].as_array().expect("files array");
    assert_eq!(files.len(), 1, "expected one manifest file");
    let file = files[0].as_object().expect("manifest file object");
    let reported_source = PathBuf::from(
        file.get("source_file")
            .and_then(Value::as_str)
            .expect("source_file"),
    );
    assert_eq!(
        reported_source
            .canonicalize()
            .expect("canonicalize source_file"),
        sample_path
            .canonicalize()
            .expect("canonicalize sample file"),
        "manifest source_file mismatch"
    );
    assert!(
        file.get("python_bundle").is_none(),
        "JS runner should not emit python_bundle"
    );

    let entries = file
        .get("entries")
        .and_then(Value::as_array)
        .expect("entries array");
    assert_eq!(entries.len(), 1, "expected one code entry");
    let entry = entries[0].as_object().expect("entry object");
    assert_eq!(entry.get("kind").and_then(Value::as_str), Some("code"));
    assert_eq!(entry.get("name").and_then(Value::as_str), Some("js-tool"));
    assert_eq!(entry.get("slug").and_then(Value::as_str), Some("js-tool"));
    assert_eq!(
        entry.get("function_type").and_then(Value::as_str),
        Some("tool")
    );
    assert_eq!(
        entry.get("preview").and_then(Value::as_str),
        Some("export function handler() { return 1; }")
    );
    assert_eq!(
        entry
            .get("location")
            .and_then(Value::as_object)
            .and_then(|value| value.get("type"))
            .and_then(Value::as_str),
        Some("function")
    );
    let function_schema = entry
        .get("function_schema")
        .and_then(Value::as_object)
        .expect("function_schema object");
    let parameters_schema = function_schema
        .get("parameters")
        .and_then(Value::as_object)
        .expect("function_schema.parameters object");
    assert_eq!(
        parameters_schema.get("type").and_then(Value::as_str),
        Some("object"),
        "raw JSON-schema parameters should pass through unchanged"
    );
    assert!(
        parameters_schema
            .get("properties")
            .is_some_and(Value::is_object),
        "raw JSON-schema properties should be preserved"
    );
}

#[test]
fn functions_js_runner_converts_zod_v4_schema_to_json_schema() {
    if !command_exists("node") {
        eprintln!(
            "Skipping functions_js_runner_converts_zod_v4_schema_to_json_schema (node not installed)."
        );
        return;
    }
    let Some(tsc) = find_tsc() else {
        eprintln!(
            "Skipping functions_js_runner_converts_zod_v4_schema_to_json_schema (tsc not installed)."
        );
        return;
    };

    let root = repo_root();
    let fixture_root = root
        .join("tests")
        .join("functions-fixtures")
        .join("push-multiple-files-accepted");
    let zod_module = fixture_root.join("node_modules").join("zod");
    if !zod_module.join("package.json").is_file() {
        eprintln!(
            "Skipping functions_js_runner_converts_zod_v4_schema_to_json_schema (fixture zod package missing)."
        );
        return;
    }

    let zod_module_literal = serde_json::to_string(
        zod_module
            .to_str()
            .expect("zod module path should be valid UTF-8"),
    )
    .expect("serialize zod module path");

    let tmp = tempdir().expect("tempdir");
    let sample_path = tmp.path().join("sample.cjs");
    std::fs::write(
        &sample_path,
        format!(
            r#"const {{ z }} = require({zod_module_literal});
globalThis._evals ??= {{ functions: [], prompts: [], parameters: [], evaluators: {{}}, reporters: {{}} }};
globalThis._evals.functions.push({{
  name: "zod-tool",
  slug: "zod-tool",
  type: "tool",
  parameters: z.object({{ orderId: z.string().describe("The order ID") }}),
  returns: z.object({{ status: z.string() }}),
  preview: "module.exports.handler = () => null;"
}});
"#
        ),
    )
    .expect("write sample.cjs");

    let runner_dir = tmp.path().join("runner");
    let compile_output = Command::new(&tsc)
        .current_dir(&root)
        .args([
            "scripts/functions-runner.ts",
            "scripts/runner-common.ts",
            "--module",
            "esnext",
            "--target",
            "es2020",
            "--moduleResolution",
            "bundler",
            "--outDir",
        ])
        .arg(&runner_dir)
        .output()
        .expect("compile functions runner");
    if !compile_output.status.success() {
        let stdout = String::from_utf8_lossy(&compile_output.stdout);
        let stderr = String::from_utf8_lossy(&compile_output.stderr);
        panic!("tsc failed for functions runner:\nstdout:\n{stdout}\nstderr:\n{stderr}");
    }

    let runner_js = runner_dir.join("functions-runner.js");
    let runner_common_js = runner_dir.join("runner-common.js");
    assert!(runner_js.is_file(), "compiled functions-runner.js missing");
    assert!(
        runner_common_js.is_file(),
        "compiled runner-common.js missing"
    );

    let runner_code = std::fs::read_to_string(&runner_js).expect("read compiled runner");
    let patched_runner_code = runner_code
        .replace("\"./runner-common\"", "\"./runner-common.js\"")
        .replace("'./runner-common'", "'./runner-common.js'");
    assert_ne!(
        runner_code, patched_runner_code,
        "compiled runner import path did not contain ./runner-common"
    );
    std::fs::write(&runner_js, patched_runner_code).expect("write patched compiled runner");
    std::fs::write(runner_dir.join("package.json"), r#"{ "type": "module" }"#)
        .expect("write runner package.json");

    let output = Command::new("node")
        .current_dir(&fixture_root)
        .arg(&runner_js)
        .arg(&sample_path)
        .output()
        .expect("run compiled functions runner");
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("compiled functions runner failed:\n{stderr}");
    }

    let manifest: Value = serde_json::from_slice(&output.stdout).expect("parse manifest JSON");
    let files = manifest["files"].as_array().expect("files array");
    assert_eq!(files.len(), 1, "expected one manifest file");
    let file = files[0].as_object().expect("manifest file object");
    let entries = file
        .get("entries")
        .and_then(Value::as_array)
        .expect("entries array");
    assert_eq!(entries.len(), 1, "expected one code entry");
    let entry = entries[0].as_object().expect("entry object");
    let function_schema = entry
        .get("function_schema")
        .and_then(Value::as_object)
        .expect("function_schema object");

    let parameters_schema = function_schema
        .get("parameters")
        .and_then(Value::as_object)
        .expect("function_schema.parameters object");
    assert_eq!(
        parameters_schema.get("type").and_then(Value::as_str),
        Some("object"),
    );
    assert_eq!(
        parameters_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|props| props.get("orderId"))
            .and_then(Value::as_object)
            .and_then(|order_id| order_id.get("type"))
            .and_then(Value::as_str),
        Some("string"),
        "zod parameters should serialize to JSON schema"
    );
    assert_eq!(
        parameters_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|props| props.get("orderId"))
            .and_then(Value::as_object)
            .and_then(|order_id| order_id.get("description"))
            .and_then(Value::as_str),
        Some("The order ID"),
    );
    assert!(
        parameters_schema.get("_def").is_none(),
        "serialized schema should not include zod internals"
    );
    assert!(
        parameters_schema.get("def").is_none(),
        "serialized schema should not include zod internals"
    );

    let returns_schema = function_schema
        .get("returns")
        .and_then(Value::as_object)
        .expect("function_schema.returns object");
    assert_eq!(
        returns_schema
            .get("properties")
            .and_then(Value::as_object)
            .and_then(|props| props.get("status"))
            .and_then(Value::as_object)
            .and_then(|status| status.get("type"))
            .and_then(Value::as_str),
        Some("string"),
        "zod return schema should serialize to JSON schema"
    );
}

#[test]
fn functions_js_runner_reexecutes_imported_input_files() {
    if !command_exists("node") {
        eprintln!(
            "Skipping functions_js_runner_reexecutes_imported_input_files (node not installed)."
        );
        return;
    }
    let Some(tsc) = find_tsc() else {
        eprintln!(
            "Skipping functions_js_runner_reexecutes_imported_input_files (tsc not installed)."
        );
        return;
    };

    let root = repo_root();
    let tmp = tempdir().expect("tempdir");
    let sample_b_path = tmp.path().join("sample-b.mjs");
    std::fs::write(
        &sample_b_path,
        r#"globalThis._evals ??= { functions: [], prompts: [], parameters: [], evaluators: {}, reporters: {} };
globalThis._evals.functions.push({
  name: "js-tool-b",
  slug: "js-tool-b",
  type: "tool",
  parameters: { type: "object", properties: {} },
  preview: "export function b() { return 2; }"
});
export const b = 2;
"#,
    )
    .expect("write sample-b.mjs");

    let sample_a_path = tmp.path().join("sample-a.mjs");
    std::fs::write(
        &sample_a_path,
        r#"import "./sample-b.mjs";
globalThis._evals ??= { functions: [], prompts: [], parameters: [], evaluators: {}, reporters: {} };
globalThis._evals.functions.push({
  name: "js-tool-a",
  slug: "js-tool-a",
  type: "tool",
  parameters: { type: "object", properties: {} },
  preview: "export function a() { return 1; }"
});
"#,
    )
    .expect("write sample-a.mjs");

    let runner_dir = tmp.path().join("runner");
    let compile_output = Command::new(&tsc)
        .current_dir(&root)
        .args([
            "scripts/functions-runner.ts",
            "scripts/runner-common.ts",
            "--module",
            "esnext",
            "--target",
            "es2020",
            "--moduleResolution",
            "bundler",
            "--outDir",
        ])
        .arg(&runner_dir)
        .output()
        .expect("compile functions runner");
    if !compile_output.status.success() {
        let stdout = String::from_utf8_lossy(&compile_output.stdout);
        let stderr = String::from_utf8_lossy(&compile_output.stderr);
        panic!("tsc failed for functions runner:\nstdout:\n{stdout}\nstderr:\n{stderr}");
    }

    let runner_js = runner_dir.join("functions-runner.js");
    let runner_common_js = runner_dir.join("runner-common.js");
    assert!(runner_js.is_file(), "compiled functions-runner.js missing");
    assert!(
        runner_common_js.is_file(),
        "compiled runner-common.js missing"
    );

    let runner_code = std::fs::read_to_string(&runner_js).expect("read compiled runner");
    let patched_runner_code = runner_code
        .replace("\"./runner-common\"", "\"./runner-common.js\"")
        .replace("'./runner-common'", "'./runner-common.js'");
    assert_ne!(
        runner_code, patched_runner_code,
        "compiled runner import path did not contain ./runner-common"
    );
    std::fs::write(&runner_js, patched_runner_code).expect("write patched compiled runner");
    std::fs::write(runner_dir.join("package.json"), r#"{ "type": "module" }"#)
        .expect("write runner package.json");

    let output = Command::new("node")
        .arg(&runner_js)
        .arg(&sample_a_path)
        .arg(&sample_b_path)
        .output()
        .expect("run compiled functions runner");
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("compiled functions runner failed:\n{stderr}");
    }

    let manifest: Value = serde_json::from_slice(&output.stdout).expect("parse manifest JSON");
    let files = manifest["files"].as_array().expect("files array");
    assert_eq!(files.len(), 2, "expected two manifest files");

    let sample_a_canonical = sample_a_path
        .canonicalize()
        .expect("canonicalize sample-a.mjs");
    let sample_b_canonical = sample_b_path
        .canonicalize()
        .expect("canonicalize sample-b.mjs");
    let mut files_by_source = BTreeMap::new();
    for file in files {
        let source_file = file
            .get("source_file")
            .and_then(Value::as_str)
            .expect("source_file");
        let canonical_source = PathBuf::from(source_file)
            .canonicalize()
            .expect("canonicalize manifest source_file");
        files_by_source.insert(canonical_source, file);
    }

    let file_a = files_by_source
        .get(&sample_a_canonical)
        .expect("manifest file for sample-a.mjs");
    let entries_a = file_a
        .get("entries")
        .and_then(Value::as_array)
        .expect("sample-a entries");
    assert!(
        entries_a
            .iter()
            .any(|entry| { entry.get("slug").and_then(Value::as_str) == Some("js-tool-a") }),
        "expected sample-a.mjs entries to include js-tool-a"
    );

    let file_b = files_by_source
        .get(&sample_b_canonical)
        .expect("manifest file for sample-b.mjs");
    let entries_b = file_b
        .get("entries")
        .and_then(Value::as_array)
        .expect("sample-b entries");
    assert!(
        entries_b
            .iter()
            .any(|entry| { entry.get("slug").and_then(Value::as_str) == Some("js-tool-b") }),
        "expected sample-b.mjs entries to include js-tool-b"
    );
}

#[test]
fn functions_python_runner_emits_valid_manifest_with_bundle() {
    let Some(python) = find_python() else {
        eprintln!(
            "Skipping functions_python_runner_emits_valid_manifest_with_bundle (python not installed)."
        );
        return;
    };

    let root = repo_root();
    let scripts_dir = root.join("scripts");
    let runner_script = scripts_dir.join("functions-runner.py");
    let tmp = tempdir().expect("tempdir");
    let stub_root = tmp.path().join("stub");
    let framework_dir = stub_root.join("braintrust").join("framework2");
    std::fs::create_dir_all(&framework_dir).expect("create stub framework dir");
    std::fs::write(stub_root.join("braintrust").join("__init__.py"), "").expect("write __init__");
    std::fs::write(framework_dir.join("__init__.py"), "").expect("write framework __init__");
    std::fs::write(
        framework_dir.join("global_.py"),
        "functions = []\nprompts = []\n",
    )
    .expect("write global_.py");
    std::fs::write(
        framework_dir.join("lazy_load.py"),
        "from contextlib import nullcontext\n\ndef _set_lazy_load(_enabled):\n    return nullcontext()\n",
    )
    .expect("write lazy_load.py");

    let sample_path = tmp.path().join("sample_tool.py");
    std::fs::write(
        &sample_path,
        r#"from braintrust.framework2.global_ import functions

class TypeEnum:
    value = "tool"

class Params:
    @staticmethod
    def model_json_schema():
        return {"type": "object", "properties": {}}

class Item:
    def __init__(self):
        self.name = "py-tool"
        self.slug = "py-tool"
        self.type_ = TypeEnum()
        self.parameters = Params
        self.preview = "def handler(x):\n    return x"

functions.append(Item())
"#,
    )
    .expect("write sample_tool.py");

    let mut python_path_entries = vec![stub_root.clone()];
    if let Some(existing) = std::env::var_os("PYTHONPATH") {
        python_path_entries.extend(std::env::split_paths(&existing));
    }
    let python_path = std::env::join_paths(python_path_entries).expect("join PYTHONPATH");
    let expected_source = sample_path
        .canonicalize()
        .expect("canonicalize sample file");

    let output = Command::new(&python)
        .current_dir(tmp.path())
        .env("PYTHONPATH", python_path)
        .arg(&runner_script)
        .arg(&expected_source)
        .output()
        .expect("run python functions runner");
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("python functions runner failed:\n{stderr}");
    }

    let manifest: Value = serde_json::from_slice(&output.stdout).expect("parse manifest JSON");
    assert_eq!(
        manifest["runtime_context"]["runtime"].as_str(),
        Some("python"),
        "runtime_context.runtime should be python"
    );
    assert!(
        manifest["runtime_context"]["version"]
            .as_str()
            .is_some_and(|value| !value.trim().is_empty()),
        "runtime_context.version should be present"
    );

    let files = manifest["files"].as_array().expect("files array");
    assert_eq!(files.len(), 1, "expected one manifest file");
    let file = files[0].as_object().expect("manifest file object");
    let reported_source = PathBuf::from(
        file.get("source_file")
            .and_then(Value::as_str)
            .expect("source_file"),
    );
    assert_eq!(
        reported_source
            .canonicalize()
            .expect("canonicalize source_file"),
        expected_source,
        "manifest source_file mismatch"
    );

    let entries = file
        .get("entries")
        .and_then(Value::as_array)
        .expect("entries array");
    assert_eq!(entries.len(), 1, "expected one code entry");
    let entry = entries[0].as_object().expect("entry object");
    assert_eq!(entry.get("kind").and_then(Value::as_str), Some("code"));
    assert_eq!(entry.get("name").and_then(Value::as_str), Some("py-tool"));
    assert_eq!(entry.get("slug").and_then(Value::as_str), Some("py-tool"));
    assert_eq!(
        entry.get("function_type").and_then(Value::as_str),
        Some("tool")
    );
    assert_eq!(
        entry.get("preview").and_then(Value::as_str),
        Some("def handler(x):\n    return x")
    );

    let bundle = file
        .get("python_bundle")
        .and_then(Value::as_object)
        .expect("python_bundle object");
    assert!(
        bundle
            .get("entry_module")
            .and_then(Value::as_str)
            .is_some_and(|value| !value.trim().is_empty()),
        "python_bundle.entry_module should be present"
    );
    let sources = bundle
        .get("sources")
        .and_then(Value::as_array)
        .expect("python_bundle.sources array");
    assert!(
        !sources.is_empty(),
        "python_bundle.sources should include source files"
    );
    let source_paths = sources
        .iter()
        .filter_map(Value::as_str)
        .map(PathBuf::from)
        .map(|path| path.canonicalize().expect("canonicalize bundled source"))
        .collect::<Vec<_>>();
    assert!(
        source_paths.contains(&expected_source),
        "python_bundle.sources should include sample file"
    );
}

#[test]
fn python_runner_common_purge_prevents_cross_file_source_leakage() {
    let Some(python) = find_python() else {
        eprintln!(
            "Skipping python_runner_common_purge_prevents_cross_file_source_leakage (python not installed)."
        );
        return;
    };

    let root = repo_root();
    let scripts_dir = root.join("scripts");
    let tmp = tempdir().expect("tempdir");
    let a_path = tmp.path().join("a.py");
    let b_path = tmp.path().join("b.py");
    std::fs::write(&a_path, "VALUE_A = 1\n").expect("write a.py");
    std::fs::write(&b_path, "VALUE_B = 2\n").expect("write b.py");

    let snippet = r#"
import importlib.util
import json
import pathlib
import sys

scripts_dir = pathlib.Path(sys.argv[1])
tmp_dir = pathlib.Path(sys.argv[2])

common_path = scripts_dir / "python_runner_common.py"
spec = importlib.util.spec_from_file_location("python_runner_common", common_path)
if spec is None or spec.loader is None:
    raise RuntimeError(f"failed to load {common_path}")
common = importlib.util.module_from_spec(spec)
spec.loader.exec_module(common)
sys.modules["python_runner_common"] = common

cwd = str(tmp_dir)
a_path = tmp_dir / "a.py"
b_path = tmp_dir / "b.py"

module_name_a, extra_a = common.resolve_module_info(str(a_path))
common.import_file(module_name_a, str(a_path), extra_a)
sources_a = common.collect_python_sources(cwd, str(a_path))

common.purge_local_modules(cwd, preserve_modules={"__main__", "python_runner_common"})

module_name_b, extra_b = common.resolve_module_info(str(b_path))
common.import_file(module_name_b, str(b_path), extra_b)
sources_b = common.collect_python_sources(cwd, str(b_path))

print(json.dumps({"sources_a": sources_a, "sources_b": sources_b}))
"#;

    let output = Command::new(&python)
        .arg("-c")
        .arg(snippet)
        .arg(&scripts_dir)
        .arg(tmp.path())
        .output()
        .expect("run python_runner_common purge regression script");

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("Python runner common regression script failed:\n{stderr}");
    }

    let stdout = String::from_utf8(output.stdout).expect("stdout utf-8");
    let parsed: Value = serde_json::from_str(stdout.trim()).expect("parse JSON output");
    let sources_a = parsed
        .get("sources_a")
        .and_then(Value::as_array)
        .expect("sources_a array")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    let sources_b = parsed
        .get("sources_b")
        .and_then(Value::as_array)
        .expect("sources_b array")
        .iter()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();

    let a_str = a_path.to_string_lossy().to_string();
    let b_str = b_path.to_string_lossy().to_string();
    assert!(
        sources_a.contains(&a_str.as_str()),
        "sources_a should contain a.py"
    );
    assert!(
        sources_b.contains(&b_str.as_str()),
        "sources_b should contain b.py"
    );
    assert!(
        !sources_b.contains(&a_str.as_str()),
        "sources_b should not include a.py from prior file import"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn functions_push_works_against_mock_api() {
    if !command_exists("node") {
        eprintln!("Skipping functions_push_works_against_mock_api (node not installed).");
        return;
    }

    let state = Arc::new(MockServerState::default());
    state
        .projects
        .lock()
        .expect("projects lock")
        .push(MockProject {
            id: "proj_mock".to_string(),
            name: "mock-project".to_string(),
            org_id: "org_mock".to_string(),
        });
    let server = MockServer::start(state.clone()).await;

    let tmp = tempdir().expect("tempdir");
    let source = tmp.path().join("tool.js");
    std::fs::write(
        &source,
        "globalThis._evals ??= { functions: [], prompts: [], parameters: [], evaluators: {}, reporters: {} };\n",
    )
    .expect("write source file");

    let runner = tmp.path().join("mock-runner.sh");
    std::fs::write(
        &runner,
        r#"#!/bin/sh
set -eu
_runner_script="$1"
shift
_runner_name="$(basename "$_runner_script")"

if [ "$_runner_name" = "functions-runner.ts" ]; then
node - "$@" <<'NODE'
const path = require("node:path");
const files = process.argv.slice(2);
const manifest = {
  runtime_context: { runtime: "node", version: process.versions.node || "unknown" },
  files: files.map((file, index) => ({
    source_file: path.resolve(file),
    entries: [
      {
        kind: "code",
        project_id: "proj_mock",
        name: index === 0 ? "mock-tool" : `mock-tool-${index}`,
        slug: index === 0 ? "mock-tool" : `mock-tool-${index}`,
        function_type: "tool",
        preview: "function handler() { return 1; }",
        location: { type: "function", index: 0 }
      }
    ]
  }))
};
process.stdout.write(JSON.stringify(manifest));
NODE
exit 0
fi

if [ "$_runner_name" = "functions-bundler.ts" ]; then
  _source_file="$1"
  _output_file="$2"
  cp "$_source_file" "$_output_file"
  exit 0
fi

echo "unexpected runner script: $_runner_name" >&2
exit 24
"#,
    )
    .expect("write mock runner");
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(&runner)
        .expect("runner metadata")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&runner, perms).expect("runner permissions");

    let output = Command::new(bt_binary_path())
        .current_dir(tmp.path())
        .args([
            "functions",
            "--json",
            "push",
            "--file",
            source
                .to_str()
                .expect("source path should be valid UTF-8 for test"),
            "--language",
            "javascript",
            "--runner",
            runner
                .to_str()
                .expect("runner path should be valid UTF-8 for test"),
            "--if-exists",
            "replace",
        ])
        .env("BRAINTRUST_API_KEY", "test-key")
        .env("BRAINTRUST_ORG_NAME", "test-org")
        .env("BRAINTRUST_API_URL", &server.base_url)
        .env("BRAINTRUST_APP_URL", &server.base_url)
        .env("BRAINTRUST_NO_COLOR", "1")
        .env_remove("BRAINTRUST_PROFILE")
        .output()
        .expect("run bt functions push");

    server.stop().await;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("mock push failed:\n{stderr}");
    }

    let summary: Value = serde_json::from_slice(&output.stdout).expect("parse push summary");
    assert_eq!(summary["status"].as_str(), Some("success"));
    assert_eq!(summary["uploaded_files"].as_u64(), Some(1));
    assert_eq!(summary["failed_files"].as_u64(), Some(0));

    let inserted = state
        .inserted_functions
        .lock()
        .expect("inserted functions lock")
        .clone();
    assert_eq!(inserted.len(), 1, "exactly one function should be inserted");
    let first = inserted[0].as_object().expect("inserted function object");
    assert_eq!(
        first.get("project_id").and_then(Value::as_str),
        Some("proj_mock")
    );
    assert_eq!(first.get("slug").and_then(Value::as_str), Some("mock-tool"));
    assert_eq!(
        first.get("function_type").and_then(Value::as_str),
        Some("tool")
    );
    let function_data = first
        .get("function_data")
        .and_then(Value::as_object)
        .expect("function_data object");
    assert_eq!(
        function_data.get("type").and_then(Value::as_str),
        Some("code"),
        "function_data.type must be code"
    );
    let data = function_data
        .get("data")
        .and_then(Value::as_object)
        .expect("function_data.data object");
    assert_eq!(data.get("type").and_then(Value::as_str), Some("bundle"));
    assert_eq!(
        data.get("preview").and_then(Value::as_str),
        Some("function handler() { return 1; }")
    );

    let uploaded = state
        .uploaded_bundles
        .lock()
        .expect("uploaded bundles lock")
        .clone();
    assert_eq!(uploaded.len(), 1, "expected one uploaded bundle");
    let bundle = &uploaded[0];
    let decompressed = decode_uploaded_bundle(bundle);
    assert!(
        decompressed.contains("globalThis._evals"),
        "uploaded bundle should contain original source"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn functions_push_prompt_environments_upsert_after_insert() {
    if !command_exists("node") {
        eprintln!(
            "Skipping functions_push_prompt_environments_upsert_after_insert (node not installed)."
        );
        return;
    }

    let state = Arc::new(MockServerState::default());
    state
        .projects
        .lock()
        .expect("projects lock")
        .push(MockProject {
            id: "proj_mock".to_string(),
            name: "mock-project".to_string(),
            org_id: "org_mock".to_string(),
        });
    let server = MockServer::start(state.clone()).await;

    let tmp = tempdir().expect("tempdir");
    let source = tmp.path().join("prompt.js");
    std::fs::write(
        &source,
        "globalThis._evals ??= { functions: [], prompts: [], parameters: [], evaluators: {}, reporters: {} };\n",
    )
    .expect("write source file");

    let runner = tmp.path().join("mock-runner.sh");
    std::fs::write(
        &runner,
        r#"#!/bin/sh
set -eu
_runner_script="$1"
shift
_runner_name="$(basename "$_runner_script")"

if [ "$_runner_name" = "functions-runner.ts" ]; then
node - "$@" <<'NODE'
const path = require("node:path");
const files = process.argv.slice(2);
const manifest = {
  runtime_context: { runtime: "node", version: process.versions.node || "unknown" },
  files: files.map((file) => ({
    source_file: path.resolve(file),
    entries: [
      {
        kind: "function_event",
        project_id: "proj_mock",
        event: {
          project_id: "proj_mock",
          name: "mock-prompt",
          slug: "mock-prompt",
          function_data: { type: "prompt" },
          prompt_data: {
            prompt: {
              type: "chat",
              messages: [{ role: "system", content: "Hello" }]
            },
            options: { model: "gpt-4o-mini" }
          },
          if_exists: "replace",
          environments: [{ slug: "staging" }, "prod"]
        }
      }
    ]
  }))
};
process.stdout.write(JSON.stringify(manifest));
NODE
exit 0
fi

if [ "$_runner_name" = "functions-bundler.ts" ]; then
  _source_file="$1"
  _output_file="$2"
  cp "$_source_file" "$_output_file"
  exit 0
fi

echo "unexpected runner script: $_runner_name" >&2
exit 24
"#,
    )
    .expect("write mock runner");
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(&runner)
        .expect("runner metadata")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&runner, perms).expect("runner permissions");

    let output = Command::new(bt_binary_path())
        .current_dir(tmp.path())
        .args([
            "functions",
            "--json",
            "push",
            "--file",
            source
                .to_str()
                .expect("source path should be valid UTF-8 for test"),
            "--language",
            "javascript",
            "--runner",
            runner
                .to_str()
                .expect("runner path should be valid UTF-8 for test"),
            "--if-exists",
            "replace",
        ])
        .env("BRAINTRUST_API_KEY", "test-key")
        .env("BRAINTRUST_ORG_NAME", "test-org")
        .env("BRAINTRUST_API_URL", &server.base_url)
        .env("BRAINTRUST_APP_URL", &server.base_url)
        .env("BRAINTRUST_NO_COLOR", "1")
        .env_remove("BRAINTRUST_PROFILE")
        .output()
        .expect("run bt functions push");

    server.stop().await;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("mock push failed:\n{stderr}");
    }

    let summary: Value = serde_json::from_slice(&output.stdout).expect("parse push summary");
    assert_eq!(summary["status"].as_str(), Some("success"));
    assert_eq!(summary["uploaded_files"].as_u64(), Some(1));
    assert_eq!(summary["failed_files"].as_u64(), Some(0));

    let inserted = state
        .inserted_functions
        .lock()
        .expect("inserted functions lock")
        .clone();
    assert_eq!(inserted.len(), 1, "exactly one function should be inserted");
    let first = inserted[0].as_object().expect("inserted function object");
    assert!(
        first.get("environments").is_none(),
        "insert payload should strip environments and use environment-object endpoint"
    );

    let upserts = state
        .environment_upserts
        .lock()
        .expect("environment upserts lock")
        .clone();
    assert_eq!(upserts.len(), 2, "expected one upsert per environment");
    let mut slugs = upserts
        .iter()
        .filter_map(|entry| entry.get("environment_slug").and_then(Value::as_str))
        .map(ToOwned::to_owned)
        .collect::<Vec<_>>();
    slugs.sort();
    assert_eq!(slugs, vec!["prod".to_string(), "staging".to_string()]);
    for upsert in &upserts {
        assert_eq!(
            upsert.get("object_type").and_then(Value::as_str),
            Some("prompt")
        );
        assert_eq!(
            upsert.get("object_id").and_then(Value::as_str),
            Some("fn_inserted_0")
        );
        assert_eq!(
            upsert.get("object_version").and_then(Value::as_str),
            Some("0000000000000001")
        );
        assert_eq!(
            upsert.get("org_name").and_then(Value::as_str),
            Some("test-org")
        );
    }

    let requests = state.requests.lock().expect("requests lock").clone();
    assert!(
        requests
            .iter()
            .any(|entry| entry == "/environment-object/prompt/fn_inserted_0/staging"),
        "push should upsert staging environment for the inserted prompt"
    );
    assert!(
        requests
            .iter()
            .any(|entry| entry == "/environment-object/prompt/fn_inserted_0/prod"),
        "push should upsert prod environment for the inserted prompt"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn functions_push_external_packages_bundles_with_runner() {
    if !command_exists("node") {
        eprintln!(
            "Skipping functions_push_external_packages_bundles_with_runner (node not installed)."
        );
        return;
    }

    let state = Arc::new(MockServerState::default());
    state
        .projects
        .lock()
        .expect("projects lock")
        .push(MockProject {
            id: "proj_mock".to_string(),
            name: "mock-project".to_string(),
            org_id: "org_mock".to_string(),
        });
    let server = MockServer::start(state.clone()).await;

    let tmp = tempdir().expect("tempdir");
    let source = tmp.path().join("tool.js");
    std::fs::write(
        &source,
        "globalThis._evals ??= { functions: [], prompts: [], parameters: [], evaluators: {}, reporters: {} };\n",
    )
    .expect("write source file");

    let runner = tmp.path().join("mock-runner.sh");
    std::fs::write(
        &runner,
        r#"#!/bin/sh
set -eu
_runner_script="$1"
shift
_runner_name="$(basename "$_runner_script")"

if [ "$_runner_name" = "functions-runner.ts" ]; then
  node - "$@" <<'NODE'
const path = require("node:path");
const files = process.argv.slice(2);
const manifest = {
  runtime_context: { runtime: "node", version: process.versions.node || "unknown" },
  files: files.map((file, index) => ({
    source_file: path.resolve(file),
    entries: [
      {
        kind: "code",
        project_id: "proj_mock",
        name: index === 0 ? "mock-tool" : `mock-tool-${index}`,
        slug: index === 0 ? "mock-tool" : `mock-tool-${index}`,
        function_type: "tool",
        preview: "function handler() { return 1; }",
        location: { type: "function", index: 0 }
      }
    ]
  }))
};
process.stdout.write(JSON.stringify(manifest));
NODE
  exit 0
fi

if [ "$_runner_name" = "functions-bundler.ts" ]; then
  if [ "${BT_FUNCTIONS_PUSH_EXTERNAL_PACKAGES:-}" != "sqlite3,fsevents" ]; then
    echo "unexpected BT_FUNCTIONS_PUSH_EXTERNAL_PACKAGES=${BT_FUNCTIONS_PUSH_EXTERNAL_PACKAGES:-}" >&2
    exit 23
  fi
  _source_file="$1"
  _output_file="$2"
  printf '%s\n' "// bundled output" >"$_output_file"
  printf '%s\n' "const externalMarker = \"externals:${BT_FUNCTIONS_PUSH_EXTERNAL_PACKAGES}\";" >>"$_output_file"
  printf '%s\n' "const sourceMarker = \"source:${_source_file}\";" >>"$_output_file"
  exit 0
fi

echo "unexpected runner script: $_runner_name" >&2
exit 24
"#,
    )
    .expect("write mock runner");
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(&runner)
        .expect("runner metadata")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&runner, perms).expect("runner permissions");

    let output = Command::new(bt_binary_path())
        .current_dir(tmp.path())
        .args([
            "functions",
            "--json",
            "push",
            "--file",
            source
                .to_str()
                .expect("source path should be valid UTF-8 for test"),
            "--language",
            "javascript",
            "--runner",
            runner
                .to_str()
                .expect("runner path should be valid UTF-8 for test"),
            "--external-packages",
            "sqlite3,fsevents",
        ])
        .env("BRAINTRUST_API_KEY", "test-key")
        .env("BRAINTRUST_ORG_NAME", "test-org")
        .env("BRAINTRUST_API_URL", &server.base_url)
        .env("BRAINTRUST_APP_URL", &server.base_url)
        .env("BRAINTRUST_NO_COLOR", "1")
        .env_remove("BRAINTRUST_PROFILE")
        .output()
        .expect("run bt functions push");

    server.stop().await;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("mock push failed:\n{stderr}");
    }

    let summary: Value = serde_json::from_slice(&output.stdout).expect("parse push summary");
    assert_eq!(summary["status"].as_str(), Some("success"));
    assert_eq!(summary["uploaded_files"].as_u64(), Some(1));
    assert_eq!(summary["failed_files"].as_u64(), Some(0));

    let uploaded = state
        .uploaded_bundles
        .lock()
        .expect("uploaded bundles lock")
        .clone();
    assert_eq!(uploaded.len(), 1, "expected one uploaded bundle");
    let bundle = &uploaded[0];
    let decompressed = decode_uploaded_bundle(bundle);
    assert!(
        decompressed.contains("externals:sqlite3,fsevents"),
        "uploaded bundle should include bundler output with external package marker"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn functions_push_js_bundles_by_default() {
    if !command_exists("node") {
        eprintln!("Skipping functions_push_js_bundles_by_default (node not installed).");
        return;
    }

    let state = Arc::new(MockServerState::default());
    state
        .projects
        .lock()
        .expect("projects lock")
        .push(MockProject {
            id: "proj_mock".to_string(),
            name: "mock-project".to_string(),
            org_id: "org_mock".to_string(),
        });
    let server = MockServer::start(state.clone()).await;

    let tmp = tempdir().expect("tempdir");
    let source = tmp.path().join("tool.js");
    std::fs::write(
        &source,
        "globalThis._evals ??= { functions: [], prompts: [], parameters: [], evaluators: {}, reporters: {} };\n",
    )
    .expect("write source file");

    let runner = tmp.path().join("mock-runner.sh");
    std::fs::write(
        &runner,
        r#"#!/bin/sh
set -eu
_runner_script="$1"
shift
_runner_name="$(basename "$_runner_script")"

if [ "$_runner_name" = "functions-runner.ts" ]; then
  node - "$@" <<'NODE'
const path = require("node:path");
const files = process.argv.slice(2);
const manifest = {
  runtime_context: { runtime: "node", version: process.versions.node || "unknown" },
  files: files.map((file, index) => ({
    source_file: path.resolve(file),
    entries: [
      {
        kind: "code",
        project_id: "proj_mock",
        name: index === 0 ? "mock-tool" : `mock-tool-${index}`,
        slug: index === 0 ? "mock-tool" : `mock-tool-${index}`,
        function_type: "tool",
        preview: "function handler() { return 1; }",
        location: { type: "function", index: 0 }
      }
    ]
  }))
};
process.stdout.write(JSON.stringify(manifest));
NODE
  exit 0
fi

if [ "$_runner_name" = "functions-bundler.ts" ]; then
  _output_file="$2"
  printf '%s\n' "// bundled by default path" >"$_output_file"
  printf '%s\n' "const marker = \"default-bundler-used\";" >>"$_output_file"
  exit 0
fi

echo "unexpected runner script: $_runner_name" >&2
exit 24
"#,
    )
    .expect("write mock runner");
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(&runner)
        .expect("runner metadata")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&runner, perms).expect("runner permissions");

    let output = Command::new(bt_binary_path())
        .current_dir(tmp.path())
        .args([
            "functions",
            "--json",
            "push",
            "--file",
            source
                .to_str()
                .expect("source path should be valid UTF-8 for test"),
            "--language",
            "javascript",
            "--runner",
            runner
                .to_str()
                .expect("runner path should be valid UTF-8 for test"),
        ])
        .env("BRAINTRUST_API_KEY", "test-key")
        .env("BRAINTRUST_ORG_NAME", "test-org")
        .env("BRAINTRUST_API_URL", &server.base_url)
        .env("BRAINTRUST_APP_URL", &server.base_url)
        .env("BRAINTRUST_NO_COLOR", "1")
        .env_remove("BRAINTRUST_PROFILE")
        .output()
        .expect("run bt functions push");

    server.stop().await;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("mock push failed:\n{stderr}");
    }

    let uploaded = state
        .uploaded_bundles
        .lock()
        .expect("uploaded bundles lock")
        .clone();
    assert_eq!(uploaded.len(), 1, "expected one uploaded bundle");
    let decompressed = decode_uploaded_bundle(&uploaded[0]);
    assert!(
        decompressed.contains("default-bundler-used"),
        "uploaded bundle should include marker emitted by bundler path"
    );
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn functions_push_tsconfig_is_forwarded_to_bundler() {
    if !command_exists("node") {
        eprintln!("Skipping functions_push_tsconfig_is_forwarded_to_bundler (node not installed).");
        return;
    }

    let state = Arc::new(MockServerState::default());
    state
        .projects
        .lock()
        .expect("projects lock")
        .push(MockProject {
            id: "proj_mock".to_string(),
            name: "mock-project".to_string(),
            org_id: "org_mock".to_string(),
        });
    let server = MockServer::start(state.clone()).await;

    let tmp = tempdir().expect("tempdir");
    let source = tmp.path().join("tool.js");
    std::fs::write(
        &source,
        "globalThis._evals ??= { functions: [], prompts: [], parameters: [], evaluators: {}, reporters: {} };\n",
    )
    .expect("write source file");
    let tsconfig = tmp.path().join("tsconfig.json");
    std::fs::write(
        &tsconfig,
        "{ \"compilerOptions\": { \"target\": \"ES2020\" } }",
    )
    .expect("write tsconfig");

    let runner = tmp.path().join("mock-runner.sh");
    std::fs::write(
        &runner,
        r#"#!/bin/sh
set -eu
_runner_script="$1"
shift
_runner_name="$(basename "$_runner_script")"

if [ "$_runner_name" = "functions-runner.ts" ]; then
  node - "$@" <<'NODE'
const path = require("node:path");
const files = process.argv.slice(2);
const manifest = {
  runtime_context: { runtime: "node", version: process.versions.node || "unknown" },
  files: files.map((file, index) => ({
    source_file: path.resolve(file),
    entries: [
      {
        kind: "code",
        project_id: "proj_mock",
        name: index === 0 ? "mock-tool" : `mock-tool-${index}`,
        slug: index === 0 ? "mock-tool" : `mock-tool-${index}`,
        function_type: "tool",
        preview: "function handler() { return 1; }",
        location: { type: "function", index: 0 }
      }
    ]
  }))
};
process.stdout.write(JSON.stringify(manifest));
NODE
  exit 0
fi

if [ "$_runner_name" = "functions-bundler.ts" ]; then
  if [ "${TS_NODE_PROJECT:-}" != "${EXPECTED_TSCONFIG:-}" ]; then
    echo "unexpected TS_NODE_PROJECT=${TS_NODE_PROJECT:-}" >&2
    exit 31
  fi
  if [ "${TSX_TSCONFIG_PATH:-}" != "${EXPECTED_TSCONFIG:-}" ]; then
    echo "unexpected TSX_TSCONFIG_PATH=${TSX_TSCONFIG_PATH:-}" >&2
    exit 32
  fi
  _output_file="$2"
  printf '%s\n' "// bundled with tsconfig" >"$_output_file"
  printf '%s\n' "const marker = \"tsconfig-forwarded:${TS_NODE_PROJECT}\";" >>"$_output_file"
  exit 0
fi

echo "unexpected runner script: $_runner_name" >&2
exit 24
"#,
    )
    .expect("write mock runner");
    use std::os::unix::fs::PermissionsExt;
    let mut perms = std::fs::metadata(&runner)
        .expect("runner metadata")
        .permissions();
    perms.set_mode(0o755);
    std::fs::set_permissions(&runner, perms).expect("runner permissions");

    let output = Command::new(bt_binary_path())
        .current_dir(tmp.path())
        .args([
            "functions",
            "--json",
            "push",
            "--file",
            source
                .to_str()
                .expect("source path should be valid UTF-8 for test"),
            "--language",
            "javascript",
            "--runner",
            runner
                .to_str()
                .expect("runner path should be valid UTF-8 for test"),
            "--tsconfig",
            tsconfig
                .to_str()
                .expect("tsconfig path should be valid UTF-8 for test"),
        ])
        .env("EXPECTED_TSCONFIG", &tsconfig)
        .env("BRAINTRUST_API_KEY", "test-key")
        .env("BRAINTRUST_ORG_NAME", "test-org")
        .env("BRAINTRUST_API_URL", &server.base_url)
        .env("BRAINTRUST_APP_URL", &server.base_url)
        .env("BRAINTRUST_NO_COLOR", "1")
        .env_remove("BRAINTRUST_PROFILE")
        .output()
        .expect("run bt functions push");

    server.stop().await;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("mock push failed:\n{stderr}");
    }

    let uploaded = state
        .uploaded_bundles
        .lock()
        .expect("uploaded bundles lock")
        .clone();
    assert_eq!(uploaded.len(), 1, "expected one uploaded bundle");
    let decompressed = decode_uploaded_bundle(&uploaded[0]);
    assert!(
        decompressed.contains(&format!(
            "tsconfig-forwarded:{}",
            tsconfig
                .to_str()
                .expect("tsconfig path should be valid UTF-8 for test")
        )),
        "uploaded bundle should include tsconfig marker emitted by bundler path"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn functions_pull_works_against_mock_api() {
    let state = Arc::new(MockServerState::default());
    state
        .projects
        .lock()
        .expect("projects lock")
        .push(MockProject {
            id: "proj_mock".to_string(),
            name: "mock-project".to_string(),
            org_id: "org_mock".to_string(),
        });
    state
        .pull_rows
        .lock()
        .expect("pull rows lock")
        .push(serde_json::json!({
            "id": "fn_123",
            "name": "Doc Search",
            "slug": "doc-search",
            "project_id": "proj_mock",
            "description": "",
            "function_data": { "type": "prompt" },
            "prompt_data": {
                "prompt": {
                    "type": "chat",
                    "messages": [
                        { "role": "system", "content": "You answer from docs." }
                    ]
                },
                "options": {
                    "model": "gpt-4o-mini"
                }
            },
            "_xact_id": "0000000000000001"
        }));
    state
        .environment_objects
        .lock()
        .expect("environment objects lock")
        .insert(
            "fn_123".to_string(),
            vec![
                serde_json::json!({
                    "environment_slug": "staging",
                    "object_version": "0000000000000001"
                }),
                serde_json::json!({
                    "environment_slug": "prod",
                    "object_version": "0000000000000000"
                }),
            ],
        );

    let server = MockServer::start(state.clone()).await;

    let tmp = tempdir().expect("tempdir");
    let out_dir = tmp.path().join("pulled");
    std::fs::create_dir_all(&out_dir).expect("create output dir");

    let output = Command::new(bt_binary_path())
        .current_dir(tmp.path())
        .args([
            "functions",
            "--json",
            "pull",
            "--project-id",
            "proj_mock",
            "--slug",
            "doc-search",
            "--force",
            "--output-dir",
            out_dir
                .to_str()
                .expect("output dir should be valid UTF-8 for test"),
            "--language",
            "typescript",
        ])
        .env("BRAINTRUST_API_KEY", "test-key")
        .env("BRAINTRUST_ORG_NAME", "test-org")
        .env("BRAINTRUST_API_URL", &server.base_url)
        .env("BRAINTRUST_APP_URL", &server.base_url)
        .env("BRAINTRUST_NO_COLOR", "1")
        .env_remove("BRAINTRUST_PROFILE")
        .output()
        .expect("run bt functions pull");

    server.stop().await;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("mock pull failed:\n{stderr}");
    }

    let summary: Value = serde_json::from_slice(&output.stdout).expect("parse pull summary");
    assert_eq!(summary["status"].as_str(), Some("success"));
    assert_eq!(summary["files_written"].as_u64(), Some(1));
    assert_eq!(summary["files_failed"].as_u64(), Some(0));

    let rendered_file = out_dir.join("mock-project.ts");
    assert!(rendered_file.is_file(), "expected rendered file to exist");
    let rendered = std::fs::read_to_string(&rendered_file).expect("read rendered file");
    assert!(
        rendered.contains("project.prompts.create"),
        "rendered file should materialize prompt definitions"
    );
    assert!(
        rendered.contains("slug: \"doc-search\""),
        "rendered file should include slug"
    );
    assert!(
        rendered.contains("gpt-4o-mini"),
        "rendered file should include model config"
    );
    assert!(
        rendered.contains("environments: ["),
        "rendered file should include environments field"
    );
    assert!(
        rendered.contains("\"staging\""),
        "rendered file should include matching environment slug"
    );
    assert!(
        !rendered.contains("\"prod\""),
        "rendered file should omit non-matching environment versions"
    );

    let requests = state.requests.lock().expect("requests lock").clone();
    assert!(
        requests.iter().any(|entry| {
            entry.contains("/v1/function")
                && entry.contains("project_id=proj_mock")
                && entry.contains("slug=doc-search")
        }),
        "pull request should include selector query params"
    );
    assert!(
        requests
            .iter()
            .any(|entry| entry == "/environment-object/prompt/fn_123"),
        "pull should hydrate prompt environments from environment-object endpoint"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn functions_pull_skips_untracked_existing_file_without_force() {
    if !command_exists("git") {
        eprintln!(
            "Skipping functions_pull_skips_untracked_existing_file_without_force (git not installed)."
        );
        return;
    }

    let state = Arc::new(MockServerState::default());
    state
        .projects
        .lock()
        .expect("projects lock")
        .push(MockProject {
            id: "proj_mock".to_string(),
            name: "mock-project".to_string(),
            org_id: "org_mock".to_string(),
        });
    state
        .pull_rows
        .lock()
        .expect("pull rows lock")
        .push(serde_json::json!({
            "id": "fn_123",
            "name": "Doc Search",
            "slug": "doc-search",
            "project_id": "proj_mock",
            "description": "",
            "function_data": { "type": "prompt" },
            "prompt_data": {
                "prompt": {
                    "type": "chat",
                    "messages": [
                        { "role": "system", "content": "You answer from docs." }
                    ]
                },
                "options": {
                    "model": "gpt-4o-mini"
                }
            },
            "_xact_id": "0000000000000001"
        }));

    let server = MockServer::start(state.clone()).await;

    let tmp = tempdir().expect("tempdir");
    let out_dir = tmp.path().join("pulled");
    std::fs::create_dir_all(&out_dir).expect("create output dir");
    let rendered_file = out_dir.join("mock-project.ts");
    std::fs::write(&rendered_file, "LOCAL\n").expect("seed untracked output file");

    run_git(tmp.path(), &["init"]);

    let output = Command::new(bt_binary_path())
        .current_dir(tmp.path())
        .args([
            "functions",
            "--json",
            "pull",
            "--project-id",
            "proj_mock",
            "--slug",
            "doc-search",
            "--output-dir",
            out_dir
                .to_str()
                .expect("output dir should be valid UTF-8 for test"),
            "--language",
            "typescript",
        ])
        .env("BRAINTRUST_API_KEY", "test-key")
        .env("BRAINTRUST_ORG_NAME", "test-org")
        .env("BRAINTRUST_API_URL", &server.base_url)
        .env("BRAINTRUST_APP_URL", &server.base_url)
        .env("BRAINTRUST_NO_COLOR", "1")
        .env_remove("BRAINTRUST_PROFILE")
        .output()
        .expect("run bt functions pull");

    server.stop().await;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("mock pull failed:\n{stderr}");
    }

    let summary: Value = serde_json::from_slice(&output.stdout).expect("parse pull summary");
    assert_eq!(summary["status"].as_str(), Some("partial"));
    assert_eq!(summary["files_written"].as_u64(), Some(0));
    assert_eq!(summary["files_skipped"].as_u64(), Some(1));
    let files = summary["files"].as_array().expect("files array");
    assert_eq!(files.len(), 1);
    assert_eq!(files[0]["status"].as_str(), Some("skipped"));
    assert_eq!(files[0]["skipped_reason"].as_str(), Some("dirty_target"));

    let rendered = std::fs::read_to_string(&rendered_file).expect("read rendered file");
    assert_eq!(rendered, "LOCAL\n");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn functions_pull_selector_with_unsupported_only_rows_still_succeeds() {
    let state = Arc::new(MockServerState::default());
    state
        .projects
        .lock()
        .expect("projects lock")
        .push(MockProject {
            id: "proj_mock".to_string(),
            name: "mock-project".to_string(),
            org_id: "org_mock".to_string(),
        });
    state
        .pull_rows
        .lock()
        .expect("pull rows lock")
        .push(serde_json::json!({
            "id": "fn_code_1",
            "name": "Legacy Code Function",
            "slug": "legacy-code",
            "project_id": "proj_mock",
            "description": "",
            "function_data": { "type": "code" },
            "_xact_id": "0000000000000001"
        }));

    let server = MockServer::start(state.clone()).await;

    let tmp = tempdir().expect("tempdir");
    let out_dir = tmp.path().join("pulled");
    std::fs::create_dir_all(&out_dir).expect("create output dir");

    let output = Command::new(bt_binary_path())
        .current_dir(tmp.path())
        .args([
            "functions",
            "--json",
            "pull",
            "--project-id",
            "proj_mock",
            "--slug",
            "legacy-code",
            "--force",
            "--output-dir",
            out_dir
                .to_str()
                .expect("output dir should be valid UTF-8 for test"),
            "--language",
            "typescript",
            "--verbose",
        ])
        .env("BRAINTRUST_API_KEY", "test-key")
        .env("BRAINTRUST_ORG_NAME", "test-org")
        .env("BRAINTRUST_API_URL", &server.base_url)
        .env("BRAINTRUST_APP_URL", &server.base_url)
        .env("BRAINTRUST_NO_COLOR", "1")
        .env_remove("BRAINTRUST_PROFILE")
        .output()
        .expect("run bt functions pull");

    server.stop().await;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        panic!("mock pull failed:\n{stderr}");
    }

    let summary: Value = serde_json::from_slice(&output.stdout).expect("parse pull summary");
    assert_eq!(summary["status"].as_str(), Some("partial"));
    assert_eq!(summary["files_written"].as_u64(), Some(0));
    assert_eq!(summary["files_failed"].as_u64(), Some(0));
    assert_eq!(summary["unsupported_records_skipped"].as_u64(), Some(1));
    assert_eq!(summary["functions_materialized"].as_u64(), Some(0));

    let rendered_file = out_dir.join("mock-project.ts");
    assert!(
        !rendered_file.exists(),
        "no file should be written when all rows are unsupported"
    );

    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(
        stderr.contains("skipping 'legacy-code' because it is not a prompt"),
        "expected warning about non-prompt function on stderr, got:\n{stderr}"
    );
}
