use std::collections::BTreeMap;
use std::fs::{self, File};
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, Mutex};

use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Debug, Deserialize)]
struct FixtureConfig {
    #[serde(default)]
    env: BTreeMap<String, String>,
    steps: Vec<FixtureStep>,
    #[serde(default)]
    expected_logs3_requests: Option<usize>,
}

#[derive(Debug, Deserialize)]
struct FixtureStep {
    command: Vec<String>,
    #[serde(default)]
    stdin_file: Option<String>,
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

fn read_fixture_config(path: &Path) -> FixtureConfig {
    let raw = fs::read_to_string(path).expect("read fixture.json");
    serde_json::from_str(&raw).expect("parse fixture.json")
}

fn sanitize_dataset_env(cmd: &mut Command) {
    for (key, _) in std::env::vars_os() {
        if key
            .to_str()
            .is_some_and(|name| name.starts_with("BRAINTRUST_") || name.starts_with("BT_DATASETS_"))
        {
            cmd.env_remove(&key);
        }
    }
}

fn expand_fixture_value(value: &str, mock_server_url: &str) -> String {
    value.replace("__MOCK_SERVER_URL__", mock_server_url)
}

#[derive(Debug, Clone)]
struct MockProject {
    id: String,
    name: String,
    org_id: String,
}

#[derive(Debug, Clone)]
struct MockDataset {
    id: String,
    name: String,
    project_id: String,
    created: String,
}

#[derive(Debug, Clone)]
struct MockDatasetVersion {
    id: String,
    dataset_id: String,
    name: String,
    description: Option<String>,
    xact_id: String,
    created: String,
}

#[derive(Debug)]
struct MockServerState {
    requests: Mutex<Vec<String>>,
    projects: Mutex<Vec<MockProject>>,
    datasets: Mutex<Vec<MockDataset>>,
    dataset_versions: Mutex<Vec<MockDatasetVersion>>,
    dataset_rows: Mutex<BTreeMap<String, BTreeMap<String, Map<String, Value>>>>,
    next_xact_id: Mutex<u64>,
}

impl MockServerState {
    fn seeded() -> Self {
        Self {
            requests: Mutex::new(Vec::new()),
            projects: Mutex::new(vec![MockProject {
                id: "proj_fixture".to_string(),
                name: "fixtures-project".to_string(),
                org_id: "org_mock".to_string(),
            }]),
            datasets: Mutex::new(Vec::new()),
            dataset_versions: Mutex::new(Vec::new()),
            dataset_rows: Mutex::new(BTreeMap::new()),
            next_xact_id: Mutex::new(1_000_192_656_880_881_099),
        }
    }
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
        let data = web::Data::new(state);

        let server = HttpServer::new(move || {
            App::new()
                .app_data(data.clone())
                .route("/api/apikey/login", web::post().to(mock_login))
                .route("/v1/project", web::get().to(mock_list_projects))
                .route("/v1/dataset", web::get().to(mock_list_datasets))
                .route("/v1/dataset", web::post().to(mock_create_dataset))
                .route(
                    "/v1/dataset/{dataset_id}/restore/preview",
                    web::post().to(mock_restore_dataset_preview),
                )
                .route(
                    "/v1/dataset/{dataset_id}/restore",
                    web::post().to(mock_restore_dataset),
                )
                .route(
                    "/v1/dataset_snapshot",
                    web::get().to(mock_list_dataset_versions),
                )
                .route(
                    "/v1/dataset_snapshot",
                    web::post().to(mock_create_dataset_version),
                )
                .route("/btql", web::post().to(mock_btql))
                .route("/version", web::get().to(mock_version))
                .route("/logs3", web::post().to(mock_logs3))
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
    log_request(state.get_ref(), &req);
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
    log_request(state.get_ref(), &req);
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

async fn mock_list_datasets(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
) -> HttpResponse {
    log_request(state.get_ref(), &req);
    let query = parse_query(req.query_string());
    let requested_project_id = query.get("project_id").cloned();
    let datasets = state.datasets.lock().expect("datasets lock").clone();
    let objects = datasets
        .into_iter()
        .filter(|dataset| {
            requested_project_id
                .as_deref()
                .is_none_or(|project_id| dataset.project_id == project_id)
        })
        .map(|dataset| {
            serde_json::json!({
                "id": dataset.id,
                "name": dataset.name,
                "project_id": dataset.project_id,
                "created": dataset.created
            })
        })
        .collect::<Vec<_>>();
    HttpResponse::Ok().json(serde_json::json!({ "objects": objects }))
}

#[derive(Debug, Deserialize)]
struct CreateDatasetRequest {
    name: String,
    project_id: String,
}

async fn mock_create_dataset(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
    body: web::Json<CreateDatasetRequest>,
) -> HttpResponse {
    log_request(state.get_ref(), &req);
    let mut datasets = state.datasets.lock().expect("datasets lock");
    if let Some(existing) = datasets
        .iter()
        .find(|dataset| dataset.project_id == body.project_id && dataset.name == body.name)
    {
        return HttpResponse::Ok().json(serde_json::json!({
            "id": existing.id,
            "name": existing.name,
            "project_id": existing.project_id,
            "created": existing.created
        }));
    }

    let created = MockDataset {
        id: format!("dataset_{}", datasets.len() + 1),
        name: body.name.clone(),
        project_id: body.project_id.clone(),
        created: "2026-01-01T00:00:00Z".to_string(),
    };
    datasets.push(created.clone());
    HttpResponse::Ok().json(serde_json::json!({
        "id": created.id,
        "name": created.name,
        "project_id": created.project_id,
        "created": created.created
    }))
}

async fn mock_list_dataset_versions(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
) -> HttpResponse {
    log_request(state.get_ref(), &req);
    let query = parse_query(req.query_string());
    let requested_dataset_id = query.get("dataset_id").cloned();
    let versions = state
        .dataset_versions
        .lock()
        .expect("dataset versions lock")
        .clone();
    let objects = versions
        .into_iter()
        .filter(|version| {
            requested_dataset_id
                .as_deref()
                .is_none_or(|dataset_id| version.dataset_id == dataset_id)
        })
        .map(|version| {
            serde_json::json!({
                "id": version.id,
                "dataset_id": version.dataset_id,
                "name": version.name,
                "description": version.description,
                "xact_id": version.xact_id,
                "created": version.created
            })
        })
        .collect::<Vec<_>>();
    HttpResponse::Ok().json(serde_json::json!({ "objects": objects }))
}

#[derive(Debug, Deserialize)]
struct CreateDatasetVersionRequest {
    dataset_id: String,
    name: String,
    #[serde(default)]
    description: Option<String>,
    xact_id: String,
}

async fn mock_create_dataset_version(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
    body: web::Json<CreateDatasetVersionRequest>,
) -> HttpResponse {
    log_request(state.get_ref(), &req);
    let mut versions = state
        .dataset_versions
        .lock()
        .expect("dataset versions lock");
    let created = MockDatasetVersion {
        id: format!("snapshot_{}", versions.len() + 1),
        dataset_id: body.dataset_id.clone(),
        name: body.name.clone(),
        description: body.description.clone(),
        xact_id: body.xact_id.clone(),
        created: "2026-01-02T00:00:00Z".to_string(),
    };
    versions.push(created.clone());
    HttpResponse::Ok().json(serde_json::json!({
        "id": created.id,
        "dataset_id": created.dataset_id,
        "name": created.name,
        "description": created.description,
        "xact_id": created.xact_id,
        "created": created.created
    }))
}

#[derive(Debug, Deserialize)]
struct RestoreDatasetRequest {
    version: String,
}

async fn mock_restore_dataset_preview(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
    dataset_id: web::Path<String>,
    _body: web::Json<RestoreDatasetRequest>,
) -> HttpResponse {
    log_request(state.get_ref(), &req);
    let dataset_id = dataset_id.into_inner();
    let current_row_count = state
        .dataset_rows
        .lock()
        .expect("dataset rows lock")
        .get(&dataset_id)
        .map(|rows| rows.len())
        .unwrap_or_default();

    HttpResponse::Ok().json(serde_json::json!({
        "rows_to_restore": current_row_count,
        "rows_to_delete": 0
    }))
}

async fn mock_restore_dataset(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
    _dataset_id: web::Path<String>,
    body: web::Json<RestoreDatasetRequest>,
) -> HttpResponse {
    log_request(state.get_ref(), &req);
    HttpResponse::Ok().json(serde_json::json!({
        "xact_id": body.version,
        "rows_restored": 1,
        "rows_deleted": 0
    }))
}

#[derive(Debug, Deserialize)]
struct BtqlRequest {
    query: String,
}

async fn mock_btql(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
    body: web::Json<BtqlRequest>,
) -> HttpResponse {
    log_request(state.get_ref(), &req);
    if !body.query.contains("filter: created >=") {
        return HttpResponse::BadRequest().body("BTQL query must include a timestamp filter");
    }

    let Some(dataset_id) = extract_dataset_id_from_query(&body.query) else {
        return HttpResponse::BadRequest().body("missing dataset(...) source in BTQL query");
    };

    let rows = state
        .dataset_rows
        .lock()
        .expect("dataset rows lock")
        .get(&dataset_id)
        .map(|rows| rows.values().cloned().collect::<Vec<_>>())
        .unwrap_or_default();

    HttpResponse::Ok().json(serde_json::json!({
        "data": rows,
        "cursor": null,
    }))
}

async fn mock_version(state: web::Data<Arc<MockServerState>>, req: HttpRequest) -> HttpResponse {
    log_request(state.get_ref(), &req);
    HttpResponse::Ok().json(serde_json::json!({}))
}

async fn mock_logs3(
    state: web::Data<Arc<MockServerState>>,
    req: HttpRequest,
    body: web::Bytes,
) -> HttpResponse {
    log_request(state.get_ref(), &req);

    let payload: Value = match serde_json::from_slice(&body) {
        Ok(payload) => payload,
        Err(err) => {
            return HttpResponse::BadRequest().body(format!("invalid logs3 body: {err}"));
        }
    };

    let Some(rows) = payload.get("rows").and_then(Value::as_array) else {
        return HttpResponse::BadRequest().body("logs3 request body must contain a rows array");
    };

    let mut dataset_rows = state.dataset_rows.lock().expect("dataset rows lock");
    let mut next_xact_id = state.next_xact_id.lock().expect("next xact id lock");
    for row in rows {
        let Some(object) = row.as_object() else {
            return HttpResponse::BadRequest().body("logs3 rows must be objects");
        };
        let Some(dataset_id) = object.get("dataset_id").and_then(Value::as_str) else {
            return HttpResponse::BadRequest().body("logs3 rows must include dataset_id");
        };
        let Some(row_id) = object.get("id").and_then(Value::as_str) else {
            return HttpResponse::BadRequest().body("logs3 rows must include id");
        };

        let rows_for_dataset = dataset_rows.entry(dataset_id.to_string()).or_default();
        if object
            .get("_object_delete")
            .and_then(Value::as_bool)
            .unwrap_or(false)
        {
            rows_for_dataset.remove(row_id);
        } else {
            let mut stored = object.clone();
            if !stored.contains_key("_xact_id") {
                stored.insert(
                    "_xact_id".to_string(),
                    Value::String(next_xact_id.to_string()),
                );
                *next_xact_id += 1;
            }
            rows_for_dataset.insert(row_id.to_string(), stored);
        }
    }

    HttpResponse::Ok().json(serde_json::json!({}))
}

fn extract_dataset_id_from_query(query: &str) -> Option<String> {
    let marker = "from: dataset('";
    let start = query.find(marker)? + marker.len();
    let rest = &query[start..];
    let end = rest.find("')")?;
    Some(rest[..end].replace("''", "'"))
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

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn datasets_fixtures() {
    let root = repo_root();
    let fixtures_root = root.join("tests").join("datasets-fixtures");
    if !fixtures_root.exists() {
        eprintln!("No dataset fixtures found.");
        return;
    }

    let bt_path = bt_binary_path();
    let mut fixture_dirs: Vec<PathBuf> = fs::read_dir(&fixtures_root)
        .expect("read datasets fixture root")
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
        if config.steps.is_empty() {
            panic!("Fixture {fixture_name} has no configured steps.");
        }

        let state = Arc::new(MockServerState::seeded());
        let server = MockServer::start(Arc::clone(&state)).await;

        for (index, step) in config.steps.iter().enumerate() {
            if step.command.is_empty() {
                panic!(
                    "Fixture {fixture_name} step {} has an empty command.",
                    index + 1
                );
            }

            let mut cmd = Command::new(&bt_path);
            cmd.args(&step.command).current_dir(&dir);
            sanitize_dataset_env(&mut cmd);
            for (key, value) in &config.env {
                cmd.env(key, expand_fixture_value(value, &server.base_url));
            }
            if let Some(stdin_file) = &step.stdin_file {
                let stdin_path = dir.join(stdin_file);
                let stdin = File::open(&stdin_path).unwrap_or_else(|err| {
                    panic!(
                        "failed to open fixture {fixture_name} step {} stdin file {}: {err}",
                        index + 1,
                        stdin_path.display(),
                    )
                });
                cmd.stdin(Stdio::from(stdin));
            }

            let output = cmd.output().unwrap_or_else(|err| {
                panic!(
                    "failed to run fixture {fixture_name} step {} {:?}: {err}",
                    index + 1,
                    step.command,
                )
            });
            let stdout = String::from_utf8_lossy(&output.stdout);
            let stderr = String::from_utf8_lossy(&output.stderr);
            if output.status.success() != step.expect_success {
                panic!(
                    "Fixture {fixture_name} step {} command {:?} had status {} (expected success={})\nstdout:\n{}\nstderr:\n{}",
                    index + 1,
                    step.command,
                    output.status,
                    step.expect_success,
                    stdout,
                    stderr
                );
            }

            for expected in &step.stdout_contains {
                assert!(
                    stdout.contains(expected),
                    "Fixture {fixture_name} step {}: stdout missing expected text: {expected}\nstdout:\n{stdout}",
                    index + 1,
                );
            }
            for expected in &step.stderr_contains {
                assert!(
                    stderr.contains(expected),
                    "Fixture {fixture_name} step {}: stderr missing expected text: {expected}\nstderr:\n{stderr}",
                    index + 1,
                );
            }
            for unexpected in &step.stdout_not_contains {
                assert!(
                    !stdout.contains(unexpected),
                    "Fixture {fixture_name} step {}: stdout unexpectedly contained text: {unexpected}\nstdout:\n{stdout}",
                    index + 1,
                );
            }
            for unexpected in &step.stderr_not_contains {
                assert!(
                    !stderr.contains(unexpected),
                    "Fixture {fixture_name} step {}: stderr unexpectedly contained text: {unexpected}\nstderr:\n{stderr}",
                    index + 1,
                );
            }
        }

        if let Some(expected_logs3_requests) = config.expected_logs3_requests {
            let actual_logs3_requests = state
                .requests
                .lock()
                .expect("requests lock")
                .iter()
                .filter(|request| request.as_str() == "/logs3")
                .count();
            assert_eq!(
                actual_logs3_requests, expected_logs3_requests,
                "Fixture {fixture_name}: expected {expected_logs3_requests} /logs3 requests, saw {actual_logs3_requests}"
            );
        }

        server.stop().await;
    }

    if !ran_any {
        eprintln!("No datasets fixtures with fixture.json found.");
    }
}
