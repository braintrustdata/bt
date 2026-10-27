mod common;

use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
use serde_json::{json, Value};
use std::net::TcpListener;
use std::path::Path;
use std::process::{Command, Output};
use std::sync::{Arc, Mutex};

const PROJECT: &str = "00000000-0000-4000-8000-000000000001"; // synthetic

#[derive(Default)]
struct State {
    queries: Mutex<Vec<Value>>,
    uploads: Mutex<Vec<Value>>,
    fail_next: Mutex<bool>,
    fail_query_at: Mutex<Option<usize>>,
}

async fn login(req: HttpRequest) -> HttpResponse {
    let base = format!("http://{}", req.connection_info().host());
    HttpResponse::Ok()
        .json(json!({"org_info": [{"id": "test-org-id", "name": "test-org", "api_url": base}]}))
}

async fn projects() -> HttpResponse {
    HttpResponse::Ok().json(
        json!({"objects": [{"id": PROJECT, "name": "test-project", "org_id": "test-org-id"}]}),
    )
}

async fn query(state: web::Data<Arc<State>>, body: web::Json<Value>) -> HttpResponse {
    let mut queries = state.queries.lock().unwrap();
    queries.push(body.into_inner());
    if *state.fail_query_at.lock().unwrap() == Some(queries.len()) {
        return HttpResponse::BadRequest().body("test query failure");
    }
    let query = queries.last().unwrap()["query"].as_str().unwrap();
    if query.contains("test-cursor") {
        HttpResponse::Ok().json(json!({"data": [{"id": "test-row-2", "span_id": "test-span-2", "root_span_id": "test-root-2"}]}))
    } else {
        HttpResponse::Ok().json(json!({"data": [{"id": "test-row-1", "span_id": "test-span-1", "root_span_id": "test-root-1"}], "cursor": "test-cursor"}))
    }
}

async fn upload(state: web::Data<Arc<State>>, body: web::Json<Value>) -> HttpResponse {
    state.uploads.lock().unwrap().push(body.into_inner());
    if std::mem::take(&mut *state.fail_next.lock().unwrap()) {
        return HttpResponse::BadRequest().body("test upload failure");
    }
    HttpResponse::Ok().json(json!({}))
}

struct Server {
    url: String,
    state: Arc<State>,
    handle: actix_web::dev::ServerHandle,
}

impl Server {
    async fn start() -> Self {
        let state = Arc::new(State::default());
        let data = web::Data::new(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = HttpServer::new(move || {
            App::new()
                .app_data(data.clone())
                .route("/api/apikey/login", web::post().to(login))
                .route("/v1/project", web::get().to(projects))
                .route("/btql", web::post().to(query))
                .route("/logs3", web::post().to(upload))
                .route(
                    "/version",
                    web::get().to(|| async { HttpResponse::Ok().json(json!({})) }),
                )
        })
        .workers(1)
        .listen(listener)
        .unwrap()
        .run();
        let handle = server.handle();
        tokio::spawn(server);
        Self { url, state, handle }
    }

    async fn run(&self, root: &Path, args: &[&str]) -> Output {
        let mut cmd = Command::new(common::bt_binary_path(root));
        // Isolate credentials/config and keep every request on the mock server.
        for (key, _) in std::env::vars_os() {
            if key.to_string_lossy().starts_with("BRAINTRUST_") {
                cmd.env_remove(key);
            }
        }
        cmd.env("HOME", root)
            .env("XDG_CONFIG_HOME", root)
            .env("BRAINTRUST_API_KEY", "test-key")
            .env("BRAINTRUST_APP_URL", &self.url)
            .env("BRAINTRUST_API_URL", &self.url)
            .args(["sync", "--json"])
            .args(args)
            .arg("--root")
            .arg(root.join("sync"));
        tokio::task::spawn_blocking(move || cmd.output().unwrap())
            .await
            .unwrap()
    }
}

fn success(output: &Output) -> Value {
    assert!(
        output.status.success(),
        "stdout: {}\nstderr: {}",
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("sync JSON output")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pull_paginates_and_completed_run_does_not_fetch_again() {
    let server = Server::start().await;
    let root = tempfile::tempdir().unwrap();
    let object = format!("project_logs:{PROJECT}");
    let args = [
        "pull",
        &object,
        "--spans",
        "2",
        "--page-size",
        "1",
        "--window",
        "1h",
    ];
    let result = success(&server.run(root.path(), &args).await);
    assert_eq!(server.state.queries.lock().unwrap().len(), 2);
    let spec_dir = result["spec_dir"].as_str().unwrap();
    let state: Value =
        serde_json::from_slice(&std::fs::read(Path::new(spec_dir).join("state.json")).unwrap())
            .unwrap();
    assert_eq!(state["items_done"], 2);
    assert_eq!(state["status"], "completed");
    success(&server.run(root.path(), &args).await);
    assert_eq!(server.state.queries.lock().unwrap().len(), 2);
    server.handle.stop(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pull_failure_can_be_resumed() {
    let server = Server::start().await;
    *server.state.fail_query_at.lock().unwrap() = Some(2);
    let root = tempfile::tempdir().unwrap();
    let object = format!("project_logs:{PROJECT}");
    let args = [
        "pull",
        &object,
        "--spans",
        "2",
        "--page-size",
        "1",
        "--window",
        "1h",
    ];
    let failed = server.run(root.path(), &args).await;
    assert!(!failed.status.success());
    assert_eq!(server.state.queries.lock().unwrap().len(), 2);
    success(&server.run(root.path(), &args).await);
    let queries = server.state.queries.lock().unwrap().clone();
    assert!(queries[2]["query"]
        .as_str()
        .unwrap()
        .contains("test-cursor"));
    assert_eq!(server.state.queries.lock().unwrap().len(), 3);
    server.handle.stop(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn push_failure_does_not_checkpoint_unacknowledged_rows() {
    let server = Server::start().await;
    *server.state.fail_next.lock().unwrap() = true;
    let root = tempfile::tempdir().unwrap();
    let input = root.path().join("rows.jsonl");
    std::fs::write(
        &input,
        "{\"id\":\"test-row\",\"span_id\":\"test-span\",\"root_span_id\":\"test-root\"}\n",
    )
    .unwrap();
    let object = format!("project_logs:{PROJECT}");
    let args = [
        "push",
        &object,
        "--in",
        input.to_str().unwrap(),
        "--spans",
        "1",
    ];
    assert!(!server.run(root.path(), &args).await.status.success());
    success(&server.run(root.path(), &args).await);
    let uploads = server.state.uploads.lock().unwrap().clone();
    assert_eq!(uploads.len(), 2);
    assert_eq!(uploads[0]["rows"], uploads[1]["rows"]);
    success(&server.run(root.path(), &args).await);
    assert_eq!(server.state.uploads.lock().unwrap().len(), 2);
    server.handle.stop(true).await;
}
