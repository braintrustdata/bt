use std::time::Duration;

use anyhow::{bail, Result};
use dialoguer::Input;

use crate::http::ApiClient;
use crate::ui::{
    is_interactive, print_command_status, with_spinner, with_spinner_visible, CommandStatus,
};

use super::api;

pub(crate) enum CreateProjectOutcome {
    Created(api::Project),
    Existing(api::Project),
}

pub(crate) async fn create_project_checked(
    client: &ApiClient,
    name: &str,
) -> Result<CreateProjectOutcome> {
    let exists = with_spinner(
        "Checking project...",
        api::get_project_by_name(client, name),
    )
    .await?;
    if let Some(project) = exists {
        return Ok(CreateProjectOutcome::Existing(project));
    }

    match with_spinner_visible(
        "Creating project...",
        api::create_project(client, name),
        Duration::from_millis(300),
    )
    .await
    {
        Ok(project) => Ok(CreateProjectOutcome::Created(project)),
        Err(err) => {
            if let Some(project) = with_spinner(
                "Checking project...",
                api::get_project_by_name(client, name),
            )
            .await?
            {
                Ok(CreateProjectOutcome::Existing(project))
            } else {
                Err(err)
            }
        }
    }
}

pub async fn run(client: &ApiClient, name: Option<&str>) -> Result<()> {
    let name = match name {
        Some(n) if !n.is_empty() => n.to_string(),
        _ => {
            if !is_interactive() {
                bail!("project name required. Use: bt projects create <name>");
            }
            Input::new().with_prompt("Project name").interact_text()?
        }
    };

    match create_project_checked(client, &name).await {
        Ok(CreateProjectOutcome::Created(_)) => {
            print_command_status(
                CommandStatus::Success,
                &format!("Successfully created '{name}'"),
            );
            Ok(())
        }
        Ok(CreateProjectOutcome::Existing(_)) => {
            print_command_status(CommandStatus::Error, &format!("Failed to create '{name}'"));
            bail!("project '{name}' already exists")
        }
        Err(e) => {
            print_command_status(CommandStatus::Error, &format!("Failed to create '{name}'"));
            Err(e)
        }
    }
}

#[cfg(test)]
mod tests {
    use std::net::TcpListener;
    use std::sync::{Arc, Mutex};

    use actix_web::{web, App, HttpResponse, HttpServer};
    use braintrust_sdk_rust::LoginState;
    use serde::Deserialize;

    use super::*;
    use crate::auth::LoginContext;

    #[derive(Clone)]
    struct MockProject {
        id: String,
        name: String,
        org_id: String,
    }

    impl MockProject {
        fn new(id: &str, name: &str) -> Self {
            Self {
                id: id.to_string(),
                name: name.to_string(),
                org_id: "test-org".to_string(),
            }
        }
    }

    #[derive(Clone, Copy)]
    enum CreateBehavior {
        Create,
        ConflictThenReveal,
    }

    struct MockState {
        projects: Mutex<Vec<MockProject>>,
        create_behavior: CreateBehavior,
    }

    struct MockServer {
        base_url: String,
        handle: actix_web::dev::ServerHandle,
    }

    impl MockServer {
        async fn start(state: Arc<MockState>) -> Self {
            let listener = TcpListener::bind(("127.0.0.1", 0)).expect("bind mock server");
            let addr = listener.local_addr().expect("mock server addr");
            let base_url = format!("http://{addr}");
            let data = web::Data::new(state);

            let server = HttpServer::new(move || {
                App::new()
                    .app_data(data.clone())
                    .route("/v1/project", web::get().to(mock_list_projects))
                    .route("/v1/project", web::post().to(mock_create_project))
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

    async fn setup_test(
        projects: Vec<MockProject>,
        create_behavior: CreateBehavior,
    ) -> (MockServer, ApiClient) {
        let state = Arc::new(MockState {
            projects: Mutex::new(projects),
            create_behavior,
        });
        let server = MockServer::start(state).await;
        let client = ApiClient::new(&LoginContext {
            login: LoginState {
                api_key: "test-key".to_string(),
                org_id: "org-1".to_string(),
                org_name: "test-org".to_string(),
                api_url: Some(server.base_url.clone()),
            },
            api_url: server.base_url.clone(),
            app_url: "https://app.example.com".to_string(),
        })
        .expect("build client");
        (server, client)
    }

    #[derive(Deserialize)]
    struct ListProjectsQuery {
        project_name: Option<String>,
    }

    async fn mock_list_projects(
        state: web::Data<Arc<MockState>>,
        query: web::Query<ListProjectsQuery>,
    ) -> HttpResponse {
        let projects = state.projects.lock().expect("projects lock").clone();
        let objects: Vec<_> = projects
            .into_iter()
            .filter(|p| query.project_name.as_deref().is_none_or(|n| p.name == n))
            .map(|p| serde_json::json!({ "id": p.id, "name": p.name, "org_id": p.org_id }))
            .collect();
        HttpResponse::Ok().json(serde_json::json!({ "objects": objects }))
    }

    #[derive(Deserialize)]
    struct CreateProjectRequest {
        name: String,
    }

    async fn mock_create_project(
        state: web::Data<Arc<MockState>>,
        body: web::Json<CreateProjectRequest>,
    ) -> HttpResponse {
        match state.create_behavior {
            CreateBehavior::Create => {
                let mut projects = state.projects.lock().expect("projects lock");
                let id = format!("proj-created-{}", projects.len() + 1);
                let created = MockProject::new(&id, &body.name);
                projects.push(created.clone());
                HttpResponse::Ok().json(
                    serde_json::json!({ "id": created.id, "name": created.name, "org_id": created.org_id }),
                )
            }
            CreateBehavior::ConflictThenReveal => {
                state
                    .projects
                    .lock()
                    .expect("projects lock")
                    .push(MockProject::new("proj-race", &body.name));
                HttpResponse::Conflict()
                    .json(serde_json::json!({ "error": "project already exists" }))
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_project_checked_returns_existing_project() {
        let (server, client) = setup_test(
            vec![MockProject::new("proj-1", "existing-project")],
            CreateBehavior::Create,
        )
        .await;

        let outcome = create_project_checked(&client, "existing-project")
            .await
            .expect("reuse existing project");

        let CreateProjectOutcome::Existing(project) = outcome else {
            panic!("expected existing project outcome");
        };
        assert_eq!(project.id, "proj-1");
        assert_eq!(project.name, "existing-project");

        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_project_checked_falls_back_to_lookup_after_create_conflict() {
        let (server, client) = setup_test(vec![], CreateBehavior::ConflictThenReveal).await;

        let outcome = create_project_checked(&client, "race-project")
            .await
            .expect("resolve project after create conflict");

        let CreateProjectOutcome::Existing(project) = outcome else {
            panic!("expected existing project outcome after create conflict");
        };
        assert_eq!(project.id, "proj-race");
        assert_eq!(project.name, "race-project");

        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn create_project_checked_returns_created_project() {
        let (server, client) = setup_test(vec![], CreateBehavior::Create).await;

        let outcome = create_project_checked(&client, "new-project")
            .await
            .expect("create new project");

        let CreateProjectOutcome::Created(project) = outcome else {
            panic!("expected created project outcome");
        };
        assert_eq!(project.id, "proj-created-1");
        assert_eq!(project.name, "new-project");

        server.stop().await;
    }
}
