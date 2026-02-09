use anyhow::Result;
use serde::{Deserialize, Serialize};
use urlencoding::encode;

use crate::http::ApiClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Project {
    pub id: String,
    pub name: String,
    pub org_id: String,
    #[serde(default)]
    pub description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    objects: Vec<Project>,
}

pub async fn list_projects(client: &ApiClient) -> Result<Vec<Project>> {
    let path = if client.org_name().is_empty() {
        // OAuth2 tokens don't need org_name filtering
        "/v1/project".to_string()
    } else {
        // API keys need org_name for proper filtering
        format!("/v1/project?org_name={}", encode(client.org_name()))
    };
    let list: ListResponse = client.get(&path).await?;
    Ok(list.objects)
}

pub async fn create_project(client: &ApiClient, name: &str) -> Result<Project> {
    let body = if client.org_name().is_empty() {
        serde_json::json!({ "name": name })
    } else {
        serde_json::json!({ "name": name, "org_name": client.org_name() })
    };
    client.post("/v1/project", &body).await
}

pub async fn delete_project(client: &ApiClient, project_id: &str) -> Result<()> {
    let path = format!("/v1/project/{}", encode(project_id));
    client.delete(&path).await
}

pub async fn get_project_by_name(client: &ApiClient, name: &str) -> Result<Option<Project>> {
    let path = if client.org_name().is_empty() {
        format!("/v1/project?name={}", encode(name))
    } else {
        format!(
            "/v1/project?org_name={}&name={}",
            encode(client.org_name()),
            encode(name)
        )
    };
    let list: ListResponse = client.get(&path).await?;
    Ok(list.objects.into_iter().next())
}
