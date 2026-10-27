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
    let list: ListResponse = client
        .get("/v1/project")
        .query(&[("org_name", client.org_name())])
        .send()
        .await?;
    Ok(list.objects)
}

pub async fn create_project(
    client: &ApiClient,
    name: &str,
    project_group_name: Option<&str>,
) -> Result<Project> {
    let mut body = serde_json::json!({ "name": name, "org_name": client.org_name() });
    if let Some(project_group_name) = project_group_name {
        body["project_group_name"] = project_group_name.into();
    }
    client.post("/v1/project").json(&body).send().await
}

pub async fn delete_project(client: &ApiClient, project_id: &str) -> Result<()> {
    let path = format!("/v1/project/{}", encode(project_id));
    client.delete(&path).discard().await
}

pub async fn get_project_by_name(client: &ApiClient, name: &str) -> Result<Option<Project>> {
    let list: ListResponse = client
        .get("/v1/project")
        .query(&[("org_name", client.org_name()), ("project_name", name)])
        .send()
        .await?;
    Ok(list.objects.into_iter().next())
}
