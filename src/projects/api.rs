use anyhow::Result;
use serde::{Deserialize, Serialize};
use urlencoding::encode;

use crate::http::{ApiClient, HttpOptions, NoContent};

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
        .with_options(HttpOptions {
            query: vec![("org_name".into(), client.org_name().into())],
            ..Default::default()
        })
        .get("/v1/project")
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
    client.post("/v1/project", &body).await
}

pub async fn delete_project(client: &ApiClient, project_id: &str) -> Result<()> {
    let path = format!("/v1/project/{}", encode(project_id));
    let _: NoContent = client.delete(&path).await?;
    Ok(())
}

pub async fn get_project_by_name(client: &ApiClient, name: &str) -> Result<Option<Project>> {
    let list: ListResponse = client
        .with_options(HttpOptions {
            query: vec![
                ("org_name".into(), client.org_name().into()),
                ("project_name".into(), name.into()),
            ],
            ..Default::default()
        })
        .get("/v1/project")
        .await?;
    Ok(list.objects.into_iter().next())
}
