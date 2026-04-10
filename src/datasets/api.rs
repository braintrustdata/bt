use anyhow::Result;
use serde::{Deserialize, Serialize};
use urlencoding::encode;

use crate::http::ApiClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dataset {
    pub id: String,
    pub name: String,
    pub project_id: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub created: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    objects: Vec<Dataset>,
}

pub async fn list_datasets(client: &ApiClient, project: &str) -> Result<Vec<Dataset>> {
    let path = format!(
        "/v1/dataset?org_name={}&project_name={}",
        encode(client.org_name()),
        encode(project)
    );
    let list: ListResponse = client.get(&path).await?;
    Ok(list.objects)
}

pub async fn get_dataset_by_name(
    client: &ApiClient,
    project: &str,
    name: &str,
) -> Result<Option<Dataset>> {
    let path = format!(
        "/v1/dataset?org_name={}&project_name={}&dataset_name={}",
        encode(client.org_name()),
        encode(project),
        encode(name)
    );
    let list: ListResponse = client.get(&path).await?;
    Ok(list.objects.into_iter().next())
}

pub async fn create_dataset(
    client: &ApiClient,
    project_id: &str,
    name: &str,
    description: Option<&str>,
) -> Result<Dataset> {
    let mut body = serde_json::json!({
        "name": name,
        "project_id": project_id,
        "org_name": client.org_name(),
    });
    if let Some(desc) = description {
        body["description"] = serde_json::Value::String(desc.to_string());
    }
    client.post("/v1/dataset", &body).await
}

pub async fn delete_dataset(client: &ApiClient, dataset_id: &str) -> Result<()> {
    let path = format!("/v1/dataset/{}", encode(dataset_id));
    client.delete(&path).await
}
