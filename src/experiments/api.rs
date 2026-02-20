use anyhow::Result;
use serde::{Deserialize, Serialize};
use urlencoding::encode;

use crate::http::ApiClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Experiment {
    pub id: String,
    pub name: String,
    pub project_id: String,
    #[serde(default)]
    pub public: bool,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub created: Option<String>,
    #[serde(default)]
    pub dataset_id: Option<String>,
    #[serde(default)]
    pub dataset_version: Option<String>,
    #[serde(default)]
    pub base_exp_id: Option<String>,
    #[serde(default)]
    pub commit: Option<String>,
    #[serde(default)]
    pub user_id: Option<String>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    objects: Vec<Experiment>,
}

pub async fn list_experiments(client: &ApiClient, project: &str) -> Result<Vec<Experiment>> {
    let path = format!(
        "/v1/experiment?org_name={}&project_name={}",
        encode(client.org_name()),
        encode(project)
    );
    let list: ListResponse = client.get(&path).await?;
    Ok(list.objects)
}

pub async fn get_experiment_by_name(
    client: &ApiClient,
    project: &str,
    name: &str,
) -> Result<Option<Experiment>> {
    let path = format!(
        "/v1/experiment?org_name={}&project_name={}&experiment_name={}",
        encode(client.org_name()),
        encode(project),
        encode(name)
    );
    let list: ListResponse = client.get(&path).await?;
    Ok(list.objects.into_iter().next())
}

pub async fn delete_experiment(client: &ApiClient, experiment_id: &str) -> Result<()> {
    let path = format!("/v1/experiment/{}", encode(experiment_id));
    client.delete(&path).await
}
