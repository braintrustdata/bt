use std::collections::HashMap;

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

pub async fn list_experiments_by_project_id(
    client: &ApiClient,
    project_id: &str,
) -> Result<Vec<Experiment>> {
    let path = format!(
        "/v1/experiment?org_name={}&project_id={}",
        encode(client.org_name()),
        encode(project_id)
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

pub async fn create_experiment(
    client: &ApiClient,
    project_id: &str,
    name: &str,
) -> Result<Experiment> {
    let body = serde_json::json!({
        "name": name,
        "project_id": project_id,
        "org_name": client.org_name(),
    });
    client.post("/v1/experiment", &body).await
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExperimentSummary {
    pub project_name: String,
    pub experiment_name: String,
    pub project_url: String,
    pub experiment_url: String,
    #[serde(default)]
    pub comparison_experiment_name: Option<String>,
    #[serde(default)]
    pub scores: Option<HashMap<String, ScoreSummary>>,
    #[serde(default)]
    pub metrics: Option<HashMap<String, MetricSummary>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScoreSummary {
    pub name: String,
    pub score: f64,
    #[serde(default)]
    pub diff: Option<f64>,
    #[serde(default)]
    pub improvements: usize,
    #[serde(default)]
    pub regressions: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricSummary {
    pub name: String,
    pub metric: f64,
    pub unit: String,
    #[serde(default)]
    pub diff: Option<f64>,
    #[serde(default)]
    pub improvements: usize,
    #[serde(default)]
    pub regressions: usize,
}

pub async fn summarize_experiment(
    client: &ApiClient,
    experiment_id: &str,
    comparison_experiment_id: Option<&str>,
) -> Result<ExperimentSummary> {
    let mut path = format!(
        "/v1/experiment/{}/summarize?summarize_scores=true",
        encode(experiment_id)
    );
    if let Some(comparison_experiment_id) = comparison_experiment_id {
        path.push_str("&comparison_experiment_id=");
        path.push_str(&encode(comparison_experiment_id));
    }

    client.get(&path).await
}
