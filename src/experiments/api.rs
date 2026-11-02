use std::collections::HashMap;

use anyhow::Result;
use serde::{Deserialize, Serialize};

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

pub async fn list_experiments(client: &ApiClient, project: &str) -> Result<Vec<Experiment>> {
    let response = client
        .run(|| {
            client
                .generated()
                .get_experiment_builder()
                .org_name(client.org_name())
                .project_name(project)
                .send()
        })
        .await?;
    crate::http::from_api(response.objects)
}

pub async fn list_experiments_by_project_id(
    client: &ApiClient,
    project_id: &str,
) -> Result<Vec<Experiment>> {
    let response = client
        .run(|| {
            client
                .generated()
                .get_experiment_builder()
                .org_name(client.org_name())
                .project_id(project_id)
                .send()
        })
        .await?;
    crate::http::from_api(response.objects)
}

pub async fn get_experiment_by_name(
    client: &ApiClient,
    project: &str,
    name: &str,
) -> Result<Option<Experiment>> {
    let response = client
        .run(|| {
            client
                .generated()
                .get_experiment_builder()
                .org_name(client.org_name())
                .project_name(project)
                .experiment_name(name)
                .send()
        })
        .await?;
    crate::http::from_api(response.objects.into_iter().next())
}

pub async fn delete_experiment(client: &ApiClient, experiment_id: &str) -> Result<()> {
    client
        .run(|| client.generated().delete_experiment_id(experiment_id))
        .await?;
    Ok(())
}

pub async fn create_experiment(
    client: &ApiClient,
    project_id: &str,
    name: &str,
) -> Result<Experiment> {
    let mut request = braintrust_api_client::types::CreateExperiment::new(project_id.parse()?);
    request.name = Some(Some(name.to_owned()));
    crate::http::from_api(
        client
            .run(|| client.generated().post_experiment(request.clone()))
            .await?,
    )
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
    let result = client
        .run(|| {
            let mut request = client
                .generated()
                .get_experiment_id_summarize_builder(experiment_id)
                .summarize_scores("true");
            if let Some(id) = comparison_experiment_id {
                request = request.comparison_experiment_id(id);
            }
            request.send()
        })
        .await?;
    crate::http::from_api(result)
}
