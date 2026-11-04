use crate::http::ApiClient;
use anyhow::Result;
pub use braintrust_api_client::types::{Experiment, MetricSummary, ScoreSummary};

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
    Ok(response.objects)
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
    Ok(response.objects)
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
    Ok(response.objects.into_iter().next())
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
    client
        .run(|| client.generated().post_experiment(request.clone()))
        .await
}

pub use braintrust_api_client::types::SummarizeExperimentResponse as ExperimentSummary;

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
    Ok(result)
}
