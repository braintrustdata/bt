use crate::http::ApiClient;
use anyhow::Result;
pub use braintrust_api_client::types::Project;

pub async fn list_projects(client: &ApiClient) -> Result<Vec<Project>> {
    let response = client
        .run(|| {
            client
                .generated()
                .get_project_builder()
                .org_name(client.org_name())
                .send()
        })
        .await?;
    Ok(response.objects)
}

pub async fn create_project(
    client: &ApiClient,
    name: &str,
    project_group_name: Option<&str>,
) -> Result<Project> {
    let mut request = braintrust_api_client::types::CreateProject::new(name.to_owned());
    request.org_name = Some(Some(client.org_name().to_owned()));
    request.project_group_name = project_group_name.map(|value| Some(value.to_owned()));
    client
        .run(|| client.generated().post_project(request.clone()))
        .await
}

pub async fn delete_project(client: &ApiClient, project_id: &str) -> Result<()> {
    client
        .run(|| client.generated().delete_project_id(project_id))
        .await?;
    Ok(())
}

pub async fn get_project_by_name(client: &ApiClient, name: &str) -> Result<Option<Project>> {
    let response = client
        .run(|| {
            client
                .generated()
                .get_project_builder()
                .org_name(client.org_name())
                .project_name(name)
                .send()
        })
        .await?;
    Ok(response.objects.into_iter().next())
}
