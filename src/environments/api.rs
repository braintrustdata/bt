use crate::http::ApiClient;
use anyhow::Result;
pub use braintrust_api_client::types::{
    CreateEnvironment, Environment, PatchEnvironment as UpdateEnvironment,
};

pub async fn list(client: &ApiClient) -> Result<Vec<Environment>> {
    let response = client
        .run(|| {
            client
                .generated()
                .list_environments_builder()
                .org_name(client.org_name())
                .send()
        })
        .await?;
    Ok(response.objects)
}

pub async fn get_by_slug(client: &ApiClient, slug: &str) -> Result<Option<Environment>> {
    Ok(list(client)
        .await?
        .into_iter()
        .find(|environment| environment.slug == slug))
}

pub async fn create(client: &ApiClient, body: &CreateEnvironment) -> Result<Environment> {
    let request = body;
    client
        .run(|| client.generated().create_environment(Some(request.clone())))
        .await
}

pub async fn update(client: &ApiClient, id: &str, body: &UpdateEnvironment) -> Result<Environment> {
    let request = body;
    client
        .run(|| {
            client
                .generated()
                .update_environment(id, Some(request.clone()))
        })
        .await
}

pub async fn delete(client: &ApiClient, id: &str) -> Result<Environment> {
    client
        .run(|| client.generated().delete_environment(id))
        .await
}
