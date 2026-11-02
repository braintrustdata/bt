use anyhow::Result;
use serde::{Deserialize, Serialize};

use crate::http::ApiClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Environment {
    pub id: String,
    pub org_id: String,
    pub name: String,
    pub slug: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub created: Option<String>,
    #[serde(default)]
    pub deleted_at: Option<String>,
}

#[derive(Serialize)]
pub struct CreateEnvironment<'a> {
    pub name: &'a str,
    pub slug: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<&'a str>,
    pub org_name: &'a str,
}

#[derive(Serialize)]
pub struct UpdateEnvironment<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slug: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Option<&'a str>>,
}

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
    crate::http::from_api(response.objects)
}

pub async fn get_by_slug(client: &ApiClient, slug: &str) -> Result<Option<Environment>> {
    Ok(list(client)
        .await?
        .into_iter()
        .find(|environment| environment.slug == slug))
}

pub async fn create(client: &ApiClient, body: &CreateEnvironment<'_>) -> Result<Environment> {
    let request: braintrust_api_client::types::CreateEnvironment = crate::http::into_api(body)?;
    crate::http::from_api(
        client
            .run(|| client.generated().create_environment(Some(request.clone())))
            .await?,
    )
}

pub async fn update(
    client: &ApiClient,
    id: &str,
    body: &UpdateEnvironment<'_>,
) -> Result<Environment> {
    let request: braintrust_api_client::types::PatchEnvironment = crate::http::into_api(body)?;
    crate::http::from_api(
        client
            .run(|| {
                client
                    .generated()
                    .update_environment(id, Some(request.clone()))
            })
            .await?,
    )
}

pub async fn delete(client: &ApiClient, id: &str) -> Result<Environment> {
    crate::http::from_api(
        client
            .run(|| client.generated().delete_environment(id))
            .await?,
    )
}
