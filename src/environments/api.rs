use anyhow::Result;
use serde::{Deserialize, Serialize};
use urlencoding::encode;

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

#[derive(Deserialize)]
struct ListResponse {
    objects: Vec<Environment>,
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
    let response: ListResponse = client
        .get(&format!(
            "/environment?org_name={}",
            encode(client.org_name())
        ))
        .await?;
    Ok(response.objects)
}

pub async fn get_by_slug(client: &ApiClient, slug: &str) -> Result<Option<Environment>> {
    Ok(list(client)
        .await?
        .into_iter()
        .find(|environment| environment.slug == slug))
}

pub async fn create(client: &ApiClient, body: &CreateEnvironment<'_>) -> Result<Environment> {
    client.post("/environment", body).await
}

pub async fn update(
    client: &ApiClient,
    id: &str,
    body: &UpdateEnvironment<'_>,
) -> Result<Environment> {
    client
        .patch(&format!("/environment/{}", encode(id)), body)
        .await
}

pub async fn delete(client: &ApiClient, id: &str) -> Result<Environment> {
    client
        .delete_with_response(&format!("/environment/{}", encode(id)))
        .await
}
