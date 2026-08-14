use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use urlencoding::encode;

use crate::http::ApiClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Prompt {
    pub id: String,
    pub name: String,
    pub slug: String,
    pub project_id: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub prompt_data: Option<serde_json::Value>,
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    objects: Vec<Prompt>,
}

pub async fn list_prompts(client: &ApiClient, project: &str) -> Result<Vec<Prompt>> {
    let path = format!(
        "/v1/prompt?org_name={}&project_name={}",
        encode(client.org_name()),
        encode(project)
    );
    let list: ListResponse = client.get(&path).await?;

    Ok(list.objects)
}

pub async fn get_prompt_by_slug(
    client: &ApiClient,
    project: &str,
    slug: &str,
) -> Result<Option<Prompt>> {
    let path = format!(
        "/v1/prompt?org_name={}&project_name={}&slug={}",
        encode(client.org_name()),
        encode(project),
        encode(slug)
    );
    let list: ListResponse = client.get(&path).await?;
    Ok(list.objects.into_iter().next())
}

pub async fn delete_prompt(client: &ApiClient, prompt_id: &str) -> Result<()> {
    let path = format!("/v1/prompt/{}", encode(prompt_id));
    client.delete(&path).await
}

/// Partially update a prompt by id via `PATCH /v1/prompt/{id}`.
///
/// Top-level fields are patched, but object-valued fields such as `prompt_data`
/// are replaced wholesale. Callers updating `prompt_data` must materialize the
/// complete value before sending the request.
pub async fn patch_prompt(client: &ApiClient, prompt_id: &str, body: &Value) -> Result<Prompt> {
    let path = format!("/v1/prompt/{}", encode(prompt_id));
    client.patch(&path, body).await
}
