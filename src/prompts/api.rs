use anyhow::Result;
use serde::{Deserialize, Serialize};
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
    #[serde(default)]
    pub created: Option<String>,
    #[serde(default)]
    pub _xact_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EnvironmentObject {
    pub id: String,
    pub object_type: String,
    pub object_id: String,
    pub object_version: String,
    pub environment_slug: String,
    #[serde(default)]
    pub environment_id: Option<String>,
    #[serde(default)]
    pub created: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListResponse<T> {
    objects: Vec<T>,
}

pub async fn list_prompts(client: &ApiClient, project: &str) -> Result<Vec<Prompt>> {
    let path = format!(
        "/v1/prompt?org_name={}&project_name={}",
        encode(client.org_name()),
        encode(project)
    );
    let list: ListResponse<Prompt> = client.get(&path).await?;

    Ok(list.objects)
}

pub async fn list_prompts_by_environment(
    client: &ApiClient,
    project: &str,
    environment: &str,
) -> Result<Vec<Prompt>> {
    let path = format!(
        "/v1/prompt?org_name={}&project_name={}&environment={}",
        encode(client.org_name()),
        encode(project),
        encode(environment)
    );
    let list: ListResponse<Prompt> = client.get(&path).await?;
    Ok(list.objects)
}

pub async fn get_prompt_by_slug(
    client: &ApiClient,
    project: &str,
    slug: &str,
    version: Option<&str>,
    environment: Option<&str>,
) -> Result<Option<Prompt>> {
    let mut params = vec![
        ("org_name", client.org_name()),
        ("project_name", project),
        ("slug", slug),
    ];
    if let Some(version) = version {
        params.push(("version", version));
    }
    if let Some(environment) = environment {
        params.push(("environment", environment));
    }
    let query = params
        .into_iter()
        .map(|(key, value)| format!("{}={}", encode(key), encode(value)))
        .collect::<Vec<_>>()
        .join("&");
    let list: ListResponse<Prompt> = client.get(&format!("/v1/prompt?{query}")).await?;
    Ok(list.objects.into_iter().next())
}

pub async fn assign_prompt(
    client: &ApiClient,
    prompt_id: &str,
    environment: &str,
    object_version: &str,
) -> Result<EnvironmentObject> {
    let path = format!(
        "/environment-object/prompt/{}/{}",
        encode(prompt_id),
        encode(environment)
    );
    let body = serde_json::json!({
        "object_version": object_version,
        "org_name": client.org_name(),
    });
    client.put(&path, &body).await
}

pub async fn unassign_prompt(
    client: &ApiClient,
    prompt_id: &str,
    environment: &str,
) -> Result<EnvironmentObject> {
    let path = format!(
        "/environment-object/prompt/{}/{}?org_name={}",
        encode(prompt_id),
        encode(environment),
        encode(client.org_name())
    );
    client.delete_with_response(&path).await
}

pub async fn delete_prompt(client: &ApiClient, prompt_id: &str) -> Result<()> {
    let path = format!("/v1/prompt/{}", encode(prompt_id));
    client.delete(&path).await
}
