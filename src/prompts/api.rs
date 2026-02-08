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

pub async fn get_prompt_by_name(client: &ApiClient, project: &str, name: &str) -> Result<Prompt> {
    let path = format!(
        "/v1/prompt?org_name={}&project_name={}&prompt_name={}",
        encode(client.org_name()),
        encode(project),
        encode(name)
    );
    let list: ListResponse = client.get(&path).await?;
    list.objects
        .into_iter()
        .next()
        .ok_or_else(|| anyhow::anyhow!("prompt '{name}' not found"))
}

pub async fn delete_prompt(client: &ApiClient, prompt_id: &str) -> Result<()> {
    let path = format!("/v1/prompt/{}", encode(prompt_id));
    client.delete(&path).await
}
