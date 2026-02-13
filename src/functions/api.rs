use anyhow::Result;
use serde::{Deserialize, Serialize};
use urlencoding::encode;

use crate::http::ApiClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Function {
    pub id: String,
    pub name: String,
    pub slug: String,
    pub project_id: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub function_type: Option<String>,
    #[serde(default)]
    pub prompt_data: Option<serde_json::Value>,
    #[serde(default)]
    pub function_data: Option<serde_json::Value>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub created: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    objects: Vec<Function>,
}

pub async fn list_functions(
    client: &ApiClient,
    project: &str,
    function_type: Option<&str>,
) -> Result<Vec<Function>> {
    let path = format!(
        "/v1/function?org_name={}&project_name={}",
        encode(client.org_name()),
        encode(project)
    );
    let list: ListResponse = client.get(&path).await?;

    Ok(match function_type {
        Some(ft) => list
            .objects
            .into_iter()
            .filter(|f| f.function_type.as_deref() == Some(ft))
            .collect(),
        None => list.objects,
    })
}

pub async fn get_function_by_slug(
    client: &ApiClient,
    project: &str,
    slug: &str,
    function_type: Option<&str>,
) -> Result<Option<Function>> {
    let path = format!(
        "/v1/function?org_name={}&project_name={}&slug={}",
        encode(client.org_name()),
        encode(project),
        encode(slug)
    );
    let list: ListResponse = client.get(&path).await?;

    Ok(list.objects.into_iter().find(|f| match function_type {
        Some(ft) => f.function_type.as_deref() == Some(ft),
        None => true,
    }))
}

pub async fn delete_function(client: &ApiClient, function_id: &str) -> Result<()> {
    let path = format!("/v1/function/{}", encode(function_id));
    client.delete(&path).await
}
