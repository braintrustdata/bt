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

pub async fn list_functions(
    client: &ApiClient,
    project_id: &str,
    function_type: Option<&str>,
) -> Result<Vec<Function>> {
    let query = match function_type {
        Some(ft) => {
            format!("SELECT * FROM project_functions('{project_id}') WHERE function_type = '{ft}'")
        }
        None => format!("SELECT * FROM project_functions('{project_id}')"),
    };
    let response = client.btql::<Function>(&query).await?;

    Ok(response.data)
}

pub async fn get_function_by_slug(
    client: &ApiClient,
    project_id: &str,
    slug: &str,
) -> Result<Option<Function>> {
    let query = format!("SELECT * FROM project_functions('{project_id}') WHERE slug = '{slug}'");
    let response = client.btql(&query).await?;

    Ok(response.data.into_iter().next())
}

pub async fn delete_function(client: &ApiClient, function_id: &str) -> Result<()> {
    let path = format!("/v1/function/{}", encode(function_id));
    client.delete(&path).await
}
