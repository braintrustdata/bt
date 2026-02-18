use anyhow::Result;
use serde::{Deserialize, Serialize};
use urlencoding::encode;

use crate::http::ApiClient;
use crate::resource_cmd::NamedResource;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Function {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub slug: String,
    pub project_id: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub function_type: Option<String>,
    #[serde(default)]
    pub function_data: Option<serde_json::Value>,
}

impl Function {
    pub fn display_type(&self) -> String {
        if let Some(function_type) = self
            .function_type
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            return function_type.to_string();
        }

        self.function_data
            .as_ref()
            .and_then(|fd| fd.get("type"))
            .and_then(|v| v.as_str())
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(ToString::to_string)
            .unwrap_or_else(|| "-".to_string())
    }
}

impl NamedResource for Function {
    fn name(&self) -> &str {
        &self.name
    }

    fn description(&self) -> Option<&str> {
        self.description.as_deref()
    }

    fn slug(&self) -> &str {
        &self.slug
    }

    fn resource_type(&self) -> Option<String> {
        Some(self.display_type())
    }
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    objects: Vec<Function>,
}

pub async fn list_functions(client: &ApiClient, project: &str) -> Result<Vec<Function>> {
    let path = format!(
        "/v1/function?org_name={}&project_name={}",
        encode(client.org_name()),
        encode(project)
    );
    let list: ListResponse = client.get(&path).await?;

    Ok(list.objects)
}

pub async fn get_function_by_slug(
    client: &ApiClient,
    project: &str,
    slug: &str,
) -> Result<Option<Function>> {
    let path = format!(
        "/v1/function?org_name={}&project_name={}&slug={}",
        encode(client.org_name()),
        encode(project),
        encode(slug)
    );
    let list: ListResponse = client.get(&path).await?;
    Ok(list.objects.into_iter().next())
}

pub async fn delete_function(client: &ApiClient, function_id: &str) -> Result<()> {
    let path = format!("/v1/function/{}", encode(function_id));
    client.delete(&path).await
}
