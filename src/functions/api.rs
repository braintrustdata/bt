use anyhow::Result;
use serde::{Deserialize, Serialize};
use urlencoding::encode;

use crate::http::ApiClient;

fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

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
    #[serde(default)]
    pub _xact_id: Option<String>,
}

pub const FUNCTION_LIST_FIELDS: &str =
    "id, name, slug, project_id, function_type, description, _xact_id";

pub async fn list_functions(
    client: &ApiClient,
    project_id: &str,
    function_type: Option<&str>,
    fields: Option<&str>,
) -> Result<Vec<Function>> {
    let pid = escape_sql(project_id);
    let fields = fields.unwrap_or("*");
    let query = match function_type {
        Some(ft) => {
            let ft = escape_sql(ft);
            format!("SELECT {fields} FROM project_functions('{pid}') WHERE function_type = '{ft}'")
        }
        None => format!("SELECT {fields} FROM project_functions('{pid}')"),
    };
    let response = client.btql::<Function>(&query).await?;

    Ok(response.data)
}

pub async fn get_function_by_slug(
    client: &ApiClient,
    project_id: &str,
    slug: &str,
) -> Result<Option<Function>> {
    let pid = escape_sql(project_id);
    let slug = escape_sql(slug);
    let query = format!("SELECT * FROM project_functions('{pid}') WHERE slug = '{slug}'");
    let response = client.btql(&query).await?;

    Ok(response.data.into_iter().next())
}

pub async fn invoke_function(
    client: &ApiClient,
    body: &serde_json::Value,
) -> Result<serde_json::Value> {
    let org_name = client.org_name();
    let headers = if !org_name.is_empty() {
        vec![("x-bt-org-name", org_name)]
    } else {
        Vec::new()
    };
    let timeout = std::time::Duration::from_secs(300);
    client
        .post_with_headers_timeout("/function/invoke", body, &headers, Some(timeout))
        .await
}

pub async fn delete_function(client: &ApiClient, function_id: &str) -> Result<()> {
    let path = format!("/v1/function/{}", encode(function_id));
    client.delete(&path).await
}
