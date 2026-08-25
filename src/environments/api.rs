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

#[derive(Debug, Deserialize)]
struct ListResponse {
    objects: Vec<Environment>,
}

#[derive(Debug, Serialize)]
pub struct CreateEnvironment<'a> {
    pub name: &'a str,
    pub slug: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<&'a str>,
    pub org_name: &'a str,
}

#[derive(Debug, Default, Serialize)]
pub struct UpdateEnvironment<'a> {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slug: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<Option<&'a str>>,
}

pub async fn list_environments(client: &ApiClient) -> Result<Vec<Environment>> {
    let path = format!("/environment?org_name={}", encode(client.org_name()));
    let response: ListResponse = client.get(&path).await?;
    Ok(response.objects)
}

pub async fn get_environment_by_slug(
    client: &ApiClient,
    slug: &str,
) -> Result<Option<Environment>> {
    Ok(list_environments(client)
        .await?
        .into_iter()
        .find(|environment| environment.slug == slug))
}

pub async fn create_environment(
    client: &ApiClient,
    input: &CreateEnvironment<'_>,
) -> Result<Environment> {
    client.post("/environment", input).await
}

pub async fn update_environment(
    client: &ApiClient,
    environment_id: &str,
    input: &UpdateEnvironment<'_>,
) -> Result<Environment> {
    let path = format!("/environment/{}", encode(environment_id));
    client.patch(&path, input).await
}

pub async fn delete_environment(client: &ApiClient, environment_id: &str) -> Result<Environment> {
    let path = format!("/environment/{}", encode(environment_id));
    client.delete_with_response(&path).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn update_omits_unchanged_fields() {
        let input = UpdateEnvironment {
            name: Some("Production"),
            ..Default::default()
        };

        assert_eq!(
            serde_json::to_value(input).expect("serialize update"),
            serde_json::json!({ "name": "Production" })
        );
    }

    #[test]
    fn update_can_clear_description() {
        let input = UpdateEnvironment {
            description: Some(None),
            ..Default::default()
        };

        assert_eq!(
            serde_json::to_value(input).expect("serialize update"),
            serde_json::json!({ "description": null })
        );
    }
}
