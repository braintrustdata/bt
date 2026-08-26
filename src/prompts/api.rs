use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
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

#[derive(Debug, Deserialize)]
struct PromptVersionsResponse {
    #[serde(default)]
    data: Vec<Value>,
}

pub async fn list_prompts(
    client: &ApiClient,
    project: &str,
    environment: Option<&str>,
) -> Result<Vec<Prompt>> {
    let mut path = format!(
        "/v1/prompt?org_name={}&project_name={}",
        encode(client.org_name()),
        encode(project)
    );
    if let Some(environment) = environment {
        path.push_str(&format!("&environment={}", encode(environment)));
    }
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

pub async fn list_prompt_versions(
    client: &ApiClient,
    project_id: &str,
    prompt_id: &str,
) -> Result<Vec<String>> {
    let body = prompt_versions_request(project_id, prompt_id);
    let org_name = client.org_name();
    let headers = if org_name.is_empty() {
        Vec::new()
    } else {
        vec![("x-bt-org-name", org_name)]
    };
    let response: PromptVersionsResponse =
        client.post_with_headers("/btql", &body, &headers).await?;

    Ok(prompt_versions_from_rows(response.data))
}

fn prompt_versions_request(project_id: &str, prompt_id: &str) -> Value {
    json!({
        "query": {
            "from": {
                "op": "function",
                "name": { "op": "ident", "name": ["project_prompts"] },
                "args": [{ "op": "literal", "value": project_id }]
            },
            "select": [{ "op": "star" }],
            "sort": [{
                "expr": { "op": "ident", "name": ["_xact_id"] },
                "dir": "desc"
            }],
            "filter": {
                "op": "eq",
                "left": { "op": "ident", "name": ["id"] },
                "right": { "op": "literal", "value": prompt_id }
            }
        },
        "audit_log": true,
        "use_columnstore": false,
        "brainstore_realtime": true,
        "fmt": "json"
    })
}

fn prompt_versions_from_rows(rows: Vec<Value>) -> Vec<String> {
    rows.into_iter()
        .filter(|row| {
            matches!(
                row.pointer("/audit_data/action").and_then(Value::as_str),
                Some("upsert" | "merge")
            )
        })
        .filter_map(|row| {
            let xact_id = row.get("_xact_id")?;
            let raw = xact_id
                .as_str()
                .map(ToOwned::to_owned)
                .or_else(|| xact_id.as_u64().map(|value| value.to_string()))?;
            let value = raw.parse::<u64>().ok()?;
            Some(crate::util_cmd::prettify_xact(value))
        })
        .collect()
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_versions_request_scopes_audit_log_to_prompt() {
        let request = prompt_versions_request("proj_test", "prompt_test");

        assert_eq!(
            request["query"]["from"]["name"]["name"],
            json!(["project_prompts"])
        );
        assert_eq!(request["query"]["from"]["args"][0]["value"], "proj_test");
        assert_eq!(request["query"]["filter"]["right"]["value"], "prompt_test");
        assert_eq!(request["audit_log"], true);
        assert_eq!(request["use_columnstore"], false);
        assert_eq!(request["brainstore_realtime"], true);
    }

    #[test]
    fn prompt_versions_include_upserts_and_merges() {
        let rows = vec![
            json!({"_xact_id": "1000192656880881099", "audit_data": {"action": "upsert"}}),
            json!({"_xact_id": 1000192656880881100_u64, "audit_data": {"action": "merge"}}),
            json!({"_xact_id": "1000192656880881101", "audit_data": {"action": "delete"}}),
            json!({"audit_data": {"action": "upsert"}}),
        ];

        assert_eq!(
            prompt_versions_from_rows(rows),
            vec!["81cd05ee665fdfb3", "81cdc1302a2a586c"]
        );
    }
}
