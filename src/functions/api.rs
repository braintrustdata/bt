use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use serde_json::Value;
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

#[derive(Debug, Clone, Default)]
pub struct FunctionListQuery {
    pub project_id: Option<String>,
    pub project_name: Option<String>,
    pub slug: Option<String>,
    pub id: Option<String>,
    pub version: Option<String>,
    pub cursor: Option<String>,
    pub snapshot: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FunctionListPage {
    pub objects: Vec<Value>,
    pub next_cursor: Option<String>,
    pub snapshot: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct CodeUploadSlot {
    pub url: String,
    #[serde(rename = "bundleId")]
    pub bundle_id: String,
}

#[derive(Debug, Clone)]
pub struct InsertFunctionsResult {
    pub ignored_entries: Option<usize>,
    pub xact_id: Option<String>,
    pub functions: Vec<InsertedFunction>,
}

#[derive(Debug, Clone)]
pub struct InsertedFunction {
    pub id: String,
    pub slug: String,
    pub project_id: String,
    pub found_existing: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct EnvironmentObject {
    #[serde(default)]
    pub environment_slug: Option<String>,
    #[serde(default)]
    pub object_version: Option<String>,
}

#[derive(Debug, Deserialize)]
struct EnvironmentObjectsResponse {
    #[serde(default)]
    objects: Vec<EnvironmentObject>,
}

pub async fn list_functions(
    client: &ApiClient,
    project_id: &str,
    function_type: Option<&str>,
) -> Result<Vec<Function>> {
    let pid = escape_sql(project_id);
    let query = match function_type {
        Some(ft) => {
            let ft = escape_sql(ft);
            format!("SELECT * FROM project_functions('{pid}') WHERE function_type = '{ft}'")
        }
        None => format!("SELECT * FROM project_functions('{pid}')"),
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
    let headers = org_headers(client);
    let timeout = std::time::Duration::from_secs(300);
    client
        .post_with_headers_timeout("/function/invoke", body, &headers, Some(timeout))
        .await
}

pub async fn delete_function(client: &ApiClient, function_id: &str) -> Result<()> {
    let path = format!("/v1/function/{}", encode(function_id));
    client.delete(&path).await
}

pub async fn list_functions_page(
    client: &ApiClient,
    query: &FunctionListQuery,
) -> Result<FunctionListPage> {
    let mut params = Vec::new();
    if let Some(project_id) = &query.project_id {
        params.push(("project_id", project_id.clone()));
    }
    if let Some(project_name) = &query.project_name {
        params.push(("project_name", project_name.clone()));
    }
    if let Some(slug) = &query.slug {
        params.push(("slug", slug.clone()));
    }
    if let Some(id) = &query.id {
        params.push(("ids", id.clone()));
    }
    if let Some(version) = &query.version {
        params.push(("version", version.clone()));
    }
    if let Some(cursor) = &query.cursor {
        params.push(("cursor", cursor.clone()));
    }
    if let Some(snapshot) = &query.snapshot {
        params.push(("snapshot", snapshot.clone()));
    }

    let path = if params.is_empty() {
        "/v1/function".to_string()
    } else {
        let query = params
            .into_iter()
            .map(|(key, value)| format!("{}={}", encode(key), encode(&value)))
            .collect::<Vec<_>>()
            .join("&");
        format!("/v1/function?{query}")
    };

    let raw: Value = client
        .get(&path)
        .await
        .with_context(|| format!("failed to list functions via {path}"))?;

    parse_function_list_page(raw)
}

fn parse_function_list_page(raw: Value) -> Result<FunctionListPage> {
    let objects = raw
        .get("objects")
        .and_then(Value::as_array)
        .cloned()
        .ok_or_else(|| anyhow::anyhow!("missing 'objects' array in /v1/function response"))?;

    let next_cursor = raw
        .get("next_cursor")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    let snapshot = raw
        .get("snapshot")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    Ok(FunctionListPage {
        objects,
        next_cursor,
        snapshot,
    })
}

pub async fn request_code_upload_slot(
    client: &ApiClient,
    org_id: &str,
    runtime: &str,
    version: &str,
) -> Result<CodeUploadSlot> {
    let body = serde_json::json!({
        "org_id": org_id,
        "runtime_context": {
            "runtime": runtime,
            "version": version,
        }
    });

    client
        .post("/function/code", &body)
        .await
        .context("failed to request code upload slot")
}

pub async fn upload_bundle(
    url: &str,
    bundle_bytes: Vec<u8>,
    content_encoding: Option<&str>,
) -> Result<()> {
    crate::http::put_signed_url(url, bundle_bytes, content_encoding)
        .await
        .context("failed to upload code bundle to signed URL")
}

pub async fn insert_functions(
    client: &ApiClient,
    functions: &[Value],
) -> Result<InsertFunctionsResult> {
    let headers = org_headers(client);
    let body = serde_json::json!({ "functions": functions });
    let raw: Value = client
        .post_with_headers("/insert-functions", &body, &headers)
        .await
        .context("failed to insert functions")?;

    Ok(InsertFunctionsResult {
        ignored_entries: ignored_count(&raw),
        xact_id: xact_id(&raw),
        functions: inserted_functions(&raw),
    })
}

pub async fn list_environment_objects_for_prompt(
    client: &ApiClient,
    prompt_id: &str,
) -> Result<Vec<EnvironmentObject>> {
    let headers = org_headers(client);
    let path = format!("/environment-object/prompt/{}", encode(prompt_id));
    let response: EnvironmentObjectsResponse = client
        .get_with_headers(&path, &headers)
        .await
        .with_context(|| format!("failed to list environments via {path}"))?;
    Ok(response.objects)
}

pub async fn upsert_environment_object_for_prompt(
    client: &ApiClient,
    prompt_id: &str,
    environment_slug: &str,
    object_version: &str,
) -> Result<()> {
    let headers = org_headers(client);
    let path = format!(
        "/environment-object/prompt/{}/{}",
        encode(prompt_id),
        encode(environment_slug)
    );
    let mut body = json!({
        "object_version": object_version,
    });
    let org_name = client.org_name().trim();
    if !org_name.is_empty() {
        body["org_name"] = Value::String(org_name.to_string());
    }
    let _: Value = client
        .put_with_headers(&path, &body, &headers)
        .await
        .with_context(|| format!("failed to upsert environment association via {path}"))?;
    Ok(())
}

fn org_headers(client: &ApiClient) -> Vec<(&str, &str)> {
    let org_name = client.org_name().trim();
    if org_name.is_empty() {
        Vec::new()
    } else {
        vec![("x-bt-org-name", org_name)]
    }
}

fn ignored_count(raw: &Value) -> Option<usize> {
    raw.get("ignored_count")
        .and_then(Value::as_u64)
        .and_then(|count| usize::try_from(count).ok())
}

fn xact_id(raw: &Value) -> Option<String> {
    raw.get("xact_id")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned)
}

fn inserted_functions(raw: &Value) -> Vec<InsertedFunction> {
    let Some(items) = raw.get("functions").and_then(Value::as_array) else {
        return Vec::new();
    };

    items
        .iter()
        .filter_map(|item| {
            let id = item.get("id").and_then(Value::as_str)?.trim();
            let slug = item.get("slug").and_then(Value::as_str)?.trim();
            let project_id = item.get("project_id").and_then(Value::as_str)?.trim();
            if id.is_empty() || slug.is_empty() || project_id.is_empty() {
                return None;
            }
            let found_existing = item
                .get("found_existing")
                .and_then(Value::as_bool)
                .unwrap_or(false);
            Some(InsertedFunction {
                id: id.to_string(),
                slug: slug.to_string(),
                project_id: project_id.to_string(),
                found_existing,
            })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignored_count_extracts_canonical_shape() {
        let first = serde_json::json!({ "ignored_count": 3 });
        assert_eq!(ignored_count(&first), Some(3));

        let second = serde_json::json!({ "ignored": [1, 2] });
        assert_eq!(ignored_count(&second), None);

        let third = serde_json::json!({ "stats": { "ignored": 5 } });
        assert_eq!(ignored_count(&third), None);

        assert_eq!(ignored_count(&serde_json::json!({})), None);
    }

    #[test]
    fn insert_functions_response_extracts_metadata() {
        let raw = serde_json::json!({
            "xact_id": "123",
            "functions": [
                {
                    "id": "fn_1",
                    "slug": "hello",
                    "project_id": "proj_1",
                    "found_existing": true
                }
            ]
        });

        assert_eq!(xact_id(&raw).as_deref(), Some("123"));
        let functions = inserted_functions(&raw);
        assert_eq!(functions.len(), 1);
        assert_eq!(functions[0].id, "fn_1");
        assert_eq!(functions[0].slug, "hello");
        assert_eq!(functions[0].project_id, "proj_1");
        assert!(functions[0].found_existing);
    }

    #[test]
    fn parse_function_list_page_allows_non_paginated_shape() {
        let raw = serde_json::json!({
            "objects": [],
        });

        let page = parse_function_list_page(raw).expect("parse function page");
        assert!(page.objects.is_empty());
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn parse_function_list_page_detects_next_pagination_field() {
        let raw = serde_json::json!({
            "objects": [],
            "next_cursor": "cursor-1",
        });

        let page = parse_function_list_page(raw).expect("parse function page");
        assert_eq!(page.next_cursor.as_deref(), Some("cursor-1"));
    }

    #[test]
    fn parse_function_list_page_extracts_snapshot() {
        let raw = serde_json::json!({
            "objects": [],
            "snapshot": "snapshot-1",
        });

        let page = parse_function_list_page(raw).expect("parse function page");
        assert_eq!(page.snapshot.as_deref(), Some("snapshot-1"));
    }
}
