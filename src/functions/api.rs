use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
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
    pub cursor: Option<String>,
    pub snapshot: Option<String>,
}

#[derive(Debug, Clone)]
pub struct FunctionListPage {
    pub objects: Vec<Value>,
    pub next_cursor: Option<String>,
    pub snapshot: Option<String>,
    pub pagination_field_present: bool,
    pub snapshot_field_present: bool,
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

    let explicit_next_cursor = raw
        .get("next_cursor")
        .and_then(Value::as_str)
        .or_else(|| raw.get("nextCursor").and_then(Value::as_str))
        .or_else(|| raw.get("next").and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    let cursor_field = raw
        .get("cursor")
        .and_then(Value::as_str)
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    let has_more = raw
        .get("has_more")
        .and_then(Value::as_bool)
        .or_else(|| raw.get("hasMore").and_then(Value::as_bool));

    let next_cursor = explicit_next_cursor.or(match has_more {
        Some(false) => None,
        _ => cursor_field,
    });

    let snapshot = raw
        .get("snapshot")
        .and_then(Value::as_str)
        .or_else(|| raw.get("snapshot_id").and_then(Value::as_str))
        .or_else(|| raw.get("as_of").and_then(Value::as_str))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToOwned::to_owned);

    Ok(FunctionListPage {
        objects,
        next_cursor,
        snapshot,
        pagination_field_present: raw.get("next_cursor").is_some()
            || raw.get("nextCursor").is_some()
            || raw.get("next").is_some()
            || raw.get("cursor").is_some()
            || raw.get("has_more").is_some()
            || raw.get("hasMore").is_some(),
        snapshot_field_present: raw.get("snapshot").is_some()
            || raw.get("snapshot_id").is_some()
            || raw.get("as_of").is_some(),
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
    let body = serde_json::json!({ "functions": functions });
    let raw: Value = client
        .post("/insert-functions", &body)
        .await
        .context("failed to insert functions")?;

    Ok(InsertFunctionsResult {
        ignored_entries: ignored_count(&raw),
    })
}

fn ignored_count(raw: &Value) -> Option<usize> {
    if let Some(count) = raw.get("ignored_count").and_then(Value::as_u64) {
        return usize::try_from(count).ok();
    }

    if let Some(items) = raw.get("ignored").and_then(Value::as_array) {
        return Some(items.len());
    }

    if let Some(count) = raw
        .get("stats")
        .and_then(Value::as_object)
        .and_then(|stats| stats.get("ignored"))
        .and_then(Value::as_u64)
    {
        return usize::try_from(count).ok();
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ignored_count_extracts_known_shapes() {
        let first = serde_json::json!({ "ignored_count": 3 });
        assert_eq!(ignored_count(&first), Some(3));

        let second = serde_json::json!({ "ignored": [1, 2] });
        assert_eq!(ignored_count(&second), Some(2));

        let third = serde_json::json!({ "stats": { "ignored": 5 } });
        assert_eq!(ignored_count(&third), Some(5));

        assert_eq!(ignored_count(&serde_json::json!({})), None);
    }

    #[test]
    fn parse_function_list_page_allows_non_paginated_shape() {
        let raw = serde_json::json!({
            "objects": [],
        });

        let page = parse_function_list_page(raw).expect("parse function page");
        assert!(page.objects.is_empty());
        assert!(!page.pagination_field_present);
        assert!(page.next_cursor.is_none());
    }

    #[test]
    fn parse_function_list_page_detects_next_pagination_field() {
        let raw = serde_json::json!({
            "objects": [],
            "next": "cursor-1",
        });

        let page = parse_function_list_page(raw).expect("parse function page");
        assert!(page.pagination_field_present);
        assert_eq!(page.next_cursor.as_deref(), Some("cursor-1"));
    }

    #[test]
    fn parse_function_list_page_supports_cursor_has_more_shape() {
        let raw = serde_json::json!({
            "objects": [],
            "cursor": "cursor-2",
            "has_more": true,
        });

        let page = parse_function_list_page(raw).expect("parse function page");
        assert!(page.pagination_field_present);
        assert_eq!(page.next_cursor.as_deref(), Some("cursor-2"));
    }

    #[test]
    fn parse_function_list_page_ignores_cursor_when_has_more_false() {
        let raw = serde_json::json!({
            "objects": [],
            "cursor": "cursor-2",
            "has_more": false,
        });

        let page = parse_function_list_page(raw).expect("parse function page");
        assert!(page.pagination_field_present);
        assert!(page.next_cursor.is_none());
    }
}
