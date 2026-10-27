use anyhow::{bail, Context, Result};
use reqwest::header::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};
use urlencoding::encode;

use crate::http::{ApiClient, BtqlResponse, Pagination};

use super::records::DATASET_RECORD_FIELDS;

const MAX_DATASET_ROWS_PAGE_LIMIT: usize = 1000;
const DATASET_ROWS_SINCE: &str = "1970-01-01T00:00:00Z";
const MAX_ERROR_RESPONSE_BODY_CHARS: usize = 4000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum DatasetRowsPreviewLength {
    Full,
    Preview(usize),
}

impl DatasetRowsPreviewLength {
    pub(crate) fn btql_value(self) -> i64 {
        match self {
            Self::Full => -1,
            Self::Preview(length) => length as i64,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dataset {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub project_id: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub created: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

pub type DatasetRow = Map<String, Value>;

impl Dataset {
    pub fn description_text(&self) -> Option<&str> {
        self.description
            .as_deref()
            .filter(|description| !description.is_empty())
            .or_else(|| {
                self.metadata
                    .as_ref()
                    .and_then(|metadata| metadata.get("description"))
                    .and_then(|description| description.as_str())
                    .filter(|description| !description.is_empty())
            })
    }

    pub fn created_text(&self) -> Option<&str> {
        self.created
            .as_deref()
            .filter(|created| !created.is_empty())
            .or_else(|| {
                self.created_at
                    .as_deref()
                    .filter(|created| !created.is_empty())
            })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetSnapshot {
    pub id: String,
    pub name: String,
    pub dataset_id: String,
    pub description: Option<String>,
    pub xact_id: String,
    pub created: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateDatasetSnapshotResult {
    pub dataset_snapshot: DatasetSnapshot,
    pub found_existing: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetRestorePreview {
    pub rows_to_restore: usize,
    pub rows_to_delete: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetRestoreResult {
    pub xact_id: Option<String>,
    pub rows_restored: usize,
    pub rows_deleted: usize,
}

#[derive(Debug, Deserialize)]
struct DatasetRestoreResultResponse {
    xact_id: Option<String>,
    rows_restored: usize,
    rows_deleted: usize,
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    objects: Vec<Dataset>,
}

#[derive(Debug, Deserialize)]
struct ListResponseGeneric<T> {
    objects: Vec<T>,
}

#[derive(Debug, Deserialize)]
struct DatasetHeadXactRow {
    #[serde(rename = "_xact_id", default)]
    xact_id: Option<String>,
}

pub async fn list_datasets(client: &ApiClient, project_id: &str) -> Result<Vec<Dataset>> {
    let path = format!(
        "/v1/dataset?org_name={}&project_id={}",
        encode(client.org_name()),
        encode(project_id)
    );
    let list: ListResponse = client.get(&path).await?;
    Ok(list.objects)
}

pub async fn get_dataset_by_name(
    client: &ApiClient,
    project_id: &str,
    name: &str,
) -> Result<Option<Dataset>> {
    let datasets = list_datasets(client, project_id).await?;
    Ok(datasets.into_iter().find(|dataset| dataset.name == name))
}

pub async fn list_dataset_rows_limited(
    client: &ApiClient,
    dataset_id: &str,
    max_rows: Option<usize>,
    preview_length: DatasetRowsPreviewLength,
) -> Result<(Vec<DatasetRow>, bool)> {
    Pagination::with_max_pages(10_000)
        .collect(
            max_rows,
            MAX_DATASET_ROWS_PAGE_LIMIT,
            |limit, cursor| async move {
                client
                    .btql(&build_dataset_rows_query(
                        dataset_id,
                        limit,
                        cursor.as_deref(),
                        preview_length,
                    ))
                    .await
            },
        )
        .await
        .with_context(|| format!("failed to paginate dataset '{dataset_id}'"))
}

pub async fn get_dataset_row_by_id(
    client: &ApiClient,
    dataset_id: &str,
    row_id: &str,
) -> Result<Option<DatasetRow>> {
    let query = build_dataset_row_by_id_query(dataset_id, row_id, DatasetRowsPreviewLength::Full);
    let response: BtqlResponse<DatasetRow> = client.btql(&query).await?;
    Ok(response.data.into_iter().next())
}

pub async fn create_dataset(
    client: &ApiClient,
    project_id: &str,
    name: &str,
    description: Option<&str>,
) -> Result<Dataset> {
    create_dataset_with_metadata(client, project_id, name, description, None).await
}

pub async fn create_dataset_with_metadata(
    client: &ApiClient,
    project_id: &str,
    name: &str,
    description: Option<&str>,
    metadata: Option<&Value>,
) -> Result<Dataset> {
    let mut body = serde_json::json!({
        "name": name,
        "project_id": project_id,
        "org_name": client.org_name(),
    });
    if let Some(description) = description.filter(|description| !description.is_empty()) {
        body["description"] = serde_json::Value::String(description.to_string());
    }
    if let Some(metadata) = metadata {
        body["metadata"] = metadata.clone();
    }
    client.post("/v1/dataset", &body).await
}

pub async fn delete_dataset(client: &ApiClient, dataset_id: &str) -> Result<()> {
    let path = format!("/v1/dataset/{}", encode(dataset_id));
    client.delete(&path).await
}

pub async fn list_dataset_snapshots(
    client: &ApiClient,
    dataset_id: &str,
) -> Result<Vec<DatasetSnapshot>> {
    let path = format!("/v1/dataset_snapshot?dataset_id={}", encode(dataset_id));
    let list: ListResponseGeneric<DatasetSnapshot> = client.get(&path).await?;
    Ok(list.objects)
}

pub async fn create_dataset_snapshot(
    client: &ApiClient,
    dataset_id: &str,
    name: &str,
    description: Option<&str>,
    xact_id: &str,
) -> Result<CreateDatasetSnapshotResult> {
    let mut body = serde_json::json!({
        "dataset_id": dataset_id,
        "name": name,
        "xact_id": xact_id,
    });
    if let Some(description) = description {
        body["description"] = Value::String(description.to_string());
    }
    let response = client
        .request(reqwest::Method::POST, "/v1/dataset_snapshot")
        .json(&body)
        .response()
        .await?;

    let found_existing = found_existing_snapshot_header(response.headers());
    let dataset_snapshot = response.json().await?;
    Ok(CreateDatasetSnapshotResult {
        dataset_snapshot,
        found_existing,
    })
}

pub async fn delete_dataset_snapshot(client: &ApiClient, snapshot_id: &str) -> Result<()> {
    let path = format!("/v1/dataset_snapshot/{}", encode(snapshot_id));
    client.delete(&path).await
}

pub async fn preview_dataset_restore(
    client: &ApiClient,
    dataset_id: &str,
    xact_id: &str,
) -> Result<DatasetRestorePreview> {
    let path = format!("/v1/dataset/{}/restore/preview", encode(dataset_id));
    client
        .post(&path, &serde_json::json!({ "version": xact_id }))
        .await
}

pub async fn restore_dataset(
    client: &ApiClient,
    dataset_id: &str,
    xact_id: &str,
) -> Result<DatasetRestoreResult> {
    let path = format!("/v1/dataset/{}/restore", encode(dataset_id));
    let response = client
        .request(reqwest::Method::POST, &path)
        .json(&serde_json::json!({ "version": xact_id }))
        .response()
        .await?;

    let body = response
        .text()
        .await
        .context("failed to read dataset restore response")?;
    parse_dataset_restore_result_response(&body, xact_id)
}

pub async fn get_dataset_head_xact_id(
    client: &ApiClient,
    dataset_id: &str,
) -> Result<Option<String>> {
    let query = build_dataset_head_xact_query(dataset_id);
    let response: BtqlResponse<DatasetHeadXactRow> = client.btql(&query).await?;
    let head = response.data.into_iter().find_map(|row| {
        row.xact_id
            .map(|value| value.trim().to_string())
            .filter(|value| !value.is_empty())
    });
    Ok(head)
}

fn build_dataset_rows_query(
    dataset_id: &str,
    limit: usize,
    cursor: Option<&str>,
    preview_length: DatasetRowsPreviewLength,
) -> Value {
    let mut query = json!({
        "select": dataset_rows_select_fields(),
        "from": {
            "op": "function",
            "name": {"op": "ident", "name": ["dataset"]},
            "args": [{"op": "literal", "value": dataset_id}]
        },
        "filter": {
            "op": "ge",
            "left": {"op": "ident", "name": ["created"]},
            "right": {"op": "literal", "value": DATASET_ROWS_SINCE}
        },
        "preview_length": preview_length.btql_value(),
        "limit": limit
    });
    if let Some(cursor) = cursor {
        query["cursor"] = Value::String(cursor.to_string());
    }
    query
}

fn build_dataset_row_by_id_query(
    dataset_id: &str,
    row_id: &str,
    preview_length: DatasetRowsPreviewLength,
) -> Value {
    json!({
        "select": dataset_rows_select_fields(),
        "from": {
            "op": "function",
            "name": {"op": "ident", "name": ["dataset"]},
            "args": [{"op": "literal", "value": dataset_id}]
        },
        "filter": {
            "op": "and",
            "children": [
                {
                    "op": "ge",
                    "left": {"op": "ident", "name": ["created"]},
                    "right": {"op": "literal", "value": DATASET_ROWS_SINCE}
                },
                {
                    "op": "eq",
                    "left": {"op": "ident", "name": ["id"]},
                    "right": {"op": "literal", "value": row_id}
                }
            ]
        },
        "preview_length": preview_length.btql_value(),
        "limit": 1
    })
}

fn dataset_rows_select_fields() -> Vec<Value> {
    DATASET_RECORD_FIELDS
        .iter()
        .map(|field| {
            json!({
                "alias": field,
                "expr": {"op": "ident", "name": [field]}
            })
        })
        .collect()
}

fn build_dataset_head_xact_query(dataset_id: &str) -> Value {
    json!({
        "select": [{
            "expr": {"op": "ident", "name": ["_xact_id"]},
            "alias": "_xact_id",
        }],
        "from": {
            "op": "function",
            "name": {"op": "ident", "name": ["dataset"]},
            "args": [{"op": "literal", "value": dataset_id}]
        },
        "filter": {
            "op": "ge",
            "left": {"op": "ident", "name": ["created"]},
            "right": {"op": "literal", "value": DATASET_ROWS_SINCE}
        },
        "sort": [{
            "expr": {"op": "ident", "name": ["_xact_id"]},
            "dir": "desc",
        }],
        "limit": 1
    })
}

fn found_existing_snapshot_header(headers: &HeaderMap) -> bool {
    headers
        .get("x-bt-found-existing")
        .and_then(|value| value.to_str().ok())
        .is_some_and(|value| value.eq_ignore_ascii_case("true") || value == "1")
}

fn parse_dataset_restore_result_response(
    body: &str,
    requested_xact_id: &str,
) -> Result<DatasetRestoreResult> {
    let response: DatasetRestoreResultResponse = serde_json::from_str(body).with_context(|| {
        format!(
            "failed to parse dataset restore response body: {}",
            format_response_body_for_error(body)
        )
    })?;
    let xact_id = response
        .xact_id
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());
    if xact_id.is_none() && (response.rows_restored > 0 || response.rows_deleted > 0) {
        bail!(
            "restore to xact '{}' changed rows ({} restored, {} deleted) but the response did not include a result xact",
            requested_xact_id,
            response.rows_restored,
            response.rows_deleted
        );
    }

    Ok(DatasetRestoreResult {
        xact_id,
        rows_restored: response.rows_restored,
        rows_deleted: response.rows_deleted,
    })
}

fn format_response_body_for_error(body: &str) -> String {
    let trimmed = body.trim();
    if trimmed.is_empty() {
        return "<empty>".to_string();
    }

    let formatted = serde_json::from_str::<Value>(trimmed)
        .map(|value| value.to_string())
        .unwrap_or_else(|_| trimmed.to_string());
    truncate_error_response_body(&formatted)
}

fn truncate_error_response_body(body: &str) -> String {
    let mut chars = body.chars();
    let truncated: String = chars.by_ref().take(MAX_ERROR_RESPONSE_BODY_CHARS).collect();
    if chars.next().is_some() {
        format!("{truncated}... [truncated]")
    } else {
        truncated
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataset_rows_query_includes_required_filter() {
        let query = build_dataset_rows_query(
            "dataset-id",
            1000,
            None,
            DatasetRowsPreviewLength::Preview(125),
        );
        assert_eq!(
            query,
            serde_json::json!({
                "select": [
                    {"alias": "id", "expr": {"op": "ident", "name": ["id"]}},
                    {"alias": "input", "expr": {"op": "ident", "name": ["input"]}},
                    {"alias": "expected", "expr": {"op": "ident", "name": ["expected"]}},
                    {"alias": "metadata", "expr": {"op": "ident", "name": ["metadata"]}},
                    {"alias": "tags", "expr": {"op": "ident", "name": ["tags"]}},
                    {"alias": "origin", "expr": {"op": "ident", "name": ["origin"]}}
                ],
                "from": {
                    "op": "function",
                    "name": {"op": "ident", "name": ["dataset"]},
                    "args": [{"op": "literal", "value": "dataset-id"}]
                },
                "filter": {
                    "op": "ge",
                    "left": {"op": "ident", "name": ["created"]},
                    "right": {"op": "literal", "value": "1970-01-01T00:00:00Z"}
                },
                "preview_length": 125,
                "limit": 1000
            })
        );
    }

    #[test]
    fn dataset_rows_query_sets_cursor() {
        let query = build_dataset_rows_query(
            "dataset-id",
            200,
            Some("cursor-123"),
            DatasetRowsPreviewLength::Preview(125),
        );
        assert_eq!(
            query.get("cursor").and_then(Value::as_str),
            Some("cursor-123")
        );
        assert_eq!(query.get("limit").and_then(Value::as_u64), Some(200));
    }

    #[test]
    fn dataset_rows_query_keeps_dataset_id_as_literal() {
        let query = build_dataset_rows_query(
            "dataset'with-quote",
            10,
            None,
            DatasetRowsPreviewLength::Preview(125),
        );
        assert_eq!(
            query.get("from").expect("from"),
            &serde_json::json!({
                "op": "function",
                "name": {"op": "ident", "name": ["dataset"]},
                "args": [{"op": "literal", "value": "dataset'with-quote"}]
            })
        );
    }

    #[test]
    fn dataset_rows_query_uses_full_preview_length_for_exact_values() {
        let query =
            build_dataset_rows_query("dataset-id", 100, None, DatasetRowsPreviewLength::Full);
        assert_eq!(
            query.get("preview_length").and_then(Value::as_i64),
            Some(-1)
        );
    }

    #[test]
    fn dataset_head_query_includes_required_filter_sort_and_limit() {
        let query = build_dataset_head_xact_query("dataset-id");
        assert_eq!(
            query,
            serde_json::json!({
                "select": [{
                    "expr": {"op": "ident", "name": ["_xact_id"]},
                    "alias": "_xact_id",
                }],
                "from": {
                    "op": "function",
                    "name": {"op": "ident", "name": ["dataset"]},
                    "args": [{"op": "literal", "value": "dataset-id"}]
                },
                "filter": {
                    "op": "ge",
                    "left": {"op": "ident", "name": ["created"]},
                    "right": {"op": "literal", "value": "1970-01-01T00:00:00Z"}
                },
                "sort": [{
                    "expr": {"op": "ident", "name": ["_xact_id"]},
                    "dir": "desc",
                }],
                "limit": 1
            })
        );
    }

    #[test]
    fn dataset_head_query_keeps_dataset_id_as_literal() {
        let query = build_dataset_head_xact_query("dataset'with-quote");
        assert_eq!(
            query.pointer("/from/args/0/value").and_then(Value::as_str),
            Some("dataset'with-quote")
        );
    }

    #[test]
    fn dataset_snapshot_deserializes_service_schema() {
        let snapshot: DatasetSnapshot = serde_json::from_value(serde_json::json!({
            "id": "01926568-8088-7109-99ab-123456789abc",
            "dataset_id": "01926568-8088-7109-99ab-abcdef012345",
            "name": "baseline",
            "description": null,
            "xact_id": "1000192656880881099",
            "created": null
        }))
        .expect("deserialize snapshot");

        assert_eq!(snapshot.dataset_id, "01926568-8088-7109-99ab-abcdef012345");
        assert_eq!(snapshot.name, "baseline");
        assert!(snapshot.description.is_none());
        assert_eq!(snapshot.xact_id, "1000192656880881099");
        assert!(snapshot.created.is_none());
    }

    #[test]
    fn dataset_restore_preview_deserializes_count_fields() {
        let preview: DatasetRestorePreview = serde_json::from_value(serde_json::json!({
            "rows_to_restore": 7,
            "rows_to_delete": 2
        }))
        .expect("deserialize preview");
        assert_eq!(preview.rows_to_restore, 7);
        assert_eq!(preview.rows_to_delete, 2);
    }

    #[test]
    fn dataset_restore_result_deserializes_count_fields() {
        let result: DatasetRestoreResult = serde_json::from_value(serde_json::json!({
            "xact_id": "1000192656880881099",
            "rows_restored": 7,
            "rows_deleted": 2
        }))
        .expect("deserialize result");
        assert_eq!(result.xact_id.as_deref(), Some("1000192656880881099"));
        assert_eq!(result.rows_restored, 7);
        assert_eq!(result.rows_deleted, 2);
    }

    #[test]
    fn dataset_restore_result_parse_error_includes_response_body() {
        let error = parse_dataset_restore_result_response(
            r#"{"xact_id":"1000192656880881099"}"#,
            "1000192656880881099",
        )
        .expect_err("missing count fields should fail");
        let message = error.to_string();
        assert!(
            message.contains(
                r#"failed to parse dataset restore response body: {"xact_id":"1000192656880881099"}"#
            ),
            "unexpected error message: {message}"
        );
    }

    #[test]
    fn empty_restore_result_parse_error_is_labeled_empty() {
        let error = parse_dataset_restore_result_response("", "1000192656880881099")
            .expect_err("empty body should fail");
        assert!(error
            .to_string()
            .contains("failed to parse dataset restore response body: <empty>"));
    }

    #[test]
    fn null_restore_result_xact_id_with_no_changes_is_noop_success() {
        let result = parse_dataset_restore_result_response(
            r#"{"rows_deleted":0,"rows_restored":0,"xact_id":null}"#,
            "1000192656880881099",
        )
        .expect("null xact id should be ok for no-op restore");
        assert!(result.xact_id.is_none());
        assert_eq!(result.rows_restored, 0);
        assert_eq!(result.rows_deleted, 0);
    }

    #[test]
    fn null_restore_result_xact_id_with_changes_is_error() {
        let error = parse_dataset_restore_result_response(
            r#"{"rows_deleted":0,"rows_restored":1,"xact_id":null}"#,
            "1000192656880881099",
        )
        .expect_err("changed restore should include a result xact id");
        assert_eq!(
            error.to_string(),
            "restore to xact '1000192656880881099' changed rows (1 restored, 0 deleted) but the response did not include a result xact"
        );
    }

    #[test]
    fn found_existing_snapshot_header_accepts_true_and_one() {
        let mut headers = HeaderMap::new();
        headers.insert("x-bt-found-existing", "true".parse().expect("header"));
        assert!(found_existing_snapshot_header(&headers));

        headers.insert("x-bt-found-existing", "1".parse().expect("header"));
        assert!(found_existing_snapshot_header(&headers));
    }

    #[test]
    fn found_existing_snapshot_header_rejects_missing_or_false() {
        assert!(!found_existing_snapshot_header(&HeaderMap::new()));

        let mut headers = HeaderMap::new();
        headers.insert("x-bt-found-existing", "false".parse().expect("header"));
        assert!(!found_existing_snapshot_header(&headers));
    }
}
