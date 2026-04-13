use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use urlencoding::encode;

use crate::http::ApiClient;

const MAX_DATASET_ROWS_PAGE_LIMIT: usize = 1000;
const DATASET_ROWS_SINCE: &str = "1970-01-01T00:00:00Z";

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
pub struct DatasetVersion {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub dataset_id: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub xact_id: Option<String>,
    #[serde(default)]
    pub created: Option<String>,
    #[serde(default)]
    pub created_at: Option<String>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
}

impl DatasetVersion {
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

    pub fn xact_id_text(&self) -> Option<&str> {
        self.xact_id
            .as_deref()
            .filter(|xact_id| !xact_id.is_empty())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetRestorePreview {
    pub rows_to_restore: usize,
    pub rows_to_delete: usize,
    #[serde(flatten, default)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatasetRestoreResult {
    pub xact_id: String,
    pub rows_restored: usize,
    pub rows_deleted: usize,
    #[serde(flatten, default)]
    pub extra: Map<String, Value>,
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    objects: Vec<Dataset>,
}

#[derive(Debug, Deserialize)]
struct DatasetRowsResponse {
    #[serde(default)]
    data: Vec<DatasetRow>,
    #[serde(default)]
    cursor: Option<String>,
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

pub async fn list_dataset_rows(client: &ApiClient, dataset_id: &str) -> Result<Vec<DatasetRow>> {
    let mut rows = Vec::new();
    let mut cursor: Option<String> = None;

    loop {
        let query =
            build_dataset_rows_query(dataset_id, MAX_DATASET_ROWS_PAGE_LIMIT, cursor.as_deref());
        let body = serde_json::json!({
            "query": query,
            "fmt": "json",
        });
        let org_name = client.org_name();
        let headers = if !org_name.is_empty() {
            vec![("x-bt-org-name", org_name)]
        } else {
            Vec::new()
        };
        let response: DatasetRowsResponse =
            client.post_with_headers("/btql", &body, &headers).await?;

        if response.data.is_empty() {
            break;
        }

        rows.extend(response.data);

        let next_cursor = response.cursor.filter(|cursor| !cursor.is_empty());
        if next_cursor.is_none() {
            break;
        }
        cursor = next_cursor;
    }

    Ok(rows)
}

pub async fn create_dataset(client: &ApiClient, project_id: &str, name: &str) -> Result<Dataset> {
    let body = serde_json::json!({
        "name": name,
        "project_id": project_id,
        "org_name": client.org_name(),
    });
    client.post("/v1/dataset", &body).await
}

pub async fn get_or_create_dataset(
    client: &ApiClient,
    project_id: &str,
    name: &str,
) -> Result<(Dataset, bool)> {
    if let Some(dataset) = get_dataset_by_name(client, project_id, name).await? {
        return Ok((dataset, false));
    }

    let dataset = create_dataset(client, project_id, name).await?;
    Ok((dataset, true))
}

pub async fn delete_dataset(client: &ApiClient, dataset_id: &str) -> Result<()> {
    let path = format!("/v1/dataset/{}", encode(dataset_id));
    client.delete(&path).await
}

pub async fn list_dataset_versions(
    client: &ApiClient,
    dataset_id: &str,
) -> Result<Vec<DatasetVersion>> {
    let path = build_dataset_versions_path(dataset_id);
    let list: ListResponseGeneric<DatasetVersion> = client.get(&path).await?;
    Ok(list.objects)
}

pub async fn create_dataset_version(
    client: &ApiClient,
    dataset_id: &str,
    name: &str,
    description: Option<&str>,
    xact_id: &str,
) -> Result<DatasetVersion> {
    #[derive(Serialize)]
    struct CreateDatasetVersionRequest<'a> {
        dataset_id: &'a str,
        name: &'a str,
        #[serde(skip_serializing_if = "Option::is_none")]
        description: Option<&'a str>,
        xact_id: &'a str,
    }

    let body = CreateDatasetVersionRequest {
        dataset_id,
        name,
        description,
        xact_id,
    };
    client.post("/v1/dataset_snapshot", &body).await
}

pub async fn preview_dataset_restore(
    client: &ApiClient,
    dataset_id: &str,
    xact_id: &str,
) -> Result<DatasetRestorePreview> {
    let path = build_dataset_restore_preview_path(dataset_id);
    client.post(&path, &restore_dataset_request(xact_id)).await
}

pub async fn restore_dataset(
    client: &ApiClient,
    dataset_id: &str,
    xact_id: &str,
) -> Result<DatasetRestoreResult> {
    let path = build_dataset_restore_path(dataset_id);
    client.post(&path, &restore_dataset_request(xact_id)).await
}

pub async fn get_dataset_head_xact_id(
    client: &ApiClient,
    dataset_id: &str,
) -> Result<Option<String>> {
    let query = build_dataset_head_xact_query(dataset_id);
    let response = client.btql::<DatasetHeadXactRow>(&query).await?;
    let head = response
        .data
        .into_iter()
        .filter_map(|row| row.xact_id)
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
        .max_by(compare_xact_ids);
    Ok(head)
}

fn build_dataset_rows_query(dataset_id: &str, limit: usize, cursor: Option<&str>) -> String {
    let cursor_clause = cursor
        .map(|cursor| format!(" | cursor: {}", btql_quote(cursor)))
        .unwrap_or_default();
    format!(
        "select: * | from: dataset({}) | filter: created >= {} | limit: {}{}",
        sql_quote(dataset_id),
        btql_quote(DATASET_ROWS_SINCE),
        limit,
        cursor_clause
    )
}

fn build_dataset_head_xact_query(dataset_id: &str) -> String {
    format!(
        "select: _xact_id | from: dataset({}) | filter: created >= {} | sort: _xact_id DESC | limit: 1",
        sql_quote(dataset_id),
        btql_quote(DATASET_ROWS_SINCE),
    )
}

fn build_dataset_versions_path(dataset_id: &str) -> String {
    format!("/v1/dataset_snapshot?dataset_id={}", encode(dataset_id))
}

fn build_dataset_restore_preview_path(dataset_id: &str) -> String {
    format!("/v1/dataset/{}/restore/preview", encode(dataset_id))
}

fn build_dataset_restore_path(dataset_id: &str) -> String {
    format!("/v1/dataset/{}/restore", encode(dataset_id))
}

fn restore_dataset_request(xact_id: &str) -> serde_json::Value {
    serde_json::json!({ "version": xact_id })
}

fn btql_quote(value: &str) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\"")))
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn compare_xact_ids(left: &String, right: &String) -> std::cmp::Ordering {
    match (left.parse::<u64>(), right.parse::<u64>()) {
        (Ok(left), Ok(right)) => left.cmp(&right),
        _ => left.cmp(right),
    }
}

#[derive(Debug, Deserialize)]
struct ListResponseGeneric<T> {
    objects: Vec<T>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn dataset_rows_query_includes_required_filter() {
        let query = build_dataset_rows_query("dataset-id", 1000, None);
        assert_eq!(
            query,
            "select: * | from: dataset('dataset-id') | filter: created >= \"1970-01-01T00:00:00Z\" | limit: 1000"
        );
    }

    #[test]
    fn dataset_rows_query_quotes_cursor() {
        let query = build_dataset_rows_query("dataset-id", 200, Some("cursor-123"));
        assert!(query.contains("limit: 200 | cursor: \"cursor-123\""));
    }

    #[test]
    fn dataset_rows_query_escapes_dataset_id() {
        let query = build_dataset_rows_query("dataset'with-quote", 10, None);
        assert!(query.contains("from: dataset('dataset''with-quote')"));
    }

    #[test]
    fn dataset_head_query_includes_required_filter_and_limit() {
        let query = build_dataset_head_xact_query("dataset-id");
        assert_eq!(
            query,
            "select: _xact_id | from: dataset('dataset-id') | filter: created >= \"1970-01-01T00:00:00Z\" | sort: _xact_id DESC | limit: 1"
        );
    }

    #[test]
    fn dataset_head_query_escapes_dataset_id() {
        let query = build_dataset_head_xact_query("dataset'with-quote");
        assert!(query.contains("from: dataset('dataset''with-quote')"));
    }

    #[test]
    fn compare_xact_ids_prefers_numeric_order_when_possible() {
        assert_eq!(
            compare_xact_ids(&"10".to_string(), &"2".to_string()),
            std::cmp::Ordering::Greater
        );
        assert_eq!(
            compare_xact_ids(&"b".to_string(), &"a".to_string()),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    fn dataset_versions_path_omits_org_name() {
        let path = build_dataset_versions_path("dataset-id");
        assert_eq!(path, "/v1/dataset_snapshot?dataset_id=dataset-id");
        assert!(!path.contains("org_name"));
    }

    #[test]
    fn dataset_versions_path_escapes_dataset_id() {
        let path = build_dataset_versions_path("dataset id/with spaces");
        assert_eq!(
            path,
            "/v1/dataset_snapshot?dataset_id=dataset%20id%2Fwith%20spaces"
        );
    }

    #[test]
    fn dataset_restore_preview_path_escapes_dataset_id() {
        let path = build_dataset_restore_preview_path("dataset id/with spaces");
        assert_eq!(
            path,
            "/v1/dataset/dataset%20id%2Fwith%20spaces/restore/preview"
        );
    }

    #[test]
    fn dataset_restore_path_escapes_dataset_id() {
        let path = build_dataset_restore_path("dataset id/with spaces");
        assert_eq!(path, "/v1/dataset/dataset%20id%2Fwith%20spaces/restore");
    }

    #[test]
    fn restore_dataset_request_uses_version_key() {
        assert_eq!(
            restore_dataset_request("1000192656880881099"),
            serde_json::json!({ "version": "1000192656880881099" })
        );
    }

    #[test]
    fn dataset_restore_preview_deserializes_count_fields() {
        let preview: DatasetRestorePreview = serde_json::from_value(serde_json::json!({
            "rows_to_restore": 7,
            "rows_to_delete": 2,
            "note": "preview"
        }))
        .expect("deserialize preview");
        assert_eq!(preview.rows_to_restore, 7);
        assert_eq!(preview.rows_to_delete, 2);
        assert_eq!(
            preview.extra.get("note"),
            Some(&Value::String("preview".to_string()))
        );
    }

    #[test]
    fn dataset_restore_result_deserializes_count_fields() {
        let result: DatasetRestoreResult = serde_json::from_value(serde_json::json!({
            "xact_id": "1000192656880881099",
            "rows_restored": 7,
            "rows_deleted": 2,
            "note": "result"
        }))
        .expect("deserialize result");
        assert_eq!(result.xact_id, "1000192656880881099");
        assert_eq!(result.rows_restored, 7);
        assert_eq!(result.rows_deleted, 2);
        assert_eq!(
            result.extra.get("note"),
            Some(&Value::String("result".to_string()))
        );
    }
}
