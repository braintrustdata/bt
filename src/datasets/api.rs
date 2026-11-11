use anyhow::{Context, Result};
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::http::{ApiClient, BtqlResponse, Pagination};

use super::records::DATASET_RECORD_FIELDS;

const MAX_DATASET_ROWS_PAGE_LIMIT: usize = 1000;
const DATASET_ROWS_SINCE: &str = "1970-01-01T00:00:00Z";

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

pub use braintrust_api_client::backend::extensions::SnapshotCreation as CreateDatasetSnapshotResult;
pub use braintrust_api_client::types::{
    Dataset, DatasetRestorePreviewResponse as DatasetRestorePreview,
    DatasetRestoreResponse as DatasetRestoreResult, DatasetSnapshot,
};
pub type DatasetRow = Map<String, Value>;

pub fn description_text(dataset: &Dataset) -> Option<&str> {
    dataset
        .description
        .as_ref()
        .and_then(|v| v.as_deref())
        .filter(|v| !v.is_empty())
        .or_else(|| {
            dataset
                .metadata
                .as_ref()
                .and_then(|v| v.as_ref())
                .and_then(|v| v.additional_properties.get("description"))
                .and_then(Value::as_str)
                .filter(|v| !v.is_empty())
        })
}
pub fn created_text(dataset: &Dataset) -> Option<String> {
    dataset
        .created
        .flatten()
        .or(dataset.created_at.flatten())
        .map(|value| value.to_rfc3339())
}

#[derive(Debug, Deserialize)]
struct DatasetHeadXactRow {
    #[serde(rename = "_xact_id", default)]
    xact_id: Option<String>,
}

pub async fn list_datasets(client: &ApiClient, project_id: &str) -> Result<Vec<Dataset>> {
    let response = client
        .run(|| {
            client
                .generated()
                .get_dataset_builder()
                .org_name(client.org_name())
                .project_id(project_id)
                .send()
        })
        .await?;
    Ok(response.objects)
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
    let mut request =
        braintrust_api_client::types::CreateDataset::new(name.to_owned(), project_id.parse()?);
    request.description = description
        .filter(|v| !v.is_empty())
        .map(|v| Some(v.to_owned()));
    request.metadata = metadata
        .map(braintrust_api_client::backend::resource_definition)
        .transpose()?
        .map(Some);
    client
        .run(|| client.generated().post_dataset(request.clone()))
        .await
}

pub async fn delete_dataset(client: &ApiClient, dataset_id: &str) -> Result<()> {
    client
        .run(|| client.generated().delete_dataset_id(dataset_id))
        .await?;
    Ok(())
}

pub async fn list_dataset_snapshots(
    client: &ApiClient,
    dataset_id: &str,
) -> Result<Vec<DatasetSnapshot>> {
    Ok(client.dataset_snapshots(dataset_id).await?.objects)
}

pub async fn create_dataset_snapshot(
    client: &ApiClient,
    dataset_id: &str,
    name: &str,
    description: Option<&str>,
    xact_id: &str,
) -> Result<CreateDatasetSnapshotResult> {
    let mut request = braintrust_api_client::types::CreateDatasetSnapshot::new(
        dataset_id.parse()?,
        name.to_owned(),
        xact_id.to_owned(),
    );
    request.description = description.map(|v| Some(v.to_owned()));
    client.create_dataset_snapshot(&request).await
}

pub async fn delete_dataset_snapshot(client: &ApiClient, snapshot_id: &str) -> Result<()> {
    client
        .run(|| client.generated().delete_dataset_snapshot_id(snapshot_id))
        .await?;
    Ok(())
}

pub async fn preview_dataset_restore(
    client: &ApiClient,
    dataset_id: &str,
    xact_id: &str,
) -> Result<DatasetRestorePreview> {
    let request = braintrust_api_client::types::DatasetRestoreRequest {
        version: xact_id.to_owned(),
    };
    client
        .run(|| {
            client
                .generated()
                .post_dataset_id_restore_preview(dataset_id, request.clone())
        })
        .await
}

pub async fn restore_dataset(
    client: &ApiClient,
    dataset_id: &str,
    xact_id: &str,
) -> Result<DatasetRestoreResult> {
    client.restore_dataset(dataset_id, xact_id).await
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
    fn dataset_creation_date_uses_legacy_fallback() {
        let mut dataset: Dataset = serde_json::from_value(serde_json::json!({
            "id": "00000000-0000-4000-8000-000000000001",
            "project_id": "00000000-0000-4000-8000-000000000002",
            "name": "test-dataset", "url_slug": "test-dataset",
            "created_at": "2024-01-02T03:04:05Z"
        }))
        .unwrap();
        assert_eq!(
            created_text(&dataset).as_deref(),
            Some("2024-01-02T03:04:05+00:00")
        );
        dataset.created = Some(Some("2024-02-03T04:05:06Z".parse().unwrap()));
        assert_eq!(
            created_text(&dataset).as_deref(),
            Some("2024-02-03T04:05:06+00:00")
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

        assert_eq!(
            snapshot.dataset_id.to_string(),
            "01926568-8088-7109-99ab-abcdef012345"
        );
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
}
