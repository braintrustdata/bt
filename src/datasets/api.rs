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

fn btql_quote(value: &str) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| format!("\"{}\"", value.replace('\\', "\\\\").replace('"', "\\\"")))
}

fn sql_quote(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
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
}
