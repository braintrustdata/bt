use std::collections::HashSet;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use urlencoding::encode;

use crate::http::ApiClient;

const MAX_DATASET_ROWS_PAGE_LIMIT: usize = 1000;
const MAX_DATASET_ROWS_PAGES: usize = 10_000;
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
) -> Result<(Vec<DatasetRow>, bool)> {
    if matches!(max_rows, Some(0)) {
        return Ok((Vec::new(), false));
    }

    let mut rows = Vec::new();
    let mut cursor: Option<String> = None;
    let mut seen_cursors = HashSet::new();
    let mut page_count = 0usize;
    let mut truncated = false;

    loop {
        let Some(page_limit) = resolve_dataset_rows_page_limit(max_rows, rows.len()) else {
            truncated = true;
            break;
        };

        page_count += 1;
        if page_count > MAX_DATASET_ROWS_PAGES {
            bail!(
                "dataset rows pagination exceeded {} pages for dataset '{}'",
                MAX_DATASET_ROWS_PAGES,
                dataset_id
            );
        }
        if let Some(current_cursor) = cursor.as_ref() {
            if !seen_cursors.insert(current_cursor.clone()) {
                bail!(
                    "dataset rows pagination loop detected for dataset '{}'",
                    dataset_id
                );
            }
        }

        let query = build_dataset_rows_query(dataset_id, page_limit, cursor.as_deref());
        let response = client.btql::<DatasetRow>(&query).await?;

        let next_cursor = response.cursor.filter(|cursor| !cursor.is_empty());

        if response.data.is_empty() {
            if next_cursor.is_some() {
                bail!(
                    "dataset rows response for '{}' returned an empty page with a cursor",
                    dataset_id
                );
            }
            break;
        }

        if let Some(max_rows) = max_rows {
            let remaining = max_rows.saturating_sub(rows.len());
            if remaining == 0 {
                truncated = true;
                break;
            }
            if response.data.len() > remaining {
                rows.extend(response.data.into_iter().take(remaining));
                truncated = true;
                break;
            }
        }

        rows.extend(response.data);

        match next_cursor {
            Some(next_cursor) => {
                if max_rows.is_some_and(|max_rows| rows.len() >= max_rows) {
                    truncated = true;
                    break;
                }
                cursor = Some(next_cursor);
            }
            None => break,
        }
    }

    Ok((rows, truncated))
}

pub async fn create_dataset(
    client: &ApiClient,
    project_id: &str,
    name: &str,
    description: Option<&str>,
) -> Result<Dataset> {
    let mut body = serde_json::json!({
        "name": name,
        "project_id": project_id,
        "org_name": client.org_name(),
    });
    if let Some(description) = description.filter(|description| !description.is_empty()) {
        body["description"] = serde_json::Value::String(description.to_string());
    }
    client.post("/v1/dataset", &body).await
}

pub async fn delete_dataset(client: &ApiClient, dataset_id: &str) -> Result<()> {
    let path = format!("/v1/dataset/{}", encode(dataset_id));
    client.delete(&path).await
}

fn resolve_dataset_rows_page_limit(max_rows: Option<usize>, loaded_rows: usize) -> Option<usize> {
    match max_rows {
        None => Some(MAX_DATASET_ROWS_PAGE_LIMIT),
        Some(max_rows) => {
            let remaining = max_rows.saturating_sub(loaded_rows);
            if remaining == 0 {
                None
            } else {
                Some(remaining.min(MAX_DATASET_ROWS_PAGE_LIMIT))
            }
        }
    }
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

    #[test]
    fn dataset_rows_page_limit_defaults_to_api_max() {
        assert_eq!(
            resolve_dataset_rows_page_limit(None, 0),
            Some(MAX_DATASET_ROWS_PAGE_LIMIT)
        );
    }

    #[test]
    fn dataset_rows_page_limit_caps_to_remaining() {
        assert_eq!(resolve_dataset_rows_page_limit(Some(200), 0), Some(200));
        assert_eq!(resolve_dataset_rows_page_limit(Some(1500), 600), Some(900));
    }

    #[test]
    fn dataset_rows_page_limit_stops_when_limit_reached() {
        assert_eq!(resolve_dataset_rows_page_limit(Some(200), 200), None);
    }
}
