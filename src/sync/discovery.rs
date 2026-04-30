use std::collections::HashSet;

use anyhow::Result;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use crate::auth::LoginContext;
use crate::http::ApiClient;
use crate::sync::execute_btql_json_query;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ProjectLogRefScope {
    Trace,
    Span,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct ProjectLogRef {
    pub(crate) root_span_id: String,
    pub(crate) id: Option<String>,
}

impl ProjectLogRef {
    pub(crate) fn to_value(&self) -> Value {
        match self.id.as_deref() {
            Some(id) => json!({ "root_span_id": self.root_span_id, "id": id }),
            None => json!({ "root_span_id": self.root_span_id }),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProjectLogRefDiscoveryResult {
    pub(crate) refs: usize,
    pub(crate) pages: usize,
}

#[derive(Debug, Deserialize)]
struct DiscoveryBtqlResponse {
    data: Vec<Map<String, Value>>,
    #[serde(default)]
    cursor: Option<String>,
}

pub(crate) async fn discover_project_log_refs<F>(
    client: &ApiClient,
    ctx: &LoginContext,
    project_id: &str,
    filter: Option<&Value>,
    scope: ProjectLogRefScope,
    target: usize,
    page_size: usize,
    mut on_ref: F,
) -> Result<ProjectLogRefDiscoveryResult>
where
    F: FnMut(ProjectLogRef) -> Result<()>,
{
    let mut seen = HashSet::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    while seen.len() < target {
        let limit = discovery_page_limit(scope, target - seen.len(), page_size);
        let query =
            build_project_log_ref_query(project_id, filter, limit, cursor.as_deref(), scope);
        let response = execute_discovery_btql(client, ctx, &query).await?;
        let row_count = response.data.len();

        for row in response.data {
            if seen.len() >= target {
                break;
            }
            let Some(reference) = project_log_ref_from_row(&row, scope) else {
                continue;
            };
            if seen.insert(reference.clone()) {
                on_ref(reference)?;
            }
        }

        pages += 1;
        cursor = response.cursor.filter(|c| !c.is_empty());
        if row_count == 0 || cursor.is_none() {
            break;
        }
    }
    Ok(ProjectLogRefDiscoveryResult {
        refs: seen.len(),
        pages,
    })
}

fn discovery_page_limit(scope: ProjectLogRefScope, remaining: usize, page_size: usize) -> usize {
    match scope {
        ProjectLogRefScope::Trace => page_size.min(1000),
        ProjectLogRefScope::Span => remaining.min(page_size).min(1000),
    }
}

async fn execute_discovery_btql(
    client: &ApiClient,
    ctx: &LoginContext,
    query: &Value,
) -> Result<DiscoveryBtqlResponse> {
    execute_btql_json_query(client, ctx, query, "bt_sync_discovery").await
}

fn build_project_log_ref_query(
    project_id: &str,
    filter: Option<&Value>,
    page_size: usize,
    cursor: Option<&str>,
    scope: ProjectLogRefScope,
) -> Value {
    let select = match scope {
        ProjectLogRefScope::Trace => vec![btql_select_field("root_span_id")],
        ProjectLogRefScope::Span => {
            vec![btql_select_field("root_span_id"), btql_select_field("id")]
        }
    };

    let mut query = json!({
        "select": select,
        "from": {
            "op": "function",
            "name": { "op": "ident", "name": ["project_logs"] },
            "args": [{ "op": "literal", "value": project_id }],
            "shape": "spans"
        },
        "limit": page_size,
        "sort": [{
            "expr": { "op": "ident", "name": ["_pagination_key"] },
            "dir": "desc"
        }]
    });

    if let Some(filter_expr) = filter {
        query["filter"] = filter_expr.clone();
    }
    if let Some(c) = cursor {
        query["cursor"] = Value::String(c.to_string());
    }
    query
}

fn project_log_ref_from_row(
    row: &Map<String, Value>,
    scope: ProjectLogRefScope,
) -> Option<ProjectLogRef> {
    let root_span_id = row_string(row, "root_span_id")?;
    match scope {
        ProjectLogRefScope::Trace => Some(ProjectLogRef {
            root_span_id,
            id: None,
        }),
        ProjectLogRefScope::Span => Some(ProjectLogRef {
            root_span_id,
            id: Some(row_string(row, "id")?),
        }),
    }
}

fn btql_select_field(field: &str) -> Value {
    json!({
        "alias": field,
        "expr": { "op": "ident", "name": [field] }
    })
}

fn row_string(row: &Map<String, Value>, key: &str) -> Option<String> {
    row.get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn project_log_ref_from_row_uses_trace_scope() {
        let row = Map::from_iter([
            ("root_span_id".to_string(), json!("root-1")),
            ("id".to_string(), json!("span-1")),
        ]);

        assert_eq!(
            project_log_ref_from_row(&row, ProjectLogRefScope::Trace),
            Some(ProjectLogRef {
                root_span_id: "root-1".to_string(),
                id: None,
            })
        );
    }

    #[test]
    fn project_log_ref_from_row_uses_span_scope() {
        let row = Map::from_iter([
            ("root_span_id".to_string(), json!("root-1")),
            ("id".to_string(), json!("span-1")),
        ]);

        assert_eq!(
            project_log_ref_from_row(&row, ProjectLogRefScope::Span),
            Some(ProjectLogRef {
                root_span_id: "root-1".to_string(),
                id: Some("span-1".to_string()),
            })
        );
    }

    #[test]
    fn span_scope_page_limit_uses_remaining_target() {
        assert_eq!(discovery_page_limit(ProjectLogRefScope::Span, 3, 1000), 3);
    }

    #[test]
    fn trace_scope_page_limit_keeps_full_page_for_dedupe() {
        assert_eq!(
            discovery_page_limit(ProjectLogRefScope::Trace, 3, 1000),
            1000
        );
    }
}
