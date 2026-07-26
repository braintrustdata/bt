use std::collections::{HashMap, HashSet};

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

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct ProjectLogRef {
    pub(crate) root_span_id: String,
    pub(crate) id: Option<String>,
    pub(crate) origin: Option<Value>,
    origin_is_root: bool,
    origin_created: Option<String>,
}

impl ProjectLogRef {
    pub(crate) fn to_value(&self) -> Value {
        let mut reference = Map::new();
        reference.insert(
            "root_span_id".to_string(),
            Value::String(self.root_span_id.clone()),
        );
        if let Some(id) = self.id.as_deref() {
            reference.insert("id".to_string(), Value::String(id.to_string()));
        }
        if let Some(origin) = self.origin.as_ref() {
            reference.insert("origin".to_string(), origin.clone());
        }
        Value::Object(reference)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ProjectLogRefDiscoveryResult {
    pub(crate) refs: usize,
    pub(crate) pages: usize,
}

pub(crate) struct ProjectLogRefDiscoveryOptions<'a> {
    pub(crate) project_id: &'a str,
    pub(crate) filter: Option<&'a Value>,
    pub(crate) scope: ProjectLogRefScope,
    pub(crate) target: usize,
    pub(crate) page_size: usize,
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
    options: ProjectLogRefDiscoveryOptions<'_>,
    mut on_ref: F,
) -> Result<ProjectLogRefDiscoveryResult>
where
    F: FnMut(ProjectLogRef) -> Result<()>,
{
    let ProjectLogRefDiscoveryOptions {
        project_id,
        filter,
        scope,
        target,
        page_size,
    } = options;
    let mut seen = HashSet::new();
    let mut trace_roots = Vec::new();
    let mut trace_refs_by_root_span_id = HashMap::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0usize;
    while discovered_ref_count(scope, seen.len(), trace_roots.len()) < target {
        let remaining = target - discovered_ref_count(scope, seen.len(), trace_roots.len());
        let limit = discovery_page_limit(scope, remaining, page_size);
        let query =
            build_project_log_ref_query(project_id, filter, limit, cursor.as_deref(), scope);
        let response = execute_discovery_btql(client, ctx, &query).await?;
        let row_count = response.data.len();

        for row in response.data {
            if matches!(scope, ProjectLogRefScope::Span) && seen.len() >= target {
                break;
            }
            let Some(reference) = project_log_ref_from_row(project_id, &row, scope) else {
                continue;
            };
            match scope {
                ProjectLogRefScope::Span => {
                    if seen.insert(project_log_ref_key(&reference)) {
                        on_ref(reference)?;
                    }
                }
                ProjectLogRefScope::Trace => {
                    let root_span_id = reference.root_span_id.clone();
                    match trace_refs_by_root_span_id.entry(root_span_id) {
                        std::collections::hash_map::Entry::Vacant(entry) => {
                            if trace_roots.len() >= target {
                                continue;
                            }
                            trace_roots.push(entry.key().clone());
                            entry.insert(reference);
                        }
                        std::collections::hash_map::Entry::Occupied(mut entry) => {
                            if better_trace_origin_ref(entry.get(), &reference) {
                                entry.insert(reference);
                            }
                        }
                    }
                }
            }
        }

        pages += 1;
        cursor = response.cursor.filter(|c| !c.is_empty());
        if row_count == 0 || cursor.is_none() {
            break;
        }
    }
    if matches!(scope, ProjectLogRefScope::Trace) {
        for root_span_id in &trace_roots {
            if let Some(reference) = trace_refs_by_root_span_id.remove(root_span_id) {
                on_ref(reference)?;
            }
        }
    }

    Ok(ProjectLogRefDiscoveryResult {
        refs: discovered_ref_count(scope, seen.len(), trace_roots.len()),
        pages,
    })
}

fn discovered_ref_count(scope: ProjectLogRefScope, span_refs: usize, trace_refs: usize) -> usize {
    match scope {
        ProjectLogRefScope::Trace => trace_refs,
        ProjectLogRefScope::Span => span_refs,
    }
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
        ProjectLogRefScope::Trace => vec![
            btql_select_field("root_span_id"),
            btql_select_field("id"),
            btql_select_field("is_root"),
            btql_select_field("created"),
            btql_select_field("_xact_id"),
        ],
        ProjectLogRefScope::Span => {
            vec![
                btql_select_field("root_span_id"),
                btql_select_field("id"),
                btql_select_field("created"),
                btql_select_field("_xact_id"),
            ]
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
    project_id: &str,
    row: &Map<String, Value>,
    scope: ProjectLogRefScope,
) -> Option<ProjectLogRef> {
    let root_span_id = row_string(row, "root_span_id")?;
    match scope {
        ProjectLogRefScope::Trace => Some(ProjectLogRef {
            root_span_id,
            id: None,
            origin: project_log_origin_from_row(project_id, row),
            origin_is_root: row_bool(row, "is_root"),
            origin_created: row_string(row, "created"),
        }),
        ProjectLogRefScope::Span => Some(ProjectLogRef {
            root_span_id,
            id: Some(row_string(row, "id")?),
            origin: project_log_origin_from_row(project_id, row),
            origin_is_root: row_bool(row, "is_root"),
            origin_created: row_string(row, "created"),
        }),
    }
}

fn btql_select_field(field: &str) -> Value {
    json!({
        "alias": field,
        "expr": { "op": "ident", "name": [field] }
    })
}

fn project_log_ref_key(reference: &ProjectLogRef) -> (String, Option<String>) {
    (reference.root_span_id.clone(), reference.id.clone())
}

fn row_string(row: &Map<String, Value>, key: &str) -> Option<String> {
    row.get(key)
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn row_bool(row: &Map<String, Value>, key: &str) -> bool {
    row.get(key).and_then(Value::as_bool).unwrap_or(false)
}

fn better_trace_origin_ref(current: &ProjectLogRef, candidate: &ProjectLogRef) -> bool {
    match (current.origin_is_root, candidate.origin_is_root) {
        (false, true) => return true,
        (true, false) => return false,
        _ => {}
    }

    match (&current.origin_created, &candidate.origin_created) {
        (Some(current_created), Some(candidate_created)) => candidate_created < current_created,
        (None, Some(_)) => true,
        _ => false,
    }
}

fn project_log_origin_from_row(project_id: &str, row: &Map<String, Value>) -> Option<Value> {
    let row_id = row_string(row, "id")?;
    Some(object_origin(
        "project_logs",
        project_id,
        &row_id,
        row.get("created").and_then(Value::as_str),
        row.get("_xact_id").and_then(Value::as_str),
    ))
}

fn object_origin(
    object_type: &str,
    object_id: &str,
    row_id: &str,
    created: Option<&str>,
    xact_id: Option<&str>,
) -> Value {
    let mut origin = Map::from_iter([
        (
            "object_type".to_string(),
            Value::String(object_type.to_string()),
        ),
        (
            "object_id".to_string(),
            Value::String(object_id.to_string()),
        ),
        ("id".to_string(), Value::String(row_id.to_string())),
    ]);
    if let Some(created) = created {
        origin.insert("created".to_string(), Value::String(created.to_string()));
    }
    if let Some(xact_id) = xact_id {
        origin.insert("_xact_id".to_string(), Value::String(xact_id.to_string()));
    }
    Value::Object(origin)
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
            project_log_ref_from_row("project-1", &row, ProjectLogRefScope::Trace),
            Some(ProjectLogRef {
                root_span_id: "root-1".to_string(),
                id: None,
                origin: Some(json!({
                    "object_type": "project_logs",
                    "object_id": "project-1",
                    "id": "span-1"
                })),
                origin_is_root: false,
                origin_created: None,
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
            project_log_ref_from_row("project-1", &row, ProjectLogRefScope::Span),
            Some(ProjectLogRef {
                root_span_id: "root-1".to_string(),
                id: Some("span-1".to_string()),
                origin: Some(json!({
                    "object_type": "project_logs",
                    "object_id": "project-1",
                    "id": "span-1"
                })),
                origin_is_root: false,
                origin_created: None,
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

    #[test]
    fn project_log_origin_from_row_includes_optional_position_fields() {
        let row = Map::from_iter([
            ("id".to_string(), json!("row-1")),
            ("created".to_string(), json!("2026-01-01T00:00:00Z")),
            ("_xact_id".to_string(), json!("100")),
        ]);

        assert_eq!(
            project_log_origin_from_row("project-1", &row),
            Some(json!({
                "object_type": "project_logs",
                "object_id": "project-1",
                "id": "row-1",
                "created": "2026-01-01T00:00:00Z",
                "_xact_id": "100"
            }))
        );
    }

    #[test]
    fn object_origin_supports_arbitrary_source_objects() {
        assert_eq!(
            object_origin("dataset", "dataset-1", "row-1", None, None),
            json!({
                "object_type": "dataset",
                "object_id": "dataset-1",
                "id": "row-1"
            })
        );
    }

    #[test]
    fn trace_origin_ref_prefers_is_root_over_earliest_created() {
        let current = ProjectLogRef {
            root_span_id: "root-1".to_string(),
            id: None,
            origin: Some(json!({ "id": "earliest" })),
            origin_is_root: false,
            origin_created: Some("2026-01-01T00:00:00Z".to_string()),
        };
        let candidate = ProjectLogRef {
            root_span_id: "root-1".to_string(),
            id: None,
            origin: Some(json!({ "id": "root" })),
            origin_is_root: true,
            origin_created: Some("2026-01-02T00:00:00Z".to_string()),
        };

        assert!(better_trace_origin_ref(&current, &candidate));
    }

    #[test]
    fn trace_origin_ref_uses_earliest_created_without_is_root() {
        let current = ProjectLogRef {
            root_span_id: "root-1".to_string(),
            id: None,
            origin: Some(json!({ "id": "later" })),
            origin_is_root: false,
            origin_created: Some("2026-01-02T00:00:00Z".to_string()),
        };
        let candidate = ProjectLogRef {
            root_span_id: "root-1".to_string(),
            id: None,
            origin: Some(json!({ "id": "earlier" })),
            origin_is_root: false,
            origin_created: Some("2026-01-01T00:00:00Z".to_string()),
        };

        assert!(better_trace_origin_ref(&current, &candidate));
    }
}
