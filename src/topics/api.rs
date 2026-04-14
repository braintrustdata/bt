use std::collections::BTreeMap;

use anyhow::Result;
use chrono::{DateTime, Utc};
use serde::Serialize;
use serde_json::Value;
use urlencoding::encode;

use crate::{http::ApiClient, projects::context::ProjectContext};

#[derive(Debug, Clone, Serialize)]
pub struct TopicsStatusReport {
    pub project: TopicsProjectSummary,
    pub automations: Vec<TopicAutomationStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicsProjectSummary {
    pub id: String,
    pub name: String,
    pub org_name: String,
    pub topics_url: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicAutomationStatus {
    pub id: String,
    pub name: String,
    pub description: String,
    pub scope_type: Option<String>,
    pub btql_filter: Option<String>,
    pub window_seconds: Option<i64>,
    pub rerun_seconds: Option<i64>,
    pub relabel_overlap_seconds: Option<i64>,
    pub idle_seconds: Option<i64>,
    pub configured_facets: usize,
    pub configured_topic_maps: usize,
    pub cursor: AutomationCursorSnapshot,
    pub object_cursor: ObjectAutomationCursorSnapshot,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct AutomationCursorSnapshot {
    pub total_segments: usize,
    pub pending_segments: usize,
    pub error_segments: usize,
    pub pending_min_compacted_xact_id: Option<String>,
    pub pending_max_compacted_xact_id: Option<String>,
    pub pending_min_executed_xact_id: Option<String>,
}

#[derive(Debug, Clone, Default, Serialize)]
pub struct ObjectAutomationCursorSnapshot {
    pub total_objects: usize,
    pub due_objects: usize,
    pub error_objects: usize,
    pub last_compacted_xact_id: Option<String>,
    pub next_run_at: Option<String>,
    pub last_run_at: Option<String>,
    pub retry_after: Option<String>,
    pub last_error: Option<String>,
    pub last_error_at: Option<String>,
    pub topic_runtime: Option<TopicRuntimeSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicRuntimeSnapshot {
    pub state: String,
    pub reason: Option<String>,
    pub entered_at: Option<String>,
    pub selected_window_seconds: Option<i64>,
    pub generation_window_start_xact_id: Option<String>,
    pub generation_window_end_xact_id: Option<String>,
    pub topic_classification_backfill_start_xact_id: Option<String>,
    pub active_topic_map_versions: BTreeMap<String, String>,
    pub window_candidates: Vec<TopicWindowCandidateSnapshot>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicWindowCandidateSnapshot {
    pub window_seconds: i64,
    pub ready_topic_maps: usize,
    pub total_topic_maps: usize,
}

pub async fn fetch_topics_status(ctx: &ProjectContext) -> Result<TopicsStatusReport> {
    let mut automations = list_topic_automations(&ctx.client, &ctx.project.id).await?;
    automations.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

    Ok(TopicsStatusReport {
        project: TopicsProjectSummary {
            id: ctx.project.id.clone(),
            name: ctx.project.name.clone(),
            org_name: ctx.client.org_name().to_string(),
            topics_url: topics_url(&ctx.app_url, ctx.client.org_name(), &ctx.project.name),
        },
        automations,
    })
}

pub fn topics_url(app_url: &str, org_name: &str, project_name: &str) -> String {
    format!(
        "{}/app/{}/p/{}/topics",
        app_url.trim_end_matches('/'),
        encode(org_name),
        encode(project_name)
    )
}

async fn list_topic_automations(
    client: &ApiClient,
    project_id: &str,
) -> Result<Vec<TopicAutomationStatus>> {
    let path = format!("/v1/project_automation?project_id={}", encode(project_id));
    let response: Value = client.get(&path).await?;

    let mut automations = Vec::new();
    for row in extract_objects(&response) {
        let Some(config) = row.get("config").and_then(Value::as_object) else {
            continue;
        };
        if string_value(config.get("event_type")) != Some("topic".to_string()) {
            continue;
        }

        let id = stringish_value(row.get("id")).unwrap_or_default();
        if id.is_empty() {
            continue;
        }

        let scope = config.get("scope").and_then(Value::as_object);
        let cursor = fetch_cursor_snapshot(client, project_id, &id).await?;
        let object_cursor = fetch_object_cursor_snapshot(client, project_id, &id).await?;

        automations.push(TopicAutomationStatus {
            id,
            name: string_value(row.get("name")).unwrap_or_else(|| "Topics".to_string()),
            description: string_value(row.get("description")).unwrap_or_default(),
            scope_type: scope.and_then(|scope| string_value(scope.get("type"))),
            btql_filter: string_value(config.get("btql_filter")),
            window_seconds: backfill_time_range_to_window_seconds(
                config.get("backfill_time_range"),
            ),
            rerun_seconds: int_value(config.get("rerun_seconds")),
            relabel_overlap_seconds: int_value(config.get("relabel_overlap_seconds")),
            idle_seconds: scope.and_then(|scope| int_value(scope.get("idle_seconds"))),
            configured_facets: config
                .get("facet_functions")
                .and_then(Value::as_array)
                .map_or(0, Vec::len),
            configured_topic_maps: config
                .get("topic_map_functions")
                .and_then(Value::as_array)
                .map_or(0, Vec::len),
            cursor,
            object_cursor,
        });
    }

    Ok(automations)
}

async fn fetch_cursor_snapshot(
    client: &ApiClient,
    project_id: &str,
    automation_id: &str,
) -> Result<AutomationCursorSnapshot> {
    let body = serde_json::json!({
        "automation_id": automation_id,
        "project_id": project_id,
    });
    let response: Value = client
        .post("/brainstore/automation/get-cursors", &body)
        .await?;
    let map = response.as_object();

    Ok(AutomationCursorSnapshot {
        total_segments: usize_value(map.and_then(|map| map.get("total_segments"))),
        pending_segments: usize_value(map.and_then(|map| map.get("pending_segments"))),
        error_segments: usize_value(map.and_then(|map| map.get("error_segments"))),
        pending_min_compacted_xact_id: stringish_value(
            map.and_then(|map| map.get("pending_min_compacted_xact_id")),
        ),
        pending_max_compacted_xact_id: stringish_value(
            map.and_then(|map| map.get("pending_max_compacted_xact_id")),
        ),
        pending_min_executed_xact_id: stringish_value(
            map.and_then(|map| map.get("pending_min_executed_xact_id")),
        ),
    })
}

async fn fetch_object_cursor_snapshot(
    client: &ApiClient,
    project_id: &str,
    automation_id: &str,
) -> Result<ObjectAutomationCursorSnapshot> {
    let body = serde_json::json!({
        "automation_id": automation_id,
        "project_id": project_id,
    });
    let response: Value = client
        .post("/brainstore/automation/get-object-cursors", &body)
        .await?;
    let map = response.as_object();

    Ok(ObjectAutomationCursorSnapshot {
        total_objects: usize_value(map.and_then(|map| map.get("total_objects"))),
        due_objects: usize_value(map.and_then(|map| map.get("due_objects"))),
        error_objects: usize_value(map.and_then(|map| map.get("error_objects"))),
        last_compacted_xact_id: stringish_value(
            map.and_then(|map| map.get("last_compacted_xact_id")),
        ),
        next_run_at: string_value(map.and_then(|map| map.get("next_run_at"))),
        last_run_at: string_value(map.and_then(|map| map.get("last_run_at"))),
        retry_after: string_value(map.and_then(|map| map.get("retry_after"))),
        last_error: string_value(map.and_then(|map| map.get("last_error"))),
        last_error_at: string_value(map.and_then(|map| map.get("last_error_at"))),
        topic_runtime: topic_runtime_from_value(map.and_then(|map| map.get("topic_runtime"))),
    })
}

fn topic_runtime_from_value(value: Option<&Value>) -> Option<TopicRuntimeSnapshot> {
    let map = value?.as_object()?;

    let active_topic_map_versions = map
        .get("active_topic_map_versions")
        .and_then(Value::as_object)
        .map(|versions| {
            versions
                .iter()
                .filter_map(|(key, value)| {
                    stringish_value(Some(value)).map(|value| (key.clone(), value))
                })
                .collect::<BTreeMap<_, _>>()
        })
        .unwrap_or_default();

    let window_candidates = map
        .get("window_candidates")
        .and_then(Value::as_array)
        .map(|candidates| {
            candidates
                .iter()
                .filter_map(|candidate| {
                    let candidate = candidate.as_object()?;
                    Some(TopicWindowCandidateSnapshot {
                        window_seconds: int_value(candidate.get("window_seconds")).unwrap_or(0),
                        ready_topic_maps: usize_value(candidate.get("ready_topic_maps")),
                        total_topic_maps: usize_value(candidate.get("total_topic_maps")),
                    })
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Some(TopicRuntimeSnapshot {
        state: string_value(map.get("state")).unwrap_or_else(|| "waiting_for_facets".to_string()),
        reason: string_value(map.get("reason")),
        entered_at: string_value(map.get("entered_at")),
        selected_window_seconds: int_value(map.get("selected_window_seconds")),
        generation_window_start_xact_id: stringish_value(
            map.get("generation_window_start_xact_id"),
        ),
        generation_window_end_xact_id: stringish_value(map.get("generation_window_end_xact_id")),
        topic_classification_backfill_start_xact_id: stringish_value(
            map.get("topic_classification_backfill_start_xact_id"),
        ),
        active_topic_map_versions,
        window_candidates,
    })
}

fn extract_objects(value: &Value) -> &[Value] {
    value
        .get("objects")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn string_value(value: Option<&Value>) -> Option<String> {
    value.and_then(Value::as_str).map(ToString::to_string)
}

fn stringish_value(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(value)) if !value.is_empty() => Some(value.clone()),
        Some(Value::Number(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn int_value(value: Option<&Value>) -> Option<i64> {
    match value {
        Some(Value::Number(number)) => number
            .as_i64()
            .or_else(|| number.as_u64().and_then(|value| i64::try_from(value).ok())),
        Some(Value::String(value)) => value.parse::<i64>().ok(),
        _ => None,
    }
}

fn usize_value(value: Option<&Value>) -> usize {
    int_value(value)
        .and_then(|value| usize::try_from(value).ok())
        .unwrap_or(0)
}

fn backfill_time_range_to_window_seconds(value: Option<&Value>) -> Option<i64> {
    let value = value?;
    if let Some(value) = value.as_str() {
        if let Some(interval_ms) = value.strip_prefix("interval_ms:") {
            let interval_ms = interval_ms.parse::<i64>().ok()?;
            return Some(std::cmp::max(
                60,
                (interval_ms as f64 / 1000.0).round() as i64,
            ));
        }
        return parse_duration_to_seconds(value);
    }

    let map = value.as_object()?;
    let from = map.get("from")?.as_str()?;
    let to = map.get("to")?.as_str()?;
    let from = DateTime::parse_from_rfc3339(from).ok()?.with_timezone(&Utc);
    let to = DateTime::parse_from_rfc3339(to).ok()?.with_timezone(&Utc);
    Some(std::cmp::max(0, (to - from).num_seconds()))
}

fn parse_duration_to_seconds(value: &str) -> Option<i64> {
    let value = value.trim();
    if value.is_empty() {
        return None;
    }

    let suffix = value.chars().last().filter(|ch| ch.is_ascii_alphabetic());
    let (number, unit) = match suffix {
        Some(unit) => (&value[..value.len() - unit.len_utf8()], unit),
        None => (value, 's'),
    };
    let amount = number.trim().parse::<i64>().ok()?;
    let multiplier = match unit.to_ascii_lowercase() {
        's' => 1,
        'm' => 60,
        'h' => 60 * 60,
        'd' => 24 * 60 * 60,
        'w' => 7 * 24 * 60 * 60,
        _ => return None,
    };
    Some(amount * multiplier)
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn topics_url_uses_app_url_directly() {
        assert_eq!(
            topics_url("https://www.example.com", "test org", "my project"),
            "https://www.example.com/app/test%20org/p/my%20project/topics"
        );
    }

    #[test]
    fn backfill_time_range_supports_duration_strings_and_intervals() {
        assert_eq!(
            backfill_time_range_to_window_seconds(Some(&json!("6h"))),
            Some(21600)
        );
        assert_eq!(
            backfill_time_range_to_window_seconds(Some(&json!("interval_ms:90000"))),
            Some(90)
        );
    }

    #[test]
    fn backfill_time_range_supports_absolute_ranges() {
        let range = json!({
            "from": "2026-04-13T10:00:00Z",
            "to": "2026-04-13T11:30:00Z",
        });
        assert_eq!(
            backfill_time_range_to_window_seconds(Some(&range)),
            Some(5400)
        );
    }

    #[test]
    fn topic_runtime_normalizes_numbers_to_strings() {
        let runtime = topic_runtime_from_value(Some(&json!({
            "state": "idle",
            "generation_window_start_xact_id": 9990001112220000_u64,
            "active_topic_map_versions": {
                "func_1": 3,
                "func_2": "v7"
            },
            "window_candidates": [
                {
                    "window_seconds": 3600,
                    "ready_topic_maps": 1,
                    "total_topic_maps": 2
                }
            ]
        })))
        .expect("runtime");

        assert_eq!(
            runtime.generation_window_start_xact_id.as_deref(),
            Some("9990001112220000")
        );
        assert_eq!(
            runtime
                .active_topic_map_versions
                .get("func_1")
                .map(String::as_str),
            Some("3")
        );
        assert_eq!(runtime.window_candidates.len(), 1);
    }
}
