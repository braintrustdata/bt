use std::collections::{BTreeMap, HashMap};

use anyhow::{bail, Result};
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
pub struct TopicsPokeReport {
    pub project: TopicsProjectSummary,
    pub queued: Vec<TopicAutomationPokeResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicsRewindReport {
    pub project: TopicsProjectSummary,
    pub rewound: Vec<TopicAutomationRewindResult>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicsConfigReport {
    pub project: TopicsProjectSummary,
    pub automations: Vec<TopicAutomationConfig>,
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
    pub processing_lag_label: Option<String>,
    pub processing_lag_seconds: Option<i64>,
    pub total_traces: usize,
    pub facet_current_count: usize,
    pub facets: Vec<TopicAutomationProgressItem>,
    pub topics: Vec<TopicAutomationProgressItem>,
    pub facet_functions: Vec<FunctionSummary>,
    pub topic_map_functions: Vec<FunctionSummary>,
    pub cursor: AutomationCursorSnapshot,
    pub object_cursor: ObjectAutomationCursorSnapshot,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicAutomationPokeResult {
    pub id: String,
    pub name: String,
    pub object_id: String,
    pub previous_next_run_at: Option<String>,
    pub runtime_state: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicAutomationRewindResult {
    pub id: String,
    pub name: String,
    pub object_id: String,
    pub window_seconds: i64,
    pub start_xact_id: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicAutomationConfig {
    pub id: String,
    pub name: String,
    pub description: String,
    pub scope_type: Option<String>,
    pub btql_filter: Option<String>,
    pub sampling_rate: Option<f64>,
    pub window_seconds: Option<i64>,
    pub rerun_seconds: Option<i64>,
    pub relabel_overlap_seconds: Option<i64>,
    pub idle_seconds: Option<i64>,
    pub facet_functions: Vec<FunctionSummary>,
    pub topic_map_functions: Vec<FunctionSummary>,
}

#[derive(Debug, Clone, Default)]
pub struct TopicAutomationConfigPatch {
    pub automation_id: Option<String>,
    pub name: Option<String>,
    pub description: Option<String>,
    pub btql_filter: Option<Option<String>>,
    pub sampling_rate: Option<f64>,
    pub window_seconds: Option<i64>,
    pub rerun_seconds: Option<i64>,
    pub relabel_overlap_seconds: Option<i64>,
    pub idle_seconds: Option<i64>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FunctionSummary {
    pub name: String,
    pub ref_type: String,
    pub function_type: Option<String>,
    pub id: Option<String>,
    pub version: Option<String>,
    pub btql_filter: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicAutomationProgressItem {
    pub name: String,
    pub matched_count: usize,
    pub completed_count: usize,
    pub processing_count: usize,
    pub error_count: usize,
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
    let rows = list_topic_automation_rows(&ctx.client, &ctx.project.id).await?;
    let mut function_cache = HashMap::new();
    let mut automations = Vec::with_capacity(rows.len());
    for row in &rows {
        automations.push(
            build_topic_automation_status(&ctx.client, &ctx.project.id, row, &mut function_cache)
                .await?,
        );
    }
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

pub async fn poke_topic_automations(ctx: &ProjectContext) -> Result<TopicsPokeReport> {
    let rows = list_topic_automation_rows(&ctx.client, &ctx.project.id).await?;
    let mut queued = Vec::with_capacity(rows.len());

    for row in &rows {
        let automation_id = stringish_value(row.get("id")).unwrap_or_default();
        let object_cursor =
            fetch_object_cursor_snapshot(&ctx.client, &ctx.project.id, &automation_id).await?;
        let object_id = topic_automation_object_id(
            &ctx.project.id,
            row.get("config")
                .and_then(Value::as_object)
                .and_then(|config| config.get("data_scope")),
        )?;

        let body = serde_json::json!({
            "automation_id": automation_id,
            "object_id": object_id,
        });
        let _: Value = ctx
            .client
            .post("/brainstore/automation/upsert-object-cursor", &body)
            .await?;

        queued.push(TopicAutomationPokeResult {
            id: automation_id,
            name: string_value(row.get("name")).unwrap_or_else(|| "Topics".to_string()),
            object_id,
            previous_next_run_at: object_cursor.next_run_at,
            runtime_state: object_cursor.topic_runtime.map(|runtime| runtime.state),
        });
    }

    queued.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

    Ok(TopicsPokeReport {
        project: TopicsProjectSummary {
            id: ctx.project.id.clone(),
            name: ctx.project.name.clone(),
            org_name: ctx.client.org_name().to_string(),
            topics_url: topics_url(&ctx.app_url, ctx.client.org_name(), &ctx.project.name),
        },
        queued,
    })
}

pub async fn rewind_topic_automations(
    ctx: &ProjectContext,
    automation_id: Option<&str>,
    window_seconds: i64,
) -> Result<TopicsRewindReport> {
    let rows = list_topic_automation_rows(&ctx.client, &ctx.project.id).await?;
    let rows = filter_or_resolve_topic_automation_rows(rows, automation_id)?;
    let mut rewound = Vec::with_capacity(rows.len());

    for row in &rows {
        let automation_id = stringish_value(row.get("id")).unwrap_or_default();
        let object_id = topic_automation_object_id(
            &ctx.project.id,
            row.get("config")
                .and_then(Value::as_object)
                .and_then(|config| config.get("data_scope")),
        )?;
        let start_xact_id = inclusive_start_xact_id_from_epoch_ms(
            Utc::now()
                .timestamp_millis()
                .saturating_sub(window_seconds.saturating_mul(1000)),
        );

        let reset_body = serde_json::json!({
            "automation_id": automation_id,
            "object_id": object_id,
            "start_xact_id": start_xact_id,
        });
        let _: Value = ctx
            .client
            .post("/brainstore/automation/reset-cursors", &reset_body)
            .await?;

        let upsert_body = serde_json::json!({
            "automation_id": automation_id,
            "object_id": object_id,
        });
        let _: Value = ctx
            .client
            .post("/brainstore/automation/upsert-object-cursor", &upsert_body)
            .await?;

        rewound.push(TopicAutomationRewindResult {
            id: automation_id,
            name: string_value(row.get("name")).unwrap_or_else(|| "Topics".to_string()),
            object_id,
            window_seconds,
            start_xact_id,
        });
    }

    rewound.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

    Ok(TopicsRewindReport {
        project: TopicsProjectSummary {
            id: ctx.project.id.clone(),
            name: ctx.project.name.clone(),
            org_name: ctx.client.org_name().to_string(),
            topics_url: topics_url(&ctx.app_url, ctx.client.org_name(), &ctx.project.name),
        },
        rewound,
    })
}

pub async fn fetch_topics_config(
    ctx: &ProjectContext,
    automation_id: Option<&str>,
) -> Result<TopicsConfigReport> {
    let rows = list_topic_automation_rows(&ctx.client, &ctx.project.id).await?;
    let rows = filter_or_resolve_topic_automation_rows(rows, automation_id)?;
    let mut function_cache = HashMap::new();
    let mut automations = Vec::with_capacity(rows.len());

    for row in &rows {
        automations
            .push(build_topic_automation_config(&ctx.client, row, &mut function_cache).await?);
    }

    automations.sort_by(|left, right| left.name.cmp(&right.name).then(left.id.cmp(&right.id)));

    Ok(TopicsConfigReport {
        project: TopicsProjectSummary {
            id: ctx.project.id.clone(),
            name: ctx.project.name.clone(),
            org_name: ctx.client.org_name().to_string(),
            topics_url: topics_url(&ctx.app_url, ctx.client.org_name(), &ctx.project.name),
        },
        automations,
    })
}

pub async fn update_topics_config(
    ctx: &ProjectContext,
    patch: TopicAutomationConfigPatch,
) -> Result<TopicAutomationConfig> {
    let rows = list_topic_automation_rows(&ctx.client, &ctx.project.id).await?;
    let row = resolve_single_topic_automation_row(rows, patch.automation_id.as_deref())?;
    let current_config = row
        .get("config")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut next_config = current_config.clone();
    let mut has_config_changes = false;

    if let Some(sampling_rate) = patch.sampling_rate {
        next_config.insert("sampling_rate".to_string(), Value::from(sampling_rate));
        has_config_changes = true;
    }
    if let Some(window_seconds) = patch.window_seconds {
        next_config.insert(
            "backfill_time_range".to_string(),
            Value::String(format_duration_seconds(window_seconds)),
        );
        has_config_changes = true;
    }
    if let Some(rerun_seconds) = patch.rerun_seconds {
        next_config.insert("rerun_seconds".to_string(), Value::from(rerun_seconds));
        has_config_changes = true;
    }
    if let Some(relabel_overlap_seconds) = patch.relabel_overlap_seconds {
        next_config.insert(
            "relabel_overlap_seconds".to_string(),
            Value::from(relabel_overlap_seconds),
        );
        has_config_changes = true;
    }
    if let Some(idle_seconds) = patch.idle_seconds {
        let mut next_scope = current_config
            .get("scope")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        next_scope.insert("type".to_string(), Value::String("trace".to_string()));
        next_scope.insert("idle_seconds".to_string(), Value::from(idle_seconds));
        next_config.insert("scope".to_string(), Value::Object(next_scope));
        has_config_changes = true;
    }
    if let Some(btql_filter) = patch.btql_filter {
        match btql_filter {
            Some(filter) => {
                next_config.insert("btql_filter".to_string(), Value::String(filter));
            }
            None => {
                next_config.remove("btql_filter");
            }
        }
        has_config_changes = true;
    }

    let mut payload = serde_json::Map::new();
    payload.insert(
        "id".to_string(),
        Value::String(stringish_value(row.get("id")).unwrap_or_default()),
    );
    if let Some(name) = patch.name {
        payload.insert("name".to_string(), Value::String(name));
    }
    if let Some(description) = patch.description {
        payload.insert("description".to_string(), Value::String(description));
    }
    if has_config_changes {
        payload.insert("config".to_string(), Value::Object(next_config));
    }
    if payload.len() == 1 {
        bail!("no topic automation updates were requested");
    }

    let response: Value = ctx
        .client
        .post("/api/project_automation/patch_id", &Value::Object(payload))
        .await?;
    let updated_row = project_automation_row_from_response(&response)?;
    let mut function_cache = HashMap::new();
    build_topic_automation_config(&ctx.client, &updated_row, &mut function_cache).await
}

pub fn topics_url(app_url: &str, org_name: &str, project_name: &str) -> String {
    format!(
        "{}/app/{}/p/{}/topics",
        app_url.trim_end_matches('/'),
        encode(org_name),
        encode(project_name)
    )
}

fn topic_automation_object_id(project_id: &str, data_scope: Option<&Value>) -> Result<String> {
    let data_scope_mapping = data_scope.and_then(Value::as_object);
    let scope_type = string_value(data_scope_mapping.and_then(|scope| scope.get("type")));
    match scope_type.as_deref() {
        None | Some("project_logs") => Ok(format!("project_logs:{project_id}")),
        Some("project_experiments") => Ok(format!("project_experiments:{project_id}")),
        Some("experiment") => {
            let Some(experiment_id) =
                string_value(data_scope_mapping.and_then(|scope| scope.get("experiment_id")))
            else {
                bail!("topic automation experiment data scope is missing experiment_id");
            };
            Ok(format!("experiment:{experiment_id}"))
        }
        Some(other) => bail!("unsupported topic automation data scope: {other}"),
    }
}

fn project_automation_row_from_response(response: &Value) -> Result<Value> {
    if let Some(project_automation) = response.get("project_automation") {
        if project_automation.is_object() {
            return Ok(project_automation.clone());
        }
    }
    if response.get("id").is_some() && response.get("config").is_some() && response.is_object() {
        return Ok(response.clone());
    }
    bail!("unexpected project automation response shape");
}

fn filter_or_resolve_topic_automation_rows(
    rows: Vec<Value>,
    automation_id: Option<&str>,
) -> Result<Vec<Value>> {
    match automation_id {
        Some(automation_id) => {
            let matching = rows
                .into_iter()
                .filter(|row| stringish_value(row.get("id")).as_deref() == Some(automation_id))
                .collect::<Vec<_>>();
            if matching.is_empty() {
                bail!("topic automation '{automation_id}' was not found");
            }
            Ok(matching)
        }
        None => Ok(rows),
    }
}

fn resolve_single_topic_automation_row(
    rows: Vec<Value>,
    automation_id: Option<&str>,
) -> Result<Value> {
    let rows = filter_or_resolve_topic_automation_rows(rows, automation_id)?;
    if rows.is_empty() {
        bail!("no topic automations found");
    }
    if rows.len() == 1 {
        return Ok(rows.into_iter().next().expect("single row"));
    }
    let names = rows
        .iter()
        .map(|row| {
            let name = string_value(row.get("name")).unwrap_or_else(|| "Topics".to_string());
            let id = stringish_value(row.get("id")).unwrap_or_default();
            format!("{name} ({id})")
        })
        .collect::<Vec<_>>()
        .join(", ");
    bail!("project has multiple topic automations ({names}); re-run with --automation-id")
}

async fn list_topic_automation_rows(client: &ApiClient, project_id: &str) -> Result<Vec<Value>> {
    let path = format!("/v1/project_automation?project_id={}", encode(project_id));
    let response: Value = client.get(&path).await?;
    Ok(extract_objects(&response)
        .iter()
        .filter(|row| {
            row.get("config")
                .and_then(Value::as_object)
                .and_then(|config| config.get("event_type"))
                .and_then(Value::as_str)
                == Some("topic")
        })
        .cloned()
        .collect())
}

async fn build_topic_automation_status(
    client: &ApiClient,
    project_id: &str,
    row: &Value,
    function_cache: &mut HashMap<String, Value>,
) -> Result<TopicAutomationStatus> {
    let config = row
        .get("config")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let scope = config
        .get("scope")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    let id = stringish_value(row.get("id")).unwrap_or_default();
    let cursor = fetch_cursor_snapshot(client, project_id, &id).await?;
    let object_cursor = fetch_object_cursor_snapshot(client, project_id, &id).await?;

    let topic_map_functions =
        summarize_topic_map_functions(client, function_cache, config.get("topic_map_functions"))
            .await?;
    let facet_functions =
        summarize_function_refs(client, function_cache, config.get("facet_functions")).await?;
    let topic_bars = build_topic_status_bars(client, function_cache, &config).await?;
    let facet_bars = build_facet_status_bars(client, function_cache, &config, &topic_bars).await?;

    let mut total_traces = 0;
    let mut facet_current_count = 0;
    let mut facets = Vec::new();
    let mut topics = Vec::new();
    let time_filter_clause =
        created_time_filter_clause(config.get("backfill_time_range")).or_else(|| {
            created_time_filter_clause_from_window_seconds(
                object_cursor
                    .topic_runtime
                    .as_ref()
                    .and_then(|runtime| runtime.selected_window_seconds),
            )
        });
    if let Some(time_filter_clause) = time_filter_clause {
        let progress = fetch_topic_automation_progress(
            client,
            project_id,
            &time_filter_clause,
            &cursor,
            &facet_bars,
            &topic_bars,
        )
        .await?;
        total_traces = progress.total_traces;
        facet_current_count = progress.facet_current_count;
        facets = progress.facets;
        topics = progress.topics;
    }

    Ok(TopicAutomationStatus {
        id,
        name: string_value(row.get("name")).unwrap_or_else(|| "Topics".to_string()),
        description: string_value(row.get("description")).unwrap_or_default(),
        scope_type: string_value(scope.get("type")),
        btql_filter: string_value(config.get("btql_filter")),
        window_seconds: backfill_time_range_to_window_seconds(config.get("backfill_time_range")),
        rerun_seconds: int_value(config.get("rerun_seconds")),
        relabel_overlap_seconds: int_value(config.get("relabel_overlap_seconds")),
        idle_seconds: int_value(scope.get("idle_seconds")),
        configured_facets: config
            .get("facet_functions")
            .and_then(Value::as_array)
            .map_or(0, Vec::len),
        configured_topic_maps: config
            .get("topic_map_functions")
            .and_then(Value::as_array)
            .map_or(0, Vec::len),
        processing_lag_label: format_processing_lag_from_xact_range(
            cursor.pending_min_executed_xact_id.as_deref(),
            cursor.pending_max_compacted_xact_id.as_deref(),
        ),
        processing_lag_seconds: processing_lag_seconds_from_xact_range(
            cursor.pending_min_executed_xact_id.as_deref(),
            cursor.pending_max_compacted_xact_id.as_deref(),
        ),
        total_traces,
        facet_current_count,
        facets,
        topics,
        facet_functions,
        topic_map_functions,
        cursor,
        object_cursor,
    })
}

async fn build_topic_automation_config(
    client: &ApiClient,
    row: &Value,
    function_cache: &mut HashMap<String, Value>,
) -> Result<TopicAutomationConfig> {
    let config = row
        .get("config")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let scope = config
        .get("scope")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();

    Ok(TopicAutomationConfig {
        id: stringish_value(row.get("id")).unwrap_or_default(),
        name: string_value(row.get("name")).unwrap_or_else(|| "Topics".to_string()),
        description: string_value(row.get("description")).unwrap_or_default(),
        scope_type: string_value(scope.get("type")),
        btql_filter: string_value(config.get("btql_filter")),
        sampling_rate: float_value(config.get("sampling_rate")),
        window_seconds: backfill_time_range_to_window_seconds(config.get("backfill_time_range")),
        rerun_seconds: int_value(config.get("rerun_seconds")),
        relabel_overlap_seconds: int_value(config.get("relabel_overlap_seconds")),
        idle_seconds: int_value(scope.get("idle_seconds")),
        facet_functions: summarize_function_refs(
            client,
            function_cache,
            config.get("facet_functions"),
        )
        .await?,
        topic_map_functions: summarize_topic_map_functions(
            client,
            function_cache,
            config.get("topic_map_functions"),
        )
        .await?,
    })
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

#[derive(Debug, Clone)]
struct TopicStatusBar {
    name: String,
    classification_path: String,
    eligible_predicate: String,
    function_key: Option<String>,
    source_facet_name: Option<String>,
}

#[derive(Debug, Clone)]
struct FacetStatusBar {
    facet_name: String,
    facet_path: String,
    function_keys: Vec<String>,
}

#[derive(Debug, Clone)]
struct TopicAutomationProgressSummary {
    total_traces: usize,
    facet_current_count: usize,
    facets: Vec<TopicAutomationProgressItem>,
    topics: Vec<TopicAutomationProgressItem>,
}

#[derive(Debug, Clone)]
struct TriggeredFunctionPredicates {
    completed_predicate: String,
    inflight_predicate: String,
    error_predicate: String,
}

async fn summarize_function_refs(
    client: &ApiClient,
    function_cache: &mut HashMap<String, Value>,
    refs: Option<&Value>,
) -> Result<Vec<FunctionSummary>> {
    let mut out = Vec::new();
    for function_ref in refs.and_then(Value::as_array).into_iter().flatten() {
        out.push(summarize_function_ref(client, function_cache, function_ref).await?);
    }
    Ok(out)
}

async fn summarize_topic_map_functions(
    client: &ApiClient,
    function_cache: &mut HashMap<String, Value>,
    refs: Option<&Value>,
) -> Result<Vec<FunctionSummary>> {
    let mut out = Vec::new();
    for topic_map_ref in refs.and_then(Value::as_array).into_iter().flatten() {
        out.push(summarize_topic_map_function(client, function_cache, topic_map_ref).await?);
    }
    Ok(out)
}

async fn summarize_topic_map_function(
    client: &ApiClient,
    function_cache: &mut HashMap<String, Value>,
    topic_map_ref: &Value,
) -> Result<FunctionSummary> {
    let summary = summarize_function_ref(
        client,
        function_cache,
        topic_map_ref.get("function").unwrap_or(&Value::Null),
    )
    .await?;
    Ok(FunctionSummary {
        btql_filter: string_value(topic_map_ref.get("btql_filter")),
        ..summary
    })
}

async fn summarize_function_ref(
    client: &ApiClient,
    function_cache: &mut HashMap<String, Value>,
    function_ref: &Value,
) -> Result<FunctionSummary> {
    let reference = function_ref.as_object().cloned().unwrap_or_default();
    let ref_type = string_value(reference.get("type")).unwrap_or_else(|| "unknown".to_string());
    if ref_type == "global" {
        return Ok(FunctionSummary {
            name: string_value(reference.get("name"))
                .unwrap_or_else(|| "<unnamed global>".to_string()),
            ref_type,
            function_type: string_value(reference.get("function_type")),
            id: None,
            version: None,
            btql_filter: None,
        });
    }

    let function_id = string_value(reference.get("id"));
    let Some(function_id) = function_id else {
        return Ok(FunctionSummary {
            name: "<unknown function>".to_string(),
            ref_type,
            function_type: None,
            id: None,
            version: None,
            btql_filter: None,
        });
    };

    let function_row = load_function_row(client, function_cache, &function_id).await?;
    Ok(FunctionSummary {
        name: string_value(function_row.get("name")).unwrap_or_else(|| function_id.clone()),
        ref_type,
        function_type: string_value(function_row.get("function_type")),
        id: Some(function_id),
        version: string_value(reference.get("version")),
        btql_filter: None,
    })
}

async fn load_function_row(
    client: &ApiClient,
    function_cache: &mut HashMap<String, Value>,
    function_id: &str,
) -> Result<Value> {
    if let Some(value) = function_cache.get(function_id) {
        return Ok(value.clone());
    }
    let path = format!("/v1/function/{}", encode(function_id));
    let value: Value = client.get(&path).await?;
    function_cache.insert(function_id.to_string(), value.clone());
    Ok(value)
}

async fn build_topic_status_bars(
    client: &ApiClient,
    function_cache: &mut HashMap<String, Value>,
    config: &serde_json::Map<String, Value>,
) -> Result<Vec<TopicStatusBar>> {
    let mut seen_topic_map_ids = std::collections::HashSet::new();
    let mut bars = Vec::new();

    for topic_map_function in config
        .get("topic_map_functions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let topic_map_mapping = topic_map_function.as_object().cloned().unwrap_or_default();
        let function_ref = topic_map_mapping
            .get("function")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        if string_value(function_ref.get("type")) != Some("function".to_string()) {
            continue;
        }
        let Some(topic_map_id) = string_value(function_ref.get("id")) else {
            continue;
        };
        if !seen_topic_map_ids.insert(topic_map_id.clone()) {
            continue;
        }

        let function_row = load_function_row(client, function_cache, &topic_map_id).await?;
        let function_data = function_row
            .get("function_data")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let source_facet_name =
            if string_value(function_data.get("type")) == Some("topic_map".to_string()) {
                string_value(function_data.get("source_facet"))
            } else {
                None
            };
        let source_facet_path = source_facet_name
            .as_ref()
            .map(|name| escape_btql_ident_path(&["facets", name]));

        let mut eligible_predicate = source_facet_path
            .as_ref()
            .map(|path| format!("({path} != 'no_match')"))
            .unwrap_or_else(|| "false".to_string());
        if let Some(btql_filter) = string_value(topic_map_mapping.get("btql_filter")) {
            eligible_predicate = format!("({eligible_predicate}) AND ({btql_filter})");
        }

        let classification_name = string_value(function_row.get("name"))
            .or_else(|| string_value(function_row.get("slug")))
            .unwrap_or_else(|| topic_map_id.clone());

        bars.push(TopicStatusBar {
            name: classification_name.clone(),
            classification_path: escape_btql_ident_path(&["classifications", &classification_name]),
            eligible_predicate,
            function_key: saved_function_id_to_triggered_function_key(&Value::Object(function_ref)),
            source_facet_name,
        });
    }

    Ok(bars)
}

async fn build_facet_status_bars(
    client: &ApiClient,
    function_cache: &mut HashMap<String, Value>,
    config: &serde_json::Map<String, Value>,
    topic_bars: &[TopicStatusBar],
) -> Result<Vec<FacetStatusBar>> {
    let mut order = Vec::<String>::new();
    let mut bars_by_name = HashMap::<String, FacetStatusBar>::new();

    for topic_bar in topic_bars {
        if let Some(source_facet_name) = topic_bar.source_facet_name.as_deref() {
            ensure_facet_bar(&mut order, &mut bars_by_name, source_facet_name);
        }
    }

    for facet_function in config
        .get("facet_functions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
    {
        let facet_mapping = facet_function.as_object().cloned().unwrap_or_default();
        let ref_type = string_value(facet_mapping.get("type"));
        if ref_type.as_deref() == Some("global")
            && string_value(facet_mapping.get("function_type")).as_deref() == Some("facet")
        {
            if let Some(facet_name) = string_value(facet_mapping.get("name")) {
                ensure_facet_bar(&mut order, &mut bars_by_name, &facet_name);
                if let Some(function_key) =
                    saved_function_id_to_triggered_function_key(&Value::Object(facet_mapping))
                {
                    bars_by_name
                        .get_mut(&facet_name)
                        .expect("facet exists")
                        .function_keys
                        .push(function_key);
                }
            }
            continue;
        }
        if ref_type.as_deref() != Some("function") {
            continue;
        }

        let Some(function_id) = string_value(facet_mapping.get("id")) else {
            continue;
        };
        let function_row = load_function_row(client, function_cache, &function_id).await?;
        let Some(facet_name) = string_value(function_row.get("name")) else {
            continue;
        };
        ensure_facet_bar(&mut order, &mut bars_by_name, &facet_name);
        if let Some(function_key) =
            saved_function_id_to_triggered_function_key(&Value::Object(facet_mapping))
        {
            bars_by_name
                .get_mut(&facet_name)
                .expect("facet exists")
                .function_keys
                .push(function_key);
        }
    }

    Ok(order
        .into_iter()
        .filter_map(|name| bars_by_name.remove(&name))
        .map(|mut bar| {
            bar.function_keys.sort();
            bar.function_keys.dedup();
            bar
        })
        .collect())
}

fn ensure_facet_bar(
    order: &mut Vec<String>,
    bars_by_name: &mut HashMap<String, FacetStatusBar>,
    facet_name: &str,
) {
    if !bars_by_name.contains_key(facet_name) {
        order.push(facet_name.to_string());
        bars_by_name.insert(
            facet_name.to_string(),
            FacetStatusBar {
                facet_name: facet_name.to_string(),
                facet_path: escape_btql_ident_path(&["facets", facet_name]),
                function_keys: Vec::new(),
            },
        );
    }
}

async fn fetch_topic_automation_progress(
    client: &ApiClient,
    project_id: &str,
    time_filter_clause: &str,
    cursor_status: &AutomationCursorSnapshot,
    facet_bars: &[FacetStatusBar],
    topic_bars: &[TopicStatusBar],
) -> Result<TopicAutomationProgressSummary> {
    let pending_min_executed_xact_id = cursor_status.pending_min_executed_xact_id.as_deref();
    let mut measure_expressions = Vec::<String>::new();

    let facet_paths = facet_bars
        .iter()
        .map(|bar| bar.facet_path.as_str())
        .collect::<Vec<_>>();
    if !facet_paths.is_empty() {
        let facet_coverage_predicate = facet_paths
            .iter()
            .map(|facet_path| format!("({facet_path} != 'no_match')"))
            .collect::<Vec<_>>()
            .join(" OR ");
        measure_expressions.push(format!(
            "count(({facet_coverage_predicate}) ? 1 : null) as facet_current_marked_traces"
        ));
    }

    for (index, bar) in facet_bars.iter().enumerate() {
        let prefix = format!("facet_{index}");
        measure_expressions.push(format!(
            "count((({} != 'no_match')) ? 1 : null) as {prefix}_current_marked_traces",
            bar.facet_path
        ));
        measure_expressions.push(format!(
            "count({}) as {prefix}_current_completed_output_traces",
            bar.facet_path
        ));
        if bar.function_keys.is_empty() {
            continue;
        }

        let mut completed_predicates = Vec::new();
        let mut inflight_predicates = Vec::new();
        let mut error_predicates = Vec::new();
        for function_key in &bar.function_keys {
            let predicates =
                build_triggered_function_predicates(function_key, pending_min_executed_xact_id);
            completed_predicates.push(format!("({})", predicates.completed_predicate));
            inflight_predicates.push(format!("({})", predicates.inflight_predicate));
            error_predicates.push(format!("({})", predicates.error_predicate));
        }

        measure_expressions.push(format!(
            "count(({} ) ? 1 : null) as {prefix}_current_completed_traces",
            completed_predicates.join(" OR ")
        ));
        measure_expressions.push(format!(
            "count(({} ) ? 1 : null) as {prefix}_current_inflight_traces",
            inflight_predicates.join(" OR ")
        ));
        measure_expressions.push(format!(
            "count(({} ) ? 1 : null) as {prefix}_current_error_traces",
            error_predicates.join(" OR ")
        ));
    }

    for (index, bar) in topic_bars.iter().enumerate() {
        let prefix = format!("topic_{index}");
        measure_expressions.push(format!(
            "count(({}) ? 1 : null) as {prefix}_current_eligible_traces",
            bar.eligible_predicate
        ));
        measure_expressions.push(format!(
            "count({}) as {prefix}_current_labeled_traces",
            bar.classification_path
        ));
        if let Some(function_key) = bar.function_key.as_deref() {
            let predicates =
                build_triggered_function_predicates(function_key, pending_min_executed_xact_id);
            measure_expressions.push(format!(
                "count(({} ) ? 1 : null) as {prefix}_current_completed_traces",
                predicates.completed_predicate
            ));
            measure_expressions.push(format!(
                "count(({} ) ? 1 : null) as {prefix}_current_inflight_traces",
                predicates.inflight_predicate
            ));
            measure_expressions.push(format!(
                "count(({} ) ? 1 : null) as {prefix}_current_error_traces",
                predicates.error_predicate
            ));
        }
    }

    let escaped_project_id = project_id.replace('\'', "''");
    let total_query = format!(
        "from: project_logs('{escaped_project_id}') spans | measures: count_distinct(root_span_id) as total_traces | filter: {time_filter_clause}"
    );
    let total_response = execute_btql_value(client, &total_query).await?;
    let total_row = first_btql_row(&total_response);
    let aggregate_row = if measure_expressions.is_empty() {
        None
    } else {
        let aggregate_query = format!(
            "from: project_logs('{escaped_project_id}') spans | measures: {} | filter: {time_filter_clause}",
            measure_expressions.join(", ")
        );
        let aggregate_response = execute_btql_value(client, &aggregate_query).await?;
        first_btql_row(&aggregate_response).cloned()
    };

    Ok(TopicAutomationProgressSummary {
        total_traces: read_btql_count_metric(total_row, "total_traces"),
        facet_current_count: read_btql_count_metric(
            aggregate_row.as_ref(),
            "facet_current_marked_traces",
        ),
        facets: facet_bars
            .iter()
            .enumerate()
            .map(|(index, bar)| TopicAutomationProgressItem {
                name: bar.facet_name.clone(),
                matched_count: read_btql_count_metric(
                    aggregate_row.as_ref(),
                    &format!("facet_{index}_current_marked_traces"),
                ),
                completed_count: read_btql_count_metric(
                    aggregate_row.as_ref(),
                    &format!("facet_{index}_current_completed_output_traces"),
                ),
                processing_count: read_btql_count_metric(
                    aggregate_row.as_ref(),
                    &format!("facet_{index}_current_inflight_traces"),
                )
                .saturating_sub(read_btql_count_metric(
                    aggregate_row.as_ref(),
                    &format!("facet_{index}_current_error_traces"),
                )),
                error_count: read_btql_count_metric(
                    aggregate_row.as_ref(),
                    &format!("facet_{index}_current_error_traces"),
                ),
            })
            .collect(),
        topics: topic_bars
            .iter()
            .enumerate()
            .map(|(index, bar)| TopicAutomationProgressItem {
                name: bar.name.clone(),
                matched_count: read_btql_count_metric(
                    aggregate_row.as_ref(),
                    &format!("topic_{index}_current_eligible_traces"),
                ),
                completed_count: read_btql_count_metric(
                    aggregate_row.as_ref(),
                    &format!("topic_{index}_current_labeled_traces"),
                ),
                processing_count: read_btql_count_metric(
                    aggregate_row.as_ref(),
                    &format!("topic_{index}_current_inflight_traces"),
                )
                .saturating_sub(read_btql_count_metric(
                    aggregate_row.as_ref(),
                    &format!("topic_{index}_current_error_traces"),
                )),
                error_count: read_btql_count_metric(
                    aggregate_row.as_ref(),
                    &format!("topic_{index}_current_error_traces"),
                ),
            })
            .collect(),
    })
}

async fn execute_btql_value(client: &ApiClient, query: &str) -> Result<Value> {
    let body = serde_json::json!({
        "query": query,
        "fmt": "json",
        "brainstore_realtime": true,
    });
    let org_name = client.org_name();
    let headers = if !org_name.is_empty() {
        vec![("x-bt-org-name", org_name)]
    } else {
        Vec::new()
    };
    client.post_with_headers("/btql", &body, &headers).await
}

fn first_btql_row(response: &Value) -> Option<&serde_json::Map<String, Value>> {
    response
        .get("data")
        .and_then(Value::as_array)
        .and_then(|data| data.first())
        .and_then(Value::as_object)
}

fn read_btql_count_metric(row: Option<&serde_json::Map<String, Value>>, alias: &str) -> usize {
    let value = row.and_then(|row| row.get(alias));
    match value {
        Some(Value::Number(number)) => number
            .as_u64()
            .or_else(|| number.as_i64().and_then(|value| u64::try_from(value).ok()))
            .and_then(|value| usize::try_from(value).ok())
            .unwrap_or(0),
        Some(Value::String(value)) => value
            .parse::<f64>()
            .ok()
            .and_then(|value| usize::try_from(value as i64).ok())
            .unwrap_or(0),
        _ => 0,
    }
}

fn build_triggered_function_predicates(
    function_key: &str,
    pending_min_executed_xact_id: Option<&str>,
) -> TriggeredFunctionPredicates {
    let triggered_xact_path = escape_btql_ident_path(&[
        "_async_scoring_state",
        "triggered_functions",
        function_key,
        "triggered_xact_id",
    ]);
    let completed_xact_path = escape_btql_ident_path(&[
        "_async_scoring_state",
        "triggered_functions",
        function_key,
        "completed_xact_id",
    ]);
    let attempts_path = escape_btql_ident_path(&[
        "_async_scoring_state",
        "triggered_functions",
        function_key,
        "attempts",
    ]);
    let attempted_predicate = format!("{triggered_xact_path} IS NOT NULL");
    let completed_predicate = format!("{completed_xact_path} >= {triggered_xact_path}");
    let incomplete_predicate = format!(
        "{attempted_predicate} AND ({completed_xact_path} IS NULL OR {completed_xact_path} < {triggered_xact_path})"
    );
    let pending_error_window_predicate = pending_min_executed_xact_id
        .map(|xact_id| format!("{triggered_xact_path} < '{}'", xact_id.replace('\'', "''")));
    let attempts_predicate = format!("{attempts_path} > 0");
    let error_condition = match pending_error_window_predicate {
        Some(predicate) => format!("{attempts_predicate} AND ({predicate})"),
        None => attempts_predicate,
    };
    let inflight_predicate = incomplete_predicate;
    let error_predicate = format!("{inflight_predicate} AND {error_condition}");

    TriggeredFunctionPredicates {
        completed_predicate,
        inflight_predicate,
        error_predicate,
    }
}

fn saved_function_id_to_triggered_function_key(function_ref: &Value) -> Option<String> {
    let reference = function_ref.as_object()?;
    let ref_type = string_value(reference.get("type"))?;
    if ref_type == "function" {
        let function_id = string_value(reference.get("id")).unwrap_or_default();
        let version = string_value(reference.get("version"));
        return Some(match version {
            Some(version) => format!("function_id:{function_id}#version:{version}"),
            None => format!("function_id:{function_id}"),
        });
    }
    let name = string_value(reference.get("name")).unwrap_or_default();
    let function_type =
        string_value(reference.get("function_type")).unwrap_or_else(|| "scorer".to_string());
    Some(format!("global:{function_type}:{name}"))
}

fn escape_btql_ident_component(value: &str) -> String {
    format!("\"{}\"", value.replace('"', "\"\""))
}

fn escape_btql_ident_path(parts: &[&str]) -> String {
    parts
        .iter()
        .map(|part| escape_btql_ident_component(part))
        .collect::<Vec<_>>()
        .join(".")
}

fn created_time_filter_clause(value: Option<&Value>) -> Option<String> {
    let value = value?;
    if let Some(value) = value.as_str() {
        if let Some(interval_ms) = value.strip_prefix("interval_ms:") {
            let interval_ms = interval_ms.parse::<i64>().ok()?;
            return Some(format!(
                "created >= NOW() - INTERVAL {} SECOND",
                std::cmp::max(1, (interval_ms as f64 / 1000.0).round() as i64)
            ));
        }
        return parse_duration_to_seconds(value)
            .map(|seconds| format!("created >= NOW() - INTERVAL {seconds} SECOND"));
    }

    let map = value.as_object()?;
    let from = map.get("from")?.as_str()?.replace('\'', "''");
    let to = map.get("to")?.as_str()?.replace('\'', "''");
    Some(format!("created >= '{from}' AND created <= '{to}'"))
}

fn created_time_filter_clause_from_window_seconds(window_seconds: Option<i64>) -> Option<String> {
    window_seconds.map(|window_seconds| {
        format!(
            "created >= NOW() - INTERVAL {} SECOND",
            std::cmp::max(1, window_seconds)
        )
    })
}

fn processing_lag_seconds_from_xact_range(
    min_executed_xact_id: Option<&str>,
    max_compacted_xact_id: Option<&str>,
) -> Option<i64> {
    let min_executed_epoch_ms = epoch_ms_from_xact_id(min_executed_xact_id?)?;
    let max_compacted_epoch_ms = epoch_ms_from_xact_id(max_compacted_xact_id?)?;
    let delta_ms = max_compacted_epoch_ms - min_executed_epoch_ms;
    if delta_ms <= 0 {
        return None;
    }
    Some(delta_ms / 1000)
}

fn format_processing_lag_from_xact_range(
    min_executed_xact_id: Option<&str>,
    max_compacted_xact_id: Option<&str>,
) -> Option<String> {
    let lag_seconds =
        processing_lag_seconds_from_xact_range(min_executed_xact_id, max_compacted_xact_id)?;
    let minutes = lag_seconds / 60;
    let hours = lag_seconds / (60 * 60);
    let days = lag_seconds / (60 * 60 * 24);
    if days >= 1 {
        return Some(format!("{days}d behind"));
    }
    if hours >= 1 {
        return Some(format!("{hours}h behind"));
    }
    Some(format!("{}m behind", std::cmp::max(1, minutes)))
}

fn epoch_ms_from_xact_id(xact_id: &str) -> Option<i64> {
    let xact_value = xact_id.parse::<i64>().ok()?;
    let removed_flag = xact_value & 0x0000_FFFF_FFFF_FFFF;
    let epoch_seconds = (removed_flag >> 16) & 0x0000_FFFF_FFFF;
    Some(epoch_seconds * 1000)
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

fn float_value(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Number(number)) => number.as_f64(),
        Some(Value::String(value)) => value.parse::<f64>().ok(),
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

fn inclusive_start_xact_id_from_epoch_ms(epoch_ms: i64) -> String {
    const XACT_NAMESPACE: i64 = 0x0DE1;
    let epoch_seconds = std::cmp::max(0, epoch_ms / 1000);
    let transaction_id = (XACT_NAMESPACE << 48) | ((epoch_seconds & 0x0000_FFFF_FFFF) << 16);
    if transaction_id <= 0 {
        return "0".to_string();
    }
    (transaction_id - 1).to_string()
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

fn format_duration_seconds(seconds: i64) -> String {
    let units = [
        ("w", 7 * 24 * 60 * 60),
        ("d", 24 * 60 * 60),
        ("h", 60 * 60),
        ("m", 60),
        ("s", 1),
    ];
    for (suffix, scale) in units {
        if seconds >= scale && seconds % scale == 0 {
            return format!("{}{}", seconds / scale, suffix);
        }
    }
    format!("{seconds}s")
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
    fn inclusive_start_xact_id_from_epoch_ms_matches_python_formula() {
        let inclusive = inclusive_start_xact_id_from_epoch_ms(1_744_539_200_123);
        let exclusive = (inclusive.parse::<i64>().expect("xact id") + 1).to_string();
        assert_eq!(epoch_ms_from_xact_id(&exclusive), Some(1_744_539_200_000));
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

    #[test]
    fn topic_automation_object_id_defaults_to_project_logs() {
        assert_eq!(
            topic_automation_object_id("proj_123", None).expect("object id"),
            "project_logs:proj_123"
        );
    }

    #[test]
    fn topic_automation_object_id_supports_project_experiments_and_experiment() {
        assert_eq!(
            topic_automation_object_id("proj_123", Some(&json!({ "type": "project_experiments" })))
                .expect("object id"),
            "project_experiments:proj_123"
        );
        assert_eq!(
            topic_automation_object_id(
                "proj_123",
                Some(&json!({ "type": "experiment", "experiment_id": "exp_123" }))
            )
            .expect("object id"),
            "experiment:exp_123"
        );
    }

    #[test]
    fn format_duration_seconds_prefers_compact_units() {
        assert_eq!(format_duration_seconds(3600), "1h");
        assert_eq!(format_duration_seconds(5400), "90m");
    }
}
