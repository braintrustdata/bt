use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};

use anyhow::{bail, Context, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use urlencoding::encode;

use crate::{http::ApiClient, project_context::ProjectContext, utils::app_project_url};

const DEFAULT_TOPIC_AUTOMATION_NAME: &str = "Topics";
const DEFAULT_TOPIC_AUTOMATION_DESCRIPTION: &str =
    "Automatically extract facets and classify logs using topic maps";
const DEFAULT_TOPIC_FACET_NAMES: &[&str] = &["Task", "Sentiment", "Issues"];
const DEFAULT_TOPIC_WINDOW_SECONDS: i64 = 24 * 60 * 60;
const DEFAULT_TOPIC_RERUN_SECONDS: i64 = 24 * 60 * 60;
const DEFAULT_TOPIC_RELABEL_OVERLAP_SECONDS: i64 = 60 * 60;
const DEFAULT_TOPIC_IDLE_SECONDS: i64 = 10 * 60;
const DEFAULT_TOPIC_SAMPLING_RATE: f64 = 1.0;
const DEFAULT_TOPIC_EMBEDDING_MODEL: &str = "brain-embedding-1";
const MAX_STATUS_PROGRESS_WINDOW_SECONDS: i64 = 24 * 60 * 60;
const ORG_USERS_PAGE_LIMIT: usize = 1000;
const TOPIC_REPO_ROOT_SPAN_PAGE_LIMIT: usize = 1000;
const TOPIC_REPO_ROOT_SPAN_MAX_IDS: usize = 5000;
const TOPIC_TRACE_CURSOR_PREFIX: &str = "bt-topic-traces-v1:";

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
pub struct TopicsDeleteReport {
    pub project: TopicsProjectSummary,
    pub automation: TopicAutomationConfig,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicMapConfigUpdate {
    pub automation: TopicAutomationConfig,
    pub topic_map_id: String,
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum TopicExploreSort {
    Count,
    Tokens,
    Cost,
    AvgTokens,
    AvgCost,
    Recent,
}

impl Default for TopicExploreSort {
    fn default() -> Self {
        Self::Count
    }
}

#[derive(Debug, Clone, Copy, Deserialize, Serialize, PartialEq, Eq, clap::ValueEnum)]
#[serde(rename_all = "snake_case")]
pub enum TopicTraceSort {
    Recent,
    Tokens,
    Cost,
}

impl Default for TopicTraceSort {
    fn default() -> Self {
        Self::Recent
    }
}

impl From<TopicTraceSort> for TopicExploreSort {
    fn from(sort: TopicTraceSort) -> Self {
        match sort {
            TopicTraceSort::Recent => Self::Recent,
            TopicTraceSort::Tokens => Self::Tokens,
            TopicTraceSort::Cost => Self::Cost,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicsExploreFacetsReport {
    pub project: TopicsProjectSummary,
    pub facets: Vec<TopicExploreFacet>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicClassificationsReport {
    pub project: TopicsProjectSummary,
    pub topic_map: TopicExploreTopicMap,
    pub classifications: Vec<TopicClassificationRow>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicTracesReport {
    pub project: TopicsProjectSummary,
    pub topic_map: TopicExploreTopicMap,
    pub topic: TopicTraceSelection,
    pub traces: Vec<TopicTraceRow>,
    pub next_cursor: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicExploreFacet {
    pub automation_id: String,
    pub automation_name: String,
    pub facet: Option<String>,
    pub topic_map: String,
    pub topic_map_id: String,
    pub version: Option<String>,
    pub eligible: usize,
    pub labeled: usize,
    pub processing: usize,
    pub errors: usize,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicExploreTopicMap {
    pub automation_id: String,
    pub automation_name: String,
    pub facet: Option<String>,
    pub topic_map: String,
    pub topic_map_id: String,
    pub version: Option<String>,
    pub classification_path: String,
    pub btql_filter: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicClassificationRow {
    pub topic: String,
    pub topic_id: String,
    pub traces: usize,
    pub tokens: f64,
    pub cost: f64,
    pub avg_tokens: f64,
    pub avg_cost: f64,
    pub latest: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicTraceSelection {
    pub topic: Option<String>,
    pub topic_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicTraceRow {
    pub created: Option<String>,
    pub root_span_id: String,
    pub span_id: Option<String>,
    pub row_id: Option<String>,
    pub created_by_user_id: Option<String>,
    pub created_by_user_name: Option<String>,
    pub created_by_user_email: Option<String>,
    pub git_origin_url: Option<String>,
    pub repo: Option<String>,
    pub topic: Option<String>,
    pub topic_id: Option<String>,
    pub tokens: f64,
    pub cost: f64,
    pub duration_seconds: Option<f64>,
    pub input: Option<String>,
    pub app_url: String,
    #[serde(skip_serializing)]
    pagination_key: Option<String>,
    #[serde(skip_serializing)]
    sort_value: Option<TopicTraceCursorValue>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
struct TopicTracePaginationCursor {
    version: u8,
    sort: TopicExploreSort,
    sort_value: TopicTraceCursorValue,
    pagination_key: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
#[serde(tag = "type", content = "value", rename_all = "snake_case")]
enum TopicTraceCursorValue {
    Number(f64),
    String(String),
}

#[derive(Debug, Clone, Deserialize)]
struct OrgUser {
    id: String,
    #[serde(default)]
    given_name: Option<String>,
    #[serde(default)]
    family_name: Option<String>,
    #[serde(default)]
    email: Option<String>,
}

#[derive(Debug, Deserialize)]
struct OrgUsersListResponse {
    objects: Vec<OrgUser>,
}

#[derive(Debug, Default)]
pub(super) struct OrgUsersCache {
    users_by_id: Option<HashMap<String, OrgUser>>,
}

impl OrgUser {
    fn display_name(&self) -> Option<String> {
        let given = self.given_name.as_deref().unwrap_or_default().trim();
        let family = self.family_name.as_deref().unwrap_or_default().trim();
        let name = match (given.is_empty(), family.is_empty()) {
            (true, true) => None,
            (false, true) => Some(given.to_string()),
            (true, false) => Some(family.to_string()),
            (false, false) => Some(format!("{given} {family}")),
        };
        name.or_else(|| {
            self.email
                .as_deref()
                .map(str::trim)
                .filter(|email| !email.is_empty())
                .map(ToString::to_string)
        })
    }
}

impl OrgUsersCache {
    async fn hydrate_trace_users(
        &mut self,
        client: &ApiClient,
        traces: &mut [TopicTraceRow],
    ) -> Result<()> {
        let user_ids = traces
            .iter()
            .filter_map(|trace| trace.created_by_user_id.as_deref())
            .collect::<HashSet<_>>();
        if user_ids.is_empty() {
            return Ok(());
        }

        let users = self.users_by_id(client).await?;
        for trace in traces {
            let Some(user_id) = trace.created_by_user_id.as_deref() else {
                continue;
            };
            let Some(user) = users.get(user_id) else {
                continue;
            };
            trace.created_by_user_name = user.display_name();
            trace.created_by_user_email = user.email.clone();
        }

        Ok(())
    }

    async fn users_by_id(&mut self, client: &ApiClient) -> Result<&HashMap<String, OrgUser>> {
        if self.users_by_id.is_none() {
            self.users_by_id = Some(fetch_org_users(client).await?);
        }

        Ok(self
            .users_by_id
            .as_ref()
            .expect("org users cache initialized"))
    }
}

#[derive(Debug, Clone, Serialize)]
struct TopicMapReportUrlRequest<'a> {
    function_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    version: Option<&'a str>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TopicMapReportUrl {
    pub url: String,
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
    pub progress_loaded: bool,
    pub progress_window_seconds: Option<i64>,
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

#[derive(Debug, Clone, Default)]
pub struct TopicAutomationConfigCreate {
    pub name: Option<String>,
    pub description: Option<String>,
    pub btql_filter: Option<String>,
    pub sampling_rate: Option<f64>,
    pub window_seconds: Option<i64>,
    pub rerun_seconds: Option<i64>,
    pub relabel_overlap_seconds: Option<i64>,
    pub idle_seconds: Option<i64>,
    pub facets: Vec<String>,
    pub embedding_model: Option<String>,
}

#[derive(Debug, Clone, Default)]
pub struct TopicMapConfigPatch {
    pub automation_id: Option<String>,
    pub topic_map_target: String,
    pub name: Option<String>,
    pub description: Option<String>,
    pub source_facet: Option<String>,
    pub embedding_model: Option<String>,
    pub distance_threshold: Option<f64>,
    pub disable_reconciliation: Option<bool>,
    pub algorithm: Option<String>,
    pub dimension_reduction: Option<String>,
    pub sample_size: Option<u32>,
    pub n_clusters: Option<u32>,
    pub min_cluster_size: Option<usize>,
    pub min_samples: Option<usize>,
    pub hierarchy_threshold: Option<usize>,
    pub naming_model: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicMapGenerationSettings {
    pub algorithm: Option<String>,
    pub dimension_reduction: Option<String>,
    pub sample_size: Option<u32>,
    pub n_clusters: Option<u32>,
    pub min_cluster_size: Option<usize>,
    pub min_samples: Option<usize>,
    pub hierarchy_threshold: Option<usize>,
    pub naming_model: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
pub struct FunctionSummary {
    pub name: String,
    pub ref_type: String,
    pub function_type: Option<String>,
    pub id: Option<String>,
    pub description: Option<String>,
    pub version: Option<String>,
    pub btql_filter: Option<String>,
    pub source_facet: Option<String>,
    pub embedding_model: Option<String>,
    pub distance_threshold: Option<f64>,
    pub disable_reconciliation: Option<bool>,
    pub generation_settings: Option<TopicMapGenerationSettings>,
}

#[derive(Debug, Clone, Serialize)]
pub struct TopicAutomationProgressItem {
    pub name: String,
    pub matched_count: usize,
    pub completed_count: usize,
    pub checked_count: usize,
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

#[derive(Debug, Clone, Serialize)]
struct RegisterTopicAutomationRequest {
    project_automation_name: String,
    description: String,
    project_id: String,
    config: RegisterTopicAutomationConfig,
    update: bool,
}

#[derive(Debug, Clone, Serialize)]
struct RegisterTopicAutomationConfig {
    event_type: &'static str,
    sampling_rate: f64,
    facet_functions: Vec<GlobalFacetFunctionRef>,
    topic_map_functions: Vec<TopicMapFunctionRef>,
    scope: TraceScopeConfig,
    rerun_seconds: i64,
    relabel_overlap_seconds: i64,
    backfill_time_range: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    btql_filter: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
struct GlobalFacetFunctionRef {
    #[serde(rename = "type")]
    ref_type: &'static str,
    name: String,
    function_type: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct TopicMapFunctionRef {
    function: FunctionIdRef,
}

#[derive(Debug, Clone, Serialize)]
struct FunctionIdRef {
    #[serde(rename = "type")]
    ref_type: &'static str,
    id: String,
}

#[derive(Debug, Clone, Serialize)]
struct TraceScopeConfig {
    #[serde(rename = "type")]
    scope_type: &'static str,
    idle_seconds: i64,
}

#[derive(Debug, Clone, Serialize)]
struct InsertFunctionsRequest {
    functions: Vec<CreateTopicMapFunctionRequest>,
}

#[derive(Debug, Clone, Serialize)]
struct CreateTopicMapFunctionRequest {
    project_id: String,
    name: String,
    slug: String,
    function_type: &'static str,
    function_data: CreateTopicMapFunctionData,
    if_exists: &'static str,
}

#[derive(Debug, Clone, Serialize)]
struct CreateTopicMapFunctionData {
    #[serde(rename = "type")]
    data_type: &'static str,
    source_facet: String,
    embedding_model: String,
}

#[derive(Debug, Clone, Serialize)]
struct AutomationProjectRequest {
    automation_id: String,
    project_id: String,
}

#[derive(Debug, Clone, Serialize)]
struct UpsertObjectCursorRequest {
    automation_id: String,
    object_id: String,
}

#[derive(Debug, Clone, Serialize)]
struct ResetObjectCursorRequest {
    automation_id: String,
    object_id: String,
    start_xact_id: String,
}

#[derive(Debug, Clone, Serialize)]
struct BtqlValueRequest<'a> {
    query: &'a str,
    fmt: &'static str,
    brainstore_realtime: bool,
}

pub async fn fetch_topics_status(
    ctx: &ProjectContext,
    include_progress: bool,
    progress_window_seconds_override: Option<i64>,
) -> Result<TopicsStatusReport> {
    let rows = list_topic_automation_rows(&ctx.client, &ctx.project.id).await?;
    let mut function_cache = HashMap::new();
    let mut automations = Vec::with_capacity(rows.len());
    for row in &rows {
        automations.push(
            build_topic_automation_status(
                &ctx.client,
                &ctx.project.id,
                row,
                include_progress,
                progress_window_seconds_override,
                &mut function_cache,
            )
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

pub async fn topic_explore_filter_clause(
    ctx: &ProjectContext,
    since: Option<&str>,
    window: &str,
    extra_filter: Option<&str>,
    repo: Option<&str>,
    print_queries: bool,
) -> Result<String> {
    let time_clause = topic_explore_time_filter_clause(since, window)?;
    let repo_filter =
        topic_repo_root_span_filter_clause(ctx, repo, &time_clause, print_queries).await?;

    Ok(combine_filter_clauses([
        Some(time_clause),
        repo_filter,
        extra_filter
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToString::to_string),
    ]))
}

fn topic_explore_time_filter_clause(since: Option<&str>, window: &str) -> Result<String> {
    Ok(
        if let Some(ts) = since.map(str::trim).filter(|value| !value.is_empty()) {
            format!("created >= {}", btql_string_literal(ts))
        } else {
            let seconds = crate::utils::parse_duration_to_seconds(window)?;
            if seconds == 0 {
                bail!("--window must be greater than zero");
            }
            format!("created >= NOW() - INTERVAL {seconds} SECOND")
        },
    )
}

pub async fn fetch_topics_explore_facets(
    ctx: &ProjectContext,
    automation_id: Option<&str>,
    base_filter_clause: &str,
    print_queries: bool,
) -> Result<TopicsExploreFacetsReport> {
    let rows = list_topic_automation_rows(&ctx.client, &ctx.project.id).await?;
    let rows = filter_or_resolve_topic_automation_rows(rows, automation_id)?;
    let mut function_cache = HashMap::new();
    let mut facets = Vec::new();

    for row in &rows {
        let config = row
            .get("config")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        let automation_id = stringish_value(row.get("id")).unwrap_or_default();
        let automation_name = string_value(row.get("name")).unwrap_or_else(|| "Topics".to_string());
        let automation_filter = combine_filter_clauses([
            Some(base_filter_clause.to_string()),
            string_value(config.get("btql_filter")),
        ]);

        let cursor = fetch_cursor_snapshot(&ctx.client, &ctx.project.id, &automation_id).await?;
        let topic_bars = build_topic_status_bars(&ctx.client, &mut function_cache, &config).await?;
        let facet_bars =
            build_facet_status_bars(&ctx.client, &mut function_cache, &config, &topic_bars).await?;
        let progress = fetch_topic_automation_progress(
            &ctx.client,
            &ctx.project.id,
            &automation_filter,
            &cursor,
            &facet_bars,
            &topic_bars,
            print_queries,
        )
        .await?;
        let topic_progress_by_name = progress
            .topics
            .into_iter()
            .map(|item| (item.name.clone(), item))
            .collect::<HashMap<_, _>>();
        let topic_maps = summarize_topic_map_functions(
            &ctx.client,
            &mut function_cache,
            config.get("topic_map_functions"),
        )
        .await?;

        for topic_map in topic_maps {
            let counts = topic_progress_by_name.get(&topic_map.name);
            facets.push(TopicExploreFacet {
                automation_id: automation_id.clone(),
                automation_name: automation_name.clone(),
                facet: topic_map.source_facet.clone(),
                topic_map: topic_map.name.clone(),
                topic_map_id: topic_map.id.clone().unwrap_or_default(),
                version: topic_map.version.clone(),
                eligible: counts.map(|item| item.matched_count).unwrap_or(0),
                labeled: counts.map(|item| item.completed_count).unwrap_or(0),
                processing: counts.map(|item| item.processing_count).unwrap_or(0),
                errors: counts.map(|item| item.error_count).unwrap_or(0),
            });
        }
    }

    facets.sort_by(|left, right| {
        left.facet
            .cmp(&right.facet)
            .then(left.topic_map.cmp(&right.topic_map))
            .then(left.topic_map_id.cmp(&right.topic_map_id))
    });

    Ok(TopicsExploreFacetsReport {
        project: topics_project_summary(ctx),
        facets,
    })
}

pub async fn fetch_topic_classifications(
    ctx: &ProjectContext,
    automation_id: Option<&str>,
    facet: Option<&str>,
    topic_map: Option<&str>,
    sort: TopicExploreSort,
    limit: usize,
    base_filter_clause: &str,
    print_queries: bool,
) -> Result<TopicClassificationsReport> {
    if limit == 0 {
        bail!("--limit must be greater than 0");
    }
    let topic_map = resolve_topic_explore_topic_map(ctx, automation_id, facet, topic_map).await?;
    let filter_clause = topic_map_filter_clause(&topic_map, base_filter_clause);
    let query = build_topic_classifications_query(
        &ctx.project.id,
        &topic_map.topic_map,
        &topic_map.topic_map_id,
        &filter_clause,
        sort,
        limit,
    );
    maybe_print_topic_query(print_queries, "classifications", &query);
    let response = execute_btql_value(&ctx.client, &query).await?;
    let classifications = btql_data_rows(&response)
        .into_iter()
        .map(topic_classification_row_from_btql)
        .collect();

    Ok(TopicClassificationsReport {
        project: topics_project_summary(ctx),
        topic_map,
        classifications,
    })
}

pub async fn fetch_topic_traces(
    ctx: &ProjectContext,
    automation_id: Option<&str>,
    facet: Option<&str>,
    topic_map: Option<&str>,
    topic: Option<&str>,
    topic_id: Option<&str>,
    sort: TopicExploreSort,
    limit: usize,
    cursor: Option<&str>,
    base_filter_clause: &str,
    print_queries: bool,
) -> Result<TopicTracesReport> {
    let mut users_cache = OrgUsersCache::default();
    fetch_topic_traces_with_user_cache(
        ctx,
        automation_id,
        facet,
        topic_map,
        topic,
        topic_id,
        sort,
        limit,
        cursor,
        base_filter_clause,
        print_queries,
        &mut users_cache,
    )
    .await
}

pub(super) async fn fetch_topic_traces_with_user_cache(
    ctx: &ProjectContext,
    automation_id: Option<&str>,
    facet: Option<&str>,
    topic_map: Option<&str>,
    topic: Option<&str>,
    topic_id: Option<&str>,
    sort: TopicExploreSort,
    limit: usize,
    cursor: Option<&str>,
    base_filter_clause: &str,
    print_queries: bool,
    users_cache: &mut OrgUsersCache,
) -> Result<TopicTracesReport> {
    if limit == 0 {
        bail!("--limit must be greater than 0");
    }
    if topic.is_none() && topic_id.is_none() {
        bail!("topic label selection required; pass --topic-id or --topic after choosing a row from `bt topics classifications`");
    }

    let topic_map = resolve_topic_explore_topic_map(ctx, automation_id, facet, topic_map).await?;
    let topic_cursor = parse_topic_trace_cursor(cursor, sort)?;
    let backend_cursor = if topic_cursor.is_some() {
        None
    } else {
        cursor.filter(|cursor| !cursor.trim().is_empty())
    };
    let mut filter_clause = topic_map_filter_clause(&topic_map, base_filter_clause);
    filter_clause = combine_filter_clauses([
        Some(filter_clause),
        topic_filter_clause(&topic_map, topic, topic_id)?,
        topic_cursor
            .as_ref()
            .map(|cursor| topic_trace_cursor_filter_clause(sort, cursor))
            .transpose()?,
    ]);
    let fetch_limit = if backend_cursor.is_some() {
        limit
    } else {
        limit.saturating_add(1)
    };
    let query = build_topic_traces_query(
        &ctx.project.id,
        &topic_map.topic_map,
        &topic_map.topic_map_id,
        &filter_clause,
        sort,
        fetch_limit,
        backend_cursor,
    );
    maybe_print_topic_query(print_queries, "traces", &query);
    let response = execute_btql_value(&ctx.client, &query).await?;
    let returned_rows = btql_data_len(&response);
    let project_url = app_project_url(
        &ctx.app_url,
        ctx.client.org_name(),
        &ctx.project.name,
        &["logs"],
    );
    let mut traces = btql_data_rows(&response)
        .into_iter()
        .map(|row| topic_trace_row_from_btql(row, &project_url))
        .collect::<Vec<_>>();
    let next_cursor = topic_trace_next_cursor(&traces, sort, limit)?
        .or_else(|| next_cursor_if_full_page(btql_cursor(&response), returned_rows, limit));
    if traces.len() > limit {
        traces.truncate(limit);
    }
    if let Err(err) =
        hydrate_trace_root_metadata(&ctx.client, &ctx.project.id, &mut traces, print_queries).await
    {
        eprintln!("warning: failed to resolve trace root metadata: {err}");
    }
    if let Err(err) = users_cache
        .hydrate_trace_users(&ctx.client, &mut traces)
        .await
    {
        eprintln!("warning: failed to resolve trace users: {err}");
    }

    Ok(TopicTracesReport {
        project: topics_project_summary(ctx),
        topic_map,
        topic: TopicTraceSelection {
            topic: topic.map(ToString::to_string),
            topic_id: topic_id.map(ToString::to_string),
        },
        traces,
        next_cursor,
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

        let body = UpsertObjectCursorRequest {
            automation_id: automation_id.clone(),
            object_id: object_id.clone(),
        };
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
        let seeded =
            seed_topic_automation_cursors(&ctx.client, &ctx.project.id, row, Some(window_seconds))
                .await?;
        let automation_id = stringish_value(row.get("id")).unwrap_or_default();

        rewound.push(TopicAutomationRewindResult {
            id: automation_id,
            name: string_value(row.get("name")).unwrap_or_else(|| "Topics".to_string()),
            object_id: seeded.object_id,
            window_seconds: seeded.window_seconds,
            start_xact_id: seeded.start_xact_id,
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

pub async fn enable_topics_config(
    ctx: &ProjectContext,
    create: TopicAutomationConfigCreate,
) -> Result<TopicAutomationConfig> {
    let existing = list_topic_automation_rows(&ctx.client, &ctx.project.id).await?;
    if !existing.is_empty() {
        bail!(
            "Topics is already enabled for this project; use `bt topics config set` to update it"
        );
    }

    let facet_names = normalize_facet_names(&create.facets);
    if facet_names.is_empty() {
        bail!("at least one facet name is required to enable Topics");
    }

    let window_seconds = create
        .window_seconds
        .unwrap_or(DEFAULT_TOPIC_WINDOW_SECONDS);
    let rerun_seconds = create.rerun_seconds.unwrap_or(DEFAULT_TOPIC_RERUN_SECONDS);
    let relabel_overlap_seconds = create
        .relabel_overlap_seconds
        .unwrap_or(DEFAULT_TOPIC_RELABEL_OVERLAP_SECONDS);
    let idle_seconds = create.idle_seconds.unwrap_or(DEFAULT_TOPIC_IDLE_SECONDS);
    let sampling_rate = create.sampling_rate.unwrap_or(DEFAULT_TOPIC_SAMPLING_RATE);
    let embedding_model = create
        .embedding_model
        .unwrap_or_else(|| DEFAULT_TOPIC_EMBEDDING_MODEL.to_string());

    let topic_map_functions = create_topic_map_function_refs(
        &ctx.client,
        &ctx.project.id,
        &facet_names,
        &embedding_model,
    )
    .await?;
    let request = RegisterTopicAutomationRequest {
        project_automation_name: create
            .name
            .unwrap_or_else(|| DEFAULT_TOPIC_AUTOMATION_NAME.to_string()),
        description: create
            .description
            .unwrap_or_else(|| DEFAULT_TOPIC_AUTOMATION_DESCRIPTION.to_string()),
        project_id: ctx.project.id.clone(),
        config: RegisterTopicAutomationConfig {
            event_type: "topic",
            sampling_rate,
            facet_functions: facet_names
                .iter()
                .map(|facet_name| GlobalFacetFunctionRef {
                    ref_type: "global",
                    name: facet_name.clone(),
                    function_type: "facet",
                })
                .collect(),
            topic_map_functions,
            scope: TraceScopeConfig {
                scope_type: "trace",
                idle_seconds,
            },
            rerun_seconds,
            relabel_overlap_seconds,
            backfill_time_range: format_duration_seconds(window_seconds),
            btql_filter: create.btql_filter,
        },
        update: true,
    };

    let response: Value = ctx
        .client
        .post("/api/project_automation/register", &request)
        .await?;
    let automation_row = project_automation_row_from_response(&response)?;
    seed_topic_automation_cursors(
        &ctx.client,
        &ctx.project.id,
        &automation_row,
        Some(window_seconds),
    )
    .await?;

    let mut function_cache = HashMap::new();
    build_topic_automation_config(&ctx.client, &automation_row, &mut function_cache).await
}

pub async fn delete_topics_config(
    ctx: &ProjectContext,
    automation_id: Option<&str>,
) -> Result<TopicsDeleteReport> {
    let rows = list_topic_automation_rows(&ctx.client, &ctx.project.id).await?;
    let row = resolve_single_topic_automation_row(rows, automation_id)?;
    let mut function_cache = HashMap::new();
    let automation = build_topic_automation_config(&ctx.client, &row, &mut function_cache).await?;

    delete_topic_automation(&ctx.client, &automation.id).await?;

    Ok(TopicsDeleteReport {
        project: TopicsProjectSummary {
            id: ctx.project.id.clone(),
            name: ctx.project.name.clone(),
            org_name: ctx.client.org_name().to_string(),
            topics_url: topics_url(&ctx.app_url, ctx.client.org_name(), &ctx.project.name),
        },
        automation,
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

pub async fn update_topic_map_config(
    ctx: &ProjectContext,
    patch: TopicMapConfigPatch,
) -> Result<TopicMapConfigUpdate> {
    let rows = list_topic_automation_rows(&ctx.client, &ctx.project.id).await?;
    let mut function_cache = HashMap::new();
    let resolved = resolve_topic_map_target(
        &ctx.client,
        rows,
        patch.automation_id.as_deref(),
        &patch.topic_map_target,
        &mut function_cache,
    )
    .await?;

    if let Some(source_facet) = patch.source_facet.as_deref() {
        validate_topic_map_source_facet(&ctx.client, &resolved, &mut function_cache, source_facet)
            .await?;
    }

    let function_row =
        load_function_row(&ctx.client, &mut function_cache, &resolved.function_id).await?;
    let mut function_data = function_row
        .get("function_data")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    if string_value(function_data.get("type")).as_deref() != Some("topic_map") {
        bail!(
            "topic map '{}' is backed by function '{}' with unsupported function_data.type",
            resolved.topic_map_name,
            resolved.function_id
        );
    }

    let mut payload = serde_json::Map::new();
    if let Some(name) = patch.name {
        payload.insert("name".to_string(), Value::String(name));
    }
    if let Some(description) = patch.description {
        payload.insert("description".to_string(), Value::String(description));
    }

    if let Some(source_facet) = patch.source_facet {
        function_data.insert("source_facet".to_string(), Value::String(source_facet));
    }
    if let Some(embedding_model) = patch.embedding_model {
        function_data.insert(
            "embedding_model".to_string(),
            Value::String(embedding_model),
        );
    }
    if let Some(distance_threshold) = patch.distance_threshold {
        function_data.insert(
            "distance_threshold".to_string(),
            Value::from(distance_threshold),
        );
    }
    if let Some(disable_reconciliation) = patch.disable_reconciliation {
        if disable_reconciliation {
            function_data.insert("disable_reconciliation".to_string(), Value::Bool(true));
        } else {
            function_data.remove("disable_reconciliation");
        }
    }

    let mut generation_settings = function_data
        .get("generation_settings")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let mut generation_settings_changed = false;
    if let Some(algorithm) = patch.algorithm {
        generation_settings.insert("algorithm".to_string(), Value::String(algorithm));
        generation_settings_changed = true;
    }
    if let Some(dimension_reduction) = patch.dimension_reduction {
        generation_settings.insert(
            "dimension_reduction".to_string(),
            Value::String(dimension_reduction),
        );
        generation_settings_changed = true;
    }
    if let Some(sample_size) = patch.sample_size {
        generation_settings.insert("sample_size".to_string(), Value::from(sample_size));
        generation_settings_changed = true;
    }
    if let Some(n_clusters) = patch.n_clusters {
        generation_settings.insert("n_clusters".to_string(), Value::from(n_clusters));
        generation_settings_changed = true;
    }
    if let Some(min_cluster_size) = patch.min_cluster_size {
        generation_settings.insert(
            "min_cluster_size".to_string(),
            Value::from(min_cluster_size),
        );
        generation_settings_changed = true;
    }
    if let Some(min_samples) = patch.min_samples {
        generation_settings.insert("min_samples".to_string(), Value::from(min_samples));
        generation_settings_changed = true;
    }
    if let Some(hierarchy_threshold) = patch.hierarchy_threshold {
        generation_settings.insert(
            "hierarchy_threshold".to_string(),
            Value::from(hierarchy_threshold),
        );
        generation_settings_changed = true;
    }
    if let Some(naming_model) = patch.naming_model {
        generation_settings.insert("naming_model".to_string(), Value::String(naming_model));
        generation_settings_changed = true;
    }
    if generation_settings_changed {
        function_data.insert(
            "generation_settings".to_string(),
            Value::Object(generation_settings),
        );
    }

    function_data.insert("type".to_string(), Value::String("topic_map".to_string()));
    payload.insert("function_data".to_string(), Value::Object(function_data));

    let path = format!("/v1/function/{}", encode(&resolved.function_id));
    let _: Value = ctx.client.patch(&path, &Value::Object(payload)).await?;

    let mut updated_function_cache = HashMap::new();
    let automation = build_topic_automation_config(
        &ctx.client,
        &resolved.automation_row,
        &mut updated_function_cache,
    )
    .await?;

    Ok(TopicMapConfigUpdate {
        automation,
        topic_map_id: resolved.function_id,
    })
}

pub fn topics_url(app_url: &str, org_name: &str, project_name: &str) -> String {
    app_project_url(app_url, org_name, project_name, &["topics"])
}

pub async fn fetch_topic_map_report_url(
    client: &ApiClient,
    function_id: &str,
    version: Option<&str>,
) -> Result<TopicMapReportUrl> {
    let request = TopicMapReportUrlRequest {
        function_id,
        version,
    };
    client.post("/topic-map-report-url", &request).await
}

pub async fn fetch_topic_map_btmap_url(
    client: &ApiClient,
    function_id: &str,
    version: Option<&str>,
) -> Result<TopicMapReportUrl> {
    let request = TopicMapReportUrlRequest {
        function_id,
        version,
    };
    client.post("/topic-map-btmap-url", &request).await
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

struct SeededTopicAutomationCursors {
    object_id: String,
    start_xact_id: String,
    window_seconds: i64,
}

async fn seed_topic_automation_cursors(
    client: &ApiClient,
    project_id: &str,
    automation_row: &Value,
    window_seconds_override: Option<i64>,
) -> Result<SeededTopicAutomationCursors> {
    let config = automation_row
        .get("config")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let window_seconds = window_seconds_override
        .or_else(|| backfill_time_range_to_window_seconds(config.get("backfill_time_range")))
        .ok_or_else(|| {
            anyhow::anyhow!(
                "topic automation {} is missing a valid backfill_time_range",
                stringish_value(automation_row.get("id")).unwrap_or_default()
            )
        })?;
    let object_id = topic_automation_object_id(project_id, config.get("data_scope"))?;
    let automation_id = stringish_value(automation_row.get("id")).unwrap_or_default();
    let start_xact_id = inclusive_start_xact_id_from_epoch_ms(
        Utc::now()
            .timestamp_millis()
            .saturating_sub(window_seconds.saturating_mul(1000)),
    );

    let reset_body = ResetObjectCursorRequest {
        automation_id: automation_id.clone(),
        object_id: object_id.clone(),
        start_xact_id: start_xact_id.clone(),
    };
    let _: Value = client
        .post("/brainstore/automation/reset-cursors", &reset_body)
        .await?;

    let upsert_body = UpsertObjectCursorRequest {
        automation_id,
        object_id: object_id.clone(),
    };
    let _: Value = client
        .post("/brainstore/automation/upsert-object-cursor", &upsert_body)
        .await?;

    Ok(SeededTopicAutomationCursors {
        object_id,
        start_xact_id,
        window_seconds,
    })
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

#[derive(Debug, Clone)]
struct ResolvedTopicMapTarget {
    automation_row: Value,
    function_id: String,
    topic_map_name: String,
}

async fn resolve_topic_map_target(
    client: &ApiClient,
    rows: Vec<Value>,
    automation_id: Option<&str>,
    topic_map_target: &str,
    function_cache: &mut HashMap<String, Value>,
) -> Result<ResolvedTopicMapTarget> {
    let rows = filter_or_resolve_topic_automation_rows(rows, automation_id)?;
    let normalized_target = topic_map_target.trim();
    if normalized_target.is_empty() {
        bail!("topic map target cannot be empty");
    }

    let mut matches = Vec::new();
    for row in rows {
        let config = row
            .get("config")
            .and_then(Value::as_object)
            .cloned()
            .unwrap_or_default();
        for topic_map_ref in config
            .get("topic_map_functions")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            let summary =
                summarize_topic_map_function(client, function_cache, topic_map_ref).await?;
            let Some(function_id) = summary.id.clone() else {
                continue;
            };
            if function_id == normalized_target
                || summary.name.eq_ignore_ascii_case(normalized_target)
            {
                matches.push(ResolvedTopicMapTarget {
                    automation_row: row.clone(),
                    function_id,
                    topic_map_name: summary.name,
                });
            }
        }
    }

    if matches.is_empty() {
        bail!("topic map '{normalized_target}' was not found");
    }
    if matches.len() == 1 {
        return Ok(matches.into_iter().next().expect("single topic map"));
    }

    let choices = matches
        .into_iter()
        .map(|item| {
            let automation_name = string_value(item.automation_row.get("name"))
                .unwrap_or_else(|| "Topics".to_string());
            let automation_id = stringish_value(item.automation_row.get("id")).unwrap_or_default();
            format!(
                "{} (topic map id: {}, automation: {} [{}])",
                item.topic_map_name, item.function_id, automation_name, automation_id
            )
        })
        .collect::<Vec<_>>()
        .join(", ");
    bail!(
        "topic map '{normalized_target}' matched multiple entries ({choices}); re-run with --automation-id or the topic map function ID"
    )
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

async fn validate_topic_map_source_facet(
    client: &ApiClient,
    resolved: &ResolvedTopicMapTarget,
    function_cache: &mut HashMap<String, Value>,
    source_facet: &str,
) -> Result<()> {
    let config = resolved
        .automation_row
        .get("config")
        .and_then(Value::as_object)
        .cloned()
        .unwrap_or_default();
    let facets =
        summarize_function_refs(client, function_cache, config.get("facet_functions")).await?;

    if facets.iter().any(|facet| facet.name == source_facet) {
        return Ok(());
    }

    let available = facets
        .iter()
        .map(|facet| facet.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    let automation_name =
        string_value(resolved.automation_row.get("name")).unwrap_or_else(|| "Topics".to_string());

    let detail = if available.is_empty() {
        "this automation has no configured facets".to_string()
    } else {
        format!("available facets: {available}")
    };
    bail!(
        "source facet '{source_facet}' was not found in topic automation '{automation_name}'; {detail}"
    )
}

async fn create_topic_map_function_refs(
    client: &ApiClient,
    project_id: &str,
    facet_names: &[String],
    embedding_model: &str,
) -> Result<Vec<TopicMapFunctionRef>> {
    let request = InsertFunctionsRequest {
        functions: facet_names
            .iter()
            .map(|facet_name| CreateTopicMapFunctionRequest {
                project_id: project_id.to_string(),
                name: facet_name.clone(),
                slug: slugify_topic_map_name(facet_name),
                function_type: "classifier",
                function_data: CreateTopicMapFunctionData {
                    data_type: "topic_map",
                    source_facet: facet_name.clone(),
                    embedding_model: embedding_model.to_string(),
                },
                if_exists: "ignore",
            })
            .collect(),
    };
    let response: Value = client.post("/insert-functions", &request).await?;
    let created = response
        .get("functions")
        .and_then(Value::as_array)
        .ok_or_else(|| anyhow::anyhow!("unexpected insert-functions response shape"))?;
    if created.len() != facet_names.len() {
        bail!("failed to create topic map functions");
    }

    created
        .iter()
        .map(|function_row| {
            let function_id = stringish_value(function_row.get("id"))
                .ok_or_else(|| anyhow::anyhow!("failed to create topic map functions"))?;
            Ok(TopicMapFunctionRef {
                function: FunctionIdRef {
                    ref_type: "function",
                    id: function_id,
                },
            })
        })
        .collect()
}

fn normalize_facet_names(facets: &[String]) -> Vec<String> {
    let mut normalized = Vec::new();
    for facet in facets {
        let trimmed = facet.trim();
        if trimmed.is_empty() {
            continue;
        }
        if normalized.iter().any(|existing| existing == trimmed) {
            continue;
        }
        normalized.push(trimmed.to_string());
    }
    if normalized.is_empty() {
        DEFAULT_TOPIC_FACET_NAMES
            .iter()
            .map(|name| (*name).to_string())
            .collect()
    } else {
        normalized
    }
}

fn slugify_topic_map_name(value: &str) -> String {
    let mut slug = String::new();
    let mut previous_was_dash = false;
    for ch in value.trim().chars().flat_map(|ch| ch.to_lowercase()) {
        if ch.is_ascii_lowercase() || ch.is_ascii_digit() {
            slug.push(ch);
            previous_was_dash = false;
            continue;
        }
        if !previous_was_dash {
            slug.push('-');
            previous_was_dash = true;
        }
    }
    let slug = slug.trim_matches('-').to_string();
    if slug.is_empty() {
        "topic-map".to_string()
    } else {
        slug
    }
}

async fn build_topic_automation_status(
    client: &ApiClient,
    project_id: &str,
    row: &Value,
    include_progress: bool,
    progress_window_seconds_override: Option<i64>,
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

    let mut total_traces = 0;
    let mut facet_current_count = 0;
    let mut facets = Vec::new();
    let mut topics = Vec::new();
    let mut progress_window_seconds = None;
    if include_progress {
        let topic_bars = build_topic_status_bars(client, function_cache, &config).await?;
        let facet_bars =
            build_facet_status_bars(client, function_cache, &config, &topic_bars).await?;
        let runtime_window_seconds = object_cursor
            .topic_runtime
            .as_ref()
            .and_then(|runtime| runtime.selected_window_seconds);
        if let Some((time_filter_clause, window_seconds)) = status_progress_time_filter_clause(
            config.get("backfill_time_range"),
            runtime_window_seconds,
            progress_window_seconds_override,
        ) {
            progress_window_seconds = Some(window_seconds);
            let progress = fetch_topic_automation_progress(
                client,
                project_id,
                &time_filter_clause,
                &cursor,
                &facet_bars,
                &topic_bars,
                false,
            )
            .await?;
            total_traces = progress.total_traces;
            facet_current_count = progress.facet_current_count;
            facets = progress.facets;
            topics = progress.topics;
        }
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
        progress_loaded: include_progress,
        progress_window_seconds,
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
    let body = AutomationProjectRequest {
        automation_id: automation_id.to_string(),
        project_id: project_id.to_string(),
    };
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
    let body = AutomationProjectRequest {
        automation_id: automation_id.to_string(),
        project_id: project_id.to_string(),
    };
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
    let mut summary = summarize_function_ref(
        client,
        function_cache,
        topic_map_ref.get("function").unwrap_or(&Value::Null),
    )
    .await?;
    summary.btql_filter = string_value(topic_map_ref.get("btql_filter"));
    if let Some(function_id) = summary.id.clone() {
        let function_row = load_function_row(client, function_cache, &function_id).await?;
        apply_topic_map_details(
            &mut summary,
            function_row.get("function_data").and_then(Value::as_object),
        );
    }
    Ok(summary)
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
            description: None,
            version: None,
            btql_filter: None,
            source_facet: None,
            embedding_model: None,
            distance_threshold: None,
            disable_reconciliation: None,
            generation_settings: None,
        });
    }

    let function_id = string_value(reference.get("id"));
    let Some(function_id) = function_id else {
        return Ok(FunctionSummary {
            name: "<unknown function>".to_string(),
            ref_type,
            function_type: None,
            id: None,
            description: None,
            version: None,
            btql_filter: None,
            source_facet: None,
            embedding_model: None,
            distance_threshold: None,
            disable_reconciliation: None,
            generation_settings: None,
        });
    };

    let function_row = load_function_row(client, function_cache, &function_id).await?;
    Ok(FunctionSummary {
        name: string_value(function_row.get("name")).unwrap_or_else(|| function_id.clone()),
        ref_type,
        function_type: string_value(function_row.get("function_type")),
        id: Some(function_id),
        description: string_value(function_row.get("description")),
        version: string_value(reference.get("version")),
        btql_filter: None,
        source_facet: None,
        embedding_model: None,
        distance_threshold: None,
        disable_reconciliation: None,
        generation_settings: None,
    })
}

fn apply_topic_map_details(
    summary: &mut FunctionSummary,
    function_data: Option<&serde_json::Map<String, Value>>,
) {
    let Some(function_data) = function_data else {
        return;
    };
    if string_value(function_data.get("type")).as_deref() != Some("topic_map") {
        return;
    }

    summary.source_facet = string_value(function_data.get("source_facet"));
    summary.embedding_model = string_value(function_data.get("embedding_model"));
    summary.distance_threshold = float_value(function_data.get("distance_threshold"));
    summary.disable_reconciliation = Some(
        function_data
            .get("disable_reconciliation")
            .and_then(Value::as_bool)
            .unwrap_or(false),
    );
    summary.generation_settings =
        topic_map_generation_settings_from_value(function_data.get("generation_settings"));
}

fn topic_map_generation_settings_from_value(
    value: Option<&Value>,
) -> Option<TopicMapGenerationSettings> {
    let map = value?.as_object()?;
    Some(TopicMapGenerationSettings {
        algorithm: string_value(map.get("algorithm")),
        dimension_reduction: string_value(map.get("dimension_reduction")),
        sample_size: int_value(map.get("sample_size")).and_then(|value| u32::try_from(value).ok()),
        n_clusters: int_value(map.get("n_clusters")).and_then(|value| u32::try_from(value).ok()),
        min_cluster_size: int_value(map.get("min_cluster_size"))
            .and_then(|value| usize::try_from(value).ok()),
        min_samples: int_value(map.get("min_samples"))
            .and_then(|value| usize::try_from(value).ok()),
        hierarchy_threshold: int_value(map.get("hierarchy_threshold"))
            .and_then(|value| usize::try_from(value).ok()),
        naming_model: string_value(map.get("naming_model")),
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

async fn delete_topic_automation(client: &ApiClient, automation_id: &str) -> Result<()> {
    let path = format!("/v1/project_automation/{}", encode(automation_id));
    client.delete(&path).await
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

async fn resolve_topic_explore_topic_map(
    ctx: &ProjectContext,
    automation_id: Option<&str>,
    facet: Option<&str>,
    topic_map: Option<&str>,
) -> Result<TopicExploreTopicMap> {
    let topic_maps = list_topic_explore_topic_maps(ctx, automation_id).await?;
    if topic_maps.is_empty() {
        bail!("no configured topic maps found; run `bt topics config` to inspect Topics setup");
    }

    let matches = topic_maps
        .into_iter()
        .filter(|candidate| {
            facet
                .map(|facet| {
                    candidate
                        .facet
                        .as_deref()
                        .map(|candidate| selector_matches(candidate, facet))
                        .unwrap_or(false)
                })
                .unwrap_or(true)
        })
        .filter(|candidate| {
            topic_map
                .map(|topic_map| {
                    selector_matches(&candidate.topic_map, topic_map)
                        || selector_matches(&candidate.topic_map_id, topic_map)
                })
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();

    match matches.len() {
        0 => bail!(
            "topic map selection did not match any configured topic map; run `bt topics facets` to list available facets and topic maps"
        ),
        1 => Ok(matches.into_iter().next().expect("single topic map match")),
        _ => {
            let choices = matches
                .iter()
                .take(5)
                .map(format_topic_map_choice)
                .collect::<Vec<_>>()
                .join(", ");
            let suffix = if matches.len() > 5 { ", ..." } else { "" };
            bail!(
                "topic map selection matched multiple entries ({choices}{suffix}); re-run with --facet or --topic-map"
            )
        }
    }
}

async fn list_topic_explore_topic_maps(
    ctx: &ProjectContext,
    automation_id: Option<&str>,
) -> Result<Vec<TopicExploreTopicMap>> {
    let rows = list_topic_automation_rows(&ctx.client, &ctx.project.id).await?;
    let rows = filter_or_resolve_topic_automation_rows(rows, automation_id)?;
    let mut function_cache = HashMap::new();
    let mut topic_maps = Vec::new();

    for row in &rows {
        let automation =
            build_topic_automation_config(&ctx.client, row, &mut function_cache).await?;
        for topic_map in &automation.topic_map_functions {
            let Some(topic_map_id) = topic_map.id.clone() else {
                continue;
            };
            let combined_filter = combine_optional_filter_clauses([
                automation.btql_filter.clone(),
                topic_map.btql_filter.clone(),
            ]);
            topic_maps.push(TopicExploreTopicMap {
                automation_id: automation.id.clone(),
                automation_name: automation.name.clone(),
                facet: topic_map.source_facet.clone(),
                topic_map: topic_map.name.clone(),
                topic_map_id,
                version: topic_map.version.clone(),
                classification_path: escape_btql_ident_path(&[
                    "classifications",
                    topic_map.name.as_str(),
                ]),
                btql_filter: combined_filter,
            });
        }
    }

    topic_maps.sort_by(|left, right| {
        left.facet
            .cmp(&right.facet)
            .then(left.topic_map.cmp(&right.topic_map))
            .then(left.topic_map_id.cmp(&right.topic_map_id))
    });
    Ok(topic_maps)
}

fn selector_matches(candidate: &str, selector: &str) -> bool {
    candidate == selector || candidate.eq_ignore_ascii_case(selector)
}

fn format_topic_map_choice(topic_map: &TopicExploreTopicMap) -> String {
    format!(
        "{} / {} (topic map id: {}, automation: {} [{}])",
        topic_map.facet.as_deref().unwrap_or("Ungrouped"),
        topic_map.topic_map,
        topic_map.topic_map_id,
        topic_map.automation_name,
        topic_map.automation_id
    )
}

fn topic_map_filter_clause(topic_map: &TopicExploreTopicMap, base_filter_clause: &str) -> String {
    let source_type_path = escape_btql_ident_path(&[
        "classifications",
        topic_map.topic_map.as_str(),
        "source",
        "type",
    ]);
    let source_id_path = escape_btql_ident_path(&[
        "classifications",
        topic_map.topic_map.as_str(),
        "source",
        "id",
    ]);
    combine_filter_clauses([
        Some(base_filter_clause.to_string()),
        topic_map.btql_filter.clone(),
        Some(format!("{} IS NOT NULL", topic_map.classification_path)),
        Some(format!("{source_type_path} = 'function'")),
        Some(format!(
            "{source_id_path} = {}",
            btql_string_literal(&topic_map.topic_map_id)
        )),
    ])
}

fn topic_filter_clause(
    topic_map: &TopicExploreTopicMap,
    topic: Option<&str>,
    topic_id: Option<&str>,
) -> Result<Option<String>> {
    match (topic, topic_id) {
        (Some(_), Some(_)) => bail!("use either --topic-id or --topic, not both"),
        (None, None) => Ok(None),
        (None, Some(topic_id)) => {
            let id_path =
                escape_btql_ident_path(&["classifications", topic_map.topic_map.as_str(), "id"]);
            Ok(Some(format!(
                "{id_path} = {}",
                btql_string_literal(topic_id)
            )))
        }
        (Some(topic), None) => {
            let id_path =
                escape_btql_ident_path(&["classifications", topic_map.topic_map.as_str(), "id"]);
            let label_path =
                escape_btql_ident_path(&["classifications", topic_map.topic_map.as_str(), "label"]);
            Ok(Some(format!(
                "COALESCE({label_path}, {id_path}) = {}",
                btql_string_literal(topic)
            )))
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct RepoOrigin {
    host: String,
    owner_path: String,
    repo: String,
}

impl RepoOrigin {
    fn canonical_slug(&self) -> String {
        format!("{}/{}/{}", self.host, self.owner_path, self.repo)
    }
}

async fn topic_repo_root_span_filter_clause(
    ctx: &ProjectContext,
    repo: Option<&str>,
    time_clause: &str,
    print_queries: bool,
) -> Result<Option<String>> {
    let Some(raw) = repo.map(str::trim).filter(|value| !value.is_empty()) else {
        return Ok(None);
    };
    let origin = parse_repo_origin_selector(raw)?;
    let root_span_ids = fetch_topic_repo_root_span_ids(
        &ctx.client,
        &ctx.project.id,
        time_clause,
        &origin,
        raw,
        print_queries,
    )
    .await?;
    Ok(Some(root_span_id_filter_clause(&root_span_ids)))
}

async fn fetch_topic_repo_root_span_ids(
    client: &ApiClient,
    project_id: &str,
    time_clause: &str,
    origin: &RepoOrigin,
    raw: &str,
    print_queries: bool,
) -> Result<Vec<String>> {
    let origin_filter = topic_repo_origin_filter_clause(origin, Some(raw));
    let mut root_span_ids = BTreeSet::new();
    let mut cursor = None::<String>;

    loop {
        if root_span_ids.len() >= TOPIC_REPO_ROOT_SPAN_MAX_IDS {
            bail!(
                "--repo matched at least {} traces; narrow the search with --window or --since before exploring",
                TOPIC_REPO_ROOT_SPAN_MAX_IDS
            );
        }

        let remaining = TOPIC_REPO_ROOT_SPAN_MAX_IDS - root_span_ids.len();
        let limit = remaining.min(TOPIC_REPO_ROOT_SPAN_PAGE_LIMIT);
        let query = build_topic_repo_root_spans_query(
            project_id,
            time_clause,
            &origin_filter,
            limit,
            cursor.as_deref(),
        );
        maybe_print_topic_query(print_queries, "repo-roots", &query);
        let response = execute_btql_value(client, &query).await?;
        let returned_rows = btql_data_len(&response);
        for row in btql_data_rows(&response) {
            if let Some(root_span_id) = value_as_string(row.get("root_span_id"))
                .map(|value| value.trim().to_string())
                .filter(|value| !value.is_empty())
            {
                root_span_ids.insert(root_span_id);
            }
        }

        let next_cursor = next_cursor_if_full_page(btql_cursor(&response), returned_rows, limit);
        let Some(next_cursor) = next_cursor else {
            break;
        };
        cursor = Some(next_cursor);
    }

    Ok(root_span_ids.into_iter().collect())
}

fn topic_repo_origin_filter_clause(origin: &RepoOrigin, raw: Option<&str>) -> String {
    let mut variants = repo_origin_url_variants(origin);
    if let Some(raw) = raw
        .map(trim_repo_selector)
        .filter(|value| !value.is_empty())
    {
        variants.insert(raw.to_string());
    }
    let values = variants
        .into_iter()
        .map(|value| btql_string_literal(&value))
        .collect::<Vec<_>>()
        .join(", ");
    let path = escape_btql_ident_path(&["metadata", "git_origin_url"]);
    format!("{path} IN [{values}]")
}

fn build_topic_repo_root_spans_query(
    project_id: &str,
    time_clause: &str,
    origin_filter: &str,
    limit: usize,
    cursor: Option<&str>,
) -> String {
    let cursor_clause = cursor
        .filter(|cursor| !cursor.trim().is_empty())
        .map(|cursor| format!(" | cursor: {}", btql_json_string_literal(cursor)))
        .unwrap_or_default();
    format!(
        "select: root_span_id, metadata.git_origin_url as git_origin_url | from: project_logs({}) spans | filter: ({time_clause}) AND (span_id = root_span_id) AND ({origin_filter}) | preview_length: 1 | sort: created DESC | limit: {limit}{cursor_clause}",
        btql_string_literal(project_id),
    )
}

fn repo_origin_url_variants(origin: &RepoOrigin) -> BTreeSet<String> {
    let mut variants = BTreeSet::new();
    for owner_repo_path in repo_owner_repo_path_variants(origin) {
        variants.insert(format!("{}/{}.git", origin.host, owner_repo_path));
        variants.insert(format!("{}/{}", origin.host, owner_repo_path));
        variants.insert(format!("{}.git", owner_repo_path));
        variants.insert(owner_repo_path.clone());
        variants.insert(format!("https://{}/{}.git", origin.host, owner_repo_path));
        variants.insert(format!("https://{}/{}", origin.host, owner_repo_path));
        variants.insert(format!("http://{}/{}.git", origin.host, owner_repo_path));
        variants.insert(format!("http://{}/{}", origin.host, owner_repo_path));
        variants.insert(format!("git@{}:{}.git", origin.host, owner_repo_path));
        variants.insert(format!("git@{}:{}", origin.host, owner_repo_path));
        variants.insert(format!("ssh://git@{}/{}.git", origin.host, owner_repo_path));
        variants.insert(format!("ssh://git@{}/{}", origin.host, owner_repo_path));
    }
    variants
}

fn repo_owner_repo_path_variants(origin: &RepoOrigin) -> BTreeSet<String> {
    let mut variants = BTreeSet::new();
    variants.insert(format!("{}/{}", origin.owner_path, origin.repo));
    let lower = format!(
        "{}/{}",
        origin.owner_path.to_ascii_lowercase(),
        origin.repo.to_ascii_lowercase()
    );
    variants.insert(lower);
    variants
}

fn repo_slug_from_origin_url(value: &str) -> Option<String> {
    parse_repo_origin_selector(value)
        .ok()
        .map(|origin| origin.canonical_slug())
}

fn parse_repo_origin_selector(value: &str) -> Result<RepoOrigin> {
    let value = trim_repo_selector(value);
    if value.is_empty() {
        bail!("--repo cannot be empty");
    }

    if let Some(rest) = value.strip_prefix("git@") {
        let Some((host, path)) = rest.split_once(':') else {
            bail!("invalid --repo git origin; expected git@host:owner/repo");
        };
        return repo_origin_from_host_path(host, path);
    }

    if let Some((_, rest)) = value.split_once("://") {
        let rest = rest.trim_start_matches('/');
        let rest = rest
            .rsplit_once('@')
            .map(|(_, without_credentials)| without_credentials)
            .unwrap_or(rest);
        let Some((authority, path)) = rest.split_once('/') else {
            bail!("invalid --repo URL; expected host/owner/repo");
        };
        return repo_origin_from_host_path(authority, path);
    }

    if let Some((host, path)) = value.split_once(':') {
        if host.contains('.') {
            return repo_origin_from_host_path(host, path);
        }
    }

    let parts = repo_path_parts(value);
    match parts.len() {
        2 => repo_origin_from_parts("github.com", &parts),
        3.. => repo_origin_from_parts(parts[0], &parts[1..]),
        _ => bail!("invalid --repo; use owner/repo, host/owner/repo, or a full git origin URL"),
    }
}

fn trim_repo_selector(value: &str) -> &str {
    value
        .trim()
        .split(['?', '#'])
        .next()
        .unwrap_or("")
        .trim_end_matches('/')
}

fn repo_origin_from_host_path(host: &str, path: &str) -> Result<RepoOrigin> {
    let host = host.split(':').next().unwrap_or(host);
    let parts = repo_path_parts(path);
    repo_origin_from_parts(host, &parts)
}

fn repo_path_parts(value: &str) -> Vec<&str> {
    value
        .trim_matches('/')
        .split('/')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        .collect()
}

fn repo_origin_from_parts(host: &str, parts: &[&str]) -> Result<RepoOrigin> {
    let host = host.trim().trim_end_matches('/').to_ascii_lowercase();
    if host.is_empty() {
        bail!("invalid --repo; repository host cannot be empty");
    }
    if parts.len() < 2 {
        bail!("invalid --repo; expected owner/repo after the host");
    }
    let owner_path = parts[..parts.len() - 1].join("/");
    let repo = parts[parts.len() - 1]
        .trim_end_matches('/')
        .strip_suffix(".git")
        .unwrap_or(parts[parts.len() - 1])
        .to_string();
    if owner_path.trim().is_empty() || repo.trim().is_empty() {
        bail!("invalid --repo; expected owner/repo after the host");
    }

    Ok(RepoOrigin {
        host,
        owner_path,
        repo,
    })
}

fn build_topic_classifications_query(
    project_id: &str,
    topic_map_name: &str,
    _topic_map_id: &str,
    filter_clause: &str,
    sort: TopicExploreSort,
    limit: usize,
) -> String {
    let topic_id_path = escape_btql_ident_path(&["classifications", topic_map_name, "id"]);
    let topic_label_path = escape_btql_ident_path(&["classifications", topic_map_name, "label"]);
    let topic_expr = format!("COALESCE({topic_label_path}, {topic_id_path})");
    format!(
        "from: project_logs({}) summary | dimensions: {topic_id_path} as topic_id, {topic_expr} as topic | measures: count_distinct(root_span_id) as traces, sum({}) as tokens, sum({}) as cost, avg({}) as avg_tokens, avg({}) as avg_cost, max(created) as latest | filter: {filter_clause} | sort: {} DESC | limit: {limit}",
        btql_string_literal(project_id),
        topic_tokens_expr(),
        topic_cost_expr(),
        topic_tokens_expr(),
        topic_cost_expr(),
        classification_sort_alias(sort),
    )
}

fn build_topic_traces_query(
    project_id: &str,
    topic_map_name: &str,
    _topic_map_id: &str,
    filter_clause: &str,
    sort: TopicExploreSort,
    limit: usize,
    cursor: Option<&str>,
) -> String {
    let topic_id_path = escape_btql_ident_path(&["classifications", topic_map_name, "id"]);
    let topic_label_path = escape_btql_ident_path(&["classifications", topic_map_name, "label"]);
    let topic_expr = format!("COALESCE({topic_label_path}, {topic_id_path})");
    let sort_expr = trace_sort_expr(sort);
    let cursor_clause = cursor
        .filter(|cursor| !cursor.trim().is_empty())
        .map(|cursor| format!(" | cursor: {}", btql_json_string_literal(cursor)))
        .unwrap_or_default();
    format!(
        "select: created, root_span_id, span_id, id, _pagination_key, span_attributes.created_by_user_id as created_by_user_id, metadata.git_origin_url as git_origin_url, {topic_id_path} as topic_id, {topic_expr} as topic, {sort_expr} as sort_value, metrics, input | from: project_logs({}) summary | filter: {filter_clause} | preview_length: 125 | sort: {sort_expr} DESC, _pagination_key DESC | limit: {limit}{cursor_clause}",
        btql_string_literal(project_id),
    )
}

fn build_topic_trace_root_metadata_query(project_id: &str, root_span_ids: &[String]) -> String {
    let root_filter = root_span_id_filter_clause(root_span_ids);
    format!(
        "select: root_span_id, span_attributes.created_by_user_id as created_by_user_id, metadata.git_origin_url as git_origin_url | from: project_logs({}) spans | filter: ({root_filter}) AND (span_id = root_span_id) | preview_length: 1 | limit: {}",
        btql_string_literal(project_id),
        root_span_ids.len().max(1),
    )
}

fn root_span_id_filter_clause(root_span_ids: &[String]) -> String {
    match root_span_ids {
        [] => "root_span_id = ''".to_string(),
        [single] => format!("root_span_id = {}", btql_string_literal(single)),
        _ => {
            let ids = root_span_ids
                .iter()
                .map(|root_span_id| btql_string_literal(root_span_id))
                .collect::<Vec<_>>()
                .join(", ");
            format!("root_span_id IN [{ids}]")
        }
    }
}

fn classification_sort_alias(sort: TopicExploreSort) -> &'static str {
    match sort {
        TopicExploreSort::Count => "traces",
        TopicExploreSort::Tokens => "tokens",
        TopicExploreSort::Cost => "cost",
        TopicExploreSort::AvgTokens => "avg_tokens",
        TopicExploreSort::AvgCost => "avg_cost",
        TopicExploreSort::Recent => "latest",
    }
}

fn normalize_trace_sort(sort: TopicExploreSort) -> TopicExploreSort {
    match sort {
        TopicExploreSort::AvgTokens => TopicExploreSort::Tokens,
        TopicExploreSort::AvgCost => TopicExploreSort::Cost,
        _ => sort,
    }
}

fn trace_sort_expr(sort: TopicExploreSort) -> &'static str {
    let sort = normalize_trace_sort(sort);
    match sort {
        TopicExploreSort::Tokens => topic_tokens_expr(),
        TopicExploreSort::Cost => topic_cost_expr(),
        TopicExploreSort::Count | TopicExploreSort::Recent => "created",
        TopicExploreSort::AvgTokens | TopicExploreSort::AvgCost => {
            unreachable!("normalized above")
        }
    }
}

fn topic_explore_sort_name(sort: TopicExploreSort) -> &'static str {
    match sort {
        TopicExploreSort::Count => "count",
        TopicExploreSort::Tokens => "tokens",
        TopicExploreSort::Cost => "cost",
        TopicExploreSort::AvgTokens => "avg-tokens",
        TopicExploreSort::AvgCost => "avg-cost",
        TopicExploreSort::Recent => "recent",
    }
}

fn topic_tokens_expr() -> &'static str {
    "COALESCE(metrics.total_tokens, metrics.tokens, metrics.prompt_tokens + metrics.completion_tokens, metrics.input_tokens + metrics.output_tokens, 0)"
}

fn topic_cost_expr() -> &'static str {
    "COALESCE(metrics.estimated_cost, metrics.cost, 0)"
}

fn topics_project_summary(ctx: &ProjectContext) -> TopicsProjectSummary {
    TopicsProjectSummary {
        id: ctx.project.id.clone(),
        name: ctx.project.name.clone(),
        org_name: ctx.client.org_name().to_string(),
        topics_url: topics_url(&ctx.app_url, ctx.client.org_name(), &ctx.project.name),
    }
}

fn btql_data_rows(response: &Value) -> Vec<&serde_json::Map<String, Value>> {
    response
        .get("data")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_object)
        .collect()
}

fn btql_data_len(response: &Value) -> usize {
    response
        .get("data")
        .and_then(Value::as_array)
        .map(Vec::len)
        .unwrap_or(0)
}

fn btql_cursor(response: &Value) -> Option<String> {
    value_as_string(response.get("cursor")).filter(|cursor| !cursor.is_empty())
}

fn next_cursor_if_full_page(
    cursor: Option<String>,
    returned_rows: usize,
    requested_limit: usize,
) -> Option<String> {
    cursor.filter(|_| requested_limit > 0 && returned_rows >= requested_limit)
}

fn topic_classification_row_from_btql(
    row: &serde_json::Map<String, Value>,
) -> TopicClassificationRow {
    TopicClassificationRow {
        topic: value_as_string(row.get("topic")).unwrap_or_else(|| "<unknown>".to_string()),
        topic_id: value_as_string(row.get("topic_id")).unwrap_or_default(),
        traces: read_btql_count_metric(Some(row), "traces"),
        tokens: read_btql_f64_metric(row, "tokens"),
        cost: read_btql_f64_metric(row, "cost"),
        avg_tokens: read_btql_f64_metric(row, "avg_tokens"),
        avg_cost: read_btql_f64_metric(row, "avg_cost"),
        latest: value_as_string(row.get("latest")),
    }
}

fn topic_trace_row_from_btql(
    row: &serde_json::Map<String, Value>,
    project_url: &str,
) -> TopicTraceRow {
    let root_span_id = value_as_string(row.get("root_span_id")).unwrap_or_default();
    let span_id = value_as_string(row.get("span_id")).filter(|value| !value.is_empty());
    let mut app_url = format!("{project_url}?r={}", encode(&root_span_id));
    if let Some(span_id) = span_id.as_deref() {
        app_url.push_str("&s=");
        app_url.push_str(&encode(span_id));
    }
    let git_origin_url = trace_git_origin_url(row);
    let repo = git_origin_url
        .as_deref()
        .and_then(repo_slug_from_origin_url);

    TopicTraceRow {
        created: value_as_string(row.get("created")),
        root_span_id,
        span_id,
        row_id: value_as_string(row.get("id")),
        created_by_user_id: trace_created_by_user_id(row),
        created_by_user_name: None,
        created_by_user_email: None,
        git_origin_url,
        repo,
        topic: value_as_string(row.get("topic")),
        topic_id: value_as_string(row.get("topic_id")),
        tokens: metrics_total_tokens(row.get("metrics")).unwrap_or(0.0),
        cost: metrics_cost(row.get("metrics")).unwrap_or(0.0),
        duration_seconds: metrics_duration_seconds(row.get("metrics")),
        input: row.get("input").map(format_preview_value),
        app_url,
        pagination_key: value_as_string(row.get("_pagination_key")),
        sort_value: topic_trace_cursor_value_from_btql(row.get("sort_value")),
    }
}

fn topic_trace_cursor_value_from_btql(value: Option<&Value>) -> Option<TopicTraceCursorValue> {
    match value {
        Some(Value::Number(number)) => number
            .as_f64()
            .filter(|value| value.is_finite())
            .map(TopicTraceCursorValue::Number),
        Some(Value::String(value)) => value
            .parse::<f64>()
            .ok()
            .filter(|value| value.is_finite())
            .map(TopicTraceCursorValue::Number)
            .or_else(|| Some(TopicTraceCursorValue::String(value.clone()))),
        Some(Value::Bool(value)) => Some(TopicTraceCursorValue::String(value.to_string())),
        _ => None,
    }
}

fn topic_trace_next_cursor(
    traces: &[TopicTraceRow],
    sort: TopicExploreSort,
    requested_limit: usize,
) -> Result<Option<String>> {
    if requested_limit == 0 || traces.len() <= requested_limit {
        return Ok(None);
    }

    let Some(last_visible) = traces.get(requested_limit.saturating_sub(1)) else {
        return Ok(None);
    };
    let Some(sort_value) = last_visible.sort_value.clone() else {
        return Ok(None);
    };
    let Some(pagination_key) = last_visible
        .pagination_key
        .as_deref()
        .filter(|value| !value.trim().is_empty())
    else {
        return Ok(None);
    };

    encode_topic_trace_cursor(&TopicTracePaginationCursor {
        version: 1,
        sort,
        sort_value,
        pagination_key: pagination_key.to_string(),
    })
    .map(Some)
}

fn encode_topic_trace_cursor(cursor: &TopicTracePaginationCursor) -> Result<String> {
    let payload = serde_json::to_vec(cursor)?;
    Ok(format!(
        "{TOPIC_TRACE_CURSOR_PREFIX}{}",
        URL_SAFE_NO_PAD.encode(payload)
    ))
}

fn parse_topic_trace_cursor(
    cursor: Option<&str>,
    expected_sort: TopicExploreSort,
) -> Result<Option<TopicTracePaginationCursor>> {
    let Some(cursor) = cursor.map(str::trim).filter(|cursor| !cursor.is_empty()) else {
        return Ok(None);
    };
    let Some(encoded) = cursor.strip_prefix(TOPIC_TRACE_CURSOR_PREFIX) else {
        return Ok(None);
    };

    let payload = URL_SAFE_NO_PAD
        .decode(encoded)
        .context("failed to decode topics trace cursor")?;
    let decoded: TopicTracePaginationCursor =
        serde_json::from_slice(&payload).context("failed to parse topics trace cursor")?;
    if decoded.version != 1 {
        bail!(
            "unsupported topics trace cursor version {}",
            decoded.version
        );
    }
    if decoded.sort != expected_sort {
        bail!(
            "cursor was created with --sort {}; this request uses --sort {}",
            topic_explore_sort_name(decoded.sort),
            topic_explore_sort_name(expected_sort)
        );
    }
    if decoded.pagination_key.trim().is_empty() {
        bail!("topics trace cursor is missing its pagination key");
    }

    Ok(Some(decoded))
}

fn topic_trace_cursor_filter_clause(
    sort: TopicExploreSort,
    cursor: &TopicTracePaginationCursor,
) -> Result<String> {
    let sort_expr = trace_sort_expr(sort);
    let pagination_key = btql_string_literal(&cursor.pagination_key);
    let sort_value = match &cursor.sort_value {
        TopicTraceCursorValue::Number(value) => {
            if !value.is_finite() {
                bail!("topics trace cursor has a non-finite sort value");
            }
            value.to_string()
        }
        TopicTraceCursorValue::String(value) => btql_string_literal(value),
    };

    Ok(format!(
        "({sort_expr} < {sort_value}) OR (({sort_expr} = {sort_value}) AND (_pagination_key < {pagination_key}))"
    ))
}

fn trace_created_by_user_id(row: &serde_json::Map<String, Value>) -> Option<String> {
    value_as_string(row.get("created_by_user_id"))
        .or_else(|| value_as_string(row.get("span_attributes.created_by_user_id")))
        .or_else(|| nested_value_as_string(row, &["span_attributes", "created_by_user_id"]))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

fn trace_git_origin_url(row: &serde_json::Map<String, Value>) -> Option<String> {
    value_as_string(row.get("git_origin_url"))
        .or_else(|| value_as_string(row.get("metadata.git_origin_url")))
        .or_else(|| nested_value_as_string(row, &["metadata", "git_origin_url"]))
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty())
}

async fn hydrate_trace_root_metadata(
    client: &ApiClient,
    project_id: &str,
    traces: &mut [TopicTraceRow],
    print_queries: bool,
) -> Result<()> {
    let root_span_ids = traces
        .iter()
        .filter(|trace| trace.created_by_user_id.is_none() || trace.git_origin_url.is_none())
        .filter_map(|trace| {
            let root_span_id = trace.root_span_id.trim();
            (!root_span_id.is_empty()).then(|| root_span_id.to_string())
        })
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>();
    if root_span_ids.is_empty() {
        return Ok(());
    }

    let query = build_topic_trace_root_metadata_query(project_id, &root_span_ids);
    maybe_print_topic_query(print_queries, "trace-root-metadata", &query);
    let response = execute_btql_value(client, &query).await?;
    let metadata_by_root_span_id = btql_data_rows(&response)
        .into_iter()
        .filter_map(|row| {
            let root_span_id = value_as_string(row.get("root_span_id"))?;
            Some((
                root_span_id,
                (trace_created_by_user_id(row), trace_git_origin_url(row)),
            ))
        })
        .collect::<HashMap<_, _>>();

    for trace in traces {
        if let Some((user_id, git_origin_url)) = metadata_by_root_span_id.get(&trace.root_span_id) {
            if trace.created_by_user_id.is_none() {
                trace.created_by_user_id = user_id.clone();
            }
            if trace.git_origin_url.is_none() {
                trace.git_origin_url = git_origin_url.clone();
                trace.repo = trace
                    .git_origin_url
                    .as_deref()
                    .and_then(repo_slug_from_origin_url);
            }
        }
    }

    Ok(())
}

async fn fetch_org_users(client: &ApiClient) -> Result<HashMap<String, OrgUser>> {
    let mut users = HashMap::new();
    let mut starting_after = None::<String>;

    loop {
        let mut path = format!(
            "/v1/user?org_name={}&limit={ORG_USERS_PAGE_LIMIT}",
            encode(client.org_name())
        );
        if let Some(cursor) = starting_after.as_deref() {
            path.push_str("&starting_after=");
            path.push_str(&encode(cursor));
        }

        let response: OrgUsersListResponse = client.get(&path).await?;
        let objects = response.objects;
        let page_len = objects.len();
        let next_cursor = objects.last().map(|user| user.id.clone());
        for user in objects {
            users.insert(user.id.clone(), user);
        }

        if page_len < ORG_USERS_PAGE_LIMIT {
            break;
        }
        let Some(next_cursor) = next_cursor else {
            break;
        };
        if starting_after.as_deref() == Some(next_cursor.as_str()) {
            break;
        }
        starting_after = Some(next_cursor);
    }

    Ok(users)
}

fn read_btql_f64_metric(row: &serde_json::Map<String, Value>, alias: &str) -> f64 {
    value_as_f64(row.get(alias)).unwrap_or(0.0)
}

fn metrics_total_tokens(metrics: Option<&Value>) -> Option<f64> {
    let metrics = metrics?.as_object()?;
    value_as_f64(metrics.get("total_tokens"))
        .or_else(|| value_as_f64(metrics.get("tokens")))
        .or_else(|| {
            let prompt = value_as_f64(metrics.get("prompt_tokens"))
                .or_else(|| value_as_f64(metrics.get("input_tokens")))?;
            let completion = value_as_f64(metrics.get("completion_tokens"))
                .or_else(|| value_as_f64(metrics.get("output_tokens")))?;
            Some(prompt + completion)
        })
}

fn metrics_cost(metrics: Option<&Value>) -> Option<f64> {
    let metrics = metrics?.as_object()?;
    value_as_f64(metrics.get("estimated_cost")).or_else(|| value_as_f64(metrics.get("cost")))
}

fn metrics_duration_seconds(metrics: Option<&Value>) -> Option<f64> {
    let metrics = metrics?.as_object()?;
    value_as_f64(metrics.get("duration")).or_else(|| {
        let start = value_as_f64(metrics.get("start"))?;
        let end = value_as_f64(metrics.get("end"))?;
        Some((end - start).max(0.0))
    })
}

fn value_as_f64(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Number(number)) => number.as_f64(),
        Some(Value::String(value)) => value.parse::<f64>().ok(),
        _ => None,
    }
}

fn value_as_string(value: Option<&Value>) -> Option<String> {
    match value {
        Some(Value::String(value)) => Some(value.clone()),
        Some(Value::Number(value)) => Some(value.to_string()),
        Some(Value::Bool(value)) => Some(value.to_string()),
        _ => None,
    }
}

fn nested_value_as_string(row: &serde_json::Map<String, Value>, path: &[&str]) -> Option<String> {
    let (first, rest) = path.split_first()?;
    let mut value = row.get(*first)?;
    for key in rest {
        value = value.as_object()?.get(*key)?;
    }
    value_as_string(Some(value))
}

fn format_preview_value(value: &Value) -> String {
    match value {
        Value::String(value) => value.clone(),
        Value::Null => String::new(),
        _ => serde_json::to_string(value).unwrap_or_else(|_| value.to_string()),
    }
}

fn combine_filter_clauses<I>(clauses: I) -> String
where
    I: IntoIterator<Item = Option<String>>,
{
    let parts = clauses
        .into_iter()
        .flatten()
        .map(|clause| clause.trim().to_string())
        .filter(|clause| !clause.is_empty())
        .map(|clause| format!("({clause})"))
        .collect::<Vec<_>>();
    if parts.is_empty() {
        "true".to_string()
    } else {
        parts.join(" AND ")
    }
}

fn combine_optional_filter_clauses<I>(clauses: I) -> Option<String>
where
    I: IntoIterator<Item = Option<String>>,
{
    let combined = combine_filter_clauses(clauses);
    if combined == "true" {
        None
    } else {
        Some(combined)
    }
}

fn btql_string_literal(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn btql_json_string_literal(value: &str) -> String {
    serde_json::to_string(value)
        .unwrap_or_else(|_| format!("\"{}\"", value.replace('\\', "\\\\").replace('\"', "\\\"")))
}

fn maybe_print_topic_query(enabled: bool, label: &str, query: &str) {
    if enabled {
        eprintln!("bt topics [{label}] BTQL:\n{query}\n");
    }
}

async fn fetch_topic_automation_progress(
    client: &ApiClient,
    project_id: &str,
    time_filter_clause: &str,
    cursor_status: &AutomationCursorSnapshot,
    facet_bars: &[FacetStatusBar],
    topic_bars: &[TopicStatusBar],
    print_queries: bool,
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
    maybe_print_topic_query(print_queries, "progress-total", &total_query);
    let total_response = execute_btql_value(client, &total_query).await?;
    let total_row = first_btql_row(&total_response);
    let aggregate_row = if measure_expressions.is_empty() {
        None
    } else {
        let aggregate_query = format!(
            "from: project_logs('{escaped_project_id}') spans | measures: {} | filter: {time_filter_clause}",
            measure_expressions.join(", ")
        );
        maybe_print_topic_query(print_queries, "progress-counts", &aggregate_query);
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
                checked_count: read_btql_count_metric(
                    aggregate_row.as_ref(),
                    &format!("facet_{index}_current_completed_traces"),
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
                checked_count: read_btql_count_metric(
                    aggregate_row.as_ref(),
                    &format!("topic_{index}_current_completed_traces"),
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
    let body = BtqlValueRequest {
        query,
        fmt: "json",
        brainstore_realtime: true,
    };
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

fn created_time_filter_clause_from_window_seconds(window_seconds: Option<i64>) -> Option<String> {
    window_seconds.map(|window_seconds| {
        format!(
            "created >= NOW() - INTERVAL {} SECOND",
            std::cmp::max(1, window_seconds)
        )
    })
}

fn status_progress_time_filter_clause(
    backfill_time_range: Option<&Value>,
    runtime_window_seconds: Option<i64>,
    progress_window_seconds_override: Option<i64>,
) -> Option<(String, i64)> {
    let window_seconds = match progress_window_seconds_override {
        Some(window_seconds) => window_seconds,
        None => {
            if let Some(absolute_filter) = absolute_time_filter_clause(backfill_time_range) {
                return Some(absolute_filter);
            }
            backfill_time_range_to_window_seconds(backfill_time_range)
                .or(runtime_window_seconds)
                .map(cap_status_progress_window_seconds)?
        }
    };
    Some((
        created_time_filter_clause_from_window_seconds(Some(window_seconds))?,
        window_seconds,
    ))
}

fn absolute_time_filter_clause(value: Option<&Value>) -> Option<(String, i64)> {
    let value = value?;
    let map = value.as_object()?;
    let from_raw = map.get("from")?.as_str()?;
    let to_raw = map.get("to")?.as_str()?;
    let from = from_raw.replace('\'', "''");
    let to = to_raw.replace('\'', "''");
    Some((
        format!("created >= '{from}' AND created <= '{to}'"),
        backfill_time_range_to_window_seconds(Some(value))?,
    ))
}

fn cap_status_progress_window_seconds(window_seconds: i64) -> i64 {
    window_seconds.clamp(1, MAX_STATUS_PROGRESS_WINDOW_SECONDS)
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

pub(crate) fn epoch_ms_from_xact_id(xact_id: &str) -> Option<i64> {
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
    fn topic_map_report_url_request_matches_endpoint_shape() {
        let without_version = serde_json::to_value(TopicMapReportUrlRequest {
            function_id: "fn_123",
            version: None,
        })
        .expect("serialize report url request");
        assert_eq!(without_version, json!({ "function_id": "fn_123" }));

        let with_version = serde_json::to_value(TopicMapReportUrlRequest {
            function_id: "fn_123",
            version: Some("0000000000000001"),
        })
        .expect("serialize report url request");
        assert_eq!(
            with_version,
            json!({
                "function_id": "fn_123",
                "version": "0000000000000001",
            })
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
    fn status_progress_time_filter_caps_to_one_day() {
        assert_eq!(
            status_progress_time_filter_clause(Some(&json!("7d")), None, None),
            Some((
                "created >= NOW() - INTERVAL 86400 SECOND".to_string(),
                86400
            ))
        );
        assert_eq!(
            status_progress_time_filter_clause(Some(&json!("6h")), None, None),
            Some((
                "created >= NOW() - INTERVAL 21600 SECOND".to_string(),
                21600
            ))
        );
        assert_eq!(
            status_progress_time_filter_clause(None, Some(604800), None),
            Some((
                "created >= NOW() - INTERVAL 86400 SECOND".to_string(),
                86400
            ))
        );
    }

    #[test]
    fn status_progress_time_filter_allows_explicit_override() {
        assert_eq!(
            status_progress_time_filter_clause(Some(&json!("1d")), None, Some(604800)),
            Some((
                "created >= NOW() - INTERVAL 604800 SECOND".to_string(),
                604800
            ))
        );
    }

    #[test]
    fn status_progress_time_filter_preserves_absolute_ranges() {
        let range = json!({
            "from": "2026-04-13T10:00:00Z",
            "to": "2026-04-13T11:30:00Z",
        });
        assert_eq!(
            status_progress_time_filter_clause(Some(&range), None, None),
            Some((
                "created >= '2026-04-13T10:00:00Z' AND created <= '2026-04-13T11:30:00Z'"
                    .to_string(),
                5400
            ))
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

    #[test]
    fn normalize_facet_names_uses_defaults_when_empty() {
        assert_eq!(
            normalize_facet_names(&[]),
            vec!["Task", "Sentiment", "Issues"]
        );
        assert_eq!(
            normalize_facet_names(&["  ".to_string(), "".to_string()]),
            vec!["Task", "Sentiment", "Issues"]
        );
    }

    #[test]
    fn normalize_facet_names_deduplicates_and_trims() {
        assert_eq!(
            normalize_facet_names(&[
                " Task ".to_string(),
                "Issues".to_string(),
                "Task".to_string(),
            ]),
            vec!["Task", "Issues"]
        );
    }

    #[test]
    fn slugify_topic_map_name_matches_python_shape() {
        assert_eq!(slugify_topic_map_name("Task"), "task");
        assert_eq!(
            slugify_topic_map_name("Emergent Issues 2026/04/14"),
            "emergent-issues-2026-04-14"
        );
        assert_eq!(slugify_topic_map_name("   "), "topic-map");
    }

    #[test]
    fn topic_explore_time_filter_combines_window_and_extra_filter() {
        let filter = combine_filter_clauses([
            Some(topic_explore_time_filter_clause(None, "6h").expect("time filter")),
            Some("metadata.environment = 'test'".to_string()),
        ]);

        assert_eq!(
            filter,
            "(created >= NOW() - INTERVAL 21600 SECOND) AND (metadata.environment = 'test')"
        );
    }

    #[test]
    fn topic_explore_filter_accepts_repo_shortcut_and_origin_urls() {
        let origin = parse_repo_origin_selector("test-org/test-repo").expect("origin");
        let filter = topic_repo_origin_filter_clause(&origin, Some("test-org/test-repo"));
        assert!(filter.contains("\"metadata\".\"git_origin_url\" IN ["));
        assert!(filter.contains("'github.com/test-org/test-repo'"));
        assert!(filter.contains("'test-org/test-repo'"));
        assert!(filter.contains("'https://github.com/test-org/test-repo.git'"));
        assert!(filter.contains("'https://github.com/test-org/test-repo'"));
        assert!(filter.contains("'git@github.com:test-org/test-repo.git'"));
        assert!(filter.contains("'ssh://git@github.com/test-org/test-repo.git'"));

        let query = build_topic_repo_root_spans_query(
            "test-project",
            "created >= NOW() - INTERVAL 604800 SECOND",
            &filter,
            25,
            Some("cursor-test"),
        );
        assert!(query.contains("from: project_logs('test-project') spans"));
        assert!(query.contains("created >= NOW() - INTERVAL 604800 SECOND"));
        assert!(query.contains("span_id = root_span_id"));
        assert!(query.contains("metadata.git_origin_url as git_origin_url"));
        assert!(query.contains("cursor: \"cursor-test\""));

        let https =
            parse_repo_origin_selector("https://github.com/test-org/test-repo.git").expect("https");
        let ssh = parse_repo_origin_selector("git@github.com:test-org/test-repo.git")
            .expect("ssh origin");
        let host_path =
            parse_repo_origin_selector("github.com/test-org/test-repo").expect("host path");
        assert_eq!(https.canonical_slug(), "github.com/test-org/test-repo");
        assert_eq!(https, ssh);
        assert_eq!(https, host_path);
    }

    #[test]
    fn topic_explore_filter_accepts_non_github_host() {
        let origin = parse_repo_origin_selector("gitlab.example.com/platform/agent/runtime.git")
            .expect("origin");

        assert_eq!(
            origin.canonical_slug(),
            "gitlab.example.com/platform/agent/runtime"
        );
        assert!(repo_origin_url_variants(&origin)
            .contains("git@gitlab.example.com:platform/agent/runtime.git"));
    }

    #[test]
    fn topic_classifications_query_is_bounded_and_topic_source_scoped() {
        let topic_map = TopicExploreTopicMap {
            automation_id: "auto_test_topics".to_string(),
            automation_name: "Topics".to_string(),
            facet: Some("Task".to_string()),
            topic_map: "Task".to_string(),
            topic_map_id: "fn_test_topic_map".to_string(),
            version: Some("123".to_string()),
            classification_path: escape_btql_ident_path(&["classifications", "Task"]),
            btql_filter: None,
        };
        let filter =
            topic_map_filter_clause(&topic_map, "created >= NOW() - INTERVAL 86400 SECOND");
        let query = build_topic_classifications_query(
            "test-project",
            "Task",
            "fn_test_topic_map",
            &filter,
            TopicExploreSort::Cost,
            25,
        );

        assert!(query.contains("from: project_logs('test-project') summary"));
        assert!(query.contains("created >= NOW() - INTERVAL 86400 SECOND"));
        assert!(query.contains("\"classifications\".\"Task\" IS NOT NULL"));
        assert!(
            query.contains("\"classifications\".\"Task\".\"source\".\"id\" = 'fn_test_topic_map'")
        );
        assert!(query.contains("sum(COALESCE(metrics.estimated_cost, metrics.cost, 0)) as cost"));
        assert!(query.contains("sort: cost DESC"));
        assert!(query.contains("limit: 25"));
    }

    #[test]
    fn topic_traces_query_filters_topic_id_and_sorts_tokens() {
        let topic_map = TopicExploreTopicMap {
            automation_id: "auto_test_topics".to_string(),
            automation_name: "Topics".to_string(),
            facet: Some("Task".to_string()),
            topic_map: "Task".to_string(),
            topic_map_id: "fn_test_topic_map".to_string(),
            version: None,
            classification_path: escape_btql_ident_path(&["classifications", "Task"]),
            btql_filter: None,
        };
        let mut filter =
            topic_map_filter_clause(&topic_map, "created >= NOW() - INTERVAL 86400 SECOND");
        filter = combine_filter_clauses([
            Some(filter),
            topic_filter_clause(&topic_map, None, Some("topic-test")).expect("topic filter"),
        ]);
        let query = build_topic_traces_query(
            "test-project",
            "Task",
            "fn_test_topic_map",
            &filter,
            TopicExploreSort::Tokens,
            10,
            Some("cursor-test"),
        );

        assert!(query.contains("select: created, root_span_id"));
        assert!(query.contains("_pagination_key"));
        assert!(query.contains("as sort_value"));
        assert!(query.contains("span_attributes.created_by_user_id as created_by_user_id"));
        assert!(query.contains("metadata.git_origin_url as git_origin_url"));
        assert!(query.contains("\"classifications\".\"Task\".\"id\" = 'topic-test'"));
        assert!(query.contains("sort: COALESCE(metrics.total_tokens, metrics.tokens"));
        assert!(query.contains(", _pagination_key DESC"));
        assert!(query.contains("limit: 10"));
        assert!(query.contains("cursor: \"cursor-test\""));
    }

    #[test]
    fn topic_trace_cursor_filters_after_last_visible_row() {
        let traces = (0..11)
            .map(|index| TopicTraceRow {
                created: Some(format!("2026-07-27T12:{index:02}:00Z")),
                root_span_id: format!("root-{index}"),
                span_id: None,
                row_id: None,
                created_by_user_id: None,
                created_by_user_name: None,
                created_by_user_email: None,
                git_origin_url: None,
                repo: None,
                topic: Some("Support".to_string()),
                topic_id: Some("topic-test".to_string()),
                tokens: (100 - index) as f64,
                cost: 0.0,
                duration_seconds: None,
                input: None,
                app_url: "https://example.com/app/org/p/project/logs".to_string(),
                pagination_key: Some(format!("p{index:020}")),
                sort_value: Some(TopicTraceCursorValue::Number((100 - index) as f64)),
            })
            .collect::<Vec<_>>();

        let cursor = topic_trace_next_cursor(&traces, TopicExploreSort::Tokens, 10)
            .expect("cursor")
            .expect("full page has cursor");
        let decoded = parse_topic_trace_cursor(Some(&cursor), TopicExploreSort::Tokens)
            .expect("parse cursor")
            .expect("topic cursor");
        assert_eq!(decoded.pagination_key, "p00000000000000000009");
        assert_eq!(decoded.sort_value, TopicTraceCursorValue::Number(91.0));

        let filter =
            topic_trace_cursor_filter_clause(TopicExploreSort::Tokens, &decoded).expect("filter");
        assert!(filter.contains(
            "COALESCE(metrics.total_tokens, metrics.tokens, metrics.prompt_tokens + metrics.completion_tokens, metrics.input_tokens + metrics.output_tokens, 0) < 91"
        ));
        assert!(filter.contains("_pagination_key < 'p00000000000000000009'"));
    }

    #[test]
    fn topic_trace_cursor_rejects_sort_mismatch() {
        let cursor = encode_topic_trace_cursor(&TopicTracePaginationCursor {
            version: 1,
            sort: TopicExploreSort::Cost,
            sort_value: TopicTraceCursorValue::Number(0.42),
            pagination_key: "p00000000000000000009".to_string(),
        })
        .expect("encode cursor");

        let err =
            parse_topic_trace_cursor(Some(&cursor), TopicExploreSort::Tokens).expect_err("err");
        assert!(err.to_string().contains("created with --sort cost"));
    }

    #[test]
    fn topic_trace_row_reads_created_by_user_id() {
        let row = serde_json::json!({
            "created": "2026-07-27T12:00:00Z",
            "root_span_id": "root-test",
            "span_id": "span-test",
            "created_by_user_id": "user-test",
            "_pagination_key": "p00000000000000000010",
            "sort_value": "15",
            "metrics": {
                "input_tokens": 10,
                "output_tokens": 5,
                "cost": 0.01
            },
            "git_origin_url": "git@github.com:test-org/test-repo.git"
        });
        let row = row.as_object().expect("row object");

        let trace = topic_trace_row_from_btql(row, "https://example.com/app/org/p/project/logs");

        assert_eq!(trace.created_by_user_id.as_deref(), Some("user-test"));
        assert_eq!(trace.created_by_user_name, None);
        assert_eq!(
            trace.git_origin_url.as_deref(),
            Some("git@github.com:test-org/test-repo.git")
        );
        assert_eq!(trace.repo.as_deref(), Some("github.com/test-org/test-repo"));
        assert_eq!(
            trace.pagination_key.as_deref(),
            Some("p00000000000000000010")
        );
        assert_eq!(trace.sort_value, Some(TopicTraceCursorValue::Number(15.0)));
        assert_eq!(trace.tokens, 15.0);
    }

    #[test]
    fn topic_trace_row_reads_nested_created_by_user_id() {
        let row = serde_json::json!({
            "root_span_id": "root-test",
            "span_attributes": {
                "created_by_user_id": "user-nested"
            }
        });
        let row = row.as_object().expect("row object");

        let trace = topic_trace_row_from_btql(row, "https://example.com/app/org/p/project/logs");

        assert_eq!(trace.created_by_user_id.as_deref(), Some("user-nested"));
    }

    #[test]
    fn topic_trace_root_metadata_query_is_root_span_bounded() {
        let query = build_topic_trace_root_metadata_query(
            "test-project",
            &["root-one".to_string(), "root-two".to_string()],
        );

        assert!(query.contains("from: project_logs('test-project') spans"));
        assert!(query.contains("root_span_id IN ['root-one', 'root-two']"));
        assert!(query.contains("span_id = root_span_id"));
        assert!(query.contains("span_attributes.created_by_user_id as created_by_user_id"));
        assert!(query.contains("metadata.git_origin_url as git_origin_url"));
    }

    #[test]
    fn org_user_display_name_prefers_name_then_email() {
        let named = OrgUser {
            id: "user-named".to_string(),
            given_name: Some("Ada".to_string()),
            family_name: Some("Lovelace".to_string()),
            email: Some("ada@example.com".to_string()),
        };
        let emailed = OrgUser {
            id: "user-emailed".to_string(),
            given_name: Some(" ".to_string()),
            family_name: None,
            email: Some("trace-user@example.com".to_string()),
        };

        assert_eq!(named.display_name().as_deref(), Some("Ada Lovelace"));
        assert_eq!(
            emailed.display_name().as_deref(),
            Some("trace-user@example.com")
        );
    }
}
