use anyhow::Result;
use serde::Serialize;
use serde_json::Value;

use crate::http::{ApiClient, BtqlResponse};

// A playground summary row, as returned by the `getPromptSessionSummary` server
// action (`POST /api/actions/getPromptSessionSummary`). Field names mirror the
// aliases emitted by the action's SQL/zod schema (created_by_email,
// created_by_avatar_url) rather than the short `email`/`avatar_url` names.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct PlaygroundSummary {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub created: Option<String>,
    pub project_id: String,
    #[serde(default)]
    pub created_by_name: Option<String>,
    #[serde(default, rename = "created_by_email")]
    pub created_by_email: Option<String>,
    #[serde(default, rename = "created_by_avatar_url")]
    pub avatar_url: Option<String>,
}

impl PlaygroundSummary {
    /// Display name for a playground's creator, falling back to the email when
    /// the user has no given/family name set in Clerk (created_by_name is an
    /// empty string in that case).
    pub fn display_name(&self) -> Option<&str> {
        self.created_by_name
            .as_deref()
            .filter(|s| !s.trim().is_empty())
            .or(self.created_by_email.as_deref())
    }
}

/// A playground's metadata, plus its tasks/runs. Only `meta` is populated by
/// `get_full_playground_view` today; `tasks`/`playground_data`/`runs` are empty
/// stubs until the follow-up logs query lands.
#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct FullPlaygroundView {
    pub meta: PlaygroundMeta,
    #[serde(default)]
    pub tasks: Vec<PlaygroundTask>,
    #[serde(default)]
    pub playground_data: Option<PlaygroundData>,
    #[serde(default)]
    pub runs: Vec<PlaygroundRun>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct PlaygroundMeta {
    pub id: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub created: Option<String>,
    #[serde(default)]
    pub project_id: Option<String>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct PlaygroundTask {
    pub id: String,
    #[serde(default)]
    pub prompt_data: Option<Value>,
    #[serde(default)]
    pub function_data: Option<Value>,
    #[serde(default)]
    pub function_type: Option<Value>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct PlaygroundData {
    #[serde(default)]
    pub dataset_id: Option<String>,
    #[serde(default)]
    pub dataset_version: Option<String>,
    #[serde(default)]
    pub scorers: Option<Value>,
    #[serde(default)]
    pub settings: Option<Value>,
}

#[derive(Debug, Clone, Serialize, serde::Deserialize)]
pub struct PlaygroundRun {
    pub id: String,
    #[serde(default)]
    pub output: Option<Value>,
    #[serde(default)]
    pub error: Option<Value>,
    #[serde(default)]
    pub metrics: Option<Value>,
    #[serde(default)]
    pub scores: Option<Value>,
    #[serde(default)]
    pub span_attributes: Option<Value>,
    #[serde(default)]
    pub origin: Option<Value>,
}

/// Envelope expected by server-action routes (`invokeServerActionHelper`).
#[derive(Serialize)]
struct ServerActionBody<'a, T: Serialize> {
    auth_info: AuthInfo<'a>,
    function_args: &'a T,
}

#[derive(Serialize)]
struct AuthInfo<'a> {
    org_name: &'a str,
}

/// Post a server action over the `/api/actions/<name>` HTTP surface used by the
/// braintrust web app. The CLI's bearer API key works as-is (the route derives
/// auth from the Authorization header via `loginToAuthId`).
async fn post_server_action<T: serde::de::DeserializeOwned, A: Serialize>(
    client: &ApiClient,
    action: &str,
    function_args: &A,
) -> Result<T> {
    let body = ServerActionBody {
        auth_info: AuthInfo {
            org_name: client.org_name(),
        },
        function_args,
    };
    let path = format!("/api/actions/{action}");
    client.post(&path, &body).await
}

#[derive(Serialize)]
struct SummaryArgs<'a> {
    org_name: &'a str,
    project_name: &'a str,
}

pub async fn list_playground_summaries(
    client: &ApiClient,
    project: &str,
) -> Result<Vec<PlaygroundSummary>> {
    let args = SummaryArgs {
        org_name: client.org_name(),
        project_name: project,
    };
    let summaries: Vec<PlaygroundSummary> =
        post_server_action(client, "getPromptSessionSummary", &args).await?;
    Ok(summaries)
}

/// How far back to look for playground runs. The playground store has no
/// index hints from the meta, so bound the `playground_logs` scan with a
/// timestamp window (per BTQL Safety) and surface the most recent runs.
const RUNS_LOOKBACK_SECONDS: i64 = 90 * 24 * 60 * 60;

/// The number of recent runs to load for `playgrounds view`.
pub const RUNS_LIMIT: usize = 50;

/// Quote a string literal for BTQL using single-quote SQL escaping.
fn btql_str(value: &str) -> String {
    let single = "\u{27}";
    let escaped = value.replace(single, &format!("{single}{single}"));
    format!("{single}{escaped}{single}")
}

/// Fetch a playground's **metadata** via the `getPromptSessionMeta` server
/// action (`POST /api/actions/getPromptSessionMeta`). There is no public
/// `/v1/playgrounds/:id/view` endpoint — that path was a no-op that the edge
/// 403'd — so we resolve by name through the action instead.
///
/// Runs come from a distinct BTQL store, `playground_logs(<prompt_session_id>)`,
/// the same source the web app's playground page reads from. `project_logs`
/// does not contain playground runs, so querying it yields "No runs found"
/// even for playgrounds with real activity. Tasks (the playground's prompt
/// columns/models) live in the prompt-session DML layer and are not surfaced
/// by `getPromptSessionMeta`, so `tasks` is left empty until that fetch lands.
pub async fn get_full_playground_view(
    client: &ApiClient,
    project_name: &str,
    playground_name: &str,
) -> Result<FullPlaygroundView> {
    #[derive(Serialize)]
    struct MetaArgs<'a> {
        org_name: &'a str,
        project_name: &'a str,
        playground: &'a str,
    }
    let args = MetaArgs {
        org_name: client.org_name(),
        project_name,
        playground: playground_name,
    };
    let meta: Option<PlaygroundMeta> =
        post_server_action(client, "getPromptSessionMeta", &args).await?;
    let meta = meta.ok_or_else(|| {
        anyhow::anyhow!("playground '{playground_name}' not found in project '{project_name}'")
    })?;

    let runs = load_playground_runs(client, &meta.id)
        .await
        .unwrap_or_else(|e| {
            // Runs are best-effort: never fail the whole view if the logs query
            // errors (the meta is already a valid summary on its own).
            eprintln!(
                "warning: could not load playground runs ({e:#}); run `bt view logs` for details"
            );
            Vec::new()
        });

    Ok(FullPlaygroundView {
        meta,
        tasks: Vec::new(),
        playground_data: None,
        runs,
    })
}

/// Load recent runs for a playground from the `playground_logs` BTQL store,
/// scoped to the prompt-session id. The query always carries a `created`
/// window to satisfy BTQL Safety (no unbounded scans).
fn build_playground_runs_query(prompt_session_id: &str, limit: usize) -> String {
    // `summary` mode matches the rows the web app renders as playground runs.
    // Sort newest-first so the limit keeps the most recent activity.
    format!(
        "select: id, created, output, error, metrics, scores, span_attributes, origin |          from: playground_logs({}) summary |          filter: created >= NOW() - INTERVAL {} SECOND |          sort: _pagination_key DESC |          limit: {}",
        btql_str(prompt_session_id),
        RUNS_LOOKBACK_SECONDS,
        limit,
    )
}

async fn load_playground_runs(
    client: &ApiClient,
    prompt_session_id: &str,
) -> Result<Vec<PlaygroundRun>> {
    let query = build_playground_runs_query(prompt_session_id, RUNS_LIMIT);
    let response: BtqlResponse<PlaygroundRun> = client.btql(&query).await?;
    Ok(response.data)
}

#[derive(Serialize)]
struct IdBody<'a> {
    id: &'a str,
}

pub async fn delete_playground(client: &ApiClient, id: &str) -> Result<()> {
    let body = IdBody { id };
    // The `pages/api/prompt_session/delete_id` route is a Next.js Pages API
    // helper whose schema (`idParamSchema`) strictly rejects extra fields.
    // It is invoked over POST — sending a DELETE is blocked at the edge
    // (returns a misleading 403 "missing equal-sign in Authorization header"
    // that is really the gateway rejecting the DELETE method, not an auth
    // failure), so use POST even though this is a deletion.
    let _: serde_json::Value = client.post("/api/prompt_session/delete_id", &body).await?;
    Ok(())
}

#[derive(Serialize)]
struct PatchBody<'a> {
    id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    description: Option<&'a str>,
}

/// Rename and/or update the description of a playground via the `patch_id`
/// route. At least one of `new_name`/`description` must be `Some`.
pub async fn rename_playground(
    client: &ApiClient,
    id: &str,
    new_name: Option<&str>,
    description: Option<&str>,
) -> Result<()> {
    let body = PatchBody {
        id,
        name: new_name,
        description,
    };
    // Like `delete_id`, the `patch_id` route is a POST-only Next.js Pages API
    // helper (PATCH is rejected at the edge with a misleading auth 403), so
    // we POST the patch body despite it being an update.
    let _: serde_json::Value = client.post("/api/prompt_session/patch_id", &body).await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn btql_str_quotes_and_escapes_single_quotes() {
        // No apostrophes.
        let id = "396d459b-0f9f-497d-ad53-9ec30f0920e9";
        assert_eq!(btql_str(id), format!("'{id}'"));
        // Apostrophes are doubled (SQL escape).
        assert_eq!(btql_str("a'b"), "'a''b'");
    }

    #[test]
    fn playground_runs_query_uses_creation_window_and_limit() {
        let q = build_playground_runs_query("abc-123", 50);
        // Collapse stray whitespace so the assertions are robust to fmt's
        // reflowing of the multi-clause BTQL string.
        let norm: String = q.split_whitespace().collect::<Vec<_>>().join(" ");
        // BTQL Safety: the query must carry a real timestamp filter.
        assert!(
            norm.contains("created >= NOW() - INTERVAL"),
            "missing time filter: {q}"
        );
        // Source must be the playground_logs store scoped to the session id.
        assert!(
            norm.contains("from: playground_logs('abc-123') summary"),
            "wrong source: {q}"
        );
        // Must be bounded, newest-first.
        assert!(norm.contains("| limit: 50"), "missing limit: {q}");
        assert!(
            norm.contains("sort: _pagination_key DESC"),
            "wrong sort: {q}"
        );
    }
}
