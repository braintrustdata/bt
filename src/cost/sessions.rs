//! Enumerate a project's prompt sessions (playgrounds).
//!
//! `playground_logs(...)` is keyed by `prompt_session_id`, so pricing playground
//! cost requires the list of session ids first. The REST list is cursor-paged
//! (`starting_after`) and returns rows newest-first, so we page until a short
//! page comes back.

use anyhow::Result;
use serde::Deserialize;
use urlencoding::encode;

use crate::http::ApiClient;

const PAGE_SIZE: u64 = 1000;

#[derive(Debug, Deserialize)]
struct PromptSession {
    id: String,
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    objects: Vec<PromptSession>,
}

/// List every prompt-session id in the project, paging through the cursor.
pub(super) async fn list_prompt_session_ids(
    client: &ApiClient,
    project_id: &str,
) -> Result<Vec<String>> {
    let mut ids = Vec::new();
    let mut starting_after: Option<String> = None;
    loop {
        let mut path = format!(
            "/v1/prompt_session?org_name={}&project_id={}&limit={}",
            encode(client.org_name()),
            encode(project_id),
            PAGE_SIZE,
        );
        if let Some(cursor) = &starting_after {
            path.push_str(&format!("&starting_after={}", encode(cursor)));
        }
        let response: ListResponse = client.get(&path).await?;
        let page_len = response.objects.len();
        let last_id = response.objects.last().map(|session| session.id.clone());
        ids.extend(response.objects.into_iter().map(|session| session.id));

        if page_len < PAGE_SIZE as usize {
            break;
        }
        match last_id {
            Some(cursor) => starting_after = Some(cursor),
            None => break,
        }
    }
    Ok(ids)
}
