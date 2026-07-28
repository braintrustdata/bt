//! Resolve user and service-account IDs returned by cost grouping into
//! human-readable details.
//!
//! IDs remain the canonical grouping key. This module only enriches those keys
//! with names and emails for rendering.

use std::collections::{HashMap, HashSet};

use anyhow::{Context, Result};
use serde::Deserialize;
use urlencoding::encode;
use uuid::Uuid;

use crate::http::ApiClient;

/// Keep filtered list requests comfortably below common URL-length limits.
const USER_IDS_PER_REQUEST: usize = 100;
/// Service tokens are the public API's directory of service accounts. Unlike
/// users, that endpoint cannot filter by service-account ID, so page through it.
const SERVICE_TOKENS_PAGE_SIZE: usize = 1000;

#[derive(Debug, Clone, Deserialize, PartialEq, Eq)]
pub(super) struct User {
    pub id: String,
    #[serde(default)]
    pub given_name: Option<String>,
    #[serde(default)]
    pub family_name: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
}

impl User {
    pub(super) fn name(&self) -> Option<String> {
        let parts: Vec<&str> = [self.given_name.as_deref(), self.family_name.as_deref()]
            .into_iter()
            .flatten()
            .map(str::trim)
            .filter(|part| !part.is_empty())
            .collect();
        (!parts.is_empty()).then(|| parts.join(" "))
    }

    pub(super) fn email(&self) -> Option<&str> {
        self.email
            .as_deref()
            .map(str::trim)
            .filter(|email| !email.is_empty())
    }
}

pub(super) type UserMap = HashMap<String, User>;

pub(super) fn user_map(users: Vec<User>) -> UserMap {
    users
        .into_iter()
        .map(|user| (user.id.clone(), user))
        .collect()
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    objects: Vec<User>,
}

#[derive(Debug, Deserialize)]
struct ServiceToken {
    id: String,
    #[serde(default)]
    service_account_id: Option<String>,
    #[serde(default)]
    service_account_name: Option<String>,
}

impl ServiceToken {
    fn into_user(self) -> Option<User> {
        Some(User {
            id: self.service_account_id?,
            given_name: self.service_account_name,
            family_name: None,
            email: None,
        })
    }
}

#[derive(Debug, Deserialize)]
struct ServiceTokenListResponse {
    objects: Vec<ServiceToken>,
}

/// Fetch the visible users matching `ids`. Invalid/non-UUID values and IDs no
/// longer visible to the current organization are left unresolved by design.
pub(super) async fn list_users_by_ids(client: &ApiClient, ids: &[String]) -> Result<Vec<User>> {
    let mut ids: Vec<String> = ids
        .iter()
        .filter(|id| Uuid::parse_str(id).is_ok())
        .cloned()
        .collect::<HashSet<_>>()
        .into_iter()
        .collect();
    ids.sort_unstable();

    let mut users = Vec::new();
    for chunk in ids.chunks(USER_IDS_PER_REQUEST) {
        let path = list_path(client.org_name(), chunk);
        let response: ListResponse = client
            .get(&path)
            .await
            .with_context(|| "failed to resolve users for the cost breakdown")?;
        users.extend(response.objects);
    }
    Ok(users)
}

/// Resolve service-account IDs through the service-token directory. The user
/// endpoint intentionally omits service accounts. Service-token rows also carry
/// synthetic, non-routable account emails; do not expose those as user emails.
pub(super) async fn list_service_accounts_by_ids(
    client: &ApiClient,
    ids: &[String],
) -> Result<Vec<User>> {
    let wanted: HashSet<String> = ids
        .iter()
        .filter(|id| Uuid::parse_str(id).is_ok())
        .cloned()
        .collect();
    if wanted.is_empty() {
        return Ok(Vec::new());
    }

    let mut accounts = Vec::new();
    let mut found = HashSet::new();
    let mut starting_after: Option<String> = None;
    loop {
        let path = service_token_list_path(client.org_name(), starting_after.as_deref());
        let response: ServiceTokenListResponse = client
            .get(&path)
            .await
            .with_context(|| "failed to resolve service accounts for the cost breakdown")?;
        let page_len = response.objects.len();
        let last_id = response.objects.last().map(|token| token.id.clone());

        for token in response.objects {
            let Some(account_id) = token.service_account_id.as_deref() else {
                continue;
            };
            if wanted.contains(account_id) && found.insert(account_id.to_string()) {
                if let Some(account) = token.into_user() {
                    accounts.push(account);
                }
            }
        }

        if found.len() == wanted.len() || page_len < SERVICE_TOKENS_PAGE_SIZE {
            break;
        }
        match last_id {
            Some(cursor) => starting_after = Some(cursor),
            None => break,
        }
    }
    Ok(accounts)
}

fn list_path(org_name: &str, ids: &[String]) -> String {
    let mut params = vec![
        format!("org_name={}", encode(org_name)),
        format!("limit={}", ids.len()),
    ];
    params.extend(ids.iter().map(|id| format!("ids={}", encode(id))));
    format!("/v1/user?{}", params.join("&"))
}

fn service_token_list_path(org_name: &str, starting_after: Option<&str>) -> String {
    let mut path = format!(
        "/v1/service_token?org_name={}&limit={SERVICE_TOKENS_PAGE_SIZE}",
        encode(org_name),
    );
    if let Some(cursor) = starting_after {
        path.push_str(&format!("&starting_after={}", encode(cursor)));
    }
    path
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn combines_non_empty_name_parts() {
        let both = User {
            id: "00000000-0000-0000-0000-000000000001".to_string(),
            given_name: Some("Test".to_string()),
            family_name: Some("User".to_string()),
            email: Some("test-user@example.test".to_string()),
        };
        assert_eq!(both.name().as_deref(), Some("Test User"));
        assert_eq!(both.email(), Some("test-user@example.test"));

        let family_only = User {
            given_name: Some("  ".to_string()),
            family_name: Some("User".to_string()),
            ..both
        };
        assert_eq!(family_only.name().as_deref(), Some("User"));
    }

    #[test]
    fn list_path_uses_repeated_encoded_ids() {
        let path = list_path(
            "test org",
            &[
                "00000000-0000-0000-0000-000000000001".to_string(),
                "00000000-0000-0000-0000-000000000002".to_string(),
            ],
        );
        assert_eq!(
            path,
            "/v1/user?org_name=test%20org&limit=2&ids=00000000-0000-0000-0000-000000000001&ids=00000000-0000-0000-0000-000000000002"
        );
    }

    #[test]
    fn converts_service_token_to_service_account_user() {
        let account = ServiceToken {
            id: "00000000-0000-0000-0000-000000000001".to_string(),
            service_account_id: Some("00000000-0000-0000-0000-000000000002".to_string()),
            service_account_name: Some("test-service-account".to_string()),
        }
        .into_user()
        .expect("service account ID");

        assert_eq!(account.id, "00000000-0000-0000-0000-000000000002");
        assert_eq!(account.name().as_deref(), Some("test-service-account"));
        assert_eq!(account.email(), None);
    }

    #[test]
    fn service_token_path_encodes_org_and_cursor() {
        assert_eq!(
            service_token_list_path("test org", Some("00000000-0000-0000-0000-000000000001")),
            "/v1/service_token?org_name=test%20org&limit=1000&starting_after=00000000-0000-0000-0000-000000000001"
        );
    }
}
