use crate::auth::ProfileInfo;

/// Slug for the authenticated human (name, else email local part). Org is
/// deliberately not a fallback: a bare API key has no author, so callers
/// substitute a generic placeholder rather than name the snapshot after the org.
pub(crate) fn profile_author_slug(profile: &ProfileInfo) -> Option<String> {
    [
        profile.user_name.as_deref(),
        profile.email.as_deref().and_then(email_local_part),
    ]
    .into_iter()
    .flatten()
    .find_map(sanitize_name_segment)
}

fn email_local_part(email: &str) -> Option<&str> {
    email
        .split_once('@')
        .map(|(local, _)| local)
        .or(Some(email))
}

pub(crate) fn sanitize_name_segment(value: &str) -> Option<String> {
    let mut normalized = String::new();
    let mut last_was_dash = false;

    for ch in value.chars() {
        if ch.is_ascii_alphanumeric() {
            normalized.push(ch.to_ascii_lowercase());
            last_was_dash = false;
        } else if !normalized.is_empty() && !last_was_dash {
            normalized.push('-');
            last_was_dash = true;
        }
    }

    while normalized.ends_with('-') {
        normalized.pop();
    }

    if normalized.is_empty() {
        None
    } else {
        Some(normalized)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn profile_info(
        org_name: Option<&str>,
        user_name: Option<&str>,
        email: Option<&str>,
    ) -> ProfileInfo {
        ProfileInfo {
            auth_method: if email.is_some() || user_name.is_some() {
                "oauth"
            } else {
                "api_key"
            }
            .to_string(),
            org_name: org_name.map(ToOwned::to_owned),
            user_name: user_name.map(ToOwned::to_owned),
            email: email.map(ToOwned::to_owned),
            api_key_hint: None,
        }
    }

    #[test]
    fn profile_author_slug_prefers_user_name() {
        let profile = profile_info(None, Some("Alice Smith"), Some("alice@example.com"));
        assert_eq!(
            profile_author_slug(&profile).as_deref(),
            Some("alice-smith")
        );
    }

    #[test]
    fn profile_author_slug_falls_back_to_email_local_part() {
        let profile = profile_info(None, None, Some("alice.dev@example.com"));
        assert_eq!(profile_author_slug(&profile).as_deref(), Some("alice-dev"));
    }

    #[test]
    fn profile_author_slug_returns_none_without_identity() {
        let profile = profile_info(None, None, None);
        assert_eq!(profile_author_slug(&profile), None);
    }

    #[test]
    fn profile_author_slug_ignores_org_name() {
        // A bare API key has an org but no human identity — not an author.
        let profile = profile_info(Some("test-org"), None, None);
        assert_eq!(profile_author_slug(&profile), None);
    }

    #[test]
    fn sanitize_name_segment_collapses_non_alnum() {
        assert_eq!(
            sanitize_name_segment("  A/B C__D  ").as_deref(),
            Some("a-b-c-d")
        );
        assert!(sanitize_name_segment("!!!").is_none());
    }
}
