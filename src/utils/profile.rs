use crate::auth::ProfileInfo;

#[cfg(test)]
fn resolve_profile_info_from_profiles(
    profile: Option<&str>,
    org: Option<&str>,
    profiles: Vec<ProfileInfo>,
) -> Option<ProfileInfo> {
    if let Some(profile_name) = profile {
        if let Some(profile) = profiles
            .iter()
            .find(|profile| profile.name == profile_name)
            .cloned()
        {
            return Some(profile);
        }
    }

    if let Some(org_name) = org {
        if profiles.iter().any(|profile| profile.name == org_name) {
            return profiles
                .into_iter()
                .find(|profile| profile.name == org_name);
        }

        let org_matches: Vec<&ProfileInfo> = profiles
            .iter()
            .filter(|profile| profile.org_name.as_deref() == Some(org_name))
            .collect();
        if org_matches.len() == 1 {
            let profile_name = org_matches[0].name.clone();
            return profiles
                .into_iter()
                .find(|profile| profile.name == profile_name);
        }
        let oauth_matches: Vec<&ProfileInfo> = org_matches
            .iter()
            .copied()
            .filter(|profile| profile.email.is_some() || profile.user_name.is_some())
            .collect();
        if oauth_matches.len() == 1 {
            let profile_name = oauth_matches[0].name.clone();
            return profiles
                .into_iter()
                .find(|profile| profile.name == profile_name);
        }
        return None;
    }

    if profiles.len() == 1 {
        return profiles.into_iter().next();
    }

    None
}

pub(crate) fn profile_author_slug(profile: &ProfileInfo) -> Option<String> {
    [
        profile.user_name.as_deref(),
        profile.email.as_deref().and_then(email_local_part),
        profile.org_name.as_deref(),
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
        name: &str,
        org_name: Option<&str>,
        user_name: Option<&str>,
        email: Option<&str>,
    ) -> ProfileInfo {
        ProfileInfo {
            name: name.to_string(),
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
    fn resolve_profile_info_prefers_explicit_profile() {
        let profile = resolve_profile_info_from_profiles(
            Some("work"),
            Some("other-org"),
            vec![
                profile_info("other", Some("other-org"), None, None),
                profile_info("work", Some("work-org"), None, None),
            ],
        )
        .expect("profile");

        assert_eq!(profile.name, "work");
    }

    #[test]
    fn resolve_profile_info_finds_profile_by_org_name() {
        let profile = resolve_profile_info_from_profiles(
            None,
            Some("work-org"),
            vec![profile_info("work", Some("work-org"), None, None)],
        )
        .expect("profile");

        assert_eq!(profile.name, "work");
    }

    #[test]
    fn profile_author_slug_prefers_user_name() {
        let profile = profile_info("work", None, Some("Alice Smith"), Some("alice@example.com"));
        assert_eq!(
            profile_author_slug(&profile).as_deref(),
            Some("alice-smith")
        );
    }

    #[test]
    fn profile_author_slug_falls_back_to_email_local_part() {
        let profile = profile_info("work", None, None, Some("alice.dev@example.com"));
        assert_eq!(profile_author_slug(&profile).as_deref(), Some("alice-dev"));
    }

    #[test]
    fn profile_author_slug_ignores_internal_profile_name() {
        let profile = profile_info("Work Profile", None, None, None);
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
