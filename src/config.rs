use std::collections::HashMap;
use std::env;
use std::fs;
use std::path::PathBuf;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

/// Auth profile containing credentials and optional project
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Profile {
    pub api_url: String,
    pub access_token: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refresh_token: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub expires_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub org_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub project: Option<String>,
}

/// Top-level config structure
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BtConfig {
    #[serde(default)]
    pub profiles: HashMap<String, Profile>,
}

/// Get the bt config directory path (same as receipt directory)
pub fn bt_config_dir() -> Option<PathBuf> {
    #[cfg(windows)]
    {
        env::var_os("APPDATA")
            .map(PathBuf::from)
            .map(|path| path.join("bt"))
    }
    #[cfg(not(windows))]
    {
        if let Some(xdg) = env::var_os("XDG_CONFIG_HOME") {
            return Some(PathBuf::from(xdg).join("bt"));
        }
        env::var_os("HOME")
            .map(PathBuf::from)
            .map(|path| path.join(".config").join("bt"))
    }
}

/// Get the auth config file path
pub fn auth_config_path() -> Option<PathBuf> {
    // Allow override via BT_CONFIG env var
    if let Ok(path) = env::var("BT_CONFIG") {
        return Some(PathBuf::from(path));
    }

    bt_config_dir().map(|dir| dir.join("config.json"))
}

/// Load the config from disk
pub fn load_config() -> Result<BtConfig> {
    let path = auth_config_path().context("failed to resolve config directory")?;

    if !path.exists() {
        return Ok(BtConfig::default());
    }

    let contents = fs::read_to_string(&path)
        .with_context(|| format!("failed to read config from {}", path.display()))?;

    let config: BtConfig = serde_json::from_str(&contents)
        .with_context(|| format!("failed to parse config from {}", path.display()))?;

    Ok(config)
}

/// Get a specific profile from config
pub fn get_profile(name: &str) -> Result<Option<Profile>> {
    let config = load_config()?;
    Ok(config.profiles.get(name).cloned())
}

/// Save a profile to config (updates existing or creates new)
pub fn save_profile(name: &str, profile: Profile) -> Result<()> {
    let path = auth_config_path().context("failed to resolve config directory")?;

    // Create parent directory if it doesn't exist
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create config directory {}", parent.display()))?;
    }

    // Load existing config or create new
    let mut config = load_config().unwrap_or_default();

    // Update profile
    config.profiles.insert(name.to_string(), profile);

    // Write to disk
    let contents = serde_json::to_string_pretty(&config).context("failed to serialize config")?;

    fs::write(&path, contents)
        .with_context(|| format!("failed to write config to {}", path.display()))?;

    Ok(())
}

/// Delete a profile from config
pub fn delete_profile(name: &str) -> Result<bool> {
    let path = auth_config_path().context("failed to resolve config directory")?;

    if !path.exists() {
        return Ok(false);
    }

    let mut config = load_config()?;
    let removed = config.profiles.remove(name).is_some();

    if removed {
        let contents =
            serde_json::to_string_pretty(&config).context("failed to serialize config")?;

        fs::write(&path, contents)
            .with_context(|| format!("failed to write config to {}", path.display()))?;
    }

    Ok(removed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn make_temp_config_path() -> PathBuf {
        let counter = TEST_COUNTER.fetch_add(1, Ordering::SeqCst);
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock before unix epoch")
            .as_nanos();
        let thread_id = std::thread::current().id();
        std::env::temp_dir().join(format!(
            "bt-config-test-{}-{:?}-{}-{}.json",
            std::process::id(),
            thread_id,
            now,
            counter
        ))
    }

    #[test]
    fn config_serialization_roundtrip() {
        let mut config = BtConfig::default();
        config.profiles.insert(
            "test".to_string(),
            Profile {
                api_url: "https://api.braintrust.dev".to_string(),
                access_token: "brt_test123".to_string(),
                refresh_token: Some("refresh_abc".to_string()),
                expires_at: Some(Utc::now()),
                org_name: Some("my-org".to_string()),
                project: Some("my-project".to_string()),
            },
        );

        let json = serde_json::to_string_pretty(&config).unwrap();
        let parsed: BtConfig = serde_json::from_str(&json).unwrap();

        assert_eq!(parsed.profiles.len(), 1);
        assert!(parsed.profiles.contains_key("test"));
    }

    #[test]
    fn profile_optional_fields() {
        let profile = Profile {
            api_url: "https://api.braintrust.dev".to_string(),
            access_token: "brt_test".to_string(),
            refresh_token: None,
            expires_at: None,
            org_name: None,
            project: None,
        };

        let json = serde_json::to_string(&profile).unwrap();
        assert!(!json.contains("refresh_token"));
        assert!(!json.contains("expires_at"));
        assert!(!json.contains("org_name"));
        assert!(!json.contains("project"));
    }

    #[test]
    #[serial]
    fn save_and_load_profile() {
        let config_path = make_temp_config_path();

        // Use a scoped block to ensure env var is set before operations
        {
            env::set_var("BT_CONFIG", &config_path);

            let profile = Profile {
                api_url: "https://api.test.com".to_string(),
                access_token: "test_token".to_string(),
                refresh_token: Some("refresh_token".to_string()),
                expires_at: None,
                org_name: Some("test-org".to_string()),
                project: Some("test-project".to_string()),
            };

            // Save profile
            save_profile("test_profile", profile.clone()).unwrap();

            // Verify file was created
            assert!(config_path.exists(), "Config file should exist after save");

            // Load it back
            let loaded = get_profile("test_profile").unwrap();
            assert!(loaded.is_some(), "Profile should be loaded");
            let loaded = loaded.unwrap();
            assert_eq!(loaded.api_url, profile.api_url);
            assert_eq!(loaded.access_token, profile.access_token);
            assert_eq!(loaded.org_name, profile.org_name);
            assert_eq!(loaded.project, profile.project);
        }

        // Cleanup
        fs::remove_file(&config_path).ok();
        env::remove_var("BT_CONFIG");
    }

    #[test]
    #[serial]
    fn get_nonexistent_profile() {
        let config_path = make_temp_config_path();
        env::set_var("BT_CONFIG", &config_path);

        let result = get_profile("nonexistent").unwrap();
        assert!(result.is_none());

        // Cleanup
        env::remove_var("BT_CONFIG");
    }

    #[test]
    #[serial]
    fn save_multiple_profiles() {
        let config_path = make_temp_config_path();

        {
            env::set_var("BT_CONFIG", &config_path);

            let profile1 = Profile {
                api_url: "https://api1.com".to_string(),
                access_token: "token1".to_string(),
                refresh_token: None,
                expires_at: None,
                org_name: None,
                project: None,
            };

            let profile2 = Profile {
                api_url: "https://api2.com".to_string(),
                access_token: "token2".to_string(),
                refresh_token: None,
                expires_at: None,
                org_name: Some("org2".to_string()),
                project: Some("proj2".to_string()),
            };

            save_profile("profile1", profile1).unwrap();
            save_profile("profile2", profile2).unwrap();

            let config = load_config().unwrap();
            assert_eq!(config.profiles.len(), 2);
            assert!(config.profiles.contains_key("profile1"));
            assert!(config.profiles.contains_key("profile2"));
        }

        // Cleanup
        fs::remove_file(&config_path).ok();
        env::remove_var("BT_CONFIG");
    }

    #[test]
    #[serial]
    fn update_existing_profile() {
        let config_path = make_temp_config_path();

        {
            env::set_var("BT_CONFIG", &config_path);

            let profile_v1 = Profile {
                api_url: "https://api1.com".to_string(),
                access_token: "token1".to_string(),
                refresh_token: None,
                expires_at: None,
                org_name: None,
                project: None,
            };

            save_profile("test", profile_v1).unwrap();

            let profile_v2 = Profile {
                api_url: "https://api2.com".to_string(),
                access_token: "token2".to_string(),
                refresh_token: Some("refresh".to_string()),
                expires_at: None,
                org_name: Some("new-org".to_string()),
                project: Some("new-project".to_string()),
            };

            save_profile("test", profile_v2).unwrap();

            let loaded = get_profile("test").unwrap();
            assert!(loaded.is_some(), "Updated profile should exist");
            let loaded = loaded.unwrap();
            assert_eq!(loaded.api_url, "https://api2.com");
            assert_eq!(loaded.access_token, "token2");
            assert_eq!(loaded.org_name, Some("new-org".to_string()));
        }

        // Cleanup
        fs::remove_file(&config_path).ok();
        env::remove_var("BT_CONFIG");
    }

    #[test]
    #[serial]
    fn delete_existing_profile() {
        let config_path = make_temp_config_path();

        {
            env::set_var("BT_CONFIG", &config_path);

            let profile = Profile {
                api_url: "https://api.com".to_string(),
                access_token: "token".to_string(),
                refresh_token: None,
                expires_at: None,
                org_name: None,
                project: None,
            };

            save_profile("to_delete", profile).unwrap();
            assert!(
                get_profile("to_delete").unwrap().is_some(),
                "Profile should exist after save"
            );

            let deleted = delete_profile("to_delete").unwrap();
            assert!(deleted, "Delete should return true for existing profile");
            assert!(
                get_profile("to_delete").unwrap().is_none(),
                "Profile should be gone after delete"
            );
        }

        // Cleanup
        fs::remove_file(&config_path).ok();
        env::remove_var("BT_CONFIG");
    }

    #[test]
    #[serial]
    fn delete_nonexistent_profile() {
        let config_path = make_temp_config_path();
        env::set_var("BT_CONFIG", &config_path);

        let deleted = delete_profile("nonexistent").unwrap();
        assert!(!deleted);

        // Cleanup
        env::remove_var("BT_CONFIG");
    }

    #[test]
    #[serial]
    fn load_config_when_file_missing() {
        let config_path = make_temp_config_path();
        env::set_var("BT_CONFIG", &config_path);

        let config = load_config().unwrap();
        assert_eq!(config.profiles.len(), 0);

        // Cleanup
        env::remove_var("BT_CONFIG");
    }

    #[test]
    fn oauth2_profile_without_org_name() {
        let profile = Profile {
            api_url: "https://api.braintrust.dev".to_string(),
            access_token: "jwt_token_here".to_string(),
            refresh_token: Some("refresh_here".to_string()),
            expires_at: Some(Utc::now()),
            org_name: None, // OAuth2 tokens don't need org_name
            project: Some("my-project".to_string()),
        };

        let json = serde_json::to_string(&profile).unwrap();
        assert!(!json.contains("org_name"));
        assert!(json.contains("refresh_token"));
        assert!(json.contains("project"));
    }

    #[test]
    fn api_key_profile_with_org_name() {
        let profile = Profile {
            api_url: "https://api.braintrust.dev".to_string(),
            access_token: "brt_apikey".to_string(),
            refresh_token: None, // API keys don't have refresh tokens
            expires_at: None,
            org_name: Some("my-org".to_string()), // API keys need org_name
            project: Some("my-project".to_string()),
        };

        let json = serde_json::to_string(&profile).unwrap();
        assert!(json.contains("org_name"));
        assert!(!json.contains("refresh_token"));
    }
}
