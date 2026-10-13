use std::collections::HashMap;
use std::ffi::OsString;
use std::path::PathBuf;

use anyhow::{Context, Result};

/// Pre-`BRAINTRUST_` names of CLI env vars. Each `BT_<NAME>` is still honored as
/// a silent alias for `BRAINTRUST_<NAME>` until support is removed. Do not add entries: new env vars must use the `BRAINTRUST_` prefix.
///
/// `BT_EVAL_*`, `BT_DATASET_PIPELINE_*`, and `BT_FUNCTIONS_PUSH_EXTERNAL_PACKAGES`
/// are also the internal protocol `bt` uses to configure runner subprocesses (and
/// that SDK runners read). That protocol is unchanged; this list only covers the
/// user-facing names that `bt` itself reads.
pub(crate) const DEPRECATED_BT_ENV_VARS: &[&str] = &[
    "BT_CUSTOM_VIEWS_BOOTSTRAP_DATASET",
    "BT_CUSTOM_VIEWS_BOOTSTRAP_DATASET_ID",
    "BT_CUSTOM_VIEWS_BOOTSTRAP_FILE",
    "BT_CUSTOM_VIEWS_BOOTSTRAP_FORCE",
    "BT_CUSTOM_VIEWS_BOOTSTRAP_NAME",
    "BT_CUSTOM_VIEWS_PREVIEW_DATASET",
    "BT_CUSTOM_VIEWS_PREVIEW_FILE",
    "BT_CUSTOM_VIEWS_PREVIEW_LOOKUP_WINDOW",
    "BT_CUSTOM_VIEWS_PREVIEW_NO_OPEN",
    "BT_CUSTOM_VIEWS_PREVIEW_PORT",
    "BT_CUSTOM_VIEWS_PREVIEW_PROJECT_ID",
    "BT_CUSTOM_VIEWS_PREVIEW_ROW_ID",
    "BT_CUSTOM_VIEWS_PREVIEW_ROW_INDEX",
    "BT_CUSTOM_VIEWS_PREVIEW_SPAN_ID",
    "BT_CUSTOM_VIEWS_PREVIEW_TRACE_ID",
    "BT_CUSTOM_VIEWS_PREVIEW_URL",
    "BT_CUSTOM_VIEWS_PREVIEW_VIEW",
    "BT_CUSTOM_VIEWS_PUSH_FILES",
    "BT_CUSTOM_VIEWS_PUSH_IF_EXISTS",
    "BT_CUSTOM_VIEWS_PUSH_YES",
    "BT_DATASETS_DESCRIPTION",
    "BT_DATASETS_FILE",
    "BT_DATASETS_FORCE",
    "BT_DATASETS_ID_FIELD",
    "BT_DATASETS_ROWS",
    "BT_DATASETS_SNAPSHOT_DELETE_FORCE",
    "BT_DATASETS_SNAPSHOT_DELETE_NAME",
    "BT_DATASETS_SNAPSHOT_DELETE_XACT_ID",
    "BT_DATASETS_SNAPSHOT_DESCRIPTION",
    "BT_DATASETS_SNAPSHOT_RESTORE_FORCE",
    "BT_DATASETS_SNAPSHOT_RESTORE_NAME",
    "BT_DATASETS_SNAPSHOT_RESTORE_XACT_ID",
    "BT_DATASETS_SNAPSHOT_XACT_ID",
    "BT_DATASETS_VERBOSE",
    "BT_DATASETS_VIEW_ALL",
    "BT_DATASETS_VIEW_FULL",
    "BT_DATASETS_VIEW_LIMIT",
    "BT_DATASETS_WEB",
    "BT_DATASET_PIPELINE_PYTHON",
    "BT_DATASET_PIPELINE_RUNNER",
    "BT_DATASET_PIPELINE_WINDOW",
    "BT_EVAL_DEV",
    "BT_EVAL_DEV_ALLOWED_ORIGIN",
    "BT_EVAL_DEV_HOST",
    "BT_EVAL_DEV_ORG_NAME",
    "BT_EVAL_DEV_PORT",
    "BT_EVAL_FILTER",
    "BT_EVAL_FIRST",
    "BT_EVAL_GO",
    "BT_EVAL_GO_BIN",
    "BT_EVAL_JSONL",
    "BT_EVAL_LANGUAGE",
    "BT_EVAL_LIST",
    "BT_EVAL_LOCAL",
    "BT_EVAL_MAX_CONCURRENCY",
    "BT_EVAL_NO_AUTO_INSTRUMENTATION",
    "BT_EVAL_NUM_WORKERS",
    "BT_EVAL_PYTHON",
    "BT_EVAL_PYTHON_RUNNER",
    "BT_EVAL_RUNNER",
    "BT_EVAL_SAMPLE",
    "BT_EVAL_SAMPLE_SEED",
    "BT_EVAL_TERMINATE_ON_FAILURE",
    "BT_EVAL_WATCH",
    "BT_FUNCTIONS_PULL_FORCE",
    "BT_FUNCTIONS_PULL_ID",
    "BT_FUNCTIONS_PULL_LANGUAGE",
    "BT_FUNCTIONS_PULL_OUTPUT_DIR",
    "BT_FUNCTIONS_PULL_PROJECT_ID",
    "BT_FUNCTIONS_PULL_SLUG",
    "BT_FUNCTIONS_PULL_VERSION",
    "BT_FUNCTIONS_PUSH_CREATE_MISSING_PROJECTS",
    "BT_FUNCTIONS_PUSH_EXTERNAL_PACKAGES",
    "BT_FUNCTIONS_PUSH_FILES",
    "BT_FUNCTIONS_PUSH_IF_EXISTS",
    "BT_FUNCTIONS_PUSH_LANGUAGE",
    "BT_FUNCTIONS_PUSH_REQUIREMENTS",
    "BT_FUNCTIONS_PUSH_RUNNER",
    "BT_FUNCTIONS_PUSH_TERMINATE_ON_FAILURE",
    "BT_FUNCTIONS_PUSH_TSCONFIG",
    "BT_FUNCTIONS_VIEW_ENVIRONMENT",
    "BT_FUNCTIONS_VIEW_ID",
    "BT_FUNCTIONS_VIEW_VERSION",
    "BT_OBSERVABILITY_TEMPLATE_PULL_FORCE",
    "BT_OBSERVABILITY_TEMPLATE_PULL_OUTPUT",
    "BT_OBSERVABILITY_TEMPLATE_PUSH_FILE",
    "BT_OBSERVABILITY_TEMPLATE_PUSH_FORCE",
    "BT_OBSERVABILITY_TEMPLATE_PUSH_TOPICS_AUTOMATION",
    "BT_OBSERVABILITY_TEMPLATE_PUSH_YES",
    "BT_SYNC_PUSH_MAX_BATCH_BYTES",
    "BT_SYNC_PUSH_MAX_IN_FLIGHT_BYTES",
    "BT_SYNC_WINDOW",
    "BT_TOPICS_BTMAP_FUNCTION_ID",
    "BT_TOPICS_BTMAP_OUTPUT",
    "BT_TOPICS_BTMAP_VERSION",
    "BT_TOPICS_REPORT_FUNCTION_ID",
    "BT_TOPICS_REPORT_OUTPUT",
    "BT_TOPICS_REPORT_VERSION",
    "BT_TOPICS_STATUS_PROGRESS_WINDOW",
];

/// Loads `--env-file` and maps deprecated env var names, before clap reads the
/// environment.
pub fn bootstrap_from_args(args: &[OsString]) -> Result<()> {
    let explicit_env_file = extract_env_file_arg(args)
        .or_else(|| std::env::var("BRAINTRUST_ENV_FILE").ok().map(PathBuf::from));
    load_env(explicit_env_file.as_ref())?;
    apply_deprecated_env_aliases(DEPRECATED_BT_ENV_VARS);
    Ok(())
}

pub(crate) fn canonical_env_name(deprecated: &str) -> String {
    let name = deprecated.strip_prefix("BT_").unwrap_or(deprecated);
    format!("BRAINTRUST_{name}")
}

// Process-internal plumbing: clap supports a single env name per arg, so
// deprecated names are copied onto their canonical names before parsing. The
// canonical name wins when both are set.
fn apply_deprecated_env_aliases(deprecated_names: &[&str]) {
    for deprecated in deprecated_names {
        let Some(value) = std::env::var_os(deprecated) else {
            continue;
        };
        let canonical = canonical_env_name(deprecated);
        if std::env::var_os(&canonical).is_some() {
            // Drop the ignored value so runner subprocesses can't inherit it.
            std::env::remove_var(deprecated);
        } else {
            std::env::set_var(&canonical, value);
        }
    }
}

pub fn load_env(explicit_env_file: Option<&PathBuf>) -> Result<()> {
    let Some(explicit_env_file) = explicit_env_file else {
        return Ok(());
    };
    let cwd = std::env::current_dir().context("failed to read current directory")?;
    let env_files = resolve_env_files(&cwd, explicit_env_file);
    let mut loaded = HashMap::new();

    for env_file in env_files {
        let parsed = dotenvy::from_path_iter(&env_file)
            .with_context(|| format!("failed to read env file {}", env_file.display()))?;
        for item in parsed {
            let (key, value) =
                item.with_context(|| format!("failed to parse env file {}", env_file.display()))?;
            if std::env::var_os(&key).is_some() {
                continue;
            }
            // Env files are processed from lowest to highest precedence,
            // so later files intentionally override earlier file values.
            loaded.insert(key, value);
        }
    }

    let mut envs: Vec<(String, String)> = loaded.into_iter().collect();
    envs.sort_by(|a, b| a.0.cmp(&b.0));
    for (key, value) in envs {
        std::env::set_var(key, value);
    }
    Ok(())
}

fn extract_env_file_arg(args: &[OsString]) -> Option<PathBuf> {
    let mut explicit = None;
    let mut idx = 1usize;
    while idx < args.len() {
        let Some(arg) = args[idx].to_str() else {
            idx += 1;
            continue;
        };

        if arg == "--" {
            break;
        }

        if arg == "--env-file" {
            if let Some(next) = args.get(idx + 1) {
                explicit = Some(PathBuf::from(next));
            }
            idx += 2;
            continue;
        }

        if let Some(value) = arg.strip_prefix("--env-file=") {
            explicit = Some(PathBuf::from(value));
        }

        idx += 1;
    }
    explicit
}

fn resolve_env_files(cwd: &std::path::Path, explicit_env_file: &PathBuf) -> Vec<PathBuf> {
    let full_path = if explicit_env_file.is_absolute() {
        explicit_env_file.clone()
    } else {
        cwd.join(explicit_env_file)
    };
    vec![full_path]
}

#[cfg(test)]
mod tests {
    use super::*;

    const DEPRECATED: &str = "BT_TEST_ENV_ALIAS_DEPRECATED";
    const CANONICAL: &str = "BRAINTRUST_TEST_ENV_ALIAS_DEPRECATED";
    const BOTH_DEPRECATED: &str = "BT_TEST_ENV_ALIAS_BOTH";
    const BOTH_CANONICAL: &str = "BRAINTRUST_TEST_ENV_ALIAS_BOTH";
    const UNSET_DEPRECATED: &str = "BT_TEST_ENV_ALIAS_UNSET";
    const UNSET_CANONICAL: &str = "BRAINTRUST_TEST_ENV_ALIAS_UNSET";

    #[test]
    fn canonical_env_name_swaps_prefix() {
        assert_eq!(canonical_env_name("BT_EVAL_WATCH"), "BRAINTRUST_EVAL_WATCH");
    }

    #[test]
    fn deprecated_env_aliases_map_onto_canonical_names() {
        std::env::set_var(DEPRECATED, "from-deprecated");
        std::env::remove_var(CANONICAL);
        std::env::set_var(BOTH_DEPRECATED, "from-deprecated");
        std::env::set_var(BOTH_CANONICAL, "from-canonical");
        std::env::remove_var(UNSET_DEPRECATED);
        std::env::remove_var(UNSET_CANONICAL);

        apply_deprecated_env_aliases(&[DEPRECATED, BOTH_DEPRECATED, UNSET_DEPRECATED]);

        let deprecated_value = std::env::var(CANONICAL).ok();
        let both_value = std::env::var(BOTH_CANONICAL).ok();
        let both_deprecated_value = std::env::var_os(BOTH_DEPRECATED);
        let unset_value = std::env::var_os(UNSET_CANONICAL);
        for key in [DEPRECATED, CANONICAL, BOTH_DEPRECATED, BOTH_CANONICAL] {
            std::env::remove_var(key);
        }

        assert_eq!(deprecated_value.as_deref(), Some("from-deprecated"));
        assert_eq!(both_value.as_deref(), Some("from-canonical"));
        assert_eq!(both_deprecated_value, None);
        assert_eq!(unset_value, None);
    }
}
