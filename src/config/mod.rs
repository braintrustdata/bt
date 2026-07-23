use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand};
use std::{
    env, fs,
    io::{self, Write as _},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::args::BaseArgs;
use crate::ui::{print_command_status, CommandStatus};

mod get;
mod list;
mod set;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub org: Option<String>,
    pub project: Option<String>,
    pub project_id: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

pub const KNOWN_KEYS: &[&str] = &["org", "project", "project_id"];

impl Config {
    pub fn get_field(&self, key: &str) -> Option<&str> {
        match key {
            "org" => self.org.as_deref(),
            "project" => self.project.as_deref(),
            "project_id" => self.project_id.as_deref(),
            _ => None,
        }
    }

    pub fn set_field(&mut self, key: &str, value: String) -> bool {
        match key {
            "org" => self.org = Some(value),
            "project" => {
                self.project = Some(value);
                self.project_id = None;
            }
            "project_id" => self.project_id = Some(value),
            _ => return false,
        }
        true
    }

    pub fn unset_field(&mut self, key: &str) -> bool {
        match key {
            "org" => self.org = None,
            "project" => {
                self.project = None;
                self.project_id = None;
            }
            "project_id" => self.project_id = None,
            _ => return false,
        }
        true
    }

    pub fn non_empty_fields(&self) -> Vec<(&str, &str)> {
        KNOWN_KEYS
            .iter()
            .filter_map(|&key| self.get_field(key).map(|v| (key, v)))
            .collect()
    }

    pub(crate) fn set_context(&mut self, org: Option<&str>, project: Option<(&str, &str)>) {
        self.org = org_option(org).map(str::to_string);
        (self.project, self.project_id) = project
            .map(|(name, id)| (name.to_string(), id.to_string()))
            .unzip();
    }

    pub(crate) fn merge(&self, local: &Config) -> Config {
        let mut extra = self.extra.clone();
        extra.extend(local.extra.clone());
        let global_id = self.project.as_ref().and(self.project_id.clone());
        let (org, project, project_id) = match (&local.org, &local.project) {
            (Some(org), Some(project)) => (
                Some(org.clone()),
                Some(project.clone()),
                local.project_id.clone(),
            ),
            (Some(org), None) if self.org.as_ref() == Some(org) => {
                (Some(org.clone()), self.project.clone(), global_id)
            }
            (Some(org), None) => (Some(org.clone()), None, None),
            (None, Some(project)) => (None, Some(project.clone()), local.project_id.clone()),
            (None, None) => (self.org.clone(), self.project.clone(), global_id),
        };
        Config {
            org,
            project,
            project_id,
            extra,
        }
    }
}

pub fn global_config_dir() -> Result<PathBuf> {
    if let Some(xdg) = env::var_os("XDG_CONFIG_HOME") {
        return Ok(PathBuf::from(xdg).join("bt"));
    }
    dirs::home_dir()
        .map(|path| path.join(".config").join("bt"))
        .ok_or_else(|| anyhow!("$HOME not configured."))
}

pub fn global_path() -> Result<PathBuf> {
    Ok(global_config_dir()?.join("config.json"))
}

pub fn load_file(path: &Path) -> Config {
    let file_contents = match fs::read_to_string(path) {
        Ok(c) => c,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Config::default(),
        Err(e) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Warning: could not read {}: {e}", path.display()),
            );
            return Config::default();
        }
    };

    let mut config: Config = match serde_json::from_str(&file_contents) {
        Ok(c) => c,
        Err(e) => {
            print_command_status(
                CommandStatus::Error,
                &format!("Warning: could not read {}: {e}", path.display()),
            );
            return Config::default();
        }
    };

    config.extra.remove("profile");

    // Fold a literal "cross-org" to the canonical "" marker on load.
    if let Some(org) = config.org.as_deref() {
        let normalized = normalize_org(org);
        if normalized != org {
            config.org = Some(normalized.to_string());
        }
    }

    for key in config.extra.keys() {
        print_command_status(
            CommandStatus::Error,
            &format!("Warning: unknown config key {} in {}", key, path.display()),
        );
    }

    config
}

pub fn load_global() -> Result<Config> {
    Ok(load_file(&global_path()?))
}

pub fn load() -> Result<Config> {
    let global = load_global().unwrap_or_default();
    let local = match local_path() {
        Some(p) => load_file(&p),
        None => Config::default(),
    };
    Ok(global.merge(&local))
}

pub fn configured_project_for_context(
    base: &BaseArgs,
    resolved_org: Option<&str>,
) -> Option<String> {
    load()
        .ok()
        .and_then(|cfg| project_from_config_for_context(base, &cfg, resolved_org))
}

pub fn configured_project_id_for_base(base: &BaseArgs) -> Option<String> {
    load().ok().and_then(|cfg| {
        config_matches_context(base, &cfg, None)
            .then(|| trimmed_option(cfg.project_id.as_deref()).map(str::to_string))
            .flatten()
    })
}

pub(crate) fn project_from_config_for_context(
    base: &BaseArgs,
    cfg: &Config,
    resolved_org: Option<&str>,
) -> Option<String> {
    config_matches_context(base, cfg, resolved_org)
        .then(|| trimmed_option(cfg.project.as_deref()).map(str::to_string))
        .flatten()
}

fn config_matches_context(base: &BaseArgs, cfg: &Config, resolved_org: Option<&str>) -> bool {
    let cfg_org = org_option(cfg.org.as_deref());
    let requested_org = org_option(resolved_org).or_else(|| org_option(base.org_name.as_deref()));

    requested_org.is_none_or(|resolved| cfg_org == Some(resolved))
}

/// Trim an org while preserving the empty cross-org marker.
pub(crate) fn org_option(value: Option<&str>) -> Option<&str> {
    value.map(str::trim)
}

/// Human-facing spelling of the empty cross-org marker.
pub(crate) const CROSS_ORG_ALIAS: &str = "cross-org";

/// Trim an org and fold the [`CROSS_ORG_ALIAS`] to the canonical `""` marker.
pub(crate) fn normalize_org(value: &str) -> &str {
    let trimmed = value.trim();
    if trimmed == CROSS_ORG_ALIAS {
        ""
    } else {
        trimmed
    }
}

pub(crate) fn display_org(org: &str) -> &str {
    if org.is_empty() {
        CROSS_ORG_ALIAS
    } else {
        org
    }
}

pub(crate) fn trimmed_option(value: Option<&str>) -> Option<&str> {
    value.map(str::trim).filter(|value| !value.is_empty())
}

pub fn save_file(path: &Path, config: &Config) -> Result<()> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    fs::create_dir_all(parent)?;

    let json = serde_json::to_string_pretty(config)?;
    let mut file = tempfile::NamedTempFile::new_in(parent)?;
    file.write_all(json.as_bytes())?;
    file.write_all(b"\n")?;
    file.as_file().sync_all()?;
    file.persist(path)?;

    Ok(())
}

pub fn save_global(config: &Config) -> Result<()> {
    save_file(&global_path()?, config)
}

pub fn find_local_config_dir() -> Option<PathBuf> {
    find_local_config_dir_from(std::env::current_dir().ok()?, dirs::home_dir().as_deref())
}

enum ProjectBoundary {
    Bt(PathBuf),
    Git(PathBuf),
    Home,
    Root,
}

fn project_boundary(start: PathBuf, home: Option<&Path>) -> ProjectBoundary {
    // `current_dir()` is the physical path (symlinks resolved) while `$HOME` may
    // not be, so also compare canonicalized forms — exact equality alone can
    // walk straight past a symlinked home boundary.
    let home_canon = home.and_then(|h| fs::canonicalize(h).ok());
    for dir in start.ancestors() {
        let at_home =
            Some(dir) == home || (home_canon.is_some() && fs::canonicalize(dir).ok() == home_canon);
        if at_home {
            return ProjectBoundary::Home;
        }
        if dir.parent().is_none() {
            return ProjectBoundary::Root;
        }
        let bt = dir.join(".bt");
        if bt.is_dir() {
            return ProjectBoundary::Bt(bt);
        }
        if dir.join(".git").exists() {
            return ProjectBoundary::Git(dir.to_path_buf());
        }
    }
    unreachable!("path ancestors always include a filesystem root")
}

fn find_local_config_dir_from(current_dir: PathBuf, home: Option<&Path>) -> Option<PathBuf> {
    match project_boundary(current_dir, home) {
        ProjectBoundary::Bt(dir) if dir.join("config.json").is_file() => Some(dir),
        _ => None,
    }
}

pub fn local_path() -> Option<PathBuf> {
    find_local_config_dir().map(|dir| dir.join("config.json"))
}

/// Resolve which config file to write based on --global/--local flags.
pub fn resolve_write_path(global: bool, local: bool) -> Result<PathBuf> {
    if global {
        return global_path();
    }
    match local_path() {
        Some(path) => Ok(path),
        None if local => {
            bail!("No existing local .bt/config.json found. Run `bt init` first, or use --global.")
        }
        None => global_path(),
    }
}

/// Resolve the create/overwrite target for `bt init`.
pub fn init_target(here: bool, force: bool) -> Result<PathBuf> {
    init_target_from(
        std::env::current_dir().context("could not read current directory")?,
        dirs::home_dir().as_deref(),
        here,
        force,
    )
}

fn init_target_from(
    current_dir: PathBuf,
    home: Option<&Path>,
    here: bool,
    force: bool,
) -> Result<PathBuf> {
    if here {
        let path = current_dir.join(".bt/config.json");
        if path.exists() && !force {
            bail!(
                "{} already exists; rerun with --force to overwrite it",
                path.display()
            );
        }
        return Ok(path);
    }

    let path = match project_boundary(current_dir, home) {
        ProjectBoundary::Home => bail!(
            "reached the home directory without finding a project git root; run `bt init` inside a repository, or pass --here"
        ),
        ProjectBoundary::Root => bail!(
            "reached the filesystem root without finding a project git root; run `bt init` inside a repository, or pass --here"
        ),
        ProjectBoundary::Git(dir) => return Ok(dir.join(".bt/config.json")),
        ProjectBoundary::Bt(dir) => dir.join("config.json"),
    };
    if !path.is_file() {
        bail!(
            "found {} without config.json; remove the incomplete .bt directory, then rerun `bt init`",
            path.parent().unwrap_or(&path).display()
        );
    }
    if !force {
        bail!(
            "{} already exists; use `bt switch` to change it, or rerun with --force to overwrite it",
            path.display()
        );
    }
    Ok(path)
}

pub fn local_save_path() -> Result<PathBuf> {
    Ok(std::env::current_dir()?.join(".bt").join("config.json"))
}

pub fn save_local(config: &Config, create_dir: bool) -> Result<PathBuf> {
    let path = local_save_path()?;
    let dir = path.parent().expect(".bt parent directory");
    if create_dir && !dir.exists() {
        fs::create_dir_all(dir)?;
    }
    save_file(&path, config)?;
    Ok(path)
}

// --- CLI commands ---

#[derive(Debug, Clone, Default, Args)]
pub struct ScopeArgs {
    /// Use global config (~/.config/bt/config.json)
    #[arg(long, short = 'g', conflicts_with = "local")]
    pub(crate) global: bool,

    /// Use local config (.bt/config.json)
    #[arg(long, short = 'l')]
    pub(crate) local: bool,
}

fn scope_labels(global: &Path, local: &Path) -> [String; 2] {
    [
        format!("Global ({})", global.parent().unwrap_or(global).display()),
        format!("Local ({})", local.parent().unwrap_or(local).display()),
    ]
}

type ResolvedScope = (PathBuf, &'static str);

impl ScopeArgs {
    pub(crate) fn preflight(&self, can_prompt: bool) -> Result<()> {
        (!can_prompt)
            .then(|| self.resolve(false, ""))
            .transpose()
            .map(drop)
    }

    pub(crate) fn resolve(&self, can_prompt: bool, prompt: &str) -> Result<ResolvedScope> {
        if self.global || self.local {
            let scope = if self.global { "global" } else { "local" };
            return resolve_write_path(self.global, self.local).map(|path| (path, scope));
        }
        let Some(local) = local_path() else {
            return Ok((global_path()?, "global"));
        };
        if !can_prompt {
            bail!("both global and local config scopes are available; pass --global or --local");
        }
        let global = global_path()?;
        let options = scope_labels(&global, &local);
        Ok(if crate::ui::fuzzy_select(prompt, &options, 1)? == 0 {
            (global, "global")
        } else {
            (local, "local")
        })
    }
}

#[derive(Debug, Clone, Args)]
pub struct ConfigArgs {
    #[command(subcommand)]
    command: Option<ConfigCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum ConfigCommands {
    /// List config values
    List {
        #[command(flatten)]
        scope: ScopeArgs,
        /// Show config values grouped by source
        #[arg(long)]
        verbose: bool,
    },
    /// Get a config value
    Get {
        /// Config key (org, project, project_id)
        key: String,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Set a config value
    Set {
        /// Config key (org, project, project_id)
        key: String,
        /// Value to set
        value: String,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Remove a config value
    Unset {
        /// Config key (org, project, project_id)
        key: String,
        #[command(flatten)]
        scope: ScopeArgs,
    },
}

fn validate_key(key: &str) -> Result<()> {
    if !KNOWN_KEYS.contains(&key) {
        bail!(
            "Unknown config key: {key}\nValid keys: {}",
            KNOWN_KEYS.join(", ")
        );
    }
    Ok(())
}

pub fn run(base: BaseArgs, args: ConfigArgs) -> Result<()> {
    match args.command {
        None => list::run(base, false, false, false),
        Some(ConfigCommands::List { scope, verbose }) => {
            list::run(base, scope.global, scope.local, verbose)
        }
        Some(ConfigCommands::Get { key, scope }) => {
            validate_key(&key)?;
            get::run(base, &key, scope.global, scope.local)
        }
        Some(ConfigCommands::Set { key, value, scope }) => {
            validate_key(&key)?;
            set::run(&key, &value, scope.global, scope.local)
        }
        Some(ConfigCommands::Unset { key, scope }) => {
            validate_key(&key)?;
            set::unset(&key, scope.global, scope.local)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn merge_keeps_org_and_project_contexts_together() {
        let c = |org: Option<&str>, project: Option<&str>, id: Option<&str>| Config {
            org: org.map(str::to_string),
            project: project.map(str::to_string),
            project_id: id.map(str::to_string),
            ..Default::default()
        };
        let g = || c(Some("global"), Some("global-proj"), Some("proj_g"));
        let cases = [
            (Config::default(), Config::default(), Config::default()),
            (
                g(),
                c(Some("other"), Some("other-proj"), None),
                c(Some("other"), Some("other-proj"), None),
            ),
            (
                c(Some("base"), None, None),
                c(None, Some("local"), None),
                c(None, Some("local"), None),
            ),
            (g(), c(Some("global"), None, None), g()),
            (
                g(),
                c(Some("local"), None, None),
                c(Some("local"), None, None),
            ),
            (
                g(),
                c(None, Some("local"), Some("proj_l")),
                c(None, Some("local"), Some("proj_l")),
            ),
            (g(), c(Some(""), None, None), c(Some(""), None, None)),
            (g(), Config::default(), g()),
        ];
        for (global, local, expected) in cases {
            assert_eq!(global.merge(&local), expected);
        }
    }

    #[test]
    fn scope_labels_are_plain_text() {
        let labels = scope_labels(
            Path::new("/home/test-user/.config/bt/config.json"),
            Path::new("/work/test-project/.bt/config.json"),
        );
        assert_eq!(labels[1], "Local (/work/test-project/.bt)");
        assert!(labels.iter().all(|label| !label.contains('\u{1b}')));
    }

    #[test]
    fn option_helpers_handle_empty_values() {
        for (input, org, trimmed) in [
            (None, None, None),
            (Some(""), Some(""), None),
            (Some("   "), Some(""), None),
            (Some("test-org"), Some("test-org"), Some("test-org")),
        ] {
            assert_eq!(org_option(input), org);
            assert_eq!(trimmed_option(input), trimmed);
        }

        let mut cfg = Config::default();
        cfg.set_context(Some(" test-org "), Some(("test-project", "proj_test")));
        assert_eq!(cfg.org.as_deref(), Some("test-org"));
        assert_eq!(cfg.project.as_deref(), Some("test-project"));
        assert_eq!(cfg.project_id.as_deref(), Some("proj_test"));
        cfg.set_context(Some(""), None);
        assert_eq!((cfg.org.as_deref(), cfg.project), (Some(""), None));
    }

    fn base_args() -> BaseArgs {
        BaseArgs::default()
    }

    fn config(org: Option<&str>, project: Option<&str>) -> Config {
        Config {
            org: org.map(str::to_string),
            project: project.map(str::to_string),
            ..Default::default()
        }
    }

    #[test]
    fn project_config_must_match_org_context() {
        let base = base_args();
        for (config_org, resolved_org, expected) in [
            (Some("test-org"), "test-org", Some("test-project")),
            (Some("other-org"), "test-org", None),
            (None, "test-org", None),
            (Some(""), "test-org", None),
            (Some(""), "", Some("test-project")),
        ] {
            let cfg = config(config_org, Some("test-project"));
            assert_eq!(
                project_from_config_for_context(&base, &cfg, Some(resolved_org)).as_deref(),
                expected
            );
        }
    }

    #[test]
    fn load_missing_file_returns_default() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nonexistent.json");
        let config = load_file(&path);
        assert_eq!(config.org, None);
        assert_eq!(config.project, None);
    }

    #[test]
    fn load_invalid_json_returns_default() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("invalid.json");
        fs::write(&path, "not valid json {{{").unwrap();
        let config = load_file(&path);
        assert_eq!(config.org, None);
    }

    #[test]
    fn save_load_roundtrip() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("config.json");

        let original = Config {
            org: Some("test-org".into()),
            project: Some("test-project".into()),
            ..Default::default()
        };

        save_file(&path, &original).unwrap();
        let loaded = load_file(&path);

        assert_eq!(loaded.org, original.org);
        assert_eq!(loaded.project, original.project);
    }

    #[test]
    fn load_unknown_keys_still_returns_config() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("config.json");
        fs::write(
            &path,
            r#"{"org": "my-org", "unknown_field": "value", "another": 123}"#,
        )
        .unwrap();

        let config = load_file(&path);
        assert_eq!(config.org, Some("my-org".into()));
        assert!(config.extra.contains_key("unknown_field"));
        assert!(config.extra.contains_key("another"));
    }

    #[test]
    fn legacy_profile_key_is_ignored_and_not_persisted() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("config.json");
        fs::write(&path, r#"{"org":"test-org","profile":"legacy-login"}"#).unwrap();

        let config = load_file(&path);
        assert_eq!(config.org.as_deref(), Some("test-org"));
        assert!(!config.extra.contains_key("profile"));

        save_file(&path, &config).unwrap();
        let persisted = fs::read_to_string(&path).unwrap();
        assert!(!persisted.contains("profile"));
    }

    #[test]
    fn load_folds_cross_org_alias_to_empty_marker() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("config.json");
        // Literal "cross-org" must load identically to the "" marker.
        for spelling in [
            r#"{"org":"cross-org"}"#,
            r#"{"org":"  cross-org  "}"#,
            r#"{"org":""}"#,
        ] {
            fs::write(&path, spelling).unwrap();
            assert_eq!(load_file(&path).org.as_deref(), Some(""), "{spelling}");
        }

        fs::write(&path, r#"{"org":"test-org"}"#).unwrap();
        assert_eq!(load_file(&path).org.as_deref(), Some("test-org"));
    }

    #[test]
    fn unknown_keys_roundtrip_through_save() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("config.json");
        fs::write(
            &path,
            r#"{"org": "my-org", "unknown_field": "value", "another": 123}"#,
        )
        .unwrap();

        let config = load_file(&path);
        save_file(&path, &config).unwrap();
        let reloaded = load_file(&path);

        assert_eq!(reloaded.org, Some("my-org".into()));
        assert!(reloaded.extra.contains_key("unknown_field"));
        assert!(reloaded.extra.contains_key("another"));
    }

    #[test]
    fn save_creates_parent_dirs() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("nested").join("dir").join("config.json");

        let config = Config {
            org: Some("test".into()),
            ..Default::default()
        };

        save_file(&path, &config).unwrap();
        assert!(path.exists());
    }

    #[test]
    fn local_discovery_requires_config_json_and_stops_at_first_bt() {
        let tmp = TempDir::new().unwrap();
        let repo = tmp.path().join("repo");
        let nested = repo.join("a").join("b");
        fs::create_dir_all(&nested).unwrap();
        fs::create_dir(repo.join(".git")).unwrap();
        fs::create_dir(repo.join(".bt")).unwrap();

        assert_eq!(find_local_config_dir_from(nested.clone(), None), None);

        fs::write(repo.join(".bt/config.json"), "{}").unwrap();
        assert_eq!(
            find_local_config_dir_from(nested, None),
            Some(repo.join(".bt"))
        );
    }

    #[test]
    fn local_discovery_does_not_use_home_bt() {
        let tmp = TempDir::new().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(home.join(".bt")).unwrap();
        fs::write(home.join(".bt/config.json"), "{}").unwrap();

        assert_eq!(
            find_local_config_dir_from(home.clone(), Some(home.as_path())),
            None
        );
    }

    #[test]
    fn init_target_finds_nested_git_directory_or_file() {
        for git_is_file in [false, true] {
            let tmp = TempDir::new().unwrap();
            let repo = tmp.path().join("repo");
            let nested = repo.join("nested").join("deeper");
            fs::create_dir_all(&nested).unwrap();
            if git_is_file {
                fs::write(repo.join(".git"), "gitdir: synthetic").unwrap();
            } else {
                fs::create_dir(repo.join(".git")).unwrap();
            }

            assert_eq!(
                init_target_from(nested, Some(tmp.path()), false, false).unwrap(),
                repo.join(".bt/config.json")
            );
        }
    }

    #[test]
    fn init_target_existing_bt_requires_force_and_existing_config() {
        let tmp = TempDir::new().unwrap();
        let repo = tmp.path().join("repo");
        let nested = repo.join("nested");
        fs::create_dir_all(repo.join(".bt")).unwrap();
        fs::create_dir_all(&nested).unwrap();

        assert!(init_target_from(nested.clone(), Some(tmp.path()), false, true).is_err());

        let target = repo.join(".bt/config.json");
        fs::write(&target, "{}").unwrap();
        assert!(init_target_from(nested.clone(), Some(tmp.path()), false, false).is_err());
        assert_eq!(
            init_target_from(nested, Some(tmp.path()), false, true).unwrap(),
            target
        );
    }

    #[test]
    fn init_target_here_bypasses_home_boundary_and_honors_force() {
        let tmp = TempDir::new().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(&home).unwrap();
        let target = home.join(".bt/config.json");

        assert_eq!(
            init_target_from(home.clone(), Some(home.as_path()), true, false).unwrap(),
            target
        );
        fs::create_dir_all(target.parent().unwrap()).unwrap();
        fs::write(&target, "{}").unwrap();
        assert!(init_target_from(home.clone(), Some(home.as_path()), true, false).is_err());
        assert_eq!(
            init_target_from(home, Some(tmp.path()), true, true).unwrap(),
            target
        );
    }

    #[test]
    fn init_target_home_wins_over_git_marker() {
        let tmp = TempDir::new().unwrap();
        let home = tmp.path().join("home");
        fs::create_dir_all(home.join(".git")).unwrap();
        assert!(init_target_from(home.clone(), Some(home.as_path()), false, false).is_err());
    }

    #[cfg(unix)]
    #[test]
    fn init_target_here_bypasses_filesystem_root_boundary() {
        let root = PathBuf::from("/");
        assert_eq!(
            init_target_from(root.clone(), None, true, true).unwrap(),
            root.join(".bt/config.json")
        );
        assert!(init_target_from(root, None, false, false).is_err());
    }
}
