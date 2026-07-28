use anyhow::{anyhow, bail, Context, Result};
use clap::{Args, Subcommand};
use std::{
    env, fs,
    io::{self, Write as _},
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};

use crate::args::{BaseArgs, DEFAULT_APP_URL};
use crate::ui::{print_command_status, CommandStatus};

mod get;
mod list;
mod set;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Config {
    pub org: Option<String>,
    pub org_id: Option<String>,
    pub project: Option<String>,
    pub project_id: Option<String>,
    pub app_url: Option<String>,
    pub api_url: Option<String>,
    #[serde(flatten)]
    pub extra: serde_json::Map<String, serde_json::Value>,
}

pub const KNOWN_KEYS: &[&str] = &[
    "org",
    "org_id",
    "project",
    "project_id",
    "app_url",
    "api_url",
];

impl Config {
    pub fn get_field(&self, key: &str) -> Option<&str> {
        match key {
            "org" => self.org.as_deref(),
            "org_id" => self.org_id.as_deref(),
            "project" => self.project.as_deref(),
            "project_id" => self.project_id.as_deref(),
            "app_url" => self.app_url.as_deref(),
            "api_url" => self.api_url.as_deref(),
            _ => None,
        }
    }

    pub fn set_field(&mut self, key: &str, value: String) -> bool {
        match key {
            "org" => {
                let value = value.trim().to_string();
                if self.org.as_ref() != Some(&value) {
                    self.org_id = None;
                    self.project = None;
                    self.project_id = None;
                }
                self.org = (!value.is_empty()).then_some(value);
            }
            "org_id" => self.org_id = self.org.as_ref().map(|_| value),
            "project" => {
                self.project = Some(value);
                self.project_id = None;
            }
            "project_id" => self.project_id = Some(value),
            "app_url" => {
                let value = value.trim().to_string();
                let previous = self.app_url.as_deref().unwrap_or(DEFAULT_APP_URL);
                let next = if !value.is_empty() {
                    value.as_str()
                } else {
                    DEFAULT_APP_URL
                };
                if !urls_equal(previous, next) {
                    self.org = None;
                    self.org_id = None;
                    self.project = None;
                    self.project_id = None;
                }
                self.app_url = (!value.is_empty()).then_some(value);
            }
            "api_url" => {
                self.api_url = trimmed_option(Some(&value)).map(str::to_string);
            }
            _ => return false,
        }
        true
    }

    pub fn unset_field(&mut self, key: &str) -> bool {
        match key {
            "org" => {
                self.org = None;
                self.org_id = None;
                self.project = None;
                self.project_id = None;
            }
            "org_id" => self.org_id = None,
            "project" => {
                self.project = None;
                self.project_id = None;
            }
            "project_id" => self.project_id = None,
            "app_url" => {
                if self
                    .app_url
                    .as_deref()
                    .is_some_and(|url| !urls_equal(url, DEFAULT_APP_URL))
                {
                    self.org = None;
                    self.org_id = None;
                    self.project = None;
                    self.project_id = None;
                }
                self.app_url = None;
            }
            "api_url" => self.api_url = None,
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

    pub(crate) fn set_context(
        &mut self,
        org: (&str, &str),
        project: Option<(&str, &str)>,
        app_url: &str,
        api_url: &str,
    ) {
        self.org = Some(org.0.trim().to_string());
        self.org_id = Some(org.1.trim().to_string());
        (self.project, self.project_id) = project
            .map(|(name, id)| (name.to_string(), id.to_string()))
            .unzip();
        self.app_url = Some(app_url.to_string());
        self.api_url = Some(api_url.to_string());
    }

    pub(crate) fn merge(&self, local: &Config) -> Config {
        let mut extra = self.extra.clone();
        extra.extend(local.extra.clone());

        let app_url = local.app_url.clone().or_else(|| self.app_url.clone());
        let api_url = local.api_url.clone().or_else(|| self.api_url.clone());
        let global_app = self.app_url.as_deref().unwrap_or(DEFAULT_APP_URL);
        let merged_app = app_url.as_deref().unwrap_or(DEFAULT_APP_URL);
        let same_instance = urls_equal(global_app, merged_app);
        let same_org = same_instance && local.org == self.org;
        let global_project_id = self.project.as_ref().and(self.project_id.clone());

        let (org, org_id, project, project_id) = match (&local.org, &local.project) {
            (Some(org), Some(project)) => (
                Some(org.clone()),
                local.org_id.clone(),
                Some(project.clone()),
                local.project_id.clone(),
            ),
            (Some(org), None) if same_org => (
                Some(org.clone()),
                local.org_id.clone().or_else(|| self.org_id.clone()),
                self.project.clone(),
                global_project_id,
            ),
            (Some(org), None) => (Some(org.clone()), local.org_id.clone(), None, None),
            (None, Some(project)) => (None, None, Some(project.clone()), local.project_id.clone()),
            (None, None) if same_instance => (
                self.org.clone(),
                self.org_id.clone(),
                self.project.clone(),
                global_project_id,
            ),
            (None, None) => (None, None, None, None),
        };
        Config {
            org,
            org_id,
            project,
            project_id,
            app_url,
            api_url,
            extra,
        }
    }
}

pub(crate) fn urls_equal(left: &str, right: &str) -> bool {
    left.trim().trim_end_matches('/') == right.trim().trim_end_matches('/')
}

/// Apply config-file URL and org-ID fallbacks after clap has resolved CLI/env.
pub fn apply_base_config(base: &mut BaseArgs) {
    let cfg = load().unwrap_or_default();
    apply_config_to_base(base, &cfg);
}

fn apply_config_to_base(base: &mut BaseArgs, cfg: &Config) {
    if base.app_url.is_none() {
        base.app_url = cfg.app_url.clone();
    }

    if base.api_url.is_none() {
        base.api_url = cfg.api_url.clone();
    }

    let effective_app = base.app_url.as_deref().unwrap_or(DEFAULT_APP_URL);
    let config_app = cfg.app_url.as_deref().unwrap_or(DEFAULT_APP_URL);
    let same_instance = urls_equal(effective_app, config_app);
    if base.org_name_source.is_none() {
        if base.org_name.is_none() && same_instance {
            base.org_name = cfg.org.clone();
        }
        if same_instance && base.org_name == cfg.org {
            base.org_id = cfg.org_id.clone();
        } else {
            base.org_id = None;
        }
    } else {
        base.org_id = None;
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

    config.org = trimmed_option(config.org.as_deref()).map(str::to_string);
    config.org_id = config
        .org
        .as_ref()
        .and(trimmed_option(config.org_id.as_deref()).map(str::to_string));
    if config.org.is_none() {
        config.project = None;
        config.project_id = None;
    }
    config.app_url = trimmed_option(config.app_url.as_deref()).map(str::to_string);
    config.api_url = trimmed_option(config.api_url.as_deref()).map(str::to_string);

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
    let requested_app = base.app_url.as_deref().unwrap_or(DEFAULT_APP_URL);
    let cfg_app = cfg.app_url.as_deref().unwrap_or(DEFAULT_APP_URL);
    if !urls_equal(requested_app, cfg_app) {
        return false;
    }

    let cfg_org = org_option(cfg.org.as_deref());
    let requested_org = org_option(resolved_org).or_else(|| org_option(base.org_name.as_deref()));

    requested_org.is_none_or(|resolved| cfg_org == Some(resolved))
}

pub(crate) fn org_option(value: Option<&str>) -> Option<&str> {
    trimmed_option(value)
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
        /// Config key (org, org_id, project, project_id, app_url, api_url)
        key: String,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Set a config value
    Set {
        /// Config key (org, org_id, project, project_id, app_url, api_url)
        key: String,
        /// Value to set
        value: String,
        #[command(flatten)]
        scope: ScopeArgs,
    },
    /// Remove a config value
    Unset {
        /// Config key (org, org_id, project, project_id, app_url, api_url)
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
    fn merge_inherits_context_only_within_the_same_instance() {
        let global = Config {
            org: Some("test-org".into()),
            org_id: Some("org_test".into()),
            project: Some("test-project".into()),
            project_id: Some("proj_test".into()),
            app_url: Some("https://www.example.test".into()),
            api_url: Some("https://api.example.test".into()),
            ..Default::default()
        };
        let same_instance = Config {
            app_url: Some("https://www.example.test/".into()),
            api_url: Some("https://proxy.example.test".into()),
            ..Default::default()
        };
        let merged = global.merge(&same_instance);
        assert_eq!(merged.org.as_deref(), Some("test-org"));
        assert_eq!(merged.org_id.as_deref(), Some("org_test"));
        assert_eq!(merged.project_id.as_deref(), Some("proj_test"));
        assert_eq!(
            merged.api_url.as_deref(),
            Some("https://proxy.example.test")
        );

        let other_instance = Config {
            app_url: Some("https://self-hosted.example.test".into()),
            ..Default::default()
        };
        let merged = global.merge(&other_instance);
        assert_eq!(merged.org, None);
        assert_eq!(merged.org_id, None);
        assert_eq!(merged.project, None);
        assert_eq!(merged.app_url, other_instance.app_url);
    }

    #[test]
    fn config_fills_urls_and_coupled_org_id_without_overriding_cli() {
        let cfg = Config {
            org: Some("config-org".into()),
            org_id: Some("org_config".into()),
            app_url: Some("https://www.example.test".into()),
            api_url: Some("https://api.example.test".into()),
            ..Default::default()
        };
        let mut base = BaseArgs::default();
        apply_config_to_base(&mut base, &cfg);
        assert_eq!(base.org_name.as_deref(), Some("config-org"));
        assert_eq!(base.org_id.as_deref(), Some("org_config"));
        assert_eq!(base.app_url, cfg.app_url);
        assert_eq!(base.api_url, cfg.api_url);

        let mut base = BaseArgs {
            org_name: Some("cli-org".into()),
            org_name_source: Some(crate::args::ArgValueSource::CommandLine),
            app_url: Some("https://cli.example.test".into()),
            ..Default::default()
        };
        apply_config_to_base(&mut base, &cfg);
        assert_eq!(base.org_name.as_deref(), Some("cli-org"));
        assert_eq!(base.org_id, None);
        assert_eq!(base.app_url.as_deref(), Some("https://cli.example.test"));
        assert_eq!(base.api_url, cfg.api_url);

        let mut same_instance = BaseArgs {
            app_url: Some("https://www.example.test/".into()),
            ..Default::default()
        };
        apply_config_to_base(&mut same_instance, &cfg);
        assert_eq!(same_instance.api_url, cfg.api_url);

        let mut other_instance = BaseArgs {
            app_url: Some("https://other.example.test".into()),
            ..Default::default()
        };
        apply_config_to_base(&mut other_instance, &cfg);
        assert_eq!(other_instance.org_name, None);
        assert_eq!(other_instance.org_id, None);
    }

    #[test]
    fn configured_project_does_not_cross_instance_boundaries() {
        let cfg = Config {
            org: Some("test-org".into()),
            project: Some("test-project".into()),
            app_url: Some("https://www.example.test".into()),
            ..Default::default()
        };
        let matching = BaseArgs {
            org_name: Some("test-org".into()),
            app_url: Some("https://www.example.test/".into()),
            ..Default::default()
        };
        assert_eq!(
            project_from_config_for_context(&matching, &cfg, Some("test-org")).as_deref(),
            Some("test-project")
        );

        let other = BaseArgs {
            app_url: Some("https://other.example.test".into()),
            ..matching
        };
        assert_eq!(
            project_from_config_for_context(&other, &cfg, Some("test-org")),
            None
        );
    }

    #[test]
    fn changing_app_url_clears_coupled_context() {
        let mut cfg = Config {
            org: Some("test-org".into()),
            org_id: Some("org_test".into()),
            project: Some("test-project".into()),
            project_id: Some("proj_test".into()),
            app_url: Some("https://www.example.test".into()),
            ..Default::default()
        };
        assert!(cfg.set_field("app_url", "https://other.example.test".into()));
        assert_eq!(cfg.org, None);
        assert_eq!(cfg.org_id, None);
        assert_eq!(cfg.project, None);
        assert_eq!(cfg.project_id, None);
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
            (Some(""), None, None),
            (Some("   "), None, None),
            (Some("test-org"), Some("test-org"), Some("test-org")),
        ] {
            assert_eq!(org_option(input), org);
            assert_eq!(trimmed_option(input), trimmed);
        }

        let mut cfg = Config::default();
        cfg.set_context(
            ("test-org", "org_test"),
            Some(("test-project", "proj_test")),
            "https://www.example.test",
            "https://api.example.test",
        );
        assert_eq!(cfg.org.as_deref(), Some("test-org"));
        assert_eq!(cfg.org_id.as_deref(), Some("org_test"));
        assert_eq!(cfg.project.as_deref(), Some("test-project"));
        assert_eq!(cfg.project_id.as_deref(), Some("proj_test"));
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
    fn load_purges_obsolete_empty_org_context() {
        let tmp = TempDir::new().unwrap();
        let path = tmp.path().join("config.json");
        fs::write(
            &path,
            r#"{"org":"","org_id":"org_old","project":"old","project_id":"proj_old"}"#,
        )
        .unwrap();
        let loaded = load_file(&path);
        assert_eq!(loaded.org, None);
        assert_eq!(loaded.org_id, None);
        assert_eq!(loaded.project, None);
        assert_eq!(loaded.project_id, None);
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
