use std::path::PathBuf;

use anyhow::{anyhow, bail, Result};
use clap::{builder::BoolishValueParser, Args, Subcommand, ValueEnum};

use crate::{
    args::BaseArgs,
    auth::{login, AvailableOrg},
    config,
    http::ApiClient,
    projects::api::{get_project_by_name, Project},
    ui::{self, is_interactive, select_project_interactive, with_spinner},
};

pub(crate) mod api;
mod delete;
mod invoke;
mod list;
mod pull;
mod push;
pub(crate) mod report;
mod view;

use api::Function;

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum FunctionTypeFilter {
    Llm,
    Scorer,
    Task,
    Tool,
    CustomView,
    Preprocessor,
    Facet,
    Classifier,
    Tag,
    Parameters,
    Sandbox,
}

impl FunctionTypeFilter {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Llm => "llm",
            Self::Scorer => "scorer",
            Self::Task => "task",
            Self::Tool => "tool",
            Self::CustomView => "custom_view",
            Self::Preprocessor => "preprocessor",
            Self::Facet => "facet",
            Self::Classifier => "classifier",
            Self::Tag => "tag",
            Self::Parameters => "parameters",
            Self::Sandbox => "sandbox",
        }
    }

    fn label(&self) -> &'static str {
        match self {
            Self::Llm => "LLM",
            Self::CustomView => "custom view",
            _ => self.as_str(),
        }
    }

    fn plural(&self) -> &'static str {
        match self {
            Self::Llm => "LLMs",
            Self::Scorer => "scorers",
            Self::Task => "tasks",
            Self::Tool => "tools",
            Self::CustomView => "custom views",
            Self::Preprocessor => "preprocessors",
            Self::Facet => "facets",
            Self::Classifier => "classifiers",
            Self::Tag => "tags",
            Self::Parameters => "parameters",
            Self::Sandbox => "sandboxes",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum IfExistsMode {
    Error,
    Replace,
    Ignore,
}

impl IfExistsMode {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Replace => "replace",
            Self::Ignore => "ignore",
        }
    }
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum FunctionsLanguage {
    Typescript,
    Python,
}

#[derive(Debug, Clone, Copy, ValueEnum, PartialEq, Eq)]
pub enum PushLanguage {
    Auto,
    #[value(name = "javascript")]
    JavaScript,
    Python,
}

fn build_web_path(function: &Function) -> String {
    let id = &function.id;
    match function.function_type.as_deref() {
        Some("tool") => format!("tools?pr={}", urlencoding::encode(id)),
        Some("scorer") => format!("scorers/{}", urlencoding::encode(id)),
        Some("classifier") => {
            let xact_id = function._xact_id.as_deref().unwrap_or("");
            format!(
                "topics?topicMapId={}&topicMapVersion={}",
                urlencoding::encode(id),
                urlencoding::encode(xact_id)
            )
        }
        Some("parameters") => format!("parameters/{}", urlencoding::encode(id)),
        _ => format!("functions/{}", urlencoding::encode(id)),
    }
}

fn label(ft: Option<FunctionTypeFilter>) -> &'static str {
    ft.map_or("function", |f| f.label())
}

fn label_plural(ft: Option<FunctionTypeFilter>) -> &'static str {
    ft.map_or("functions", |f| f.plural())
}

#[derive(Debug, Clone, Args)]
struct SlugArgs {
    /// Function slug
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,
    /// Function slug
    #[arg(long = "slug", short = 's')]
    slug_flag: Option<String>,
}

impl SlugArgs {
    fn slug(&self) -> Option<&str> {
        self.slug_positional
            .as_deref()
            .or(self.slug_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt tools list
  bt tools view my-tool
  bt scorers list
  bt scorers delete my-scorer
")]
pub struct FunctionArgs {
    #[command(subcommand)]
    command: Option<FunctionCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum FunctionCommands {
    /// List all in the current project
    List,
    /// View a function's details
    View(ViewArgs),
    /// Delete a function
    Delete(DeleteArgs),
    /// Invoke a function
    Invoke(invoke::InvokeArgs),
}

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt functions list
  bt functions view my-function
  bt functions invoke my-function --input '{\"key\":\"value\"}'
  bt functions push --file ./functions
  bt functions pull --output-dir ./braintrust
")]
pub struct FunctionsArgs {
    /// Filter by function type
    #[arg(long = "type", short = 't', value_enum)]
    function_type: Option<FunctionTypeFilter>,

    #[command(subcommand)]
    command: Option<FunctionsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum FunctionsCommands {
    /// List functions in the current project
    List(FunctionsListArgs),
    /// View function details
    View(FunctionsViewArgs),
    /// Delete a function
    Delete(FunctionsDeleteArgs),
    /// Invoke a function
    Invoke(FunctionsInvokeArgs),
    /// Push local function definitions
    Push(PushArgs),
    /// Pull remote function definitions
    Pull(PullArgs),
}

#[derive(Debug, Clone, Args)]
struct FunctionsListArgs {
    /// Filter by function type
    #[arg(long = "type", short = 't', value_enum)]
    function_type: Option<FunctionTypeFilter>,
}

#[derive(Debug, Clone, Args)]
struct FunctionsViewArgs {
    #[command(flatten)]
    inner: ViewArgs,
    /// Filter by function type (for interactive selection)
    #[arg(long = "type", short = 't', value_enum)]
    function_type: Option<FunctionTypeFilter>,
}

#[derive(Debug, Clone, Args)]
struct FunctionsDeleteArgs {
    #[command(flatten)]
    slug: SlugArgs,
    /// Skip confirmation
    #[arg(long, short = 'f')]
    force: bool,
    /// Filter by function type (for interactive selection)
    #[arg(long = "type", short = 't', value_enum)]
    function_type: Option<FunctionTypeFilter>,
}

impl FunctionsDeleteArgs {
    fn slug(&self) -> Option<&str> {
        self.slug.slug()
    }
}

#[derive(Debug, Clone, Args)]
struct FunctionsInvokeArgs {
    #[command(flatten)]
    inner: invoke::InvokeArgs,
    /// Filter by function type (for interactive selection)
    #[arg(long = "type", short = 't', value_enum)]
    function_type: Option<FunctionTypeFilter>,
}

#[derive(Debug, Clone, Args)]
pub(crate) struct PushArgs {
    /// File or directory path(s) to scan for function definitions.
    #[arg(value_name = "PATH")]
    pub files: Vec<PathBuf>,

    /// File or directory path(s) to scan for function definitions.
    #[arg(
        long = "file",
        env = "BT_FUNCTIONS_PUSH_FILES",
        value_name = "PATH",
        value_delimiter = ','
    )]
    pub file_flag: Vec<PathBuf>,

    /// Behavior when a function with the same slug already exists.
    #[arg(
        long = "if-exists",
        env = "BT_FUNCTIONS_PUSH_IF_EXISTS",
        value_enum,
        default_value = "error"
    )]
    pub if_exists: IfExistsMode,

    /// Stop after the first hard failure.
    #[arg(
        long,
        env = "BT_FUNCTIONS_PUSH_TERMINATE_ON_FAILURE",
        default_value_t = false,
        value_parser = BoolishValueParser::new()
    )]
    pub terminate_on_failure: bool,

    /// Override runner binary (e.g. tsx, vite-node, deno, python).
    #[arg(long, env = "BT_FUNCTIONS_PUSH_RUNNER", value_name = "RUNNER")]
    pub runner: Option<String>,

    /// Force runtime language selection.
    #[arg(
        long = "language",
        env = "BT_FUNCTIONS_PUSH_LANGUAGE",
        value_enum,
        default_value = "auto"
    )]
    pub language: PushLanguage,

    /// Optional Python requirements file.
    #[arg(long, env = "BT_FUNCTIONS_PUSH_REQUIREMENTS", value_name = "PATH")]
    pub requirements: Option<PathBuf>,

    /// Optional tsconfig path for JS runner and bundler.
    #[arg(long, env = "BT_FUNCTIONS_PUSH_TSCONFIG", value_name = "PATH")]
    pub tsconfig: Option<PathBuf>,

    /// Additional packages to mark external during JS bundling.
    #[arg(
        long = "external-packages",
        env = "BT_FUNCTIONS_PUSH_EXTERNAL_PACKAGES",
        num_args = 1..,
        value_delimiter = ',',
        value_name = "PACKAGE"
    )]
    pub external_packages: Vec<String>,

    /// Skip confirmation prompt.
    #[arg(long, short = 'y')]
    pub yes: bool,
}

impl PushArgs {
    pub fn resolved_files(&self) -> Vec<PathBuf> {
        let mut all = self.files.clone();
        all.extend(self.file_flag.iter().cloned());
        if all.is_empty() {
            vec![PathBuf::from(".")]
        } else {
            all
        }
    }
}

#[derive(Debug, Clone, Args)]
pub(crate) struct PullArgs {
    /// Function slug(s) to pull.
    #[arg(value_name = "SLUG")]
    pub slugs: Vec<String>,

    /// Function slug(s) to pull.
    #[arg(
        long = "slug",
        short = 's',
        env = "BT_FUNCTIONS_PULL_SLUG",
        value_delimiter = ','
    )]
    pub slug_flag: Vec<String>,

    /// Destination directory for generated files.
    #[arg(
        long,
        env = "BT_FUNCTIONS_PULL_OUTPUT_DIR",
        default_value = "./braintrust",
        value_name = "PATH"
    )]
    pub output_dir: PathBuf,

    /// Output language.
    #[arg(
        long = "language",
        env = "BT_FUNCTIONS_PULL_LANGUAGE",
        value_enum,
        default_value = "typescript"
    )]
    pub language: FunctionsLanguage,

    /// Project id filter.
    #[arg(long, env = "BT_FUNCTIONS_PULL_PROJECT_ID")]
    pub project_id: Option<String>,

    /// Function id selector.
    #[arg(long, env = "BT_FUNCTIONS_PULL_ID")]
    pub id: Option<String>,

    /// Version selector.
    #[arg(long, env = "BT_FUNCTIONS_PULL_VERSION")]
    pub version: Option<String>,

    /// Overwrite targets even when dirty or already existing.
    #[arg(
        long,
        env = "BT_FUNCTIONS_PULL_FORCE",
        default_value_t = false,
        value_parser = BoolishValueParser::new()
    )]
    pub force: bool,

    /// Show skipped files in output.
    #[arg(long, default_value_t = false)]
    pub verbose: bool,
}

impl PullArgs {
    pub fn resolved_slugs(&self) -> Vec<String> {
        let mut seen = std::collections::BTreeSet::new();
        let mut result = Vec::new();
        for s in self.slugs.iter().chain(self.slug_flag.iter()) {
            if seen.insert(s.clone()) {
                result.push(s.clone());
            }
        }
        result
    }
}

#[derive(Debug, Clone, Args)]
pub struct ViewArgs {
    #[command(flatten)]
    slug: SlugArgs,
    /// Open in browser
    #[arg(long)]
    web: bool,
    /// Show all configuration details
    #[arg(long)]
    verbose: bool,
}

impl ViewArgs {
    fn slug(&self) -> Option<&str> {
        self.slug.slug()
    }
}

#[derive(Debug, Clone, Args)]
pub struct DeleteArgs {
    #[command(flatten)]
    slug: SlugArgs,
    /// Skip confirmation
    #[arg(long, short = 'f')]
    force: bool,
}

impl DeleteArgs {
    fn slug(&self) -> Option<&str> {
        self.slug.slug()
    }
}

pub(crate) struct AuthContext {
    pub client: ApiClient,
    pub app_url: String,
    pub org_id: String,
}

pub(crate) struct ResolvedContext {
    pub client: ApiClient,
    pub app_url: String,
    pub project: Project,
}

pub(crate) async fn resolve_auth_context(base: &BaseArgs) -> Result<AuthContext> {
    let ctx = login(base).await?;
    let client = ApiClient::new(&ctx)?;
    Ok(AuthContext {
        client,
        app_url: ctx.app_url,
        org_id: ctx.login.org_id,
    })
}

pub(crate) fn current_org_label(auth_ctx: &AuthContext) -> String {
    if auth_ctx.client.org_name().trim().is_empty() {
        auth_ctx.org_id.clone()
    } else {
        auth_ctx.client.org_name().to_string()
    }
}

pub(crate) fn validate_explicit_org_selection(
    base: &BaseArgs,
    available_orgs: &[AvailableOrg],
) -> Result<()> {
    let Some(explicit_org) = base
        .org_name
        .as_deref()
        .map(str::trim)
        .filter(|value| !value.is_empty())
    else {
        return Ok(());
    };

    let exists = available_orgs
        .iter()
        .any(|org| org.name == explicit_org || org.name.eq_ignore_ascii_case(explicit_org));
    if exists {
        return Ok(());
    }

    let available = available_orgs
        .iter()
        .map(|org| org.name.as_str())
        .collect::<Vec<_>>()
        .join(", ");
    bail!("org '{explicit_org}' is not available for this credential. Available: {available}");
}

pub(crate) async fn resolve_project_context(
    base: &BaseArgs,
    auth_ctx: &AuthContext,
) -> Result<Project> {
    resolve_project_context_optional(base, auth_ctx, true)
        .await?
        .ok_or_else(|| anyhow!("--project required (or set BRAINTRUST_DEFAULT_PROJECT)"))
}

pub(crate) async fn resolve_project_context_optional(
    base: &BaseArgs,
    auth_ctx: &AuthContext,
    allow_interactive_selection: bool,
) -> Result<Option<Project>> {
    let config_project = config::load().ok().and_then(|c| c.project);
    let project_name = match base.project.as_deref().or(config_project.as_deref()) {
        Some(p) => Some(p.to_string()),
        None if allow_interactive_selection && is_interactive() => {
            Some(select_project_interactive(&auth_ctx.client, None, None).await?)
        }
        None => None,
    };

    match project_name {
        Some(project_name) => get_project_by_name(&auth_ctx.client, &project_name)
            .await?
            .map(Some)
            .ok_or_else(|| anyhow!("project '{project_name}' not found")),
        None => Ok(None),
    }
}

async fn resolve_context(base: &BaseArgs) -> Result<ResolvedContext> {
    let auth_ctx = resolve_auth_context(base).await?;
    let project = resolve_project_context(base, &auth_ctx).await?;
    Ok(ResolvedContext {
        client: auth_ctx.client,
        app_url: auth_ctx.app_url,
        project,
    })
}

pub(crate) async fn select_function_interactive(
    client: &ApiClient,
    project_id: &str,
    ft: Option<FunctionTypeFilter>,
) -> Result<Function> {
    let function_type = ft.map(|f| f.as_str());
    let mut functions = with_spinner(
        &format!("Loading {}...", label_plural(ft)),
        api::list_functions(client, project_id, function_type),
    )
    .await?;

    if functions.is_empty() {
        bail!("no {} found", label_plural(ft));
    }

    functions.sort_by(|a, b| a.name.cmp(&b.name));

    let names: Vec<String> = if ft.is_none() {
        functions
            .iter()
            .map(|f| {
                let t = f.function_type.as_deref().unwrap_or("?");
                format!("{} ({})", f.name, t)
            })
            .collect()
    } else {
        functions.iter().map(|f| f.name.clone()).collect()
    };

    let selection = ui::fuzzy_select(&format!("Select {}", label(ft)), &names, 0)?;
    Ok(functions[selection].clone())
}

pub async fn run_typed(base: BaseArgs, args: FunctionArgs, kind: FunctionTypeFilter) -> Result<()> {
    let ctx = resolve_context(&base).await?;
    let ft = Some(kind);
    match args.command {
        None | Some(FunctionCommands::List) => list::run(&ctx, base.json, ft).await,
        Some(FunctionCommands::View(v)) => {
            view::run(&ctx, v.slug(), base.json, v.web, v.verbose, ft).await
        }
        Some(FunctionCommands::Delete(d)) => delete::run(&ctx, d.slug(), d.force, ft).await,
        Some(FunctionCommands::Invoke(i)) => invoke::run(&ctx, &i, base.json, ft).await,
    }
}

pub async fn run(base: BaseArgs, args: FunctionsArgs) -> Result<()> {
    let function_type = args.function_type;
    match args.command {
        Some(FunctionsCommands::Push(push_args)) => push::run(base, push_args).await,
        Some(FunctionsCommands::Pull(pull_args)) => pull::run(base, pull_args).await,
        command => {
            let ctx = resolve_context(&base).await?;
            match command {
                None => list::run(&ctx, base.json, function_type).await,
                Some(FunctionsCommands::List(la)) => {
                    list::run(&ctx, base.json, la.function_type.or(function_type)).await
                }
                Some(FunctionsCommands::View(v)) => {
                    view::run(
                        &ctx,
                        v.inner.slug(),
                        base.json,
                        v.inner.web,
                        v.inner.verbose,
                        v.function_type.or(function_type),
                    )
                    .await
                }
                Some(FunctionsCommands::Delete(d)) => {
                    delete::run(&ctx, d.slug(), d.force, d.function_type.or(function_type)).await
                }
                Some(FunctionsCommands::Invoke(i)) => {
                    invoke::run(&ctx, &i.inner, base.json, i.function_type.or(function_type)).await
                }
                Some(FunctionsCommands::Push(_)) | Some(FunctionsCommands::Pull(_)) => {
                    unreachable!("handled before context resolution")
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Mutex, MutexGuard, OnceLock};

    use clap::{Parser, Subcommand};

    use super::*;

    #[derive(Debug, Parser)]
    struct CliHarness {
        #[command(subcommand)]
        command: Commands,
    }

    #[derive(Debug, Subcommand)]
    enum Commands {
        Functions(FunctionsArgs),
    }

    fn parse(args: &[&str]) -> anyhow::Result<FunctionsArgs> {
        let mut argv = vec!["bt"];
        argv.extend_from_slice(args);
        let parsed = CliHarness::try_parse_from(argv)?;
        match parsed.command {
            Commands::Functions(args) => Ok(args),
        }
    }

    fn test_lock() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
            .lock()
            .unwrap_or_else(|err| err.into_inner())
    }

    #[test]
    fn push_rejects_legacy_type_flag() {
        let _guard = test_lock();
        let err = parse(&["functions", "push", "--type", "tool"]).expect_err("should fail");
        let msg = err.to_string();
        assert!(msg.contains("--type"));
    }

    #[test]
    fn top_level_type_flag_still_parses_for_functions_namespace() {
        let _guard = test_lock();
        let parsed = parse(&["functions", "--type", "tool"]).expect("parse functions");
        assert!(matches!(
            parsed.function_type,
            Some(FunctionTypeFilter::Tool)
        ));
    }

    #[test]
    fn push_file_env_uses_delimiter() {
        let _guard = test_lock();
        unsafe {
            std::env::set_var("BT_FUNCTIONS_PUSH_FILES", "a.ts,b.ts");
        }
        let parsed = parse(&["functions", "push"]).expect("parse push");
        unsafe {
            std::env::remove_var("BT_FUNCTIONS_PUSH_FILES");
        }

        let FunctionsCommands::Push(push) = parsed.command.expect("subcommand") else {
            panic!("expected push command");
        };

        assert_eq!(
            push.file_flag,
            vec![PathBuf::from("a.ts"), PathBuf::from("b.ts")]
        );
    }

    #[test]
    fn push_boolish_flag_from_env() {
        let _guard = test_lock();
        unsafe {
            std::env::set_var("BT_FUNCTIONS_PUSH_TERMINATE_ON_FAILURE", "true");
        }
        let parsed = parse(&["functions", "push"]).expect("parse push");
        unsafe {
            std::env::remove_var("BT_FUNCTIONS_PUSH_TERMINATE_ON_FAILURE");
        }

        let FunctionsCommands::Push(push) = parsed.command.expect("subcommand") else {
            panic!("expected push command");
        };
        assert!(push.terminate_on_failure);
    }

    #[test]
    fn push_repeated_file_flags_append_in_order() {
        let _guard = test_lock();
        let parsed = parse(&[
            "functions",
            "push",
            "--file",
            "a.ts",
            "--file",
            "b.ts",
            "--file",
            "c.ts",
        ])
        .expect("parse push");

        let FunctionsCommands::Push(push) = parsed.command.expect("subcommand") else {
            panic!("expected push command");
        };
        assert_eq!(
            push.file_flag,
            vec![
                PathBuf::from("a.ts"),
                PathBuf::from("b.ts"),
                PathBuf::from("c.ts")
            ]
        );
    }

    #[test]
    fn push_language_from_env() {
        let _guard = test_lock();
        unsafe {
            std::env::set_var("BT_FUNCTIONS_PUSH_LANGUAGE", "python");
        }
        let parsed = parse(&["functions", "push"]).expect("parse push");
        unsafe {
            std::env::remove_var("BT_FUNCTIONS_PUSH_LANGUAGE");
        }

        let FunctionsCommands::Push(push) = parsed.command.expect("subcommand") else {
            panic!("expected push command");
        };
        assert_eq!(push.language, PushLanguage::Python);
    }

    #[test]
    fn push_requirements_from_env() {
        let _guard = test_lock();
        unsafe {
            std::env::set_var("BT_FUNCTIONS_PUSH_REQUIREMENTS", "requirements.txt");
        }
        let parsed = parse(&["functions", "push"]).expect("parse push");
        unsafe {
            std::env::remove_var("BT_FUNCTIONS_PUSH_REQUIREMENTS");
        }

        let FunctionsCommands::Push(push) = parsed.command.expect("subcommand") else {
            panic!("expected push command");
        };
        assert_eq!(push.requirements, Some(PathBuf::from("requirements.txt")));
    }

    #[test]
    fn push_external_packages_flag_accepts_space_separated_values() {
        let _guard = test_lock();
        let parsed = parse(&[
            "functions",
            "push",
            "--external-packages",
            "sqlite3",
            "fsevents",
            "@mapbox/node-pre-gyp",
        ])
        .expect("parse push");

        let FunctionsCommands::Push(push) = parsed.command.expect("subcommand") else {
            panic!("expected push command");
        };
        assert_eq!(
            push.external_packages,
            vec!["sqlite3", "fsevents", "@mapbox/node-pre-gyp"]
        );
    }

    #[test]
    fn push_external_packages_flag_accepts_comma_delimited_values() {
        let _guard = test_lock();
        let parsed = parse(&[
            "functions",
            "push",
            "--external-packages",
            "sqlite3,fsevents,@mapbox/node-pre-gyp",
        ])
        .expect("parse push");

        let FunctionsCommands::Push(push) = parsed.command.expect("subcommand") else {
            panic!("expected push command");
        };
        assert_eq!(
            push.external_packages,
            vec!["sqlite3", "fsevents", "@mapbox/node-pre-gyp"]
        );
    }

    #[test]
    fn pull_language_from_env() {
        let _guard = test_lock();
        unsafe {
            std::env::set_var("BT_FUNCTIONS_PULL_LANGUAGE", "python");
        }
        let parsed = parse(&["functions", "pull"]).expect("parse pull");
        unsafe {
            std::env::remove_var("BT_FUNCTIONS_PULL_LANGUAGE");
        }

        let FunctionsCommands::Pull(pull) = parsed.command.expect("subcommand") else {
            panic!("expected pull command");
        };
        assert_eq!(pull.language, FunctionsLanguage::Python);
    }

    #[test]
    fn pull_language_defaults_to_typescript() {
        let _guard = test_lock();
        unsafe {
            std::env::remove_var("BT_FUNCTIONS_PULL_LANGUAGE");
        }
        let parsed = parse(&["functions", "pull"]).expect("parse pull");
        let FunctionsCommands::Pull(pull) = parsed.command.expect("subcommand") else {
            panic!("expected pull command");
        };
        assert_eq!(pull.language, FunctionsLanguage::Typescript);
    }

    #[test]
    fn pull_rejects_invalid_language() {
        let _guard = test_lock();
        let err = parse(&["functions", "pull", "--language", "ruby"]).expect_err("should fail");
        assert!(err.to_string().contains("ruby"));
    }

    #[test]
    fn push_rejects_invalid_language() {
        let _guard = test_lock();
        let err =
            parse(&["functions", "push", "--language", "typescript"]).expect_err("should fail");
        assert!(err.to_string().contains("typescript"));
    }

    #[test]
    fn pull_conflicts_id_and_slug_flag() {
        let _guard = test_lock();
        let parsed =
            parse(&["functions", "pull", "--id", "f1", "--slug", "slug"]).expect("parse pull");
        let FunctionsCommands::Pull(pull) = parsed.command.expect("subcommand") else {
            panic!("expected pull");
        };
        assert_eq!(pull.id.as_deref(), Some("f1"));
        assert_eq!(pull.resolved_slugs(), vec!["slug"]);
    }

    #[test]
    fn pull_conflicts_id_and_positional_slug() {
        let _guard = test_lock();
        let parsed = parse(&["functions", "pull", "--id", "f1", "my-slug"]).expect("parse pull");
        let FunctionsCommands::Pull(pull) = parsed.command.expect("subcommand") else {
            panic!("expected pull");
        };
        assert_eq!(pull.id.as_deref(), Some("f1"));
        assert_eq!(pull.resolved_slugs(), vec!["my-slug"]);
    }

    #[test]
    fn pull_positional_slugs_parse() {
        let _guard = test_lock();
        let parsed = parse(&["functions", "pull", "slug-a", "slug-b"]).expect("parse pull");
        let FunctionsCommands::Pull(pull) = parsed.command.expect("subcommand") else {
            panic!("expected pull");
        };
        assert_eq!(pull.resolved_slugs(), vec!["slug-a", "slug-b"]);
    }

    #[test]
    fn pull_slug_flag_repeats() {
        let _guard = test_lock();
        let parsed =
            parse(&["functions", "pull", "--slug", "a", "--slug", "b"]).expect("parse pull");
        let FunctionsCommands::Pull(pull) = parsed.command.expect("subcommand") else {
            panic!("expected pull");
        };
        assert_eq!(pull.resolved_slugs(), vec!["a", "b"]);
    }

    #[test]
    fn pull_merges_positional_and_flag_slugs() {
        let _guard = test_lock();
        let parsed =
            parse(&["functions", "pull", "pos-slug", "--slug", "flag-slug"]).expect("parse pull");
        let FunctionsCommands::Pull(pull) = parsed.command.expect("subcommand") else {
            panic!("expected pull");
        };
        assert_eq!(pull.resolved_slugs(), vec!["pos-slug", "flag-slug"]);
    }

    #[test]
    fn pull_deduplicates_slugs() {
        let _guard = test_lock();
        let parsed = parse(&["functions", "pull", "same", "--slug", "same"]).expect("parse pull");
        let FunctionsCommands::Pull(pull) = parsed.command.expect("subcommand") else {
            panic!("expected pull");
        };
        assert_eq!(pull.resolved_slugs(), vec!["same"]);
    }

    #[test]
    fn pull_slug_env_uses_delimiter() {
        let _guard = test_lock();
        unsafe {
            std::env::set_var("BT_FUNCTIONS_PULL_SLUG", "a,b,c");
        }
        let parsed = parse(&["functions", "pull"]).expect("parse pull");
        unsafe {
            std::env::remove_var("BT_FUNCTIONS_PULL_SLUG");
        }

        let FunctionsCommands::Pull(pull) = parsed.command.expect("subcommand") else {
            panic!("expected pull command");
        };
        assert_eq!(pull.slug_flag, vec!["a", "b", "c"]);
    }
}
