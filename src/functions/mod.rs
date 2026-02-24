use anyhow::{anyhow, bail, Result};
use clap::{Args, Subcommand, ValueEnum};

use crate::{
    args::BaseArgs,
    auth::login,
    config,
    http::ApiClient,
    projects::api::{get_project_by_name, Project},
    ui::{self, is_interactive, select_project_interactive, with_spinner},
};

pub mod api;
mod delete;
mod list;
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
        }
    }

    fn url_segment(&self) -> &'static str {
        match self {
            Self::Tool => "tools?pr=",
            Self::Scorer => "scorers/",
            _ => "functions/",
        }
    }

    fn from_api_str(s: &str) -> Option<Self> {
        match s {
            "llm" => Some(Self::Llm),
            "scorer" => Some(Self::Scorer),
            "task" => Some(Self::Task),
            "tool" => Some(Self::Tool),
            "custom_view" => Some(Self::CustomView),
            "preprocessor" => Some(Self::Preprocessor),
            "facet" => Some(Self::Facet),
            "classifier" => Some(Self::Classifier),
            "tag" => Some(Self::Tag),
            "parameters" => Some(Self::Parameters),
            _ => None,
        }
    }
}

fn url_segment_for(function_type: Option<&str>) -> &'static str {
    function_type
        .and_then(FunctionTypeFilter::from_api_str)
        .map_or("functions/", |ft| ft.url_segment())
}

fn label(ft: Option<FunctionTypeFilter>) -> &'static str {
    ft.map_or("function", |f| f.label())
}

fn label_plural(ft: Option<FunctionTypeFilter>) -> &'static str {
    ft.map_or("functions", |f| f.plural())
}

// --- Slug args (shared) ---

#[derive(Debug, Clone, Args)]
struct SlugArgs {
    #[arg(value_name = "SLUG")]
    slug_positional: Option<String>,
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

// --- Wrapper args (bt tools / bt scorers) ---

#[derive(Debug, Clone, Args)]
pub struct FunctionArgs {
    #[command(subcommand)]
    pub command: Option<FunctionCommands>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum FunctionCommands {
    /// List all in the current project
    List,
    /// View details
    View(ViewArgs),
    /// Delete by slug
    Delete(DeleteArgs),
}

// --- bt functions args ---

#[derive(Debug, Clone, Args)]
pub struct FunctionsArgs {
    #[command(subcommand)]
    pub command: Option<FunctionsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
pub enum FunctionsCommands {
    /// List functions in the current project
    List(FunctionsListArgs),
    /// View function details
    View(ViewArgs),
    /// Delete a function
    Delete(FunctionsDeleteArgs),
}

#[derive(Debug, Clone, Args)]
pub struct FunctionsListArgs {
    #[arg(long = "type", short = 't', value_enum)]
    pub function_type: Option<FunctionTypeFilter>,
}

#[derive(Debug, Clone, Args)]
pub struct FunctionsDeleteArgs {
    #[command(flatten)]
    slug: SlugArgs,
    #[arg(long, short = 'f')]
    force: bool,
    #[arg(long = "type", short = 't', value_enum)]
    pub function_type: Option<FunctionTypeFilter>,
}

impl FunctionsDeleteArgs {
    fn slug(&self) -> Option<&str> {
        self.slug.slug()
    }
}

// --- Shared view/delete args ---

#[derive(Debug, Clone, Args)]
pub struct ViewArgs {
    #[command(flatten)]
    slug: SlugArgs,
    #[arg(long)]
    web: bool,
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
    #[arg(long, short = 'f')]
    force: bool,
}

impl DeleteArgs {
    fn slug(&self) -> Option<&str> {
        self.slug.slug()
    }
}

// --- Resolved context ---

pub(crate) struct ResolvedContext {
    pub client: ApiClient,
    pub app_url: String,
    pub project: Project,
}

async fn resolve_context(base: &BaseArgs) -> Result<ResolvedContext> {
    let ctx = login(base).await?;
    let client = ApiClient::new(&ctx)?;
    let config_project = config::load().ok().and_then(|c| c.project);
    let project_name = match base.project.as_deref().or(config_project.as_deref()) {
        Some(p) => p.to_string(),
        None if is_interactive() => select_project_interactive(&client, None, None).await?,
        None => anyhow::bail!(
            "--project required (or set BRAINTRUST_DEFAULT_PROJECT, or run `bt switch`)"
        ),
    };
    let project = get_project_by_name(&client, &project_name)
        .await?
        .ok_or_else(|| anyhow!("project '{project_name}' not found"))?;
    Ok(ResolvedContext {
        client,
        app_url: ctx.app_url,
        project,
    })
}

// --- Interactive selection ---

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

// --- Entry points ---

pub async fn run(base: BaseArgs, args: FunctionArgs, kind: FunctionTypeFilter) -> Result<()> {
    let ctx = resolve_context(&base).await?;
    let ft = Some(kind);
    match args.command {
        None | Some(FunctionCommands::List) => list::run(&ctx, base.json, ft).await,
        Some(FunctionCommands::View(v)) => {
            view::run(&ctx, v.slug(), base.json, v.web, v.verbose, ft).await
        }
        Some(FunctionCommands::Delete(d)) => delete::run(&ctx, d.slug(), d.force, ft).await,
    }
}

pub async fn run_functions(base: BaseArgs, args: FunctionsArgs) -> Result<()> {
    let ctx = resolve_context(&base).await?;
    match args.command {
        None => list::run(&ctx, base.json, None).await,
        Some(FunctionsCommands::List(ref la)) => list::run(&ctx, base.json, la.function_type).await,
        Some(FunctionsCommands::View(v)) => {
            view::run(&ctx, v.slug(), base.json, v.web, v.verbose, None).await
        }
        Some(FunctionsCommands::Delete(d)) => {
            delete::run(&ctx, d.slug(), d.force, d.function_type).await
        }
    }
}
