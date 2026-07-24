use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use reqwest::Url;
use urlencoding::decode;

use crate::{
    args::BaseArgs,
    http::ApiClient,
    project_context::resolve_project_command_context_with_auth_mode,
    ui::{self, with_spinner},
};

pub(crate) mod api;
mod compare;
mod delete;
mod list;
mod view;

use api::{self as experiments_api, Experiment};

pub(crate) use crate::project_context::ProjectContext as ResolvedContext;

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt experiments list
  bt experiments view my-experiment
  bt experiments compare baseline challenger-a challenger-b
  bt experiments compare 'https://www.braintrust.dev/app/test-org/p/test-project/experiments/baseline?c=challenger&lt=comparison'
  bt experiments delete my-experiment
")]
pub struct ExperimentsArgs {
    #[command(subcommand)]
    command: Option<ExperimentsCommands>,
}

#[derive(Debug, Clone, Subcommand)]
enum ExperimentsCommands {
    /// List all experiments
    List,
    /// View an experiment
    View(ViewArgs),
    /// Compare summary metrics for up to eight experiments
    #[command(visible_alias = "summary")]
    Compare(CompareArgs),
    /// Delete an experiment
    Delete(DeleteArgs),
}

#[derive(Debug, Clone, Args)]
struct ViewArgs {
    /// Experiment name (positional)
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Experiment name (flag)
    #[arg(long = "name", short = 'n')]
    name_flag: Option<String>,

    /// Open in browser
    #[arg(long)]
    web: bool,
}

impl ViewArgs {
    fn name(&self) -> Option<&str> {
        self.name_positional
            .as_deref()
            .or(self.name_flag.as_deref())
    }
}

#[derive(Debug, Clone, Args)]
#[command(about = "Compare summary metrics for up to eight experiments")]
struct CompareArgs {
    /// Base experiment name, or a Braintrust experiment comparison URL
    #[arg(value_name = "BASE_OR_URL")]
    base_or_url: Option<String>,

    /// Comparison experiment names; up to seven may be provided
    #[arg(value_name = "COMPARISON")]
    comparison_positional: Vec<String>,

    /// Braintrust experiment comparison URL
    #[arg(long)]
    url: Option<String>,

    /// Base experiment name
    #[arg(long)]
    base: Option<String>,

    /// Comparison experiment name; may be passed multiple times
    #[arg(long, short = 'c')]
    comparison: Vec<String>,

    /// Include zero/no-change rows in the summary table
    #[arg(long)]
    all: bool,
}

#[derive(Debug, Clone, Args)]
struct DeleteArgs {
    /// Experiment name (positional)
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Experiment name (flag)
    #[arg(long = "name", short = 'n')]
    name_flag: Option<String>,

    /// Skip confirmation
    #[arg(long, short = 'f')]
    force: bool,
}

impl DeleteArgs {
    fn name(&self) -> Option<&str> {
        self.name_positional
            .as_deref()
            .or(self.name_flag.as_deref())
    }
}

pub(crate) async fn select_experiment_interactive(
    client: &ApiClient,
    project: &str,
) -> Result<Experiment> {
    let mut experiments = with_spinner(
        "Loading experiments...",
        experiments_api::list_experiments(client, project),
    )
    .await?;

    if experiments.is_empty() {
        bail!("no experiments found");
    }

    experiments.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = experiments.iter().map(|e| e.name.as_str()).collect();
    let selection = ui::fuzzy_select("Select experiment", &names, 0)?;
    Ok(experiments[selection].clone())
}

pub async fn run(base: BaseArgs, args: ExperimentsArgs) -> Result<()> {
    let parsed_startup_url = parse_startup_experiment_compare_url_from_args(&args)?;
    let base = apply_experiment_url_hints_to_base(base, parsed_startup_url.as_ref());
    let read_only = experiments_command_is_read_only(args.command.as_ref());
    let ctx = resolve_project_command_context_with_auth_mode(&base, read_only).await?;

    match args.command {
        None | Some(ExperimentsCommands::List) => list::run(&ctx, base.json).await,
        Some(ExperimentsCommands::View(v)) => view::run(&ctx, v.name(), base.json, v.web).await,
        Some(ExperimentsCommands::Compare(c)) => {
            compare::run(&ctx, c, parsed_startup_url.as_ref(), base.json).await
        }
        Some(ExperimentsCommands::Delete(d)) => delete::run(&ctx, d.name(), d.force).await,
    }
}

fn experiments_command_is_read_only(command: Option<&ExperimentsCommands>) -> bool {
    matches!(
        command,
        None | Some(ExperimentsCommands::List)
            | Some(ExperimentsCommands::View(_))
            | Some(ExperimentsCommands::Compare(_))
    )
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedExperimentCompareUrl {
    org: Option<String>,
    project: Option<String>,
    base_experiment: Option<String>,
    comparison_experiment: Option<String>,
}

fn parse_startup_experiment_compare_url_from_args(
    args: &ExperimentsArgs,
) -> Result<Option<ParsedExperimentCompareUrl>> {
    let Some(ExperimentsCommands::Compare(compare_args)) = args.command.as_ref() else {
        return Ok(None);
    };

    let startup_url = select_startup_experiment_url(
        compare_args.url.as_deref(),
        compare_args
            .base_or_url
            .as_deref()
            .filter(|value| looks_like_url(value)),
    )?;

    startup_url
        .as_deref()
        .map(parse_experiment_compare_url)
        .transpose()
}

fn apply_experiment_url_hints_to_base(
    mut base: BaseArgs,
    parsed_url: Option<&ParsedExperimentCompareUrl>,
) -> BaseArgs {
    let Some(parsed_url) = parsed_url else {
        return base;
    };

    if base
        .project
        .as_deref()
        .map(str::trim)
        .is_none_or(str::is_empty)
    {
        if let Some(project) = parsed_url
            .project
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            base.project = Some(project.to_string());
        }
    }

    let has_org_override = base
        .org_name
        .as_deref()
        .map(str::trim)
        .is_some_and(|v| !v.is_empty());
    if !has_org_override {
        if let Some(org) = parsed_url
            .org
            .as_deref()
            .map(str::trim)
            .filter(|v| !v.is_empty())
        {
            base.org_name = Some(org.to_string());
        }
    }

    base
}

fn select_startup_experiment_url(
    long_url: Option<&str>,
    positional_url: Option<&str>,
) -> Result<Option<String>> {
    match (long_url, positional_url) {
        (Some(a), Some(b)) if a.trim() != b.trim() => {
            bail!("received both --url and positional URL with different values; pass only one")
        }
        (Some(a), _) => Ok(Some(a.trim().to_string())),
        (_, Some(b)) => Ok(Some(b.trim().to_string())),
        _ => Ok(None),
    }
}

fn looks_like_url(value: &str) -> bool {
    let value = value.trim();
    value.contains("://") || value.starts_with('/') || value.contains("/app/")
}

fn parse_experiment_compare_url(input: &str) -> Result<ParsedExperimentCompareUrl> {
    let input = input.trim();
    if input.is_empty() {
        bail!("experiment comparison URL is empty");
    }

    let parsed_url = if let Ok(url) = Url::parse(input) {
        url
    } else if input.contains("://") {
        Url::parse(input).context("invalid experiment comparison URL")?
    } else {
        let with_scheme = if input.starts_with('/') {
            format!("https://www.braintrust.dev{input}")
        } else {
            format!("https://{input}")
        };
        Url::parse(&with_scheme).context("invalid experiment comparison URL")?
    };

    let mut parsed = ParsedExperimentCompareUrl {
        org: None,
        project: None,
        base_experiment: None,
        comparison_experiment: None,
    };

    if let Some(segments) = parsed_url.path_segments() {
        let parts: Vec<String> = segments
            .filter(|part| !part.is_empty())
            .map(decode_url_segment)
            .collect();
        if parts.len() >= 2 && parts[0] == "app" {
            parsed.org = Some(parts[1].clone());
            if parts.len() >= 4 && parts[2] == "p" {
                parsed.project = Some(parts[3].clone());
                if parts.len() >= 6 && parts[4] == "experiments" {
                    parsed.base_experiment = Some(parts[5].clone());
                }
            }
        }
    }

    for (key, value) in parsed_url.query_pairs() {
        if key == "c" && !value.is_empty() {
            parsed.comparison_experiment = Some(value.to_string());
            break;
        }
    }

    if parsed.base_experiment.is_none() {
        bail!("experiment comparison URL must include /experiments/<base-experiment>");
    }

    Ok(parsed)
}

fn decode_url_segment(segment: &str) -> String {
    decode(segment)
        .map(|decoded| decoded.into_owned())
        .unwrap_or_else(|_| segment.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn experiments_routes_list_and_view_to_read_only_auth() {
        assert!(experiments_command_is_read_only(None));
        assert!(experiments_command_is_read_only(Some(
            &ExperimentsCommands::List
        )));
        assert!(experiments_command_is_read_only(Some(
            &ExperimentsCommands::View(ViewArgs {
                name_positional: Some("my-experiment".to_string()),
                name_flag: None,
                web: false,
            })
        )));
        assert!(experiments_command_is_read_only(Some(
            &ExperimentsCommands::Compare(CompareArgs {
                base_or_url: Some("baseline".to_string()),
                comparison_positional: vec!["challenger".to_string()],
                url: None,
                base: None,
                comparison: Vec::new(),
                all: false,
            })
        )));
    }

    #[test]
    fn experiments_routes_delete_to_validated_auth() {
        assert!(!experiments_command_is_read_only(Some(
            &ExperimentsCommands::Delete(DeleteArgs {
                name_positional: Some("my-experiment".to_string()),
                name_flag: None,
                force: true,
            })
        )));
    }

    #[test]
    fn parses_experiment_comparison_url() {
        let parsed = parse_experiment_compare_url(
            "https://www.example.test/app/test-org/p/test-project/experiments/baseline?c=challenger&lt=comparison",
        )
        .expect("parse url");

        assert_eq!(
            parsed,
            ParsedExperimentCompareUrl {
                org: Some("test-org".to_string()),
                project: Some("test-project".to_string()),
                base_experiment: Some("baseline".to_string()),
                comparison_experiment: Some("challenger".to_string()),
            }
        );
    }

    #[test]
    fn compare_startup_url_uses_url_like_positional_arg() {
        let args = ExperimentsArgs {
            command: Some(ExperimentsCommands::Compare(CompareArgs {
                base_or_url: Some(
                    "https://www.example.test/app/test-org/p/test-project/experiments/baseline?c=challenger".to_string(),
                ),
                comparison_positional: Vec::new(),
                url: None,
                base: None,
                comparison: Vec::new(),
                all: false,
            })),
        };

        let parsed = parse_startup_experiment_compare_url_from_args(&args)
            .expect("parse startup url")
            .expect("startup url");

        assert_eq!(parsed.base_experiment.as_deref(), Some("baseline"));
        assert_eq!(parsed.comparison_experiment.as_deref(), Some("challenger"));
    }
}
