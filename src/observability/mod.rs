mod pull;
mod push;
mod template;

use std::path::PathBuf;

use anyhow::{bail, Context, Result};
use clap::{Args, Subcommand};
use dialoguer::{theme::ColorfulTheme, Confirm};

use crate::{
    args::BaseArgs,
    functions::api::list_all_functions,
    http::{build_http_client, DEFAULT_HTTP_TIMEOUT},
    project_context::resolve_project_command_context_with_auth_mode,
    topics::api::list_project_automations,
    ui::{self, print_command_status, with_spinner, CommandStatus},
    utils::read_text_source,
};

use self::{push::Snapshot, template::ActiveObservabilityTemplate};

#[derive(Debug, Clone, Args)]
pub(crate) struct ObservabilityArgs {
    #[command(subcommand)]
    command: ObservabilityCommand,
}

#[derive(Debug, Clone, Subcommand)]
enum ObservabilityCommand {
    /// Pull and push facets and Loop automations as a portable template
    Template(TemplateArgs),
}

#[derive(Debug, Clone, Args)]
#[command(after_help = "\
Examples:
  bt observability template pull --output active-observability-template.json
  bt observability template push active-observability-template.json --project test-project
  bt observability template push https://example.com/active-observability-template.json
  bt observability template pull | bt observability template push - --project test-project
")]
struct TemplateArgs {
    #[command(subcommand)]
    command: TemplateCommand,
}

#[derive(Debug, Clone, Subcommand)]
enum TemplateCommand {
    /// Pull facets and Loop automations into a portable template
    Pull(PullArgs),
    /// Push facets and Loop automations from a portable template
    Push(PushArgs),
}

#[derive(Debug, Clone, Args)]
pub(super) struct PullArgs {
    /// Write the template to this path instead of stdout
    #[arg(
        long,
        short = 'O',
        env = "BT_OBSERVABILITY_TEMPLATE_PULL_OUTPUT",
        value_name = "PATH"
    )]
    output: Option<PathBuf>,

    /// Overwrite an existing output file
    #[arg(
        long,
        env = "BT_OBSERVABILITY_TEMPLATE_PULL_FORCE",
        default_value_t = false,
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    force: bool,
}

#[derive(Debug, Clone, Args)]
struct PushArgs {
    /// Template path, HTTP(S) URL, or - to read from stdin
    #[arg(value_name = "SOURCE")]
    source_positional: Option<String>,

    /// Template path, HTTP(S) URL, or - to read from stdin
    #[arg(
        long = "file",
        short = 'f',
        env = "BT_OBSERVABILITY_TEMPLATE_PUSH_FILE",
        value_name = "SOURCE"
    )]
    source_flag: Option<String>,

    /// Use this existing Topics automation for every facet
    #[arg(
        long,
        env = "BT_OBSERVABILITY_TEMPLATE_PUSH_TOPICS_AUTOMATION",
        value_name = "NAME_OR_ID"
    )]
    topics_automation: Option<String>,

    /// Replace existing matching resources
    #[arg(
        long,
        env = "BT_OBSERVABILITY_TEMPLATE_PUSH_FORCE",
        default_value_t = false,
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    force: bool,

    /// Skip the confirmation prompt
    #[arg(
        long,
        short = 'y',
        env = "BT_OBSERVABILITY_TEMPLATE_PUSH_YES",
        default_value_t = false,
        value_parser = clap::builder::BoolishValueParser::new()
    )]
    yes: bool,
}

impl PushArgs {
    fn source(&self) -> Result<&str> {
        match (&self.source_positional, &self.source_flag) {
            (Some(_), Some(_)) => bail!("use either a template source or --file, not both"),
            (Some(source), None) | (None, Some(source)) => Ok(source),
            (None, None) => bail!(
                "active observability template source required. Use: bt observability template push <source>"
            ),
        }
    }
}

pub(crate) async fn run(base: BaseArgs, args: ObservabilityArgs) -> Result<()> {
    match args.command {
        ObservabilityCommand::Template(args) => run_template(base, args).await,
    }
}

async fn run_template(base: BaseArgs, args: TemplateArgs) -> Result<()> {
    match args.command {
        TemplateCommand::Pull(args) => pull::run(base, args).await,
        TemplateCommand::Push(args) => run_push(base, args).await,
    }
}

async fn run_push(base: BaseArgs, args: PushArgs) -> Result<()> {
    let template = with_spinner(
        "Loading active observability template...",
        read_template_source(args.source()?),
    )
    .await?;
    template::validate(&template)?;

    let ctx = resolve_project_command_context_with_auth_mode(&base, false).await?;
    let snapshot = with_spinner("Checking target resources...", async {
        let (functions, automations) = tokio::try_join!(
            list_all_functions(&ctx.client, &ctx.project.id),
            list_project_automations(&ctx.client, &ctx.project.id),
        )?;
        Ok::<_, anyhow::Error>(Snapshot {
            functions,
            automations,
        })
    })
    .await?;
    let plan = push::plan(
        &template,
        snapshot,
        args.topics_automation.as_deref(),
        args.force,
    )?;

    if should_confirm_push(base.json, args.yes, ui::is_interactive())
        && !confirm_push(&ctx, &template, args.force)?
    {
        return Ok(());
    }

    let result = with_spinner(
        "Pushing active observability template...",
        push::execute(&ctx.client, &ctx.project.id, plan),
    )
    .await?;

    if base.json {
        println!(
            "{}",
            serde_json::to_string(&serde_json::json!({
                "kind": "active_observability_template",
                "status": "pushed",
                "project": ctx.project.name,
                "facets": result.facets,
                "automations": result.automations,
            }))?
        );
    } else {
        print_command_status(
            CommandStatus::Success,
            &format!(
                "Pushed active observability template to '{}' ({} facets, {} Loop automations)",
                ctx.project.name,
                result.facets.len(),
                result.automations.len()
            ),
        );
    }
    Ok(())
}

fn should_confirm_push(json: bool, yes: bool, interactive: bool) -> bool {
    !json && !yes && interactive
}

fn confirm_push(
    ctx: &crate::project_context::ProjectContext,
    template: &ActiveObservabilityTemplate,
    force: bool,
) -> Result<bool> {
    let replacement = if force {
        " and replace matching resources"
    } else {
        ""
    };
    let prompt = format!(
        "Push {} facets and {} Loop automations, including Topics wiring, to {}/{}{}?",
        template.facets.len(),
        template.automations.len(),
        ctx.client.org_name(),
        ctx.project.name,
        replacement
    );
    let term =
        ui::prompt_term().ok_or_else(|| anyhow::anyhow!("interactive mode requires a TTY"))?;
    Confirm::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .default(false)
        .interact_on(&term)
        .context("failed to confirm active observability template push")
}

async fn read_template_source(source: &str) -> Result<ActiveObservabilityTemplate> {
    let contents = if source == "-" {
        read_text_source(source, "active observability template")?
    } else if source.starts_with("http://") || source.starts_with("https://") {
        let url = reqwest::Url::parse(source).context("invalid template URL")?;
        let response = build_http_client(DEFAULT_HTTP_TIMEOUT)?
            .get(url)
            .send()
            .await
            .context("failed to download template URL")?
            .error_for_status()
            .context("failed to download template URL")?;
        response
            .text()
            .await
            .context("failed to read template URL response")?
    } else {
        std::fs::read_to_string(source)
            .with_context(|| format!("failed to read active observability template {source}"))?
    };

    serde_json::from_str(&contents).with_context(|| {
        if source == "-" {
            "failed to parse active observability template from stdin as JSON".to_string()
        } else if source.starts_with("http://") || source.starts_with("https://") {
            "failed to parse template URL as JSON; for GitHub Gists, use the Raw URL".to_string()
        } else {
            format!("failed to parse active observability template {source} as JSON")
        }
    })
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::*;

    #[test]
    fn active_observability_commands_parse_from_the_root_cli() {
        for args in [
            vec![
                "bt",
                "observability",
                "template",
                "pull",
                "--output",
                "template.json",
            ],
            vec![
                "bt",
                "observability",
                "template",
                "push",
                "template.json",
                "--topics-automation",
                "Topics",
                "--force",
                "--yes",
            ],
            vec![
                "bt",
                "observability",
                "template",
                "push",
                "--file",
                "template.json",
            ],
        ] {
            crate::Cli::try_parse_from(args).expect("command should parse");
        }
    }

    #[test]
    fn active_observability_push_requires_exactly_one_source() {
        let neither = PushArgs {
            source_positional: None,
            source_flag: None,
            topics_automation: None,
            force: false,
            yes: false,
        };
        assert!(neither
            .source()
            .unwrap_err()
            .to_string()
            .contains("source required"));

        let both = PushArgs {
            source_positional: Some("one.json".to_string()),
            source_flag: Some("two.json".to_string()),
            topics_automation: None,
            force: false,
            yes: false,
        };
        assert!(both.source().unwrap_err().to_string().contains("either"));
    }

    #[test]
    fn active_observability_push_never_prompts_for_json_output() {
        assert!(!should_confirm_push(true, false, true));
        assert!(should_confirm_push(false, false, true));
        assert!(!should_confirm_push(false, true, true));
        assert!(!should_confirm_push(false, false, false));
    }

    #[tokio::test]
    async fn active_observability_reads_a_local_template() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("template.json");
        std::fs::write(
            &path,
            r#"{"kind":"active_observability_template","schema_version":1}"#,
        )
        .expect("write template");

        let template = read_template_source(path.to_str().expect("UTF-8 path"))
            .await
            .expect("read template");
        assert_eq!(template.kind, template::KIND);
    }
}
