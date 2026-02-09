use std::io::IsTerminal;

use anyhow::{bail, Context, Result};
use clap::Args;
use reqwest::Client;
use serde::Deserialize;
use urlencoding::encode;

use crate::args::BaseArgs;
use crate::login::{self, LoginContext};
use crate::projects::api::Project;
use crate::ui::{self, print_command_status, with_spinner, CommandStatus};

#[derive(Debug, Clone, Args)]
pub struct SwitchArgs {
    /// Target: name (project or org), org/project, or omit to select
    #[arg(value_name = "TARGET")]
    target: Option<String>,

    /// Organization name
    #[arg(long = "org", short = 'o')]
    org_flag: Option<String>,

    /// Project name
    #[arg(long = "proj")]
    project_flag: Option<String>,
}

enum ParsedTarget<'a> {
    /// org/project - explicit both
    OrgAndProject(&'a str, &'a str),
    /// bare name - could be project or org, need to resolve
    Ambiguous(&'a str),
    /// no target - fuzzy select both
    Interactive,
    /// flags provided
    Flags(Option<&'a str>, Option<&'a str>),
}

impl SwitchArgs {
    fn parse_target(&self) -> ParsedTarget<'_> {
        if self.org_flag.is_some() || self.project_flag.is_some() {
            return ParsedTarget::Flags(self.org_flag.as_deref(), self.project_flag.as_deref());
        }

        match &self.target {
            Some(t) if t.contains('/') => {
                let parts: Vec<&str> = t.splitn(2, '/').collect();
                ParsedTarget::OrgAndProject(parts[0], parts[1])
            }
            Some(t) => ParsedTarget::Ambiguous(t.as_str()),
            None => ParsedTarget::Interactive,
        }
    }
}

pub async fn run(base: BaseArgs, args: SwitchArgs) -> Result<()> {
    let http = Client::new();
    let ctx = login::login(&base).await?;

    let (org_name, project_name) = match args.parse_target() {
        ParsedTarget::OrgAndProject(org, proj) => {
            let project = validate_or_create_project(&http, &ctx, org, proj).await?;
            (org.to_string(), project)
        }
        ParsedTarget::Ambiguous(name) => resolve_ambiguous_target(&http, &ctx, name).await?,
        ParsedTarget::Interactive => {
            let org = select_org_interactive(&http, &ctx).await?;
            let project = select_project_interactive(&http, &ctx, &org).await?;
            (org, project)
        }
        ParsedTarget::Flags(org, proj) => {
            let org = match org {
                Some(o) => o.to_string(),
                None => select_org_interactive(&http, &ctx).await?,
            };
            let project = match proj {
                Some(p) => validate_or_create_project(&http, &ctx, &org, p).await?,
                None => select_project_interactive(&http, &ctx, &org).await?,
            };
            (org, project)
        }
    };

    print_switch_exports(&org_name, &project_name);
    Ok(())
}

/// Resolve ambiguous target - could be project name or org name.
/// Priority: project in current org > org name
async fn resolve_ambiguous_target(
    http: &Client,
    ctx: &LoginContext,
    name: &str,
) -> Result<(String, String)> {
    // First, check if it's a project in the current org
    let current_org = &ctx.login.org_name;
    let project = with_spinner(
        "Checking project...",
        get_project_by_name(http, ctx, current_org, name),
    )
    .await?;

    if project.is_some() {
        return Ok((current_org.clone(), name.to_string()));
    }

    // Not a project - check if it's an org
    if org_exists(http, ctx, name).await? {
        let project = select_project_interactive(http, ctx, name).await?;
        return Ok((name.to_string(), project));
    }

    bail!("'{name}' not found as project in '{current_org}' or as organization")
}

async fn select_org_interactive(_http: &Client, _ctx: &LoginContext) -> Result<String> {
    // TODO: Replace with actual API call to list orgs
    let orgs = vec!["acme-corp", "parker-test", "personal"];
    let selection = ui::fuzzy_select("Select organization", &orgs)?;
    Ok(orgs[selection].to_string())
}

async fn org_exists(_http: &Client, _ctx: &LoginContext, _name: &str) -> Result<bool> {
    // TODO: Replace with actual API call to check org exists
    // For now, check against placeholder list
    let orgs = ["acme-corp", "dev-team", "personal"];
    Ok(orgs.contains(&_name))
}

async fn validate_or_create_project(
    http: &Client,
    ctx: &LoginContext,
    org_name: &str,
    project_name: &str,
) -> Result<String> {
    let exists = with_spinner(
        "Loading project...",
        get_project_by_name(http, ctx, org_name, project_name),
    )
    .await?;

    if exists.is_some() {
        return Ok(project_name.to_string());
    }

    if !std::io::stdin().is_terminal() {
        bail!("project '{project_name}' not found");
    }

    let create = dialoguer::Confirm::new()
        .with_prompt(format!("Project '{project_name}' not found. Create it?"))
        .default(false)
        .interact()?;

    if create {
        with_spinner(
            "Creating project...",
            create_project(http, ctx, org_name, project_name),
        )
        .await?;
        Ok(project_name.to_string())
    } else {
        bail!("project '{project_name}' not found");
    }
}

async fn select_project_interactive(
    http: &Client,
    ctx: &LoginContext,
    org_name: &str,
) -> Result<String> {
    let mut projects =
        with_spinner("Loading projects...", list_projects(http, ctx, org_name)).await?;

    if projects.is_empty() {
        bail!("no projects found in org '{org_name}'");
    }

    projects.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = projects.iter().map(|p| p.name.as_str()).collect();

    let selection = ui::fuzzy_select("Select project", &names)?;
    Ok(projects[selection].name.clone())
}

fn print_switch_exports(org: &str, project: &str) {
    print_command_status(
        CommandStatus::Success,
        &format!("Switched to {org}/{project}"),
    );
}

// API helpers - variants that accept explicit org_name

#[derive(Debug, Deserialize)]
struct ListResponse {
    objects: Vec<Project>,
}

async fn list_projects(http: &Client, ctx: &LoginContext, org_name: &str) -> Result<Vec<Project>> {
    let url = format!(
        "{}/v1/project?org_name={}",
        ctx.api_url.trim_end_matches('/'),
        encode(org_name)
    );

    let response = http
        .get(&url)
        .bearer_auth(&ctx.login.api_key)
        .send()
        .await
        .context("failed to list projects")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("failed to list projects ({status}): {body}");
    }

    let list: ListResponse = response
        .json()
        .await
        .context("failed to parse projects response")?;

    Ok(list.objects)
}

async fn get_project_by_name(
    http: &Client,
    ctx: &LoginContext,
    org_name: &str,
    name: &str,
) -> Result<Option<Project>> {
    let url = format!(
        "{}/v1/project?org_name={}&name={}",
        ctx.api_url.trim_end_matches('/'),
        encode(org_name),
        encode(name)
    );

    let response = http
        .get(&url)
        .bearer_auth(&ctx.login.api_key)
        .send()
        .await
        .context("failed to get project")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("failed to get project ({status}): {body}");
    }

    let list: ListResponse = response
        .json()
        .await
        .context("failed to parse project response")?;

    Ok(list.objects.into_iter().next())
}

async fn create_project(
    http: &Client,
    ctx: &LoginContext,
    org_name: &str,
    name: &str,
) -> Result<Project> {
    let url = format!("{}/v1/project", ctx.api_url.trim_end_matches('/'));
    let body = serde_json::json!({ "name": name, "org_name": org_name });

    let response = http
        .post(&url)
        .bearer_auth(&ctx.login.api_key)
        .json(&body)
        .send()
        .await
        .context("failed to create project")?;

    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        bail!("failed to create project ({status}): {body}");
    }

    response
        .json()
        .await
        .context("failed to parse create project response")
}
