use std::fmt::Write as _;
use std::io::IsTerminal;

use anyhow::{anyhow, bail, Result};
use dialoguer::console;
use urlencoding::encode;

use crate::http::ApiClient;
use crate::projects::api::Project;
use crate::ui::{print_command_status, print_with_pager, with_spinner, CommandStatus};

use super::{api, delete};

pub async fn run(
    client: &ApiClient,
    app_url: &str,
    project: &Project,
    org_name: &str,
    name: Option<&str>,
    json: bool,
    web: bool,
) -> Result<()> {
    let project_name = &project.name;
    let experiment = match name {
        Some(n) => with_spinner(
            "Loading experiment...",
            api::get_experiment_by_name(client, project_name, n),
        )
        .await?
        .ok_or_else(|| anyhow!("experiment '{n}' not found"))?,
        None => {
            if !std::io::stdin().is_terminal() {
                bail!("experiment name required. Use: bt experiments view <name>");
            }
            delete::select_experiment_interactive(client, project_name).await?
        }
    };

    if web {
        let url = format!(
            "{}/app/{}/p/{}/experiments/{}",
            app_url.trim_end_matches('/'),
            encode(org_name),
            encode(project_name),
            encode(&experiment.name)
        );
        open::that(&url)?;
        print_command_status(CommandStatus::Success, &format!("Opened {url} in browser"));
        return Ok(());
    }

    if json {
        println!("{}", serde_json::to_string(&experiment)?);
        return Ok(());
    }

    let mut output = String::new();
    writeln!(
        output,
        "Viewing {}",
        console::style(&experiment.name).bold()
    )?;

    if let Some(desc) = &experiment.description {
        if !desc.is_empty() {
            writeln!(output, "{} {}", console::style("Description:").dim(), desc)?;
        }
    }
    if let Some(created) = &experiment.created {
        writeln!(output, "{} {}", console::style("Created:").dim(), created)?;
    }
    if let Some(commit) = &experiment.commit {
        writeln!(output, "{} {}", console::style("Commit:").dim(), commit)?;
    }
    if let Some(dataset_id) = &experiment.dataset_id {
        writeln!(
            output,
            "{} {}",
            console::style("Dataset:").dim(),
            dataset_id
        )?;
    }
    writeln!(
        output,
        "{} {}",
        console::style("Public:").dim(),
        if experiment.public { "yes" } else { "no" }
    )?;
    if let Some(tags) = &experiment.tags {
        if !tags.is_empty() {
            writeln!(
                output,
                "{} {}",
                console::style("Tags:").dim(),
                tags.join(", ")
            )?;
        }
    }

    print_with_pager(&output)?;
    Ok(())
}
