use std::fmt::Write as _;

use anyhow::{anyhow, bail, Result};
use dialoguer::console;
use urlencoding::encode;

use crate::ui::{
    is_interactive, print_command_status, print_with_pager, with_spinner, CommandStatus,
};

use super::{api, ResolvedContext};

pub async fn run(ctx: &ResolvedContext, name: Option<&str>, json: bool, web: bool) -> Result<()> {
    let project_name = &ctx.project.name;
    let experiment = match name {
        Some(n) => with_spinner(
            "Loading experiment...",
            api::get_experiment_by_name(&ctx.client, project_name, n),
        )
        .await?
        .ok_or_else(|| anyhow!("experiment '{n}' not found"))?,
        None => {
            if !is_interactive() {
                bail!("experiment name required. Use: bt experiments view <name>");
            }
            super::select_experiment_interactive(&ctx.client, project_name).await?
        }
    };

    let url = format!(
        "{}/app/{}/p/{}/experiments/{}",
        ctx.app_url.trim_end_matches('/'),
        encode(ctx.client.org_name()),
        encode(project_name),
        encode(&experiment.name)
    );

    if web {
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

    let description = experiment
        .description
        .as_deref()
        .filter(|d| !d.is_empty())
        .or_else(|| {
            experiment
                .metadata
                .as_ref()
                .and_then(|m| m.get("description"))
                .and_then(|d| d.as_str())
                .filter(|d| !d.is_empty())
        });
    if let Some(desc) = description {
        writeln!(output, "{} {}", console::style("Description:").dim(), desc)?;
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

    writeln!(
        output,
        "\n{} {}",
        console::style("View experiment results:").dim(),
        console::style(&url).underlined()
    )?;

    print_with_pager(&output)?;
    Ok(())
}
