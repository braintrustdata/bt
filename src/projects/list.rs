use anyhow::Result;
use dialoguer::console;
use unicode_width::UnicodeWidthStr;

use crate::http::ApiClient;
use crate::ui::with_spinner;

use super::api;

pub async fn run(client: &ApiClient, org_name: &str, json: bool) -> Result<()> {
    let projects = with_spinner("Loading projects...", api::list_projects(client)).await?;

    if json {
        println!("{}", serde_json::to_string(&projects)?);
    } else {
        println!(
            "{} projects found in {}\n",
            console::style(&projects.len()),
            console::style(org_name).bold()
        );

        // Calculate column widths
        let name_width = projects
            .iter()
            .map(|p| p.name.width())
            .max()
            .unwrap_or(20)
            .max(20);

        // Print header
        println!(
            "{}  {}",
            console::style(format!("{:width$}", "Project name", width = name_width))
                .dim()
                .bold(),
            console::style("Description").dim().bold()
        );

        // Print rows
        for project in &projects {
            let desc = project
                .description
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("-");
            let padding = name_width - project.name.width();
            println!(
                "{}{:padding$}  {}",
                project.name,
                "",
                desc,
                padding = padding
            );
        }
    }

    Ok(())
}
