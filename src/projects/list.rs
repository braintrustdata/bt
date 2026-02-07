use anyhow::Result;
use dialoguer::console;

use crate::http::ApiClient;
use crate::ui::{header, styled_table, with_spinner};

use super::api;

pub async fn run(client: &ApiClient, org_name: &str, json: bool) -> Result<()> {
    let projects = with_spinner("Loading projects...", api::list_projects(client)).await?;

    if json {
        println!("{}", serde_json::to_string(&projects)?);
    } else {
        println!(
            "{} projects found in {}\n",
            console::style(projects.len()),
            console::style(org_name).bold()
        );

        let mut table = styled_table();
        table.set_header(vec![header("Name"), header("Description")]);

        for project in &projects {
            let desc = project
                .description
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("-");
            table.add_row(vec![&project.name, desc]);
        }

        println!("{table}");
    }

    Ok(())
}
