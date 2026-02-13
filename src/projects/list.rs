use std::fmt::Write as _;

use anyhow::Result;
use dialoguer::console;

use crate::http::ApiClient;
use crate::ui::{
    apply_column_padding, header, print_with_pager, styled_table, truncate, with_spinner,
};

use super::api;

pub async fn run(client: &ApiClient, org_name: &str, json: bool) -> Result<()> {
    let projects = with_spinner("Loading projects...", api::list_projects(client)).await?;

    if json {
        println!("{}", serde_json::to_string(&projects)?);
    } else {
        let mut output = String::new();

        writeln!(
            output,
            "{} projects found in {}\n",
            console::style(projects.len()),
            console::style(org_name).bold()
        )?;

        let mut table = styled_table();
        table.set_header(vec![header("Name"), header("Description")]);
        apply_column_padding(&mut table, (0, 6));

        for project in &projects {
            let desc = project
                .description
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(|s| truncate(s, 60))
                .unwrap_or_else(|| "-".to_string());
            table.add_row(vec![&project.name, &desc]);
        }

        write!(output, "{table}")?;
        print_with_pager(&output)?;
    }

    Ok(())
}
