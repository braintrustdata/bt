use std::fmt::Write as _;

use anyhow::Result;
use dialoguer::console;

use crate::{
    http::ApiClient,
    projects::api::Project,
    ui::{apply_column_padding, header, print_with_pager, styled_table, truncate, with_spinner},
    utils::pluralize,
};

use super::api;

pub async fn run(client: &ApiClient, project: &Project, org: &str, json: bool) -> Result<()> {
    let project_name = &project.name;
    let experiments = with_spinner(
        "Loading experiments...",
        api::list_experiments(client, project_name),
    )
    .await?;

    if json {
        println!("{}", serde_json::to_string(&experiments)?);
    } else {
        let mut output = String::new();
        let count = format!(
            "{} {}",
            experiments.len(),
            pluralize(experiments.len(), "experiment", None)
        );
        writeln!(
            output,
            "{} found in {} {} {}\n",
            console::style(count),
            console::style(org).bold(),
            console::style("/").dim().bold(),
            console::style(project_name).bold()
        )?;

        let mut table = styled_table();
        table.set_header(vec![
            header("Name"),
            header("Description"),
            header("Created"),
            header("Commit"),
        ]);
        apply_column_padding(&mut table, (0, 6));

        for exp in &experiments {
            let desc = exp
                .description
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(|s| truncate(s, 40))
                .unwrap_or_else(|| "-".to_string());
            let created = exp
                .created
                .as_deref()
                .map(|c| truncate(c, 10))
                .unwrap_or_else(|| "-".to_string());
            let commit = exp
                .commit
                .as_deref()
                .map(|c| truncate(c, 7))
                .unwrap_or_else(|| "-".to_string());
            table.add_row(vec![&exp.name, &desc, &created, &commit]);
        }

        write!(output, "{table}")?;
        print_with_pager(&output)?;
    }
    Ok(())
}
