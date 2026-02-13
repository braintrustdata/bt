use std::fmt::Write as _;

use anyhow::Result;
use dialoguer::console;

use crate::{
    http::ApiClient,
    ui::{apply_column_padding, header, print_with_pager, styled_table, truncate, with_spinner},
    utils::pluralize,
};

use super::api;

pub async fn run(client: &ApiClient, project: &str, org: &str, json: bool) -> Result<()> {
    let prompts = with_spinner("Loading prompts...", api::list_prompts(client, project)).await?;

    if json {
        println!("{}", serde_json::to_string(&prompts)?);
    } else {
        let mut output = String::new();

        let count = format!(
            "{} {}",
            prompts.len(),
            pluralize(prompts.len(), "prompt", None)
        );
        writeln!(
            output,
            "{} found in {} {} {}\n",
            console::style(count),
            console::style(org).bold(),
            console::style("/").dim().bold(),
            console::style(project).bold()
        )?;

        let mut table = styled_table();
        table.set_header(vec![header("Name"), header("Description"), header("Slug")]);
        apply_column_padding(&mut table, (0, 6));

        for prompt in &prompts {
            let desc = prompt
                .description
                .as_deref()
                .filter(|s| !s.is_empty())
                .map(|s| truncate(s, 60))
                .unwrap_or_else(|| "-".to_string());
            table.add_row(vec![&prompt.name, &desc, &prompt.slug]);
        }

        write!(output, "{table}")?;
        print_with_pager(&output)?;
    }

    Ok(())
}
