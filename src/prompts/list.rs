use anyhow::Result;
use dialoguer::console;

use crate::{
    http::ApiClient,
    ui::{header, styled_table, with_spinner},
    utils::pluralize,
};

use super::api;

pub async fn run(client: &ApiClient, project: &str, org: &str, json: bool) -> Result<()> {
    let prompts = with_spinner("Loading prompts...", api::list_prompts(client, project)).await?;

    if json {
        println!("{}", serde_json::to_string(&prompts)?);
    } else {
        let count = format!(
            "{} {}",
            prompts.len(),
            pluralize(prompts.len(), "prompt", None)
        );
        println!(
            "{} found in {} {} {}\n",
            console::style(count),
            console::style(org).bold(),
            console::style("/").dim().bold(),
            console::style(project).bold()
        );

        let mut table = styled_table();
        table.set_header(vec![header("Name"), header("Description"), header("Slug")]);

        for prompt in &prompts {
            let desc = prompt
                .description
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("-");
            table.add_row(vec![&prompt.name, desc, &prompt.slug]);
        }

        println!("{table}");
    }

    Ok(())
}
