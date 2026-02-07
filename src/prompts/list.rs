use anyhow::Result;
use dialoguer::console;
use unicode_width::UnicodeWidthStr;

use crate::{http::ApiClient, ui::with_spinner, utils::pluralize};

use super::api;

pub async fn run(client: &ApiClient, project: &str, org: &str, json: bool) -> Result<()> {
    let prompts = with_spinner("Loading prompts...", api::list_prompts(client, project)).await?;

    if json {
        println!("{}", serde_json::to_string(&prompts)?);
    } else {
        println!(
            "{} found in {}\n",
            console::style(format!(
                "{} {}",
                &prompts.len(),
                pluralize(&prompts.len(), "prompt", None)
            )),
            &format!(
                "{} {} {}",
                console::style(org).bold(),
                console::style("/").dim().bold(),
                console::style(project).bold()
            )
        );

        let name_width = prompts
            .iter()
            .map(|p| p.name.width())
            .max()
            .unwrap_or(24)
            .max(20);

        let description_width = prompts
            .iter()
            .map(|p| p.description.as_deref().unwrap_or("").width())
            .max()
            .unwrap_or(24)
            .max(32);

        // Table Header
        println!(
            "{}  {}  {}",
            console::style(format!("{:width$}", "Prompt name", width = name_width))
                .dim()
                .bold(),
            console::style(format!(
                "{:width$}",
                "Description",
                width = description_width
            ))
            .dim()
            .bold(),
            console::style("Slug").dim().bold()
        );

        for prompt in &prompts {
            let name_padding = name_width - prompt.name.width();
            let desc = prompt
                .description
                .as_deref()
                .filter(|s| !s.is_empty())
                .unwrap_or("-");
            let desc_padding = description_width - desc.width();
            println!(
                "{}{:np$}  {}{:dp$}  {}",
                prompt.name,
                "",
                desc,
                "",
                prompt.slug,
                np = name_padding,
                dp = desc_padding
            );
        }
    }

    Ok(())
}
