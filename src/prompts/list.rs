use std::fmt::Write as _;

use anyhow::Result;
use dialoguer::console;

use crate::{
    ui::{apply_column_padding, header, print_with_pager, styled_table, truncate, with_spinner},
    utils::pluralize,
};

use super::{api, ResolvedContext};

pub async fn run(ctx: &ResolvedContext, environment: Option<&str>, json: bool) -> Result<()> {
    let project_name = &ctx.project.name;
    let prompts = with_spinner("Loading prompts...", async {
        match environment {
            Some(environment) => {
                api::list_prompts_by_environment(&ctx.client, project_name, environment).await
            }
            None => api::list_prompts(&ctx.client, project_name).await,
        }
    })
    .await?;

    if json {
        println!("{}", serde_json::to_string(&prompts)?);
        return Ok(());
    }

    let mut output = String::new();

    let count = format!(
        "{} {}",
        prompts.len(),
        pluralize(prompts.len(), "prompt", None)
    );
    writeln!(
        output,
        "{} found in {} {} {}{}\n",
        console::style(count),
        console::style(ctx.client.org_name()).bold(),
        console::style("/").dim().bold(),
        console::style(project_name).bold(),
        environment
            .map(|environment| format!(" for environment {}", console::style(environment).bold()))
            .unwrap_or_default()
    )?;

    let mut table = styled_table();
    if environment.is_some() {
        table.set_header(vec![
            header("Name"),
            header("Description"),
            header("Slug"),
            header("Version"),
        ]);
    } else {
        table.set_header(vec![header("Name"), header("Description"), header("Slug")]);
    }
    apply_column_padding(&mut table, (0, 6));

    for prompt in &prompts {
        let desc = prompt
            .description
            .as_deref()
            .filter(|s| !s.is_empty())
            .map(|s| truncate(s, 60))
            .unwrap_or_else(|| "-".to_string());
        if environment.is_some() {
            table.add_row(vec![
                prompt.name.as_str(),
                desc.as_str(),
                prompt.slug.as_str(),
                prompt._xact_id.as_deref().unwrap_or("-"),
            ]);
        } else {
            table.add_row(vec![
                prompt.name.as_str(),
                desc.as_str(),
                prompt.slug.as_str(),
            ]);
        }
    }

    write!(output, "{table}")?;
    print_with_pager(&output)?;
    Ok(())
}
