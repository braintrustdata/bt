use std::fmt::Write as _;

use anyhow::Result;
use dialoguer::console;

use crate::ui::prompt_render::{render_options, render_prompt_block};
use crate::ui::{print_command_status, print_with_pager, CommandStatus};
use crate::utils::app_project_url;

use super::{resolve_prompt, ResolvedContext};

pub async fn run(
    ctx: &ResolvedContext,
    slug: Option<&str>,
    version: Option<&str>,
    environment: Option<&str>,
    json: bool,
    web: bool,
    verbose: bool,
) -> Result<()> {
    let project_name = &ctx.project.name;
    let prompt = resolve_prompt(ctx, slug, version, environment, "bt prompts view <slug>").await?;

    if web {
        let url = app_project_url(
            &ctx.app_url,
            ctx.client.org_name(),
            project_name,
            &["prompts", &prompt.id],
        );
        open::that(&url)?;
        print_command_status(CommandStatus::Success, &format!("Opened {url} in browser"));
        return Ok(());
    }

    if json {
        println!("{}", serde_json::to_string(&prompt)?);
        return Ok(());
    }

    let mut output = String::new();

    writeln!(output, "Viewing {}", console::style(&prompt.name).bold())?;
    if let Some(environment) = environment {
        writeln!(
            output,
            "{} {}",
            console::style("Environment:").dim(),
            environment
        )?;
    }
    if let Some(version) = prompt._xact_id.as_deref().or(version) {
        writeln!(
            output,
            "{} {}",
            console::style("Version:").dim(),
            display_version(version)
        )?;
    }

    let options = prompt.prompt_data.as_ref().and_then(|pd| pd.get("options"));

    if let Some(model) = options
        .and_then(|o| o.get("model"))
        .and_then(|m| m.as_str())
    {
        writeln!(output, "{} {}", console::style("Model:").dim(), model)?;
    }

    if verbose {
        if let Some(opts) = options {
            render_options(&mut output, opts)?;
        }
    }

    writeln!(output)?;

    if let Some(prompt_block) = prompt.prompt_data.as_ref().and_then(|pd| pd.get("prompt")) {
        render_prompt_block(&mut output, prompt_block)?;
    }

    print_with_pager(&output)?;
    Ok(())
}

fn display_version(version: &str) -> String {
    if version.len() == 16 && version.chars().all(|c| c.is_ascii_hexdigit()) {
        return version.to_string();
    }

    version
        .parse::<u64>()
        .map(crate::util_cmd::prettify_xact)
        .unwrap_or_else(|_| version.to_string())
}

#[cfg(test)]
mod tests {
    use super::display_version;

    #[test]
    fn display_version_uses_pretty_encoding_for_xact_ids() {
        assert_eq!(display_version("1000192656880881099"), "81cd05ee665fdfb3");
        assert_eq!(display_version("81cd05ee665fdfb3"), "81cd05ee665fdfb3");
        assert_eq!(display_version("1234567890123456"), "1234567890123456");
    }
}
