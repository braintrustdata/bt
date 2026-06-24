use std::fmt::Write as _;

use anyhow::Result;
use dialoguer::console;
use urlencoding::encode;

use crate::ui::{print_command_status, print_with_pager, with_spinner, CommandStatus};

use super::delete::resolve_playground;
use super::{api, ResolvedContext};

pub async fn run(
    ctx: &ResolvedContext,
    name: Option<&str>,
    json: bool,
    web: bool,
    verbose: bool,
) -> Result<()> {
    let project_name = &ctx.project.name;

    // Resolve the target playground's summary (we need its name for --web and
    // as the lookup key for the meta action; the action is keyed by name, not
    // id). Fall back to an interactive picker over summaries.
    let summary = resolve_playground(ctx, name, "view").await?;
    let playground_name = summary.name.clone();

    if web {
        let url = format!(
            "{}/app/{}/p/{}/playgrounds/{}",
            ctx.app_url.trim_end_matches('/'),
            encode(ctx.client.org_name()),
            encode(project_name),
            encode(&playground_name)
        );
        open::that(&url)?;
        print_command_status(CommandStatus::Success, &format!("Opened {url} in browser"));
        return Ok(());
    }

    const RENDER_LIMIT: usize = 50;

    // Load the full meta via the getPromptSessionMeta server action (keyed by
    // name). Tasks/runs come back empty here; render_full_view shows the
    // "No runs found." line for that case until a follow-up wires the logs query.
    let full = with_spinner(
        "Loading playground...",
        api::get_full_playground_view(&ctx.client, project_name, &playground_name),
    )
    .await;

    match full {
        Ok(mut view) => {
            // `getPromptSessionMeta` returns no name, so fill in the one we
            // already resolved to keep the terminal/JSON view self-describing.
            view.meta.name = Some(playground_name);
            if json {
                println!("{}", serde_json::to_string(&view)?);
                return Ok(());
            }
            let mut output = String::new();
            render_full_view(&mut output, &view, RENDER_LIMIT, verbose)?;
            print_with_pager(&output)?;
            Ok(())
        }
        Err(e) => {
            print_command_status(
                CommandStatus::Warning,
                &format!("Full view unavailable ({e:#}); showing summary only"),
            );
            if json {
                println!("{}", serde_json::to_string(&summary)?);
                return Ok(());
            }
            let mut output = String::new();
            writeln!(output, "Viewing {}", console::style(&summary.name).bold())?;
            if let Some(created) = summary.created.as_deref().filter(|s| !s.is_empty()) {
                writeln!(output, "{} {}", console::style("Created:").dim(), created)?;
            }
            if let Some(by) = summary.created_by_name.as_deref().filter(|s| !s.is_empty()) {
                writeln!(output, "{} {}", console::style("Created by:").dim(), by)?;
            }
            writeln!(
                output,
                "{} {}",
                console::style("Project:").dim(),
                project_name
            )?;
            print_with_pager(&output)?;
            Ok(())
        }
    }
}

fn render_full_view(
    output: &mut String,
    view: &api::FullPlaygroundView,
    limit: usize,
    verbose: bool,
) -> Result<()> {
    let name = view.meta.name.as_deref().unwrap_or("(unnamed)");
    writeln!(output, "Viewing {}", console::style(name).bold())?;
    if let Some(created) = view.meta.created.as_deref().filter(|s| !s.is_empty()) {
        writeln!(output, "{} {}", console::style("Created:").dim(), created)?;
    }
    if let Some(pid) = view.meta.project_id.as_deref().filter(|s| !s.is_empty()) {
        writeln!(
            output,
            "{} {} ({})",
            console::style("Project:").dim(),
            console::style(pid).dim(),
            view.meta.id
        )?;
    } else {
        writeln!(output, "{} {}", console::style("Id:").dim(), view.meta.id)?;
    }

    if let Some(data) = view.playground_data.as_ref() {
        render_playground_data(output, data)?;
    }

    if !view.tasks.is_empty() {
        writeln!(output, "\n{}", console::style("Tasks").bold())?;
        for (i, task) in view.tasks.iter().take(limit).enumerate() {
            let kind = task
                .function_type
                .as_ref()
                .and_then(|v| v.as_str())
                .unwrap_or("prompt");
            let model = task
                .prompt_data
                .as_ref()
                .and_then(|pd| pd.get("options"))
                .and_then(|o| o.get("model"))
                .and_then(|m| m.as_str())
                .unwrap_or("-");
            writeln!(
                output,
                "{}  [{}] {}",
                console::style(format!("#{}", i + 1)).dim(),
                console::style(kind).green(),
                console::style(format!("model={model}")).dim()
            )?;
            if verbose {
                if let Some(options) = task.prompt_data.as_ref().and_then(|pd| pd.get("options")) {
                    crate::ui::prompt_render::render_options(output, options)?;
                }
            }
            if let Some(prompt_block) = task.prompt_data.as_ref().and_then(|pd| pd.get("prompt")) {
                crate::ui::prompt_render::render_prompt_block(output, prompt_block)?;
            }
        }
    }

    if !view.runs.is_empty() {
        writeln!(output, "\n{}", console::style("Recent runs").bold())?;
        let mut table = crate::ui::styled_table();
        table.set_header(vec![
            crate::ui::header("Run id"),
            crate::ui::header("Status"),
            crate::ui::header("Output"),
            crate::ui::header("Error"),
        ]);
        crate::ui::apply_column_padding(&mut table, (0, 6));
        for run in view.runs.iter().take(limit) {
            let status = if run.error.is_some() { "error" } else { "ok" };
            let out = run
                .output
                .as_ref()
                .map(|o| crate::ui::truncate(&o.to_string(), 60))
                .unwrap_or_else(|| "-".to_string());
            let err = run
                .error
                .as_ref()
                .map(|e| crate::ui::truncate(&e.to_string(), 60))
                .unwrap_or_else(|| "-".to_string());
            table.add_row(vec![&crate::ui::truncate(&run.id, 12), status, &out, &err]);
        }
        write!(output, "{table}")?;
    } else {
        writeln!(output, "\n{}", console::style("No runs found.").dim())?;
    }

    Ok(())
}

fn render_playground_data(output: &mut String, data: &api::PlaygroundData) -> Result<()> {
    let dataset = data
        .dataset_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .map(String::from);
    let scorer_count = data
        .scorers
        .as_ref()
        .and_then(|s| s.as_array())
        .map(|a| a.len());

    if dataset.is_none() && scorer_count.is_none() {
        return Ok(());
    }

    writeln!(output, "\n{}", console::style("Settings").bold())?;
    if let Some(ds) = dataset {
        let version = data
            .dataset_version
            .as_deref()
            .map(|v| format!(" @ {v}"))
            .unwrap_or_default();
        writeln!(
            output,
            "{} {}{}",
            console::style("Dataset:").dim(),
            ds,
            version
        )?;
    }
    if let Some(n) = scorer_count {
        writeln!(output, "{} {}", console::style("Scorers:").dim(), n)?;
    }
    Ok(())
}
