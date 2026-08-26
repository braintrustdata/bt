use std::collections::HashSet;
use std::path::Path;

use anyhow::{bail, Context, Result};
use dialoguer::{theme::ColorfulTheme, MultiSelect};

use crate::{
    args::BaseArgs,
    functions::api::list_all_functions,
    project_context::resolve_project_command_context_with_auth_mode,
    topics::api::list_project_automations,
    ui::{self, print_command_status, with_spinner, CommandStatus},
    utils::write_json_atomic,
};

use super::{
    template::{
        deduplicate_preprocessors, from_remote, ActiveObservabilityTemplate, AutomationTemplate,
        FacetTemplate,
    },
    PullArgs,
};

pub(crate) async fn run(base: BaseArgs, args: PullArgs) -> Result<()> {
    let ctx = resolve_project_command_context_with_auth_mode(&base, true).await?;
    let (functions, automations) =
        with_spinner("Loading active observability resources...", async {
            tokio::try_join!(
                list_all_functions(&ctx.client, &ctx.project.id),
                list_project_automations(&ctx.client, &ctx.project.id),
            )
        })
        .await?;
    let mut template = from_remote(&functions, &automations)?;
    if !base.json && !base.no_input && ui::is_interactive() {
        (template.facets, template.automations) =
            select_resources(template.facets, template.automations)?;
    } else {
        (template.facets, template.automations) =
            filter_active_resources(template.facets, template.automations);
    }
    // Selection happens first so a selected facet never loses its required definition.
    deduplicate_preprocessors(&mut template.facets);

    write_template(&template, args.output.as_deref(), args.force)?;
    if let Some(path) = args
        .output
        .as_deref()
        .filter(|path| *path != Path::new("-"))
    {
        if base.json {
            println!(
                "{}",
                serde_json::to_string(&serde_json::json!({
                    "kind": "active_observability_template",
                    "status": "pulled",
                    "project": ctx.project.name,
                    "output": path,
                    "facet_count": template.facets.len(),
                    "automation_count": template.automations.len(),
                }))?
            );
        } else {
            print_command_status(
                CommandStatus::Success,
                &format!(
                    "Pulled active observability template from '{}' to {} ({} facets, {} Loop automations)",
                    ctx.project.name,
                    path.display(),
                    template.facets.len(),
                    template.automations.len()
                ),
            );
        }
    }
    Ok(())
}

fn write_template(
    template: &ActiveObservabilityTemplate,
    output: Option<&Path>,
    force: bool,
) -> Result<()> {
    match output {
        Some(path) if path != Path::new("-") => {
            if !force
                && path
                    .try_exists()
                    .with_context(|| format!("failed to check {}", path.display()))?
            {
                bail!(
                    "output file {} already exists; use --force to overwrite it",
                    path.display()
                );
            }
            write_json_atomic(path, template)
        }
        _ => {
            println!("{}", serialize_stdout(template)?);
            Ok(())
        }
    }
}

fn serialize_stdout(template: &ActiveObservabilityTemplate) -> Result<String> {
    serde_json::to_string_pretty(template).context("failed to serialize template")
}

fn select_resources(
    facets: Vec<FacetTemplate>,
    automations: Vec<AutomationTemplate>,
) -> Result<(Vec<FacetTemplate>, Vec<AutomationTemplate>)> {
    if facets.is_empty() && automations.is_empty() {
        return Ok((facets, automations));
    }
    let labels = facets
        .iter()
        .map(|facet| label("Facet", &facet.name, facet.active()))
        .chain(
            automations
                .iter()
                .map(|automation| label("Automation", &automation.name, automation.active())),
        )
        .collect::<Vec<_>>();
    let defaults = facets
        .iter()
        .map(FacetTemplate::active)
        .chain(automations.iter().map(AutomationTemplate::active))
        .collect::<Vec<_>>();
    let term =
        ui::prompt_term().ok_or_else(|| anyhow::anyhow!("interactive mode requires a TTY"))?;
    let selected = MultiSelect::with_theme(&ColorfulTheme::default())
        .with_prompt("Select facets and Loop automations to include")
        .items(&labels)
        .defaults(&defaults)
        .report(false)
        .interact_on(&term)
        .context("failed to select active observability resources")?;
    Ok(filter_resources(facets, automations, &selected))
}

fn label(kind: &str, name: &str, active: bool) -> String {
    format!(
        "{kind:<12}{name}{}",
        if active { "" } else { " (inactive)" }
    )
}

fn filter_resources(
    facets: Vec<FacetTemplate>,
    automations: Vec<AutomationTemplate>,
    selected: &[usize],
) -> (Vec<FacetTemplate>, Vec<AutomationTemplate>) {
    let facet_count = facets.len();
    let selected = selected.iter().copied().collect::<HashSet<_>>();
    let facets = facets
        .into_iter()
        .enumerate()
        .filter_map(|(index, facet)| selected.contains(&index).then_some(facet))
        .collect();
    let automations = automations
        .into_iter()
        .enumerate()
        .filter_map(|(index, automation)| {
            selected
                .contains(&(facet_count + index))
                .then_some(automation)
        })
        .collect();
    (facets, automations)
}

fn filter_active_resources(
    facets: Vec<FacetTemplate>,
    automations: Vec<AutomationTemplate>,
) -> (Vec<FacetTemplate>, Vec<AutomationTemplate>) {
    (
        facets.into_iter().filter(FacetTemplate::active).collect(),
        automations
            .into_iter()
            .filter(AutomationTemplate::active)
            .collect(),
    )
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;
    use crate::observability::template::{KIND, SCHEMA_VERSION};

    fn template() -> ActiveObservabilityTemplate {
        serde_json::from_value(json!({
            "kind": KIND,
            "schema_version": SCHEMA_VERSION,
            "facets": [{
                "name": "Test facet",
                "slug": "test-facet",
                "function_data": {"type": "facet", "prompt": "Classify"}
            }],
            "automations": [{
                "name": "Test Loop",
                "config": {"event_type": "windowed", "window": {}, "loop": {}}
            }]
        }))
        .unwrap()
    }

    #[test]
    fn active_observability_stdout_is_only_pretty_json() {
        let text = serialize_stdout(&template()).expect("serialize stdout");
        let value: serde_json::Value = serde_json::from_str(&text).expect("clean JSON");
        assert_eq!(value["kind"], KIND);
        assert!(!text.contains("Pulled active observability"));
    }

    #[test]
    fn active_observability_selection_filters_both_resource_types() {
        let template = template();
        let (facets, automations) = filter_resources(template.facets, template.automations, &[1]);
        assert!(facets.is_empty());
        assert_eq!(automations.len(), 1);
    }

    #[test]
    fn active_observability_noninteractive_pull_uses_active_defaults() {
        let mut template = template();
        template.facets.push(FacetTemplate {
            name: "Active facet".to_string(),
            slug: "active-facet".to_string(),
            topics_automation: Some("Synthetic Topics".to_string()),
            ..template.facets[0].clone()
        });
        template.automations.push(AutomationTemplate {
            name: "Paused Loop".to_string(),
            description: None,
            config: json!({
                "event_type": "windowed",
                "status": "paused",
                "window": {},
                "loop": {}
            }),
        });

        let (facets, automations) = filter_active_resources(template.facets, template.automations);

        assert_eq!(
            facets
                .iter()
                .map(|facet| facet.name.as_str())
                .collect::<Vec<_>>(),
            ["Active facet"]
        );
        assert_eq!(
            automations
                .iter()
                .map(|automation| automation.name.as_str())
                .collect::<Vec<_>>(),
            ["Test Loop"]
        );
    }
}
