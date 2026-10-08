use std::collections::HashSet;
use std::path::Path;

use anyhow::{bail, Context, Result};
use dialoguer::{theme::ColorfulTheme, MultiSelect};

use crate::{
    args::BaseArgs,
    functions::api::list_all_functions,
    functions::api::Function,
    project_context::resolve_project_command_context_with_auth_mode,
    topics::api::{list_project_automations, ProjectAutomation},
    ui::{self, print_command_status, with_spinner, CommandStatus},
    utils::write_json_atomic,
};

use super::{
    template::{
        deduplicate_preprocessors, facet_export_issue, from_remote, retain_topics_dependencies,
        validate, ActiveObservabilityTemplate, AutomationTemplate, FacetTemplate,
    },
    PullArgs,
};

const PUBLIC_PREVIEW_NOTICE: &str = "Observability templates are in public preview and subject to change. Learn more: https://www.braintrust.dev/docs/feature-lifecycle#public-preview";

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
    let mut template = template_for_pull(&functions, &automations, &args.exclude_facet)?;
    if !base.json && !base.no_input && ui::is_interactive() {
        (template.facets, template.automations) = select_resources(
            template.facets,
            template.automations,
            &template.topics_automations,
        )?;
    } else {
        (template.facets, template.automations) =
            filter_active_resources(template.facets, template.automations);
    }
    retain_topics_dependencies(&mut template);
    validate(&template)?;
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
    print_command_status(CommandStatus::Warning, PUBLIC_PREVIEW_NOTICE);
    Ok(())
}

fn template_for_pull(
    functions: &[Function],
    automations: &[ProjectAutomation],
    excluded: &[String],
) -> Result<ActiveObservabilityTemplate> {
    if excluded.is_empty() {
        return from_remote(functions, automations);
    }
    let slugs = functions
        .iter()
        .filter(|function| function.function_type.as_deref() == Some("facet"))
        .map(|function| function.slug.as_str())
        .collect::<HashSet<_>>();
    for slug in excluded {
        if !slugs.contains(slug.as_str()) {
            bail!("--exclude-facet '{slug}' does not match a facet slug in this project; use an exact facet slug");
        }
    }
    let excluded = excluded.iter().map(String::as_str).collect::<HashSet<_>>();
    let functions = functions
        .iter()
        .filter(|function| {
            function.function_type.as_deref() != Some("facet")
                || !excluded.contains(function.slug.as_str())
        })
        .cloned()
        .collect::<Vec<_>>();
    from_remote(&functions, automations)
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
    topics: &[AutomationTemplate],
) -> Result<(Vec<FacetTemplate>, Vec<AutomationTemplate>)> {
    if facets.is_empty() && automations.is_empty() {
        return Ok((facets, automations));
    }
    let labels = facets
        .iter()
        .map(|facet| facet_label(facet, topics))
        .chain(
            automations
                .iter()
                .map(|automation| label("Automation", &automation.name, automation.active())),
        )
        .collect::<Vec<_>>();
    let defaults = facets
        .iter()
        .map(|facet| facet_default(facet, topics))
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

fn facet_default(facet: &FacetTemplate, topics: &[AutomationTemplate]) -> bool {
    facet.active() && facet_export_issue(facet, topics).is_none()
}

fn facet_label(facet: &FacetTemplate, topics: &[AutomationTemplate]) -> String {
    let label = label("Facet", &facet.name, facet.active());
    match facet_export_issue(facet, topics) {
        Some(reason) => format!("{label} (cannot export: {reason})"),
        None => label,
    }
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
    fn active_observability_selection_filters_both_resource_types() {
        let template = template();
        let (facets, automations) = filter_resources(template.facets, template.automations, &[1]);
        assert!(facets.is_empty());
        assert_eq!(automations.len(), 1);
    }

    #[test]
    fn active_observability_experiment_scope_error_explains_selection_recovery() {
        let mut template = template();
        template.facets[0].topics_automation = Some("Test experiment Topics".to_string());
        template.topics_automations.push(AutomationTemplate {
            name: "Test experiment Topics".to_string(),
            description: None,
            config: json!({"event_type": "topic", "data_scope": {"type": "experiment", "experiment_id": "test-experiment-id"}}),
        });
        let error = validate(&template).unwrap_err().to_string();
        assert!(error.contains("Test facet"));
        assert!(error.contains("Test experiment Topics"));
        assert!(error.contains("deselect these facets"));
        assert!(error.contains("--exclude-facet test-facet"));

        template.facets.push(FacetTemplate {
            name: "Portable test facet".to_string(),
            slug: "test-portable-facet".to_string(),
            topics_automation: None,
            ..template.facets[0].clone()
        });
        (template.facets, template.automations) =
            filter_resources(template.facets, template.automations, &[1]);
        retain_topics_dependencies(&mut template);
        validate(&template).unwrap();
        assert_eq!(template.facets.len(), 1);
        assert!(template.topics_automations.is_empty());
    }

    fn mixed_project() -> (Vec<Function>, Vec<ProjectAutomation>) {
        let functions = [
            ("test-portable-facet", json!({"type": "facet"})),
            ("test-experiment-facet", json!({"type": "facet"})),
            (
                "test-bundle-facet",
                json!({"type": "code", "data": {"type": "bundle", "bundle_id": "test-bundle-id"}}),
            ),
        ]
        .into_iter()
        .map(|(slug, data)| {
            serde_json::from_value(json!({
                "id": format!("fn-{slug}"), "slug": slug, "name": slug,
                "project_id": "test-project-id", "function_type": "facet", "function_data": data
            }))
            .unwrap()
        })
        .collect();
        let automations = vec![
            ProjectAutomation {
                id: "test-topics-id".to_string(),
                project_id: "test-project-id".to_string(),
                name: "Test Topics".to_string(),
                description: None,
                config: json!({"event_type": "topic", "facet_functions": [
                    {"type": "function", "id": "fn-test-portable-facet"}, {"type": "function", "id": "fn-test-bundle-facet"}
                ]}),
            },
            ProjectAutomation {
                id: "test-experiment-topics-id".to_string(),
                project_id: "test-project-id".to_string(),
                name: "Test experiment Topics".to_string(),
                description: None,
                config: json!({"event_type": "topic", "data_scope": {"type": "experiment", "experiment_id": "test-experiment-id"},
                "facet_functions": [{"type": "function", "id": "fn-test-experiment-facet"}]}),
            },
        ];
        (functions, automations)
    }

    #[test]
    fn active_observability_scripted_pull_excludes_unsupported_facets() {
        let (functions, automations) = mixed_project();
        let mut template = template_for_pull(
            &functions,
            &automations,
            &[
                "test-experiment-facet".to_string(),
                "test-bundle-facet".to_string(),
            ],
        )
        .unwrap();
        (template.facets, template.automations) =
            filter_active_resources(template.facets, template.automations);
        retain_topics_dependencies(&mut template);
        validate(&template).unwrap();
        let output: serde_json::Value =
            serde_json::from_str(&serialize_stdout(&template).unwrap()).unwrap();
        assert_eq!(output["facets"].as_array().unwrap().len(), 1);
        assert_eq!(output["facets"][0]["slug"], "test-portable-facet");
        assert_eq!(template.topics_automations.len(), 1);
        let error = template_for_pull(
            &functions,
            &automations,
            &["test-unknown-facet".to_string()],
        )
        .unwrap_err();
        assert!(error.to_string().contains("does not match a facet slug"));
    }

    #[test]
    fn active_observability_picker_marks_unsupported_facets_before_selection() {
        let (functions, automations) = mixed_project();
        let mut template = template_for_pull(&functions, &automations, &[]).unwrap();
        let selected = template
            .facets
            .iter()
            .enumerate()
            .filter_map(|(index, facet)| {
                let label = facet_label(facet, &template.topics_automations);
                if facet.slug == "test-portable-facet" {
                    assert!(!label.contains("cannot export"));
                    assert!(facet_default(facet, &template.topics_automations));
                    Some(index)
                } else {
                    assert!(label.contains(if facet.slug == "test-bundle-facet" {
                        "bundled code"
                    } else {
                        "specific experiment scope"
                    }));
                    assert!(!facet_default(facet, &template.topics_automations));
                    None
                }
            })
            .collect::<Vec<_>>();
        (template.facets, template.automations) =
            filter_resources(template.facets, template.automations, &selected);
        retain_topics_dependencies(&mut template);
        validate(&template).unwrap();
        assert_eq!(template.facets.len(), 1);
    }

    #[test]
    fn active_observability_active_defaults_exclude_inactive_resources() {
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
