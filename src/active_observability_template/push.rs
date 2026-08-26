use std::collections::{BTreeMap, HashMap};

use anyhow::{anyhow, bail, Context, Result};
use serde::Serialize;
use serde_json::{json, Value};

use crate::{
    functions::api::{create_function, replace_function, Function},
    http::ApiClient,
    topics::api::{
        create_project_automation, patch_project_automation, replace_project_automation,
        seed_new_topic_automation_cursors, ProjectAutomation,
    },
};

use super::template::{
    add_topics_functions, default_topics_config, embedding_model, is_loop_config, is_topics,
    loop_config_for_target, new_topic_map_request, reconciled_topic_map_request,
    saved_preprocessor_slug, topic_map_slug, with_preprocessor_id, ActiveObservabilityTemplate,
    AutomationTemplate, FacetTemplate, PortableFunction, DEFAULT_TOPICS_DESCRIPTION,
};

#[derive(Debug)]
pub(crate) struct Snapshot {
    pub functions: Vec<Function>,
    pub automations: Vec<ProjectAutomation>,
}

#[derive(Debug)]
pub(crate) struct MutationPlan {
    preprocessors: Vec<FunctionMutation<PortableFunction>>,
    facets: Vec<FacetMutation>,
    topics: BTreeMap<String, TopicsMutation>,
    loops: Vec<LoopMutation>,
    function_ids: HashMap<String, String>,
}

#[derive(Debug)]
struct FunctionMutation<T> {
    template: T,
    existing: Option<Function>,
}

#[derive(Debug)]
struct FacetMutation {
    template: FacetTemplate,
    existing: Option<Function>,
    topic_map: Option<Function>,
    topics_key: String,
}

#[derive(Debug)]
struct LoopMutation {
    template: AutomationTemplate,
    existing: Option<ProjectAutomation>,
}

#[derive(Debug, Clone)]
enum TopicsTarget {
    Existing(ProjectAutomation),
    New(String),
}

#[derive(Debug)]
struct TopicsMutation {
    target: TopicsTarget,
    embedding_model: String,
    config: Value,
}

#[derive(Debug, Serialize)]
pub(crate) struct PushResult {
    pub facets: Vec<PushedResource>,
    pub automations: Vec<PushedResource>,
}

#[derive(Debug, serde::Serialize)]
pub(crate) struct PushedResource {
    pub id: String,
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slug: Option<String>,
}

pub(crate) fn plan(
    template: &ActiveObservabilityTemplate,
    snapshot: Snapshot,
    topics_override: Option<&str>,
    force: bool,
) -> Result<MutationPlan> {
    let functions_by_slug = unique_functions_by_slug(&snapshot.functions)?;
    let functions_by_id = snapshot
        .functions
        .iter()
        .map(|function| (function.id.as_str(), function))
        .collect::<HashMap<_, _>>();
    let automations_by_name = unique_automations_by_name(&snapshot.automations)?;

    let mut preprocessor_templates = BTreeMap::new();
    for facet in &template.facets {
        if let Some(preprocessor) = &facet.preprocessor {
            preprocessor_templates
                .entry(preprocessor.slug.clone())
                .or_insert_with(|| preprocessor.clone());
        }
    }
    let preprocessors = preprocessor_templates
        .into_values()
        .map(|template| {
            let existing = checked_function(
                functions_by_slug.get(template.slug.as_str()).copied(),
                &template.slug,
                "preprocessor",
                "preprocessor",
            )?;
            conflict(existing, "preprocessor", &template.slug, force)?;
            Ok(FunctionMutation {
                template,
                existing: existing.cloned(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let topics_automations = snapshot
        .automations
        .iter()
        .filter(|automation| is_topics(&automation.config))
        .collect::<Vec<_>>();
    let mut topics = BTreeMap::<String, TopicsMutation>::new();
    let mut facets = Vec::with_capacity(template.facets.len());

    for facet in &template.facets {
        let existing = checked_function(
            functions_by_slug.get(facet.slug.as_str()).copied(),
            &facet.slug,
            "facet",
            "facet",
        )?;
        conflict(existing, "facet", &facet.slug, force)?;

        if let Some(slug) = saved_preprocessor_slug(&facet.function_data)? {
            let target = functions_by_slug.get(slug).copied();
            if facet.preprocessor.is_none() && target.is_none() {
                bail!(
                    "facet '{}' references preprocessor '{slug}', but it is not bundled or present in the target project",
                    facet.name
                );
            }
            if let Some(target) = target {
                checked_function(Some(target), slug, "preprocessor", "preprocessor")?;
            }
        }

        let map_slug = topic_map_slug(&facet.slug);
        let topic_map = checked_function(
            functions_by_slug.get(map_slug.as_str()).copied(),
            &map_slug,
            "classifier topic map",
            "classifier",
        )?;
        if let Some(topic_map) = topic_map {
            if topic_map
                .function_data
                .as_ref()
                .and_then(|data| data.get("type"))
                .and_then(Value::as_str)
                != Some("topic_map")
            {
                bail!(
                    "function slug '{map_slug}' is occupied by a classifier that is not a topic map"
                );
            }
        }
        conflict(topic_map, "topic map", &map_slug, force)?;

        let target = resolve_topics_target(
            facet,
            topics_override,
            &snapshot.automations,
            &topics_automations,
            &automations_by_name,
        )?;
        let key = target.key();
        let model = match &target {
            TopicsTarget::Existing(automation) => embedding_model(automation, &functions_by_id),
            TopicsTarget::New(_) => super::template::DEFAULT_EMBEDDING_MODEL.to_string(),
        };
        // Validate destination-owned Topics configuration before any mutation begins.
        let config = match &target {
            TopicsTarget::Existing(automation) => add_topics_functions(&automation.config, &[])?,
            TopicsTarget::New(_) => default_topics_config(),
        };
        topics.entry(key.clone()).or_insert(TopicsMutation {
            target,
            embedding_model: model,
            config,
        });
        facets.push(FacetMutation {
            template: facet.clone(),
            existing: existing.cloned(),
            topic_map: topic_map.cloned(),
            topics_key: key,
        });
    }

    let loops = template
        .automations
        .iter()
        .map(|template| {
            let existing = automations_by_name.get(template.name.as_str()).copied();
            if let Some(existing) = existing {
                if !is_loop_config(&existing.config) {
                    bail!(
                        "automation '{}' already exists but is not a Loop automation",
                        template.name
                    );
                }
                if !force {
                    bail!(
                        "Loop automation '{}' already exists; use --force to replace it",
                        template.name
                    );
                }
            }
            if topics.values().any(|topics| {
                matches!(&topics.target, TopicsTarget::New(name) if name == &template.name)
            }) {
                bail!(
                    "template would create both a Topics and Loop automation named '{}'",
                    template.name
                );
            }
            Ok(LoopMutation {
                template: template.clone(),
                existing: existing.cloned(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    Ok(MutationPlan {
        preprocessors,
        facets,
        topics,
        loops,
        function_ids: snapshot
            .functions
            .into_iter()
            .map(|function| (function.slug, function.id))
            .collect(),
    })
}

fn unique_functions_by_slug(functions: &[Function]) -> Result<HashMap<&str, &Function>> {
    let mut by_slug = HashMap::new();
    for function in functions {
        if by_slug.insert(function.slug.as_str(), function).is_some() {
            bail!("multiple target functions have slug '{}'", function.slug);
        }
    }
    Ok(by_slug)
}

fn unique_automations_by_name(
    automations: &[ProjectAutomation],
) -> Result<HashMap<&str, &ProjectAutomation>> {
    let mut by_name = HashMap::new();
    for automation in automations {
        if by_name
            .insert(automation.name.as_str(), automation)
            .is_some()
        {
            bail!(
                "multiple target automations are named '{}'",
                automation.name
            );
        }
    }
    Ok(by_name)
}

fn checked_function<'a>(
    function: Option<&'a Function>,
    slug: &str,
    label: &str,
    expected_type: &str,
) -> Result<Option<&'a Function>> {
    if let Some(function) = function {
        if function.function_type.as_deref() != Some(expected_type) {
            bail!(
                "function slug '{slug}' is occupied by a '{}' function, not a {label}",
                function.function_type.as_deref().unwrap_or("unknown")
            );
        }
    }
    Ok(function)
}

fn conflict(existing: Option<&Function>, label: &str, slug: &str, force: bool) -> Result<()> {
    if existing.is_some() && !force {
        bail!("{label} with slug '{slug}' already exists; use --force to replace it");
    }
    Ok(())
}

fn resolve_topics_target(
    facet: &FacetTemplate,
    topics_override: Option<&str>,
    all_automations: &[ProjectAutomation],
    topics_automations: &[&ProjectAutomation],
    by_name: &HashMap<&str, &ProjectAutomation>,
) -> Result<TopicsTarget> {
    if let Some(selector) = topics_override {
        if selector.trim().is_empty() {
            bail!("--topics-automation must not be empty");
        }
        let matches = all_automations
            .iter()
            .filter(|automation| automation.id == selector || automation.name == selector)
            .collect::<Vec<_>>();
        let automation = match matches.as_slice() {
            [] => bail!(
                "Topics automation '{selector}' was not found; use an exact name or ID with --topics-automation"
            ),
            [automation] => *automation,
            _ => bail!(
                "--topics-automation '{selector}' is ambiguous; use the exact automation ID"
            ),
        };
        if !is_topics(&automation.config) {
            bail!("automation '{selector}' is not a Topics automation");
        }
        return Ok(TopicsTarget::Existing(automation.clone()));
    }

    if let Some(name) = facet.topics_automation.as_deref() {
        return match by_name.get(name).copied() {
            Some(automation) if is_topics(&automation.config) => {
                Ok(TopicsTarget::Existing(automation.clone()))
            }
            Some(_) => bail!(
                "automation '{name}' exists but is not a Topics automation; use a different mapping"
            ),
            None => Ok(TopicsTarget::New(name.to_string())),
        };
    }

    match topics_automations {
        [automation] => Ok(TopicsTarget::Existing((*automation).clone())),
        [] => bail!(
            "no Topics automation can be inferred for facet '{}'; use --topics-automation <NAME_OR_ID>",
            facet.name
        ),
        _ => bail!(
            "multiple Topics automations exist for facet '{}'; use --topics-automation <NAME_OR_ID>",
            facet.name
        ),
    }
}

impl TopicsTarget {
    fn key(&self) -> String {
        match self {
            Self::Existing(automation) => format!("id:{}", automation.id),
            Self::New(name) => format!("new:{name}"),
        }
    }
}

pub(crate) async fn execute(
    client: &ApiClient,
    project_id: &str,
    mut plan: MutationPlan,
) -> Result<PushResult> {
    for mutation in &plan.preprocessors {
        let request = mutation.template.request(project_id, "preprocessor");
        let pushed = upsert_function(client, &request, mutation.existing.is_some())
            .await
            .with_context(|| format!("failed to push preprocessor '{}'", mutation.template.slug))?;
        plan.function_ids.insert(pushed.slug, pushed.id);
    }

    let mut topic_functions: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
    let mut pushed_facets = Vec::with_capacity(plan.facets.len());
    for mutation in &plan.facets {
        let preprocessor_id = saved_preprocessor_slug(&mutation.template.function_data)?
            .map(|slug| {
                plan.function_ids
                    .get(slug)
                    .map(String::as_str)
                    .ok_or_else(|| {
                        anyhow!("preprocessor '{slug}' disappeared from the mutation plan")
                    })
            })
            .transpose()?;
        let function_data =
            with_preprocessor_id(&mutation.template.function_data, preprocessor_id)?;
        let request = mutation.template.request(project_id, &function_data);
        let facet = upsert_function(client, &request, mutation.existing.is_some())
            .await
            .with_context(|| format!("failed to push facet '{}'", mutation.template.slug))?;

        let topic_map = match &mutation.topic_map {
            Some(existing) => {
                let request =
                    reconciled_topic_map_request(existing, &mutation.template, &facet.id)?;
                replace_function(client, &request)
                    .await
                    .with_context(|| format!("failed to reconcile topic map '{}'", existing.slug))?
            }
            None => {
                let model = &plan
                    .topics
                    .get(&mutation.topics_key)
                    .expect("facet Topics plan exists")
                    .embedding_model;
                let request =
                    new_topic_map_request(project_id, &mutation.template, &facet.id, model);
                create_function(client, &request).await.with_context(|| {
                    format!(
                        "failed to create topic map '{}'",
                        topic_map_slug(&mutation.template.slug)
                    )
                })?
            }
        };
        topic_functions
            .entry(mutation.topics_key.clone())
            .or_default()
            .push((facet.id.clone(), topic_map.id));
        pushed_facets.push(PushedResource {
            id: facet.id,
            name: facet.name,
            slug: Some(facet.slug),
        });
    }

    for (key, mutation) in &plan.topics {
        let pairs = topic_functions
            .get(key)
            .map(Vec::as_slice)
            .unwrap_or_default();
        let config = add_topics_functions(&mutation.config, pairs)?;
        match &mutation.target {
            TopicsTarget::Existing(automation) => {
                let body = json!({
                    "name": automation.name,
                    "description": automation.description,
                    "config": config,
                });
                patch_project_automation(client, &automation.id, &body)
                    .await
                    .with_context(|| {
                        format!("failed to update Topics automation '{}'", automation.name)
                    })?;
            }
            TopicsTarget::New(name) => {
                let body = json!({
                    "project_id": project_id,
                    "name": name,
                    "description": DEFAULT_TOPICS_DESCRIPTION,
                    "config": config,
                });
                let created = create_project_automation(client, &body)
                    .await
                    .with_context(|| format!("failed to create Topics automation '{name}'"))?;
                seed_new_topic_automation_cursors(client, project_id, &created)
                    .await
                    .with_context(|| format!("failed to seed Topics automation '{name}'"))?;
            }
        }
    }

    let mut pushed_loops = Vec::with_capacity(plan.loops.len());
    for mutation in &plan.loops {
        let existing_config = mutation.existing.as_ref().map(|row| &row.config);
        let config = loop_config_for_target(&mutation.template.config, existing_config)?;
        let pushed = if mutation.existing.is_some() {
            let body = json!({
                "project_id": project_id,
                "name": mutation.template.name,
                "description": mutation.template.description,
                "config": config,
            });
            replace_project_automation(client, &body)
                .await
                .with_context(|| {
                    format!(
                        "failed to replace Loop automation '{}'",
                        mutation.template.name
                    )
                })?
        } else {
            let body = json!({
                "project_id": project_id,
                "name": mutation.template.name,
                "description": mutation.template.description,
                "config": config,
            });
            create_project_automation(client, &body)
                .await
                .with_context(|| {
                    format!(
                        "failed to create Loop automation '{}'",
                        mutation.template.name
                    )
                })?
        };
        pushed_loops.push(PushedResource {
            id: pushed.id,
            name: pushed.name,
            slug: None,
        });
    }

    Ok(PushResult {
        facets: pushed_facets,
        automations: pushed_loops,
    })
}

async fn upsert_function(client: &ApiClient, request: &Value, replace: bool) -> Result<Function> {
    if replace {
        replace_function(client, request).await
    } else {
        create_function(client, request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::active_observability_template::template::{validate, KIND, SCHEMA_VERSION};

    fn facet(topics: Option<&str>) -> FacetTemplate {
        FacetTemplate {
            name: "Test facet".to_string(),
            slug: "test-facet".to_string(),
            topics_automation: topics.map(str::to_string),
            description: None,
            preprocessor: None,
            function_data: json!({"type": "facet", "prompt": "Classify this trace"}),
            prompt_data: None,
            tags: None,
            function_schema: None,
        }
    }

    fn template(facet: FacetTemplate) -> ActiveObservabilityTemplate {
        ActiveObservabilityTemplate {
            kind: KIND.to_string(),
            schema_version: SCHEMA_VERSION,
            facets: vec![facet],
            automations: Vec::new(),
        }
    }

    fn automation(id: &str, name: &str, event_type: &str) -> ProjectAutomation {
        ProjectAutomation {
            id: id.to_string(),
            project_id: "test-project-id".to_string(),
            name: name.to_string(),
            description: None,
            config: json!({
                "event_type": event_type,
                "facet_functions": [],
                "topic_map_functions": []
            }),
        }
    }

    fn existing_function(slug: &str, function_type: &str, data_type: &str) -> Function {
        Function {
            id: format!("fn-{slug}"),
            name: slug.to_string(),
            slug: slug.to_string(),
            project_id: "test-project-id".to_string(),
            description: None,
            function_type: Some(function_type.to_string()),
            prompt_data: None,
            function_data: Some(json!({"type": data_type})),
            tags: None,
            function_schema: None,
            metadata: None,
            created: None,
            _xact_id: None,
        }
    }

    #[test]
    fn active_observability_plans_topics_selection_rules() {
        let only = automation("auto-topics", "Topics", "topic");
        let inferred = plan(
            &template(facet(None)),
            Snapshot {
                functions: vec![],
                automations: vec![only.clone()],
            },
            None,
            false,
        )
        .expect("single Topics automation");
        assert!(inferred.topics.contains_key("id:auto-topics"));

        let missing = plan(
            &template(facet(None)),
            Snapshot {
                functions: vec![],
                automations: vec![],
            },
            None,
            false,
        )
        .expect_err("missing selector");
        assert!(missing.to_string().contains("--topics-automation"));

        let multiple = plan(
            &template(facet(None)),
            Snapshot {
                functions: vec![],
                automations: vec![only.clone(), automation("auto-other", "Other", "topic")],
            },
            None,
            false,
        )
        .expect_err("ambiguous selector");
        assert!(multiple.to_string().contains("multiple Topics"));

        let selected = plan(
            &template(facet(None)),
            Snapshot {
                functions: vec![],
                automations: vec![only, automation("auto-other", "Other", "topic")],
            },
            Some("auto-other"),
            false,
        )
        .expect("CLI override");
        assert!(selected.topics.contains_key("id:auto-other"));
    }

    #[test]
    fn active_observability_named_topics_mapping_can_plan_one_creation() {
        let mut source = template(facet(Some("Synthetic Topics")));
        source.facets.push(FacetTemplate {
            slug: "second-facet".to_string(),
            name: "Second facet".to_string(),
            ..facet(Some("Synthetic Topics"))
        });
        validate(&source).expect("valid template");
        let plan = plan(
            &source,
            Snapshot {
                functions: vec![],
                automations: vec![],
            },
            None,
            false,
        )
        .expect("one new destination");
        assert_eq!(plan.topics.len(), 1);
        assert_eq!(plan.facets.len(), 2);
    }

    #[test]
    fn active_observability_preflight_rejects_type_and_no_force_conflicts() {
        let wrong = plan(
            &template(facet(Some("Topics"))),
            Snapshot {
                functions: vec![existing_function("test-facet", "tool", "code")],
                automations: vec![automation("auto-topics", "Topics", "topic")],
            },
            None,
            true,
        )
        .expect_err("wrong type");
        assert!(wrong.to_string().contains("not a facet"));

        let no_force = plan(
            &template(facet(Some("Topics"))),
            Snapshot {
                functions: vec![existing_function("test-facet", "facet", "facet")],
                automations: vec![automation("auto-topics", "Topics", "topic")],
            },
            None,
            false,
        )
        .expect_err("no force conflict");
        assert!(no_force.to_string().contains("--force"));

        let wrong_loop = plan(
            &ActiveObservabilityTemplate {
                kind: KIND.to_string(),
                schema_version: SCHEMA_VERSION,
                facets: Vec::new(),
                automations: vec![AutomationTemplate {
                    name: "Test automation".to_string(),
                    description: None,
                    config: json!({"event_type": "windowed", "window": {}, "loop": {}}),
                }],
            },
            Snapshot {
                functions: vec![],
                automations: vec![automation("auto-test", "Test automation", "topic")],
            },
            None,
            true,
        )
        .expect_err("wrong automation type");
        assert!(wrong_loop.to_string().contains("not a Loop"));
    }

    #[test]
    fn active_observability_preflight_rejects_malformed_topics_config() {
        let mut topics = automation("auto-topics", "Topics", "topic");
        topics.config["facet_functions"] = json!("not-an-array");

        let error = plan(
            &template(facet(Some("Topics"))),
            Snapshot {
                functions: vec![],
                automations: vec![topics],
            },
            None,
            false,
        )
        .expect_err("malformed Topics config");

        assert!(error
            .to_string()
            .contains("Topics automation facet_functions must be an array"));
    }
}
