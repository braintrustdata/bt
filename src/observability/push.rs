use std::collections::{BTreeMap, HashMap, HashSet};

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
    remove_topics_functions, saved_preprocessor_slug, settings_equal, topic_map_ids,
    topic_map_matches, topic_map_order, topic_map_slug, topics_config_for_target,
    with_preprocessor_id, ActiveObservabilityTemplate, AutomationTemplate, FacetTemplate,
    PortableFunction, TopicMapFilter, DEFAULT_TOPICS_DESCRIPTION,
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
    detached_topic_maps: Vec<PushedResource>,
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
    filter: TopicMapFilter,
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
    description: Option<String>,
}

impl TopicsMutation {
    fn request(
        &self,
        project_id: &str,
        functions: &[(String, String, TopicMapFilter)],
    ) -> Result<Value> {
        let name = match &self.target {
            TopicsTarget::Existing(automation) => &automation.name,
            TopicsTarget::New(name) => name,
        };
        let mut body = json!({
            "name": name,
            "description": self.description,
            "config": add_topics_functions(&self.config, functions)?,
        });
        if matches!(self.target, TopicsTarget::New(_)) {
            body["project_id"] = json!(project_id);
        }
        Ok(body)
    }
}

#[derive(Debug, Serialize)]
pub(crate) struct PushResult {
    pub facets: Vec<PushedResource>,
    pub automations: Vec<PushedResource>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub detached_topic_maps: Vec<PushedResource>,
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
    let imports = template.facet_imports(topics_override.is_some())?;
    let functions_by_slug = functions_by_slug(&snapshot.functions);
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
    let bundled_preprocessor_slugs = preprocessor_templates
        .keys()
        .cloned()
        .collect::<HashSet<_>>();
    let preprocessors = preprocessor_templates
        .into_values()
        .map(|template| {
            let existing = checked_function_by_slug(
                &functions_by_slug,
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
    let mut settings_by_target = BTreeMap::new();
    let mut duplicates_by_facet = HashMap::new();
    let mut detached_topic_maps = BTreeMap::new();
    let mut facets = Vec::with_capacity(template.facets.len());

    // Resolve destinations before changing settings or wiring.
    for import in imports {
        let facet = import.facet;
        let existing = checked_function_by_slug(&functions_by_slug, &facet.slug, "facet", "facet")?;
        conflict(existing, "facet", &facet.slug, force)?;

        if let Some(slug) = saved_preprocessor_slug(&facet.function_data)? {
            let target =
                checked_function_by_slug(&functions_by_slug, slug, "preprocessor", "preprocessor")?;
            if !bundled_preprocessor_slugs.contains(slug) && target.is_none() {
                bail!(
                    "facet '{}' references preprocessor '{slug}', but it is not bundled or present in the target project",
                    facet.name
                );
            }
        }

        let map_slug = topic_map_slug(&facet.slug);
        let (topic_map, duplicate_topic_maps) = existing_topic_map(
            existing,
            &topics_automations,
            &functions_by_id,
            &functions_by_slug,
            &map_slug,
            force,
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
        conflict(
            topic_map,
            "topic map",
            topic_map.map_or(map_slug.as_str(), |function| function.slug.as_str()),
            force,
        )?;

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
        let config = match &target {
            TopicsTarget::Existing(automation) => add_topics_functions(&automation.config, &[])?,
            TopicsTarget::New(_) => default_topics_config(),
        };
        let description = match &target {
            TopicsTarget::Existing(automation) => automation.description.clone(),
            TopicsTarget::New(_) => Some(DEFAULT_TOPICS_DESCRIPTION.to_string()),
        };
        topics.entry(key.clone()).or_insert(TopicsMutation {
            target,
            embedding_model: model,
            config,
            description,
        });
        if let Some(settings) = import.settings {
            settings_by_target.insert(key.clone(), settings);
        }
        for map in &duplicate_topic_maps {
            detached_topic_maps
                .entry(map.id.clone())
                .or_insert_with(|| PushedResource {
                    id: map.id.clone(),
                    name: map.name.clone(),
                    slug: Some(map.slug.clone()),
                });
        }
        duplicates_by_facet.insert(facet.slug.clone(), duplicate_topic_maps);
        facets.push(FacetMutation {
            template: facet.clone(),
            existing: existing.cloned(),
            topic_map: topic_map.cloned(),
            topics_key: key,
            filter: import.filter,
        });
    }

    // Decide settings once per destination, retaining destination wiring.
    for (key, settings) in settings_by_target {
        let mutation = topics.get_mut(&key).expect("resolved Topics destination");
        let config = topics_config_for_target(&settings.config, &mutation.config)?;
        if let TopicsTarget::Existing(existing) = &mutation.target {
            if !force
                && (!settings_equal(&config, &mutation.config)
                    || settings.description != existing.description)
            {
                bail!("Topics automation '{}' has different settings; use --force to replace its settings or --topics-automation to use the destination settings", settings.name);
            }
        }
        mutation.config = config;
        mutation.description = settings.description.clone();
    }

    // Remove old wiring only after every destination's settings are decided.
    // Additions use the function IDs returned by the API during execution.
    for facet in &facets {
        plan_topics_removals(
            &mut topics,
            &topics_automations,
            &facet.topics_key,
            facet.existing.as_ref(),
            facet.topic_map.as_ref(),
            &duplicates_by_facet[&facet.template.slug],
            &functions_by_id,
        )?;
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
        detached_topic_maps: detached_topic_maps.into_values().collect(),
        function_ids: snapshot
            .functions
            .into_iter()
            .filter(|function| function.function_type.as_deref() == Some("preprocessor"))
            .map(|function| (function.slug, function.id))
            .collect(),
    })
}

fn plan_topics_removals(
    topics: &mut BTreeMap<String, TopicsMutation>,
    automations: &[&ProjectAutomation],
    selected_key: &str,
    facet: Option<&Function>,
    topic_map: Option<&Function>,
    duplicate_topic_maps: &[&Function],
    functions_by_id: &HashMap<&str, &Function>,
) -> Result<()> {
    if facet.is_none() && topic_map.is_none() && duplicate_topic_maps.is_empty() {
        return Ok(());
    }

    for &automation in automations {
        let key = TopicsTarget::Existing(automation.clone()).key();
        let selected = key == selected_key;
        let mut config = topics
            .get(&key)
            .map(|mutation| mutation.config.clone())
            .unwrap_or_else(|| automation.config.clone());
        let mut changed = false;
        let facet_to_remove = (!selected)
            .then(|| facet.map(|facet| facet.id.as_str()))
            .flatten();
        let topic_map_to_remove = (!selected)
            .then(|| topic_map.map(|topic_map| topic_map.id.as_str()))
            .flatten();

        if let Some(updated) =
            remove_topics_functions(&config, facet_to_remove, topic_map_to_remove)?
        {
            config = updated;
            changed = true;
        }
        for duplicate in duplicate_topic_maps {
            if let Some(updated) =
                remove_topics_functions(&config, None, Some(duplicate.id.as_str()))?
            {
                config = updated;
                changed = true;
            }
        }
        if !changed {
            continue;
        }

        match topics.get_mut(&key) {
            Some(mutation) => mutation.config = config,
            None => {
                topics.insert(
                    key,
                    TopicsMutation {
                        target: TopicsTarget::Existing(automation.clone()),
                        embedding_model: embedding_model(automation, functions_by_id),
                        config,
                        description: automation.description.clone(),
                    },
                );
            }
        }
    }
    Ok(())
}

fn functions_by_slug(functions: &[Function]) -> HashMap<&str, Vec<&Function>> {
    let mut by_slug = HashMap::new();
    for function in functions {
        by_slug
            .entry(function.slug.as_str())
            .or_insert_with(Vec::new)
            .push(function);
    }
    by_slug
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

fn checked_function_by_slug<'a>(
    functions_by_slug: &HashMap<&str, Vec<&'a Function>>,
    slug: &str,
    label: &str,
    expected_type: &str,
) -> Result<Option<&'a Function>> {
    let Some(functions) = functions_by_slug.get(slug) else {
        return Ok(None);
    };
    let mut matching = functions
        .iter()
        .copied()
        .filter(|function| function.function_type.as_deref() == Some(expected_type));
    let Some(function) = matching.next() else {
        let actual_type = functions
            .first()
            .and_then(|function| function.function_type.as_deref())
            .unwrap_or("unknown");
        bail!("function slug '{slug}' is occupied by a '{actual_type}' function, not a {label}");
    };
    if matching.next().is_some() {
        bail!("multiple target {label} functions have slug '{slug}'");
    }
    Ok(Some(function))
}

fn existing_topic_map<'a>(
    facet: Option<&Function>,
    topics_automations: &[&ProjectAutomation],
    functions_by_id: &HashMap<&str, &'a Function>,
    functions_by_slug: &HashMap<&str, Vec<&'a Function>>,
    fallback_slug: &str,
    force: bool,
) -> Result<(Option<&'a Function>, Vec<&'a Function>)> {
    if let Some(facet) = facet {
        let mut matches = BTreeMap::new();
        for automation in topics_automations {
            for id in topic_map_ids(&automation.config) {
                let Some(topic_map) = functions_by_id.get(id).copied() else {
                    continue;
                };
                if topic_map.function_type.as_deref() == Some("classifier")
                    && topic_map
                        .function_data
                        .as_ref()
                        .and_then(|data| data.get("type"))
                        .and_then(Value::as_str)
                        == Some("topic_map")
                    && topic_map_matches(topic_map, facet)
                {
                    matches.insert(topic_map.id.as_str(), topic_map);
                }
            }
        }
        if matches.len() == 1 {
            return Ok((matches.into_values().next(), Vec::new()));
        }
        if matches.len() > 1 {
            if !force {
                bail!(
                    "facet '{}' is wired to multiple topic maps in the target project; use --force to preserve the oldest map and detach the duplicates",
                    facet.slug
                );
            }

            let mut matches = matches.into_values().collect::<Vec<_>>();
            // Older template-push versions could append a newly generated map without
            // noticing an already-wired map with a different slug. Preserve the oldest
            // map so existing reports and destination-owned customization survive.
            matches.sort_by(|left, right| topic_map_order(left, right));
            let selected = matches.remove(0);
            return Ok((Some(selected), matches));
        }
    }

    Ok((
        checked_function_by_slug(
            functions_by_slug,
            fallback_slug,
            "classifier topic map",
            "classifier",
        )?,
        Vec::new(),
    ))
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

    let mut topic_functions: BTreeMap<String, Vec<(String, String, TopicMapFilter)>> =
        BTreeMap::new();
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
            .push((facet.id.clone(), topic_map.id, mutation.filter.clone()));
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
        let body = mutation.request(project_id, pairs)?;
        match &mutation.target {
            TopicsTarget::Existing(automation) => {
                patch_project_automation(client, &automation.id, &body)
                    .await
                    .with_context(|| {
                        format!("failed to update Topics automation '{}'", automation.name)
                    })?;
            }
            TopicsTarget::New(name) => {
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
        detached_topic_maps: plan.detached_topic_maps,
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
    use crate::observability::template::{
        deduplicate_preprocessors, validate, KIND, SCHEMA_VERSION,
    };

    fn facet(topics: Option<&str>) -> FacetTemplate {
        FacetTemplate {
            name: "Test facet".to_string(),
            slug: "test-facet".to_string(),
            topics_automation: topics.map(str::to_string),
            topic_map_btql_filter: None,
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
            schema_version: 1,
            topics_automations: Vec::new(),
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

    fn portable_preprocessor(slug: &str) -> PortableFunction {
        PortableFunction {
            name: "Shared preprocessor".to_string(),
            slug: slug.to_string(),
            description: None,
            function_data: json!({
                "type": "code",
                "data": {"type": "inline", "code": "return input"}
            }),
            prompt_data: None,
            tags: None,
            function_schema: None,
        }
    }

    fn scoped_template() -> ActiveObservabilityTemplate {
        let mut source = template(facet(Some("Test scoped Topics")));
        source.schema_version = SCHEMA_VERSION;
        source.facets[0].topic_map_btql_filter = Some("metadata.test_enabled = true".to_string());
        source.topics_automations.push(AutomationTemplate {
            name: "Test scoped Topics".to_string(),
            description: Some("Classify selected spans".to_string()),
            config: json!({
                "event_type": "topic", "scope": {"type": "span"},
                "btql_filter": "span_attributes.name = 'test-tool'",
                "sampling_rate": 0.25, "facet_model": "brain-facet-2",
                "data_scope": {"type": "project_logs"},
                "rerun_seconds": 7200, "backfill_time_range": "6h",
                "status": "paused"
            }),
        });
        source
    }

    #[test]
    fn active_observability_inline_code_facet_round_trips() {
        use crate::observability::template::from_remote;

        let mut facet = existing_function("test-code-facet", "facet", "code");
        let data = json!({
            "type": "code",
            "data": {
                "type": "inline",
                "runtime_context": {"runtime": "node", "version": "22"},
                "code": "export default (span) => span.metadata.test_value;",
                "code_hash": "test-code-hash"
            }
        });
        facet.function_data = Some(data.clone());
        facet.tags = Some(vec!["test-code-facet".to_string()]);
        let mut topic_map =
            existing_function("test-code-facet-topic-map", "classifier", "topic_map");
        topic_map.function_data = Some(
            json!({"type": "topic_map", "source_facet_function": {"type": "function", "id": facet.id}}),
        );
        let mut automation = automation("test-source-automation", "Test scoped Topics", "topic");
        automation.config = add_topics_functions(
            &scoped_template().topics_automations[0].config,
            &[(
                facet.id.clone(),
                topic_map.id.clone(),
                TopicMapFilter::Replace(Some("metadata.test_enabled = true".to_string())),
            )],
        )
        .unwrap();

        let exported = from_remote(&[facet, topic_map], &[automation]).unwrap();
        let source: ActiveObservabilityTemplate =
            serde_json::from_str(&serde_json::to_string(&exported).unwrap()).unwrap();
        validate(&source).unwrap();
        let planned = plan(
            &source,
            Snapshot {
                functions: vec![],
                automations: vec![],
            },
            None,
            false,
        )
        .unwrap();
        let mutation = &planned.facets[0];
        let request = mutation.template.request(
            "test-destination-project-id",
            &mutation.template.function_data,
        );
        assert_eq!(request["function_type"], "facet");
        assert_eq!(request["function_data"], data);
        assert_eq!(request["tags"], json!(["test-code-facet"]));
        assert_eq!(
            planned.topics["new:Test scoped Topics"].config["scope"],
            json!({"type": "span"})
        );
        assert_eq!(
            mutation.template.topic_map_btql_filter.as_deref(),
            Some("metadata.test_enabled = true")
        );
    }

    #[test]
    fn active_observability_scoped_topics_round_trip() {
        use crate::observability::template::from_remote;

        for scope in [
            json!({"type": "span"}),
            json!({"type": "trace", "idle_seconds": 90}),
            json!({"type": "group", "group_by": "metadata.test_session", "placement": "each", "interval_seconds": 3600, "max_traces": 12}),
        ] {
            let facet = existing_function("test-facet", "facet", "facet");
            let mut topic_map =
                existing_function("test-facet-topic-map", "classifier", "topic_map");
            topic_map.function_data = Some(
                json!({"type": "topic_map", "source_facet_function": {"type": "function", "id": facet.id}}),
            );
            let mut automation =
                automation("test-source-automation", "Test scoped Topics", "topic");
            let mut expected = scoped_template();
            expected.topics_automations[0].config["scope"] = scope;
            automation.description = expected.topics_automations[0].description.clone();
            automation.config = add_topics_functions(
                &expected.topics_automations[0].config,
                &[(
                    facet.id.clone(),
                    topic_map.id.clone(),
                    TopicMapFilter::Replace(expected.facets[0].topic_map_btql_filter.clone()),
                )],
            )
            .unwrap();
            let source = from_remote(&[facet.clone(), topic_map.clone()], &[automation]).unwrap();
            let serialized = serde_json::to_string(&source).unwrap();
            assert!(!serialized.contains(&facet.id));
            assert!(!serialized.contains(&topic_map.id));
            let source: ActiveObservabilityTemplate = serde_json::from_str(&serialized).unwrap();
            validate(&source).unwrap();
            let planned = plan(
                &source,
                Snapshot {
                    functions: vec![],
                    automations: vec![],
                },
                None,
                false,
            )
            .unwrap();
            assert_eq!(planned.topics.len(), 1);
            let topics = &planned.topics["new:Test scoped Topics"];
            let config = add_topics_functions(
                &topics.config,
                &[(
                    "fn-test-destination-facet".to_string(),
                    "fn-test-destination-map".to_string(),
                    planned.facets[0].filter.clone(),
                )],
            )
            .unwrap();
            let mut settings = config.clone();
            settings.as_object_mut().unwrap().remove("facet_functions");
            settings
                .as_object_mut()
                .unwrap()
                .remove("topic_map_functions");
            assert_eq!(settings, expected.topics_automations[0].config);
            assert_eq!(
                topics.description,
                expected.topics_automations[0].description
            );
            assert_eq!(
                config["topic_map_functions"][0]["btql_filter"],
                "metadata.test_enabled = true"
            );
        }
    }

    #[test]
    fn active_observability_scoped_settings_require_force_and_preserve_other_wiring() {
        let source = scoped_template();
        let mut existing = automation("test-existing-automation", "Test scoped Topics", "topic");
        existing.config = add_topics_functions(
            &default_topics_config(),
            &[(
                "fn-test-other-facet".to_string(),
                "fn-test-other-map".to_string(),
                TopicMapFilter::Replace(Some("metadata.test_other = true".to_string())),
            )],
        )
        .unwrap();
        let snapshot = || Snapshot {
            functions: vec![],
            automations: vec![existing.clone()],
        };
        let error = plan(&source, snapshot(), None, false).unwrap_err();
        assert!(error.to_string().contains("--force"));
        let planned = plan(&source, snapshot(), None, true).unwrap();
        let config = &planned.topics["id:test-existing-automation"].config;
        assert_eq!(config["scope"], json!({"type": "span"}));
        assert_eq!(
            config["facet_functions"],
            existing.config["facet_functions"]
        );
        assert_eq!(
            config["topic_map_functions"],
            existing.config["topic_map_functions"]
        );
        let overridden =
            plan(&source, snapshot(), Some("test-existing-automation"), false).unwrap();
        assert_eq!(
            overridden.topics["id:test-existing-automation"].config,
            existing.config
        );
        assert_eq!(
            overridden.topics["id:test-existing-automation"].description,
            existing.description
        );
    }

    #[test]
    fn active_observability_matching_settings_do_not_require_force() {
        let mut source = scoped_template();
        source.topics_automations[0].config["sampling_rate"] = json!(1);
        let mut existing = automation("test-existing-automation", "Test scoped Topics", "topic");
        existing.config =
            topics_config_for_target(&source.topics_automations[0].config, &json!({})).unwrap();
        existing.description = source.topics_automations[0].description.clone();
        existing.config["sampling_rate"] = json!(1.0);
        let snapshot = |row| Snapshot {
            functions: vec![],
            automations: vec![row],
        };
        plan(&source, snapshot(existing.clone()), None, false).unwrap();
        existing.description = Some("Different test description".to_string());
        assert!(plan(&source, snapshot(existing), None, false)
            .unwrap_err()
            .to_string()
            .contains("--force"));
    }

    #[test]
    fn active_observability_override_ignores_unused_experiment_and_timing_settings() {
        let mut source = scoped_template();
        source.topics_automations[0].config["data_scope"] =
            json!({"type": "experiment", "experiment_id": "test-experiment-id"});
        source.topics_automations[0].config["backfill_time_range"] = json!("bogus");
        let existing = automation(
            "test-existing-automation",
            "Test destination Topics",
            "topic",
        );
        let snapshot = || Snapshot {
            functions: vec![],
            automations: vec![existing.clone()],
        };
        assert!(plan(&source, snapshot(), None, false).is_err());
        let planned = plan(&source, snapshot(), Some("test-existing-automation"), false).unwrap();
        let mutation = &planned.topics["id:test-existing-automation"];
        assert_eq!(mutation.config, existing.config);
        assert_eq!(mutation.description, existing.description);
        let request = mutation
            .request(
                "test-project-id",
                &[(
                    "fn-test-facet".to_string(),
                    "fn-test-map".to_string(),
                    planned.facets[0].filter.clone(),
                )],
            )
            .unwrap();
        assert_eq!(
            request["config"]["topic_map_functions"][0]["btql_filter"],
            "metadata.test_enabled = true"
        );
    }

    #[test]
    fn active_observability_invalid_backfill_is_rejected_for_new_and_existing_targets() {
        let mut source = scoped_template();
        source.topics_automations[0].config["backfill_time_range"] = json!("bogus");
        for automations in [
            vec![],
            vec![automation(
                "test-existing-automation",
                "Test scoped Topics",
                "topic",
            )],
        ] {
            let error = plan(
                &source,
                Snapshot {
                    functions: vec![],
                    automations,
                },
                None,
                true,
            )
            .unwrap_err();
            assert!(format!("{error:#}").contains("backfill_time_range"));
        }
    }

    #[test]
    fn active_observability_topics_request_restores_settings_description_and_filters() {
        let mut source = scoped_template();
        source.topics_automations[0].description = None;
        source.topics_automations[0].config["backfill_time_range"] = json!({
            "from": "2026-01-01T00:00:00Z", "to": "2026-01-02T00:00:00Z"
        });
        let mut existing = automation("test-existing-automation", "Test scoped Topics", "topic");
        existing.config["test_destination_only"] = json!(true);
        existing.description = Some("Test destination description".to_string());
        existing.config = add_topics_functions(
            &existing.config,
            &[(
                "fn-test-other-facet".to_string(),
                "fn-test-other-map".to_string(),
                TopicMapFilter::Replace(Some("metadata.test_other = true".to_string())),
            )],
        )
        .unwrap();
        for automations in [vec![], vec![existing.clone()]] {
            let updating = !automations.is_empty();
            let planned = plan(
                &source,
                Snapshot {
                    functions: vec![],
                    automations,
                },
                None,
                true,
            )
            .unwrap();
            let mutation = planned.topics.values().next().unwrap();
            let request = mutation
                .request(
                    "test-destination-project",
                    &[(
                        "fn-test-new-facet".to_string(),
                        "fn-test-new-map".to_string(),
                        planned.facets[0].filter.clone(),
                    )],
                )
                .unwrap();
            assert_eq!(request["name"], "Test scoped Topics");
            assert!(request["description"].is_null());
            assert_eq!(request.get("project_id").is_none(), updating);
            assert_eq!(request["config"]["scope"], json!({"type": "span"}));
            assert!(request["config"].get("test_destination_only").is_none());
            assert_eq!(
                request["config"]["backfill_time_range"],
                source.topics_automations[0].config["backfill_time_range"]
            );
            let maps = request["config"]["topic_map_functions"].as_array().unwrap();
            assert_eq!(
                maps.last().unwrap()["btql_filter"],
                "metadata.test_enabled = true"
            );
            if updating {
                assert_eq!(maps[0]["btql_filter"], "metadata.test_other = true");
            }
        }
    }

    #[tokio::test]
    async fn active_observability_execute_sends_scoped_settings_and_map_filter() {
        use actix_web::{web, App, HttpRequest, HttpResponse, HttpServer};
        use std::{
            net::TcpListener,
            sync::{Arc, Mutex},
        };

        type RecordedRequests = Arc<Mutex<Vec<(String, String, Value)>>>;
        let requests: RecordedRequests = Arc::new(Mutex::new(Vec::new()));
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let api_url = format!("http://{}", listener.local_addr().unwrap());
        let state = web::Data::new(requests.clone());
        let server = HttpServer::new(move || {
            App::new().app_data(state.clone()).default_service(web::to(
                |request: HttpRequest,
                 body: web::Json<Value>,
                 state: web::Data<RecordedRequests>| async move {
                    let mut response = body.into_inner();
                    state.lock().unwrap().push((
                        request.method().to_string(),
                        request.path().to_string(),
                        response.clone(),
                    ));
                    match (request.method().as_str(), request.path()) {
                        ("PUT", "/v1/function") => {
                            response["id"] = if response["function_type"] == "facet" {
                                json!("fn-test-facet")
                            } else {
                                json!("fn-test-map")
                            };
                        }
                        ("PATCH", "/v1/project_automation/test-existing-automation") => {
                            response["id"] = json!("test-existing-automation");
                            response["project_id"] = json!("test-project-id");
                        }
                        _ => return HttpResponse::NotFound().finish(),
                    }
                    HttpResponse::Ok().json(response)
                },
            ))
        })
        .workers(1)
        .listen(listener)
        .unwrap()
        .run();
        let handle = server.handle();
        tokio::spawn(server);
        let login = braintrust_sdk_rust::LoginState::new();
        login.set(
            "test-key".to_string(),
            "test-org-id".to_string(),
            "test-org".to_string(),
            api_url.clone(),
            "https://example.invalid".to_string(),
        );
        let client = ApiClient::new(&crate::auth::LoginContext {
            login,
            api_url,
            app_url: "https://example.invalid".to_string(),
            profile: None,
        })
        .unwrap();

        let mut source = scoped_template();
        source.topics_automations[0].description = None;
        let mut facet = existing_function("test-facet", "facet", "facet");
        facet.id = "fn-test-facet".to_string();
        let mut map = existing_function("test-facet-topic-map", "classifier", "topic_map");
        map.id = "fn-test-map".to_string();
        map.function_data = Some(
            json!({"type": "topic_map", "source_facet_function": {"type": "function", "id": facet.id}}),
        );
        let mut automation = automation("test-existing-automation", "Test scoped Topics", "topic");
        automation.description = Some("Test old description".to_string());
        automation.config = add_topics_functions(
            &automation.config,
            &[(
                facet.id.clone(),
                map.id.clone(),
                TopicMapFilter::Replace(Some("metadata.test_old = true".to_string())),
            )],
        )
        .unwrap();
        let planned = plan(
            &source,
            Snapshot {
                functions: vec![facet, map],
                automations: vec![automation],
            },
            None,
            true,
        )
        .unwrap();
        let result = execute(&client, "test-project-id", planned).await;
        handle.stop(true).await;
        assert_eq!(result.unwrap().facets[0].id, "fn-test-facet");
        let requests = requests.lock().unwrap();
        assert_eq!(requests.len(), 3);
        assert_eq!(requests[0].0, "PUT");
        assert_eq!(requests[1].0, "PUT");
        assert_eq!(requests[2].0, "PATCH");
        let body = &requests[2].2;
        assert_eq!(body["name"], "Test scoped Topics");
        assert!(body["description"].is_null());
        assert_eq!(body["config"]["scope"], json!({"type": "span"}));
        assert_eq!(
            body["config"]["btql_filter"],
            "span_attributes.name = 'test-tool'"
        );
        assert_eq!(body["config"]["facet_functions"][0]["id"], "fn-test-facet");
        assert_eq!(
            body["config"]["topic_map_functions"][0]["function"]["id"],
            "fn-test-map"
        );
        assert_eq!(
            body["config"]["topic_map_functions"][0]["btql_filter"],
            "metadata.test_enabled = true"
        );
    }

    #[test]
    fn active_observability_request_clears_v2_filters_but_preserves_v1_and_destination_policy() {
        let mut existing_facet = existing_function("test-facet", "facet", "facet");
        existing_facet.id = "fn-test-destination-facet".to_string();
        let mut map = existing_function("test-facet-topic-map", "classifier", "topic_map");
        map.id = "fn-test-destination-map".to_string();
        map.function_data = Some(
            json!({"type": "topic_map", "source_facet_function": {"type": "function", "id": existing_facet.id}}),
        );
        let mut existing = automation("test-existing-automation", "Test scoped Topics", "topic");
        existing.config = add_topics_functions(
            &existing.config,
            &[(
                existing_facet.id.clone(),
                map.id.clone(),
                TopicMapFilter::Replace(Some("metadata.test_old = true".to_string())),
            )],
        )
        .unwrap();
        let mut source = scoped_template();
        source.facets[0].topic_map_btql_filter = None;
        for version in [1, 2] {
            let mut source = source.clone();
            source.schema_version = version;
            if version == 1 {
                source.topics_automations.clear();
            }
            let planned = plan(
                &source,
                Snapshot {
                    functions: vec![existing_facet.clone(), map.clone()],
                    automations: vec![existing.clone()],
                },
                Some("test-existing-automation"),
                true,
            )
            .unwrap();
            let request = planned.topics["id:test-existing-automation"]
                .request(
                    "test-project",
                    &[(
                        existing_facet.id.clone(),
                        map.id.clone(),
                        planned.facets[0].filter.clone(),
                    )],
                )
                .unwrap();
            assert_eq!(request["config"]["scope"], existing.config["scope"]);
            assert_eq!(request["description"], json!(existing.description));
            let filter = request["config"]["topic_map_functions"][0].get("btql_filter");
            if version == 1 {
                assert_eq!(filter, Some(&json!("metadata.test_old = true")));
            } else {
                assert!(filter.is_none());
            }
        }
    }

    #[test]
    fn active_observability_unmapped_facet_does_not_skip_bundled_settings() {
        let mut source = scoped_template();
        source.facets.insert(
            0,
            FacetTemplate {
                name: "Unmapped test facet".to_string(),
                slug: "test-unmapped-facet".to_string(),
                topics_automation: None,
                topic_map_btql_filter: None,
                ..source.facets[0].clone()
            },
        );
        let snapshot = || Snapshot {
            functions: vec![],
            automations: vec![automation(
                "test-existing-automation",
                "Test scoped Topics",
                "topic",
            )],
        };
        let error = plan(&source, snapshot(), None, false).unwrap_err();
        assert!(error.to_string().contains("--force"));
        let planned = plan(&source, snapshot(), None, true).unwrap();
        assert_eq!(
            planned.topics["id:test-existing-automation"].config["scope"],
            json!({"type": "span"})
        );
    }

    #[test]
    fn active_observability_settings_preserve_prior_removals_from_shared_destinations() {
        let mut source = scoped_template();
        source.facets.insert(
            0,
            FacetTemplate {
                name: "First test facet".to_string(),
                slug: "test-first-facet".to_string(),
                topics_automation: Some("Test other Topics".to_string()),
                ..source.facets[0].clone()
            },
        );
        source.topics_automations.push(AutomationTemplate {
            name: "Test other Topics".to_string(),
            description: None,
            config: json!({"event_type": "topic", "scope": {"type": "trace"}, "sampling_rate": 1}),
        });
        let first = existing_function("test-first-facet", "facet", "facet");
        let mut existing = automation("test-existing-automation", "Test scoped Topics", "topic");
        existing.config = add_topics_functions(
            &default_topics_config(),
            &[(
                first.id.clone(),
                "fn-test-old-map".to_string(),
                TopicMapFilter::Preserve,
            )],
        )
        .unwrap();
        let planned = plan(
            &source,
            Snapshot {
                functions: vec![first],
                automations: vec![existing],
            },
            None,
            true,
        )
        .unwrap();
        assert_eq!(
            planned.topics["id:test-existing-automation"].config["facet_functions"],
            json!([])
        );
        assert_eq!(
            planned.topics["id:test-existing-automation"].config["scope"],
            json!({"type": "span"})
        );
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
    fn active_observability_shared_preprocessor_round_trips_once() {
        let preprocessor_slug = "shared-preprocessor";
        let mut first = facet(Some("Topics"));
        first.function_data["preprocessor"] =
            json!({"type": "function", "slug": preprocessor_slug});
        let shared = portable_preprocessor(preprocessor_slug);
        first.preprocessor = Some(shared.clone());
        let mut second = FacetTemplate {
            slug: "second-facet".to_string(),
            name: "Second facet".to_string(),
            ..facet(Some("Topics"))
        };
        second.function_data["preprocessor"] =
            json!({"type": "function", "slug": preprocessor_slug});
        second.preprocessor = Some(shared);
        let mut facets = vec![first, second];
        deduplicate_preprocessors(&mut facets);
        let source = ActiveObservabilityTemplate {
            kind: KIND.to_string(),
            schema_version: 1,
            topics_automations: Vec::new(),
            facets,
            automations: Vec::new(),
        };
        validate(&source).expect("valid shared preprocessor template");

        let planned = plan(
            &source,
            Snapshot {
                functions: Vec::new(),
                automations: vec![automation("auto-topics", "Topics", "topic")],
            },
            None,
            false,
        )
        .expect("sibling bundle satisfies dependency");

        assert_eq!(planned.preprocessors.len(), 1);
        assert_eq!(planned.facets.len(), 2);
    }

    #[test]
    fn active_observability_finds_and_remaps_an_existing_topic_map() {
        let mut existing_facet = existing_function("test-facet", "facet", "facet");
        existing_facet.id = "fn-existing-facet".to_string();
        let mut existing_topic_map = existing_function("test-facet", "classifier", "topic_map");
        existing_topic_map.id = "fn-existing-topic-map".to_string();
        existing_topic_map.function_data = Some(json!({
            "type": "topic_map",
            "source_facet": "Test facet",
            "source_facet_function": {
                "type": "function",
                "id": "fn-existing-facet"
            }
        }));
        let mut previous = automation("auto-previous", "Previous Topics", "topic");
        previous.config["facet_functions"] = json!([
            {"type": "function", "id": "fn-existing-facet"},
            {"type": "function", "id": "fn-unrelated-facet"}
        ]);
        previous.config["topic_map_functions"] = json!([
            {"function": {"type": "function", "id": "fn-existing-topic-map"}},
            {"function": {"type": "function", "id": "fn-unrelated-topic-map"}}
        ]);

        let planned = plan(
            &template(facet(Some("Destination Topics"))),
            Snapshot {
                functions: vec![existing_facet, existing_topic_map],
                automations: vec![
                    previous,
                    automation("auto-destination", "Destination Topics", "topic"),
                ],
            },
            None,
            true,
        )
        .expect("existing topic map is found through Topics wiring");

        let topic_map = planned.facets[0]
            .topic_map
            .as_ref()
            .expect("existing topic map");
        assert_eq!(topic_map.id, "fn-existing-topic-map");
        assert_eq!(topic_map.slug, "test-facet");
        let previous = &planned.topics["id:auto-previous"].config;
        assert_eq!(
            previous["facet_functions"],
            json!([{"type": "function", "id": "fn-unrelated-facet"}])
        );
        assert_eq!(
            previous["topic_map_functions"],
            json!([{"function": {"type": "function", "id": "fn-unrelated-topic-map"}}])
        );
        assert!(planned.topics.contains_key("id:auto-destination"));
    }

    #[test]
    fn active_observability_force_repairs_duplicate_topic_map_wiring() {
        let mut existing_facet = existing_function("test-facet", "facet", "facet");
        existing_facet.id = "fn-existing-facet".to_string();

        let mut original_topic_map =
            existing_function("legacy-test-topic-map", "classifier", "topic_map");
        original_topic_map.id = "fn-original-topic-map".to_string();
        original_topic_map.created = Some("2026-01-01T00:00:00Z".to_string());
        original_topic_map.function_data = Some(json!({
            "type": "topic_map",
            "source_facet_function": {
                "type": "function",
                "id": "fn-existing-facet"
            },
            "generation_settings": {"max_topics": 12}
        }));

        let mut duplicate_topic_map =
            existing_function("test-facet-topic-map", "classifier", "topic_map");
        duplicate_topic_map.id = "fn-duplicate-topic-map".to_string();
        duplicate_topic_map.created = Some("2026-02-01T00:00:00Z".to_string());
        duplicate_topic_map.function_data = Some(json!({
            "type": "topic_map",
            "source_facet_function": {
                "type": "function",
                "id": "fn-existing-facet"
            }
        }));

        let mut topics = automation("auto-topics", "Topics", "topic");
        topics.config["facet_functions"] = json!([{"type": "function", "id": "fn-existing-facet"}]);
        topics.config["topic_map_functions"] = json!([
            {"function": {"type": "function", "id": "fn-original-topic-map"}},
            {"function": {"type": "function", "id": "fn-duplicate-topic-map"}}
        ]);

        let planned = plan(
            &template(facet(Some("Topics"))),
            Snapshot {
                functions: vec![existing_facet, original_topic_map, duplicate_topic_map],
                automations: vec![topics],
            },
            None,
            true,
        )
        .expect("force repairs duplicate topic map wiring");

        let selected = planned.facets[0]
            .topic_map
            .as_ref()
            .expect("oldest topic map is retained");
        assert_eq!(selected.id, "fn-original-topic-map");
        assert_eq!(planned.detached_topic_maps.len(), 1);
        assert_eq!(planned.detached_topic_maps[0].id, "fn-duplicate-topic-map");
        assert_eq!(
            selected.function_data.as_ref().unwrap()["generation_settings"],
            json!({"max_topics": 12})
        );
        assert_eq!(
            planned.topics["id:auto-topics"].config["topic_map_functions"],
            json!([
                {"function": {"type": "function", "id": "fn-original-topic-map"}}
            ])
        );
    }

    #[test]
    fn active_observability_preflight_rejects_a_function_type_collision() {
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
    }

    #[test]
    fn active_observability_preflight_requires_force_for_an_existing_facet() {
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
    }

    #[test]
    fn active_observability_preflight_rejects_an_automation_type_collision() {
        let wrong_loop = plan(
            &ActiveObservabilityTemplate {
                kind: KIND.to_string(),
                schema_version: SCHEMA_VERSION,
                topics_automations: Vec::new(),
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
}
