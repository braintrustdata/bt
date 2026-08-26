use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::{functions::api::Function, topics::api::ProjectAutomation};

pub(crate) const KIND: &str = "active_observability_template";
pub(crate) const SCHEMA_VERSION: u32 = 1;
pub(crate) const DEFAULT_EMBEDDING_MODEL: &str = "brain-embedding-1";
pub(crate) const DEFAULT_TOPICS_DESCRIPTION: &str =
    "Automatically extract facets and classify logs using topic maps";

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub(crate) struct ActiveObservabilityTemplate {
    pub kind: String,
    pub schema_version: u32,
    #[serde(default)]
    pub facets: Vec<FacetTemplate>,
    #[serde(default)]
    pub automations: Vec<AutomationTemplate>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub(crate) struct FacetTemplate {
    pub name: String,
    pub slug: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub topics_automation: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preprocessor: Option<PortableFunction>,
    pub function_data: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_data: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_schema: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub(crate) struct PortableFunction {
    pub name: String,
    pub slug: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub function_data: Value,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prompt_data: Option<Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tags: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub function_schema: Option<Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub(crate) struct AutomationTemplate {
    pub name: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    pub config: Value,
}

pub(crate) fn from_remote(
    functions: &[Function],
    automations: &[ProjectAutomation],
) -> Result<ActiveObservabilityTemplate> {
    let by_id = functions
        .iter()
        .map(|function| (function.id.as_str(), function))
        .collect::<HashMap<_, _>>();
    let facets = functions
        .iter()
        .filter(|function| function.function_type.as_deref() == Some("facet"))
        .collect::<Vec<_>>();
    let topics = topics_by_facet(&facets, &by_id, automations)?;

    let mut facet_templates = facets
        .into_iter()
        .map(|facet| facet_from_remote(facet, topics.get(&facet.id).cloned(), &by_id))
        .collect::<Result<Vec<_>>>()?;
    facet_templates.sort_by(|a, b| a.name.cmp(&b.name).then(a.slug.cmp(&b.slug)));

    let mut loop_templates = automations
        .iter()
        .filter(|automation| is_loop_config(&automation.config))
        .map(|automation| {
            let mut config = object(&automation.config, "Loop automation config")?.clone();
            config.remove("actions");
            Ok(AutomationTemplate {
                name: automation.name.clone(),
                description: automation.description.clone(),
                config: Value::Object(config),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    loop_templates.sort_by(|a, b| a.name.cmp(&b.name));

    Ok(ActiveObservabilityTemplate {
        kind: KIND.to_string(),
        schema_version: SCHEMA_VERSION,
        facets: facet_templates,
        automations: loop_templates,
    })
}

fn facet_from_remote(
    facet: &Function,
    topics_automation: Option<String>,
    by_id: &HashMap<&str, &Function>,
) -> Result<FacetTemplate> {
    let mut function_data = facet
        .function_data
        .clone()
        .ok_or_else(|| anyhow!("facet '{}' is missing function_data", facet.name))?;
    if function_data.get("type").and_then(Value::as_str) != Some("facet") {
        bail!(
            "function '{}' has facet type but non-facet function_data",
            facet.name
        );
    }

    let preprocessor = saved_preprocessor(&mut function_data, by_id)?;
    Ok(FacetTemplate {
        name: facet.name.clone(),
        slug: facet.slug.clone(),
        topics_automation,
        description: facet.description.clone(),
        preprocessor,
        function_data,
        prompt_data: facet.prompt_data.clone(),
        tags: facet.tags.clone(),
        function_schema: facet.function_schema.clone(),
    })
}

fn saved_preprocessor(
    function_data: &mut Value,
    by_id: &HashMap<&str, &Function>,
) -> Result<Option<PortableFunction>> {
    let Some(reference) = function_data
        .get_mut("preprocessor")
        .and_then(Value::as_object_mut)
        .filter(|reference| reference.get("type").and_then(Value::as_str) == Some("function"))
    else {
        return Ok(None);
    };
    let id = reference
        .get("id")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("saved facet preprocessor reference is missing its function id"))?;
    let function = by_id
        .get(id)
        .ok_or_else(|| anyhow!("facet references missing preprocessor function '{id}'"))?;
    if function.function_type.as_deref() != Some("preprocessor") {
        bail!("facet preprocessor reference '{id}' is not a preprocessor function");
    }
    let portable = PortableFunction::from_remote(function)?;
    *reference = Map::from_iter([
        ("type".to_string(), Value::String("function".to_string())),
        ("slug".to_string(), Value::String(portable.slug.clone())),
    ]);
    Ok(Some(portable))
}

fn topics_by_facet(
    facets: &[&Function],
    by_id: &HashMap<&str, &Function>,
    automations: &[ProjectAutomation],
) -> Result<HashMap<String, String>> {
    let mut names: HashMap<String, HashSet<String>> = HashMap::new();
    for automation in automations
        .iter()
        .filter(|automation| is_topics(&automation.config))
    {
        for id in topic_map_ids(&automation.config) {
            let Some(topic_map) = by_id.get(id) else {
                continue;
            };
            if topic_map.function_type.as_deref() != Some("classifier")
                || topic_map
                    .function_data
                    .as_ref()
                    .and_then(|data| data.get("type"))
                    .and_then(Value::as_str)
                    != Some("topic_map")
            {
                continue;
            }
            for facet in facets
                .iter()
                .copied()
                .filter(|facet| topic_map_matches(topic_map, facet))
            {
                names
                    .entry(facet.id.clone())
                    .or_default()
                    .insert(automation.name.clone());
            }
        }
    }

    names
        .into_iter()
        .map(|(facet_id, names)| {
            if names.len() != 1 {
                let mut names = names.into_iter().collect::<Vec<_>>();
                names.sort();
                bail!(
                    "facet '{facet_id}' belongs to multiple Topics automations ({}); use one Topics destination per facet",
                    names.join(", ")
                );
            }
            Ok((facet_id, names.into_iter().next().expect("one name")))
        })
        .collect()
}

fn topic_map_matches(topic_map: &Function, facet: &Function) -> bool {
    let Some(data) = topic_map.function_data.as_ref() else {
        return false;
    };
    match data
        .get("source_facet_function")
        .and_then(Value::as_object)
        .filter(|reference| reference.get("type").and_then(Value::as_str) == Some("function"))
        .and_then(|reference| reference.get("id"))
        .and_then(Value::as_str)
    {
        Some(id) => id == facet.id,
        None => data
            .get("source_facet")
            .and_then(Value::as_str)
            .is_some_and(|source| source == facet.name || source == facet.slug),
    }
}

pub(crate) fn validate(template: &ActiveObservabilityTemplate) -> Result<()> {
    if template.kind != KIND {
        bail!("template kind must be '{KIND}'");
    }
    if template.schema_version != SCHEMA_VERSION {
        bail!(
            "unsupported template schema version {}; supported version is {SCHEMA_VERSION}",
            template.schema_version
        );
    }

    let mut slugs = HashMap::<String, &'static str>::new();
    let mut preprocessors = HashMap::<String, &PortableFunction>::new();
    for facet in &template.facets {
        require_text(&facet.name, "facet name")?;
        require_text(&facet.slug, "facet slug")?;
        if facet.function_data.get("type").and_then(Value::as_str) != Some("facet") {
            bail!("facet '{}' function_data.type must be 'facet'", facet.name);
        }
        reserve_slug(&mut slugs, &facet.slug, "facet")?;
        reserve_slug(
            &mut slugs,
            &topic_map_slug(&facet.slug),
            "generated topic map",
        )?;
        if let Some(name) = facet.topics_automation.as_deref() {
            require_text(name, "Topics automation name")?;
        }

        let saved_slug = saved_preprocessor_slug(&facet.function_data)?;
        match (&facet.preprocessor, saved_slug) {
            (Some(preprocessor), Some(slug)) => {
                preprocessor.validate()?;
                if preprocessor.slug != slug {
                    bail!(
                        "facet '{}' bundles preprocessor '{}' but references '{}'",
                        facet.name,
                        preprocessor.slug,
                        slug
                    );
                }
                match preprocessors.get(slug) {
                    Some(existing) if **existing != *preprocessor => {
                        bail!("template contains conflicting preprocessors with slug '{slug}'")
                    }
                    Some(_) => {}
                    None => {
                        reserve_slug(&mut slugs, slug, "bundled preprocessor")?;
                        preprocessors.insert(slug.to_string(), preprocessor);
                    }
                }
            }
            (Some(_), None) => bail!(
                "facet '{}' bundles a preprocessor without a saved preprocessor reference",
                facet.name
            ),
            _ => {}
        }
    }

    let mut automation_names = HashSet::new();
    for automation in &template.automations {
        require_text(&automation.name, "automation name")?;
        if !automation_names.insert(automation.name.as_str()) {
            bail!(
                "template contains duplicate automation name '{}'",
                automation.name
            );
        }
        if !is_loop_config(&automation.config) {
            bail!(
                "automation '{}' is not a Loop automation (expected windowed config with loop)",
                automation.name
            );
        }
    }
    Ok(())
}

fn reserve_slug(
    slugs: &mut HashMap<String, &'static str>,
    slug: &str,
    kind: &'static str,
) -> Result<()> {
    if let Some(previous) = slugs.insert(slug.to_string(), kind) {
        if previous == "facet" && kind == "facet" {
            bail!("template contains duplicate facet slug '{slug}'");
        }
        bail!("template uses function slug '{slug}' for both {previous} and {kind}");
    }
    Ok(())
}

fn require_text(value: &str, label: &str) -> Result<()> {
    if value.trim().is_empty() {
        bail!("{label} must not be empty");
    }
    Ok(())
}

impl PortableFunction {
    fn from_remote(function: &Function) -> Result<Self> {
        Ok(Self {
            name: function.name.clone(),
            slug: function.slug.clone(),
            description: function.description.clone(),
            function_data: function
                .function_data
                .clone()
                .ok_or_else(|| anyhow!("function '{}' is missing function_data", function.name))?,
            prompt_data: function.prompt_data.clone(),
            tags: function.tags.clone(),
            function_schema: function.function_schema.clone(),
        })
    }

    fn validate(&self) -> Result<()> {
        require_text(&self.name, "preprocessor name")?;
        require_text(&self.slug, "preprocessor slug")?;
        if !self.function_data.is_object() {
            bail!(
                "preprocessor '{}' function_data must be an object",
                self.name
            );
        }
        Ok(())
    }

    pub(crate) fn request(&self, project_id: &str, function_type: &str) -> Value {
        portable_function_request(
            project_id,
            &self.name,
            &self.slug,
            self.description.as_ref(),
            function_type,
            &self.function_data,
            self.prompt_data.as_ref(),
            self.tags.as_ref(),
            self.function_schema.as_ref(),
        )
    }
}

impl FacetTemplate {
    pub(crate) fn request(&self, project_id: &str, function_data: &Value) -> Value {
        portable_function_request(
            project_id,
            &self.name,
            &self.slug,
            self.description.as_ref(),
            "facet",
            function_data,
            self.prompt_data.as_ref(),
            self.tags.as_ref(),
            self.function_schema.as_ref(),
        )
    }

    pub(crate) fn active(&self) -> bool {
        self.topics_automation.is_some()
    }
}

fn portable_function_request(
    project_id: &str,
    name: &str,
    slug: &str,
    description: Option<&String>,
    function_type: &str,
    function_data: &Value,
    prompt_data: Option<&Value>,
    tags: Option<&Vec<String>>,
    function_schema: Option<&Value>,
) -> Value {
    json!({
        "project_id": project_id,
        "name": name,
        "slug": slug,
        "description": description,
        "function_type": function_type,
        "function_data": function_data,
        "prompt_data": prompt_data,
        "tags": tags,
        "function_schema": function_schema,
    })
}

pub(crate) fn saved_preprocessor_slug(function_data: &Value) -> Result<Option<&str>> {
    let Some(reference) = function_data
        .get("preprocessor")
        .and_then(Value::as_object)
        .filter(|reference| reference.get("type").and_then(Value::as_str) == Some("function"))
    else {
        return Ok(None);
    };
    if reference.contains_key("id") {
        bail!("portable saved preprocessor reference must use 'slug', not source-project 'id'");
    }
    reference
        .get("slug")
        .and_then(Value::as_str)
        .filter(|slug| !slug.trim().is_empty())
        .map(Some)
        .ok_or_else(|| anyhow!("portable saved preprocessor reference is missing its slug"))
}

pub(crate) fn with_preprocessor_id(function_data: &Value, id: Option<&str>) -> Result<Value> {
    let mut data = object(function_data, "facet function_data")?.clone();
    if let Some(id) = id {
        data.insert(
            "preprocessor".to_string(),
            json!({"type": "function", "id": id}),
        );
    }
    Ok(Value::Object(data))
}

pub(crate) fn topic_map_slug(facet_slug: &str) -> String {
    format!("{facet_slug}-topic-map")
}

pub(crate) fn new_topic_map_request(
    project_id: &str,
    facet: &FacetTemplate,
    facet_id: &str,
    embedding_model: &str,
) -> Value {
    json!({
        "project_id": project_id,
        "name": facet.name,
        "slug": topic_map_slug(&facet.slug),
        "description": facet.description,
        "function_type": "classifier",
        "function_data": {
            "type": "topic_map",
            "source_facet": facet.name,
            "source_facet_function": {"type": "function", "id": facet_id},
            "embedding_model": embedding_model,
        }
    })
}

pub(crate) fn reconciled_topic_map_request(
    existing: &Function,
    facet: &FacetTemplate,
    facet_id: &str,
) -> Result<Value> {
    let mut data = object(
        existing
            .function_data
            .as_ref()
            .ok_or_else(|| anyhow!("topic map '{}' is missing function_data", existing.slug))?,
        "topic map function_data",
    )?
    .clone();
    data.insert(
        "source_facet".to_string(),
        Value::String(facet.name.clone()),
    );
    data.insert(
        "source_facet_function".to_string(),
        json!({"type": "function", "id": facet_id}),
    );
    Ok(portable_function_request(
        &existing.project_id,
        &facet.name,
        &existing.slug,
        existing.description.as_ref(),
        "classifier",
        &Value::Object(data),
        existing.prompt_data.as_ref(),
        existing.tags.as_ref(),
        existing.function_schema.as_ref(),
    ))
}

pub(crate) fn deduplicate_preprocessors(facets: &mut [FacetTemplate]) {
    let mut included = HashSet::new();
    for facet in facets {
        if facet
            .preprocessor
            .as_ref()
            .is_some_and(|preprocessor| !included.insert(preprocessor.slug.clone()))
        {
            facet.preprocessor = None;
        }
    }
}

pub(crate) fn is_topics(config: &Value) -> bool {
    config.get("event_type").and_then(Value::as_str) == Some("topic")
}

pub(crate) fn is_loop_config(config: &Value) -> bool {
    config.get("event_type").and_then(Value::as_str) == Some("windowed")
        && config.get("loop").and_then(Value::as_object).is_some()
}

pub(crate) fn loop_config_for_target(template: &Value, existing: Option<&Value>) -> Result<Value> {
    let mut config = object(template, "Loop automation config")?.clone();
    let actions = existing
        .and_then(Value::as_object)
        .and_then(|config| config.get("actions"))
        .cloned()
        .unwrap_or_else(|| Value::Array(Vec::new()));
    config.insert("actions".to_string(), actions);
    Ok(Value::Object(config))
}

pub(crate) fn default_topics_config() -> Value {
    json!({
        "event_type": "topic",
        "sampling_rate": 1.0,
        "facet_functions": [],
        "topic_map_functions": [],
        "scope": {"type": "trace", "idle_seconds": 600},
        "rerun_seconds": 86400,
        "relabel_overlap_seconds": 3600,
        "backfill_time_range": "86400s",
    })
}

pub(crate) fn add_topics_functions(
    config: &Value,
    functions: &[(String, String)],
) -> Result<Value> {
    let mut config = object(config, "Topics automation config")?.clone();
    let facets = array_entry(&mut config, "facet_functions")?;
    for (facet_id, _) in functions {
        if !facets
            .iter()
            .any(|entry| function_ref_id(entry) == Some(facet_id.as_str()))
        {
            facets.push(json!({"type": "function", "id": facet_id}));
        }
    }
    let topic_maps = array_entry(&mut config, "topic_map_functions")?;
    for (_, topic_map_id) in functions {
        if !topic_maps.iter().any(|entry| {
            entry.get("function").and_then(function_ref_id) == Some(topic_map_id.as_str())
        }) {
            topic_maps.push(json!({"function": {"type": "function", "id": topic_map_id}}));
        }
    }
    Ok(Value::Object(config))
}

pub(crate) fn remove_topics_functions(
    config: &Value,
    facet_id: Option<&str>,
    topic_map_id: Option<&str>,
) -> Result<Option<Value>> {
    let contains_facet = facet_id.is_some_and(|id| {
        config
            .get("facet_functions")
            .and_then(Value::as_array)
            .is_some_and(|facets| {
                facets
                    .iter()
                    .any(|entry| function_ref_id(entry) == Some(id))
            })
    });
    let contains_topic_map = topic_map_id.is_some_and(|id| {
        config
            .get("topic_map_functions")
            .and_then(Value::as_array)
            .is_some_and(|topic_maps| {
                topic_maps
                    .iter()
                    .any(|entry| entry.get("function").and_then(function_ref_id) == Some(id))
            })
    });
    if !contains_facet && !contains_topic_map {
        return Ok(None);
    }

    let mut config = object(config, "Topics automation config")?.clone();
    array_entry(&mut config, "facet_functions")?
        .retain(|entry| facet_id.is_none_or(|id| function_ref_id(entry) != Some(id)));
    array_entry(&mut config, "topic_map_functions")?.retain(|entry| {
        topic_map_id.is_none_or(|id| entry.get("function").and_then(function_ref_id) != Some(id))
    });
    Ok(Some(Value::Object(config)))
}

pub(crate) fn embedding_model(
    automation: &ProjectAutomation,
    functions_by_id: &HashMap<&str, &Function>,
) -> String {
    topic_map_ids(&automation.config)
        .filter_map(|id| functions_by_id.get(id))
        .filter_map(|function| function.function_data.as_ref())
        .filter_map(|data| data.get("embedding_model").and_then(Value::as_str))
        .find(|model| !model.trim().is_empty())
        .unwrap_or(DEFAULT_EMBEDDING_MODEL)
        .to_string()
}

fn topic_map_ids(config: &Value) -> impl Iterator<Item = &str> {
    config
        .get("topic_map_functions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.get("function"))
        .filter_map(function_ref_id)
}

fn function_ref_id(reference: &Value) -> Option<&str> {
    (reference.get("type").and_then(Value::as_str) == Some("function"))
        .then(|| reference.get("id").and_then(Value::as_str))
        .flatten()
}

fn array_entry<'a>(config: &'a mut Map<String, Value>, name: &str) -> Result<&'a mut Vec<Value>> {
    config
        .entry(name.to_string())
        .or_insert_with(|| Value::Array(Vec::new()))
        .as_array_mut()
        .ok_or_else(|| anyhow!("Topics automation {name} must be an array"))
}

fn object<'a>(value: &'a Value, label: &str) -> Result<&'a Map<String, Value>> {
    value
        .as_object()
        .ok_or_else(|| anyhow!("{label} must be a JSON object"))
}

impl AutomationTemplate {
    pub(crate) fn active(&self) -> bool {
        self.config.get("status").and_then(Value::as_str) != Some("paused")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn function(id: &str, slug: &str, function_type: &str, data: Value) -> Function {
        Function {
            id: id.to_string(),
            name: slug.to_string(),
            slug: slug.to_string(),
            project_id: "test-project-id".to_string(),
            description: None,
            function_type: Some(function_type.to_string()),
            prompt_data: None,
            function_data: Some(data),
            tags: None,
            function_schema: None,
            metadata: None,
            created: None,
            _xact_id: None,
        }
    }

    fn automation(name: &str, config: Value) -> ProjectAutomation {
        ProjectAutomation {
            id: format!("test-{name}-id"),
            project_id: "test-project-id".to_string(),
            name: name.to_string(),
            description: None,
            config,
        }
    }

    #[test]
    fn active_observability_pull_is_portable_and_maps_topics() {
        let functions = vec![
            function(
                "fn-test-preprocessor",
                "test-preprocessor",
                "preprocessor",
                json!({"type": "code", "data": {"type": "inline", "code": "return input"}}),
            ),
            function(
                "fn-test-facet",
                "test-facet",
                "facet",
                json!({
                    "type": "facet",
                    "prompt": "Classify this trace",
                    "preprocessor": {"type": "function", "id": "fn-test-preprocessor"}
                }),
            ),
            function(
                "fn-test-topic-map",
                "test-facet-topic-map",
                "classifier",
                json!({
                    "type": "topic_map",
                    "source_facet": "legacy-name",
                    "source_facet_function": {"type": "function", "id": "fn-test-facet"},
                    "embedding_model": "test-embedding-model"
                }),
            ),
        ];
        let automations = vec![
            automation(
                "Topics",
                json!({
                    "event_type": "topic",
                    "topic_map_functions": [{"function": {"type": "function", "id": "fn-test-topic-map"}}]
                }),
            ),
            automation(
                "Test Loop",
                json!({
                    "event_type": "windowed",
                    "window": {},
                    "loop": {},
                    "actions": [{"type": "webhook", "url": "https://example.invalid/hook"}]
                }),
            ),
        ];

        let template = from_remote(&functions, &automations).expect("portable template");
        let value = serde_json::to_value(&template).expect("serialize");

        assert_eq!(
            template.facets[0].topics_automation.as_deref(),
            Some("Topics")
        );
        assert_eq!(
            template.facets[0].function_data["preprocessor"],
            json!({"type": "function", "slug": "test-preprocessor"})
        );
        assert_eq!(
            template.facets[0].preprocessor.as_ref().unwrap().slug,
            "test-preprocessor"
        );
        assert!(value["facets"][0].get("id").is_none());
        assert!(value["automations"][0]["config"].get("actions").is_none());
    }

    #[test]
    fn active_observability_pull_bundles_shared_preprocessor_once() {
        let shared = PortableFunction {
            name: "Shared preprocessor".to_string(),
            slug: "shared-preprocessor".to_string(),
            description: None,
            function_data: json!({"type": "code", "data": {"type": "inline"}}),
            prompt_data: None,
            tags: None,
            function_schema: None,
        };
        let facet = |name: &str, slug: &str| FacetTemplate {
            name: name.to_string(),
            slug: slug.to_string(),
            topics_automation: Some("Topics".to_string()),
            description: None,
            preprocessor: Some(shared.clone()),
            function_data: json!({
                "type": "facet",
                "preprocessor": {"type": "function", "slug": "shared-preprocessor"}
            }),
            prompt_data: None,
            tags: None,
            function_schema: None,
        };
        let mut facets = vec![
            facet("First facet", "first-facet"),
            facet("Second facet", "second-facet"),
        ];

        deduplicate_preprocessors(&mut facets);

        assert!(facets[0].preprocessor.is_some());
        assert!(facets[1].preprocessor.is_none());
        assert!(facets.iter().all(|facet| {
            facet.function_data["preprocessor"]
                == json!({"type": "function", "slug": "shared-preprocessor"})
        }));
    }

    #[test]
    fn active_observability_force_conversions_preserve_customization_and_actions() {
        let topic_map = function(
            "fn-test-topic-map",
            "test-facet-topic-map",
            "classifier",
            json!({
                "type": "topic_map",
                "source_facet": "Old",
                "embedding_model": "custom-model",
                "generation_settings": {"algorithm": "kmeans"},
                "report_key": "remote-report"
            }),
        );
        let facet: FacetTemplate = serde_json::from_value(json!({
            "name": "Test facet", "slug": "test-facet", "function_data": {"type": "facet", "prompt": "Test"}
        })).unwrap();
        let request = reconciled_topic_map_request(&topic_map, &facet, "fn-test-facet").unwrap();
        assert_eq!(request["function_data"]["embedding_model"], "custom-model");
        assert_eq!(
            request["function_data"]["generation_settings"]["algorithm"],
            "kmeans"
        );
        assert_eq!(request["function_data"]["report_key"], "remote-report");
        assert_eq!(request["function_data"]["source_facet"], "Test facet");
        assert_eq!(
            request["function_data"]["source_facet_function"],
            json!({"type": "function", "id": "fn-test-facet"})
        );

        let config = loop_config_for_target(
            &json!({"event_type": "windowed", "window": {}, "loop": {}, "actions": ["source"]}),
            Some(&json!({"event_type": "windowed", "loop": {}, "actions": ["target"]})),
        )
        .unwrap();
        assert_eq!(config["actions"], json!(["target"]));
    }

    #[test]
    fn active_observability_topics_update_is_idempotent() {
        let config = json!({
            "event_type": "topic",
            "custom": {"keep": true},
            "facet_functions": [{"type": "function", "id": "fn-test-facet"}],
            "topic_map_functions": []
        });
        let pairs = vec![("fn-test-facet".to_string(), "fn-test-topic-map".to_string())];
        let once = add_topics_functions(&config, &pairs).unwrap();
        let twice = add_topics_functions(&once, &pairs).unwrap();
        assert_eq!(once, twice);
        assert_eq!(twice["custom"]["keep"], true);
        assert_eq!(twice["facet_functions"].as_array().unwrap().len(), 1);
        assert_eq!(twice["topic_map_functions"].as_array().unwrap().len(), 1);
    }
}
