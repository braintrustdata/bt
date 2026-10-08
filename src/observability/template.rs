use std::collections::{HashMap, HashSet};

use anyhow::{anyhow, bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{json, Map, Value};

use crate::{
    functions::api::Function,
    topics::api::{new_topic_automation_window_seconds, ProjectAutomation},
};

pub(crate) const KIND: &str = "active_observability_template";
pub(crate) const SCHEMA_VERSION: u32 = 2;
pub(crate) const DEFAULT_EMBEDDING_MODEL: &str = "brain-embedding-1";
pub(crate) const DEFAULT_TOPICS_DESCRIPTION: &str =
    "Automatically extract facets and classify logs using topic maps";
const WIRING_KEYS: [&str; 2] = ["facet_functions", "topic_map_functions"];

#[derive(Debug, Clone, Deserialize, Serialize, PartialEq)]
pub(crate) struct ActiveObservabilityTemplate {
    pub kind: String,
    pub schema_version: u32,
    #[serde(default)]
    pub facets: Vec<FacetTemplate>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub topics_automations: Vec<AutomationTemplate>,
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
    pub topic_map_btql_filter: Option<String>,
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

#[derive(Debug, Clone, Default)]
pub(crate) enum TopicMapFilter {
    #[default]
    Preserve,
    Replace(Option<String>),
}

pub(crate) struct FacetImport<'a> {
    pub facet: &'a FacetTemplate,
    pub settings: Option<&'a AutomationTemplate>,
    pub filter: TopicMapFilter,
}

impl ActiveObservabilityTemplate {
    pub(crate) fn facet_imports(&self) -> Result<Vec<FacetImport<'_>>> {
        validate(self)?;
        Ok(self
            .facets
            .iter()
            .map(|facet| FacetImport {
                facet,
                settings: self.topics_automations.iter().find(|settings| {
                    facet.topics_automation.as_deref() == Some(settings.name.as_str())
                }),
                filter: if self.schema_version == SCHEMA_VERSION
                    && (facet.topics_automation.is_some() || facet.topic_map_btql_filter.is_some())
                {
                    TopicMapFilter::Replace(facet.topic_map_btql_filter.clone())
                } else {
                    TopicMapFilter::Preserve
                },
            })
            .collect())
    }
}

#[derive(Serialize)]
struct PortableFunctionRequest<'a> {
    project_id: &'a str,
    name: &'a str,
    slug: &'a str,
    description: Option<&'a str>,
    function_type: &'a str,
    function_data: &'a Value,
    prompt_data: Option<&'a Value>,
    tags: Option<&'a [String]>,
    function_schema: Option<&'a Value>,
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

    let mut topics_automations = automations
        .iter()
        .filter(|automation| {
            is_topics(&automation.config)
                && facet_templates.iter().any(|facet| {
                    facet.topics_automation.as_deref() == Some(automation.name.as_str())
                })
        })
        .map(|automation| {
            Ok(AutomationTemplate {
                name: automation.name.clone(),
                description: automation.description.clone(),
                config: topics_settings(&automation.config)?,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    topics_automations.sort_by(|a, b| a.name.cmp(&b.name));

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
        topics_automations,
        automations: loop_templates,
    })
}

fn facet_from_remote(
    facet: &Function,
    topics: Option<(String, Option<String>)>,
    by_id: &HashMap<&str, &Function>,
) -> Result<FacetTemplate> {
    let mut function_data = facet
        .function_data
        .clone()
        .ok_or_else(|| anyhow!("facet '{}' is missing function_data", facet.name))?;
    validate_facet_data(&function_data, &facet.name)?;

    let preprocessor = saved_preprocessor(&mut function_data, by_id)?;
    Ok(FacetTemplate {
        name: facet.name.clone(),
        slug: facet.slug.clone(),
        topics_automation: topics.as_ref().map(|(name, _)| name.clone()),
        topic_map_btql_filter: topics.and_then(|(_, filter)| filter),
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
) -> Result<HashMap<String, (String, Option<String>)>> {
    let mut names: HashMap<String, HashMap<String, Option<String>>> = HashMap::new();
    for automation in automations
        .iter()
        .filter(|automation| is_topics(&automation.config))
    {
        for facet in facets {
            let oldest = topic_map_entries(&automation.config)
                .filter_map(|(id, filter)| by_id.get(id).map(|map| (*map, filter)))
                .filter(|(map, _)| {
                    map.function_type.as_deref() == Some("classifier")
                        && map
                            .function_data
                            .as_ref()
                            .and_then(|data| data.get("type"))
                            .and_then(Value::as_str)
                            == Some("topic_map")
                        && topic_map_matches(map, facet)
                })
                .min_by(|(left, _), (right, _)| topic_map_order(left, right));
            if let Some((_, filter)) = oldest {
                names
                    .entry(facet.id.clone())
                    .or_default()
                    .insert(automation.name.clone(), filter.map(str::to_string));
            }
        }
    }
    // A map is the authoritative destination. Direct extraction membership is
    // only a fallback for facets with no attached map anywhere in the project.
    let mapped_facets = names.keys().cloned().collect::<HashSet<_>>();
    for facet in facets
        .iter()
        .copied()
        .filter(|facet| !mapped_facets.contains(&facet.id))
    {
        for automation in automations
            .iter()
            .filter(|automation| is_topics(&automation.config))
        {
            if automation
                .config
                .get("facet_functions")
                .and_then(Value::as_array)
                .is_some_and(|references| {
                    references
                        .iter()
                        .any(|reference| function_ref_id(reference) == Some(facet.id.as_str()))
                })
            {
                names
                    .entry(facet.id.clone())
                    .or_default()
                    .entry(automation.name.clone())
                    .or_insert(None);
            }
        }
    }

    names
        .into_iter()
        .map(|(facet_id, names)| {
            if names.len() != 1 {
                let mut names = names.into_keys().collect::<Vec<_>>();
                names.sort();
                bail!(
                    "facet '{}' belongs to multiple Topics automations ({}); use one Topics destination per facet",
                    by_id.get(facet_id.as_str()).map_or(facet_id.as_str(), |facet| facet.name.as_str()),
                    names.join(", ")
                );
            }
            Ok((facet_id, names.into_iter().next().expect("one name")))
        })
        .collect()
}

pub(crate) fn topic_map_order(left: &Function, right: &Function) -> std::cmp::Ordering {
    left.created
        .cmp(&right.created)
        .then_with(|| left.id.cmp(&right.id))
}

pub(crate) fn topic_map_matches(topic_map: &Function, facet: &Function) -> bool {
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
    if !matches!(template.schema_version, 1 | SCHEMA_VERSION) {
        bail!(
            "unsupported template schema version {}; supported versions are 1 and {SCHEMA_VERSION}",
            template.schema_version
        );
    }
    if template.schema_version == 1
        && (!template.topics_automations.is_empty()
            || template
                .facets
                .iter()
                .any(|facet| facet.topic_map_btql_filter.is_some()))
    {
        bail!("Topics automation settings and topic map filters require template schema version {SCHEMA_VERSION}");
    }

    let mut slugs = HashMap::<String, &'static str>::new();
    let mut preprocessors = HashMap::<String, &PortableFunction>::new();
    for facet in &template.facets {
        require_text(&facet.name, "facet name")?;
        require_text(&facet.slug, "facet slug")?;
        validate_facet_data(&facet.function_data, &facet.name)?;
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
    for automation in &template.topics_automations {
        require_text(&automation.name, "Topics automation name")?;
        if !automation_names.insert(automation.name.as_str()) {
            bail!(
                "template contains duplicate automation name '{}'",
                automation.name
            );
        }
        if !is_topics(&automation.config) {
            bail!(
                "automation '{}' is not a Topics automation",
                automation.name
            );
        }
        let config = object(&automation.config, "Topics automation settings")?;
        if WIRING_KEYS.iter().any(|key| config.contains_key(*key)) {
            bail!("Topics automation '{}' must omit project-specific function references; wiring is generated from facets", automation.name);
        }
        if config
            .get("data_scope")
            .and_then(|scope| scope.get("type"))
            .and_then(Value::as_str)
            == Some("experiment")
        {
            let facets = template
                .facets
                .iter()
                .filter(|facet| {
                    facet.topics_automation.as_deref() == Some(automation.name.as_str())
                })
                .map(|facet| format!("'{}'", facet.name))
                .collect::<Vec<_>>()
                .join(", ");
            bail!("Topics automation '{}' attached to facets [{}] uses a project-specific experiment_id; portable templates require project_logs or project_experiments data_scope. Run `bt observability template pull` in an interactive terminal without --json or --no-input and deselect these facets", automation.name, facets);
        }
        new_topic_automation_window_seconds(&automation.config).with_context(|| {
            format!(
                "invalid timing settings for Topics automation '{}'",
                automation.name
            )
        })?;
        if !template
            .facets
            .iter()
            .any(|facet| facet.topics_automation.as_deref() == Some(automation.name.as_str()))
        {
            bail!(
                "Topics automation '{}' is not referenced by a template facet",
                automation.name
            );
        }
    }
    if template.schema_version == SCHEMA_VERSION {
        for facet in &template.facets {
            if let Some(name) = facet.topics_automation.as_deref() {
                if !automation_names.contains(name) {
                    bail!(
                        "facet '{}' references missing Topics automation settings '{name}'",
                        facet.name
                    );
                }
            }
        }
    }
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

fn validate_facet_data(data: &Value, name: &str) -> Result<()> {
    match data.get("type").and_then(Value::as_str) {
        Some("facet") => Ok(()),
        Some("code") => match data.get("data").and_then(|data| data.get("type")).and_then(Value::as_str) {
            Some("inline") => Ok(()),
            Some("bundle") => bail!("facet '{name}' uses bundled code, which observability templates cannot package; use an inline code facet"),
            _ => bail!("code facet '{name}' must use function_data.data.type 'inline'"),
        },
        _ => bail!("facet '{name}' function_data must be type 'facet' or inline 'code'"),
    }
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
        json!(PortableFunctionRequest {
            project_id,
            name: &self.name,
            slug: &self.slug,
            description: self.description.as_deref(),
            function_type,
            function_data: &self.function_data,
            prompt_data: self.prompt_data.as_ref(),
            tags: self.tags.as_deref(),
            function_schema: self.function_schema.as_ref(),
        })
    }
}

impl FacetTemplate {
    pub(crate) fn request(&self, project_id: &str, function_data: &Value) -> Value {
        json!(PortableFunctionRequest {
            project_id,
            name: &self.name,
            slug: &self.slug,
            description: self.description.as_deref(),
            function_type: "facet",
            function_data,
            prompt_data: self.prompt_data.as_ref(),
            tags: self.tags.as_deref(),
            function_schema: self.function_schema.as_ref(),
        })
    }

    pub(crate) fn active(&self) -> bool {
        self.topics_automation.is_some()
    }
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
    Ok(json!(PortableFunctionRequest {
        project_id: &existing.project_id,
        name: &facet.name,
        slug: &existing.slug,
        description: existing.description.as_deref(),
        function_type: "classifier",
        function_data: &Value::Object(data),
        prompt_data: existing.prompt_data.as_ref(),
        tags: existing.tags.as_deref(),
        function_schema: existing.function_schema.as_ref(),
    }))
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
        "backfill_time_range": "86400s",
    })
}

fn topics_settings(config: &Value) -> Result<Value> {
    let mut settings = object(config, "Topics automation config")?.clone();
    for key in WIRING_KEYS {
        settings.remove(key);
    }
    Ok(Value::Object(settings))
}

pub(crate) fn topics_config_for_target(settings: &Value, existing: &Value) -> Result<Value> {
    let mut config = object(settings, "Topics automation settings")?.clone();
    for key in WIRING_KEYS {
        config.insert(
            key.to_string(),
            existing.get(key).cloned().unwrap_or_else(|| json!([])),
        );
    }
    add_topics_functions(&Value::Object(config), &[])
}

pub(crate) fn retain_topics_dependencies(template: &mut ActiveObservabilityTemplate) {
    let names = template
        .facets
        .iter()
        .filter_map(|facet| facet.topics_automation.as_deref())
        .collect::<HashSet<_>>();
    template
        .topics_automations
        .retain(|automation| names.contains(automation.name.as_str()));
}

pub(crate) fn add_topics_functions(
    config: &Value,
    functions: &[(String, String, TopicMapFilter)],
) -> Result<Value> {
    let mut config = object(config, "Topics automation config")?.clone();
    let facets = array_entry(&mut config, "facet_functions")?;
    for (facet_id, _, _) in functions {
        if !facets
            .iter()
            .any(|entry| function_ref_id(entry) == Some(facet_id.as_str()))
        {
            facets.push(json!({"type": "function", "id": facet_id}));
        }
    }
    let topic_maps = array_entry(&mut config, "topic_map_functions")?;
    for (_, topic_map_id, filter) in functions {
        if !topic_maps.iter().any(|entry| {
            entry.get("function").and_then(function_ref_id) == Some(topic_map_id.as_str())
        }) {
            topic_maps.push(json!({"function": {"type": "function", "id": topic_map_id}}));
        }
        if let TopicMapFilter::Replace(filter) = filter {
            for entry in topic_maps.iter_mut().filter(|entry| {
                entry.get("function").and_then(function_ref_id) == Some(topic_map_id.as_str())
            }) {
                let entry = entry
                    .as_object_mut()
                    .expect("topic map reference is an object");
                match filter {
                    Some(filter) => {
                        entry.insert("btql_filter".to_string(), json!(filter));
                    }
                    None => {
                        entry.remove("btql_filter");
                    }
                }
            }
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

pub(crate) fn topic_map_ids(config: &Value) -> impl Iterator<Item = &str> {
    topic_map_entries(config).map(|(id, _)| id)
}

pub(crate) fn topic_map_entries(config: &Value) -> impl Iterator<Item = (&str, Option<&str>)> {
    config
        .get("topic_map_functions")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(|entry| {
            let id = entry.get("function").and_then(function_ref_id)?;
            Some((id, entry.get("btql_filter").and_then(Value::as_str)))
        })
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

    fn test_facet_and_maps() -> Vec<Function> {
        let mut oldest = function(
            "fn-test-old",
            "test-old-map",
            "classifier",
            json!({
                "type": "topic_map", "source_facet_function": {"type": "function", "id": "fn-test-facet"}
            }),
        );
        oldest.created = Some("2026-01-01T00:00:00Z".to_string());
        let mut newest = oldest.clone();
        newest.id = "fn-test-new".to_string();
        newest.slug = "test-new-map".to_string();
        newest.created = Some("2026-02-01T00:00:00Z".to_string());
        vec![
            function(
                "fn-test-facet",
                "test-facet",
                "facet",
                json!({"type": "facet"}),
            ),
            oldest,
            newest,
        ]
    }

    #[test]
    fn active_observability_export_uses_oldest_duplicate_map_filter() {
        let functions = test_facet_and_maps();
        for reverse in [false, true] {
            for filter_oldest in [false, true] {
                let mut entries = vec![
                    json!({"function": {"type": "function", "id": "fn-test-old"}}),
                    json!({"function": {"type": "function", "id": "fn-test-new"}}),
                ];
                entries[usize::from(!filter_oldest)]["btql_filter"] =
                    json!("metadata.test_selected = true");
                if reverse {
                    entries.reverse();
                }
                let template = from_remote(
                    &functions,
                    &[automation(
                        "Test Topics",
                        json!({
                            "event_type": "topic", "topic_map_functions": entries
                        }),
                    )],
                )
                .unwrap();
                validate(&template).unwrap();
                assert_eq!(
                    template.facets[0].topic_map_btql_filter.as_deref(),
                    filter_oldest.then_some("metadata.test_selected = true")
                );
            }
        }
        // Match the push tie breaker when creation timestamps are equal.
        let mut tied = functions;
        tied[2].created = tied[1].created.clone();
        assert_eq!(
            topic_map_order(&tied[1], &tied[2]),
            std::cmp::Ordering::Greater
        );
    }

    #[test]
    fn active_observability_export_prefers_map_over_direct_membership() {
        let functions = test_facet_and_maps();
        let direct = automation(
            "Test A",
            json!({
                "event_type": "topic", "facet_functions": [{"type": "function", "id": "fn-test-facet"}]
            }),
        );
        let mapped = automation(
            "Test B",
            json!({
                "event_type": "topic", "topic_map_functions": [{"function": {"type": "function", "id": "fn-test-old"}}]
            }),
        );
        for automations in [
            vec![direct.clone(), mapped.clone()],
            vec![mapped, direct.clone()],
        ] {
            let template = from_remote(&functions, &automations).unwrap();
            validate(&template).unwrap();
            assert_eq!(
                template.facets[0].topics_automation.as_deref(),
                Some("Test B")
            );
            assert_eq!(template.topics_automations.len(), 1);
        }
        let fallback = from_remote(&functions[..1], &[direct]).unwrap();
        assert_eq!(
            fallback.facets[0].topics_automation.as_deref(),
            Some("Test A")
        );
    }

    #[test]
    fn active_observability_validation_rejects_invalid_topics_timing() {
        let template = from_remote(&test_facet_and_maps()[..1], &[automation("Test Topics", json!({
            "event_type": "topic", "facet_functions": [{"type": "function", "id": "fn-test-facet"}],
            "backfill_time_range": "bogus"
        }))]).unwrap();
        let error = validate(&template).unwrap_err();
        let message = format!("{error:#}");
        assert!(message.contains("Test Topics"));
        assert!(message.contains("backfill_time_range"));
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
    fn active_observability_rejects_nonportable_or_unrelated_facet_data() {
        for (data, expected) in [
            (
                json!({"type": "code", "data": {"type": "bundle", "bundle_id": "test-bundle-id"}}),
                "cannot package",
            ),
            (
                json!({"type": "code", "data": {}}),
                "must use function_data.data.type 'inline'",
            ),
            (
                json!({"type": "topic_map"}),
                "must be type 'facet' or inline 'code'",
            ),
        ] {
            let facet = function("fn-test-facet", "test-facet", "facet", data.clone());
            assert!(from_remote(&[facet], &[])
                .unwrap_err()
                .to_string()
                .contains(expected));
            let template: ActiveObservabilityTemplate = serde_json::from_value(json!({
                "kind": KIND,
                "schema_version": SCHEMA_VERSION,
                "facets": [{"name": "Test facet", "slug": "test-facet", "function_data": data}]
            }))
            .unwrap();
            assert!(validate(&template)
                .unwrap_err()
                .to_string()
                .contains(expected));
        }
    }

    #[test]
    fn active_observability_topics_dependencies_follow_selected_facets() {
        let facets = vec![
            function(
                "fn-test-first",
                "test-first",
                "facet",
                json!({"type": "facet"}),
            ),
            function(
                "fn-test-second",
                "test-second",
                "facet",
                json!({"type": "facet"}),
            ),
            function(
                "fn-test-third",
                "test-third",
                "facet",
                json!({"type": "facet"}),
            ),
        ];
        let shared = automation(
            "Test shared Topics",
            json!({"event_type": "topic", "facet_functions": [{"type": "function", "id": facets[0].id}, {"type": "function", "id": facets[1].id}], "scope": {"type": "span"}, "sampling_rate": 1}),
        );
        let other = automation(
            "Test other Topics",
            json!({"event_type": "topic", "facet_functions": [{"type": "function", "id": facets[2].id}], "sampling_rate": 1}),
        );
        let mut template = from_remote(&facets, &[shared, other]).unwrap();
        assert_eq!(template.topics_automations.len(), 2);
        template.facets.retain(|facet| facet.slug == "test-second");
        retain_topics_dependencies(&mut template);
        validate(&template).unwrap();
        assert_eq!(template.topics_automations.len(), 1);
        assert_eq!(template.topics_automations[0].name, "Test shared Topics");
        template.facets.clear();
        retain_topics_dependencies(&mut template);
        assert!(template.topics_automations.is_empty());
    }

    #[test]
    fn active_observability_versions_and_topics_settings_are_validated() {
        let legacy = json!({"kind": KIND, "schema_version": 1, "facets": [{"name": "Test facet", "slug": "test-facet", "topics_automation": "Test Topics", "function_data": {"type": "facet"}}]});
        let mut template: ActiveObservabilityTemplate = serde_json::from_value(legacy).unwrap();
        validate(&template).unwrap();
        template.schema_version = SCHEMA_VERSION;
        assert!(validate(&template)
            .unwrap_err()
            .to_string()
            .contains("missing Topics automation settings"));
        template.topics_automations.push(automation_template());
        validate(&template).unwrap();
        template.schema_version = 1;
        assert!(validate(&template)
            .unwrap_err()
            .to_string()
            .contains("require template schema version"));
        template.schema_version = SCHEMA_VERSION;
        template.topics_automations[0].config["facet_functions"] =
            json!([{ "type": "function", "id": "fn-test-source" }]);
        assert!(validate(&template)
            .unwrap_err()
            .to_string()
            .contains("project-specific function references"));
        template.topics_automations[0]
            .config
            .as_object_mut()
            .unwrap()
            .remove("facet_functions");
        template.topics_automations[0].config["data_scope"] =
            json!({"type": "experiment", "experiment_id": "test-experiment-id"});
        assert!(validate(&template)
            .unwrap_err()
            .to_string()
            .contains("project-specific experiment_id"));
        template.topics_automations[0]
            .config
            .as_object_mut()
            .unwrap()
            .remove("data_scope");
        template.automations.push(AutomationTemplate {
            name: "Test Topics".to_string(),
            description: None,
            config: json!({"event_type": "windowed", "loop": {}}),
        });
        assert!(validate(&template)
            .unwrap_err()
            .to_string()
            .contains("duplicate automation name"));
    }

    fn automation_template() -> AutomationTemplate {
        AutomationTemplate {
            name: "Test Topics".to_string(),
            description: None,
            config: json!({"event_type": "topic", "sampling_rate": 1, "scope": {"type": "span"}}),
        }
    }

    #[test]
    fn active_observability_topic_map_filters_replace_or_clear_only_selected_maps() {
        let mut config = json!({"topic_map_functions": [
            {"function": {"type": "function", "id": "fn-test-selected"}, "btql_filter": "metadata.test_old = true"},
            {"function": {"type": "function", "id": "fn-test-other"}, "btql_filter": "metadata.test_other = true"}
        ]});
        config = add_topics_functions(
            &config,
            &[(
                "fn-test-facet".to_string(),
                "fn-test-selected".to_string(),
                TopicMapFilter::Replace(Some("metadata.test_new = true".to_string())),
            )],
        )
        .unwrap();
        assert_eq!(
            config["topic_map_functions"][0]["btql_filter"],
            "metadata.test_new = true"
        );
        let preserved = add_topics_functions(
            &config,
            &[(
                "fn-test-facet".to_string(),
                "fn-test-selected".to_string(),
                TopicMapFilter::Preserve,
            )],
        )
        .unwrap();
        assert_eq!(preserved, config);
        config = add_topics_functions(
            &config,
            &[(
                "fn-test-facet".to_string(),
                "fn-test-selected".to_string(),
                TopicMapFilter::Replace(None),
            )],
        )
        .unwrap();
        assert!(config["topic_map_functions"][0]
            .get("btql_filter")
            .is_none());
        assert_eq!(
            config["topic_map_functions"][1]["btql_filter"],
            "metadata.test_other = true"
        );
    }

    #[test]
    fn active_observability_topic_map_reconciliation_preserves_customization() {
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
    }

    #[test]
    fn active_observability_loop_replacement_preserves_target_actions() {
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
        let pairs = vec![(
            "fn-test-facet".to_string(),
            "fn-test-topic-map".to_string(),
            TopicMapFilter::Preserve,
        )];
        let once = add_topics_functions(&config, &pairs).unwrap();
        let twice = add_topics_functions(&once, &pairs).unwrap();
        assert_eq!(once, twice);
        assert_eq!(twice["custom"]["keep"], true);
        assert_eq!(twice["facet_functions"].as_array().unwrap().len(), 1);
        assert_eq!(twice["topic_map_functions"].as_array().unwrap().len(), 1);
    }
}
