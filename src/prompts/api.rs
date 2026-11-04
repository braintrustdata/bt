use anyhow::Result;
use serde_json::{json, Value};

use crate::http::{ApiClient, BtqlOptions, BtqlResponse};

pub use braintrust_api_client::types::Prompt;

pub use braintrust_api_client::backend::extensions::EnvironmentObject;

pub async fn list_prompts(
    client: &ApiClient,
    project: &str,
    environment: Option<&str>,
) -> Result<Vec<Prompt>> {
    let response = client
        .run(|| {
            let mut request = client
                .generated()
                .get_prompt_builder()
                .org_name(client.org_name())
                .project_name(project);
            if let Some(environment) = environment {
                request = request.environment(environment);
            }
            request.send()
        })
        .await?;
    Ok(response.objects)
}

pub async fn get_prompt_by_slug(
    client: &ApiClient,
    project: &str,
    slug: &str,
    version: Option<&str>,
    environment: Option<&str>,
) -> Result<Option<Prompt>> {
    let version = version
        .map(crate::util_cmd::normalize_xact_id)
        .transpose()?;
    let response = client
        .run(|| {
            let mut request = client
                .generated()
                .get_prompt_builder()
                .org_name(client.org_name())
                .project_name(project)
                .slug(slug);
            if let Some(version) = version.as_deref() {
                request = request.version(version);
            }
            if let Some(environment) = environment {
                request = request.environment(environment);
            }
            request.send()
        })
        .await?;
    Ok(response.objects.into_iter().next())
}

pub async fn list_prompt_versions(
    client: &ApiClient,
    project_id: &str,
    prompt_id: &str,
) -> Result<Vec<String>> {
    let query = prompt_versions_query(project_id, prompt_id);
    let response: BtqlResponse<Value> = client
        .btql_with_options(
            &query,
            BtqlOptions {
                audit_log: Some(true),
                use_columnstore: Some(false),
                realtime: Some(true),
                ..Default::default()
            },
        )
        .await?;

    Ok(prompt_versions_from_rows(response.data))
}

fn prompt_versions_query(project_id: &str, prompt_id: &str) -> Value {
    json!({
            "from": {
                "op": "function",
                "name": { "op": "ident", "name": ["project_prompts"] },
                "args": [{ "op": "literal", "value": project_id }]
            },
            "select": [{ "op": "star" }],
            "sort": [{
                "expr": { "op": "ident", "name": ["_xact_id"] },
                "dir": "desc"
            }],
            "filter": {
                "op": "eq",
                "left": { "op": "ident", "name": ["id"] },
                "right": { "op": "literal", "value": prompt_id }
            }
    })
}

fn prompt_versions_from_rows(rows: Vec<Value>) -> Vec<String> {
    rows.into_iter()
        .filter(|row| {
            matches!(
                row.pointer("/audit_data/action").and_then(Value::as_str),
                Some("upsert" | "merge")
            )
        })
        .filter_map(|row| {
            let xact_id = row.get("_xact_id")?;
            let raw = xact_id
                .as_str()
                .map(ToOwned::to_owned)
                .or_else(|| xact_id.as_u64().map(|value| value.to_string()))?;
            let value = raw.parse::<u64>().ok()?;
            Some(crate::util_cmd::prettify_xact(value))
        })
        .collect()
}

pub async fn assign_prompt(
    client: &ApiClient,
    prompt_id: &str,
    environment: &str,
    object_version: &str,
) -> Result<EnvironmentObject> {
    client
        .assign_prompt(
            prompt_id,
            environment,
            &braintrust_api_client::backend::extensions::EnvironmentAssignment {
                object_version: object_version.to_owned(),
                org_name: client.org_name().to_owned(),
            },
        )
        .await
}

pub async fn unassign_prompt(
    client: &ApiClient,
    prompt_id: &str,
    environment: &str,
) -> Result<EnvironmentObject> {
    client.unassign_prompt(prompt_id, environment).await
}

pub async fn delete_prompt(client: &ApiClient, prompt_id: &str) -> Result<()> {
    client
        .run(|| client.generated().delete_prompt_id(prompt_id))
        .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_versions_request_scopes_audit_log_to_prompt() {
        let request = prompt_versions_query("proj_test", "prompt_test");

        assert_eq!(request["from"]["name"]["name"], json!(["project_prompts"]));
        assert_eq!(request["from"]["args"][0]["value"], "proj_test");
        assert_eq!(request["filter"]["right"]["value"], "prompt_test");
    }

    #[test]
    fn prompt_versions_include_upserts_and_merges() {
        let rows = vec![
            json!({"_xact_id": "1000192656880881099", "audit_data": {"action": "upsert"}}),
            json!({"_xact_id": 1000192656880881100_u64, "audit_data": {"action": "merge"}}),
            json!({"_xact_id": "1000192656880881101", "audit_data": {"action": "delete"}}),
            json!({"audit_data": {"action": "upsert"}}),
        ];

        assert_eq!(
            prompt_versions_from_rows(rows),
            vec!["81cd05ee665fdfb3", "81cdc1302a2a586c"]
        );
    }
}
