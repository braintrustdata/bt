use crate::http::OperationOptions;
use std::collections::HashSet;

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{
    error::user_error,
    http::{ApiClient, BtqlResponse},
};

fn escape_sql(s: &str) -> String {
    s.replace('\'', "''")
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionRow {
    pub id: String,
    pub name: String,
    pub slug: String,
    pub project_id: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub function_type: Option<String>,
    #[serde(default)]
    pub prompt_data: Option<serde_json::Value>,
    #[serde(default)]
    pub function_data: Option<serde_json::Value>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub function_schema: Option<serde_json::Value>,
    #[serde(default)]
    pub metadata: Option<serde_json::Value>,
    #[serde(default)]
    pub created: Option<String>,
    #[serde(default)]
    pub _xact_id: Option<String>,
}

pub use braintrust_api_client::backend::extensions::{
    CodeUploadSlot, FunctionListQuery, FunctionPage as FunctionListPage,
};
pub use braintrust_api_client::types::Function;

pub use braintrust_api_client::backend::extensions::InsertedFunction as InsertedFunctionResult;

pub use braintrust_api_client::backend::extensions::{
    FunctionValidationError, FunctionValidationReport,
};

#[derive(Debug, Clone)]
pub struct InsertFunctionsResult {
    pub ignored_entries: Option<usize>,
    pub xact_id: Option<String>,
    pub functions: Vec<InsertedFunctionResult>,
}

pub async fn list_functions(
    client: &ApiClient,
    project_id: &str,
    function_type: Option<&str>,
) -> Result<Vec<FunctionRow>> {
    let query = list_functions_query(project_id, function_type);
    let response: BtqlResponse<FunctionRow> = client.btql(&query).await?;

    Ok(response.data)
}

fn list_functions_query(project_id: &str, function_type: Option<&str>) -> String {
    let pid = escape_sql(project_id);
    let type_filter = match function_type {
        // Match the web UI's Scorers tab: label-producing classifiers appear
        // alongside score-producing scorers, while topic maps do not.
        Some("scorer") => "function_type IN ('scorer', 'classifier') \
            AND COALESCE(function_data.type, '') != 'topic_map' \
            AND (origin IS NULL OR NOT COALESCE(origin.internal, FALSE))"
            .to_string(),
        Some(ft) => {
            let ft = escape_sql(ft);
            format!("function_type = '{ft}'")
        }
        None => return format!("SELECT * FROM project_functions('{pid}')"),
    };

    format!("SELECT * FROM project_functions('{pid}') WHERE {type_filter}")
}

pub async fn get_function_by_slug(
    client: &ApiClient,
    project_id: &str,
    slug: &str,
    version: Option<&str>,
    environment: Option<&str>,
) -> Result<Option<Function>> {
    let query = FunctionListQuery {
        project_id: Some(project_id.to_string()),
        slug: Some(slug.to_string()),
        version: version.map(ToOwned::to_owned),
        environment: environment.map(ToOwned::to_owned),
        ..Default::default()
    };
    let page = list_functions_page(client, &query).await?;
    let Some(raw) = page.objects.into_iter().next() else {
        return Ok(None);
    };

    Ok(Some(raw))
}

pub async fn get_function_by_id(
    client: &ApiClient,
    id: &str,
    version: Option<&str>,
    environment: Option<&str>,
) -> Result<Option<Function>> {
    let query = FunctionListQuery {
        id: Some(id.to_string()),
        version: version.map(ToOwned::to_owned),
        environment: environment.map(ToOwned::to_owned),
        ..Default::default()
    };
    let page = list_functions_page(client, &query).await?;
    let Some(raw) = page.objects.into_iter().next() else {
        return Ok(None);
    };

    Ok(Some(raw))
}

pub async fn invoke_function(
    client: &ApiClient,
    body: &braintrust_api_client::backend::extensions::Invocation,
) -> Result<serde_json::Value> {
    let result = client
        .with_options(OperationOptions {
            timeout: Some(std::time::Duration::from_secs(300)),
            ..Default::default()
        })
        .invoke(body)
        .await;
    match result {
        Err(error)
            if braintrust_api_client::backend::is_provider_authentication_failure(&error) =>
        {
            Err(user_error(error))
        }
        result => result,
    }
}

#[cfg(test)]
use crate::http::HttpError;
#[cfg(test)]
fn is_provider_auth_response(error: &HttpError) -> bool {
    braintrust_api_client::backend::extensions::is_provider_auth_response(error)
}

pub async fn delete_function(client: &ApiClient, function_id: &str) -> Result<()> {
    client
        .run(|| client.generated().delete_function_id(function_id))
        .await?;
    Ok(())
}

pub async fn list_functions_page(
    client: &ApiClient,
    query: &FunctionListQuery,
) -> Result<FunctionListPage> {
    client.function_page(query).await
}

/// List a project's functions using the public API's cursor contract.
pub async fn list_all_functions(client: &ApiClient, project_id: &str) -> Result<Vec<Function>> {
    let mut query = FunctionListQuery {
        project_id: Some(project_id.to_string()),
        ..Default::default()
    };
    let mut functions = Vec::new();
    let mut cursors = HashSet::new();

    loop {
        let page = list_functions_page(client, &query).await?;
        if query.snapshot.is_none() {
            query.snapshot = page.snapshot;
        }
        functions.extend(page.objects);
        let Some(cursor) = page.next_cursor else {
            break;
        };
        if !cursors.insert(cursor.clone()) {
            anyhow::bail!("function pagination returned a repeated cursor");
        }
        query.cursor = Some(cursor);
    }

    Ok(functions)
}

pub async fn create_function(client: &ApiClient, body: &Value) -> Result<Function> {
    let request: braintrust_api_client::types::CreateFunction =
        braintrust_api_client::backend::resource_definition(body)?;
    client
        .run(|| client.generated().post_function(request.clone()))
        .await
}

pub async fn replace_function(client: &ApiClient, body: &Value) -> Result<Function> {
    let request: braintrust_api_client::types::CreateFunction =
        braintrust_api_client::backend::resource_definition(body)?;
    client
        .run(|| client.generated().put_function(request.clone()))
        .await
}

pub async fn request_code_upload_slot(
    client: &ApiClient,
    org_id: &str,
    runtime: &str,
    version: &str,
) -> Result<CodeUploadSlot> {
    let request = braintrust_api_client::backend::extensions::CodeUploadRequest {
        org_id: org_id.to_owned(),
        runtime_context: braintrust_api_client::backend::extensions::RuntimeContext {
            runtime: runtime.to_owned(),
            version: version.to_owned(),
        },
    };
    client.code_upload(&request).await
}

pub async fn upload_bundle(
    url: &str,
    bundle_bytes: Vec<u8>,
    content_encoding: Option<&str>,
) -> Result<()> {
    crate::http::put_signed_url(url, bundle_bytes, content_encoding)
        .await
        .context("failed to upload code bundle to signed URL")
}

pub async fn insert_functions(
    client: &ApiClient,
    functions: &[Value],
) -> Result<InsertFunctionsResult> {
    let body = insert_functions_body(functions);
    let response = client
        .insert_functions(&braintrust_api_client::backend::resource_definition(&body)?)
        .await?;
    let ignored_entries = response.ignored_count.or_else(|| {
        (response.functions.len() == functions.len()).then(|| {
            response
                .functions
                .iter()
                .zip(functions)
                .filter(|(result, request)| {
                    request.get("if_exists").and_then(Value::as_str) == Some("ignore")
                        && result.found_existing
                })
                .count()
        })
    });
    Ok(InsertFunctionsResult {
        ignored_entries,
        xact_id: response.xact_id,
        functions: response.functions,
    })
}

pub(crate) fn insert_functions_body(functions: &[Value]) -> Value {
    serde_json::json!({ "functions": functions })
}

#[cfg(test)]
fn ignored_count(raw: &Value) -> Option<usize> {
    raw.get("ignored_count")
        .and_then(Value::as_u64)
        .and_then(|count| usize::try_from(count).ok())
}

#[cfg(test)]
fn ignored_count_from_function_results(raw: &Value, requests: &[Value]) -> Option<usize> {
    let results = raw.get("functions")?.as_array()?;
    if results.len() != requests.len() {
        return None;
    }

    results
        .iter()
        .zip(requests)
        .try_fold(0usize, |count, (result, request)| {
            let should_ignore = request.get("if_exists").and_then(Value::as_str) == Some("ignore");
            if !should_ignore {
                return Some(count);
            }

            let found_existing = result.get("found_existing")?.as_bool()?;
            Some(count + usize::from(found_existing))
        })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn provider_auth_detection_requires_an_auth_status_and_provider_shape() {
        let provider_error = HttpError {
            status: reqwest::StatusCode::UNAUTHORIZED,
            body: serde_json::json!({
                "error": {
                    "message": "Incorrect API key provided: synthetic-key",
                    "code": "invalid_api_key"
                }
            })
            .to_string(),
        };
        assert!(is_provider_auth_response(&provider_error));

        let bad_request = HttpError {
            status: reqwest::StatusCode::BAD_REQUEST,
            body: provider_error.body,
        };
        assert!(!is_provider_auth_response(&bad_request));
    }

    #[test]
    fn scorer_list_query_matches_web_ui_filter() {
        let query = list_functions_query("test-project-id", Some("scorer"));

        assert!(query.contains("function_type IN ('scorer', 'classifier')"));
        assert!(query.contains("COALESCE(function_data.type, '') != 'topic_map'"));
        assert!(query.contains("origin IS NULL"));
        assert!(query.contains("origin.internal"));
    }

    #[test]
    fn non_scorer_list_query_keeps_exact_type_filter() {
        let query = list_functions_query("test-project-id", Some("tool"));

        assert!(query.contains("function_type = 'tool'"));
        assert!(!query.contains("classifier"));
    }

    #[test]
    fn ignored_count_extracts_canonical_shape() {
        let first = serde_json::json!({ "ignored_count": 3 });
        assert_eq!(ignored_count(&first), Some(3));

        let second = serde_json::json!({ "ignored": [1, 2] });
        assert_eq!(ignored_count(&second), None);

        let third = serde_json::json!({ "stats": { "ignored": 5 } });
        assert_eq!(ignored_count(&third), None);

        assert_eq!(ignored_count(&serde_json::json!({})), None);
    }

    #[test]
    fn derives_ignored_count_from_found_existing_results() {
        let requests = vec![
            serde_json::json!({ "slug": "first", "if_exists": "ignore" }),
            serde_json::json!({ "slug": "second", "if_exists": "replace" }),
            serde_json::json!({ "slug": "third", "if_exists": "ignore" }),
        ];
        let response = serde_json::json!({
            "functions": [
                { "slug": "first", "found_existing": true },
                { "slug": "second", "found_existing": true },
                { "slug": "third", "found_existing": false },
            ]
        });

        assert_eq!(
            ignored_count_from_function_results(&response, &requests),
            Some(1)
        );
    }

    #[test]
    fn ignored_count_fallback_rejects_mismatched_response_length() {
        let requests = vec![serde_json::json!({
            "slug": "first",
            "if_exists": "ignore"
        })];
        let response = serde_json::json!({ "functions": [] });

        assert_eq!(
            ignored_count_from_function_results(&response, &requests),
            None
        );
    }

    #[test]
    fn parses_insert_function_operation_fields() {
        let response: braintrust_api_client::backend::extensions::InsertFunctionsResponse =
            serde_json::from_value(serde_json::json!({
                "xact_id": "1000000000000000001",
                "functions": [{
                    "id": "fn_test_scorer",
                    "project_id": "test-project",
                    "slug": "test-scorer",
                    "found_existing": true
                }]
            }))
            .expect("insert response");

        assert_eq!(response.xact_id.as_deref(), Some("1000000000000000001"));
        assert_eq!(response.functions[0].id, "fn_test_scorer");
        assert_eq!(response.functions[0].project_id, "test-project");
        assert_eq!(response.functions[0].slug, "test-scorer");
        assert!(response.functions[0].found_existing);
    }

    #[test]
    fn insert_functions_body_wraps_functions_array() {
        let functions = vec![serde_json::json!({ "slug": "demo" })];
        let body = insert_functions_body(&functions);

        assert_eq!(body, serde_json::json!({ "functions": functions }));
    }
}
