use std::{collections::HashMap, time::Duration};

use serde::Deserialize;
use serde_json::Value;

use crate::{http::build_http_client, project_context::ProjectContext};

const MODEL_CATALOG_TIMEOUT: Duration = Duration::from_secs(5);

/// The model metadata used by the web UI to decide which controls and ranges
/// to expose. Unknown fields are intentionally ignored so newer app versions
/// can extend the catalog without breaking older CLI versions.
#[derive(Debug, Clone, Deserialize)]
pub(crate) struct ModelSpec {
    pub(crate) format: String,
    #[serde(rename = "flavor")]
    pub(crate) _flavor: String,
    #[serde(default, rename = "displayName")]
    pub(crate) display_name: Option<String>,
    #[serde(default)]
    pub(crate) o1_like: Option<bool>,
    #[serde(default)]
    pub(crate) reasoning: Option<bool>,
    #[serde(default)]
    pub(crate) reasoning_budget: Option<bool>,
    #[serde(default)]
    pub(crate) max_output_tokens: Option<u64>,
}

impl ModelSpec {
    pub(crate) fn supports_reasoning(&self, model: &str) -> bool {
        if self.reasoning.unwrap_or(false) || self.o1_like.unwrap_or(false) {
            return true;
        }

        // Match `modelProviderHasReasoning` from the web model catalog. The UI
        // applies these fallbacks to custom models that omit `reasoning`.
        let lower = model.to_ascii_lowercase();
        match self.format.as_str() {
            "openai" => {
                ["o1", "o2", "o3", "o4"]
                    .iter()
                    .any(|prefix| lower.starts_with(prefix))
                    || lower.contains("gpt-5")
            }
            "anthropic" => lower.starts_with("claude-3.7"),
            "google" => lower.ends_with("gemini-2.0-flash") || lower.contains("gemini-2.5"),
            _ => false,
        }
    }
}

#[derive(Debug, Deserialize)]
struct SecretWithMetadata {
    #[serde(default)]
    metadata: Option<Value>,
}

#[derive(Debug, Deserialize)]
struct SecretListResponse {
    objects: Vec<SecretWithMetadata>,
}

/// Resolve a model from the same shared catalog and configured custom-model
/// metadata used by the prompt UI. Project custom models take precedence over
/// org custom models, which take precedence over the shared catalog. Failure
/// to load metadata is non-fatal; callers then apply only provider-independent
/// validation so arbitrary/custom model names remain usable.
pub(crate) async fn resolve_model_spec(ctx: &ProjectContext, model: &str) -> Option<ModelSpec> {
    let http = build_http_client(MODEL_CATALOG_TIMEOUT).ok()?;
    let app_url = ctx.app_url.trim_end_matches('/');
    let catalog_url = format!("{app_url}/api/models/model_list.json");
    let org_secrets_url = format!("{app_url}/api/ai_secret/get");
    let project_secrets_path = format!(
        "/v1/env_var?object_type=project&object_id={}&secret_category=ai_provider",
        urlencoding::encode(&ctx.project.id),
    );

    let catalog_request = async {
        let response = http.get(catalog_url).send().await.ok()?;
        if !response.status().is_success() {
            return None;
        }
        response.json::<HashMap<String, ModelSpec>>().await.ok()
    };
    let org_selector = if ctx.client.org_id().trim().is_empty() {
        serde_json::json!({ "org_name": ctx.client.org_name() })
    } else {
        serde_json::json!({ "org_id": ctx.client.org_id() })
    };
    let org_models_request = async {
        let response = http
            .post(org_secrets_url)
            .bearer_auth(ctx.client.api_key())
            .json(&org_selector)
            .send()
            .await
            .ok()?;
        if !response.status().is_success() {
            return None;
        }
        let secrets = response.json::<Vec<SecretWithMetadata>>().await.ok()?;
        Some(custom_models_from_secrets(secrets))
    };
    let project_models_request = async {
        let response = tokio::time::timeout(
            MODEL_CATALOG_TIMEOUT,
            ctx.client.get::<SecretListResponse>(&project_secrets_path),
        )
        .await
        .ok()?
        .ok()?;
        Some(custom_models_from_secrets(response.objects))
    };

    let (catalog, org_models, project_models) =
        tokio::join!(catalog_request, org_models_request, project_models_request);

    let mut models = catalog.unwrap_or_default();
    models.extend(org_models.unwrap_or_default());
    models.extend(project_models.unwrap_or_default());

    resolve_from_models(&models, model).cloned()
}

fn custom_models_from_secrets(secrets: Vec<SecretWithMetadata>) -> HashMap<String, ModelSpec> {
    secrets
        .into_iter()
        .filter_map(|secret| secret.metadata)
        .filter_map(|metadata| metadata.get("customModels").cloned())
        .filter_map(|models| serde_json::from_value::<HashMap<String, ModelSpec>>(models).ok())
        .flatten()
        .collect()
}

fn resolve_from_models<'a>(
    models: &'a HashMap<String, ModelSpec>,
    model: &str,
) -> Option<&'a ModelSpec> {
    models.get(model).or_else(|| {
        models
            .values()
            .find(|spec| spec.display_name.as_deref() == Some(model))
    })
}

#[cfg(test)]
mod tests {
    use actix_web::{dev::ServerHandle, web, App, HttpResponse, HttpServer};

    use braintrust_sdk_rust::LoginState;

    use crate::{auth::LoginContext, http::ApiClient, projects::api::Project};

    use super::*;

    fn spec(format: &str, display_name: Option<&str>) -> ModelSpec {
        ModelSpec {
            format: format.to_string(),
            _flavor: "chat".to_string(),
            display_name: display_name.map(ToOwned::to_owned),
            o1_like: None,
            reasoning: None,
            reasoning_budget: None,
            max_output_tokens: None,
        }
    }

    #[test]
    fn extracts_custom_models_from_secret_metadata() {
        let secrets = vec![SecretWithMetadata {
            metadata: Some(serde_json::json!({
                "customModels": {
                    "test-custom-model": {
                        "format": "anthropic",
                        "flavor": "chat",
                        "max_output_tokens": 4096
                    }
                }
            })),
        }];

        let models = custom_models_from_secrets(secrets);
        let model = models.get("test-custom-model").expect("custom model");
        assert_eq!(model.format, "anthropic");
        assert_eq!(model.max_output_tokens, Some(4096));
    }

    #[test]
    fn applies_web_ui_reasoning_fallbacks_for_custom_models() {
        let openai = spec("openai", None);
        assert!(openai.supports_reasoning("o3"));
        assert!(openai.supports_reasoning("test-gpt-5-deployment"));
        assert!(!openai.supports_reasoning("gpt-4.1"));

        let anthropic = spec("anthropic", None);
        assert!(anthropic.supports_reasoning("claude-3.7-sonnet"));
    }

    #[test]
    fn resolves_catalog_models_by_id_or_display_name() {
        let models = HashMap::from([(
            "test-model-id".to_string(),
            spec("openai", Some("Test model")),
        )]);

        assert!(resolve_from_models(&models, "test-model-id").is_some());
        assert!(resolve_from_models(&models, "Test model").is_some());
        assert!(resolve_from_models(&models, "missing").is_none());
    }

    struct MockServer {
        base_url: String,
        handle: ServerHandle,
    }

    impl MockServer {
        async fn start() -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock server");
            let address = listener.local_addr().expect("mock server address");
            let base_url = format!("http://{address}");
            let server = HttpServer::new(|| {
                App::new()
                    .route(
                        "/api/models/model_list.json",
                        web::get().to(|| async {
                            HttpResponse::Ok().json(serde_json::json!({
                                "test-shared-model": {
                                    "format": "openai",
                                    "flavor": "chat",
                                    "max_output_tokens": 100
                                }
                            }))
                        }),
                    )
                    .route(
                        "/api/ai_secret/get",
                        web::post().to(|| async {
                            HttpResponse::Ok().json(serde_json::json!([{
                                "metadata": {
                                    "customModels": {
                                        "test-precedence-model": {
                                            "format": "anthropic",
                                            "flavor": "chat",
                                            "max_output_tokens": 200
                                        }
                                    }
                                }
                            }]))
                        }),
                    )
                    .route(
                        "/v1/env_var",
                        web::get().to(|| async {
                            HttpResponse::Ok().json(serde_json::json!({
                                "objects": [{
                                    "metadata": {
                                        "customModels": {
                                            "test-precedence-model": {
                                                "format": "google",
                                                "flavor": "chat",
                                                "max_output_tokens": 300
                                            }
                                        }
                                    }
                                }]
                            }))
                        }),
                    )
            })
            .workers(1)
            .listen(listener)
            .expect("listen mock server")
            .run();
            let handle = server.handle();
            tokio::spawn(server);
            Self { base_url, handle }
        }

        async fn stop(self) {
            self.handle.stop(false).await;
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resolves_shared_and_custom_models_with_project_precedence() {
        let server = MockServer::start().await;
        let login = LoginState::new();
        login.set(
            "test-key".to_string(),
            "test-org-id".to_string(),
            "test-org".to_string(),
            server.base_url.clone(),
            server.base_url.clone(),
        );
        let client = ApiClient::new(&LoginContext {
            login,
            api_url: server.base_url.clone(),
            app_url: server.base_url.clone(),
        })
        .expect("API client");
        let ctx = ProjectContext {
            client,
            app_url: server.base_url.clone(),
            project: Project {
                id: "test-project-id".to_string(),
                name: "test-project".to_string(),
                org_id: "test-org-id".to_string(),
                description: None,
            },
        };

        let shared = resolve_model_spec(&ctx, "test-shared-model")
            .await
            .expect("shared model");
        assert_eq!(shared.format, "openai");
        assert_eq!(shared.max_output_tokens, Some(100));

        let custom = resolve_model_spec(&ctx, "test-precedence-model")
            .await
            .expect("custom model");
        assert_eq!(custom.format, "google");
        assert_eq!(custom.max_output_tokens, Some(300));

        server.stop().await;
    }
}
