use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};

use crate::{
    http::build_http_client,
    project_context::ProjectContext,
    utils::{bt_cache_root, write_bytes_atomic},
};

const MODEL_CATALOG_TIMEOUT: Duration = Duration::from_secs(5);
/// The catalog changes only on app deploy. A lookup miss refetches, so a newly
/// added custom model is still seen before the TTL expires.
const MODEL_CATALOG_TTL: Duration = Duration::from_secs(24 * 60 * 60);

/// The model metadata used by the web UI to decide which controls and ranges
/// to expose. Unknown fields are intentionally ignored so newer app versions
/// can extend the catalog without breaking older CLI versions.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct ModelSpec {
    #[serde(default)]
    pub(crate) format: String,
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

/// Both `Unknown` and `Unavailable` yield no spec, but only `Unavailable` means
/// the checks were skipped rather than deliberately not applicable.
#[derive(Debug, Clone)]
pub(crate) enum ModelLookup {
    Found(ModelSpec),
    /// Resolved from the on-disk cache after a fetch failed, so the spec may
    /// predate changes made in the web UI. Yields a spec, but any limit it
    /// implies is only as current as the last successful fetch.
    Stale(ModelSpec),
    /// Metadata loaded, but nothing defines this model.
    Unknown,
    /// Metadata could not be loaded; availability and ranges went unchecked.
    Unavailable,
}

impl ModelLookup {
    pub(crate) fn spec(&self) -> Option<&ModelSpec> {
        match self {
            Self::Found(spec) | Self::Stale(spec) => Some(spec),
            Self::Unknown | Self::Unavailable => None,
        }
    }

    pub(crate) fn is_unavailable(&self) -> bool {
        matches!(self, Self::Unavailable)
    }

    pub(crate) fn is_stale(&self) -> bool {
        matches!(self, Self::Stale(_))
    }
}

/// Resolve a model from the same shared catalog and configured custom-model
/// metadata used by the prompt UI. Project custom models take precedence over
/// org custom models, which take precedence over the shared catalog. Cached on
/// disk for [`MODEL_CATALOG_TTL`].
///
/// Missing metadata is non-fatal: callers fall back to provider-independent
/// validation so custom model names stay usable.
pub(crate) async fn resolve_model_lookup(
    ctx: &ProjectContext,
    model: &str,
    refresh: bool,
) -> ModelLookup {
    resolve_model_lookup_in(ctx, model, &catalog_cache_path(ctx), refresh).await
}

async fn resolve_model_lookup_in(
    ctx: &ProjectContext,
    model: &str,
    cache_path: &Path,
    refresh: bool,
) -> ModelLookup {
    if let Some(cached) =
        read_cached_catalog(cache_path, Some(MODEL_CATALOG_TTL)).filter(|_| !refresh)
    {
        if let Some(spec) = resolve_from_models(&cached, model) {
            return ModelLookup::Found(spec.clone());
        }
        // Miss: fall through in case the model was added since we cached.
    }

    let fetch = fetch_models(ctx).await;
    if fetch.catalog_loaded && fetch.custom_models_loaded {
        // A partial view would cache gaps as if they were absences.
        let _ = write_cached_catalog(cache_path, &fetch.models);
    }

    if fetch.custom_models_loaded {
        if let Some(spec) = resolve_from_models(&fetch.models, model) {
            return ModelLookup::Found(spec.clone());
        }
        if fetch.catalog_loaded {
            return ModelLookup::Unknown;
        }
    }

    // A partial fetch cannot establish precedence or absence. In particular, a
    // shared match may be overridden by custom metadata we failed to load. The
    // cache is consulted with no age bound here, including under `refresh`, so
    // the result is reported as stale rather than current.
    if let Some(cached) = read_cached_catalog(cache_path, None) {
        if let Some(spec) = resolve_from_models(&cached, model) {
            return ModelLookup::Stale(spec.clone());
        }
    }
    ModelLookup::Unavailable
}

#[derive(Debug, Default)]
struct CatalogFetch {
    models: HashMap<String, ModelSpec>,
    catalog_loaded: bool,
    custom_models_loaded: bool,
}

async fn fetch_models(ctx: &ProjectContext) -> CatalogFetch {
    let Ok(http) = build_http_client(MODEL_CATALOG_TIMEOUT) else {
        return CatalogFetch::default();
    };
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
        // Authentication and authorization failures do not prove that no
        // custom models exist, so every unsuccessful response is inconclusive.
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
        .ok()?;
        response
            .ok()
            .map(|response| custom_models_from_secrets(response.objects))
    };

    let (catalog, org_models, project_models) =
        tokio::join!(catalog_request, org_models_request, project_models_request);

    let catalog_loaded = catalog.is_some();
    let custom_models_loaded = org_models.is_some() && project_models.is_some();

    let mut models = catalog.unwrap_or_default();
    models.extend(org_models.unwrap_or_default());
    models.extend(project_models.unwrap_or_default());

    CatalogFetch {
        models,
        catalog_loaded,
        custom_models_loaded,
    }
}

#[derive(Debug, Deserialize)]
struct CachedCatalog {
    fetched_at: u64,
    models: HashMap<String, ModelSpec>,
}

#[derive(Debug, Serialize)]
struct CatalogToCache<'a> {
    fetched_at: u64,
    models: &'a HashMap<String, ModelSpec>,
}

/// Keyed by app URL, org, and project because custom models are org- and
/// project-scoped. Version in the path invalidates on [`ModelSpec`] changes.
fn catalog_cache_path(ctx: &ProjectContext) -> PathBuf {
    let mut hasher = Sha256::new();
    for part in [
        ctx.app_url.trim_end_matches('/'),
        ctx.client.org_id(),
        ctx.client.org_name(),
        ctx.project.id.as_str(),
    ] {
        hasher.update(part.as_bytes());
        hasher.update([0]);
    }
    let key = hasher
        .finalize()
        .iter()
        .take(8)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();

    bt_cache_root()
        .join("model-catalog")
        .join(env!("CARGO_PKG_VERSION"))
        .join(format!("{key}.json"))
}

/// `max_age` of `None` accepts a cache of any age.
fn read_cached_catalog(
    path: &Path,
    max_age: Option<Duration>,
) -> Option<HashMap<String, ModelSpec>> {
    let raw = std::fs::read(path).ok()?;
    let cached: CachedCatalog = serde_json::from_slice(&raw).ok()?;

    if let Some(max_age) = max_age {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        // Saturate: a future timestamp is clock skew, not infinite freshness.
        if Duration::from_secs(now.saturating_sub(cached.fetched_at)) > max_age {
            return None;
        }
    }

    Some(cached.models)
}

fn write_cached_catalog(path: &Path, models: &HashMap<String, ModelSpec>) -> Result<()> {
    let fetched_at = SystemTime::now().duration_since(UNIX_EPOCH)?.as_secs();
    let payload = serde_json::to_vec(&CatalogToCache { fetched_at, models })?;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    write_bytes_atomic(path, &payload)
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
    use actix_web::{dev::ServerHandle, http::StatusCode, web, App, HttpResponse, HttpServer};

    use braintrust_sdk_rust::LoginState;

    use crate::{auth::LoginContext, http::ApiClient, projects::api::Project};

    use super::*;

    fn spec(format: &str, display_name: Option<&str>) -> ModelSpec {
        ModelSpec {
            format: format.to_string(),
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
            Self::start_with_org_status(StatusCode::OK).await
        }

        async fn start_with_org_status(org_status: StatusCode) -> Self {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").expect("bind mock server");
            let address = listener.local_addr().expect("mock server address");
            let base_url = format!("http://{address}");
            let server = HttpServer::new(move || {
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
                        web::post().to(move || async move {
                            if !org_status.is_success() {
                                return HttpResponse::build(org_status).finish();
                            }
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

    fn test_context(base_url: &str) -> ProjectContext {
        let login = LoginState::new();
        login.set(
            "test-key".to_string(),
            "test-org-id".to_string(),
            "test-org".to_string(),
            base_url.to_string(),
            base_url.to_string(),
        );
        let client = ApiClient::new(&LoginContext {
            login,
            api_url: base_url.to_string(),
            app_url: base_url.to_string(),
        })
        .expect("API client");
        ProjectContext {
            client,
            app_url: base_url.to_string(),
            project: Project {
                id: "test-project-id".to_string(),
                name: "test-project".to_string(),
                org_id: "test-org-id".to_string(),
                description: None,
            },
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn resolves_shared_and_custom_models_with_project_precedence() {
        let server = MockServer::start().await;
        let ctx = test_context(&server.base_url);
        let cache = tempfile::tempdir().expect("tempdir");
        let cache_path = cache.path().join("catalog.json");

        let shared = resolve_model_lookup_in(&ctx, "test-shared-model", &cache_path, false)
            .await
            .spec()
            .cloned()
            .expect("shared model");
        assert_eq!(shared.format, "openai");
        assert_eq!(shared.max_output_tokens, Some(100));

        let custom = resolve_model_lookup_in(&ctx, "test-precedence-model", &cache_path, false)
            .await
            .spec()
            .cloned()
            .expect("custom model");
        assert_eq!(custom.format, "google");
        assert_eq!(custom.max_output_tokens, Some(300));

        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_shared_match_is_unavailable_when_custom_models_cannot_be_loaded() {
        let server = MockServer::start_with_org_status(StatusCode::FORBIDDEN).await;
        let ctx = test_context(&server.base_url);
        let cache = tempfile::tempdir().expect("tempdir");

        let lookup = resolve_model_lookup_in(
            &ctx,
            "test-shared-model",
            &cache.path().join("catalog.json"),
            false,
        )
        .await;
        assert!(lookup.is_unavailable());
        assert!(lookup.spec().is_none());

        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_known_model_is_unknown_not_unavailable() {
        let server = MockServer::start().await;
        let ctx = test_context(&server.base_url);
        let cache = tempfile::tempdir().expect("tempdir");

        let lookup = resolve_model_lookup_in(
            &ctx,
            "test-absent-model",
            &cache.path().join("catalog.json"),
            false,
        )
        .await;
        assert!(matches!(lookup, ModelLookup::Unknown));
        assert!(!lookup.is_unavailable());

        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_fresh_cache_serves_lookups_after_the_server_is_gone() {
        let server = MockServer::start().await;
        let ctx = test_context(&server.base_url);
        let cache = tempfile::tempdir().expect("tempdir");
        let cache_path = cache.path().join("catalog.json");

        resolve_model_lookup_in(&ctx, "test-shared-model", &cache_path, false)
            .await
            .spec()
            .expect("warm the cache");
        server.stop().await;

        // Nothing is listening, so a hit can only be from the cache.
        let cached = resolve_model_lookup_in(&ctx, "test-shared-model", &cache_path, false)
            .await
            .spec()
            .cloned()
            .expect("cached model");
        assert_eq!(cached.max_output_tokens, Some(100));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn refresh_bypasses_a_fresh_cache() {
        let cache = tempfile::tempdir().expect("tempdir");
        let cache_path = cache.path().join("catalog.json");
        // A fresh cache claiming a model the server does not serve.
        write_cached_catalog(
            &cache_path,
            &HashMap::from([("test-only-in-cache".to_string(), spec("openai", None))]),
        )
        .expect("seed cache");

        let server = MockServer::start().await;
        let ctx = test_context(&server.base_url);

        assert!(
            resolve_model_lookup_in(&ctx, "test-only-in-cache", &cache_path, false)
                .await
                .spec()
                .is_some(),
            "the cached entry should satisfy a normal lookup"
        );
        assert!(
            resolve_model_lookup_in(&ctx, "test-only-in-cache", &cache_path, true)
                .await
                .spec()
                .is_none(),
            "refresh should ignore the cache and see only the server's catalog"
        );

        server.stop().await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unreachable_catalog_without_a_cache_is_unavailable() {
        // Port 9 refuses connections.
        let ctx = test_context("http://127.0.0.1:9");
        let cache = tempfile::tempdir().expect("tempdir");

        let lookup = resolve_model_lookup_in(
            &ctx,
            "gpt-4.1-mini",
            &cache.path().join("catalog.json"),
            false,
        )
        .await;
        assert!(lookup.is_unavailable());
        assert!(lookup.spec().is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_unreachable_catalog_falls_back_to_a_stale_cache() {
        let cache = tempfile::tempdir().expect("tempdir");
        let cache_path = cache.path().join("catalog.json");
        let models = HashMap::from([("test-stale-model".to_string(), spec("openai", None))]);
        write_cached_catalog(&cache_path, &models).expect("seed cache");
        // Backdated past the TTL, so only the stale-fallback path can hit.
        let stale = serde_json::json!({ "fetched_at": 0, "models": models });
        std::fs::write(&cache_path, serde_json::to_vec(&stale).unwrap()).expect("backdate cache");

        let ctx = test_context("http://127.0.0.1:9");
        let lookup = resolve_model_lookup_in(&ctx, "test-stale-model", &cache_path, false).await;
        assert_eq!(lookup.spec().expect("stale model").format, "openai");
    }

    #[test]
    fn cached_catalog_round_trips_and_expires() {
        let cache = tempfile::tempdir().expect("tempdir");
        let cache_path = cache.path().join("catalog.json");
        let models = HashMap::from([(
            "test-model-id".to_string(),
            spec("anthropic", Some("Test model")),
        )]);

        write_cached_catalog(&cache_path, &models).expect("write cache");
        let read = read_cached_catalog(&cache_path, Some(MODEL_CATALOG_TTL)).expect("fresh cache");
        assert_eq!(read["test-model-id"].format, "anthropic");
        assert_eq!(
            read["test-model-id"].display_name.as_deref(),
            Some("Test model")
        );

        // Past the TTL: rejected as fresh, still returned when unbounded.
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("clock")
            .as_secs();
        let stale = serde_json::json!({
            "fetched_at": now - MODEL_CATALOG_TTL.as_secs() - 60,
            "models": models,
        });
        std::fs::write(&cache_path, serde_json::to_vec(&stale).expect("encode"))
            .expect("backdate cache");
        assert!(read_cached_catalog(&cache_path, Some(MODEL_CATALOG_TTL)).is_none());
        assert!(read_cached_catalog(&cache_path, None).is_some());
    }

    #[test]
    fn cache_paths_are_scoped_per_org_and_project() {
        let a = catalog_cache_path(&test_context("https://app.test.example"));
        let mut other = test_context("https://app.test.example");
        other.project.id = "test-other-project".to_string();
        let b = catalog_cache_path(&other);

        assert_ne!(a, b);
        assert!(a.starts_with(bt_cache_root().join("model-catalog")));
        assert!(a.to_string_lossy().contains(env!("CARGO_PKG_VERSION")));
    }
}
