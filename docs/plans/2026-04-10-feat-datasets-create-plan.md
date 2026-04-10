---
title: "feat: Add datasets create command"
type: feat
date: 2026-04-10
---

# feat: Add datasets create command

Add `bt datasets create <name>` to create an empty dataset with an optional description, following the `projects create` pattern.

## Acceptance Criteria

- [x] `bt datasets create my-dataset` creates a dataset in the current project
- [x] `bt datasets create my-dataset --description "some desc"` sets a description
- [x] `bt datasets create` with no name in interactive mode prompts for name
- [x] `bt datasets create` with no name in non-interactive mode shows usage error
- [x] Creating a dataset that already exists shows an error
- [x] `bt datasets create --help` shows usage

## Context

- Create pattern: `src/projects/create.rs`
- API: `POST /v1/dataset` with `{ name, project_id, org_name, description }`
- Existing create in sync: `src/sync.rs:3201`
- Dataset API module: `src/datasets/api.rs`
- Dataset mod: `src/datasets/mod.rs`

## MVP

### src/datasets/api.rs

Add `create_dataset` function:

```rust
pub async fn create_dataset(
    client: &ApiClient,
    project_id: &str,
    name: &str,
    description: Option<&str>,
) -> Result<Dataset> {
    let mut body = serde_json::json!({
        "name": name,
        "project_id": project_id,
        "org_name": client.org_name(),
    });
    if let Some(desc) = description {
        body["description"] = serde_json::Value::String(desc.to_string());
    }
    client.post("/v1/dataset", &body).await
}
```

### src/datasets/create.rs

Follow `projects/create.rs` pattern:

- Accept optional name (positional or prompt interactively)
- Accept optional `--description` flag
- Check if dataset already exists by name
- Create via API
- Print success/failure status

### src/datasets/mod.rs

- Add `mod create;`
- Add `Create(CreateArgs)` variant to `DatasetsCommands`
- Add `CreateArgs` struct with `name: Option<String>` and `description: Option<String>`
- Add dispatch arm
- Update after_help examples

## References

- `src/projects/create.rs` — create pattern to follow
- `src/datasets/api.rs` — existing API module to extend
- `src/datasets/mod.rs` — command registration
