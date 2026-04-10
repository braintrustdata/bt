---
title: "feat: Add datasets list, view, and delete commands"
type: feat
date: 2026-04-09
---

# feat: Add datasets list, view, and delete commands

## Overview

Add a `bt datasets` command group with `list`, `view`, and `delete` subcommands, following the established patterns from experiments/prompts/functions. The `view` command uniquely shows metadata plus a sample of dataset rows using BTQL.

## Problem Statement / Motivation

Datasets exist in Braintrust and are accessible via `bt sync pull dataset:<name>`, but there's no way to quickly list, inspect, or delete datasets from the CLI. Users must go to the web UI or use `bt sql` with manual queries.

## Proposed Solution

Create a new `src/datasets/` module following the same structure as `src/experiments/`:

```
src/datasets/
  mod.rs    -- Args, subcommands, project resolution, dispatch
  api.rs    -- Dataset struct, list/get/delete API calls
  list.rs   -- Table display of all datasets in a project
  view.rs   -- Metadata + sample rows display
  delete.rs -- Confirmation + delete
```

Register in `main.rs` as a new `Commands::Datasets` variant.

## Technical Approach

### New files

#### `src/datasets/api.rs`

```rust
use anyhow::Result;
use serde::{Deserialize, Serialize};
use urlencoding::encode;

use crate::http::ApiClient;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Dataset {
    pub id: String,
    pub name: String,
    pub project_id: String,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub created: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ListResponse {
    objects: Vec<Dataset>,
}

pub async fn list_datasets(client: &ApiClient, project: &str) -> Result<Vec<Dataset>> {
    let path = format!(
        "/v1/dataset?org_name={}&project_name={}",
        encode(client.org_name()),
        encode(project)
    );
    let list: ListResponse = client.get(&path).await?;
    Ok(list.objects)
}

pub async fn get_dataset_by_name(
    client: &ApiClient,
    project: &str,
    name: &str,
) -> Result<Option<Dataset>> {
    let path = format!(
        "/v1/dataset?org_name={}&project_name={}&dataset_name={}",
        encode(client.org_name()),
        encode(project),
        encode(name)
    );
    let list: ListResponse = client.get(&path).await?;
    Ok(list.objects.into_iter().next())
}

pub async fn delete_dataset(client: &ApiClient, dataset_id: &str) -> Result<()> {
    let path = format!("/v1/dataset/{}", encode(dataset_id));
    client.delete(&path).await
}
```

**Note:** The `Dataset` struct fields should be verified against the actual API response. The API may return additional fields like `num_records`, `user_id`, `metadata`, etc. Start with the minimal set and add fields as discovered.

#### `src/datasets/mod.rs`

Follow the `prompts/mod.rs` pattern exactly:

- `ResolvedContext` struct with `client`, `app_url`, `project`
- `DatasetsArgs` with optional subcommand (default to `list`)
- `DatasetsCommands` enum: `List`, `View(ViewArgs)`, `Delete(DeleteArgs)`
- `ViewArgs`: positional name, `--name` flag, `--web`, `--limit` (default 10)
- `DeleteArgs`: positional name, `--name` flag, `--force`
- `run()` function: login -> resolve project -> dispatch

```rust
#[derive(Debug, Clone, Args)]
pub struct ViewArgs {
    /// Dataset name (positional)
    #[arg(value_name = "NAME")]
    name_positional: Option<String>,

    /// Dataset name (flag)
    #[arg(long = "name", short = 'n')]
    name_flag: Option<String>,

    /// Open in browser instead of showing in terminal
    #[arg(long)]
    web: bool,

    /// Number of sample rows to display (default: 10)
    #[arg(long, default_value = "10")]
    limit: usize,
}
```

#### `src/datasets/list.rs`

Follow `prompts/list.rs` exactly:

- Spinner: "Loading datasets..."
- JSON mode: serialize full list
- Table columns: **Name**, **Description**, **Created**
- Truncate descriptions to 60 chars
- Count line: "3 datasets found in org / project"
- Pager output

#### `src/datasets/view.rs`

This is the only file with novel behavior (sample rows). Two sections:

**Section 1 — Metadata** (follow `experiments/view.rs` pattern):
- "Viewing **my-dataset**"
- Description, Created date
- Web URL hint

**Section 2 — Sample rows** (new):
- Use `client.btql()` to run: `SELECT * FROM dataset('<dataset_id>') LIMIT <limit>`
- If rows returned: render as a table using `styled_table()`, truncating cells to ~50 chars
- If no rows: display "(no rows)"
- Separate from metadata with a blank line and "Sample rows:" header

```rust
// Fetch sample rows via BTQL
let query = format!(
    "SELECT * FROM dataset('{}') LIMIT {}",
    dataset.id, limit
);
let response: BtqlResponse<serde_json::Map<String, serde_json::Value>> =
    client.btql(&query).await?;
```

For rendering rows in the table, use `serde_json::to_string()` for non-string values (objects, arrays, numbers) and truncate all cells. This matches the approach in `sql.rs:format_cell()`.

**`--web` flag:** Open `{app_url}/app/{org}/p/{project}/datasets/{dataset_id}` in browser. (URL pattern needs verification — check against the actual Braintrust web UI.)

**`--json` flag:** Output dataset metadata only (matching existing patterns). Users wanting row data as JSON should use `bt sql --json`.

#### `src/datasets/delete.rs`

Follow `prompts/delete.rs` exactly:

- Fuzzy select if no name provided (interactive)
- Bail if no name and non-interactive
- Bail if `--force` without name
- Confirmation: "Delete dataset 'X' from {project}?"
- Spinner: "Deleting dataset..."
- Success hint: "Run `bt datasets list` to see remaining datasets."

### Modified files

#### `src/main.rs`

1. Add `mod datasets;` declaration (~line 12)
2. Add to help template under "Projects & resources": `datasets     Manage datasets`
3. Add `Commands::Datasets(CLIArgs<datasets::DatasetsArgs>)` variant
4. Add `Commands::Datasets(cmd) => &cmd.base` in `base()` match
5. Add `Commands::Datasets(cmd) => datasets::run(cmd.base, cmd.args).await?` in dispatch

## Acceptance Criteria

- [x] `bt datasets list` shows a table of datasets in the current project
- [x] `bt datasets list --json` outputs JSON array
- [x] `bt datasets view <name>` shows metadata + sample rows
- [x] `bt datasets view <name> --limit 5` limits sample rows to 5
- [x] `bt datasets view <name> --web` opens dataset in browser
- [x] `bt datasets view <name> --json` outputs dataset metadata as JSON
- [x] `bt datasets view` with no name in interactive mode presents fuzzy select
- [x] `bt datasets view` with no name in non-interactive mode shows usage error
- [x] `bt datasets delete <name>` prompts for confirmation then deletes
- [x] `bt datasets delete <name> --force` skips confirmation
- [x] `bt datasets delete` with no name in interactive mode presents fuzzy select
- [x] `bt datasets` (bare) defaults to `list`
- [x] `bt --help` shows datasets in the help template
- [x] Empty dataset (no rows) shows "(no rows)" in view
- [x] Empty project (no datasets) shows "0 datasets found" in list

## Edge Cases

| Scenario | Expected behavior |
|---|---|
| Dataset with 0 rows | View shows metadata, "(no rows)" for sample section |
| Dataset name not found | Error: "dataset 'X' not found" |
| No datasets in project | List shows "0 datasets found in org / project" |
| No project specified (non-interactive) | Error: "--project required (or set BRAINTRUST_DEFAULT_PROJECT)" |
| `--force` without name | Error: "name required when using --force" |
| Very wide/nested JSON in row cells | Truncated to ~50 chars via `truncate()` |
| Dataset name with special characters | URL-encoded via `urlencoding::encode()` |

## Open Questions

1. **Dataset API response shape** — The `Dataset` struct fields need verification against the actual `/v1/dataset` API. May include `num_records`, `user_id`, `metadata`, `tags`, etc.
2. **Web URL path** — Needs verification: is it `/app/{org}/p/{project}/datasets/{id}` or another pattern?
3. **BTQL for dataset rows** — Verify `SELECT * FROM dataset('<id>') LIMIT N` works as expected. Alternative: the `/v1/dataset/{id}/fetch` endpoint.

## Implementation Order

1. `src/datasets/api.rs` — struct + API calls (test against real API to verify fields)
2. `src/datasets/mod.rs` — args, subcommands, dispatch
3. `src/datasets/list.rs` — table rendering
4. `src/datasets/delete.rs` — confirmation + delete
5. `src/datasets/view.rs` — metadata + sample rows (most complex)
6. `src/main.rs` — register command

## References

### Internal References

- Module pattern: `src/prompts/mod.rs`, `src/experiments/mod.rs`
- API pattern: `src/prompts/api.rs`, `src/experiments/api.rs`
- List rendering: `src/prompts/list.rs`
- View rendering: `src/experiments/view.rs`
- Delete pattern: `src/prompts/delete.rs`
- BTQL execution: `src/http.rs:190` (`client.btql()`)
- Table utilities: `src/ui/table.rs` (`styled_table`, `truncate`, `header`)
- Dataset in sync: `src/sync.rs:3214` (`list_project_named_objects`)
- Dataset in sync: `src/sync.rs:3201` (`create_dataset`)
- Help template: `src/main.rs:51-97`
- Command dispatch: `src/main.rs:206-234`
