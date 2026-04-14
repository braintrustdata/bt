# `bt topics` CLI Spec

Status: draft
Last updated: 2026-04-13

## Background

The internal `btapi` CLI in `../braintrust/local/py/src/braintrust_local/btapi.py`
currently exposes a fairly wide Topics surface:

- `btapi topics generate`
- `btapi topics view`
- `btapi topics automation view`
- `btapi topics automation create`
- `btapi topics automation edit`
- `btapi topics automation delete`
- `btapi topics automation backfill rewind`
- `btapi topics automation backfill poke`
- `btapi topics automation backfill run`

That surface is useful for internal debugging, but it is too broad and too
implementation-shaped for the public `bt` CLI.

For `bt`, the first Topics release should be narrower:

- project-scoped
- read-only
- state-machine-first
- consistent with existing `bt` command patterns
- no nested `topics automation ...` command tree
- reserve `view` for topic-content inspection, not runtime status

## Goals

- Make it easy to inspect Topics status from the terminal.
- Make the topic automation state machine a first-class view.
- Reuse `bt`'s existing global project context instead of per-command
  `--project` flags.
- Support both human-readable output and `--json`.
- Avoid BTQL-heavy or write-path behavior in the initial version.

## Non-goals for v1

- Full parity with `btapi topics ...`
- Creating, editing, deleting, rewinding, poking, or running automations
- Topic generation or cluster exploration
- Per-facet and per-topic detailed progress bars
- Topic map diffing/version management

## Proposed Command Surface

```text
bt topics
bt topics status [--watch] [--full]
bt topics open
```

Explicitly not proposed:

```text
bt topics automation ...
bt topics state-machine
```

Rationale:

- it mirrors backend internals too directly
- it adds command depth without adding much user value
- the main public workflows are inspection-oriented and fit naturally at the
  top level of `bt topics`

### Command summary

- `bt topics`
  Default read-only overview for the active project. Equivalent to
  `bt topics status`.

- `bt topics status`
  Shows a compact summary of topic automations in the active project.
  This is where automation status lives; there is no separate
  `bt topics automation view`.

- `bt topics status --full`
  Shows the normal overview and appends the expanded diagnostic section,
  including the state machine.

- `bt topics open`
  Opens the Topics page in the browser for the active project.

## CLI Conventions

`bt topics` should follow normal `bt` conventions:

- Project comes from `-p/--project`, config, or interactive selection.
- Org comes from normal `bt` auth/context resolution.
- `--json` is supported globally.
- `--app-url` remains the source of truth for browser links.
- All v1 Topics commands use read-only auth/context resolution.
- `status` is the runtime/automation inspection verb.
- `view` is intentionally left unused in v1 so it can later mean "show me the
  topics/topic map contents" rather than "show me automation status."

## Subcommand Details

### `bt topics`

Behavior:

- Resolve the active project using existing `bt` project-context logic.
- Load all topic automations for the project.
- Render the same output as `bt topics status`.

Failure cases:

- No project context available:
  fail exactly like other project-scoped `bt` commands.
- Project has no topic automations:
  print `No topic automations found.`

### `bt topics status`

Flags:

- `--watch`
  Refresh every 2 seconds until interrupted.
- `--full`
  Append the expanded diagnostic section after the overview. In v1 this
  primarily means the state-machine section, but the name leaves room for
  future detail without introducing more flags.

Human output:

- Project header
- Count of topic automations
- One section per topic automation containing:
  - name
  - id
  - execution scope
  - top-level BTQL filter
  - configured schedule:
    - topic window
    - generation cadence
    - relabel overlap
    - idle time
  - current runtime state
  - next run time, if present
  - pending/error segment counts
  - due/error object counts
  - active topic map versions, if present
  - last error, if present

Notes:

- v1 should stay compact.
- v1 should not run BTQL queries to compute detailed facet/topic coverage.
- If a runtime state is unavailable, render that explicitly rather than trying
  to infer one.
- `--full` is preferred because it is short, discoverable, and easy for users
  and LLMs to request naturally.

In `--full` mode, the expanded section should expose the backend states
directly:

- `waiting_for_facets`
- `recomputing_topic_maps`
- `pending_topic_classification_backfill`
- `backfilling_topic_classifications`
- `idle`

### `bt topics open`

Behavior:

- Open:

```text
{app_url}/app/{org}/p/{project}/topics
```

- Reuse existing `bt` browser-opening behavior and success messaging.

## JSON Output

`--json` should return normalized inspection data for the active project.

Proposed shape:

```json
{
  "project": {
    "id": "proj_123",
    "name": "my-project",
    "org_name": "my-org",
    "topics_url": "https://www.braintrust.dev/app/my-org/p/my-project/topics"
  },
  "automations": [
    {
      "id": "auto_123",
      "name": "Topics",
      "description": "Automatically extract facets and classify logs using topic maps",
      "scope_type": "trace",
      "btql_filter": null,
      "window_seconds": 86400,
      "rerun_seconds": 86400,
      "relabel_overlap_seconds": 3600,
      "idle_seconds": 600,
      "configured_facets": 3,
      "configured_topic_maps": 2,
      "cursor": {
        "total_segments": 12,
        "pending_segments": 3,
        "error_segments": 1,
        "pending_min_compacted_xact_id": "9990001112223334",
        "pending_max_compacted_xact_id": "9990001112223399",
        "pending_min_executed_xact_id": "9990001112223300"
      },
      "object_cursor": {
        "total_objects": 5,
        "due_objects": 2,
        "error_objects": 1,
        "last_compacted_xact_id": "9990001112223334",
        "next_run_at": "2026-03-09T12:00:00Z",
        "last_run_at": "2026-03-09T11:00:00Z",
        "retry_after": "2026-03-09T11:15:00Z",
        "last_error": "Example object automation failure",
        "last_error_at": "2026-03-09T11:05:00Z",
        "topic_runtime": {
          "state": "backfilling_topic_classifications",
          "reason": "segment_replay_pending",
          "entered_at": "2026-03-09T11:00:00Z",
          "selected_window_seconds": 3600,
          "generation_window_start_xact_id": "9990001112220000",
          "generation_window_end_xact_id": "9990001112223334",
          "topic_classification_backfill_start_xact_id": "9990001112220000",
          "active_topic_map_versions": {
            "func_1": "v3"
          },
          "window_candidates": [
            {
              "window_seconds": 3600,
              "ready_topic_maps": 2,
              "total_topic_maps": 3
            }
          ]
        }
      }
    }
  ]
}
```

Implementation note:

- Numeric or string transaction IDs should be normalized to strings in JSON.

## Data Sources and API Calls

v1 should use a small backend surface:

1. Resolve project from existing `bt` project context.
2. `GET /v1/project_automation?project_id=<project_id>`
3. Filter rows where `config.event_type == "topic"`.
4. For each topic automation:
   - `POST /brainstore/automation/get-cursors`
   - `POST /brainstore/automation/get-object-cursors`

Deliberate v1 omission:

- No BTQL queries for detailed coverage/progress.
- No function lookups to expand facet/topic-map names unless already present in
  the returned automation payload.

## Human Output Design

### Overview mode

The overview should be optimized for fast scanning, not parity with the Python
debug CLI.

Example:

```text
Project: my-project
Topic automations: 1

Topics (auto_123)
  execution scope: trace
  filter: none
  schedule:
    - topic window: 1d
    - generation cadence: 1d
    - relabel overlap: 1h
    - idle time: 10m
  status:
    - runtime: idle
    - pending segments: 0
    - error segments: 0
    - due objects: 0
    - error objects: 0
    - next run: 2026-04-13T18:00:00Z (in 42m)
  configured:
    - facets: 3
    - topic maps: 2
```

### State-machine mode

The state-machine output should stay close to the backend semantics.

Example:

```text
Project: my-project
Topic automation state machines:

Topics (auto_123)
  current state: idle
  entered at: 2026-04-13T16:18:00Z (22m ago)
  to transition:
    - on the next rerun, readiness is checked again using the candidate windows
    - last observed readiness: 1h 0/2 ready | 1d 1/2 ready
    - next rerun: 2026-04-13T18:00:00Z (in 42m)
    - if one or more topic maps are ready at rerun time, it transitions to recomputing_topic_maps; otherwise it stays idle

  state machine:
    +-----------------------------------------+
    |   waiting_for_facets                    |
    +-----------------------------------------+
                       |
                       | first topic map is ready
                       v
    +-----------------------------------------+
    |   recomputing_topic_maps                |
    +-----------------------------------------+
                       |
                       | generated topic maps
                       v
    +-----------------------------------------+
    |   pending_topic_classification_backfill |
    +-----------------------------------------+
                       |
                       | next object check
                       v
                       pending segments?
                       +- yes -> backfilling_topic_classifications
                       '- no  -> idle
    +-----------------------------------------+
    |   backfilling_topic_classifications     |
    +-----------------------------------------+
                       |
                       | pending_segments == 0
                       v
    +-----------------------------------------+
    | * idle                                  |
    +-----------------------------------------+
                       |
                       | rerun due
                       v
                       ready topic maps?
                       +- yes -> recomputing_topic_maps
                       '- no  -> idle
```

Formatting requirements:

- Use ASCII, not Unicode box-drawing characters.
- Mark the active state with `*`.
- Keep terminal output stable enough for `--watch`.

## Watch Mode

Applicable to:

- `bt topics status --watch`
- `bt topics status --full --watch`

Behavior:

- Refresh every 2 seconds.
- Clear the terminal between frames when stdout is a TTY.
- Exit cleanly on Ctrl-C.
- Do not use a pager in watch mode.

## Error Handling

- No project selected:
  use the existing `bt` project-context error.
- Project exists but has no topic automations:
  return success with empty-state output.
- Topic automation exists but object-cursor runtime is absent:
  show `state machine: unavailable`.
- Partial API failure while fetching a specific automation status:
  fail the command for v1.

Rationale:

- Partial rendering is possible, but v1 should stay predictable and easy to
  reason about.

## Why this is simpler than `btapi`

Compared with the internal Python CLI, this spec intentionally removes:

- mutation commands
- manual backfill controls
- generation commands
- BTQL-heavy progress summaries
- the nested `topics automation ...` command depth entirely

The public `bt` CLI should start from the highest-value read-only workflows:

- "What Topics automations exist in this project?"
- "What state is the automation in right now?"
- "Why is it not progressing?"
- "Open the Topics page in the app."

And it should keep `view` available for a different question later:

- "What topics does this topic map currently contain?"

## Future Expansion

If v1 lands cleanly, later additions could include:

- `bt topics generate`
- `bt topics diff`
- `bt topics backfill`
- `bt topics configure`

Those should be added only if the read-only inspection model proves useful and
the command tree still feels coherent.

## Open Questions

1. Should v1 expose an `--automation-id` filter if a project has multiple topic
   automations?
2. Should `bt topics open` be the only browser-oriented action, or should
   `bt topics status --web` also exist for consistency with other namespaces?
3. Do we want string-normalized transaction IDs in JSON from day one, or only
   in human output?
