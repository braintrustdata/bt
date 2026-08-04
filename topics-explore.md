# Topics Explore Spec

## Goal

Add read-only Topics exploration commands that bridge automation status and trace inspection:

```bash
bt topics facets
bt topics classifications
bt topics traces
bt topics explore
```

## Commands

```bash
bt topics facets --window 7d
```

Lists active facets and topic maps for the current project.

Columns: `facet`, `topic_map`, `topic_map_id`, `version`, `eligible`, `labeled`, `processing`, `errors`.

```bash
bt topics classifications --facet Task --repo test-org/test-repo --sort cost --window 7d
```

Lists current topic labels for one facet or topic map. The command name follows the underlying `classifications` BTQL field. When `--repo` is provided, label counts and metrics are scoped to that repository.

Text columns: `Topic label`, `Topic ID`, `Traces`, `Tokens`, `Cost`, `Avg tokens`, `Avg cost`, `Latest`.

```bash
bt topics traces --facet Task --topic-id <topic-id> --repo test-org/test-repo --sort cost --window 7d
```

Lists traces matching a selected topic label.

Columns: `created`, `root_span_id`, `user`, `repo`, `topic`, `tokens`, `cost`, `duration`, `input`.

```bash
bt topics explore
```

Guided flow:

1. Select automation if multiple exist.
2. Select facet/topic map.
3. Browse topic labels, sortable by count/tokens/cost.
4. Select a topic label to browse matching traces.
5. Select a trace to open the interactive trace view.
6. Quit the trace view to return to the topic-filtered trace list and keep exploring.
7. Select "Load more traces" to fetch the next trace page when available.
8. Press Esc on the trace list to move back up to the topic-label list.
9. Press Esc on the topic-label list to move back up to the facet/topic-map selector.

## Shared Flags

```bash
--automation-id <id>
--facet <name>
--topic-map <name-or-function-id>
--topic <label>
--topic-id <id>
--sort count|tokens|cost|avg-tokens|avg-cost|recent  # labels/explore
--sort recent|tokens|cost                            # traces
--limit <n>
--trace-page-size <n>  # explore only, defaults to 10
--cursor <cursor>      # traces only
--window <duration>
--since <timestamp>
--repo <owner/repo-or-host/owner/repo-or-origin-url>
--filter <btql>
--json
--print-queries
```

Sort defaults can also be configured with `BT_TOPICS_LABEL_SORT` for label ranking and `BT_TOPICS_TRACE_SORT` for trace lists. They are intentionally separate because label ranking supports aggregate average metrics while trace rows do not.

Repo filters can also be configured with `BT_TOPICS_REPO`. The preferred explicit format is `host/owner/repo`, for example `github.com/test-org/test-repo`. `owner/repo` is accepted as a GitHub shortcut, and pasted origins such as `git@github.com:test-org/test-repo.git` and `https://github.com/test-org/test-repo.git` normalize to the same selector.

All generated BTQL must include a time bound. Default to `--window 7d` unless `--since` is supplied. Combine the time filter, the resolved `--repo` root-span filter, `--filter`, and the automation's configured BTQL filter.

## Agent Usage

The non-interactive flow should use the report commands with `--json --no-input`, an explicit org/project context, and stable IDs returned by the previous step:

```bash
bt topics facets --json --no-input --org "$BRAINTRUST_ORG_NAME" --project "$BRAINTRUST_DEFAULT_PROJECT"
bt topics classifications --json --no-input --org "$BRAINTRUST_ORG_NAME" --project "$BRAINTRUST_DEFAULT_PROJECT" --automation-id <automation-id> --topic-map <topic-map-id> --repo github.com/test-org/test-repo --sort cost
bt topics traces --json --no-input --org "$BRAINTRUST_ORG_NAME" --project "$BRAINTRUST_DEFAULT_PROJECT" --automation-id <automation-id> --topic-map <topic-map-id> --topic-id <topic-id> --repo github.com/test-org/test-repo --sort tokens --limit 10
```

Agents should prefer `topic_map_id` from the facets response and `topic_id` from the classifications response. To continue paging traces, pass the returned `next_cursor` back to `bt topics traces` with the same selector, time filter, and sort.

## Query Shape

Resolve user-facing selectors (`--facet`, `--topic-map`) to a configured topic-map function. Use the configured classification path plus source function ID:

```btql
classifications."<topic_map_name>" IS NOT NULL
classifications."<topic_map_name>".source.id = '<topic_map_function_id>'
```

Topic filtering should prefer the stable topic id:

```btql
classifications."<topic_map_name>".id = '<topic_id>'
```

Use summary-shaped project logs for classification ranking and trace drilldown so trace-level token and cost metrics are available.
Trace drilldown should fetch one extra row per page, select `_pagination_key` plus a `sort_value`, and return an opaque topics trace cursor when another page exists. The next request uses that cursor as a keyset filter, so metric-sorted trace pages can continue even when BTQL does not return a backend cursor for the sort.
Repo filtering should match root spans by `metadata.git_origin_url` in `project_logs(...) spans`, then apply the resulting `root_span_id` set to the summary queries used for facet counts, topic labels, and trace lists. The CLI accepts `owner/repo`, `host/owner/repo`, and full origin URLs, then expands the canonical selector into exact SSH/HTTPS origin variants with and without `.git` so Codex and Claude traces match consistently.
Trace drilldown should select `span_attributes.created_by_user_id` from the root span. If any trace has a user ID, fetch the org's users once via `/v1/user?org_name=...` and cache that user map while resolving trace rows. Display the user's full name when available, then email, then raw user ID. User-name enrichment is best-effort; trace rows should still be returned if enrichment fails.

## Existing Command Relationship

This complements, rather than replaces:

- `bt topics status`: operational health/progress.
- `bt topics config`: automation/topic-map setup.
- `bt topics report` / `bt topics btmap`: artifact download.
- `bt view logs`: generic trace browser.
- `bt sql`: manual query escape hatch.
