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
bt topics classifications --facet Task --sort cost --window 7d
```

Lists current topic labels for one facet or topic map. The command name follows the underlying `classifications` BTQL field.

Text columns: `Topic label`, `Topic ID`, `Traces`, `Tokens`, `Cost`, `Avg tokens`, `Avg cost`, `Latest`.

```bash
bt topics traces --facet Task --topic-id <topic-id> --sort cost --window 7d
```

Lists traces matching a selected topic label.

Columns: `created`, `root_span_id`, `user`, `topic`, `tokens`, `cost`, `duration`, `input`.

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
--filter <btql>
--json
--print-queries
```

All generated BTQL must include a time bound. Default to `--window 7d` unless `--since` is supplied. Combine `--filter` with the time filter and the automation's configured BTQL filter.

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
Trace drilldown should select `span_attributes.created_by_user_id` from the root span. If any trace has a user ID, fetch the org's users once via `/v1/user?org_name=...` and cache that user map while resolving trace rows. Display the user's full name when available, then email, then raw user ID.

## Existing Command Relationship

This complements, rather than replaces:

- `bt topics status`: operational health/progress.
- `bt topics config`: automation/topic-map setup.
- `bt topics report` / `bt topics btmap`: artifact download.
- `bt view logs`: generic trace browser.
- `bt sql`: manual query escape hatch.
