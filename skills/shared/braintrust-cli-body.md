Use the Braintrust `bt` CLI for projects, traces, prompts, and sync workflows.

Prefer using the local `bt` CLI for Braintrust workflows.

Common commands:

- `bt login status`
- `bt projects list`
- `bt prompts list --project <name>`
- `bt view logs --project <name>`
- `bt view trace --object-ref <ref> --trace-id <id>`
- `bt sync status --spec <name>`

Guidelines:

- Prefer CLI output (`--json`) for structured follow-up processing.
- Respect existing login/profile settings from `bt login`.
- Avoid direct API calls when `bt` already supports the operation.
