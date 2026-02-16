Use the Braintrust `bt` CLI for projects, traces, prompts, and sync workflows.

## When To Use

- The user asks to inspect traces, prompts, projects, or sync state.
- You need reliable auth/profile behavior without manually handling API tokens.
- You are automating CLI workflows where `--json` output can be piped to other tools.

## How To Use

1. Confirm auth and context:
   - `bt login status`
   - `bt projects list`
2. Run the smallest command that answers the question:
   - `bt prompts list --project <name>`
   - `bt view logs --project <name>`
   - `bt view trace --object-ref <ref> --trace-id <id>`
3. Prefer machine-readable output for follow-up:
   - add `--json` when results need further parsing.

## Guardrails

- Prefer `bt` commands over direct API calls when both can accomplish the task.
- Respect existing login/profile settings from `bt login`.
