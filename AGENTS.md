# Agent Guidelines

## Configuration Policy

- Do not introduce new ad-hoc configuration reads via `std::env::var(...)` / `process.env` / `os.getenv(...)` in command logic.
- All user-facing/runtime configuration must be defined in `clap` arguments:
  - a CLI flag, and
  - a corresponding environment variable (`#[arg(env = ...)]`).
- Entry points/runners should receive configuration from the CLI layer (via args/env that the CLI owns), not from independent, undocumented env lookups.
- Extreme edge-case exceptions are allowed only when a flag is not feasible (for example, process-internal plumbing), and must be:
  - documented inline with a short rationale, and
  - minimized in scope.
- Do not infer/transform app URLs from API URLs (for example, replacing `api.` with `www.`). Treat `--app-url` / `BRAINTRUST_APP_URL` as the source of truth for app URLs.

## BTQL Safety

- Every BTQL query must include either:
  - a timestamp filter (for example, `created >= NOW() - INTERVAL ...` or `created >= "<ts>"`), or
  - a `root_span_id` filter.
- Do not run BTQL queries that lack both constraints.

## Tooling

- This repo is managed with `mise` and `pre-commit`; prefer using the repo-defined toolchain and hooks when running local validation.

## CLI Implementation Conventions

- Follow existing resource-command patterns before adding new structure; `projects/` is a good reference for module layout and command dispatch.
- Guard interactive prompts with TTY checks, and make non-interactive failures actionable.
- Prefer existing output helpers for status messages, tables, pagers, JSON output, and spinners instead of ad-hoc `println!` / `eprintln!`.
- Keep progress indicators on stderr and machine-readable output on stdout.
- Prefer actionable errors with exact command hints where possible, and add `anyhow::Context` when it improves user-facing debugging.
- For API helpers, follow existing list/get conventions: `ListResponse { objects }`, `get_by_* -> Option<T>`, and URL-encoded query params.
