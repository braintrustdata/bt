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
