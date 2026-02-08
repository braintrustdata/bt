# `bt eval` Parity TODO (No `--push`)

## Scope

This tracks parity for `eval` only against:

- `braintrust-sdk/js` CLI (`npx braintrust eval`)
- `braintrust-sdk/py` CLI (`braintrust eval`)

`--push` parity is intentionally out of scope for now.

## Decision: Custom `--tsconfig`

Recommendation: **defer** adding `--tsconfig` in `bt` for now.

Reasoning:

- With the current runner-first architecture (`--runner`, `BT_EVAL_JS_RUNNER`) we execute user code with their runtime (`tsx`, `bun`, `ts-node`, etc.), and that runtime already owns TS config discovery.
- Adding `--tsconfig` in `bt` only makes sense if `bt` itself compiles/bundles TS (like legacy JS CLI bundling flow).
- If we later need explicit control, add a runner-agnostic pass-through (for example: `--runner-arg ...`) instead of hardcoding TS-specific behavior.

## Feature Parity Checklist

Legend:

- `done`: implemented in `bt`
- `partial`: implemented but behavior differs
- `todo`: missing

| Feature / Flag              |           JS CLI |           PY CLI |                                       `bt` | Notes                                                                                                    |
| --------------------------- | ---------------: | ---------------: | -----------------------------------------: | -------------------------------------------------------------------------------------------------------- |
| Run eval files              |              yes |              yes |                                     `done` | Single-language per invocation currently enforced.                                                       |
| Local/no-upload mode        | `--no-send-logs` | `--no-send-logs` | `done` (`--local`, alias `--no-send-logs`) |                                                                                                          |
| Global auth/env passthrough |              yes |              yes |                                     `done` | Via base args/env (`BRAINTRUST_API_KEY`, `BRAINTRUST_API_URL`, project).                                 |
| Progress rendering          |              yes |              yes |                                  `partial` | `bt` consumes local SSE and renders Rust TUI/progress, but not full SDK parity yet.                      |
| `--list` (discover only)    |              yes |              yes |                                     `todo` |                                                                                                          |
| `--filter`                  |              yes |              yes |                                     `todo` |                                                                                                          |
| `--jsonl` summaries         |              yes |              yes |                                     `todo` |                                                                                                          |
| `--terminate-on-failure`    |              yes |              yes |                                     `todo` |                                                                                                          |
| `--watch`                   |              yes |              yes |                                  `partial` | Poll-based watcher with Node/Bun dependency hooks, Deno graph collection, and static JS import fallback. |
| `--verbose`                 |              yes |      parent flag |                                     `todo` |                                                                                                          |
| `--env-file`                |              yes |              yes |                                     `todo` |                                                                                                          |
| `--dev` remote eval server  |              yes |              yes |                                     `todo` | Important for `test_remote_evals.py` parity.                                                             |
| `--dev-host`                |              yes |              yes |                                     `todo` |                                                                                                          |
| `--dev-port`                |              yes |              yes |                                     `todo` |                                                                                                          |
| `--dev-org-name`            |              yes |              yes |                                     `todo` |                                                                                                          |
| `--num-workers`             |              n/a |              yes |                                     `todo` | Python-specific concurrency control.                                                                     |
| Directory input expansion   |              yes |              yes |                                     `todo` | Today `bt` expects explicit files/extensions.                                                            |
| Mixed runtime selection     |              n/a |              n/a |                                  `partial` | Current `--runner` plus env vars; per-language runner matrix deferred.                                   |

## Braintrust Test Callsite Inventory

Source repo scanned: `braintrust/tests/bt_services`

### Direct `eval` callsites

- `test_bundled_code.py`
  - `npx braintrust eval <file> --bundle --jsonl`
  - `npx braintrust eval <file> --bundle --jsonl --push`
- `test_function_hooks.py`
  - `npx braintrust eval <file> --bundle --jsonl --push`
- `test_remote_evals.py`
  - `npx braintrust eval <file> --dev --dev-port <port>`
  - `braintrust eval <file> --dev --dev-port <port>`
- `test_expect.py`
  - TS path: `npx braintrust eval --verbose --terminate-on-failure ... --env-file ... <file>`
  - PY path: `braintrust eval --verbose --num-workers 4 --terminate-on-failure ... --env-file ... <file>`

## Test Coverage in This Repo

Current fixtures under `tests/evals/` now include:

- JS module system coverage:
  - `eval-esm`, `eval-cjs`, `eval-ts-esm`, `eval-ts-cjs`
- JS execution mode coverage:
  - `entrypoint-basic`, `direct-basic`
- JS runtime compatibility coverage:
  - `eval-ts-esm` runs with both `tsx` and `bun` runners from one fixture
  - `eval-bun` covers Bun-only APIs (`bun`, `bun:sqlite`, `Bun.file`)
- Python import behavior coverage:
  - `basic`, `local_import`, `relative`, `absolute`

These cover the major interoperability scenarios from `braintrust-sdk/js/cli-tests` plus Python import quirks that show up in `braintrust` expect tests.

## Next Implementation Order

1. Add CLI flags needed by `test_expect.py`: `--verbose`, `--terminate-on-failure`, `--env-file`, `--num-workers`.
2. Add evaluation control flags: `--list`, `--filter`, `--jsonl`.
3. Add remote mode: `--dev`, `--dev-host`, `--dev-port`, `--dev-org-name`.
4. Add directory discovery and glob matching parity.
5. Tighten parity output for progress/summary formatting.

## Test Work Remaining

- Add fixture cases that directly assert new flags as they are implemented (especially `--list`, `--filter`, `--jsonl`, `--dev`).
- Add negative fixtures for incompatible runtime/file combos (expected failures).
- Keep runtime matrix in one fixture via `runners` to avoid file duplication across runtimes.
