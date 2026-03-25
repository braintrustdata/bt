# Eval SSE Transport Research (btcli + braintrust-sdk-rust)

## Scope

This document explains how eval execution and SSE event transport currently work across:

- `btcli` (parent process and UI)
- JavaScript runner script (`scripts/eval-runner.ts`)
- Python runner script (`scripts/eval-runner.py`)
- Rust SDK runner (`../braintrust-sdk-rust/src/eval/bt_runner.rs`)

It focuses on the Windows-native issues we are fixing:

1. incorrect/unsafe Unix-socket handling on Windows
2. nondeterministic transport selection when both `BT_EVAL_SSE_SOCK` and `BT_EVAL_SSE_ADDR` are present

## High-Level Architecture

### Parent/child split

`btcli` is the parent orchestrator. It:

1. starts a local SSE listener
2. chooses transport (`unix socket` or `tcp`)
3. passes the selected endpoint to the child runner via env vars
4. reads SSE events and maps them into `EvalEvent` for UI/output

Child runners (JS/Python/Rust SDK) do not host the listener. They connect to what `btcli` exposes and emit SSE events.

## btcli Flow

### Listener binding and transport selection

Entry path:

- `spawn_eval_runner(...)` in `src/eval.rs`
- calls `bind_sse_listener()`

Transport chosen by platform in `src/eval.rs`:

- Unix: `bind_sse_listener()` -> `UnixListener::bind(...)`, exports `BT_EVAL_SSE_SOCK`
- Non-Unix (Windows): `bind_sse_listener()` -> `bind_sse_tcp_listener()`, exports `BT_EVAL_SSE_ADDR` with `127.0.0.1:<ephemeral_port>`

Relevant code:

- `src/eval.rs:2471` (unix binding)
- `src/eval.rs:2512` (non-unix binding)
- `src/eval.rs:2531` (env key for non-unix is `BT_EVAL_SSE_ADDR`)

### Child env wiring

After constructing the child command, `btcli` sets eval env vars and now clears the opposite SSE key:

- sets chosen key/value
- removes conflicting key (`BT_EVAL_SSE_SOCK` or `BT_EVAL_SSE_ADDR`)

Relevant code:

- `src/eval.rs:801`
- `src/eval.rs:807`

This makes transport selection deterministic for all runners.

### SSE parsing and event mapping

`btcli` reads SSE framing (`event:` / `data:` blocks) and maps events:

- `start`
- `summary`
- `processing`
- `progress`
- `console`
- `error`
- `dependencies`
- `done`

Relevant code:

- `read_sse_stream(...)` in `src/eval.rs:2715`
- `handle_sse_event(...)` in `src/eval.rs:2748`

## Child Runner Implementations

### JavaScript runner (`scripts/eval-runner.ts`)

Config/env reader:

- `readRunnerConfig()` consumes `BT_EVAL_*` flags (`BT_EVAL_JSONL`, `BT_EVAL_LIST`, etc.)

SSE transport connection order in `createSseWriter()`:

1. if `BT_EVAL_SSE_SOCK` is set, connect with `node:net` path mode
2. else if `BT_EVAL_SSE_ADDR` is set, connect host/port TCP

Relevant code:

- `scripts/eval-runner.ts:256`
- `scripts/eval-runner.ts:678`
- `scripts/eval-runner.ts:687`
- `scripts/eval-runner.ts:721`

Event emission:

- sends `processing`, `summary`, `error`, `dependencies`, `done`, and progress/console events
- finalization sends `dependencies` + `done` then closes socket

Relevant code:

- `scripts/eval-runner.ts:1908`
- `scripts/eval-runner.ts:1958`

### Python runner (`scripts/eval-runner.py`)

Config/env reader:

- `read_runner_config()` consumes `BT_EVAL_*` flags

SSE transport connection order in `create_sse_writer()`:

1. if `BT_EVAL_SSE_SOCK` is set, connect using `socket.AF_UNIX`
2. else if `BT_EVAL_SSE_ADDR` is set, parse and connect TCP

Relevant code:

- `scripts/eval-runner.py:145`
- `scripts/eval-runner.py:90`
- `scripts/eval-runner.py:97`

Event emission:

- sends `dependencies` and `done` before exit, closes socket in `finally`

Relevant code:

- `scripts/eval-runner.py:958`
- `scripts/eval-runner.py:985`

## Rust SDK Eval Runner (`braintrust-sdk-rust`)

### Core entry points

- `BtEvalRunner::from_env()` reads config from env and initializes SSE writer
- `eval(...)` runs evaluator, emits `start` and `summary` or `error`
- `finish(...)` handles list mode output and returns pass/fail aggregate

Relevant code:

- `../braintrust-sdk-rust/src/eval/bt_runner.rs:223`
- `../braintrust-sdk-rust/src/eval/bt_runner.rs:287`
- `../braintrust-sdk-rust/src/eval/bt_runner.rs:364`

### Current transport logic and Windows break

In `create_sse_writer()`:

1. checks `BT_EVAL_SSE_SOCK` first
2. tries `std::os::unix::net::UnixStream::connect(...)`
3. falls back to `BT_EVAL_SSE_ADDR` TCP

Relevant code:

- `../braintrust-sdk-rust/src/eval/bt_runner.rs:127`
- `../braintrust-sdk-rust/src/eval/bt_runner.rs:131`
- `../braintrust-sdk-rust/src/eval/bt_runner.rs:145`

Problem: `std::os::unix` is not available on Windows. Because that symbol is referenced unguarded, Windows compile fails before runtime fallback is possible.

Observed CI error pattern:

- compiler error: `could not find unix in os`
- location: `src/eval/bt_runner.rs:131`
- downstream `cargo-dist` exits with `127` after failing to produce `bt.exe`

## Why We Still Need Platform-Aware Logic in btcli

Even if SDK/runners become fully cross-platform, `btcli` must still:

- bind listener socket/port
- choose endpoint type
- pass endpoint env to child
- parse SSE for UI/progress integration

So the right boundary is:

- `btcli`: transport host/binding + env provisioning + event consumption
- SDK/runners: transport client/connection + event emission

## Root Causes for the Current Regression Class

### 1) Compile-time portability gap in Rust SDK

`bt_runner.rs` references Unix APIs without `cfg(unix)` guards.

### 2) Runtime transport ambiguity from inherited env

All child runners currently check `BT_EVAL_SSE_SOCK` before `BT_EVAL_SSE_ADDR`.
If both are present (for example inherited parent env), the runner can choose the wrong transport.

This is why clearing the opposite key in `btcli` is necessary even after SDK fixes.

## Fix Strategy for Native Windows Support (No WSL)

### In `braintrust-sdk-rust`

Refactor `create_sse_writer()` to be platform-safe and deterministic:

1. Gate Unix socket connect behind `#[cfg(unix)]`.
2. On non-Unix, never reference `std::os::unix`.
3. Keep TCP path (`BT_EVAL_SSE_ADDR`) available on all platforms.
4. Optional: on Unix, still prefer socket then TCP; on non-Unix, ignore `BT_EVAL_SSE_SOCK` entirely or log that it is unsupported.

### In `btcli`

Keep the opposite-key clearing behavior in `spawn_eval_runner()`:

- set selected env key
- remove the conflicting one

This guarantees deterministic behavior across SDK, JS runner, and Python runner.

### In JS/Python runners

Current behavior is acceptable with CLI env cleanup, but optional hardening:

- On non-Unix, ignore or warn on `BT_EVAL_SSE_SOCK` before attempting Unix connect.

## Validation Matrix

### Windows

1. Build `bt` and confirm no compile error from `std::os::unix`.
2. Run at least one JS eval and one Python eval via `bt eval`.
3. Verify progress/summary stream appears (SSE connected through TCP).
4. Verify behavior when parent env includes stale `BT_EVAL_SSE_SOCK`:
   - child should still use `BT_EVAL_SSE_ADDR`.

### Unix (macOS/Linux)

1. Run eval and verify Unix socket path still works.
2. Confirm no regression in progress/summary/error event handling.

## Key Takeaway

Native Windows support is achieved by using TCP SSE end-to-end on Windows, not by disabling eval features.
The minimal robust model is:

- `btcli` chooses and exports exactly one transport key
- SDK/runners connect using platform-safe transport code
- SSE event protocol remains unchanged
