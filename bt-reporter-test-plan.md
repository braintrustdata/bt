# Plan: Characterization Tests Before the Reporter Refactor

Phase 0 of `bt-reporter-compliance-plan.md`. Everything here lands **before** any reporter code changes, on today's `main`, and must be green there. The refactor's acceptance criterion — byte-identical output — is only meaningful if the bytes are pinned first.

## What exists today (and the gap)

- `tests/eval_fixtures.rs` runs real runtimes (tsx/bun/deno/python) against real eval files and asserts **exit codes** (`expect_success`) and watch behavior — not output content.
- `tests/eval_dev_server.rs` covers devserver endpoints; wire-level byte assertions need extending.
- ~80 unit tests in `src/eval.rs` cover SSE parsing and summary formatting.
- `assert_cmd` + `predicates` are already dev-dependencies. No snapshot crate (plain golden files are fine; `insta` optional).

The gap: **nothing asserts what `bt eval` prints**, on which stream, in what order.

## The fake runner

The mechanism that makes all of this cheap and deterministic:

- `bt` hands runners an SSE callback endpoint via `BT_EVAL_SSE_SOCK` (Unix socket) / `BT_EVAL_SSE_ADDR` (TCP, Windows) — `src/eval.rs:908`.
- `--runner` / `BT_EVAL_RUNNER` (`src/eval.rs:267`) substitutes the runtime binary.

A fake runner is a small Python script that ignores its argv (the materialized runner script and eval files), connects to the socket from the env var, replays a frame script, and exits with a scripted code. Frame scripts are JSONL data files, one directive per line:

```jsonl
{"event": "processing", "data": {"evaluators": 2}}
{"event": "start", "data": {"projectName": "test-project", "experimentName": "exp-1"}}
{"event": "progress", "data": {"id": "1", "object_type": "task", "format": "code", "output_type": "completion", "name": "exp-1", "event": "start", "data": "{\"type\":\"eval_progress\",\"kind\":\"start\",\"total\":10}"}}
{"event": "summary", "data": {"projectName": "test-project", "experimentName": "exp-1", "scores": {}}}
{"event": "done", "data": ""}
{"exit": 0}
```

One script interprets many scenario files — the corpus is data, not code. In Phase 3 of the compliance plan, the same corpus becomes the canonical-protocol conformance fixtures (new frame files, same harness).

**Runner-kind nuance (matters for two behaviors):** `bt` selects console policy and ESM-retry eligibility by runner kind — `should_retry_esm` requires the tsx runner (`src/eval.rs:1049`), and `ConsolePolicy::BufferStderr` applies only when retry is allowed. So the harness needs two spawn modes:

1. `--runner <path-to-fake>` → custom runner kind, `ConsolePolicy::Forward`.
2. A fake binary **named `tsx`** on a test-controlled `PATH` → tsx runner kind, `ConsolePolicy::BufferStderr`, retry-eligible.

Console-routing and retry scenarios must run under mode 2; everything else uses mode 1.

## Golden-output harness

A test helper in a new `tests/eval_golden.rs`:

```text
run_scenario(frames_file, flags, spawn_mode) -> { stdout, stderr, exit_code }
```

- Spawns the real `bt` binary via `assert_cmd`, captures stdout and stderr **separately**.
- Compares each against checked-in golden files: `tests/golden/eval/<scenario>--<mode>.stdout` / `.stderr`, plus expected exit code in the scenario manifest.
- `UPDATE_GOLDENS=1` regenerates.
- Captures are non-TTY, which is a feature: `indicatif` hides live bars on a non-terminal stderr, so only persistent lines appear — deterministic by construction. The TTY animation branch is deliberately untested (see Non-goals).
- Scenario payloads carry all displayed values (names, scores, durations, URLs), so no timestamp/path normalization should be needed; scenarios must avoid embedding temp paths in error messages.

## Scenario corpus

| # | Scenario (frame script) | Modes | What it pins |
| --- | --- | --- | --- |
| 1 | Happy path: processing → start → bar progress (start/set_total/increments/stop) → summary → done | default, `--jsonl`, `--verbose` | Persistent lines, summary table vs JSONL line, stdout/stderr separation |
| 2 | Summary with comparison fields; run with `--profile test-profile` | default, `--jsonl` | `compare_command` enrichment incl. profile flag, table + JSONL forms |
| 3 | Summary without comparison/metrics | default, `--jsonl` | Minimal-summary rendering |
| 4 | Errors mid-run: 3 distinct + 1 duplicate + enough to exceed `MAX_DEFERRED_EVAL_ERRORS` | default | Deferred footnote wording, dedup, cap |
| 5 | Same error frames | `--verbose` | Inline error + stack rendering, **ordering relative to console events** |
| 6 | Api-key error message | default, `--verbose` | Hint text, hint placement (deferred vs inline) |
| 7 | Console events: stdout + stderr lines interleaved | default, `--jsonl`, `--list`, `--verbose` | Full routing matrix: stdout forwarding, stderr suppression count |
| 8 | Same as 7, spawn mode 2 (fake tsx) | default, `--verbose` | `BufferStderr` policy path + `report_buffered_stderr` output |
| 9 | Crash: frames end with no `done`, exit 3 | default | Footnote still prints, exit code propagates |
| 10 | `error` events but exit 0; and the empty stream | default | Exit-code independence from rendering |
| 11 | Unknown SSE event names interleaved with known ones | default | Forward-compat: unknown events ignored silently (Phase 3 dual-emission depends on this) |
| 12 | Two evaluators interleaved (distinct names, overlapping progress) | default, `--jsonl` | Bar keying by name, per-evaluator summary ordering |
| 13 | `set_total` below current position; total under `EVAL_MIN_DETERMINATE_TOTAL` | default | Clamping and spinner-vs-bar selection (final output only; live rendering is non-TTY-hidden) |
| 14 | ESM interop error text on stderr + nonzero exit, spawn mode 2 | default, `--verbose` | Retry actually re-runs, first attempt's buffered stderr replays, "Suppressed N stderr line(s)" comes from the right source |

Frame scripts avoid timing dependence: order frames so the `set_total` smoothing force-paths decide, never `Instant` elapsed intervals.

## Devserver byte-contract tests

Extend `tests/eval_dev_server.rs`, using the same fake runner:

- **`/eval` stream mode**: snapshot the exact SSE frame sequence — event names, JSON key casing, frame framing — for a happy path and an error path. This is the browser UI's wire contract; compliance-plan Phase 4 must leave these snapshots unchanged (legacy format) while adding the negotiated canonical format alongside.
- **`/eval` non-stream mode**: snapshot the JSON response body; assert the error-event → HTTP-status mapping (including the fallback to 500 and "Eval runner exited with an error").
- **`/list`**: response shape snapshot.

## Unit tests to add in `src/eval.rs`

Small, on logic that survives the refactor:

- `record_deferred_error`: trims, dedups, respects `MAX_DEFERRED_EVAL_ERRORS`.
- Progress-total helpers: `should_apply_total_update` force paths and position-clamping (`ensure_total_not_below_position`, `maybe_apply_pending_total` with `force=true`); skip the elapsed-interval branches.
- `handle_sse_event`: malformed JSON payloads dropped without send; unknown event names produce no event (belt to scenario 11's braces).
- `EvalUi`: `finish()` idempotent; `Drop` calls `finish()` when not already finished.

## Non-goals

- **TTY animation rendering** — nondeterministic, small code branch, gated cleanly on `is_terminal()`; accept untested.
- **Time-interval total smoothing** — `Instant`-dependent; scenarios are constructed so it never decides an assertion.
- **Real-runtime coverage** — `tests/eval_fixtures.rs` already covers spawn/bundling/watch paths with real tsx/bun/deno/python; don't duplicate it. The golden harness runs in milliseconds with no runtime dependency; the two suites are complementary.

## Exit criteria and sizing

- All goldens green on `main` before the Phase 1 branch is cut.
- Compliance-plan Phase 1's cutover PR must show **zero golden diffs**. Phase 2's deliberate `--jsonl` console change lands as an explicit golden update in the same PR — the diff is the release note.
- Sizing: fake runner + harness ~1 day, scenario corpus 1–2 days, devserver snapshots ~0.5 day, unit tests ~0.5 day. One PR (or two: harness, then corpus).
