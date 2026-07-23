# Plan: Make `bt` Compliant with the Eval Reporter Design

Target: `eval-reporter-design.md`. This plan covers the `bt` side only — the reporter core, built-in reporters, the runner-script wire protocol, and the devserver. SDK-side work (emission hooks for full case fidelity) is a dependency of the final phase, not of this plan. Supersedes `eval-reporters-plan.md`.

**Phase 0 — characterization tests — is specified separately in `bt-reporter-test-plan.md` and must be green on `main` before Phase 1 begins.**

## Where `bt` is today

- `src/eval.rs` parses runner SSE (`handle_sse_event`, ~line 2737) into a legacy `EvalEvent` enum (~2602): `Processing`, `Start`, `Summary`, `Progress` (bar kinds `start`/`increment`/`set_total`/`stop`), `Console`, `Error`, `Dependencies`, `Done`.
- `EvalUi` (~2797) is a monolithic renderer: progress bars via `MultiProgress`, summary tables, console echo/suppression, deferred errors, JSONL mode, all keyed off constructor flags.
- `run_eval_attempt` (~725) wires `EvalUi` directly into `drive_eval_runner` (~964).
- The devserver endpoints drive `drive_eval_runner` with three ad-hoc closures (~1550, ~1679, ~1745): manifest-stdout collection, SSE re-encoding for the browser UI, summary/error collection.
- The runner scripts (`scripts/eval-runner.py`, `scripts/eval-runner-impl.ts`) ship inside the `bt` binary, so **both ends of the SSE protocol change atomically in one `bt` release**. The only cross-release boundary is runner ↔ installed SDK.
- Exit/retry logic is independent of rendering: `drive_eval_runner` collects `error_messages` and buffered stderr itself (~980–1026), and the ESM-retry path consumes those. Reporter work cannot break exit codes.

## Compliance gaps, mapped to the design

| Design requirement | `bt` today |
| --- | --- |
| Canonical event union with IDs (`runId`/`evalId`/`caseId`) | Legacy events joined by name; evaluator name (progress) and experiment name (start/summary) don't even match |
| Method-based reporters + manager | One monolithic `EvalUi` |
| `--reporter` selection, default sets, stdout claiming | Mode flags (`--jsonl`) baked into `EvalUi` |
| Shared `Terminal` facade | `MultiProgress` owned privately by `EvalUi` |
| `case:start`/`case:end`, `eval:progress` totals, `case:delta` | Bar-kind progress events only |
| `onRunEnd` veto → exit code | No reporter influence on exit |
| Devserver as bridge/collector reporters + wire negotiation | Three ad-hoc closures, one hardcoded vocabulary |
| structured `error` scope, `console` attribution | Flat message/stack, no scope |

## Phase 1 — Reporter core (behavior-preserving refactor)

**Goal:** the design's machinery exists inside `bt`; output is byte-identical.

1. **Canonical types** in a new `src/eval/reporter.rs` (or module split of `eval.rs`): `EvalReporterEvent` union and payload structs exactly as the design specifies — `EvalRun{run_id, evaluator_count, protocol_version}`, `EvalInfo{run_id, eval_id, name, experiment}`, `EvalCaseInfo`/`EvalCaseResult` (status `completed|errored|skipped`), `EvalEnd`, `EvalRunEnd`, `ReporterError{scope, ...}`, `ConsoleEvent`, `ProgressEvent{eval_id, total_cases}`, `CaseDelta`. Serde-ready from day one (camelCase wire names).
2. **`EvalReporter` trait**: default no-op methods for every lifecycle event including `on_case_start`/`on_case_end`/`on_case_delta` (uncalled until Phase 3), `on_error`, `on_run_end(&mut self, ...) -> Option<bool>`, plus `finish()` for terminal cleanup.
3. **`ReporterManager`**: serialized dispatch (trivially — the event loop is single-threaded), per-reporter failure isolation (a reporter error is logged once to stderr, never fatal), guaranteed exactly-once `run:end` (synthesized with `status: errored` if the stream dies; `Drop` safety), veto aggregation returned from `finish()`, and interest advertisement (`wants_case_delta()`).
4. **`Terminal` facade** wrapping `MultiProgress`: `println` (persistent line, suspends live region), `live_region()` (bars/spinners), `is_interactive()`. Handed to reporters via `EvalReporterContext{terminal, profile, output_file}` — no mode flags in the context.
5. **Legacy adapter**: a translation layer from today's runner SSE into canonical events, so reporters are written against the final protocol from the start:
   - `processing` → `run:start` (synthesized `run_id`)
   - `start` → `eval:start` (`eval_id` = experiment name for now)
   - `summary` → `eval:end` with `status: completed`, summary attached
   - progress `increment` → **synthesized `case:end`** `{eval_id: bar name, case_id: synthetic, status: completed}` — the SDKs tick increments exactly once per finished case, so this is honest; scores/duration absent
   - progress `set_total` → `eval:progress{total_cases}`
   - progress `start`/`stop` → dropped (reporters create bars on first sight of an unknown `eval_id`)
   - `error` → `error` with run-level scope; `console` → `console`; `done` → `run:end`
   - **Resilience rule (permanent, not transitional):** reporters tolerate events for `eval_id`s they haven't seen `eval:start` for. This absorbs the legacy evaluator-name vs experiment-name mismatch without pretending to fix it.
6. **`FancyReporter`** replaces `EvalUi`: bars positioned by counting `case:end`, totals from `eval:progress`, console echo/suppression, deferred-error footnote, api-key hint. Constructed with `summaries: bool` (default true). JSONL rendering moves out (Phase 2 wires it back via reporter selection); until then the `--jsonl` flag constructs the interim equivalent set internally.
7. `run_eval_attempt` goes through the manager. Devserver closures untouched.

**Acceptance:** default, `--jsonl`, `--list`, `--verbose` output byte-identical (golden tests against a scripted fake runner); exit codes unchanged; existing `handle_sse_event`/summary-formatting tests pass.

## Phase 2 — Selection, built-ins, exit-code veto

**Goal:** the design's user-facing surface.

1. **Flags** per repo config policy: `--reporter` repeatable + `BRAINTRUST_EVAL_REPORTER` (comma-separated), `--output-file` + `BRAINTRUST_EVAL_OUTPUT_FILE`. Explicit selection replaces the default set.
2. **Built-ins:** `fancy`, `verbose` (inline errors + stacks, forwarded stderr, per-case lines once case events carry names), `jsonl`, `silent`, `events` (NDJSON canonical stream — nearly free once serde exists). `dot`, `junit`, `github-actions` are declared but **refuse to run** with an actionable error until Phase 3 delivers real case fidelity ("requires per-case results; upgrade braintrust to ≥ X / not yet supported by this runner").
3. **Default sets** exactly per the design table: `[fancy]`; `--verbose` → `[verbose]`; `--jsonl` → `[jsonl, fancy(summaries: false)]` — stderr keeps bars and footnotes, no summary table, stdout is pure JSONL. Reporter construction options exist internally only (no CLI syntax).
4. **stdout claiming:** manager errors at startup if two stdout-claiming reporters are installed without file routing.
5. **Exit-code veto:** `run_eval_attempt` maps any `Some(false)` from `on_run_end` to command failure, alongside (never replacing) the existing exit-status/error-message logic.
6. Behavior change shipped here, called out in release notes: under `--jsonl`, user `print()` output echoes to stderr instead of interleaving into stdout.

**Acceptance:** `--reporter=silent`/`events` work; `--jsonl` stdout is parseable JSONL under a noisy eval; `--verbose` matches today; vetoes fail the command.

## Phase 3 — Canonical wire protocol in the runner scripts

**Goal:** the runners emit canonical events natively; the legacy adapter becomes a fallback.

1. Runners emit the canonical union over SSE: `run:start` (with `protocolVersion`, `runId`), `eval:start` (emitter-assigned `evalId`, resolving the name-mismatch problem at the source), `eval:progress`, scoped `error`, `console`, `eval:end`, `run:end`. `bt` ships both ends, so this is one atomic change — `bt`'s decoder accepts canonical events first and falls back to the legacy adapter per event name.
2. **Case events at the best fidelity the installed SDK allows**, feature-detected by the runners (`hasattr` / `typeof`):
   - New SDK hooks available → real `case:start`/`case:end` with `caseId` = root span ID, status, duration, scores; `case:delta` forwarded from the SDK `stream` plumbing **only when `bt` signals a subscribed reporter** (env var or handshake — interest advertisement crossing the process boundary).
   - Old SDK → increment-derived synthetic `case:end` (Phase 1 behavior), no deltas. `dot`/`junit`/`github-actions` detect the degraded stream and refuse with the upgrade message.
3. Dependencies/watch-mode events stay outside the reporter protocol (run-mode machinery, per the design's "what is not a reporter").

**Acceptance:** with a current SDK, `--reporter=dot` renders real per-case status and `junit` writes a valid file (and vetoes on write failure); with an old SDK pinned, both fail actionably and `fancy` still renders bars from synthetic case ends.

## Phase 4 — Devserver as reporters + wire negotiation

**Goal:** the devserver stops being a parallel event-handling path.

1. Replace the three closures with reporters on the same manager: a manifest-stdout collector, a summary/error collector, and an HTTP bridge.
2. The bridge implements the design's wire-compatibility contract: legacy vocabulary (`start`/`progress`/`summary`/`error`/`done`) by default, canonical events when the client sends `x-bt-stream-fmt`. The browser UI keeps working unchanged; it upgrades whenever the app adopts the canonical format.
3. Devserver installs its fixed reporter set; `--reporter` is ignored there (documented, not an error).

**Acceptance:** devserver HTTP responses byte-compatible for legacy clients; canonical stream available behind the header; no rendering side effects on the devserver's own stdout/stderr.

## Sequencing and sizing

| Phase | PRs | Depends on |
| --- | --- | --- |
| 1 | 2 (types+manager+adapter; FancyReporter+cutover) | — |
| 2 | 2 (flags+defaults+claiming; veto+events+silent) | 1 |
| 3 | 2–3 (canonical emission; case events + feature detection; delta subscription) | 1; SDK hooks for full fidelity |
| 4 | 1–2 | 1 (not 2/3) |

Phases 2 and 4 are independent of each other and of 3; only Phase 3's full-fidelity half waits on SDK releases (JS/Python already have most of the plumbing — `stream` callbacks, per-case root spans, per-case increments). Everything before that point is `bt`-internal with zero coordination.

## Risks and pinned decisions

- **Output-ordering regressions in Phase 1** are the main risk (verbose inline errors interleaved with console lines; footnote after bars clear). Mitigation: golden-output tests with a scripted fake runner before the cutover PR, and `on_error` firing immediately (never deferred to `run:end`) to preserve verbose ordering.
- **The `Terminal` facade must be the only path to the screen** — a reporter holding its own `eprintln!` reintroduces bar tearing. Enforce by construction: reporters get `&Terminal`, not stdio.
- **ESM-retry stderr buffering stays where it is** (`drive_eval_runner` + `report_buffered_stderr`): it exists so a failed first attempt's stderr survives the retry, which is run-mode machinery, not rendering. `--verbose` keeps its residual global meaning there.
- **`--list` remains a run mode**, untouched by reporter selection.
- **Synthetic case ends are honest but unlabeled** — they carry no scores or duration, and reporters must not invent them. `verbose` per-case lines and `dot` glyph fidelity are gated on real case events, not faked from increments.
