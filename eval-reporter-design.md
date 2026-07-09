# Eval Reporter System — Target Design

This document describes the ideal end state of the eval reporter system, designed from first principles. It is a target design, not a migration plan.

## Design Principles

1. **One protocol, every host.** A single canonical event stream describes an eval run. The same events are consumed by reporters running inside the SDKs (JS, Python, Go, Java, Ruby), inside the `bt` CLI, and by remote-eval hosts (SDK dev servers serving the playground, `bt devserver`). Every transport — terminal rendering, playground SSE, `bt`'s runner SSE, devserver HTTP — is a serialization of the same canonical events; no host defines its own event vocabulary.
2. **Reporters render; they never decide.** Reporters are pure consumers of the event stream. What runs, in what order, with what concurrency, and what exit code results are decided outside the reporter system (with one narrow exception: `onRunEnd` may veto success — see Exit Codes).
3. **Three concerns, never entangled:**
   - **Run mode** — what the runner does (execute, list, watch, sample). Not a reporter concern.
   - **Output format** — how results are rendered. This is exactly what a reporter is.
   - **Verbosity** — how much a given rendering shows. Expressed by *choosing a different reporter* (`fancy` vs `verbose`), not by flags that mutate reporter behavior.
4. **stdout is machine-owned; stderr is human-owned.** Machine-readable output (JSONL, event streams) goes to stdout. Everything decorative — progress bars, summary tables, echoed user console output, error footnotes — goes to stderr. A pipeline consuming stdout must never see a stray `print()` from user eval code.
5. **Events carry identity, not just names.** Every scoped event carries emitter-assigned IDs (`run_id`, `eval_id`, `case_id`). Names are display metadata. Reporters correlate events by ID; they never join on names.
6. **Reporter failures are non-fatal.** An eval run represents real time and LLM spend. A rendering bug is logged, never fatal — except an artifact reporter's explicit veto at `onRunEnd`.
7. **Big data stays in-process.** `input` / `output` / `expected` never appear on lifecycle events. Reporters that need full case data run inside the SDK process, where they have direct access. The single sanctioned exception is the `case:delta` side channel — live task-output streaming for hosts that render output as it generates (the playground) — and it is emitted only when a subscribed reporter asks for it.

## The Event Protocol

### Lifecycle

```text
run:start
  eval:start          (one per evaluator; evaluators may interleave)
    case:start        (one per case; cases may interleave)
    case:end
  eval:end            (terminal per-eval event)
run:end               (terminal event, exactly once)
```

Side channels, valid at any point between `run:start` and `run:end`:

```text
error          immediate error surfacing; scope carried in payload
console        echoed user stdout/stderr; attribution carried in payload
eval:progress  announces or revises an eval's expected case total (see notes)
case:delta     streaming task output for a live case (opt-in; see notes)
```

### Event schema

```ts
type EvalReporterEvent =
  | { type: "run:start";  run: EvalRun }
  | { type: "eval:start"; eval: EvalInfo }
  | { type: "case:start"; case: EvalCaseInfo }
  | { type: "case:end";   case: EvalCaseResult }
  | { type: "eval:end";   eval: EvalEnd }
  | { type: "run:end";    run: EvalRunEnd }
  | { type: "error";         error: ReporterError }
  | { type: "console";       log: ConsoleEvent }
  | { type: "eval:progress"; progress: ProgressEvent }
  | { type: "case:delta";    delta: CaseDelta };

interface EvalRun {
  runId: string;
  evaluatorCount: number;
}

interface EvalInfo {
  runId: string;
  evalId: string;              // emitter-assigned, unique within the run
  name: string;                // display name (evaluator name)
  experiment?: ExperimentInfo; // project/experiment names, IDs, URLs
}

interface EvalCaseInfo {
  evalId: string;
  caseId: string;              // emitter-assigned, unique within the eval
  index: number;
  name?: string;
}

interface EvalCaseResult extends EvalCaseInfo {
  status: "completed" | "errored" | "skipped";
  durationMs: number;
  scores: Record<string, number>;
  error?: { message: string; stack?: string };
}

interface EvalEnd {
  evalId: string;
  status: "completed" | "errored";
  durationMs: number;
  caseCounts: { completed: number; errored: number; skipped: number };
  summary?: ExperimentSummary; // scores, metrics, comparison, URLs
  errors: ReporterError[];     // errors scoped to this eval
}

interface EvalRunEnd {
  runId: string;
  status: "completed" | "errored";
  durationMs: number;
  errors: ReporterError[];     // errors not attributable to any eval
}

interface ReporterError {
  scope: { runId: string; evalId?: string; caseId?: string };
  message: string;
  stack?: string;
  status?: number;             // HTTP status when the error came from the API
}

interface ConsoleEvent {
  stream: "stdout" | "stderr";
  message: string;
  evalId?: string;             // when attributable
}

interface ProgressEvent {
  evalId: string;
  totalCases: number; // best current estimate; may be revised upward mid-run
}

interface CaseDelta {
  evalId: string;
  caseId: string;
  kind: "text" | "json" | "reasoning";
  data: string;
}
```

Notes on deliberate choices:

- **A run is one invocation.** In `bt eval` and devservers — hosts with discovery — a run contains many evals. In the in-process SDKs (Go, Java, Ruby today; JS/Python programmatic use), one API call = one eval = one experiment, and a single-eval run is the normal case: the manager synthesizes `run:start`/`run:end` around it. No SDK needs a multi-eval "session" concept for the protocol to hold.
- **`eval:end` is the only terminal per-eval event.** There are no separate `summary` or per-eval `error` lifecycle events; the summary and scoped errors ride on `eval:end`. Side-channel `error` events exist purely for *immediate* rendering.
- **`eval:end.summary` is optional by design, not by accident.** Some SDKs compute summaries locally; others (Go, Java) fetch them from the Braintrust API after traces flush. A host may emit `eval:end` without a summary, or block on the fetch first — both are conforming. Reporters must render sensibly when `summary` is absent.
- **`console` is a host capability, not an SDK obligation.** Console events exist where a host owns a process boundary and can capture streams (`bt` wrapping a runner subprocess, devservers). In-process SDK libraries do not hijack an application's stdout/stderr, so reporters running there simply never receive `console` events — which the all-optional interface tolerates.
- **`eval:progress` carries only what lifecycle events cannot.** With `case:start`/`case:end` as first-class events, completion is derivable: a bar's position is the count of `case:end` events for that eval, and start/stop bracket at `eval:start`/`eval:end`. The single quantitative fact reporters cannot derive is the *expected total* — datasets are lazy iterators, so totals are discovered and revised mid-run. `eval:progress` announces exactly that and nothing else. A reporter that has seen no `eval:progress` event renders indeterminate progress (a spinner). There is deliberately no `increment` event — that would duplicate `case:end`.
- **`case:delta` streams live task output, and it is opt-in.** Live-rendering hosts need output as it is generated — the playground shows tokens streaming per case; a `verbose` terminal reporter may too. This is the one sanctioned exception to keeping big data off the wire, and it is bounded: emitters produce `case:delta` only when an installed reporter implements `onCaseDelta` (the manager advertises interest), and `case:end` never carries final `input`/`output`/`expected` regardless.
- **`caseId` is the case's root span ID.** Every case already gets a root span; using its ID as the case identity links every case event to its trace and permalink for free — a reporter can render a "view trace" link for an errored case with no extra lookup.
- **Status is `completed | errored | skipped` — there is no `failed`.** Evals do not assert; scores are continuous and are carried separately. `errored` means an exception in the task or a scorer. Score-threshold CI gating (fail the run when a score is below X) is a separate feature — a `--fail-under`-style flag that maps scores to exit codes — never a per-case status.
- **`input` / `output` / `expected` are not in any payload.** See Principle 7.
- **The schema is versioned.** `run:start` carries a `protocolVersion` so consumers can detect emitters ahead of or behind them. Unknown event types are ignored.

## The Reporter Interface

The lifecycle interface, identical in shape across all three hosts:

```ts
interface EvalReporter {
  onInit?(ctx: EvalReporterContext): Awaitable<void>;

  onRunStart?(run: EvalRun): Awaitable<void>;
  onEvalStart?(evalInfo: EvalInfo): Awaitable<void>;
  onCaseStart?(caseInfo: EvalCaseInfo): Awaitable<void>;
  onCaseEnd?(caseResult: EvalCaseResult): Awaitable<void>;
  onEvalEnd?(evalResult: EvalEnd): Awaitable<void>;
  onRunEnd?(runResult: EvalRunEnd): Awaitable<boolean | void>;

  onError?(error: ReporterError): Awaitable<void>;
  onConsole?(log: ConsoleEvent): Awaitable<void>;
  onProgress?(progress: ProgressEvent): Awaitable<void>;
  onCaseDelta?(delta: CaseDelta): Awaitable<void>;
}
```

The TypeScript shape above is the reference, but the contract is defined language-neutrally:

- **Every method is optional** — a reporter implements only what it renders. How "optional" is expressed follows each language's idiom: optional methods in TS, default no-op implementations in Python, a trait with default methods plus a `finish()` cleanup hook in Rust, interfaces with default methods in Java (already the house idiom there — see `Scorer`), duck typing via `respond_to?` in Ruby, and in Go — the one language with no optional interface methods — an embeddable `NoopReporter` base struct.
- **Methods are synchronous by default.** `Awaitable` is a JS-ism; async hosts may await reporter methods, sync hosts call them directly. A reporter must not assume it can block the event loop or the eval workers — dispatch happens on the manager's cadence (see below).

### The context

```ts
interface EvalReporterContext {
  terminal: Terminal;          // shared output facade — see Output Contract
  profile?: string;            // active config profile, for rendering correct command hints
  outputFile?: string;         // resolved output path for artifact reporters
}
```

The context is small on purpose. It does **not** carry mode flags (`jsonl`, `list`, `verbose`): output format is expressed by *which* reporters are installed, not by flags reporters must each interpret. If a concern seems to need a context flag, it is usually either a run mode (not a reporter concern) or a missing reporter variant.

### The error contract

`onError` fires immediately when an error occurs, so interactive reporters can render it in real time ("your API key is invalid" must not wait for the end of the run). The same errors appear aggregated on `eval:end` (scoped) or `run:end` (unscoped). Summarizing reporters simply don't implement `onError`; interactive ones render it and skip the aggregates. Nothing is lost by ignoring either half.

## The Reporter Manager

Each host runs one manager. Its responsibilities:

- **Dispatch** every event, in arrival order, to every installed reporter.
- **Serialize dispatch.** Eval execution is concurrent in most hosts (goroutine worker pools in Go, thread pools in Ruby and Python, interleaved async in JS), but the manager delivers events to reporters **one at a time, in emission order** — reporters never need internal locking. Cross-case and cross-eval events interleave (that's real concurrency), but per-scope ordering is guaranteed: `case:start` before that case's `case:end`, every case event inside its eval's `eval:start`/`eval:end` bracket. Each host funnels events through whatever serializer is idiomatic (a channel in Go, a queue/mutex in Ruby, a synchronized dispatcher in Java).
- **Isolate failures**: a reporter method that throws is caught; the failure is reported once to stderr and the run continues. (A reporter that throws persistently may be disabled for the rest of the run.)
- **Guarantee termination**: `onRunEnd` fires exactly once, even when the producer crashes or the process is interrupted — the manager synthesizes a `run:end` with `status: "errored"` if the stream ends without one.
- **Advertise interest**: expensive side channels are produced only when someone is listening. If no installed reporter implements `onCaseDelta`, the emitter skips delta generation entirely.
- **Aggregate the exit verdict** — see below.

### Exit codes

The process exit code is decided by the run outcome (runner exit status, run-level errors), not by reporters — with one exception: `onRunEnd` may return `false` to veto success. The manager aggregates these verdicts and the host maps "any veto" to a non-zero exit. This exists for artifact reporters: if `junit` cannot write the file CI depends on, the command must fail even though the evals succeeded. No other reporter method can influence exit status.

## Output Contract

### Streams

- **stdout** — machine output only. At most one installed reporter may claim stdout (e.g. `jsonl`). Selecting two stdout-claiming reporters without routing one to a file is an error at startup, not silent interleaving.
- **stderr** — everything human: progress, tables, echoed user console output, error footnotes.
- **User console output is always decoration.** A `print()` in eval code is echoed to stderr regardless of mode. It never appears on stdout, so machine streams are always parseable. (Reporters may choose to suppress, count, or annotate echoed output — that's rendering policy.)

### The shared terminal

Multiple reporters render to one screen, so raw writes are forbidden. The context provides a `Terminal` facade owning the live region (progress bars/spinners) and coordinated line output:

```ts
interface Terminal {
  println(line: string): void;        // persistent line above the live region
  liveRegion(): LiveRegion;           // progress bars; cleared before final output
  isInteractive(): boolean;           // TTY + animations enabled + not quiet
}
```

Reporters that render progress do so through the live region; reporters that print lines do so through `println`, which suspends the live region to avoid tearing. Cleanup order is a manager contract: live regions are cleared before end-of-run output (footnotes, summaries) prints. Non-TTY output is the same reporter adapting via `isInteractive()` (no animations, plain lines) — not a different reporter.

## Built-in Reporters

| Reporter | Stream | What it renders |
| --- | --- | --- |
| `fancy` *(default)* | stderr | Progress bars, experiment summary tables, deferred error footnote, suppressed-stderr count. Adapts to non-TTY. |
| `verbose` | stderr | Everything `fancy` shows, plus one line per case as it completes, inline errors with stacks, and full echoed stderr. Sibling of `fancy`, not a modifier of it. |
| `dot` | stderr | One character per case (`.` completed, `E` errored, `s` skipped), then the summary. |
| `jsonl` | **stdout** | One JSON object per `eval:end` summary. Claims stdout. |
| `events` | **stdout** | The raw canonical event stream as NDJSON, for tooling. Claims stdout. |
| `junit` | file | JUnit XML: suite per eval, testcase per case, `errored` cases as failures. Requires `--output-file`. May veto success via `onRunEnd`. |
| `github-actions` | stderr | Workflow annotations (`::error` etc.) for errored cases and run errors. |
| `silent` | — | Nothing except fatal errors. |

Custom reporters are an SDK feature: registered in eval code or SDK config, they run inside the runner process with in-process access to full case data (`input`/`output`/`expected`). `bt --reporter` selects built-ins only; it never loads user code.

## Selection and Configuration

Per repo configuration policy, every knob is a `clap` flag with a corresponding env var:

```bash
bt eval .                                   # fancy (default)
bt eval . --reporter=verbose
bt eval . --reporter=dot --reporter=junit --output-file=results.xml
bt eval . --reporter=jsonl > summaries.jsonl
BRAINTRUST_EVAL_REPORTER=github-actions bt eval .
```

- `--reporter` (repeatable) / `BRAINTRUST_EVAL_REPORTER` (comma-separated). Explicit selection **replaces** the default set.
- `--output-file` / `BRAINTRUST_EVAL_OUTPUT_FILE`. Applies to the single file-producing reporter; `--output-file junit=path` disambiguates if there is ever more than one.
- Sugar aliases, kept forever, no deprecations:
  - `--jsonl` ≡ install `jsonl` plus `fancy` with summary tables disabled (see default sets below).
  - `--verbose` ≡ replace the default `fancy` with `verbose` (when no explicit `--reporter` is given). `--verbose` additionally retains its global meaning for diagnostics that live outside the reporter system (e.g. buffered-stderr reporting on retry paths).

### Default reporter sets in `bt`

| Invocation | Installed reporters |
| --- | --- |
| `bt eval` | `fancy` |
| `bt eval --verbose` | `verbose` |
| `bt eval --jsonl` | `jsonl` + `fancy({ summaries: false })` |
| `bt eval --reporter=…` | exactly the named reporters, nothing implicit |
| `bt devserver` | fixed bridge/collector set; reporter flags ignored |

`fancy` alone reproduces the full default terminal experience — progress bars, persistent status lines, console echo and stderr suppression, the deferred-error footnote. There are no always-on companion reporters: console and error policy are part of what distinguishes `fancy` from `verbose`, so they must travel with the rendering reporter.

Built-in reporters may take construction options (precedent: Vitest's default reporter accepts `summary: false`). The `--jsonl` sugar uses this to preserve today's experience exactly: stdout carries only JSON lines, and stderr keeps the bars and footnotes but **no summary table** — the summary has exactly one home. Options have no CLI syntax initially; they exist for sugar mappings and host defaults. An explicit `--reporter=jsonl --reporter=fancy` composes literally and does show tables — explicit selection means you get what you asked for.

One deliberate change under `--jsonl`: user `print()` output is echoed to stderr, not stdout (see Output Contract), so stdout is always parseable JSONL.

### What is *not* a reporter

- **`--list`** — a run mode: the runner enumerates evaluators and executes nothing. Its output is produced from the discovery result directly (optionally formatted as JSON), not by echoing runner stdout. Vitest models this the same way: `vitest list` is a command, not a reporter.
- **`--filter`, `--first`, `--sample`, `--terminate-on-failure`** — run modes and execution policy.
- **`--profile`** — context data reporters use to render correct command hints.
- **Score-threshold gating** (`--fail-under`-style) — a mapping from scores to exit codes, evaluated by the host after `run:end`; reporters render its verdict but do not compute it.

## Architecture: Where Reporters Run

```text
┌───────────────────────────── SDK process ─────────────────────────────┐
│  eval execution ──emits──▶ in-process ReporterManager                 │
│    installed reporters vary by host:                                  │
│      ├── terminal + custom reporters     (standalone use, full data)  │
│      ├── playground SSE bridge           (braintrust eval --dev)      │
│      └── bt bridge reporter ──▶ SSE ──▶ bt (below)                    │
└───────────────────────────────────────────────────────────────────────┘
                                        │
                          ┌─────────────┴──────────────┐
                          ▼                            ▼
              ┌──── bt eval (terminal) ────┐   ┌──── bt devserver ────┐
              │  ReporterManager           │   │  ReporterManager     │
              │   ├── fancy / verbose /    │   │   ├── HTTP bridge    │
              │   │   dot / jsonl / ...    │   │   └── collectors     │
              │   └── junit (artifact)     │   │  (--reporter ignored;│
              └────────────────────────────┘   │   browser UI renders)│
                                               └──────────────────────┘
```

- **SDK standalone** (`braintrust eval`, programmatic `Eval()`): the in-process manager dispatches directly to reporters; progress rendering is itself a reporter, not hardcoded in eval execution.
- **SDK dev server / remote evals** (`braintrust eval --dev`): the playground connects to the SDK's dev server over HTTP. Same manager, one reporter: an SSE bridge encoding canonical events for the app — including `case:delta`, so the playground renders task output live. No terminal reporters are installed, so nothing prints to the server process's stdout as a side effect.
- **`bt eval`**: the runner's bridge reporter serializes canonical events over SSE; `bt`'s manager deserializes and dispatches to terminal reporters. The bridge is just another reporter — the SDK doesn't know or care that `bt` is listening.
- **`bt devserver`**: same manager, different reporter set — an HTTP re-encoding bridge and collectors. `--reporter` has no effect; the browser UI is the display.

**Remote execution is below the protocol.** A scorer (or task) that executes on Braintrust via `function/invoke` is wrapped as an ordinary scorer before the run starts; its scores and errors flow through normal case handling. No event distinguishes local from remote execution — reporters cannot tell, and must not care.

## Wire Compatibility

A bridge reporter owns its wire format, and format negotiation is part of the bridge's design — not a temporary migration shim.

- **The playground bridge serves two formats from one event stream.** Clients request a format per connection via the `x-bt-stream-fmt` header (already an allowed header on the dev servers). Without it, the bridge translates canonical events into the legacy playground vocabulary; with it, the bridge emits canonical events directly. The translation is mechanical and lossy only in ways the legacy format already was:

  | Canonical | Legacy SSE |
  | --- | --- |
  | `eval:start` | `start` (experiment metadata) |
  | `case:delta` | `progress` with `text_delta` / `json_delta` payloads |
  | `error` | `error` |
  | `eval:end` (summary) | `summary` |
  | `run:end` | `done` |
  | `run:start`, `case:start`, `case:end`, `eval:progress`, `console` | dropped (no legacy equivalent) |

- **Consumers detect capability, not version guesswork.** `run:start` carries `protocolVersion`; unknown event types are ignored by all consumers. A client that wants per-case lifecycle, scoped errors, or run totals opts into the canonical format and gets them; a client that never upgrades keeps working indefinitely.
- **The data plane is untouched.** Experiment data still flows through spans/logs; summaries are still computed server-side; `function/invoke` is unchanged. The reporter protocol is presentation-plane only — the sole consumer that ever notices a format change is one that subscribes to a bridge's stream.

The runner scripts ship inside the `bt` binary, so the SSE encoding never skews across versions. The only skew boundary is runner ↔ installed SDK, handled by feature detection with graceful fallback (old SDK: no case events; case-dependent reporters fail with a clear "upgrade braintrust to ≥ X" message).

## Portability Across SDKs

The design was checked against all five SDKs. The lifecycle interface is idiomatic in each; the per-SDK work is additive (new value types and emission points), never breaking:

| SDK | Fits today | Work required to adopt |
| --- | --- | --- |
| JavaScript | Richest starting point: a named-reporter registry (`Reporter`, `reportEval`/`reportRun`), a `ProgressReporter` interface, per-case `reportProgress` → `stream` plumbing keyed by root span ID, and a dev server that already drives the same `Eval()` engine via injected callbacks. | Adapt legacy `Reporter`/`reportRun` to the lifecycle interface behind adapters; replace the ad-hoc `stream`/`onStart` callback injection with the manager + bridge reporters; emit `case:start`/`case:end` (case identity and per-case ticks already exist); move `BarProgressReporter`'s hardcoded increments into a reporter; route `reportRun`'s exit-code logic through `onRunEnd`. |
| Python | Mirror of JS: `ReporterDef` (`report_eval`/`report_run`), `stream`/`on_start` callbacks into `EvalAsync`, per-case root spans, and a dev server sharing the same `run_evaluator` engine. | Same shape as JS: legacy-reporter adapters, manager + bridge reporters in place of callback injection, `case:start`/`case:end` emission points, and stop firing `default_reporter` as a stdout side effect in dev-server mode. |
| Go | Generic `Evaluator[I,R].Run` with clean seams at exactly the callback boundaries; `context.Context` plumbed throughout. | Introduce a per-case result type (case data currently escapes only as OTel spans); serialize dispatch from the worker pool through a channel; `NoopReporter` embed for optionality. |
| Java | `Eval.builder()` + default-method interfaces are the house idioms; Devserver already emits `progress`/`summary`/`done`/`error` over SSE. | Introduce per-case/summary value types (`EvalResult` carries only URLs today); unify the duplicated `Eval` vs `Devserver` execution paths behind one event-emitting executor — the reporter manager is the tool that collapses them. |
| Ruby | Closest fit among the newer SDKs: an `on_progress` per-case callback, summary view models (`ExperimentSummary`, `ScorerStats`), and an SSE devserver already exist. | Structure errors (currently collected as strings, discarding the exception and case identity); add the missing emission points (`run/eval/case:start`); serialize dispatch from worker threads. |

Recurring themes the protocol already accommodates by design: single-eval runs are the norm in-process (see "A run is one invocation"), summaries may be server-fetched (`eval:end.summary` optional), console capture doesn't exist in-process (`console` is a host capability), and concurrent execution is hidden behind the manager's serialized dispatch.

## Example Reporters

Illustrative TypeScript sketches, not production code. Each shows which lifecycle methods a real reporter of that kind needs — and, as importantly, which it can ignore.

### `fancy` (default)

Progress bars driven by case lifecycle plus `eval:progress` totals; errors deferred to a footnote. All output through the shared `Terminal`.

```ts
class FancyReporter implements EvalReporter {
  private terminal!: Terminal;
  private bars = new Map<string, ProgressBar>(); // evalId → bar
  private deferred: ReporterError[] = [];

  onInit(ctx: EvalReporterContext) {
    this.terminal = ctx.terminal;
  }
  onEvalStart(e: EvalInfo) {
    this.bars.set(e.evalId, this.terminal.liveRegion().addBar(e.name));
  }
  onCaseEnd(c: EvalCaseResult) {
    this.bars.get(c.evalId)?.increment(); // position = case:end count
  }
  onProgress(p: ProgressEvent) {
    this.bars.get(p.evalId)?.setTotal(p.totalCases); // spinner until first total
  }
  onError(err: ReporterError) {
    this.deferred.push(err); // render later; verbose sibling prints inline instead
  }
  onEvalEnd(e: EvalEnd) {
    this.bars.get(e.evalId)?.finish();
    if (e.summary) this.terminal.println(formatSummaryTable(e.summary));
  }
  onRunEnd(r: EvalRunEnd) {
    for (const err of this.deferred) {
      this.terminal.println(`  - ${err.message}`);
    }
  }
}
```

### `jsonl`

The machine reporter: one JSON line per completed eval, nothing else. Claims stdout, so the manager rejects a second stdout-claiming reporter at startup.

```ts
class JsonlReporter implements EvalReporter {
  onEvalEnd(e: EvalEnd) {
    if (e.summary) {
      process.stdout.write(JSON.stringify(e.summary) + "\n");
    }
  }
  // No other methods. Progress, console, and errors are stderr concerns
  // handled by other reporters; stdout stays pure.
}
```

### Remote evals (playground SSE bridge)

The dev server's only reporter. Serializes events onto the HTTP response — canonical when the client asked for it via `x-bt-stream-fmt`, legacy translation otherwise. Implementing `onCaseDelta` is what subscribes the run to output streaming.

```ts
class PlaygroundBridgeReporter implements EvalReporter {
  constructor(
    private sse: SSEWriter,
    private format: "canonical" | "legacy",
  ) {}

  onEvalStart(e: EvalInfo) {
    this.format === "canonical"
      ? this.sse.event("eval:start", e)
      : this.sse.event("start", toLegacyStart(e.experiment));
  }
  onCaseDelta(d: CaseDelta) { // presence of this method enables delta emission
    this.format === "canonical"
      ? this.sse.event("case:delta", d)
      : this.sse.event("progress", toLegacyProgress(d)); // text_delta / json_delta
  }
  onCaseEnd(c: EvalCaseResult) {
    if (this.format === "canonical") this.sse.event("case:end", c);
    // legacy: dropped — the old vocabulary has no per-case lifecycle
  }
  onError(err: ReporterError) {
    this.sse.event("error", this.format === "canonical" ? err : { message: err.message });
  }
  onEvalEnd(e: EvalEnd) {
    this.format === "canonical"
      ? this.sse.event("eval:end", e)
      : e.summary && this.sse.event("summary", toLegacySummary(e.summary));
  }
  onRunEnd(r: EvalRunEnd) {
    if (this.format === "canonical") this.sse.event("run:end", r);
    this.sse.event("done", "");
    this.sse.close();
  }
}
```

### `github-actions`

Emits [workflow commands](https://docs.github.com/en/actions/reference/workflows-and-actions/workflow-commands#setting-an-error-message) so errored cases and run errors become annotations on the workflow run. Note the mandated escaping, and the `onRunEnd` veto if annotation output failed.

```ts
class GitHubActionsReporter implements EvalReporter {
  private terminal!: Terminal;

  onInit(ctx: EvalReporterContext) {
    this.terminal = ctx.terminal;
  }
  onCaseEnd(c: EvalCaseResult) {
    if (c.status === "errored" && c.error) {
      this.annotate("error", `case ${c.name ?? c.index} errored`, c.error.message);
    }
  }
  onError(err: ReporterError) {
    this.annotate("error", "eval run error", err.message);
  }
  onEvalEnd(e: EvalEnd) {
    if (e.summary) {
      const scores = Object.entries(e.summary.scores)
        .map(([name, s]) => `${name}=${s.score.toFixed(2)}`)
        .join(" ");
      this.annotate("notice", e.summary.experimentName, scores);
    }
  }

  // ::error title={title}::{message} — data must be escaped per the docs:
  // message: % → %25, \r → %0D, \n → %0A; properties additionally : → %3A, , → %2C
  private annotate(kind: "error" | "notice", title: string, message: string) {
    const prop = (s: string) =>
      s.replace(/%/g, "%25").replace(/\r/g, "%0D").replace(/\n/g, "%0A")
       .replace(/:/g, "%3A").replace(/,/g, "%2C");
    const msg = (s: string) =>
      s.replace(/%/g, "%25").replace(/\r/g, "%0D").replace(/\n/g, "%0A");
    this.terminal.println(`::${kind} title=${prop(title)}::${msg(message)}`);
  }
}
```
