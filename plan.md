# Plan: Make `bt eval` Work Natively on Windows (CLI + SDK)

## Goal

Ship reliable native Windows support for `bt eval` (no WSL), with deterministic SSE transport, passing Windows build/tests, and no regressions on macOS/Linux.

## Non-Goals

- No protocol redesign (keep existing SSE event protocol).
- No new ad-hoc runtime env configuration outside existing CLI-owned `BT_EVAL_*` contract.
- No behavior changes to unrelated commands.

## Current Failures to Address

1. **SDK compile break on Windows**
- `braintrust-sdk-rust` references `std::os::unix::net::UnixStream` unguarded in `src/eval/bt_runner.rs`.
- Windows build fails at compile time before TCP fallback can run.

2. **Transport ambiguity from inherited env**
- Runners check `BT_EVAL_SSE_SOCK` before `BT_EVAL_SSE_ADDR`.
- If both are present, runner may choose wrong transport and fail to stream.

## Workstream A: `braintrust-sdk-rust` (must land first)

Owner: SDK repo (`../braintrust-sdk-rust`)

### A1. Make SSE writer platform-safe

- Refactor `create_sse_writer()` in `src/eval/bt_runner.rs`:
  - Unix-only Unix socket connect path behind `#[cfg(unix)]`.
  - Keep TCP connect path available on all platforms.
  - Ensure non-Unix builds never reference `std::os::unix::*`.

### A2. Keep deterministic transport semantics

- On Unix:
  - Preserve current preference order (`BT_EVAL_SSE_SOCK`, then `BT_EVAL_SSE_ADDR`).
- On non-Unix:
  - Ignore `BT_EVAL_SSE_SOCK` (optionally log unsupported warning) and use `BT_EVAL_SSE_ADDR`.

### A3. Add SDK tests

- Unit tests for:
  - env-driven selection logic by platform
  - valid/invalid `BT_EVAL_SSE_ADDR` handling
- Add at least one CI check that compiles SDK eval on Windows.

### A4. Merge and pin

- Merge SDK fix.
- Record commit SHA to consume from `btcli`.

## Workstream B: `btcli` Runtime Wiring

Owner: CLI repo (`btcli`)

### B1. Keep platform listener ownership in CLI

- Retain current listener model in `src/eval.rs`:
  - Unix binds local Unix socket and exports `BT_EVAL_SSE_SOCK`.
  - Non-Unix binds localhost TCP and exports `BT_EVAL_SSE_ADDR`.

### B2. Enforce one transport env in child process

- Before spawn, set selected SSE env key and remove the conflicting key.
- This avoids inherited-env collisions across Rust/JS/Python runners.

Status: implemented in `src/eval.rs` (keep and test).

### B3. Add CLI tests for transport env determinism

- Add unit/integration coverage that verifies:
  - chosen key is present in child env
  - opposite key is removed
  - Windows path uses `BT_EVAL_SSE_ADDR`
  - Unix path uses `BT_EVAL_SSE_SOCK`

## Workstream C: JS/Python Runner Hardening (recommended)

Owner: CLI repo runner scripts

### C1. Keep current compatibility behavior

- JS and Python runners may keep env priority as-is, since CLI now sanitizes env.

### C2. Optional defensive hardening

- On non-Unix, if `BT_EVAL_SSE_SOCK` is present:
  - skip Unix socket attempt (or emit explicit warning)
  - continue with `BT_EVAL_SSE_ADDR` if provided

This reduces dependency on perfect parent env hygiene and improves diagnostics.

## Workstream D: Dependency Update in `btcli`

Owner: CLI repo

### D1. Bump SDK revision

- Update `braintrust-sdk-rust` `rev` in `Cargo.toml`.
- Refresh `Cargo.lock`.

### D2. Verify compile and behavior

- Ensure `cargo check` passes locally.
- Ensure release-canary Windows artifact build compiles with new SDK rev.

## Workstream E: CI + Validation Matrix

Owner: CLI + SDK repos

### E1. Required validation

1. **Windows build path**
- `x86_64-pc-windows-msvc` build succeeds in release-canary/release matrix.

2. **Runtime eval path (Windows)**
- JS eval run emits SSE (`start`, `summary`, `done`) and exits correctly.
- Python eval run emits SSE and exits correctly.

3. **Conflict env regression**
- Simulate inherited `BT_EVAL_SSE_SOCK` while Windows listener selects TCP.
- Verify child still uses TCP via `BT_EVAL_SSE_ADDR`.

4. **Unix regression check**
- Unix eval path continues to use Unix sockets with no behavior regressions.

### E2. Nice-to-have validation

- Add a dedicated Windows eval integration job (JS + Python smoke tests).

## Sequencing (Execution Order)

1. Implement and merge **Workstream A** in `braintrust-sdk-rust`.
2. Land/keep **Workstream B** in `btcli` with tests.
3. Apply **Workstream D** SDK rev bump in `btcli`.
4. Run **Workstream E** validation gates.
5. Optional **Workstream C** hardening if diagnostics still weak.

## Risks and Mitigations

1. Risk: SDK change lands but CLI still has env collisions.
- Mitigation: keep CLI opposite-key removal as a hard requirement.

2. Risk: CI only proves compile, not runtime SSE correctness.
- Mitigation: add Windows eval smoke tests that assert SSE event flow.

3. Risk: Unix regressions from transport refactor.
- Mitigation: keep Unix ordering unchanged and run existing Unix eval tests.

## Definition of Done

- Windows artifact build succeeds without `std::os::unix` compile errors.
- `bt eval` runs natively on Windows for JS and Python with working SSE progress/summary.
- Conflicting inherited SSE env vars do not break transport selection.
- macOS/Linux behavior remains unchanged.
- `btcli` is pinned to an SDK commit that contains the Windows-safe eval transport fix.
