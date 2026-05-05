# Plan: add `bt agent schema` and `bt agent guide` to `bt`

## What I reviewed

### `bt` codebase

- Command wiring/dispatch lives in `src/main.rs` (`Cli`, `Commands`, `try_main`, dispatch match).
- Global flags/env are defined in `src/args.rs` (`BaseArgs`) via clap.
- Top-level help is a custom `HELP_TEMPLATE` in `src/main.rs`.
- `.env` loading happens before clap parse in `try_main()` via `env::bootstrap_from_args(&argv)`.

### `pup` reference

Reviewed `pup` at `/Users/parker/.cache/checkouts/github.com/datadog-labs/pup`:

- `pup agent schema`, `pup agent schema --compact`, `pup agent guide`
- pre-clap `--help` interception in agent mode
- agent detection via `--agent`, `--no-agent`, `FORCE_AGENT_MODE`, and known agent env vars
- truthy env parsing: `1` or `true` (case-insensitive)

---

## Final decisions (resolved)

1. Command path: `bt agent schema`
2. Add `bt agent guide` (full guide only in v1; no topic arg)
3. Include extra top-level agent context in verbose schema
4. Compact mode exists in v1 and includes `global_flags`
5. Compact mode output is **minified JSON** (`to_string`) with trailing newline
6. Default/verbose mode output is pretty JSON
7. Visibility policy: include only interfaceable surfaces (exclude clap-hidden items)
8. `read_only` is included on **leaf commands only**
9. Help interception scope: include top-level metadata + scoped command tree
10. Scoped tree for `bt <...> --help` uses deepest matched path and returns a **reconstructed chain**
11. Command/subcommand ordering: alphabetical at every level
12. `args`/`flags` keys are omitted when empty
13. `description` uses `long_about` first, then `about`, verbatim
14. Schema contract starts at `schema_version: 1`
15. Include `context_sources` as repo-relative file paths
16. `docs_url` is `https://braintrust.dev/docs/reference/cli`
17. Include top-level `auth` guidance strings
18. Context shape uses arrays of strings (`best_practices`, `safety_rules`, `usage_patterns`, `anti_patterns`)
19. `bt agent guide` is plain text, local-only, checked-in source content
20. Guide file location: `src/agent/guide.txt`
21. Guide includes a dynamic version header line, then static body
22. Guide should reference safety rules directly (not paraphrase), especially from `AGENTS.md`
23. `bt agent` with no subcommand shows `agent` help
24. Keep scope minimal for now: schema + guide + help interception (no broader agent-mode runtime behavior changes)

### Agent-mode activation/interception policy (mirror pup)

25. Add top-level CLI flags:
   - `--agent` with `env = "BRAINTRUST_AGENT"`
   - `--no-agent` with `env = "BRAINTRUST_NO_AGENT"`
26. Interception precedence:
   - if `--help`/`-h` and `--no-agent` present -> normal text help
   - else if `--help`/`-h` and (`--agent` or detected agent mode) -> verbose JSON schema
   - else -> normal execution
27. Detected agent mode includes:
   - `FORCE_AGENT_MODE` truthy, or
   - known agent env vars truthy
28. Known env vars mirror pup allowlist:
   - `CLAUDECODE`, `CLAUDE_CODE`, `CURSOR_AGENT`, `CODEX`, `OPENAI_CODEX`, `OPENCODE`, `AIDER`, `CLINE`, `WINDSURF_AGENT`, `GITHUB_COPILOT`, `AMAZON_Q`, `AWS_Q_DEVELOPER`, `GEMINI_CODE_ASSIST`, `SRC_CODY`, `AGENT`
29. Truthy means exactly `1` or `true` (case-insensitive)
30. Intercept only `--help` / `-h` (not `bt help ...`), mirroring pup
31. Intercepted output goes to stdout, exits 0
32. Detection runs after `.env` bootstrap (existing ordering in `try_main`)

### `read_only` semantics

33. `read_only=false` means command mutates **any** state (remote or local filesystem/config)
34. Use heuristic by command name/prefix plus bt-specific full-path override map
35. Known explicit classifications:
   - `sync pull` -> `false`
   - `functions pull` -> `false`
   - `status`, `view`, `sql`, `projects view` -> `true`
   - `agent schema`, `agent guide` -> `true`

---

## Implementation plan

## 1) [x] Add `agent` command module

Create `src/agent/mod.rs`:

- clap surface:
  - `AgentArgs { command: Option<AgentCommands> }`
  - `AgentCommands::Schema(AgentSchemaArgs)`
  - `AgentCommands::Guide`
  - `AgentSchemaArgs { compact: bool }`
- `run(base: BaseArgs, args: AgentArgs) -> Result<()>`
  - `None` => print `agent` help
  - `Schema` => emit schema JSON
  - `Guide` => print version header + `include_str!("guide.txt")`

Add `src/agent/guide.txt` with checked-in guidance and direct references to `AGENTS.md` policy/safety language.

## 2) [x] Wire into top-level CLI

Update `src/main.rs`:

- `mod agent;`
- add `Commands::Agent(CLIArgs<agent::AgentArgs>)`
- add `base()` / `base_mut()` arms
- add dispatch arm in async match
- update `HELP_TEMPLATE` command list to include `agent`
- update `HELP_TEMPLATE` Flags block to include `--agent` and `--no-agent`

Update `src/main.rs` (`Cli`):

- add `agent: bool` with `env = "BRAINTRUST_AGENT"`, boolish parser
- add `no_agent: bool` with `env = "BRAINTRUST_NO_AGENT"`, boolish parser, conflicts with `agent`

## 3) [x] Implement agent detection helper (centralized)

In `src/agent/mod.rs` (or `src/agent/detect.rs`):

- table-driven detector list mirroring pup env allowlist
- `is_env_truthy("...")` with pup semantics (`1`/`true`)
- helper for “agent mode enabled by env”

Note: this is a scoped exception to config policy for interoperability with external agent env vars; keep it isolated and documented inline.

## 4) [x] Implement verbose schema builder

Implement builders from clap command tree (`Cli::command()`):

- top-level object fields:
  - `schema_version`
  - `bt_version`
  - `description`
  - `docs_url`
  - `auth`
  - `global_flags`
  - `context`
  - `context_sources`
  - `commands`
- recursive command fields:
  - `name`, `full_path`, `description`
  - optional `args`
  - optional `flags`
  - optional `subcommands`
  - `read_only` only on leaf nodes

Arg/flag metadata:

- `required`, `default`, `env`, `short`, `long`, `value_names`, `possible_values`, `multiple`
- trust clap metadata; no inferred/redacted fields beyond hidden filtering

Filtering rules:

- exclude hidden commands (`is_hide_set`)
- exclude hidden args/flags (`is_hide_set`)
- exclude clap help/version internals

Ordering:

- sort commands/subcommands alphabetically
- keep positional args in declaration order with 1-based `index`

## 5) [x] Implement compact schema builder

`--compact` output:

- fields: `schema_version`, `bt_version`, `global_flags`, `commands`
- reduced command shape: `name`, `full_path`, optional `flags`, optional `subcommands`
- serialize minified JSON (`serde_json::to_string`) + newline

## 6) [x] Implement `--help` interception (agent mode)

In `try_main()` pre-clap help path:

- detect `--help`/`-h`
- apply precedence:
  - `--no-agent` => skip interception
  - else if `--agent` or env detection => intercept
- scope to deepest matched command path from argv (mirror pup parsing behavior)
- build scoped reconstructed chain in `commands`
- include normal top-level metadata/context/global_flags/auth in scoped response
- print verbose pretty JSON and return `Ok(())`

## 7) [x] `read_only` classifier

- implement pup-like mutating-name heuristic
- add bt-specific full-path overrides for correctness
- ensure leaf-only emission

## 8) [x] Tests

### Unit tests (`src/agent/mod.rs`)

- schema shape and required top-level fields
- global flags include `--agent` and `--no-agent`
- hidden filtering behavior
- positional index ordering
- alphabetical ordering
- compact mode shape and minified serialization
- scoped reconstructed-chain behavior
- env truthy semantics + allowlist detection
- interception precedence (`--no-agent` over env/`--agent`)
- `read_only` heuristic + override cases (`sync pull`, `functions pull`, etc.)

### Integration tests (`tests/cli.rs`)

- `bt agent schema` valid JSON
- `bt agent schema --compact` valid minified JSON
- `bt agent guide` outputs version header + expected anchors
- agent-mode `bt --help` returns verbose JSON
- agent-mode deep help (`bt projects create --help`) returns scoped reconstructed chain
- non-agent `bt --help` remains text help

Status:

- Added new unit tests under `src/agent/mod.rs` for schema shape, ordering, flags, env truthiness, interception precedence, and `read_only` classification.
- Added integration tests in `tests/cli.rs` for `bt agent schema`, `bt agent schema --compact`, `bt agent guide`, and agent-mode help interception behavior.

## 9) [x] Documentation updates

- `README.md`:
  - add `agent` command(s) to command table
  - add examples:
    - `bt agent schema`
    - `bt agent schema --compact`
    - `bt agent guide`
    - `CLAUDE_CODE=true bt --help`
- top-level help template updates in `src/main.rs`

Status:

- Updated `README.md` command table and added a `bt agent` usage section.
- Updated top-level help template command catalog and flags.

## Completion status

- [x] All planned implementation phases completed.

---

## Deferred (post-v1)

- subtree selector flags for `bt agent schema` beyond help-scoping
- richer structured context objects (titles/sources per entry)
- additional agent-mode behaviors beyond help interception
