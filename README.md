# Braintrust CLI (`bt`)

## Current Limitations

- `bt eval` is currently Unix-only (Linux/macOS). Windows support is planned.

## Install

### Unix (macOS / Linux)

```bash
curl -fsSL https://bt.dev/cli/install.sh | bash
```

Install a specific version:

```bash
curl -fsSL https://bt.dev/cli/install.sh | bash -s -- --version 0.2.0
```

Install the latest canary build (latest `main`):

```bash
curl -fsSL https://bt.dev/cli/install.sh | bash -s -- --canary
```

### Windows (PowerShell)

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://raw.githubusercontent.com/braintrustdata/bt/main/install.ps1 | iex"
```

Install a specific version:

```powershell
$env:BT_VERSION='0.1.2'; powershell -ExecutionPolicy Bypass -c "irm https://raw.githubusercontent.com/braintrustdata/bt/main/install.ps1 | iex"
```

Canary:

```powershell
$env:BT_CHANNEL='canary'; powershell -ExecutionPolicy Bypass -c "irm https://raw.githubusercontent.com/braintrustdata/bt/main/install.ps1 | iex"
```

### PR/branch builds (no release)

Non-`main` branch builds are available as GitHub Actions run artifacts (download from the workflow run page or with `gh run download`). They are not published as GitHub Releases.

## Verify

```bash
bt --version
```

Canary builds include a canary suffix in the reported version string.
Local/dev builds default to `-canary.<shortsha>` when git metadata is available (fallback: `-canary.dev`).

On first install, open a new shell if `bt` is not found immediately.

For manual archive installs, verify checksums before extracting:

```bash
curl -fsSL -O "https://github.com/braintrustdata/bt/releases/download/<tag>/bt-<target>.tar.gz"
curl -fsSL -O "https://github.com/braintrustdata/bt/releases/download/<tag>/bt-<target>.tar.gz.sha256"
shasum -a 256 -c "bt-<target>.tar.gz.sha256"
```

## Self Update

`bt` can self-update when installed via the official installer.

```bash
# update on the current build channel (canary for local/dev builds, stable for official releases)
bt self update

# check without installing
bt self update --check

# switch/update to latest mainline canary
bt self update --channel canary
```

If `bt` was installed via another package manager (Homebrew, apt, choco, etc), use that package manager to update instead.

## Uninstall

Unix-like systems:

```bash
rm -f "${XDG_BIN_HOME:-${XDG_DATA_HOME:-$HOME/.local}/bin}/bt"
rm -rf "${XDG_CONFIG_HOME:-$HOME/.config}/bt"
hash -r
```

Windows (PowerShell):

```powershell
$cargoHome = if ($env:CARGO_HOME) { $env:CARGO_HOME } else { Join-Path $HOME ".cargo" }
Remove-Item -Force (Join-Path $cargoHome "bin\\bt.exe") -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force (Join-Path $env:APPDATA "bt") -ErrorAction SilentlyContinue
```

## Troubleshooting

- If `bt` is not found after install, start a new shell or add `${XDG_BIN_HOME:-$HOME/.local/bin}` to your `PATH`.
- If `bt self update --check` hits GitHub API limits in CI, set `GITHUB_TOKEN` in the environment.
- If your network blocks GitHub asset downloads, install from a machine with direct access or configure your proxy/firewall to allow `github.com` and `api.github.com`.

## Commands

| Command          | Description                                                          |
| ---------------- | -------------------------------------------------------------------- |
| `bt init`        | Initialize `.bt/` config directory and link to a project             |
| `bt auth`        | Authenticate with Braintrust                                         |
| `bt switch`      | Switch org and project context                                       |
| `bt status`      | Show current org and project context                                 |
| `bt eval`        | Run eval files (Unix only)                                           |
| `bt sql`         | Run SQL queries against Braintrust                                   |
| `bt view`        | View logs, traces, and spans                                         |
| `bt projects`    | Manage projects (list, create, view, delete)                         |
| `bt datasets`    | Manage remote datasets (list, create, upload, refresh, view, delete) and dataset versions                                   |
| `bt prompts`     | Manage prompts (list, view, delete)                                  |
| `bt sync`        | Synchronize project logs between Braintrust and local NDJSON files   |
| `bt self update` | Update bt in-place                                                   |

## `bt eval`

**File selection:**

- `bt eval` — discover and run all eval files in the current directory (recursive)
- `bt eval tests/` — discover eval files under a specific directory
- `bt eval "tests/**/*.eval.ts"` — glob pattern
- `bt eval a.eval.ts b.eval.ts` — one or more explicit files

Files inside `node_modules`, `.venv`, `venv`, `site-packages`, `dist-packages`, and `__pycache__` are excluded from automatic discovery. Explicit paths and globs bypass these exclusions.

**Runners:**

- By default, `bt eval` auto-detects a JavaScript runner from your project (`tsx`, `vite-node`, `ts-node`, then `ts-node-esm`).
- Set a runner explicitly with `--runner` / `BT_EVAL_RUNNER`:
  - `bt eval --runner vite-node tutorial.eval.ts`
  - `bt eval --runner tsx tutorial.eval.ts`
- `bt` resolves local `node_modules/.bin` entries automatically — no need for a full path.
- If eval execution fails with ESM/top-level-await related errors, retry with:
  - `bt eval --runner vite-node tutorial.eval.ts`

**Passing arguments to the eval file:**

Use `--` to forward extra arguments to the eval file via `process.argv`:

```bash
bt eval foo.eval.ts -- --description "Prod" --shard=1/4
```

## `bt datasets`

- `bt datasets` works directly against remote Braintrust datasets — no local `bt sync` artifact flow is required.
- `bt datasets create my-dataset` — create an empty remote dataset in the current project.
- `bt datasets create my-dataset --file records.jsonl` — create the remote dataset and seed it from a JSON/JSONL file.
- `cat records.jsonl | bt datasets create my-dataset` — create the dataset and seed it from stdin.
- `bt datasets create my-dataset --rows '[{"id":"case-1","input":{"text":"hi"},"expected":"hello"}]'` — create the dataset from inline JSON rows.
- `bt datasets add my-dataset --file records.jsonl` — add rows to an existing remote dataset.
- `bt datasets append my-dataset --rows '[{"id":"case-2","input":{"text":"bye"},"expected":"goodbye"}]'` — alias for `add`/`upload` when you want to append rows explicitly.
- `bt datasets upload my-dataset --file records.jsonl` — legacy-compatible alias for `add`.
- `bt datasets refresh my-dataset --file records.jsonl --id-field metadata.case_id --prune` — deterministically upsert rows by stable record id and optionally prune stale remote rows.
- `bt datasets view my-dataset` — show dataset metadata and the important row fields by default; pass `--verbose` to inspect full row payloads.
- `bt datasets versions create my-dataset` — create a named dataset version using a generated `<user>-<timestamp>` name and the dataset's current head xact.
- `bt datasets versions create my-dataset baseline` — create a named dataset version from the dataset's current head xact.
- `bt datasets versions list my-dataset` — list saved dataset versions for a dataset.
- `bt datasets versions restore my-dataset` — on a TTY, interactively pick a saved version to restore.
- `bt datasets versions restore my-dataset --name baseline` — preview restoring a dataset to a saved version, including `rows_to_restore` / `rows_to_delete`, then confirm before applying it.
- `bt datasets versions restore my-dataset --version 1000192656880881099 --force` — preview and immediately restore a dataset to a transaction id without prompting.
- Applied restores return `xact_id`, `rows_restored`, and `rows_deleted`.
- `bt datasets versions create my-dataset baseline --xact-id 1000192656880881099` — create a named dataset version from a transaction id.
- `bt datasets versions create my-dataset baseline --xact-id 1000192656880881099 --description "Initial snapshot"` — attach an optional description to the dataset version.
- `bt datasets versions restore ...` resolves `--name` through the dataset snapshot list endpoint; if you already know the xact id, pass it directly with `--version`.
- Accepted row fields for create/upload/update/refresh are `id` (or your `--id-field` path), `input`, `expected`, `metadata`, and `tags`.

## `bt sql`

- Runs interactively on TTY by default.
- Runs non-interactively when stdin is not a TTY, when `--non-interactive` is set, or when a query argument is provided.
- Braintrust SQL queries should include a `FROM` clause against a Braintrust table function (for example `project_logs(...)`).
- In non-interactive mode, provide SQL via:
  - Positional query: `bt sql "SELECT id FROM project_logs('<PROJECT_ID>') LIMIT 1"`
  - stdin pipe: `echo "SELECT id FROM project_logs('<PROJECT_ID>') LIMIT 1" | bt sql`
- Pagination:
  - SQL queries: pass cursor tokens inline with `OFFSET '<CURSOR_TOKEN>'`.
- Quick guidance:
  - Prefer filtering with `WHERE`; use `HAVING` only after aggregation.
  - Unsupported SQL features include joins, subqueries, unions/intersections, and window functions.
  - Use explicit aliases for computed fields and cast timestamps/JSON values when needed.
  - Full reference: `https://www.braintrust.dev/docs/reference/sql`

## `bt view`

- List logs (interactive on TTY by default, non-interactive otherwise):
  - `bt view logs`
  - `bt view logs --object-ref project_logs:<project-id>`
  - `bt view logs --list-mode spans` (one row per span)
- Fetch one trace (returns truncated span rows by default):
  - `bt view trace --object-ref project_logs:<project-id> --trace-id <root-span-id>`
  - `bt view trace --url <braintrust-trace-url>`
- Fetch one span (full payload):
  - `bt view span --object-ref project_logs:<project-id> --id <row-id>`
- Common flags:
  - `--limit <N>`: max rows per request/page
  - `--cursor <CURSOR>`: continue pagination explicitly
  - `--preview-length <N>`: truncation length for non-single-span fetches
  - `--print-queries`: print SQL/invoke payloads before execution
  - `-j, --json`: machine-readable envelope output
- `logs` filter flags:
  - `--search <TEXT>`
  - `--filter <EXPR>`
  - `--window <DURATION>` (default `1h`)
  - `--since <TIMESTAMP>` (overrides `--window`)
- Interactive controls (`bt view logs` TUI):
  - Table: `Up/Down` to select, `Enter` to open trace, `r` to refresh
  - Search: `/` edit, `Enter` apply, `Esc` cancel, `Ctrl+u` clear
  - Open URL: `Ctrl+k`, then `Enter`
  - Detail view: `t` span/thread, `Left/Right` switch panes, `Backspace`/`Esc` back
  - Global: `q` quit

## `bt util xact`

Local transaction-id conversion helpers:

- Convert transaction id to pretty version id:
  - `bt util xact to-pretty 1000192656880881099`
- Convert pretty version id to transaction id:
  - `bt util xact from-pretty 81cd05ee665fdfb3`
- Convert transaction id to timestamp:
  - `bt util xact to-time 1000192656880881099`
  - `bt util xact to-time 1000192656880881099 --format unix`
- Convert timestamp to transaction id:
  - `bt util xact from-time` (defaults to current time)
  - `bt util xact from-time 2025-01-01` (date-only ISO at UTC midnight)
  - `bt util xact from-time 2024-03-14T18:00:00Z`
  - `bt util xact from-time 1710439200 --input unix --counter 42`
- Inspect any xact value:
  - `bt util xact inspect 1000192656880881099`
  - `bt util xact inspect 81cd05ee665fdfb3`

## `bt auth`

- Authenticate interactively (prompts for auth method, profile name defaults to org name):
  - `bt auth login`
  - First prompt chooses: `OAuth (browser)` (default) or `API key`.
  - If your API key can access multiple orgs, `bt` uses a searchable picker (alphabetized) and lets you choose a specific org or no default org (cross-org mode).
  - `bt` confirms the resolved API URL before saving.
- Login with OAuth (browser-based, stores refresh token in secure credential store):
  - `bt auth login --oauth --profile work`
  - You can pass `--no-browser` to print the URL without auto-opening.
  - On remote/SSH hosts, paste the final callback URL from your local browser if localhost callback cannot be delivered.
- List profiles:
  - `bt auth profiles`
- Log out (remove a saved profile):
  - `bt auth logout`
  - `bt auth logout --force` (skip confirmation)
- Show current auth source/profile:
  - `bt auth status`
- Force-refresh OAuth access token for debugging:
  - `bt auth refresh --profile work`

Auth resolution order for commands is:

1. `--api-key` or `BRAINTRUST_API_KEY` (unless `--prefer-profile` is set)
2. `--profile` or `BRAINTRUST_PROFILE`
3. Org-based profile match (profile whose org matches `--org`/config org)
4. Single-profile auto-select (if only one profile exists)

On Linux, secure storage uses `secret-tool` (libsecret) with a running Secret Service daemon. On macOS, it uses the `security` keychain utility. If a secure store is unavailable, `bt` falls back to a plaintext secrets file with `0600` permissions.

## `bt switch`

Interactively switch org and project context:

- `bt switch` — interactive picker for org and project
- `bt switch myproject` — switch to a project by name
- `bt switch myorg/myproject` — switch to a specific org and project
- `bt switch --global` — persist to global config (`~/.config/bt/config.json`)
- `bt switch --local` — persist to local config (`.bt/config.json`)

## `bt status`

Show current org and project context:

- `bt status` — display current org, project, and config source
- `bt status --verbose` — show detailed config resolution
- `bt status -j` — JSON output

## `bt setup` and `bt docs`

Use setup/docs commands to configure coding-agent skills and workflow docs for Braintrust.

- Configure skills with default setup flow:
  - `bt setup --local`
  - `bt setup --global`
- Explicit skills subcommand:
  - `bt setup skills --local --agent claude --agent codex`
- Instrument a repo with an agent:
  - `bt setup instrument --agent codex`
  - `bt setup instrument --agent claude --agent-cmd '<your claude command>'`
- Configure MCP:
  - `bt setup mcp --local --agent claude --agent codex`
  - `bt setup mcp --global --yes`
- Diagnose setup:
  - `bt setup doctor`
  - `bt setup doctor --local`
  - `bt setup doctor --global`
- Prefetch specific workflow docs during setup:
  - `bt setup skills --local --workflow instrument --workflow evaluate`
- Skip docs prefetch during setup:
  - `bt setup skills --local --no-fetch-docs`
- Force-refresh prefetched docs during setup (clears existing docs output first):
  - `bt setup skills --local --refresh-docs`
- Non-interactive runs should pass an explicit scope:
  - `bt setup skills --global --yes`
- Sync workflow docs markdown from Braintrust Docs (Mintlify `llms.txt`):
  - `bt docs fetch --workflow instrument --workflow evaluate`
  - `bt docs fetch --refresh` (clear output dir first to avoid stale pages)
  - `bt docs fetch --dry-run`
  - `bt docs fetch --strict` (fail if any page download fails)

Current behavior:

- Supported agents: `claude`, `codex`, `cursor`, `opencode`.
- If no `--agent` values are provided, `bt` auto-detects likely agents from local/global context and falls back to all supported agents when none are detected.
- In interactive TTY mode, skills setup shows a checklist so you can select/deselect agents before install.
- In interactive TTY mode, setup also shows a workflow checklist and prefetches those docs automatically.
- Running bare `bt setup` opens a top-level setup wizard with: `instrument`, `skills`, `mcp`, and `doctor`.
- `bt setup instrument` always targets the local git repo, reuses the `skills` setup flow, and guarantees `instrument` docs are included.
- In interactive mode, `bt setup instrument` always includes `instrument` and lets you multi-select additional docs for `observe` and/or `evaluate`.
- `bt setup instrument` defaults to `codex` when no agent is specified; pass `--agent-cmd` for agents without a built-in default command.
- In setup wizards, press `Esc` to go back to the previous step.
- If `--workflow` is omitted in non-interactive mode, setup defaults to all workflows.
- Use `--refresh-docs` in setup (or `bt docs fetch --refresh`) to clear old docs before re-fetching.
- `cursor` is local-only in this flow. If selected with `--global`, `bt` prints a warning and continues installing the other selected agents.
- Claude integration installs the Braintrust skill file under `.claude/skills/braintrust/SKILL.md`.
- Cursor integration installs `.cursor/rules/braintrust.mdc` with the same shared Braintrust guidance plus an auto-generated command-reference excerpt from this README.
- Setup-time docs prefetch writes to `.bt/skills/docs` for `--local` and `~/.config/bt/skills/docs` (or `$XDG_CONFIG_HOME/bt/skills/docs`) for `--global`.
- Docs fetch writes LLM-friendly local indexes: `.bt/skills/docs/README.md` and per-section `.bt/skills/docs/<section>/_index.md` (or the global equivalents under `~/.config/bt/skills/docs`).
- Setup/docs prefetch always includes SQL reference docs at `.bt/skills/docs/reference/sql.md` (or `~/.config/bt/skills/docs/reference/sql.md` for global setup).

Skill smoke-test harness:

- `scripts/skill-smoke-test.sh --agent codex --bt-bin ./target/debug/bt`
- The script scaffolds a demo repo, installs the selected agent skill, writes `AGENT_TASK.md`, and verifies that post-agent changes include both tracing and an eval file.

## Roadmap / TODO

- Add richer channel controls for self-update (for example pinned/branch canary selection).
- Expand release verification and smoke tests for installer flows across more architectures/environments.
- Add `bt eval` support on Windows (today, `bt eval` is Unix-only due to Unix socket usage).
- Add signed artifact verification guidance (signature flow) in install and upgrade docs.
