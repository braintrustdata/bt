# Braintrust CLI (`bt`)

## Current Limitations

- `bt eval` is currently Unix-only (Linux/macOS). Windows support is planned.

## Install

### Stable (latest release)

Unix-like systems:

```bash
curl -fsSL https://github.com/braintrustdata/bt/releases/latest/download/bt-installer.sh | sh
```

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://github.com/braintrustdata/bt/releases/latest/download/bt-installer.ps1 | iex"
```

### Canary (latest `main`)

Unix-like systems:

```bash
curl -fsSL https://github.com/braintrustdata/bt/releases/download/canary/bt-installer.sh | sh
```

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://github.com/braintrustdata/bt/releases/download/canary/bt-installer.ps1 | iex"
```

### Canary (exact `main` commit build)

Exact `main` canary builds are published as `canary-<shortsha>`.

Unix-like systems:

```bash
curl -fsSL https://github.com/braintrustdata/bt/releases/download/canary-<shortsha>/bt-installer.sh | sh
```

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://github.com/braintrustdata/bt/releases/download/canary-<shortsha>/bt-installer.ps1 | iex"
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
rm -f "${CARGO_HOME:-$HOME/.cargo}/bin/bt"
rm -rf "$HOME/.config/bt"
hash -r
```

Windows (PowerShell):

```powershell
$cargoHome = if ($env:CARGO_HOME) { $env:CARGO_HOME } else { Join-Path $HOME ".cargo" }
Remove-Item -Force (Join-Path $cargoHome "bin\\bt.exe") -ErrorAction SilentlyContinue
Remove-Item -Recurse -Force (Join-Path $env:APPDATA "bt") -ErrorAction SilentlyContinue
```

## Troubleshooting

- If `bt` is not found after install, start a new shell or add `${CARGO_HOME:-$HOME/.cargo}/bin` to your `PATH`.
- If `bt self update --check` hits GitHub API limits in CI, set `GITHUB_TOKEN` in the environment.
- If your network blocks GitHub asset downloads, install from a machine with direct access or configure your proxy/firewall to allow `github.com` and `api.github.com`.

## `bt eval` runners

- By default, `bt eval` auto-detects a JavaScript runner from your project (`tsx`, `vite-node`, `ts-node`, then `ts-node-esm`).
- You can also set a runner explicitly with `--runner`:
  - `bt eval --runner vite-node tutorial.eval.ts`
  - `bt eval --runner tsx tutorial.eval.ts`
- You do not need to pass a full path for common runners; `bt` resolves local `node_modules/.bin` entries automatically.
- If eval execution fails with ESM/top-level-await related errors, retry with:
  - `bt eval --runner vite-node tutorial.eval.ts`

## `bt sql`

- Runs interactively on TTY by default.
- Runs non-interactively when stdin is not a TTY, when `--non-interactive` is set, or when a query argument is provided.
- Braintrust SQL queries should include a `FROM` clause against a Braintrust table function (for example `project_logs(...)`).
- In non-interactive mode, provide SQL via:
  - Positional query: `bt sql "SELECT id FROM project_logs('<PROJECT_ID>') LIMIT 1"`
  - stdin pipe: `echo "SELECT id FROM project_logs('<PROJECT_ID>') LIMIT 1" | bt sql`
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
  - `--print-queries`: print BTQL/invoke payloads before execution
  - `-j, --json`: machine-readable envelope output
- `logs` filter flags:
  - `--search <TEXT>`
  - `--filter <BTQL_EXPR>`
  - `--window <DURATION>` (default `1h`)
  - `--since <TIMESTAMP>` (overrides `--window`)
- Interactive controls (`bt view logs` TUI):
  - Table: `Up/Down` to select, `Enter` to open trace, `r` to refresh
  - Search: `/` edit, `Enter` apply, `Esc` cancel, `Ctrl+u` clear
  - Open URL: `Ctrl+k`, then `Enter`
  - Detail view: `t` span/thread, `Left/Right` switch panes, `Backspace`/`Esc` back
  - Global: `q` quit

## `bt login` profiles

- Save an API key to a named profile (stored in OS keychain):
  - `bt login set --api-key <KEY> --profile work`
- Save interactively (prompts for auth method first, profile name defaults to org name):
  - `bt login`
  - First prompt chooses: `OAuth (browser)` (default) or `API key`.
  - If your API key can access multiple orgs, `bt` uses a searchable picker (alphabetized) and lets you choose a specific org or no default org (cross-org mode).
  - `bt` confirms the resolved API URL before saving.
- Login with OAuth (browser-based, stores refresh token in OS keychain):
  - `bt login set --oauth --profile work`
  - You can pass `--no-browser` to print the URL without auto-opening.
  - If multiple profiles already exist, `bt` asks whether the new/updated profile should become the default instead of changing it automatically.
- Switch active profile:
  - `bt login use work`
  - Persist per-project (when `.bt/` exists): `bt login use work --local`
  - Persist globally: `bt login use work --global`
- List profiles:
  - `bt login list`
- Delete a profile:
  - `bt login delete work`
- Clear active profile:
  - `bt login logout`
- Show current auth source/profile:
  - `bt login status`
- Force-refresh OAuth access token for debugging:
  - `bt login refresh --profile work`

Auth resolution order for commands is:

1. `--api-key` or `BRAINTRUST_API_KEY`
2. `--profile` or `BRAINTRUST_PROFILE`
3. profile from config files (`.bt/config.json` over `~/.config/bt/config.json`)
4. active saved profile

On Linux, keychain storage uses `secret-tool` (libsecret). On macOS, it uses the `security` keychain utility.

## `bt setup` and `bt docs`

Use setup/docs commands to configure coding-agent skills and workflow docs for Braintrust.

- Configure skills with default setup flow:
  - `bt setup --local`
  - `bt setup --global`
- Explicit skills subcommand:
  - `bt setup skills --local --agent claude --agent codex`
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
- In setup wizards, press `Esc` to go back to the previous step.
- If `--workflow` is omitted in non-interactive mode, setup defaults to all workflows.
- Use `--refresh-docs` in setup (or `bt docs fetch --refresh`) to clear old docs before re-fetching.
- `cursor` is local-only in this flow. If selected with `--global`, `bt` prints a warning and continues installing the other selected agents.
- Claude integration installs the Braintrust skill file under `.claude/skills/braintrust/SKILL.md`.
- Cursor integration installs `.cursor/rules/braintrust.mdc` with the same shared Braintrust guidance plus an auto-generated command-reference excerpt from this README.
- Setup-time docs prefetch writes to `skills/docs` for `--local` and `~/.config/bt/skills/docs` (or `$XDG_CONFIG_HOME/bt/skills/docs`) for `--global`.
- Docs fetch writes LLM-friendly local indexes: `skills/docs/README.md` and per-section `skills/docs/<section>/_index.md`.
- Setup/docs prefetch always includes SQL reference docs at `skills/docs/reference/sql.md`.

Skill smoke-test harness:

- `scripts/skill-smoke-test.sh --agent codex --bt-bin ./target/debug/bt`
- The script scaffolds a demo repo, installs the selected agent skill, writes `AGENT_TASK.md`, and verifies that post-agent changes include both tracing and an eval file.

## Roadmap / TODO

- Add richer channel controls for self-update (for example pinned/branch canary selection).
- Expand release verification and smoke tests for installer flows across more architectures/environments.
- Add `bt eval` support on Windows (today, `bt eval` is Unix-only due to Unix socket usage).
- Add signed artifact verification guidance (signature flow) in install and upgrade docs.
