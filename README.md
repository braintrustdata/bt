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

## Commands

| Command          | Description                                                        |
| ---------------- | ------------------------------------------------------------------ |
| `bt init`        | Initialize `.bt/` config directory and link to a project           |
| `bt auth`        | Manage authentication profiles                                     |
| `bt switch`      | Switch org and project context                                     |
| `bt status`      | Show current org and project context                               |
| `bt config`      | View and modify config                                             |
| `bt eval`        | Run eval files (Unix only)                                         |
| `bt sql`         | Run SQL queries against Braintrust                                 |
| `bt view`        | View logs, traces, and spans                                       |
| `bt projects`    | Manage projects (list, create, view, delete)                       |
| `bt prompts`     | Manage prompts (list, view, delete)                                |
| `bt sync`        | Synchronize project logs between Braintrust and local NDJSON files |
| `bt self update` | Update bt in-place                                                 |

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
- In non-interactive mode, provide SQL via:
  - Positional query: `bt sql "SELECT 1"`
  - stdin pipe: `echo "SELECT 1" | bt sql`

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
  - `bt auth list`
- Delete a profile:
  - `bt auth delete work`
- Log out (remove a saved profile):
  - `bt auth logout`
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

## `bt config`

View and modify config values (`org`, `project`, `api_url`, `app_url`):

- `bt config list` — list all config values
- `bt config list --verbose` — show values grouped by source
- `bt config get <key>` — get a specific config value
- `bt config set <key> <value>` — set a config value
- `bt config unset <key>` — remove a config value
- Scope flags: `--global` (user-level) or `--local` (project-level, requires `.bt/`)

## Roadmap / TODO

- Add richer channel controls for self-update (for example pinned/branch canary selection).
- Expand release verification and smoke tests for installer flows across more architectures/environments.
- Add `bt eval` support on Windows (today, `bt eval` is Unix-only due to Unix socket usage).
- Add signed artifact verification guidance (signature flow) in install and upgrade docs.
