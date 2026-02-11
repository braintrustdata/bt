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

### Canary (specific branch)

Branch canary aliases are published as `canary-<branch-slug>`, where `<branch-slug>` is:

- lowercased branch name
- non-alphanumeric runs replaced with `-`
- leading/trailing `-` removed
- truncated to 40 chars

Example for branch `feature/sql-v2`: alias tag `canary-feature-sql-v2`.

Unix-like systems:

```bash
curl -fsSL https://github.com/braintrustdata/bt/releases/download/canary-<branch-slug>/bt-installer.sh | sh
```

Windows (PowerShell):

```powershell
powershell -ExecutionPolicy Bypass -c "irm https://github.com/braintrustdata/bt/releases/download/canary-<branch-slug>/bt-installer.ps1 | iex"
```

### Canary (exact commit build)

Exact canary builds are published as:

- `canary-<shortsha>` for `main`
- `canary-<branch-slug>-<shortsha>` for non-main branches

Use those tags in `/releases/download/<tag>/...` to pin an install.

## Verify

```bash
bt --version
```

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
# install latest stable
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

## `bt traces`

- Open an interactive terminal trace viewer for a project:
  - `bt -p <project-name> traces`
  - `bt traces --project-id <project-id>`
- Optional flags:
  - `--limit <N>`: number of summary traces to load (default `50`)
  - `--preview-length <N>`: preview length used in summary rows (default `125`)
  - `--print-queries`: print each BTQL query and invoke payload before execution
- Controls:
  - Trace table: `Up/Down` to select, `Enter` to open trace, `r` to refresh
  - Detail view: `t` toggles between span detail and thread view (project default preprocessor)
  - Split-pane focus: `Right` focuses detail pane, `Left` focuses span tree
  - Span/detail nav: with tree focus `Up/Down` selects spans; in span detail focus `Up/Down` scrolls (`PgUp/PgDn` also scroll)
  - Thread blocks: in thread detail focus, `Up/Down` selects messages and `Enter` expands/collapses (collapsed by default)
  - Back: `Backspace` or `Esc` returns to trace table
  - Global: `q` to quit

## Roadmap / TODO

- Add richer channel controls for self-update (for example pinned/branch canary selection).
- Expand release verification and smoke tests for installer flows across more architectures/environments.
- Add `bt eval` support on Windows (today, `bt eval` is Unix-only due to Unix socket usage).
- Add signed artifact verification guidance (signature flow) in install and upgrade docs.
