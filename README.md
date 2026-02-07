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

## Roadmap / TODO

- Add first-class self-update command in the CLI (`bt self update`) with stable/canary channels.
- Improve release verification and smoke tests for installer flows across macOS/Linux/Windows.
- Add `bt eval` support on Windows (today, `bt eval` is Unix-only due to Unix socket usage).
- Add clearer upgrade/downgrade docs for channel switching and pinned canary installs.
