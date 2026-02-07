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

## Roadmap / TODO

- Add richer channel controls for self-update (for example pinned/branch canary selection).
- Expand release verification and smoke tests for installer flows across macOS/Linux/Windows.
- Add `bt eval` support on Windows (today, `bt eval` is Unix-only due to Unix socket usage).
- Add signed artifact verification guidance (checksums/signatures) in install and upgrade docs.
