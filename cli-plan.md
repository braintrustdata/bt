# Braintrust CLI Distribution Plan

## Goals

- Provide a one-command installer for Unix-like systems and Windows.
- Publish stable release artifacts from tags.
- Publish per-commit canary artifacts for fast testing.
- Support in-CLI self-update for installer-based installs.

## Constraints and Assumptions

- CLI is a Rust binary (`bt`) and must not require Cargo for end users.
- The repo depends on a private GitHub dependency (`braintrust-sdk-rust`), so CI jobs that build must configure authenticated git access.
- Installers should place binaries in a user-scoped location and handle PATH setup where possible.

## Channel Strategy

- `stable`: semver tags like `v0.2.0`.
- `canary`: every branch push (including PR branches) with branch-aware tags:
  - immutable: `canary-<branch-slug>-<shortsha>` (main keeps `canary-<shortsha>`)
  - moving alias: `canary-<branch-slug>` (main keeps `canary`)

## PR Breakdown

## PR1: Dist Configuration (Done)

- Add `cargo-dist` metadata to `Cargo.toml`:
  - Installer generation: `shell`, `powershell`.
  - Targets:
    - `aarch64-apple-darwin`
    - `x86_64-apple-darwin`
    - `aarch64-unknown-linux-gnu`
    - `x86_64-unknown-linux-gnu`
    - `x86_64-pc-windows-msvc`
  - Archives:
    - Unix `.tar.gz`
    - Windows `.zip`
  - Installer behavior:
    - `install-path = "CARGO_HOME"`
    - `install-updater = true`
  - CI backend: GitHub.

Deliverables:

- `Cargo.toml` updated with `[profile.dist]` and `[package.metadata.dist]`.

## PR2: Stable Release Workflow (Done)

- Add `.github/workflows/release.yml` for tag-based releases (`v*.*.*`).
- Workflow stages:
  - `plan`: compute dist manifest/release plan.
  - `build-local-artifacts`: matrix build for target-specific binaries.
  - `build-global-artifacts`: installers/checksums and other global artifacts.
  - `host`: upload artifacts and prepare release metadata.
  - `announce`: create GitHub Release and attach artifacts.
- Reuse private dependency auth pattern:
  - configure git URL rewrite using `BT_GITHUB_TOKEN` fallback to `GITHUB_TOKEN`.

Deliverables:

- `.github/workflows/release.yml`
- First installable stable artifacts after pushing a semver tag.

## PR3: Canary (Per-Commit) Releases (Done, Pending First Run)

Scope:

- Added `.github/workflows/release-canary.yml`.
- Trigger on pushes to all branches and `workflow_dispatch`.
- Builds the same target matrix as stable.
- Publishes prerelease artifacts using:
  - immutable tag:
    - `canary-<shortsha>` for `main`
    - `canary-<branch-slug>-<shortsha>` for non-main branches
  - moving alias tag/release:
    - `canary` for `main`
    - `canary-<branch-slug>` for non-main branches

Behavior:

- Canary installers should be installable directly from release assets.
- Canary should not override stable releases.

Acceptance criteria:

- A commit to any branch yields installable canary binaries/installers for all configured targets.

## PR4: In-CLI Self-Update (Done)

Scope:

- Add a `self` command group:
  - `bt self update`
  - `bt self update --check`
  - optional `--channel stable|canary`
- Integrate updater logic (installer/updater-compatible path).
- If install source is package-manager-managed (Homebrew, etc.), print a clear message to use that package manager instead of self-update.

Acceptance criteria:

- Installer-based `bt` can update itself to latest stable.
- Canary opt-in works when explicitly selected.

## PR5: Docs and Operational Hardening

Scope:

- Update README/docs with install commands:
  - Unix: `curl ... | sh`
  - Windows: `irm ... | iex`
- Add PATH behavior note (new shell required after first install).
- Add troubleshooting:
  - proxy/firewall
  - GitHub API/rate limits
  - signature/checksum verification flow if required.
- Add smoke tests in CI:
  - install from generated installer
  - run `bt --version`
  - self-update dry-run/check path.

Status in this branch:

- Added README install/update/troubleshooting docs and forward-looking roadmap updates.
- Added release smoke install jobs for stable and canary workflows (Linux/macOS/Windows).
- Added checksum verification guidance for manual archive installs.
- Remaining hardening is mostly expansion work (for example extra architecture coverage and signature verification flow docs).

Acceptance criteria:

- New user can install and run `bt` without Cargo.
- Docs clearly differentiate stable vs canary.

## Implementation Checklist

1. Merge PR1 + PR2.
2. Push test tag (for example `v0.1.1`) from mainline commit.
3. Verify release assets include:
   - platform archives
   - `bt-installer.sh`
   - `bt-installer.ps1`
   - updater artifacts
4. Validate installation on:
   - macOS (arm64/x64)
   - Linux (x64)
   - Windows (x64 PowerShell)
5. Verify PR3 canary channel on both `main` and a feature branch.
6. Implement PR4 self-update command. (Done)
7. Complete PR5 docs/tests. (Mostly done)

## Risks and Mitigations

- Private git dependency fetch failures in release CI:
  - Mitigation: keep git auth setup in all build jobs.
- Installer PATH confusion:
  - Mitigation: explicit post-install message and docs.
- Canary instability:
  - Mitigation: separate prerelease channel and explicit opt-in.

## Rollout

1. Launch stable channel first (PR1 + PR2).
2. Run one or two stable cycles to validate installer reliability.
3. Validate branch canary behavior and keep `main` alias (`canary`) as the default fast channel.
4. Ship `bt self update`.
