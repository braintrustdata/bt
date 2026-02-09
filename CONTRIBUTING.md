# Contributing

## Setup

Prereqs:

- `mise` (recommended for tool version management)
- `rust` toolchain (installed via `mise`)
- `pre-commit` (optional but recommended)

Clone and install tools:

```bash
mise trust
mise install
```

Install JS dependencies (for `tsc`):

```bash
pnpm install --ignore-scripts
```

Enable the `mise` shell hook (if you haven't already), then re-open your shell or `cd` back into the repo. This repo adds `./target/debug` to your `PATH` via `mise.toml`, so binaries built in debug mode are on your PATH while you're in this directory.

Install git hooks:

```bash
pre-commit install
```

If you want to run this via mise:

```bash
mise run pre-commit-install
```

## Local SDK Development

The CLI depends on `braintrust-sdk-rust`. The default dependency is pinned to a git rev in `Cargo.toml`.

To override with a local checkout:

```bash
cp .cargo/config.toml.example .cargo/config.toml
```

Then ensure the path in `.cargo/config.toml` points to your local SDK checkout (default: `../braintrust-sdk-rust`). This file is ignored by git.

Note: when the local override is enabled, Cargo will treat the SDK as a path dependency and update `Cargo.lock` accordingly. The committed lockfile should reflect the git dependency (for CI). If you need to update `Cargo.lock`, temporarily move `.cargo/config.toml` out of the way, run `cargo generate-lockfile`, then restore it.

## Running

Build:

```bash
cargo build
```

Run the CLI:

```bash
cargo run -- sql "SELECT 1"
```

Run evals (JS default runner):

```bash
bt eval path/to/foo.eval.ts
```

Run evals locally (no logs sent):

```bash
bt eval --local path/to/foo.eval.ts
```

Eval fixture tests:

```bash
mise run eval-tests
```

Note: eval fixtures use Node + pnpm (or npm) to install dependencies in `tests/evals/js/*`.

Required env vars:

- `BRAINTRUST_API_KEY`: API key used for login

Optional env vars:

- `BRAINTRUST_API_URL`: override API endpoint (default `https://api.braintrust.dev`)
- `BRAINTRUST_DEFAULT_PROJECT`: default project name

## Formatting and Linting

Pre-commit runs:

- `cargo fmt --all`
- `cargo clippy --all-targets --all-features -- -D warnings`
