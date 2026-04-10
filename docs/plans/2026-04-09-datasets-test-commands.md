# Testing `bt datasets` commands

## Setup

**bash:**

```bash
# Build from source and put on your PATH
cargo build --release
export PATH="$PWD/target/release:$PATH"

# Verify you're using the local build
which bt        # should show .../target/release/bt
bt --version    # should show canary version

# Set these to match your Braintrust org/project/dataset
export BT_ORG="my-org"
export BT_PROJECT="my-project"
export BT_DATASET="my-dataset"
```

**fish:**

```fish
# Build from source and put on your PATH
cargo build --release
set -x PATH (pwd)/target/release $PATH

# Verify you're using the local build
which bt        # should show .../target/release/bt
bt --version    # should show canary version

# Set these to match your Braintrust org/project/dataset
set -x BT_ORG "my-org"
set -x BT_PROJECT "my-project"
set -x BT_DATASET "my-dataset"
```

## Create

```bash
# Create a dataset
bt datasets create "test-dataset" --org "$BT_ORG" --project "$BT_PROJECT"

# Create with description
bt datasets create "test-dataset-2" --org "$BT_ORG" --project "$BT_PROJECT" --description "A test dataset"

# Interactive mode (no name, should prompt)
bt datasets create --org "$BT_ORG" --project "$BT_PROJECT"

# Non-interactive without name (should error)
bt datasets create --org "$BT_ORG" --project "$BT_PROJECT" --no-input

# Create duplicate (should error)
bt datasets create "test-dataset" --org "$BT_ORG" --project "$BT_PROJECT"
```

## List

```bash
# List all datasets in a project
bt datasets list --org "$BT_ORG" --project "$BT_PROJECT"

# JSON output
bt datasets list --org "$BT_ORG" --project "$BT_PROJECT" --json

# Bare command (should default to list)
bt datasets --org "$BT_ORG" --project "$BT_PROJECT"

# Non-interactive without project (errors only if no default project set via `bt switch`)
bt datasets list --org "$BT_ORG" --no-input
```

## View

```bash
# View metadata + sample rows (default 10 rows)
bt datasets view "$BT_DATASET" --org "$BT_ORG" --project "$BT_PROJECT"

# Limit sample rows
bt datasets view "$BT_DATASET" --org "$BT_ORG" --project "$BT_PROJECT" --limit 3

# No sample rows
bt datasets view "$BT_DATASET" --org "$BT_ORG" --project "$BT_PROJECT" --limit 0

# JSON output (metadata only)
bt datasets view "$BT_DATASET" --org "$BT_ORG" --project "$BT_PROJECT" --json

# Open in browser
bt datasets view "$BT_DATASET" --org "$BT_ORG" --project "$BT_PROJECT" --web

# Using --name flag instead of positional
bt datasets view --name "$BT_DATASET" --org "$BT_ORG" --project "$BT_PROJECT"

# Interactive mode (no name, should fuzzy select)
bt datasets view --org "$BT_ORG" --project "$BT_PROJECT"

# Non-interactive without name (should error)
bt datasets view --org "$BT_ORG" --project "$BT_PROJECT" --no-input

# Dataset that doesn't exist (should error)
bt datasets view "nonexistent-dataset" --org "$BT_ORG" --project "$BT_PROJECT"
```

## Delete

```bash
# Interactive delete (will prompt for confirmation)
bt datasets delete "$BT_DATASET" --org "$BT_ORG" --project "$BT_PROJECT"

# Force delete (skip confirmation) -- careful!
# bt datasets delete "$BT_DATASET" --org "$BT_ORG" --project "$BT_PROJECT" --force

# Interactive mode (no name, should fuzzy select)
bt datasets delete --org "$BT_ORG" --project "$BT_PROJECT"

# Non-interactive without name (should error)
bt datasets delete --org "$BT_ORG" --project "$BT_PROJECT" --no-input

# Force without name (should error)
bt datasets delete --org "$BT_ORG" --project "$BT_PROJECT" --force
```

## Help

```bash
# Top-level help (should show datasets)
bt --help

# Datasets help
bt datasets --help

# Subcommand help
bt datasets view --help
bt datasets delete --help
```
