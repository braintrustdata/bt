#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Benchmark `bt` command suites.

Usage:
  scripts/benchmark.sh [options]

Options:
  --bt-bin <path>       bt binary path (single-run mode, default: bt)
  --stable-bin <path>   Stable bt binary (compare mode, default: bt)
  --release-bin <path>  Release bt binary (compare mode, default: target/release/bt)
  --build-release       Build release binary before running (default)
  --no-build-release    Skip release build
  --compare             Compare stable vs release (default)
  --no-compare          Disable comparison (single-run mode)
  --project <name>      Project name (defaults to BRAINTRUST_DEFAULT_PROJECT)
  --suite <name>        Benchmark suite:
                        functions|prompts|experiments|tools|scorers|projects|sql|status|view|sync|auth|setup|docs|init|switch|eval|self|all
                        (default: functions)
  --sql-query <query>   SQL query for sql suite (default: SELECT 1)
  --view-project-id <id>  Project id for view suite (optional if --project is set)
  --view-trace-id <id>    Root span id for view trace subcommand (required for view suite)
  --view-span-id <id>     Span row id for view span subcommand (required for view suite)
  --view-limit <n>        Limit for view logs/trace (default: 1)
  --warmup <n>          Hyperfine warmup runs (default: 3)
  --min-runs <n>        Minimum runs (default: 10)
  --json                Use --json output (where supported)
  --cmd <string>        Override full command (single-run mode only)
  -h, --help            Show this help

Notes:
  - Ensure auth is configured (BRAINTRUST_API_KEY or a valid profile).
  - For stable results, use a fixed project and disable interactive prompts.

Examples:
  scripts/benchmark.sh --bt-bin target/debug/bt --project Loop
  scripts/benchmark.sh --project Loop --compare
  scripts/benchmark.sh --project Loop --compare --suite all
  scripts/benchmark.sh --bt-bin target/debug/bt --suite sql --sql-query 'SELECT 1'
  scripts/benchmark.sh --project Loop --suite view --view-trace-id <root-span-id> --view-span-id <row-id>
  scripts/benchmark.sh --cmd 'bt functions list -p Loop'
EOF
}

BT_BIN="bt"
STABLE_BIN="bt"
RELEASE_BIN="target/release/bt"
PROJECT=""
SUITE="functions"
SQL_QUERY="SELECT 1"
VIEW_PROJECT_ID=""
VIEW_TRACE_ID=""
VIEW_SPAN_ID=""
VIEW_LIMIT=1
WARMUP=3
MIN_RUNS=10
JSON=0
CMD_OVERRIDE=""
COMPARE=1
BUILD_RELEASE=1

while [[ $# -gt 0 ]]; do
  case "$1" in
    --bt-bin)
      BT_BIN="${2:-}"
      COMPARE=0
      shift 2
      ;;
    --stable-bin)
      STABLE_BIN="${2:-}"
      shift 2
      ;;
    --release-bin)
      RELEASE_BIN="${2:-}"
      shift 2
      ;;
    --build-release)
      BUILD_RELEASE=1
      shift
      ;;
    --no-build-release)
      BUILD_RELEASE=0
      shift
      ;;
    --compare)
      COMPARE=1
      shift
      ;;
    --no-compare)
      COMPARE=0
      shift
      ;;
    --project)
      PROJECT="${2:-}"
      shift 2
      ;;
    --suite)
      SUITE="${2:-}"
      shift 2
      ;;
    --sql-query)
      SQL_QUERY="${2:-}"
      shift 2
      ;;
    --view-project-id)
      VIEW_PROJECT_ID="${2:-}"
      shift 2
      ;;
    --view-trace-id)
      VIEW_TRACE_ID="${2:-}"
      shift 2
      ;;
    --view-span-id)
      VIEW_SPAN_ID="${2:-}"
      shift 2
      ;;
    --view-limit)
      VIEW_LIMIT="${2:-}"
      shift 2
      ;;
    --warmup)
      WARMUP="${2:-}"
      shift 2
      ;;
    --min-runs)
      MIN_RUNS="${2:-}"
      shift 2
      ;;
    --json)
      JSON=1
      shift
      ;;
    --cmd)
      CMD_OVERRIDE="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown option: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

if [[ -z "$PROJECT" ]]; then
  PROJECT="${BRAINTRUST_DEFAULT_PROJECT:-}"
fi

if [[ -n "$CMD_OVERRIDE" && "$COMPARE" -eq 1 ]]; then
  echo "error: --cmd is only supported in single-run mode. Use --no-compare." >&2
  exit 2
fi

quote_cmd() {
  local out=""
  for arg in "$@"; do
    out+=" $(printf '%q' "$arg")"
  done
  echo "${out:1}"
}

require_project() {
  local suite="$1"
  if [[ -z "$PROJECT" ]]; then
    echo "error: --project or BRAINTRUST_DEFAULT_PROJECT is required for suite '$suite' (or use --cmd)." >&2
    exit 2
  fi
}

require_view_ids() {
  if [[ -z "$VIEW_TRACE_ID" ]]; then
    echo "error: --view-trace-id is required for suite 'view'." >&2
    exit 2
  fi
  if [[ -z "$VIEW_SPAN_ID" ]]; then
    echo "error: --view-span-id is required for suite 'view'." >&2
    exit 2
  fi
  if [[ -z "$VIEW_PROJECT_ID" && -z "$PROJECT" ]]; then
    echo "error: --view-project-id or --project is required for suite 'view'." >&2
    exit 2
  fi
}

build_commands() {
  local bin="$1"
  local -n out="$2"
  local suite="${3:-$SUITE}"
  out=()

  if [[ -n "$CMD_OVERRIDE" ]]; then
    out+=("$CMD_OVERRIDE")
    return
  fi

  case "$suite" in
    functions)
      require_project "$suite"
      cmd=("$bin" functions list "-p" "$PROJECT")
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    prompts)
      require_project "$suite"
      cmd=("$bin" prompts list "-p" "$PROJECT")
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    experiments)
      require_project "$suite"
      cmd=("$bin" experiments list "-p" "$PROJECT")
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    sql)
      cmd=("$bin" sql --non-interactive "$SQL_QUERY")
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    tools)
      require_project "$suite"
      cmd=("$bin" tools list "-p" "$PROJECT")
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    scorers)
      require_project "$suite"
      cmd=("$bin" scorers list "-p" "$PROJECT")
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    projects)
      cmd=("$bin" projects list)
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    status)
      cmd=("$bin" status)
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    view)
      require_view_ids
      cmd=("$bin" view logs --non-interactive --limit "$VIEW_LIMIT")
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      if [[ -n "$VIEW_PROJECT_ID" ]]; then
        cmd+=("--project-id" "$VIEW_PROJECT_ID")
      else
        cmd+=("-p" "$PROJECT")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")

      cmd=("$bin" view trace --non-interactive --limit "$VIEW_LIMIT" --trace-id "$VIEW_TRACE_ID")
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      if [[ -n "$VIEW_PROJECT_ID" ]]; then
        cmd+=("--project-id" "$VIEW_PROJECT_ID")
      else
        cmd+=("-p" "$PROJECT")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")

      cmd=("$bin" view span --non-interactive --id "$VIEW_SPAN_ID")
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      if [[ -n "$VIEW_PROJECT_ID" ]]; then
        cmd+=("--project-id" "$VIEW_PROJECT_ID")
      else
        cmd+=("-p" "$PROJECT")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    sync)
      cmd=("$bin" sync --help)
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    auth)
      cmd=("$bin" auth --help)
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    setup)
      cmd=("$bin" setup --help)
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    docs)
      cmd=("$bin" docs --help)
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    init)
      cmd=("$bin" init --help)
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    switch)
      cmd=("$bin" switch --help)
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    eval)
      cmd=("$bin" eval --help)
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    self)
      cmd=("$bin" self --help)
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    all)
      require_project "$suite"
      cmd=("$bin" functions list "-p" "$PROJECT")
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")

      cmd=("$bin" prompts list "-p" "$PROJECT")
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")

      cmd=("$bin" experiments list "-p" "$PROJECT")
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")

      cmd=("$bin" sql --non-interactive "$SQL_QUERY")
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")

      cmd=("$bin" projects list)
      if [[ "$JSON" -eq 1 ]]; then
        cmd+=("--json")
      fi
      out+=("$(quote_cmd "${cmd[@]}")")
      ;;
    *)
      echo "error: unknown --suite '$SUITE' (use functions|prompts|experiments|tools|scorers|projects|sql|status|view|sync|auth|setup|docs|init|switch|eval|self|all)" >&2
      exit 2
      ;;
  esac
}

STABLE_CMDS=()
RELEASE_CMDS=()

if [[ "$COMPARE" -eq 1 ]]; then
  if [[ "$BUILD_RELEASE" -eq 1 ]]; then
    echo "Building release binary..."
    cargo build --release
  fi
  if [[ ! -x "$RELEASE_BIN" ]]; then
    echo "error: release binary not found or not executable: $RELEASE_BIN" >&2
    exit 2
  fi
  build_commands "$STABLE_BIN" STABLE_CMDS
  build_commands "$RELEASE_BIN" RELEASE_CMDS
else
  build_commands "$BT_BIN" STABLE_CMDS
fi

if command -v hyperfine >/dev/null 2>&1; then
  if [[ "$COMPARE" -eq 1 ]]; then
    for idx in "${!STABLE_CMDS[@]}"; do
      stable_cmd="${STABLE_CMDS[$idx]}"
      release_cmd="${RELEASE_CMDS[$idx]}"
      echo "Benchmark: stable vs release"
      echo "  stable:  $stable_cmd"
      echo "  release: $release_cmd"
      hyperfine -i --warmup "$WARMUP" --min-runs "$MIN_RUNS" \
        --command-name stable "$stable_cmd" \
        --command-name release "$release_cmd"
    done
  else
    for cmd in "${STABLE_CMDS[@]}"; do
      echo "Benchmark: $cmd"
    done
    hyperfine -i --warmup "$WARMUP" --min-runs "$MIN_RUNS" "${STABLE_CMDS[@]}"
  fi
else
  echo "hyperfine not found; using time -p with $MIN_RUNS runs." >&2
  if [[ "$COMPARE" -eq 1 ]]; then
    for idx in "${!STABLE_CMDS[@]}"; do
      stable_cmd="${STABLE_CMDS[$idx]}"
      release_cmd="${RELEASE_CMDS[$idx]}"
      echo "Benchmark: stable vs release"
      echo "  stable:  $stable_cmd"
      echo "  release: $release_cmd"
      for i in $(seq 1 "$MIN_RUNS"); do
        echo "Run $i/$MIN_RUNS (stable)"
        time -p bash -lc "$stable_cmd" >/dev/null
        echo "Run $i/$MIN_RUNS (release)"
        time -p bash -lc "$release_cmd" >/dev/null
      done
    done
  else
    for cmd in "${STABLE_CMDS[@]}"; do
      echo "Benchmark: $cmd"
      for i in $(seq 1 "$MIN_RUNS"); do
        echo "Run $i/$MIN_RUNS"
        time -p bash -lc "$cmd" >/dev/null
      done
    done
  fi
fi
