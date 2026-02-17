#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Smoke test harness for Braintrust agent skills.

This script:
1) creates a fresh demo repo,
2) installs a Braintrust skill via `bt setup skills`,
3) writes a concrete task prompt for an agent,
4) optionally runs an agent command, and
5) verifies the repo now includes both tracing and eval additions.

Usage:
  scripts/skill-smoke-test.sh [options]

Options:
  --agent <name>         Agent to install (claude|codex|cursor|opencode). Default: codex
  --bt-bin <path>        bt binary path. Default: bt
  --demo-dir <path>      Demo repo directory. Default: create temp dir
  --agent-cmd <command>  Command to run the agent after scaffold (optional)
  --verify-only          Skip scaffold/agent steps and only run verification
  --eval-list-check      Also run `bt eval --list --no-send-logs` on discovered eval files
  --keep                 Keep temp directory (default: kept and printed)
  -h, --help             Show this help

Examples:
  scripts/skill-smoke-test.sh --agent codex
  scripts/skill-smoke-test.sh --agent codex --agent-cmd 'codex run --prompt-file AGENT_TASK.md'
  scripts/skill-smoke-test.sh --demo-dir /tmp/bt-skill-demo --verify-only
EOF
}

AGENT="codex"
BT_BIN="bt"
DEMO_DIR=""
AGENT_CMD=""
VERIFY_ONLY=0
EVAL_LIST_CHECK=0
KEEP=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --agent)
      AGENT="${2:-}"
      shift 2
      ;;
    --bt-bin)
      BT_BIN="${2:-}"
      shift 2
      ;;
    --demo-dir)
      DEMO_DIR="${2:-}"
      shift 2
      ;;
    --agent-cmd)
      AGENT_CMD="${2:-}"
      shift 2
      ;;
    --verify-only)
      VERIFY_ONLY=1
      shift
      ;;
    --eval-list-check)
      EVAL_LIST_CHECK=1
      shift
      ;;
    --keep)
      KEEP=1
      shift
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

case "$AGENT" in
  claude|codex|cursor|opencode) ;;
  *)
    echo "Unsupported --agent value: $AGENT" >&2
    exit 2
    ;;
esac

if [[ -z "$DEMO_DIR" ]]; then
  DEMO_DIR="$(mktemp -d /tmp/bt-skill-smoke-XXXXXX)"
  CREATED_TEMP_DIR=1
else
  CREATED_TEMP_DIR=0
fi

if [[ "$CREATED_TEMP_DIR" -eq 1 && "$KEEP" -eq 0 ]]; then
  trap 'rm -rf "$DEMO_DIR"' EXIT
fi

scaffold_demo() {
  rm -rf "$DEMO_DIR"
  mkdir -p "$DEMO_DIR"
  cd "$DEMO_DIR"

  git init -q
  mkdir -p app tests

  cat > app/main.py <<'PY'
def answer_question(question: str) -> str:
    """Return a deterministic response for a question."""
    q = question.strip()
    if not q:
        return "Please provide a question."
    return f"You asked: {q}"
PY

  cat > tests/test_main.py <<'PY'
from app.main import answer_question


def test_answer_question():
    assert answer_question("hello") == "You asked: hello"
    assert answer_question("  hi  ") == "You asked: hi"
PY

  cat > README.md <<'MD'
# Demo App

A tiny deterministic app used to smoke-test whether Braintrust coding-agent skills
can guide an agent to add:

1. tracing/instrumentation in app code, and
2. at least one eval definition file.
MD

  "$BT_BIN" setup skills --local --agent "$AGENT" --yes --no-fetch-docs >/dev/null

  cat > AGENT_TASK.md <<'MD'
You are editing a tiny Python demo app.

Goal:
1) Add Braintrust tracing/instrumentation to the application code in `app/main.py`.
2) Add at least one runnable Braintrust eval file for this app.

Requirements:
- Keep existing app behavior and tests valid.
- Make focused edits only.
- Prefer straightforward Braintrust patterns.

Deliverables:
- Updated app code with Braintrust instrumentation.
- One eval file (`*.eval.py` or `eval_*.py`).
MD

  git add .
  git -c user.name='bt-skill-smoke' -c user.email='bt-skill-smoke@example.com' \
    commit -q -m "baseline demo with skill installed"
}

find_skill_path() {
  case "$AGENT" in
    claude)
      echo ".claude/skills/braintrust/SKILL.md"
      ;;
    codex|opencode)
      echo ".agents/skills/braintrust/SKILL.md"
      ;;
    cursor)
      echo ".cursor/rules/braintrust.mdc"
      ;;
  esac
}

verify_demo() {
  cd "$DEMO_DIR"
  local skill_path
  skill_path="$(find_skill_path)"

  local failures=0

  if [[ ! -f "$skill_path" ]]; then
    echo "FAIL: expected installed skill/rule missing: $skill_path"
    failures=$((failures + 1))
  else
    echo "PASS: skill/rule exists: $skill_path"
  fi

  if git diff --quiet HEAD --; then
    echo "FAIL: no changes were made after baseline commit"
    failures=$((failures + 1))
  fi

  local changed_files=()
  while IFS= read -r line; do
    [[ -n "$line" ]] && changed_files+=("$line")
  done < <(git diff --name-only HEAD --)
  while IFS= read -r line; do
    [[ -n "$line" ]] && changed_files+=("$line")
  done < <(git ls-files --others --exclude-standard)

  local changed_user_files=()
  if [[ ${#changed_files[@]} -gt 0 ]]; then
    while IFS= read -r line; do
      [[ -n "$line" ]] && changed_user_files+=("$line")
    done < <(
      printf '%s\n' "${changed_files[@]}" \
        | rg -v '^(\.claude/|\.agents/|\.cursor/|skills/docs/|AGENT_TASK\.md$)' || true
    )
  fi

  if [[ ${#changed_user_files[@]} -eq 0 ]]; then
    echo "FAIL: only skill/config files changed; no app/eval changes detected"
    failures=$((failures + 1))
  else
    echo "PASS: changed user files:"
    printf '  - %s\n' "${changed_user_files[@]}"
  fi

  local trace_pattern='(from[[:space:]]+braintrust|import[[:space:]]+braintrust|braintrust\.(init|trace|start_span|wrap_)|wrap_openai|wrap_anthropic)'
  local non_eval_changed_files=()
  if [[ ${#changed_user_files[@]} -gt 0 ]]; then
    while IFS= read -r line; do
      [[ -n "$line" ]] && non_eval_changed_files+=("$line")
    done < <(
      printf '%s\n' "${changed_user_files[@]}" \
        | rg -v '(^|/)(eval_.*\.py|.*\.eval\.(py|ts|js|mjs|cjs))$' || true
    )
  fi

  if [[ ${#non_eval_changed_files[@]} -eq 0 ]] || ! rg -n -S "$trace_pattern" "${non_eval_changed_files[@]}" >/dev/null 2>&1; then
    echo "FAIL: no tracing/instrumentation evidence found in non-eval changed files"
    failures=$((failures + 1))
  else
    echo "PASS: tracing/instrumentation evidence found"
  fi

  local eval_files=()
  if [[ ${#changed_user_files[@]} -gt 0 ]]; then
    while IFS= read -r line; do
      [[ -n "$line" ]] && eval_files+=("$line")
    done < <(
      printf '%s\n' "${changed_user_files[@]}" \
        | rg '(^|/)(eval_.*\.py|.*\.eval\.(py|ts|js|mjs|cjs))$' || true
    )
  fi

  if [[ ${#eval_files[@]} -eq 0 ]]; then
    echo "FAIL: no eval file added/changed"
    failures=$((failures + 1))
  else
    echo "PASS: eval file(s) present:"
    printf '  - %s\n' "${eval_files[@]}"
  fi

  if [[ "$EVAL_LIST_CHECK" -eq 1 && ${#eval_files[@]} -gt 0 ]]; then
    if "$BT_BIN" eval --no-send-logs --list "${eval_files[@]}" >/dev/null 2>&1; then
      echo "PASS: bt eval --list succeeded"
    else
      echo "WARN: bt eval --list failed (not counted as failure)"
    fi
  fi

  if [[ "$failures" -gt 0 ]]; then
    echo "RESULT: FAIL ($failures check(s) failed)"
    return 1
  fi

  echo "RESULT: PASS"
  return 0
}

if [[ "$VERIFY_ONLY" -eq 0 ]]; then
  scaffold_demo
  echo "Demo repo: $DEMO_DIR"
  echo "Agent task: $DEMO_DIR/AGENT_TASK.md"

  if [[ -n "$AGENT_CMD" ]]; then
    echo "Running agent command..."
    (
      cd "$DEMO_DIR"
      export BT_SKILL_SMOKE_TASK_FILE="$DEMO_DIR/AGENT_TASK.md"
      bash -lc "$AGENT_CMD"
    )
  else
    echo "No --agent-cmd provided. Run your agent manually in: $DEMO_DIR"
    echo "Then re-run with:"
    echo "  scripts/skill-smoke-test.sh --demo-dir \"$DEMO_DIR\" --agent \"$AGENT\" --verify-only"
  fi
fi

verify_demo
