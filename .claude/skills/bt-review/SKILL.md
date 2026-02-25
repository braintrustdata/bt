---
name: bt-review
description: >
  This skill should be used to review and audit the bt CLI for adherence to CLI
  best practices from clig.dev AND internal codebase patterns. It checks source
  code for help text, flags, error handling, output formatting, subcommand
  structure, pattern consistency, and more. Triggers on "review my code",
  "audit the CLI", "check CLI best practices", or /bt-review.
---

# CLI Best Practices Review

Audit the `bt` CLI codebase against two reference documents:

1. **clig.dev guidelines** — industry CLI best practices
2. **bt codebase patterns** — established internal conventions for consistency

## When to Use

- When a user asks to review, audit, or check the CLI
- When triggered via `/bt-review`
- After implementing new commands or subcommands
- Before releases to ensure CLI quality

## Review Process

### 1. Scope the Review

Determine what to review:

- **Full audit**: All commands and subcommands
- **Targeted review**: Specific command or area (e.g., just `prompts`, just error handling)
- **Diff review**: Only changed files on the current branch vs main

To scope a diff review, run `git diff main --name-only -- '*.rs'` and focus on changed files.

### 2. Load Reference Documents

Read both reference files:

- `references/clig-checklist.md` — industry CLI guidelines organized by category
- `references/bt-patterns.md` — established codebase patterns to check for consistency

### 3. Analyze Source Code

#### clig.dev Compliance

For each category in the checklist, examine relevant source files:

- **Args & flags**: Read `src/args.rs` and `src/main.rs` — check clap derive attributes, flag naming, long/short forms
- **Help text**: Check all `#[command]` and `#[arg]` attributes for descriptions, examples, and help templates
- **Error handling**: Grep for `anyhow::`, `.context(`, `eprintln!`, and error types — verify human-readable messages
- **Output**: Check stdout vs stderr usage, TTY detection, color handling, JSON output support
- **Subcommands**: Review `src/*/mod.rs` files for consistency in naming and structure
- **Interactivity**: Check `dialoguer` usage for TTY guards and `--no-input` support
- **Robustness**: Look for timeout handling, progress indicators (`indicatif`), signal handling
- **Config**: Check env var handling (`dotenvy`, `clap` env features), XDG compliance

#### Pattern Consistency

Compare new or changed code against `references/bt-patterns.md`. Check:

- **Module structure**: Does it follow `mod.rs` / `api.rs` / `list.rs` / `view.rs` / `delete.rs` layout?
- **run() dispatcher**: Does `mod.rs` have the standard `Args → Optional<Commands> → match` with `None => List`?
- **api.rs conventions**: `ListResponse { objects }` wrapper, `get_by_*` returns `Option<T>`, URL-encoded params
- **Interactive fallback**: `match identifier { Some → fetch, None → TTY check → fuzzy_select or bail }`
- **Delete confirmation**: `Confirm::new().default(false)`, only when stdin is terminal, `return Ok(())` on decline
- **Success/error status**: `print_command_status(CommandStatus::Success/Error, "Past tense message")`
- **List output**: JSON early return → summary line → `styled_table` → `print_with_pager`
- **Spinner usage**: `with_spinner("Present participle...", future)`, stderr-only, 300ms delay
- **Positional + flag dual args**: positional precedence over flag, both optional, `.identifier()` accessor method
- **Project resolution**: `base.project → interactive select → bail with env var hint`
- **Color/styling**: bold for names, dim for secondary, green/red for status, cyan for template vars
- **Import order**: std → external crates → `crate::` → `super::`
- **Error messages**: `bail!("thing required. Use: bt <exact command syntax>")`

### 4. Report Findings

Produce a structured report document to `bt-review.md`:

```
# CLI Review: bt

## Summary
[1-2 sentence overall assessment]

## clig.dev Compliance

### [Category Name] — [PASS / NEEDS WORK / NOT APPLICABLE]

| Guideline | Status | Details |
|-----------|--------|---------|
| [item]    | PASS/FAIL/PARTIAL | [specific finding with file:line references] |

## Pattern Consistency

### [Pattern Name] — [CONSISTENT / INCONSISTENT / NOT APPLICABLE]

| Expected Pattern | Status | Details |
|------------------|--------|---------|
| [pattern]        | OK/DEVIATION | [what differs and where, with file:line] |

## Priority Fixes
1. [Most impactful issue with specific fix suggestion]
2. ...

## Good Practices Already Followed
- [List what's already done well]
```

### 5. Prioritization

Rank findings by impact:

- **P0 — Broken**: Exit codes wrong, secrets in flags, no help text, crashes
- **P1 — Inconsistent**: Deviates from established patterns, missing TTY detection, inconsistent flags
- **P2 — Polish**: Missing `--json`, no pager, could suggest next commands
- **P3 — Nice-to-have**: Man pages, completion scripts, ASCII art

Pattern deviations are typically P1 unless the deviation is an intentional improvement.

## Important Notes

- This is a Rust project — check `clap` derive patterns, not manual arg parsing
- The `projects/` module is the reference implementation — new resource modules should match its patterns
- The CLI uses `anyhow` for error handling — look for `.context()` calls for user-friendly errors
- Interactive features use `dialoguer` — verify TTY checks before prompting
- Progress uses `indicatif` — check spinner/progress bar usage for long ops
- Focus findings on actionable, specific issues with file paths and line numbers
- Do not suggest changes to test files or build configuration
- When a pattern deviation is found, reference both the new code and the established pattern location
