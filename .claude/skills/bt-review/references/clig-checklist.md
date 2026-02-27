# CLI Guidelines Checklist (clig.dev)

Audit checklist organized by category. Each item is a specific, verifiable guideline.

## EXIT CODES & STREAMS

- [ ] Return 0 on success, non-zero on failure
- [ ] Map distinct non-zero codes to important failure modes
- [ ] Primary output goes to stdout
- [ ] Errors, logs, status messages go to stderr

## HELP

- [ ] `-h` and `--help` show help
- [ ] Subcommands have their own `--help`/`-h`
- [ ] Running command with no required args shows concise help (description, examples, flag summary, pointer to `--help`)
- [ ] `myapp help` and `myapp help <subcommand>` work (for git-like tools)
- [ ] Top-level help includes support path (website or GitHub link)
- [ ] Help text links to web documentation where applicable
- [ ] Help leads with examples showing common complex uses
- [ ] Most common flags and commands listed first
- [ ] Suggest corrections when users make typos in commands/flags
- [ ] If program expects piped stdin but gets interactive terminal, show help instead of hanging

## OUTPUT

- [ ] Detect TTY to distinguish human vs machine output
- [ ] Support `--json` flag for JSON output
- [ ] Support `--plain` flag for machine-readable plain text (if applicable)
- [ ] Display brief output on success confirming state changes
- [ ] Support `-q`/`--quiet` to suppress non-essential output
- [ ] Make current state easily visible (like `git status`)
- [ ] Suggest next commands in workflows
- [ ] Be explicit about boundary-crossing actions (file I/O, network calls)
- [ ] Use color intentionally, not saturated
- [ ] Disable color when stdout is not interactive TTY
- [ ] Disable color when `NO_COLOR` env var is set
- [ ] Disable color when `TERM=dumb`
- [ ] Support `--no-color` flag
- [ ] Disable animations when stdout is not interactive
- [ ] Don't output debug info by default
- [ ] Don't treat stderr like a log file (no log level labels by default)
- [ ] Use pager for large text output when stdout is interactive

## ERRORS

- [ ] Catch expected errors and rewrite for humans (conversational, suggest fixes)
- [ ] High signal-to-noise ratio (group similar errors)
- [ ] Important information at end of output where users look
- [ ] Unexpected errors: provide debug info + bug report instructions
- [ ] Streamline bug reporting (URLs with pre-populated info if possible)

## ARGUMENTS & FLAGS

- [ ] Prefer flags over positional arguments
- [ ] All flags have long-form versions (`--help` not just `-h`)
- [ ] Single-letter flags reserved for commonly used options only
- [ ] Use standard flag names where conventions exist:
  - `-a, --all` | `-d, --debug` | `-f, --force` | `--json`
  - `-h, --help` | `-n, --dry-run` | `--no-input`
  - `-o, --output` | `-p, --port` | `-q, --quiet`
  - `-u, --user` | `--version` | `-v` (version, not verbose — or skip)
- [ ] Defaults are correct for most users
- [ ] Prompt for missing input interactively
- [ ] Never _require_ prompts — always allow flags/args to skip them
- [ ] Skip prompts when stdin is non-interactive
- [ ] Confirm before dangerous actions (prompt or `--force`)
- [ ] Support `-` for stdin/stdout where applicable
- [ ] Arguments, flags, subcommands are order-independent where possible
- [ ] Never read secrets from flags (use files, stdin, or IPC)

## INTERACTIVITY

- [ ] Only use prompts when stdin is interactive TTY
- [ ] Support `--no-input` flag to disable prompts
- [ ] Don't echo passwords
- [ ] Ctrl-C works reliably to exit

## SUBCOMMANDS

- [ ] Consistent flag names across subcommands
- [ ] Consistent output formatting across subcommands
- [ ] Consistent naming convention (noun-verb or verb-noun)
- [ ] No ambiguous or similarly-named commands

## ROBUSTNESS

- [ ] Validate user input
- [ ] Output something within 100ms (responsive feel)
- [ ] Show progress for long operations (spinners/progress bars)
- [ ] Add timeouts with sensible defaults (no hanging forever)
- [ ] Handle Ctrl-C immediately, say something, then clean up
- [ ] Second Ctrl-C skips cleanup
- [ ] Handle uncleaned state from previous crashes

## CONFIGURATION

- [ ] Follow XDG Base Directory Specification for config files
- [ ] Parameter precedence: flags > env vars > project config > user config > system config
- [ ] Ask consent before modifying non-owned config
- [ ] Read from `.env` files where appropriate

## ENVIRONMENT VARIABLES

- [ ] Env var names: uppercase, numbers, underscores only
- [ ] Don't commandeer widely-used env var names
- [ ] Respect `NO_COLOR`, `EDITOR`, `HTTP_PROXY`, `TERM`, `PAGER`, `HOME`
- [ ] Don't read secrets from env vars (use credential files, stdin, IPC)

## NAMING

- [ ] Simple, memorable name
- [ ] Lowercase letters only (dashes if needed)
- [ ] Short enough for frequent typing
- [ ] Easy to type ergonomically

## FUTURE-PROOFING

- [ ] Keep changes additive (new flags rather than changed behavior)
- [ ] Warn before non-additive changes with migration guidance
- [ ] No catch-all subcommands that prevent future command names
- [ ] No arbitrary subcommand abbreviations
- [ ] Don't create time bombs (external dependencies that will disappear)

## DISTRIBUTION

- [ ] Distribute as single binary if possible
- [ ] Easy uninstall instructions available
