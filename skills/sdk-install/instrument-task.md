# Braintrust SDK Installation (Agent Instructions)

## Hard Rules

{RUN_MODE_CONTEXT}

- **Only add Braintrust code.** Do not refactor or modify unrelated code.
- **One language, one service per install run.** If the repo has more than one candidate, ask the user which one to instrument before starting. Do not instrument multiple languages or services in the same run.
- **If the language is unclear, ask the user.** Do not guess. See Step 2.
- **Install the latest Braintrust SDK.** Do not hard-pin the Braintrust SDK version unless the user asks for it -- use the package manager's normal install (which may produce an exact or a ranged version, whichever is idiomatic for that ecosystem). Build-time dependencies (e.g. Orchestrion for Go) must still be pinned to an exact version -- see the language-specific resource.
- **Set the project name in code.** Do NOT configure project name via env vars.
- **App must run without Braintrust.** If `BRAINTRUST_API_KEY` is missing at runtime, do not crash.
- **Abort install if API key is not set.** (Do not modify runtime behavior.)
- **Do not guess APIs.** Use official documentation/examples only.
- **Do not add eval code** unless explicitly requested.
- **Do not add manual flush/shutdown logic** unless the app is a short-lived script, serverless function, Lambda, or CLI that exits immediately after LLM calls -- in which case a single `flush()` (or language equivalent) right before exit is correct, since otherwise traces get dropped. Do not add flush/shutdown for long-running processes (servers, daemons, workers).
- **If SDK is already installed/configured, do not duplicate work.**
- **Do not create setup-only files or directories in the repo.** Do not write `.bt/setup/`, `.bt/skills/docs/`, agent skill directories, or setup task files unless explicitly asked by the user.

---

## Execution Requirements

Before writing any code:

1. Create a **checklist** from the steps below.
2. Execute each step in order.
3. Do not skip steps.

---

## Steps

### 1. Verify API Key (Install Precondition)

Check if `BRAINTRUST_API_KEY` is exported:

```bash
if env | grep 'BRAINTRUST_API_KEY=' >/dev/null 2>&1 ; then echo "api key set" ; else echo "api key NOT set"; fi
```

If not set, **abort installation immediately**.

---

{LANGUAGE_CONTEXT}

---

{INSTALL_SDK_CONTEXT}

---

### 4. Verify Installation (MANDATORY)

- If the SDK relies on build-time or launch-time auto-instrumentation, make sure the project's normal build/run path now uses it. A one-off verification command is not sufficient.
- Run the application.
- Confirm at least one log/trace is emitted to Braintrust.
- Confirm no runtime errors.
- Confirm the app still runs if `BRAINTRUST_API_KEY` is unset.

If you do not know how to run the app, ask the user and wait for the response before proceeding.

---

### 5. Verify in Braintrust (CRITICAL)

The permalink must be included in the final output. This confirms the full installation succeeded.

The project name is the project field of `bt status --json`. The project must be set in code during installation — do not guess the project name from context.

**How to obtain the permalink:**

Use the project name you configured in code during Step 3 — do not re-derive it, do not guess, and do not read it from the Braintrust UI.

Most language SDKs print a direct URL to the emitted trace after the app runs. Capture that URL and print it.

If the SDK does not print a URL, construct one manually using the URL format documented in `{SDK_INSTALL_DIR}/braintrust-url-formats.md`.

Use `bt status --json` to confirm the active org/project/API URL before reporting validation results. If you use `bt sql` or another BTQL query to verify traces/logs, include a timestamp filter (for example `created >= NOW() - INTERVAL 1 HOUR`) or a `root_span_id` filter.

---

### 6. Final Summary

Summarize:

- What SDK version was installed
- Where code was modified
- What logs/traces were emitted
- The Braintrust permalink (required)

---

### 7. Post-Success Verification and Next Steps

Tell the user exactly how to verify the setup after this run:

- Run the instrumented application path again and open the Braintrust logs link for the configured org/project. If you emitted a specific trace, include the trace permalink from Step 5; otherwise include the project logs URL.
- If eval code was added or requested, include the Braintrust experiments/evals page URL for the configured org/project.
- Reusable Braintrust coding-agent skills were not installed by default. The user can opt in later with `bt setup skills`.
- The Braintrust MCP server can be added explicitly with `bt setup mcp`. More information at https://www.braintrust.dev/docs/integrations/developer-tools/mcp
- For more information on Braintrust, visit https://www.braintrust.dev/docs

{WORKFLOW_CONTEXT}
