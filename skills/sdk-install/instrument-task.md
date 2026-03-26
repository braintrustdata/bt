# Braintrust SDK Installation (Agent Instructions)

## Hard Rules

- **Only add Braintrust code.** Do not refactor or modify unrelated code.
- **Pin exact versions.** Never use `latest`.
- **Set the project name in code.** Do NOT configure project name via env vars.
- **App must run without Braintrust.** If `BRAINTRUST_API_KEY` is missing at runtime, do not crash.
- **Abort install if API key is not set.** (Do not modify runtime behavior.)
- **Do not guess APIs.** Use official documentation/examples only.
- **Do not add eval code** unless explicitly requested.
- **Do not add manual flush/shutdown logic.**
- **If SDK is already installed/configured, do not duplicate work.**

---

## Execution Requirements

Before writing any code:

1. Create a **checklist** from the steps below.
2. Execute each step in order.
3. Do not skip steps.

---

## Steps

{LANGUAGE_CONTEXT}

### 1. Verify API Key (Install Precondition)

Check if `BRAINTRUST_API_KEY` is exported:

```bash
if env | grep 'BRAINTRUST_API_KEY=' >/dev/null 2>&1 ; then echo "api key set" ; else echo "api key NOT set"; fi
```

If not set, **abort installation immediately**.

---

### 2. Detect Language

Determine the project language using concrete signals:

- `package.json` → TypeScript
- `requirements.txt` or `pyproject.toml` → Python
- `pom.xml` or `build.gradle` → Java
- `go.mod` → Go
- `Gemfile` → Ruby
- `.csproj` → C#

If the language is not obvious from standard build/dependency files:

- infer it from concrete repo evidence (e.g., entrypoint file extensions, build scripts, framework config)
- State the single strongest piece of evidence you used
- If still ambiguous (polyglot/monorepo), ask the user which service/app to instrument
- If the inferred language is not in the supported list, **abort the install**.

If none match, **abort installation**.

---

### 3. Install SDK (Language-Specific)

Read the install guide for the detected language from the local docs:

| Language   | Local doc                               |
| ---------- | --------------------------------------- |
| Java       | `{SDK_INSTALL_DIR}/java.md`       |
| TypeScript | `{SDK_INSTALL_DIR}/typescript.md` |
| Python     | `{SDK_INSTALL_DIR}/python.md`     |
| Go         | `{SDK_INSTALL_DIR}/go.md`         |
| Ruby       | `{SDK_INSTALL_DIR}/ruby.md`       |
| C#         | `{SDK_INSTALL_DIR}/csharp.md`     |

Requirements:

- Pin an exact SDK version (resolve via package manager).
- Modify only dependency files and a minimal application entry point (e.g., main/bootstrap).
- Do not change unrelated code.

---

### 4. Verify Installation (MANDATORY)

- Run the application.
- Confirm at least one log/trace is emitted to Braintrust.
- Confirm no runtime errors.
- Confirm the app still runs if `BRAINTRUST_API_KEY` is unset.

If you do not know how to run the app, ask the user.

---

### 5. Verify in Braintrust (CRITICAL)

Using the Braintrust MCP (preferred):

1. Query for the emitted logs/traces.
2. Generate a **permalink to the data**.
3. Print the permalink clearly.

The permalink must be included in the final output.
This confirms the full installation succeeded.

Notes:

- The agent must not "guess" the project from Braintrust UI. The project must be set in code during installation.
- If a language SDK provides a deterministic URL to the emitted trace/log (e.g. a `/logs?r=<traceId>&s=<spanId>` link), it is acceptable to print that as the permalink, but it still must point to the specific emitted data.

Minimal MCP workflow to generate a permalink (use this if the SDK does not provide a deterministic URL):

1. Resolve the project ID using the project name that was configured in code:
   - Call `resolve_object` with `object_type="project_logs"` and `project_name=<your project name>`
2. Find the newest emitted row in that project:
   - Call `sql_query` with `object_type="project_logs"`, `object_ids=[<project id>]`, and a time filter, e.g. `created > now() - interval 1 hour`, ordered by `created DESC`, `limit 1`
3. Generate a permalink to that row:
   - Call `generate_permalink` with `object_type="project_logs"`, `object_id=<project id>`, `row_id=<row id from sql_query>`

---

### 6. Final Summary

Summarize:

- What SDK version was installed
- Where code was modified
- What logs/traces were emitted
- The Braintrust permalink (required)

{WORKFLOW_CONTEXT}