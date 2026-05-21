use super::language::DetectedLanguage;

const SDK_INSTALL_DOCS_BASE: &str = "https://www.braintrust.dev/docs/instrument/trace-llm-calls";

const TEMPLATE: &str = r"# Braintrust SDK Installation (Agent Instructions)

## Hard Rules

- **Only add Braintrust code.** Do not refactor or modify unrelated code.
- **One language, one service per install run.** If the repo has more than one candidate, ask the user which one to instrument before starting. Do not instrument multiple languages or services in the same run.
- **If the language is unclear, ask the user.** Do not guess. See Step 2.
- **Install the latest Braintrust SDK.** Do not hard-pin the Braintrust SDK version unless the user asks for it -- use the package manager's normal install (which may produce an exact or a ranged version, whichever is idiomatic for that ecosystem). Build-time dependencies (e.g. Orchestrion for Go) must still be pinned to an exact version -- see the language-specific resource.
- **Set the project name in code.** Do NOT configure project name via env vars.
- **App must run without Braintrust.** If `BRAINTRUST_API_KEY` is missing at runtime, do not crash.
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

{LANGUAGE_CONTEXT}

---

{INSTALL_SDK_CONTEXT}

---

### 4. Resolve Target Org/Project

The repo contains `.bt/config.json` with `{ org, project, project_id }` set by the `bt setup` wizard. Read the **project name** from `bt status --json` (preferred) or fall back to reading `.bt/config.json` directly. Use that project name when configuring the SDK in code. Do not guess the project name from context.

---

### 5. Verify Installation (MANDATORY)

- If the SDK relies on build-time or launch-time auto-instrumentation, make sure the project's normal build/run path now uses it. A one-off verification command is not sufficient.
- Run the application.
- Confirm at least one log/trace is emitted to Braintrust.
- Confirm no runtime errors.
- Confirm the app still runs if `BRAINTRUST_API_KEY` is unset.

If you do not know how to run the app, ask the user and wait for the response before proceeding.

---

### 6. Final Summary

Summarize:

- What SDK version was installed
- Where code was modified
- What logs/traces were emitted
- The Braintrust permalink (required)

## Latest Braintrust Setup Docs

Use the canonical Braintrust docs at https://www.braintrust.dev/docs as the source of truth for SDK setup behavior. Prefer local `bt` CLI commands over direct API calls when verifying state.
";

const INSTALL_SDK_REQUIREMENTS: &str = "- Install the latest Braintrust SDK via the language's package manager. Do not hard-pin the SDK version unless the user asks. Build-time dependencies called out by the language-specific resource (e.g. Orchestrion for Go) must still be pinned to an exact version.
- Modify only dependency files, a minimal application entry point (e.g., main/bootstrap), and any existing build/run scripts or checked-in env/config that must change to keep auto-instrumentation active in normal use. Auto-instrument the app (except for Java and C# which don't support auto-instrumentation).
- Do not change unrelated code.";

const DETECT_LANGUAGE_BLOCK: &str = "### 2. Detect Language

**Instrument exactly one language/service per install run.** Do not install Braintrust for multiple languages or multiple services in the same run, even if the repo contains more than one. If more than one candidate exists, stop and ask the user which single service to instrument before doing anything else.

Determine the project language using concrete signals:

- `package.json` -> TypeScript
- `requirements.txt`, `setup.py` or `pyproject.toml` -> Python
- `pom.xml` or `build.gradle` -> Java
- `go.mod` -> Go
- `Gemfile` -> Ruby
- `.csproj` -> C#

**If exactly one of these matches at the repo root and there is no ambiguity, proceed with that language.**

In every other case, **stop and ask the user** before continuing. Do not guess, do not pick the \"most likely\" language, and do not instrument more than one.";

pub const SKILL_NAME: &str = "instrument-code";
pub const SKILL_DESCRIPTION: &str =
    "Install the Braintrust SDK in this repo and verify a trace lands in Braintrust.";
pub const SKILL_WHEN_TO_USE: &str =
    "User says \"instrument this repo\", \"set up Braintrust\", \"add traces\", or just ran `bt setup`.";

pub fn render_skill_body(languages: &[DetectedLanguage]) -> String {
    let (language_context, install_sdk_context) = match languages.len() {
        0 => {
            let rows = DetectedLanguage::all()
                .iter()
                .map(|lang| {
                    format!(
                        "| {} | `{}#{}` |",
                        lang.display(),
                        SDK_INSTALL_DOCS_BASE,
                        lang.slug()
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            let install = format!(
                "### 3. Install SDK (Language-Specific)\n\nRead the install guide for the detected language from the canonical docs:\n\n| Language | Doc URL |\n| -------- | ------- |\n{rows}\n\nRequirements:\n\n{INSTALL_SDK_REQUIREMENTS}",
            );
            (DETECT_LANGUAGE_BLOCK.to_string(), install)
        }
        1 => {
            let lang = languages[0];
            let context = format!(
                "### 2. Language\n\nThe target language has been specified: **{}**.",
                lang.display()
            );
            let install = format!(
                "### 3. Install SDK\n\nRead the install guide from the canonical docs: `{}#{}`\n\nRequirements:\n\n{INSTALL_SDK_REQUIREMENTS}",
                SDK_INSTALL_DOCS_BASE,
                lang.slug()
            );
            (context, install)
        }
        _ => {
            let list = languages
                .iter()
                .map(|l| format!("**{}**", l.display()))
                .collect::<Vec<_>>()
                .join(", ");
            let context = format!(
                "### 2. Language\n\nCandidate languages detected: {list}. Pick exactly one with the user before proceeding.",
            );
            let rows = languages
                .iter()
                .map(|l| {
                    format!(
                        "| {} | `{}#{}` |",
                        l.display(),
                        SDK_INSTALL_DOCS_BASE,
                        l.slug()
                    )
                })
                .collect::<Vec<_>>()
                .join("\n");
            let install = format!(
                "### 3. Install SDK\n\nRead the install guide for the chosen language from the canonical docs:\n\n| Language | Doc URL |\n| -------- | ------- |\n{rows}\n\nRequirements:\n\n{INSTALL_SDK_REQUIREMENTS}",
            );
            (context, install)
        }
    };

    TEMPLATE
        .replace("{LANGUAGE_CONTEXT}", &language_context)
        .replace("{INSTALL_SDK_CONTEXT}", &install_sdk_context)
}

pub fn render_skill_markdown(languages: &[DetectedLanguage]) -> String {
    let frontmatter = format!(
        "---\nname: {SKILL_NAME}\ndescription: {SKILL_DESCRIPTION}\nwhen_to_use: {SKILL_WHEN_TO_USE}\n---\n\n"
    );
    frontmatter + &render_skill_body(languages)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn includes_yaml_frontmatter() {
        let body = render_skill_markdown(&[]);
        assert!(body.starts_with("---\nname: instrument-code\n"));
    }

    #[test]
    fn includes_language_specific_url_when_one_language() {
        let body = render_skill_markdown(&[DetectedLanguage::Python]);
        assert!(body.contains("#python"));
        assert!(body.contains("Python"));
    }

    #[test]
    fn lists_multiple_languages() {
        let body = render_skill_markdown(&[DetectedLanguage::Go, DetectedLanguage::Typescript]);
        assert!(body.contains("**Go**"));
        assert!(body.contains("**TypeScript**"));
    }
}
