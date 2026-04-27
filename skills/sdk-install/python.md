# Python SDK Install

Reference guide for installing the Braintrust Python SDK.

- SDK repo: https://github.com/braintrustdata/braintrust-sdk-python
- PyPI: https://pypi.org/project/braintrust/
- Requires Python 3.9+

## Install the SDK

Install the latest published version of `braintrust`. Do not hard-pin the version unless the user asks -- let the package manager record whatever it normally records.

### pip

```bash
pip install braintrust
```

### poetry

```bash
poetry add braintrust
```

### uv

```bash
uv add braintrust
```

## Instrument the application

**You must read https://www.braintrust.dev/docs/instrument/trace-llm-calls before instrumenting anything.** That page is the source of truth for supported libraries, extras, and setup, and may have changed since this guide was written.

### Prefer automatic instrumentation

**Automatic instrumentation (`auto_instrument()`) is the recommended path and should be used whenever possible.** It patches every supported library that is installed at startup with no call-site changes, so new code and third-party code are traced automatically. See the docs page above for the current list of covered libraries -- do not rely on a hard-coded list here, since coverage changes over time.

Manual `wrap_openai` / `wrap_anthropic` / `wrap_litellm` / etc. call-site wrappers should only be used as a **last resort** -- e.g. when instrumenting a library that `auto_instrument()` doesn't yet cover, or when you need per-client isolation. Don't reach for manual wrappers before confirming auto-instrumentation can't do the job.

### Quick start

```python
import braintrust

braintrust.init_logger(project="my-project")
braintrust.auto_instrument()
```

`init_logger` is the main entry point for tracing and reads `BRAINTRUST_API_KEY` from the environment automatically. `auto_instrument()` must be called **before** creating any LLM clients.

To selectively enable or disable integrations, or to see which libraries require extras (e.g. `braintrust[openai-agents]`, `braintrust[otel]`) or a companion package (e.g. `braintrust-langchain`), follow the docs page -- it lists the current extras, packages, and per-integration setup.

## Run the application

Try to figure out how to run the application from the project structure:

- **Script**: `python main.py`, `python -m mypackage`
- **Poetry**: `poetry run python main.py`
- **uv**: `uv run python main.py`
- **Django**: `python manage.py runserver`
- **FastAPI**: `uvicorn app:app --reload`
- **Flask**: `flask run`

If you can't determine how to run the app, ask the user.

## Generate a permalink (required)

Follow the permalink generation steps in the agent task (Step 5). Use the project name you configured in code above.
