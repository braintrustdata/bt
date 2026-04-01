# Python SDK Install

Reference guide for installing the Braintrust Python SDK.

- SDK repo: https://github.com/braintrustdata/braintrust-sdk-python
- PyPI: https://pypi.org/project/braintrust/
- Requires Python 3.9+

## Find the latest version of the SDK

Look up the latest version from PyPI **without installing anything**. Do not guess -- use a read-only query so the environment stays unchanged until you pin the exact version.

```bash
pip index versions braintrust
```

Then install that exact version with the project's package manager:

### pip

```bash
pip install braintrust==<VERSION>
```

### poetry

```bash
poetry add braintrust==<VERSION>
```

### uv

```bash
uv add braintrust==<VERSION>
```

## Initialize the SDK

```python
import braintrust

braintrust.init_logger(project="my-project")
```

`init_logger` is the main entry point for tracing. It reads `BRAINTRUST_API_KEY` from the environment automatically.

## Install instrumentation

The Python SDK supports two approaches: **auto-instrumentation** (recommended) and **manual wrapping**.

### Auto-instrumentation (recommended)

`auto_instrument()` automatically patches all supported libraries that are installed. Call it once at startup, before creating any clients. This is the simplest approach.

```python
import braintrust

braintrust.init_logger(project="my-project")
braintrust.auto_instrument()
```

After calling `auto_instrument()`, any supported client created afterwards is automatically traced -- no wrapping needed:

```python
import openai

client = openai.OpenAI()

import anthropic

client = anthropic.Anthropic()
```

Supported libraries: OpenAI, Anthropic, LiteLLM, Pydantic AI, Google GenAI, Agno, Claude Agent SDK (Anthropic), DSPy.

You can selectively disable specific integrations:

```python
braintrust.auto_instrument(litellm=False, dspy=False)
```

### Manual wrapping

If you prefer explicit control, wrap individual clients instead.

#### OpenAI (`openai`)

```python
from braintrust import wrap_openai
from openai import OpenAI

client = wrap_openai(OpenAI())
```

#### Anthropic (`anthropic`)

```python
import anthropic
from braintrust import wrap_anthropic

client = wrap_anthropic(anthropic.Anthropic())
```

#### LiteLLM (`litellm`)

```python
import litellm
from braintrust import wrap_litellm

litellm = wrap_litellm(litellm)
```

### OpenAI Agents SDK (`openai-agents`)

Install with the extra and register the trace processor:

```bash
pip install "braintrust[openai-agents]"
```

```python
from agents import Agent, Runner, set_trace_processors
from braintrust import init_logger
from braintrust.wrappers.openai import BraintrustTracingProcessor

set_trace_processors([BraintrustTracingProcessor(init_logger("my-project"))])

agent = Agent(name="Assistant", instructions="You are a helpful assistant.")
result = await Runner.run(agent, "Hello!")
```

### LangChain (`langchain`)

Install the callback handler package:

```bash
pip install braintrust-langchain
```

```python
from braintrust import init_logger
from braintrust_langchain import BraintrustCallbackHandler, set_global_handler
from langchain_openai import ChatOpenAI

init_logger(project="my-project")

handler = BraintrustCallbackHandler()
set_global_handler(handler)

model = ChatOpenAI()
response = await model.ainvoke("What is the capital of France?")
```

### LlamaIndex (`llama-index`)

LlamaIndex traces via OpenTelemetry. Install with the otel extra:

```bash
pip install "braintrust[otel]" llama-index
```

Set environment variables:

```bash
export BRAINTRUST_API_KEY=your-api-key
export BRAINTRUST_PARENT=project_name:my-project
```

```python
import os

import llama_index.core

braintrust_api_url = os.environ.get("BRAINTRUST_API_URL", "https://api.braintrust.dev")
llama_index.core.set_global_handler("arize_phoenix", endpoint=f"{braintrust_api_url}/otel/v1/traces")
```

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

Follow the permalink generation steps in the agent task (Step 5). Use the `project=` argument passed to `init_logger` as the project name.
