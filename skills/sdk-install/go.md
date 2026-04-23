# Go SDK Install

Reference guide for installing the Braintrust Go SDK.

- SDK repo: https://github.com/braintrustdata/braintrust-sdk-go
- pkg.go.dev: https://pkg.go.dev/github.com/braintrustdata/braintrust-sdk-go
- Requires Go 1.22+

## Install the SDK

```bash
go get github.com/braintrustdata/braintrust-sdk-go
```

## Instrument the application

**You must read https://www.braintrust.dev/docs/instrument/trace-llm-calls before instrumenting anything.** That page is the source of truth for supported providers and setup, and may have changed since this guide was written.

### Prefer automatic instrumentation (Orchestrion)

**Automatic instrumentation via [Orchestrion](https://github.com/DataDog/orchestrion) is the recommended path and should be used whenever possible.** It injects tracing at compile time with no wrapper code in the application, so LLM client calls are traced automatically across your codebase and third-party code.

Manual span/wrapper code should only be used as a **last resort** -- e.g. for bespoke business-logic spans, or when a provider isn't yet supported by the Orchestrion contrib packages. Don't reach for manual tracing before confirming Orchestrion can do the job.

### Quick start

Every Go project needs OpenTelemetry setup and a Braintrust client.

```go
package main

import (
	"context"
	"log"

	"github.com/braintrustdata/braintrust-sdk-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	ctx := context.Background()

	tp := trace.NewTracerProvider()
	defer tp.Shutdown(ctx)
	otel.SetTracerProvider(tp)

	_, err := braintrust.New(tp, braintrust.WithProject("my-project"))
	if err != nil {
		log.Fatal(err)
	}
}
```

`braintrust.New` reads `BRAINTRUST_API_KEY` from the environment automatically.

### Requirement: build with Orchestrion

Auto-instrumentation requires building the project with Orchestrion -- without this step, nothing will be traced.

**1. Install orchestrion:**

```bash
go install github.com/DataDog/orchestrion@v1.6.1
```

**2. Create `orchestrion.tool.go` in the project root:**

To instrument all supported providers:

```go
//go:build tools

package main

import (
	_ "github.com/DataDog/orchestrion"
	_ "github.com/braintrustdata/braintrust-sdk-go/trace/contrib/all"
)
```

Or import only the integrations the project actually uses:

```go
//go:build tools

package main

import (
	_ "github.com/DataDog/orchestrion"
	_ "github.com/braintrustdata/braintrust-sdk-go/trace/contrib/anthropic"                         // anthropic-sdk-go
	_ "github.com/braintrustdata/braintrust-sdk-go/trace/contrib/genai"                             // Google GenAI
	_ "github.com/braintrustdata/braintrust-sdk-go/trace/contrib/github.com/sashabaranov/go-openai" // sashabaranov/go-openai
	_ "github.com/braintrustdata/braintrust-sdk-go/trace/contrib/langchaingo"                       // LangChainGo
	_ "github.com/braintrustdata/braintrust-sdk-go/trace/contrib/openai"                            // openai-go
)
```

**3. Build with orchestrion:**

```bash
orchestrion go build ./...
```

Or set GOFLAGS to use orchestrion automatically:

```bash
export GOFLAGS="-toolexec='orchestrion toolexec'"
go build ./...
```

After this, LLM client calls are automatically traced with no code changes.

### Supported providers

For the current list of supported providers and their `trace/contrib/` import paths, see https://www.braintrust.dev/docs/instrument/trace-llm-calls.

## Run the application

Try to figure out how to run the application from the project structure:

- **go run**: `go run .` or `go run ./cmd/myapp`
- **Orchestrion**: `orchestrion go run .`
- **Makefile**: check for `run`, `serve`, or similar targets
- **Docker**: check for a `Dockerfile`

If you can't determine how to run the app, ask the user.

## Generate a permalink (required)

Follow the permalink generation steps in the agent task (Step 5). Use the value passed to `braintrust.WithProject(...)` as the project name.
