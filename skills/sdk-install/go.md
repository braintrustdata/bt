# Go SDK Install

Reference guide for installing the Braintrust Go SDK.

- SDK repo: https://github.com/braintrustdata/braintrust-sdk-go
- pkg.go.dev: https://pkg.go.dev/github.com/braintrustdata/braintrust-sdk-go
- Requires Go 1.22+

## Install the SDK

```bash
go get github.com/braintrustdata/braintrust-sdk-go
```

## Initialize the SDK

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

## Install instrumentation

The Go SDK uses [Orchestrion](https://github.com/DataDog/orchestrion) to automatically inject tracing at compile time -- no wrapper code needed in the application.

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

Orchestrion supports these providers (import the corresponding `trace/contrib/` package in `orchestrion.tool.go`):

| Provider               | Import path                                                                                   |
| ---------------------- | --------------------------------------------------------------------------------------------- |
| OpenAI (`openai-go`)   | `github.com/braintrustdata/braintrust-sdk-go/trace/contrib/openai`                            |
| Anthropic              | `github.com/braintrustdata/braintrust-sdk-go/trace/contrib/anthropic`                         |
| Google GenAI / Gemini  | `github.com/braintrustdata/braintrust-sdk-go/trace/contrib/genai`                             |
| LangChainGo            | `github.com/braintrustdata/braintrust-sdk-go/trace/contrib/langchaingo`                       |
| sashabaranov/go-openai | `github.com/braintrustdata/braintrust-sdk-go/trace/contrib/github.com/sashabaranov/go-openai` |
| All of the above       | `github.com/braintrustdata/braintrust-sdk-go/trace/contrib/all`                               |

## Run the application

Try to figure out how to run the application from the project structure:

- **go run**: `go run .` or `go run ./cmd/myapp`
- **Orchestrion**: `orchestrion go run .`
- **Makefile**: check for `run`, `serve`, or similar targets
- **Docker**: check for a `Dockerfile`

If you can't determine how to run the app, ask the user.

## Generate a permalink (required)

Follow the permalink generation steps in the agent task (Step 5). Use the value passed to `braintrust.WithProject(...)` as the project name.
