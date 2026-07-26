# Go SDK Install

Reference guide for installing the Braintrust Go SDK.

- SDK repo: https://github.com/braintrustdata/braintrust-sdk-go
- pkg.go.dev: https://pkg.go.dev/github.com/braintrustdata/braintrust-sdk-go
- Requires Go 1.22+

## Install the SDK

Install the latest Braintrust SDK. Do not hard-pin the SDK version unless the user asks -- `go get` without a version suffix is fine and will record whatever version `go mod tidy` resolves.

```bash
go get github.com/braintrustdata/braintrust-sdk-go
```

If you need to know what the latest version is:

```bash
go list -m -versions github.com/braintrustdata/braintrust-sdk-go
```

**Note:** Orchestrion, the build-time instrumentation tool described below, **must** be pinned to an exact version. That requirement is separate from the SDK itself.

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
	"log"

	"github.com/braintrustdata/braintrust-sdk-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/trace"
)

func main() {
	tp := trace.NewTracerProvider()
	otel.SetTracerProvider(tp)

	_, err := braintrust.New(tp, braintrust.WithProject("my-project"))
	if err != nil {
		log.Fatal(err)
	}
}
```

`braintrust.New` reads `BRAINTRUST_API_KEY` from the environment automatically.

### Requirement: persist Orchestrion into the normal build/run path

Auto-instrumentation requires the project to be built and run with Orchestrion. A one-off `orchestrion go build` during verification is **not enough** if the next developer, CI job, or deploy will go back to plain `go build` / `go run`.

**1. Resolve and pin an exact Orchestrion version:**

Orchestrion is a build-time dependency that modifies the Go toolchain, so it **must** be pinned to an exact version for reproducible builds -- this is different from the Braintrust SDK itself.

```bash
go list -m -versions github.com/DataDog/orchestrion
go install github.com/DataDog/orchestrion@vX.Y.Z
```

Do not use `@latest`. Prefer the newest version that is compatible with the project's existing `go` / `toolchain` version. If Orchestrion would require bumping the project's Go version or toolchain, ask the user before making that change.

**2. Create `orchestrion.tool.go` in the module root (the same directory as `go.mod`):**

Prefer importing only the integrations the project actually uses. Use `trace/contrib/all` only if provider detection is genuinely unclear or the project uses many supported integrations.

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

Then run `go mod tidy` so the exact Orchestrion and contrib versions are recorded in `go.mod` / `go.sum`.

**3. Persist Orchestrion into the project's actual workflow:**

Update the command the project already expects developers or CI to use:

- `Makefile` / `justfile` / shell scripts: change `go build`, `go run`, and `go test` invocations to `orchestrion go ...` where appropriate.
- `Dockerfile`: change build steps to use Orchestrion.
- Bootstrap / CI / devcontainer setup: if the repo already has a checked-in way to install required tooling, add Orchestrion there too so future users do not hit `orchestrion: command not found`.
- Repo-local env/config files: if the project already uses a checked-in mechanism for env vars, set `GOFLAGS="-toolexec=orchestrion toolexec"` there.

A shell-local `export GOFLAGS=...` in the current terminal does **not** satisfy this requirement by itself, because it will not help the next user or CI run.

If you add `orchestrion.tool.go` but do **not** modify any persisted build/run path, treat the installation as incomplete.

**4. Verify using the same persisted command:**

After wiring Orchestrion into the normal workflow, run that exact command and confirm traces are emitted. Do not verify with a custom one-off command that the project will not use later.

After this, LLM client calls are automatically traced with no application wrapper code.

### Supported providers

For the current list of supported providers and their `trace/contrib/` import paths, see https://www.braintrust.dev/docs/instrument/trace-llm-calls.

## Run the application

Prefer the project's existing build/run entrypoint, and make sure that entrypoint now goes through Orchestrion.

Try to figure out how the project is normally run from the project structure:

- **Makefile / justfile / scripts**: prefer `make run`, `just run`, or the existing repo script if present
- **go run**: if the project is normally run directly, update that path to `orchestrion go run .` or `orchestrion go run ./cmd/myapp`
- **Docker**: check for a `Dockerfile` or container build script

If you can't determine how the app is supposed to be built or run in normal use, ask the user before proceeding.

## Generate a permalink (required)

Follow the permalink generation steps in the agent task (Step 5). Use the project name you configured in code above.
