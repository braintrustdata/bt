# C# SDK Install

Reference guide for installing the Braintrust C# SDK.

- SDK repo: https://github.com/braintrustdata/braintrust-sdk-dotnet
- NuGet: https://www.nuget.org/packages/Braintrust.Sdk
- Requires .NET 8.0+

## Find the latest version of the SDK

Look up the latest version from NuGet **without installing anything**. Do not guess -- use a read-only query so the environment stays unchanged until you pin the exact version.

```bash
dotnet package search Braintrust.Sdk --exact-match
```

Then install that exact version:

### .NET CLI

```bash
dotnet add package Braintrust.Sdk --version <VERSION>
```

### Or add to .csproj

```xml
<ItemGroup>
  <PackageReference Include="Braintrust.Sdk" Version="<VERSION>" />
</ItemGroup>
```

## Initialize the SDK

```csharp
using Braintrust.Sdk;
using Braintrust.Sdk.Config;

var apiKey = Environment.GetEnvironmentVariable("BRAINTRUST_API_KEY");
Braintrust? braintrust = null;
System.Diagnostics.ActivitySource? activitySource = null;

if (!string.IsNullOrEmpty(apiKey))
{
    // Set the project name in code (do NOT require an env var for project name).
    var config = BraintrustConfig.Of(
        ("BRAINTRUST_API_KEY", apiKey),
        ("BRAINTRUST_DEFAULT_PROJECT_NAME", "my-project")
    );

    braintrust = Braintrust.Get(config);
    activitySource = braintrust.GetActivitySource();
}
```

`Braintrust.Get(config)` is the main entry point. The SDK requires an API key to be present, so initialize Braintrust conditionally and run the application normally when `BRAINTRUST_API_KEY` is missing. `GetActivitySource()` returns the `System.Diagnostics.ActivitySource` used to create spans.

## Install instrumentation

The C# SDK instruments LLM clients by wrapping them. Only instrument clients that are actually present in the project.

### OpenAI (`OpenAI` NuGet package)

```bash
dotnet add package OpenAI
```

Create an instrumented OpenAI client:

```csharp
using Braintrust.Sdk;
using Braintrust.Sdk.Config;
using Braintrust.Sdk.Instrumentation.OpenAI;

var btApiKey = Environment.GetEnvironmentVariable("BRAINTRUST_API_KEY");
var openAIApiKey = Environment.GetEnvironmentVariable("OPENAI_API_KEY");

if (!string.IsNullOrEmpty(btApiKey) && !string.IsNullOrEmpty(openAIApiKey))
{
    var config = BraintrustConfig.Of(
        ("BRAINTRUST_API_KEY", btApiKey),
        ("BRAINTRUST_DEFAULT_PROJECT_NAME", "my-project")
    );
    var braintrust = Braintrust.Get(config);
    var activitySource = braintrust.GetActivitySource();
    var client = BraintrustOpenAI.WrapOpenAI(activitySource, openAIApiKey);

    // Optional: create a root activity so you can generate a permalink.
    using var activity = activitySource.StartActivity("braintrust-openai-example");

    var chatClient = client.GetChatClient("gpt-5-mini");
    var response = await chatClient.CompleteChatAsync(
        new ChatMessage[]
        {
            new SystemChatMessage("You are a helpful assistant."),
            new UserChatMessage("What is the capital of France?")
        }
    );

    if (activity != null)
    {
        var projectUri = await braintrust.GetProjectUriAsync();
        var url = $"{projectUri}/logs?r={activity.TraceId}&s={activity.SpanId}";
        Console.WriteLine($"View your data in Braintrust: {url}");
    }
}
```

### Custom spans

For business logic that isn't an LLM call, create spans manually with the `ActivitySource`:

```csharp
using (var activity = activitySource.StartActivity("my-operation"))
{
    activity?.SetTag("some.attribute", "value");
    // LLM calls inside here are automatically nested under this span
}
```

## Run the application

Try to figure out how to run the application from the project structure:

- **dotnet run**: `dotnet run` or `dotnet run --project path/to/Project.csproj`
- **ASP.NET**: `dotnet run` (typically starts Kestrel)
- **Published app**: `dotnet path/to/app.dll`
- **Visual Studio / Rider**: run from IDE

If you can't determine how to run the app, ask the user.

## Generate a permalink (required)

The installer must produce a permalink to the emitted trace/logs in its final output.

In .NET, the most reliable permalink can be generated from the root Activity's TraceId/SpanId:

```csharp
if (braintrust != null && activitySource != null)
{
    using var activity = activitySource.StartActivity("braintrust-install-verify");
    if (activity != null)
    {
        // Perform a real operation that triggers LLM spans / instrumentation here.

        var projectUri = await braintrust.GetProjectUriAsync();
        var url = $"{projectUri}/logs?r={activity.TraceId}&s={activity.SpanId}";
        Console.WriteLine($"View your data in Braintrust: {url}");
    }
}
```

The final assistant response must include the printed URL.

If the SDK-generated URL is not available, construct the permalink manually using the URL format documented in `braintrust-url-formats.md` as described in the agent task (Step 5).
