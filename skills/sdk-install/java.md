# Java SDK Install

Reference guide for installing the Braintrust Java SDK.

- SDK repo: https://github.com/braintrustdata/braintrust-sdk-java
- Maven Central: https://central.sonatype.com/artifact/dev.braintrust/braintrust-sdk-java/versions
- Requires Java 17+

## Find the latest version of the SDK

Look up the latest version from Maven Central **without modifying the project**. Do not guess -- use a read-only query so dependencies stay unchanged until you write the version into the build file.

```bash
curl -s 'https://search.maven.org/solrsearch/select?q=g:dev.braintrust+AND+a:braintrust-sdk-java&rows=1&wt=json' | python3 -c "import sys,json; print(json.load(sys.stdin)['response']['docs'][0]['latestVersion'])"
```

Then add the dependency with that version (the latest published release):

### Gradle

```groovy
dependencies {
    implementation 'dev.braintrust:braintrust-sdk-java:<VERSION>'
}
```

### Maven

```xml
<dependency>
    <groupId>dev.braintrust</groupId>
    <artifactId>braintrust-sdk-java</artifactId>
    <version><VERSION></version>
</dependency>
```

### SBT

```scala
libraryDependencies += "dev.braintrust" % "braintrust-sdk-java" % "<VERSION>"
```

### Generic fallback

If the project uses a different build tool, the Maven coordinates are:

- Group: `dev.braintrust`
- Artifact: `braintrust-sdk-java`

## Initialize the SDK

```java
import dev.braintrust.Braintrust;
import dev.braintrust.config.BraintrustConfig;
import io.opentelemetry.api.OpenTelemetry;

var apiKey = System.getenv("BRAINTRUST_API_KEY");
Braintrust braintrust = null;
OpenTelemetry openTelemetry = null;

if (apiKey != null && !apiKey.isEmpty()) {
    var config = BraintrustConfig.builder()
        .apiKey(apiKey)
        .defaultProjectName("my-project")
        .build();
    braintrust = Braintrust.get(config);
    openTelemetry = braintrust.openTelemetryCreate();
}
```

`Braintrust.get()` is the main entry point. The SDK requires an API key to be present, so initialize Braintrust conditionally and run the application normally when `BRAINTRUST_API_KEY` is missing.

## Install instrumentation

The Java SDK instruments existing LLM clients by wrapping them. Find which clients the project already uses and wrap them as shown below. Only instrument frameworks that are actually present in the project.

### OpenAI (`com.openai:openai-java`)

Wrap the existing `OpenAIClient`:

```java
import dev.braintrust.instrumentation.openai.BraintrustOpenAI;

OpenAIClient openAIClient = BraintrustOpenAI.wrapOpenAI(openTelemetry, existingOpenAIClient);
```

### Anthropic (`com.anthropic:anthropic-java`)

Wrap the existing `AnthropicClient`:

```java
import dev.braintrust.instrumentation.anthropic.BraintrustAnthropic;

AnthropicClient anthropicClient = BraintrustAnthropic.wrap(openTelemetry, existingAnthropicClient);
```

### Google GenAI / Gemini (`com.google.genai:google-genai`)

Wrap the existing `Client.Builder`:

```java
import dev.braintrust.instrumentation.genai.BraintrustGenAI;

Client geminiClient = BraintrustGenAI.wrap(openTelemetry, existingClientBuilder);
```

### LangChain4j (`dev.langchain4j:langchain4j`)

Wrap an existing `OpenAiChatModel.Builder`:

```java
import dev.braintrust.instrumentation.langchain.BraintrustLangchain;

ChatModel model = BraintrustLangchain.wrap(openTelemetry, existingOpenAiChatModelBuilder);
```

For LangChain4j AI Services, wrap the `AiServices` builder directly. This instruments LLM calls, tool calls, and concurrent tool execution:

```java
import dev.braintrust.instrumentation.langchain.BraintrustLangchain;

Assistant assistant = BraintrustLangchain.wrap(
    openTelemetry,
    AiServices.builder(Assistant.class)
        .chatModel(existingChatModel)
        .tools(new MyTools()));
```

### Spring AI

For Spring Boot apps using Spring AI, only register Braintrust beans when `BRAINTRUST_API_KEY` is non-empty, and wrap the underlying LLM client only in that case. Example with Google GenAI:

```java
@Bean
public Braintrust braintrust() {
    return Braintrust.get(BraintrustConfig.fromEnvironment());
}

@Bean
public OpenTelemetry openTelemetry(Braintrust braintrust) {
    return braintrust.openTelemetryCreate();
}

@Bean
public ChatModel chatModel(OpenTelemetry openTelemetry) {
    Client genAIClient = BraintrustGenAI.wrap(openTelemetry, new Client.Builder());
    return GoogleGenAiChatModel.builder()
        .genAiClient(genAIClient)
        .defaultOptions(GoogleGenAiChatOptions.builder()
            .model("gemini-2.0-flash-lite")
            .build())
        .build();
}
```

## Run the application

Try to figure out how to run the application from the project structure:

- **Gradle**: `./gradlew run`, `./gradlew bootRun` (Spring Boot), or a custom run task
- **Maven**: `mvn exec:java`, `mvn spring-boot:run` (Spring Boot)
- **SBT**: `sbt run`
- **Plain jar**: `java -jar <jarfile>`

If you can't determine how to run the app, ask the user.

## Generate a permalink (required)

Follow the permalink generation steps in the agent task (Step 5). Use the project name you configured in code above.
