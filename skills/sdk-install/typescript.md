# TypeScript SDK Install

Reference guide for installing the Braintrust TypeScript SDK.

- SDK repo: https://github.com/braintrustdata/braintrust-sdk-javascript
- npm: https://www.npmjs.com/package/braintrust
- Requires Node.js 18+

## Find the latest version of the SDK

Look up the latest version from npm. Do not guess -- use the package manager to find the actual latest version.

### npm

```bash
npm view braintrust version
```

### yarn

```bash
yarn info braintrust version
```

### pnpm

```bash
pnpm view braintrust version
```

## Install the SDK

Install with exact versions.

If the repo uses `pnpm` (e.g. `pnpm-lock.yaml` exists), use `pnpm` rather than `npm install`.

### npm

```bash
npm install --save-exact braintrust@<version> --no-audit --no-fund
```

### yarn

```bash
yarn add --exact braintrust@<version>
```

### pnpm

```bash
pnpm add --save-exact braintrust@<version>
```

## Initialize the SDK

```typescript
import { initLogger } from "braintrust";

const logger = initLogger({
  projectName: "my-project",
  apiKey: process.env.BRAINTRUST_API_KEY,
});
```

`initLogger` is the main entry point for tracing. It reads `BRAINTRUST_API_KEY` from the environment automatically if `apiKey` is not provided. If `initLogger` is not called, all wrapping functions are no-ops.

## Install instrumentation

The TypeScript SDK instruments existing LLM clients by wrapping them. Find which clients/frameworks the project already uses and wrap them as shown below. Only instrument frameworks that are actually present in the project.

### OpenAI (`openai`)

Wrap the existing `OpenAI` client:

```typescript
import OpenAI from "openai";
import { wrapOpenAI } from "braintrust";

const client = wrapOpenAI(new OpenAI({ apiKey: process.env.OPENAI_API_KEY }));
```

### Anthropic (`@anthropic-ai/sdk`)

Wrap the existing `Anthropic` client:

```typescript
import Anthropic from "@anthropic-ai/sdk";
import { wrapAnthropic } from "braintrust";

const client = wrapAnthropic(
  new Anthropic({ apiKey: process.env.ANTHROPIC_API_KEY }),
);
```

### Vercel AI SDK (`ai`) -- module-level wrapper

Wrap the `ai` module to automatically trace `generateText`, `streamText`, `generateObject`, and `streamObject`:

```typescript
import { wrapAISDK } from "braintrust";
import * as ai from "ai";
import { openai } from "@ai-sdk/openai";

const { generateText, streamText } = wrapAISDK(ai);

const { text } = await generateText({
  model: openai("gpt-5-mini"),
  prompt: "What is the capital of France?",
});
```

### Vercel AI SDK (`ai`) -- model-level wrapper

Alternatively, wrap individual model instances:

```typescript
import { wrapAISDKModel } from "braintrust";
import { openai } from "@ai-sdk/openai";

const model = wrapAISDKModel(openai("gpt-5-mini"));
```

### OpenAI Agents SDK (`@openai/agents`)

Install the trace processor package and register it:

```bash
npm install @braintrust/openai-agents @openai/agents
```

```typescript
import { initLogger } from "braintrust";
import { OpenAIAgentsTraceProcessor } from "@braintrust/openai-agents";
import { Agent, run, addTraceProcessor } from "@openai/agents";

const logger = initLogger({ projectName: "my-project" });
const processor = new OpenAIAgentsTraceProcessor({ logger });
addTraceProcessor(processor);

const agent = new Agent({
  name: "Assistant",
  model: "gpt-5-mini",
  instructions: "You are a helpful assistant.",
});

const result = await run(agent, "Hello!");
```

### LangChain.js (`@langchain/core`)

Install the callback handler package and pass it to LangChain calls:

```bash
npm install @braintrust/langchain-js
```

```typescript
import { initLogger } from "braintrust";
import { BraintrustCallbackHandler } from "@braintrust/langchain-js";
import { ChatOpenAI } from "@langchain/openai";

initLogger({ projectName: "my-project" });

const handler = new BraintrustCallbackHandler();
const model = new ChatOpenAI();

await model.invoke("What is the capital of France?", {
  callbacks: [handler],
});
```

## Run the application

Try to figure out how to run the application from the project structure:

- **npm scripts**: check `package.json` for `start`, `dev`, or similar scripts
- **Next.js**: `npm run dev` or `npx next dev`
- **ts-node**: `npx ts-node src/index.ts`
- **tsx**: `npx tsx src/index.ts`
- **Node with TypeScript**: `npx tsc && node dist/index.js`

If you can't determine how to run the app, ask the user.

## Generate a permalink (required)

Follow the permalink generation steps in the agent task (Step 5). Use the `projectName` argument passed to `initLogger` as the project name.
