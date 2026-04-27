# TypeScript SDK Install

Reference guide for installing the Braintrust TypeScript SDK.

- SDK repo: https://github.com/braintrustdata/braintrust-sdk-javascript
- npm: https://www.npmjs.com/package/braintrust
- Requires Node.js 18.19.0+ or 20.6.0+ (or Bun 1.0+, Deno with Node compat)

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

### bun

```bash
bun pm view braintrust version
```

## Install the SDK

Install with exact versions.

Match the package manager the repo already uses. Check lockfiles to decide:

- `pnpm-lock.yaml` → `pnpm`
- `yarn.lock` → `yarn`
- `bun.lock` or `bun.lockb` → `bun`
- `package-lock.json` (or none) → `npm`

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

### bun

```bash
bun add --exact braintrust@<version>
```

## Instrument the application

**You must read https://www.braintrust.dev/docs/instrument/trace-llm-calls before instrumenting anything.** That page is the source of truth and may have changed since this guide was written.

### Prefer automatic instrumentation

**Automatic instrumentation is the recommended path and should be used whenever possible.** It patches supported LLM clients/frameworks (OpenAI, Anthropic, Vercel AI SDK, OpenAI Agents SDK, LangChain.js, etc.) at module load time with no call-site changes, so new code and third-party code are traced automatically.

Automatic instrumentation is enabled one of two ways:

- **Node.js, no bundler** → preload via `node --import` (see below). Node.js only -- `--import` does not work under Bun, Deno, or Cloudflare Workers.
- **Any runtime with a bundler** (Next.js, webpack, Vite, esbuild, etc.) → use the Braintrust bundler plugin. The bundler plugin is the preferred option whenever a bundler is in play and works regardless of runtime (Node, Bun, Deno, Cloudflare Workers, etc.).

Manual `wrapOpenAI` / `wrapAnthropic` / `wrapAISDK` / etc. call-site wrappers should only be used when automatic instrumentation isn't available for your setup. The legitimate cases are:

- Running on **Bun, Deno, or Cloudflare Workers without a bundler** -- there is no automatic path in that configuration, so manual wrappers are the correct choice.
- Instrumenting a client/framework that automatic instrumentation doesn't yet support.

In every other case (Node.js, or any runtime with a bundler), prefer automatic instrumentation and don't reach for manual wrappers until you've confirmed neither `--import` nor a bundler plugin can be made to work.

### Quick start

Create a dedicated setup file (e.g. `instrumentation.ts`) that calls `initLogger`:

```typescript
import { initLogger } from "braintrust";

initLogger({
  projectName: "my-project",
  apiKey: process.env.BRAINTRUST_API_KEY,
});
```

`initLogger` is the main entry point for tracing. It reads `BRAINTRUST_API_KEY` from the environment automatically if `apiKey` is not provided. If `initLogger` is not called, instrumentation is a no-op.

The exact contents of this file (which instrumentations to register, etc.) come from https://www.braintrust.dev/docs/instrument/trace-llm-calls -- follow it.

### Setting up automatic instrumentation (recommended)

Automatic instrumentation only works if the setup file is loaded **before** the rest of your application, so it can patch LLM client modules before user code imports them. The patch happens at startup, and no per-call code change is required. Pick whichever matches your setup:

**Node.js without a bundler (`--import`)**

`--import` is a Node.js-only flag. Do not use it under Bun, Deno, or Cloudflare Workers.
Call `initLogger()` once at startup, then run your application with the `--import` flag:

```bash
node --import braintrust/hook.mjs ./dist/index.js
# or with tsx
npx tsx --import braintrust/hook.mjs ./src/index.ts
```

Or via the environment:

```bash
export NODE_OPTIONS="--import braintrust/hook.mjs"
```

**Any runtime with a bundler (Next.js, webpack, Vite, esbuild, etc.)**

Use the appropriate Braintrust bundler plugin / framework integration -- see https://www.braintrust.dev/docs/instrument/trace-llm-calls for the supported plugins and framework setup (e.g. Next.js `instrumentation.ts`, webpack/Vite/esbuild plugins). This is the preferred option whenever a bundler is in play and works under Node, Bun, Deno, and Cloudflare Workers alike.

**Bun / Deno / Cloudflare Workers without a bundler → use manual wrappers**

There is no automatic instrumentation path for these runtimes without a bundler. Use manual wrappers (`wrapOpenAI`, `wrapAnthropic`, `wrapAISDK`, etc.) at call sites instead -- see https://www.braintrust.dev/docs/instrument/trace-llm-calls for the available wrappers and how to apply them.

If none of the above is configured, automatic instrumentation will silently do nothing.

## Run the application

Try to figure out how to run the application from the project structure:

- **npm scripts**: check `package.json` for `start`, `dev`, or similar scripts and add `--import braintrust/hook.mjs` to the run command, for example:
  ```json
  "start": "node --import braintrust/hook.mjs dist/index.js"
  "dev": "tsx --import braintrust/hook.mjs src/index.ts"
  ```
- **Next.js**: `npm run dev` or `npx next dev`
- **ts-node**: ts-node does not support `--import`; migrate to `tsx` instead (`npm install --save-dev tsx`)
- **tsx**: `npx tsx --import braintrust/hook.mjs src/index.ts`
- **Node with TypeScript**: `npx tsc && node --import braintrust/hook.mjs dist/index.js`
- **Bun**: `bun run <script>` or `bun ./src/index.ts` (without a bundler, use manual wrappers -- `--import` is Node-only and does not apply here)
- **Deno**: `deno run <script>` (without a bundler, use manual wrappers)
- **Cloudflare Workers**: `wrangler dev` / `wrangler deploy` (without a bundler, use manual wrappers; with a bundler, use the Braintrust bundler plugin)

If you can't determine how to run the app, ask the user.

## Generate a permalink (required)

Follow the permalink generation steps in the agent task (Step 5). Search the project for the `initLogger` call to find the `projectName` — it may be in a separate bootstrap or entry file, not the file currently being edited.
