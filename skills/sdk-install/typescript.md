# TypeScript SDK Install

Reference guide for installing the Braintrust TypeScript SDK.

- SDK repo: https://github.com/braintrustdata/braintrust-sdk-javascript
- npm: https://www.npmjs.com/package/braintrust
- Requires Node.js 18.19.0+ or 20.6.0+ (or Bun 1.0+, Deno with Node compat)

## Install the SDK

Install the latest published version of `braintrust`. Do not hard-pin the version unless the user asks -- let the package manager record whatever it normally records (a caret range or an exact version, whichever is idiomatic).

Match the package manager the repo already uses. Check lockfiles to decide:

- `pnpm-lock.yaml` → `pnpm`
- `yarn.lock` → `yarn`
- `bun.lock` or `bun.lockb` → `bun`
- `package-lock.json` (or none) → `npm`

### npm

```bash
npm install braintrust --no-audit --no-fund
```

### yarn

```bash
yarn add braintrust
```

### pnpm

```bash
pnpm add braintrust
```

### bun

```bash
bun add braintrust
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

**Any runtime with a bundler (Next.js, webpack, Vite, esbuild, etc.)**

Use the appropriate Braintrust bundler plugin / framework integration -- see https://www.braintrust.dev/docs/instrument/trace-llm-calls for the supported plugins and framework setup (e.g. Next.js `instrumentation.ts`, webpack/Vite/esbuild plugins). This is the preferred option whenever a bundler is in play and works under Node, Bun, Deno, and Cloudflare Workers alike.

**Bun / Deno / Cloudflare Workers without a bundler → use manual wrappers**

There is no automatic instrumentation path for these runtimes without a bundler. Use manual wrappers (`wrapOpenAI`, `wrapAnthropic`, `wrapAISDK`, etc.) at call sites instead -- see https://www.braintrust.dev/docs/instrument/trace-llm-calls for the available wrappers and how to apply them.

If none of the above is configured, automatic instrumentation will silently do nothing.

### Requirement: persist the launch hook into the normal run path

Auto-instrumentation requires the application to be started with the hook on every run. A one-off `node --import braintrust/hook.mjs ...` or a shell-local `export NODE_OPTIONS=...` during verification is **not enough** if the next developer, CI job, or deploy will go back to plain `node`, `tsx`, or `npm start`.

Persist the hook into whichever launch path the project actually uses:

- **`package.json` scripts**: update `start`, `dev`, `serve`, etc. to include `--import braintrust/hook.mjs`, for example:
  ```json
  "start": "node --import braintrust/hook.mjs dist/index.js",
  "dev": "tsx --import braintrust/hook.mjs src/index.ts"
  ```
- **`Dockerfile` / container entrypoint**: update the `CMD` / `ENTRYPOINT` or a checked-in start script so containers launch with the hook.
- **Process managers / deploy config**: update `Procfile`, systemd units, PM2 config, ECS task definitions, Kubernetes manifests, etc. that define the real start command.
- **Checked-in env/config**: if the project already uses a checked-in mechanism for env vars, set `NODE_OPTIONS="--import braintrust/hook.mjs"` there. Do **not** rely on a shell-local `export NODE_OPTIONS=...` -- it will not help the next user or CI run.
- **Bundler / framework config**: if a bundler plugin is used, register it in the project's real bundler/framework config file, not in an ad-hoc script.

If you add `initLogger` but do **not** modify any persisted launch path, treat the installation as incomplete.

Verify using the same persisted command the project will actually use (e.g. `npm start`, `npm run dev`, `docker run ...`), not a custom one-off invocation.

## Run the application

Prefer the project's existing launch entrypoint, and make sure that entrypoint now loads the Braintrust hook (or bundler plugin) automatically.

Try to figure out how the project is normally run from the project structure:

- **npm scripts**: prefer `npm start` / `npm run dev` / `pnpm dev` / `yarn start` / `bun run start` -- update the script in `package.json` so it includes `--import braintrust/hook.mjs`, for example:
  ```json
  "start": "node --import braintrust/hook.mjs dist/index.js",
  "dev": "tsx --import braintrust/hook.mjs src/index.ts"
  ```
- **Next.js**: `npm run dev` or `npx next dev` -- wire the Braintrust bundler/framework integration in the project's real Next.js config (e.g. `instrumentation.ts`), not a one-off command
- **ts-node**: ts-node does not support `--import`; migrate to `tsx` instead (`npm install --save-dev tsx`) and update the persisted script
- **tsx**: update the persisted script to `tsx --import braintrust/hook.mjs src/index.ts`
- **Node with TypeScript**: update the persisted build + start scripts to `tsc && node --import braintrust/hook.mjs dist/index.js`
- **Bun**: `bun run <script>` or `bun ./src/index.ts` (without a bundler, use manual wrappers -- `--import` is Node-only and does not apply here)
- **Deno**: `deno run <script>` (without a bundler, use manual wrappers)
- **Cloudflare Workers**: `wrangler dev` / `wrangler deploy` (without a bundler, use manual wrappers; with a bundler, register the Braintrust bundler plugin in the project's real bundler config)
- **Docker / container**: update the `Dockerfile`'s `CMD`/`ENTRYPOINT` or the checked-in start script

If you can't determine how the app is supposed to be built or run in normal use, ask the user before proceeding.

## Generate a permalink (required)

Follow the permalink generation steps in the agent task (Step 5). Use the project name you configured in code above.
