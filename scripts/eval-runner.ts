import { createRequire } from "node:module";
import path from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

type EvaluatorEntry = {
  evaluator: {
    evalName: string;
    projectName: string;
  } & Record<string, unknown>;
  reporter?: unknown;
};

type EvalResult = {
  results: Array<{ error?: unknown }>;
  summary: unknown;
};

type ProgressReporter = {
  start: (name: string, total: number) => void;
  stop: (name: string) => void;
  increment: (name: string) => void;
  setTotal: (name: string, total: number) => void;
};

type EvalOptions = Record<string, unknown> & {
  progress?: Partial<ProgressReporter>;
  stream?: (data: unknown) => void;
  onStart?: (data: unknown) => void;
  reporter?: unknown;
  noSendLogs?: boolean;
};

type EvalFunction = (
  projectName: string,
  evaluator: Record<string, unknown>,
  options?: EvalOptions,
) => Promise<EvalResult>;

type LoginFunction = (options?: Record<string, unknown>) => Promise<unknown>;
type InitDatasetFunction = (
  projectOrOptions: string | Record<string, unknown>,
  options?: Record<string, unknown>,
) => unknown;
type InvokeFunction = (options: Record<string, unknown>) => Promise<unknown>;

type BraintrustModule = {
  Eval?: EvalFunction;
  login?: LoginFunction;
  initDataset?: InitDatasetFunction;
  invoke?: InvokeFunction;
  default?: BraintrustModule;
};

type GlobalEvals = {
  functions: unknown[];
  prompts: unknown[];
  parameters: unknown[];
  evaluators: Record<string, EvaluatorEntry>;
  reporters: Record<string, unknown>;
};

type BtEvalMain = (context: BtEvalContext) => void | Promise<void>;

type BtEvalContext = {
  Eval: EvalFunction;
  runEval: (
    projectName: string,
    evaluator: Record<string, unknown>,
    options?: EvalOptions,
  ) => Promise<EvalResult>;
  runRegisteredEvals: () => Promise<boolean>;
  makeEvalOptions: (
    evaluatorName: string,
    options?: EvalOptions,
  ) => EvalOptions | undefined;
  sendConsole: (message: string, stream?: "stdout" | "stderr") => void;
  sendEvent: (event: string, data: unknown) => void;
};

type SseWriter = {
  send: (event: string, data: unknown) => void;
  close: () => void;
};

type EvalFilter = {
  path: string[];
  pattern: RegExp;
};

type SerializedEvalFilter = {
  path: string[];
  pattern: string;
};

type RunnerConfig = {
  jsonl: boolean;
  list: boolean;
  terminateOnFailure: boolean;
  filters: EvalFilter[];
  remoteListJson: boolean;
  remoteRequest: RemoteEvalRequest | null;
};

type RemoteScoreSpec = {
  name: string;
  function_id: Record<string, unknown>;
};

type RemoteEvalRequest = {
  name: string;
  parameters?: Record<string, unknown>;
  parent?: unknown;
  experiment_name?: string;
  project_id?: string;
  data:
    | { data: unknown[] }
    | { project_id: string; dataset_name: string; _internal_btql?: unknown }
    | { project_name: string; dataset_name: string; _internal_btql?: unknown };
  scores?: RemoteScoreSpec[];
  stream?: boolean;
};

declare global {
  // eslint-disable-next-line no-var
  var _evals: GlobalEvals | undefined;
  // eslint-disable-next-line no-var
  var _lazy_load: boolean | undefined;
  // eslint-disable-next-line no-var
  var __inherited_braintrust_state: unknown;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isBraintrustModule(value: unknown): value is BraintrustModule {
  return isObject(value) && ("Eval" in value || "login" in value);
}

function normalizeBraintrustModule(value: unknown): BraintrustModule {
  if (isBraintrustModule(value)) {
    return value;
  }
  if (isObject(value) && isBraintrustModule(value.default)) {
    return value.default;
  }
  throw new Error("Unable to load braintrust module.");
}

function normalizeFiles(files: string[]): string[] {
  return files.map((file) => path.resolve(process.cwd(), file));
}

function envFlag(name: string): boolean {
  const value = process.env[name];
  if (!value) {
    return false;
  }
  const normalized = value.toLowerCase();
  return !["0", "false", "no", "off", ""].includes(normalized);
}

function serializeJSONWithPlainString(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  return JSON.stringify(value);
}

function parseSerializedFilters(serialized: string | undefined): EvalFilter[] {
  if (!serialized) {
    return [];
  }

  try {
    const parsed = JSON.parse(serialized);
    if (!Array.isArray(parsed)) {
      throw new Error("BT_EVAL_FILTER_PARSED must be a JSON array.");
    }
    return parsed.map((value) => {
      if (!isObject(value)) {
        throw new Error(
          "BT_EVAL_FILTER_PARSED entries must be objects with {path, pattern}.",
        );
      }
      const { path: rawPath, pattern: rawPattern } =
        value as SerializedEvalFilter;
      if (
        !Array.isArray(rawPath) ||
        !rawPath.every((part) => typeof part === "string")
      ) {
        throw new Error(
          "BT_EVAL_FILTER_PARSED entry path must be an array of strings.",
        );
      }
      if (typeof rawPattern !== "string") {
        throw new Error(
          "BT_EVAL_FILTER_PARSED entry pattern must be a string.",
        );
      }
      return {
        path: rawPath,
        pattern: new RegExp(rawPattern),
      };
    });
  } catch (err) {
    throw new Error(
      `Invalid BT_EVAL_FILTER_PARSED value: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

function parseRemoteEvalRequest(
  serialized: string | undefined,
): RemoteEvalRequest | null {
  if (!serialized) {
    return null;
  }
  try {
    const parsed = JSON.parse(serialized);
    if (!isObject(parsed)) {
      throw new Error("BT_EVAL_REMOTE_REQUEST_JSON must be a JSON object.");
    }
    if (typeof parsed.name !== "string" || parsed.name.length === 0) {
      throw new Error("Remote eval request must include a non-empty name.");
    }
    if (!isObject(parsed.data)) {
      throw new Error("Remote eval request must include a data object.");
    }
    return parsed as RemoteEvalRequest;
  } catch (err) {
    throw new Error(
      `Invalid BT_EVAL_REMOTE_REQUEST_JSON value: ${err instanceof Error ? err.message : String(err)}`,
    );
  }
}

function readRunnerConfig(): RunnerConfig {
  return {
    jsonl: envFlag("BT_EVAL_JSONL"),
    list: envFlag("BT_EVAL_LIST"),
    terminateOnFailure: envFlag("BT_EVAL_TERMINATE_ON_FAILURE"),
    filters: parseSerializedFilters(process.env.BT_EVAL_FILTER_PARSED),
    remoteListJson: envFlag("BT_EVAL_REMOTE_LIST_JSON"),
    remoteRequest: parseRemoteEvalRequest(
      process.env.BT_EVAL_REMOTE_REQUEST_JSON,
    ),
  };
}

const runtimeRequire = createRequire(
  process.argv[1] ?? path.join(process.cwd(), "package.json"),
);
const fsMutable = runtimeRequire("node:fs") as typeof import("node:fs");
const moduleMutable = (() => {
  try {
    return runtimeRequire("node:module") as Record<string, unknown>;
  } catch {
    return {};
  }
})();

type NetModule = {
  createConnection: (options: Record<string, unknown>) => {
    writable: boolean;
    end: () => void;
    setNoDelay: (value?: boolean) => void;
    on: (event: string, listener: (...args: unknown[]) => void) => void;
    write: (data: string) => void;
  };
};

const dependencyFiles = new Set<string>();
const DEPENDENCY_EXTENSIONS = new Set([
  ".ts",
  ".tsx",
  ".js",
  ".jsx",
  ".mjs",
  ".cjs",
  ".mts",
  ".cts",
  ".json",
]);
const IGNORED_DEPENDENCY_SEGMENTS = [
  "/node_modules/",
  "/.git/",
  "/.venv/",
  "/__pycache__/",
  "/site-packages/",
  "/dist-packages/",
];
const STATIC_IMPORT_PATTERN =
  /(?:import|export)\s+(?:[^"'`]*?\sfrom\s*)?["'`]([^"'`]+)["'`]|import\s*\(\s*["'`]([^"'`]+)["'`]\s*\)|require\s*\(\s*["'`]([^"'`]+)["'`]\s*\)/g;

function toDependencyPath(input: unknown): string | null {
  try {
    if (input instanceof URL) {
      return fileURLToPath(input);
    }
    if (Buffer.isBuffer(input)) {
      return path.resolve(process.cwd(), input.toString());
    }
    if (typeof input !== "string") {
      return null;
    }
    if (input.startsWith("file://")) {
      return fileURLToPath(input);
    }
    return path.isAbsolute(input)
      ? path.normalize(input)
      : path.resolve(process.cwd(), input);
  } catch {
    return null;
  }
}

function shouldIgnoreDependencyPath(filePath: string): boolean {
  const normalized = filePath.replaceAll("\\", "/");
  return IGNORED_DEPENDENCY_SEGMENTS.some((segment) =>
    normalized.includes(segment),
  );
}

function maybeRecordDependency(input: unknown) {
  const filePath = toDependencyPath(input);
  if (!filePath || shouldIgnoreDependencyPath(filePath)) {
    return;
  }

  const extension = path.extname(filePath).toLowerCase();
  if (!DEPENDENCY_EXTENSIONS.has(extension)) {
    return;
  }

  try {
    if (fsMutable.statSync(filePath).isFile()) {
      dependencyFiles.add(filePath);
    }
  } catch {
    // Ignore inaccessible or non-file inputs.
  }
}

function maybeRecordDependencyFromSpecifier(
  specifier: string,
  resolveDir?: string,
) {
  if (
    specifier.startsWith("node:") ||
    specifier.startsWith("bun:") ||
    specifier.startsWith("npm:")
  ) {
    return;
  }

  if (
    specifier.startsWith("./") ||
    specifier.startsWith("../") ||
    specifier.startsWith("/") ||
    specifier.startsWith("file://")
  ) {
    const baseDir = resolveDir ?? process.cwd();
    const candidate = specifier.startsWith("file://")
      ? specifier
      : path.resolve(baseDir, specifier);
    maybeRecordDependency(candidate);
  }
}

function collectStaticLocalDependencies(entryFiles: string[]) {
  const queue = [...entryFiles];
  const visited = new Set<string>();

  while (queue.length > 0) {
    const file = queue.pop();
    if (!file) {
      continue;
    }
    const absolute = path.resolve(file);
    if (visited.has(absolute)) {
      continue;
    }
    visited.add(absolute);
    maybeRecordDependency(absolute);

    let source = "";
    try {
      source = fsMutable.readFileSync(absolute, "utf8");
    } catch {
      continue;
    }

    STATIC_IMPORT_PATTERN.lastIndex = 0;
    let match: RegExpExecArray | null;
    while ((match = STATIC_IMPORT_PATTERN.exec(source)) !== null) {
      const specifier = match[1] ?? match[2] ?? match[3];
      if (!specifier) {
        continue;
      }
      const resolved = resolveLocalSpecifier(absolute, specifier);
      if (!resolved) {
        continue;
      }
      maybeRecordDependency(resolved);
      if (!visited.has(resolved)) {
        queue.push(resolved);
      }
    }
  }
}

function resolveLocalSpecifier(
  fromFile: string,
  specifier: string,
): string | null {
  if (
    !specifier.startsWith("./") &&
    !specifier.startsWith("../") &&
    !specifier.startsWith("/") &&
    !specifier.startsWith("file://")
  ) {
    return null;
  }

  const fromDir = path.dirname(fromFile);
  const base = specifier.startsWith("file://")
    ? fileURLToPath(specifier)
    : specifier.startsWith("/")
      ? path.normalize(specifier)
      : path.resolve(fromDir, specifier);

  const candidates = [base];
  if (!path.extname(base)) {
    for (const ext of DEPENDENCY_EXTENSIONS) {
      candidates.push(`${base}${ext}`);
    }
    for (const ext of DEPENDENCY_EXTENSIONS) {
      candidates.push(path.join(base, `index${ext}`));
    }
  }

  for (const candidate of candidates) {
    try {
      if (fsMutable.statSync(candidate).isFile()) {
        return path.normalize(candidate);
      }
    } catch {
      continue;
    }
  }

  return null;
}

function installNodeModuleHooks() {
  const registerHooks = moduleMutable.registerHooks as
    | ((hooks: Record<string, (...args: unknown[]) => unknown>) => void)
    | undefined;
  if (typeof registerHooks !== "function") {
    return;
  }

  registerHooks({
    resolve: (specifier, context, nextResolve) => {
      const next = nextResolve as (
        specifier: unknown,
        context: Record<string, unknown>,
      ) => { url?: string } & Record<string, unknown>;
      const ctx = (context ?? {}) as Record<string, unknown>;
      const result = next(specifier, ctx);
      const resolvedUrl = result?.url;
      if (typeof resolvedUrl === "string") {
        maybeRecordDependency(resolvedUrl);
      } else if (typeof specifier === "string") {
        const resolveDir =
          typeof ctx.parentURL === "string" &&
          ctx.parentURL.startsWith("file://")
            ? path.dirname(fileURLToPath(ctx.parentURL))
            : undefined;
        maybeRecordDependencyFromSpecifier(specifier, resolveDir);
      }
      return result;
    },
  });
}

function installBunModuleHooks() {
  const bun = (globalThis as { Bun?: Record<string, unknown> }).Bun as
    | {
        plugin?: (plugin: {
          name: string;
          setup: (build: Record<string, unknown>) => void;
        }) => void;
      }
    | undefined;
  if (!bun || typeof bun.plugin !== "function") {
    return;
  }

  bun.plugin({
    name: "bt-eval-dependency-tracker",
    setup: (build: Record<string, unknown>) => {
      const onResolve = build.onResolve as
        | ((
            options: { filter: RegExp },
            callback: (args: Record<string, unknown>) => unknown,
          ) => void)
        | undefined;
      if (typeof onResolve === "function") {
        onResolve({ filter: /.*/ }, (args) => {
          const specifier = args.path;
          const resolveDir =
            typeof args.resolveDir === "string"
              ? args.resolveDir
              : process.cwd();
          if (typeof specifier === "string") {
            maybeRecordDependencyFromSpecifier(specifier, resolveDir);
          }
          return null;
        });
      }
    },
  });
}

function installDependencyTracking() {
  installNodeModuleHooks();
  installBunModuleHooks();

  const fsPatched = fsMutable as unknown as Record<string, unknown>;
  const originalReadFileSync = fsMutable.readFileSync.bind(fsMutable);
  Reflect.set(
    fsPatched,
    "readFileSync",
    (file: unknown, ...args: unknown[]) => {
      maybeRecordDependency(file);
      const callArgs = [file, ...args] as unknown[];
      return Reflect.apply(
        originalReadFileSync as (...params: unknown[]) => unknown,
        fsMutable,
        callArgs,
      );
    },
  );

  const originalReadFile = fsMutable.readFile.bind(fsMutable);
  Reflect.set(fsPatched, "readFile", (file: unknown, ...args: unknown[]) => {
    maybeRecordDependency(file);
    const callArgs = [file, ...args] as unknown[];
    return Reflect.apply(
      originalReadFile as (...params: unknown[]) => unknown,
      fsMutable,
      callArgs,
    );
  });

  const originalPromisesReadFile = fsMutable.promises.readFile.bind(
    fsMutable.promises,
  );
  const fsPromisesPatched = fsMutable.promises as unknown as Record<
    string,
    unknown
  >;
  Reflect.set(
    fsPromisesPatched,
    "readFile",
    async (file: unknown, ...args: unknown[]) => {
      maybeRecordDependency(file);
      const callArgs = [file, ...args] as unknown[];
      return Reflect.apply(
        originalPromisesReadFile as (...params: unknown[]) => Promise<unknown>,
        fsMutable.promises,
        callArgs,
      );
    },
  );
}

function collectRequireCacheDependencies() {
  const cache = runtimeRequire.cache as Record<
    string,
    { filename?: string } | undefined
  >;
  if (!cache) {
    return;
  }
  for (const [cacheKey, moduleValue] of Object.entries(cache)) {
    maybeRecordDependency(moduleValue?.filename ?? cacheKey);
  }
}

async function collectDenoInfoDependencies(files: string[]) {
  const deno = (globalThis as Record<string, unknown>).Deno as
    | {
        Command?: new (
          command: string,
          options: Record<string, unknown>,
        ) => {
          output: () => Promise<{
            success: boolean;
            stdout: Uint8Array;
          }>;
        };
      }
    | undefined;
  if (!deno || typeof deno.Command !== "function") {
    return;
  }

  for (const file of files) {
    try {
      const cmd = new deno.Command("deno", {
        args: ["info", "--json", file],
        stdout: "piped",
        stderr: "null",
      });
      const output = await cmd.output();
      if (!output.success) {
        continue;
      }
      const parsed = JSON.parse(new TextDecoder().decode(output.stdout));
      collectFileUrlsFromJson(parsed);
    } catch {
      continue;
    }
  }
}

function collectFileUrlsFromJson(value: unknown) {
  if (typeof value === "string") {
    maybeRecordDependency(value);
    return;
  }
  if (Array.isArray(value)) {
    for (const item of value) {
      collectFileUrlsFromJson(item);
    }
    return;
  }
  if (!value || typeof value !== "object") {
    return;
  }
  for (const child of Object.values(value)) {
    collectFileUrlsFromJson(child);
  }
}

function collectDependencyFiles(): string[] {
  return Array.from(dependencyFiles).sort();
}

function serializeSseEvent(event: { event?: string; data: string }): string {
  return (
    Object.entries(event)
      .filter(([_key, value]) => value !== undefined)
      .map(([key, value]) => `${key}: ${value}`)
      .join("\n") + "\n\n"
  );
}

function createSseWriter(): SseWriter | null {
  const netModule = (() => {
    try {
      return runtimeRequire("node:net") as NetModule;
    } catch {
      return null;
    }
  })();

  const sock = process.env.BT_EVAL_SSE_SOCK;
  if (sock) {
    if (!netModule) {
      return null;
    }
    let socket: ReturnType<NetModule["createConnection"]>;
    try {
      socket = netModule.createConnection({ path: sock });
    } catch (err) {
      console.error(
        `Failed to connect to SSE socket: ${
          err instanceof Error ? err.message : String(err)
        }`,
      );
      return null;
    }
    socket.on("error", (err) => {
      const message = err instanceof Error ? err.message : String(err);
      console.error(`Failed to connect to SSE socket: ${message}`);
    });
    const send = (event: string, payload: unknown) => {
      if (!socket.writable) {
        return;
      }
      const data =
        typeof payload === "string" ? payload : JSON.stringify(payload);
      socket.write(serializeSseEvent({ event, data }));
    };
    const close = () => {
      socket.end();
    };
    return { send, close };
  }

  const addr = process.env.BT_EVAL_SSE_ADDR;
  if (!addr) {
    return null;
  }

  const [host, portStr] = addr.split(":");
  const port = Number(portStr);
  if (!host || !Number.isFinite(port)) {
    throw new Error(`Invalid BT_EVAL_SSE_ADDR: ${addr}`);
  }

  if (!netModule) {
    return null;
  }

  let socket: ReturnType<NetModule["createConnection"]>;
  try {
    socket = netModule.createConnection({ host, port });
  } catch (err) {
    console.error(
      `Failed to connect to SSE address ${addr}: ${
        err instanceof Error ? err.message : String(err)
      }`,
    );
    return null;
  }
  socket.setNoDelay(true);
  socket.on("error", (err) => {
    const message = err instanceof Error ? err.message : String(err);
    console.error(`Failed to connect to SSE address ${addr}: ${message}`);
  });

  const send = (event: string, payload: unknown) => {
    if (!socket.writable) {
      return;
    }
    const data =
      typeof payload === "string" ? payload : JSON.stringify(payload);
    socket.write(serializeSseEvent({ event, data }));
  };

  const close = () => {
    socket.end();
  };

  return { send, close };
}

function initRegistry() {
  globalThis._evals = {
    functions: [],
    prompts: [],
    parameters: [],
    evaluators: {},
    reporters: {},
  };
  globalThis._lazy_load = true;
}

function ensureBraintrustAvailable() {
  resolveBraintrustPath();
}

function resolveBraintrustPath(): string {
  const files = normalizeFiles(process.argv.slice(2));
  for (const file of files) {
    try {
      const require = createRequire(pathToFileURL(file).href);
      return require.resolve("braintrust");
    } catch {
      continue;
    }
  }

  try {
    const require = createRequire(process.cwd() + "/");
    return require.resolve("braintrust");
  } catch {
    const message =
      "Unable to resolve the `braintrust` package. " +
      "Please install it in your project (e.g. `pnpm add braintrust` or `npm install braintrust`).";
    throw new Error(message);
  }
}

async function loadBraintrust() {
  const resolved = resolveBraintrustPath();
  const moduleUrl = pathToFileURL(resolved).href;
  const mod: unknown = await import(moduleUrl);
  return normalizeBraintrustModule(mod);
}

function propagateInheritedBraintrustState(braintrust: BraintrustModule) {
  const getter = (braintrust as Record<string, unknown>)
    ._internalGetGlobalState;
  if (typeof getter !== "function") {
    return;
  }
  const state = getter();
  if (state !== undefined && state !== null) {
    globalThis.__inherited_braintrust_state = state;
  }
}

async function loadFiles(files: string[]): Promise<unknown[]> {
  const modules: unknown[] = [];
  for (const file of files) {
    const fileUrl = pathToFileURL(file).href;
    const preferRequire =
      file.endsWith(".ts") || file.endsWith(".tsx") || file.endsWith(".cjs");

    if (preferRequire) {
      try {
        const require = createRequire(fileUrl);
        const mod = require(file);
        modules.push(mod);
        continue;
      } catch (requireErr) {
        try {
          const mod = await import(fileUrl);
          modules.push(mod);
          continue;
        } catch (esmErr) {
          throw new Error(
            `Failed to load ${file} as CJS (${formatError(requireErr)}) or ESM (${formatError(esmErr)}).`,
          );
        }
      }
    }

    try {
      const mod = await import(fileUrl);
      modules.push(mod);
      continue;
    } catch (err) {
      if (!shouldTryRequire(file, err)) {
        throw err;
      }
      try {
        const require = createRequire(fileUrl);
        const mod = require(file);
        modules.push(mod);
        continue;
      } catch (requireErr) {
        throw new Error(
          `Failed to load ${file} as ESM (${formatError(err)}) or CJS (${formatError(requireErr)}).`,
        );
      }
    }
  }
  return modules;
}

function shouldTryRequire(file: string, err: unknown): boolean {
  if (process.env.BT_EVAL_CJS === "1" || file.endsWith(".cjs")) {
    return true;
  }
  if (
    (file.endsWith(".ts") || file.endsWith(".tsx")) &&
    isNodeErrorCode(err, "ERR_UNKNOWN_FILE_EXTENSION")
  ) {
    return true;
  }
  if (!(err instanceof Error)) {
    return false;
  }
  const message = err.message || "";
  return (
    message.includes("require is not defined") ||
    message.includes("exports is not defined") ||
    message.includes("module is not defined") ||
    message.includes("Cannot use import statement outside a module")
  );
}

function isNodeErrorCode(err: unknown, code: string): boolean {
  if (!isObject(err) || !("code" in err)) {
    return false;
  }
  return typeof err.code === "string" && err.code === code;
}

function formatError(err: unknown): string {
  if (err instanceof Error) {
    return err.message;
  }
  return String(err);
}

function createEvalProgressReporter(
  sse: SseWriter | null,
  evaluatorName: string,
) {
  let activeName = evaluatorName;
  return {
    start: (name: string, total: number) => {
      activeName = name;
      sendEvalProgress(sse, name, "start", total);
    },
    stop: () => {
      if (activeName) {
        sendEvalProgress(sse, activeName, "stop");
      }
    },
    increment: (name: string) => {
      sendEvalProgress(sse, name, "increment");
    },
    setTotal: (name: string, total: number) => {
      sendEvalProgress(sse, name, "set_total", total);
    },
  };
}

function sendEvalProgress(
  sse: SseWriter | null,
  evaluatorName: string,
  kind: "start" | "increment" | "set_total" | "stop",
  total?: number,
) {
  if (!sse) {
    return;
  }
  sse.send("progress", {
    id: `eval-progress:${evaluatorName}`,
    object_type: "task",
    format: "global",
    output_type: "any",
    name: evaluatorName,
    event: "progress",
    data: JSON.stringify({
      type: "eval_progress",
      kind,
      ...(total !== undefined ? { total } : {}),
    }),
  });
}

function serializeError(err: unknown, status?: number) {
  if (err instanceof Error) {
    return {
      message: err.message,
      stack: err.stack,
      ...(status !== undefined ? { status } : {}),
    };
  }
  return {
    message: String(err),
    ...(status !== undefined ? { status } : {}),
  };
}

function sendConsole(
  sse: SseWriter | null,
  message: string,
  stream: "stdout" | "stderr" = "stderr",
) {
  if (!sse) {
    if (stream === "stderr") {
      console.error(message);
    } else {
      console.log(message);
    }
    return;
  }
  sse.send("console", { stream, message });
}

function getEvaluators(): EvaluatorEntry[] {
  const evals = globalThis._evals;
  if (!evals || !evals.evaluators) {
    return [];
  }
  return Object.values(evals.evaluators) as EvaluatorEntry[];
}

function getReporters(): Record<string, unknown> {
  const evals = globalThis._evals;
  if (!evals || !evals.reporters) {
    return {};
  }
  return evals.reporters as Record<string, unknown>;
}

function resolveReporter(
  reporter: unknown,
  reporters: Record<string, unknown>,
): unknown | undefined {
  if (typeof reporter === "string") {
    if (!(reporter in reporters)) {
      throw new Error(`Reporter ${reporter} not found`);
    }
    return reporters[reporter];
  }
  if (reporter !== undefined && reporter !== null) {
    return reporter;
  }

  const values = Object.values(reporters);
  if (values.length === 0) {
    return undefined;
  }
  if (values.length === 1) {
    return values[0];
  }
  const names = Object.keys(reporters).join(", ");
  throw new Error(
    `Multiple reporters found (${names}). Please specify a reporter explicitly.`,
  );
}

function evaluateFilter(
  object: Record<string, unknown>,
  filter: EvalFilter,
): boolean {
  const key = filter.path.reduce<unknown>((acc, part) => {
    if (!isObject(acc)) {
      return undefined;
    }
    return acc[part];
  }, object);
  if (key === undefined) {
    return false;
  }
  return filter.pattern.test(serializeJSONWithPlainString(key));
}

function filterEvaluators(
  evaluators: EvaluatorEntry[],
  filters: EvalFilter[],
): EvaluatorEntry[] {
  if (filters.length === 0) {
    return evaluators;
  }
  return evaluators.filter((entry) =>
    filters.every((filter) => evaluateFilter(entry.evaluator, filter)),
  );
}

function sendRunnerError(sse: SseWriter | null, err: unknown, status?: number) {
  if (sse) {
    sse.send("error", serializeError(err, status));
  } else if (err instanceof Error) {
    console.error(err.message);
  } else {
    console.error(String(err));
  }
}

function extractScoreName(score: unknown, idx: number): string {
  if (typeof score === "function" && typeof score.name === "string") {
    return score.name || `scorer_${idx}`;
  }
  return `scorer_${idx}`;
}

async function serializeEvaluatorParameters(
  raw: unknown,
): Promise<unknown | undefined> {
  if (raw === undefined || raw === null) {
    return undefined;
  }

  const resolved = raw instanceof Promise ? await raw : raw;
  if (!isObject(resolved)) {
    return undefined;
  }

  const marker = Reflect.get(resolved, "__braintrust_parameters_marker");
  if (marker === true) {
    const schema = Reflect.get(resolved, "schema");
    const source = {
      parametersId: Reflect.get(resolved, "id"),
      slug: Reflect.get(resolved, "slug"),
      name: Reflect.get(resolved, "name"),
      projectId: Reflect.get(resolved, "projectId"),
      version: Reflect.get(resolved, "version"),
    };
    return {
      type: "braintrust.parameters",
      schema,
      source,
    };
  }

  const schema: Record<string, unknown> = {};
  for (const [name, value] of Object.entries(resolved)) {
    if (isObject(value) && value.type === "prompt") {
      schema[name] = {
        type: "prompt",
        ...(value.default !== undefined ? { default: value.default } : {}),
        ...(typeof value.description === "string"
          ? { description: value.description }
          : {}),
      };
    } else {
      schema[name] = {
        type: "data",
        schema: {},
      };
    }
  }

  return {
    type: "braintrust.staticParameters",
    schema,
    source: null,
  };
}

async function buildEvaluatorDefinitions(evaluators: EvaluatorEntry[]) {
  const result: Record<
    string,
    { parameters?: unknown; scores: Array<{ name: string }> }
  > = {};

  for (const entry of evaluators) {
    const scores = Array.isArray(entry.evaluator.scores)
      ? entry.evaluator.scores.map((score, idx) => ({
          name: extractScoreName(score, idx),
        }))
      : [];
    const parameters = await serializeEvaluatorParameters(
      entry.evaluator.parameters,
    );

    result[entry.evaluator.evalName] = {
      ...(parameters !== undefined ? { parameters } : {}),
      scores,
    };
  }

  return result;
}

function normalizeRemoteParent(parent: unknown): string | undefined {
  if (parent === undefined || parent === null) {
    return undefined;
  }
  if (typeof parent === "string") {
    return parent;
  }
  return JSON.stringify(parent);
}

function resolveRemoteData(
  data: RemoteEvalRequest["data"],
  initDataset: InitDatasetFunction | undefined,
): unknown {
  if ("data" in data) {
    return data.data;
  }
  if ("project_id" in data && "dataset_name" in data) {
    if (typeof initDataset !== "function") {
      throw new Error(
        "Unable to resolve dataset references: initDataset() is unavailable.",
      );
    }
    return initDataset({
      projectId: data.project_id,
      dataset: data.dataset_name,
      _internal_btql: data._internal_btql,
    });
  }
  if ("project_name" in data && "dataset_name" in data) {
    if (typeof initDataset !== "function") {
      throw new Error(
        "Unable to resolve dataset references: initDataset() is unavailable.",
      );
    }
    return initDataset(data.project_name, {
      dataset: data.dataset_name,
      _internal_btql: data._internal_btql,
    });
  }
  throw new Error("Invalid remote eval data payload.");
}

function convertRemoteFunctionId(
  functionId: Record<string, unknown>,
): Record<string, unknown> {
  const converted: Record<string, unknown> = {};

  if (functionId.function_id !== undefined) {
    converted.function_id = functionId.function_id;
  }
  if (typeof functionId.project_name === "string") {
    converted.projectName = functionId.project_name;
  }
  if (typeof functionId.slug === "string") {
    converted.slug = functionId.slug;
  }
  if (typeof functionId.global_function === "string") {
    converted.globalFunction = functionId.global_function;
  }
  if (typeof functionId.function_type === "string") {
    converted.functionType = functionId.function_type;
  }
  if (typeof functionId.prompt_session_id === "string") {
    converted.promptSessionId = functionId.prompt_session_id;
  }
  if (typeof functionId.prompt_session_function_id === "string") {
    converted.promptSessionFunctionId = functionId.prompt_session_function_id;
  }
  if (typeof functionId.version === "string") {
    converted.version = functionId.version;
  }

  return converted;
}

function makeRemoteScorer(
  invoke: InvokeFunction,
  score: RemoteScoreSpec,
  projectId: string | undefined,
) {
  const scorer = async (args: Record<string, unknown>) => {
    return await invoke({
      ...convertRemoteFunctionId(score.function_id),
      input: {
        input: args.input,
        output: args.output,
        expected: args.expected,
        metadata: args.metadata,
      },
      stream: false,
      mode: "auto",
      strict: true,
      ...(projectId ? { projectId } : {}),
    });
  };

  Object.defineProperty(scorer, "name", {
    value: score.name,
    writable: false,
  });
  return scorer;
}

function inferRemoteErrorStatus(err: unknown): number {
  if (!(err instanceof Error)) {
    return 500;
  }
  const message = err.message.toLowerCase();
  if (
    message.includes("invalid parameter") ||
    message.includes("invalid parameters") ||
    message.includes("must include") ||
    message.includes("invalid remote eval")
  ) {
    return 400;
  }
  if (message.includes("not found")) {
    return 404;
  }
  return 500;
}

type EvalRunner = {
  Eval: EvalFunction;
  initDataset?: InitDatasetFunction;
  invoke?: InvokeFunction;
  sse: SseWriter | null;
  login?: LoginFunction;
  runEval: (
    projectName: string,
    evaluator: Record<string, unknown>,
    options?: EvalOptions,
  ) => Promise<EvalResult>;
  runRegisteredEvals: (evaluators: EvaluatorEntry[]) => Promise<boolean>;
  makeEvalOptions: (
    evaluatorName: string,
    options?: EvalOptions,
  ) => EvalOptions | undefined;
  finish: (ok: boolean) => void;
  noSendLogs: boolean;
};

async function runRemoteEvalRequest(
  runner: EvalRunner,
  config: RunnerConfig,
  request: RemoteEvalRequest,
): Promise<boolean> {
  const evaluators = filterEvaluators(getEvaluators(), config.filters);
  const entry = evaluators.find(
    (candidate) => candidate.evaluator.evalName === request.name,
  );
  if (!entry) {
    sendRunnerError(
      runner.sse,
      new Error(`Evaluator '${request.name}' not found`),
      404,
    );
    return false;
  }

  if (typeof runner.invoke !== "function") {
    sendRunnerError(
      runner.sse,
      new Error("Unable to run remote eval: invoke() is unavailable."),
      500,
    );
    return false;
  }

  try {
    const data = resolveRemoteData(request.data, runner.initDataset);
    const extraScores = (request.scores ?? []).map((score) =>
      makeRemoteScorer(
        runner.invoke as InvokeFunction,
        score,
        request.project_id,
      ),
    );
    const mergedScores = Array.isArray(entry.evaluator.scores)
      ? entry.evaluator.scores.concat(extraScores)
      : extraScores;

    const evaluator = {
      ...entry.evaluator,
      data,
      scores: mergedScores,
      ...(request.experiment_name
        ? { experimentName: request.experiment_name }
        : {}),
      ...(request.project_id ? { projectId: request.project_id } : {}),
    };

    const reporters = getReporters();
    const resolvedReporter = resolveReporter(entry.reporter, reporters);
    const options: EvalOptions = {
      ...(request.parameters ? { parameters: request.parameters } : {}),
      ...(request.parent !== undefined
        ? { parent: normalizeRemoteParent(request.parent) }
        : {}),
      ...(resolvedReporter !== undefined ? { reporter: resolvedReporter } : {}),
    };
    const result = await runner.runEval(
      entry.evaluator.projectName,
      evaluator,
      options,
    );
    const failingResults = result.results.filter(
      (row: { error?: unknown }) => row.error !== undefined,
    );
    return failingResults.length === 0 || resolvedReporter !== undefined;
  } catch (err) {
    sendRunnerError(runner.sse, err, inferRemoteErrorStatus(err));
    return false;
  }
}

function extractBtEvalMain(mod: unknown): BtEvalMain | null {
  if (!mod || typeof mod !== "object") {
    return null;
  }
  const candidate = mod as Record<string, unknown>;
  if (typeof candidate.btEvalMain === "function") {
    return candidate.btEvalMain as BtEvalMain;
  }
  const defaultExport = candidate.default as
    | Record<string, unknown>
    | undefined;
  if (defaultExport && typeof defaultExport.btEvalMain === "function") {
    return defaultExport.btEvalMain as BtEvalMain;
  }
  return null;
}

function collectBtEvalMains(mods: unknown[]): BtEvalMain[] {
  const mains: BtEvalMain[] = [];
  for (const mod of mods) {
    const main = extractBtEvalMain(mod);
    if (main) {
      mains.push(main);
    }
  }
  return mains;
}

function shouldDisableSendLogs(): boolean {
  return (
    process.env.BT_EVAL_NO_SEND_LOGS === "1" ||
    process.env.BT_EVAL_LOCAL === "1"
  );
}

function getEvaluatorName(
  evaluator: Record<string, unknown>,
  fallback: string,
): string {
  const candidate = evaluator.evalName ?? evaluator.name ?? evaluator.task;
  if (typeof candidate === "string" && candidate.length > 0) {
    return candidate;
  }
  return fallback;
}

function mergeEvalOptions(
  base: EvalOptions,
  overrides?: EvalOptions,
): EvalOptions {
  if (!overrides) {
    return base;
  }

  const merged: EvalOptions = { ...base, ...overrides };

  const baseProgress = base.progress as Record<string, unknown> | undefined;
  const overrideProgress = overrides.progress as
    | Record<string, unknown>
    | undefined;
  if (baseProgress || overrideProgress) {
    merged.progress = mergeProgress(baseProgress, overrideProgress);
  }

  const baseStream = base.stream as ((data: unknown) => void) | undefined;
  const overrideStream = overrides.stream as
    | ((data: unknown) => void)
    | undefined;
  if (baseStream || overrideStream) {
    merged.stream = mergeHandlers(baseStream, overrideStream);
  }

  const baseOnStart = base.onStart as ((data: unknown) => void) | undefined;
  const overrideOnStart = overrides.onStart as
    | ((data: unknown) => void)
    | undefined;
  if (baseOnStart || overrideOnStart) {
    merged.onStart = mergeHandlers(baseOnStart, overrideOnStart);
  }

  if (base.reporter && overrides.reporter === undefined) {
    merged.reporter = base.reporter;
  }

  return merged;
}

function mergeHandlers<Args extends unknown[]>(
  base?: (...args: Args) => void,
  override?: (...args: Args) => void,
): ((...args: Args) => void) | undefined {
  if (base && override) {
    return (...args: Args) => {
      base(...args);
      override(...args);
    };
  }
  return base ?? override;
}

function mergeProgress(
  base?: Partial<ProgressReporter>,
  override?: Partial<ProgressReporter>,
): ProgressReporter | undefined {
  if (!base) {
    return override as ProgressReporter | undefined;
  }
  if (!override) {
    return base as ProgressReporter;
  }
  const noopName = (_name: string) => {};
  const noopStart = (_name: string, _total: number) => {};
  return {
    start:
      mergeHandlers(base.start, override.start) ??
      base.start ??
      override.start ??
      noopStart,
    stop:
      mergeHandlers(base.stop, override.stop) ??
      base.stop ??
      override.stop ??
      noopName,
    increment:
      mergeHandlers(base.increment, override.increment) ??
      base.increment ??
      override.increment ??
      noopName,
    setTotal:
      mergeHandlers(base.setTotal, override.setTotal) ??
      base.setTotal ??
      override.setTotal ??
      noopStart,
  };
}

async function createEvalRunner(config: RunnerConfig): Promise<EvalRunner> {
  const braintrust = await loadBraintrust();
  const Eval = braintrust.Eval;
  if (typeof Eval !== "function") {
    throw new Error("Unable to load Eval() from braintrust package.");
  }
  const login = braintrust.login;
  const initDataset = braintrust.initDataset;
  const invoke = braintrust.invoke;

  const sse = createSseWriter();
  const noSendLogs = shouldDisableSendLogs();

  const makeEvalOptions = (
    evaluatorName: string,
    overrides?: EvalOptions,
  ): EvalOptions | undefined => {
    let base: EvalOptions = {};
    if (noSendLogs) {
      base.noSendLogs = true;
    }
    if (sse) {
      base = {
        ...base,
        reporter: {
          name: "bt-silent-reporter",
          reportEval: () => true,
          reportRun: () => true,
        },
        progress: createEvalProgressReporter(sse, evaluatorName),
        stream: (data: unknown) => {
          sse.send("progress", data);
        },
        onStart: (metadata: unknown) => {
          sse.send("start", metadata);
        },
      };
    }

    if (!overrides) {
      return Object.keys(base).length === 0 ? undefined : base;
    }
    return mergeEvalOptions(base, overrides);
  };

  const runEval = async (
    projectName: string,
    evaluator: Record<string, unknown>,
    options?: EvalOptions,
  ) => {
    globalThis._lazy_load = false;
    const evaluatorName = getEvaluatorName(evaluator, projectName);
    const opts = makeEvalOptions(evaluatorName, options);
    const result = await Eval(projectName, evaluator, opts);
    const failingResults = result.results.filter(
      (r: { error?: unknown }) => r.error !== undefined,
    );
    if (failingResults.length > 0 && sse) {
      sendConsole(
        sse,
        `Evaluator ${evaluatorName} failed with ${failingResults.length} error${failingResults.length === 1 ? "" : "s"}.`,
      );
    }
    if (sse) {
      sse.send("summary", result.summary);
    } else if (config.jsonl) {
      console.log(JSON.stringify(result.summary));
    }
    return result;
  };

  const runRegisteredEvals = async (evaluators: EvaluatorEntry[]) => {
    const reporters = getReporters();
    const runEntry = async (entry: EvaluatorEntry): Promise<boolean> => {
      try {
        const resolvedReporter = resolveReporter(entry.reporter, reporters);
        const options =
          resolvedReporter !== undefined
            ? { reporter: resolvedReporter }
            : undefined;
        const result = await runEval(
          entry.evaluator.projectName,
          entry.evaluator,
          options,
        );
        const failingResults = result.results.filter(
          (r: { error?: unknown }) => r.error !== undefined,
        );
        if (failingResults.length > 0 && resolvedReporter === undefined) {
          return false;
        }
        return true;
      } catch (err) {
        if (sse) {
          sse.send("error", serializeError(err));
        } else {
          console.error(err);
        }
        return false;
      }
    };

    if (config.terminateOnFailure) {
      for (const entry of evaluators) {
        const ok = await runEntry(entry);
        if (!ok) {
          return false;
        }
      }
      return true;
    }

    const results = await Promise.all(
      evaluators.map((entry) => runEntry(entry)),
    );
    return results.every(Boolean);
  };

  const finish = (ok: boolean) => {
    if (sse) {
      sse.send("dependencies", { files: collectDependencyFiles() });
      sse.send("done", "");
      sse.close();
    }
    if (!ok) {
      process.exitCode = 1;
    }
  };

  return {
    Eval,
    initDataset,
    invoke,
    sse,
    login,
    runEval,
    runRegisteredEvals,
    makeEvalOptions,
    finish,
    noSendLogs,
  };
}

async function main() {
  const config = readRunnerConfig();
  const files = process.argv.slice(2);
  if (files.length === 0) {
    console.error("No eval files provided.");
    process.exit(1);
  }

  installDependencyTracking();
  const normalized = normalizeFiles(files);
  for (const file of normalized) {
    maybeRecordDependency(file);
  }
  collectStaticLocalDependencies(normalized);
  ensureBraintrustAvailable();
  const braintrust = await loadBraintrust();
  propagateInheritedBraintrustState(braintrust);
  initRegistry();
  const modules = await loadFiles(normalized);
  const btEvalMains = collectBtEvalMains(modules);

  const runner = await createEvalRunner(config);
  if (!runner.noSendLogs && typeof runner.login === "function") {
    try {
      await runner.login({});
    } catch (err) {
      if (runner.sse) {
        runner.sse.send("error", serializeError(err));
      } else {
        console.error(err);
      }
      runner.finish(false);
      return;
    }
  }
  const context: BtEvalContext = {
    Eval: runner.Eval,
    runEval: runner.runEval,
    runRegisteredEvals: () =>
      runner.runRegisteredEvals(
        filterEvaluators(getEvaluators(), config.filters),
      ),
    makeEvalOptions: runner.makeEvalOptions,
    sendConsole: (message: string, stream?: "stdout" | "stderr") => {
      sendConsole(runner.sse, message, stream);
    },
    sendEvent: (event: string, data: unknown) => {
      if (runner.sse) {
        runner.sse.send(event, data);
      }
    },
  };

  let ok = true;
  try {
    const discoveredEvaluators = getEvaluators();
    const filteredEvaluators = filterEvaluators(
      discoveredEvaluators,
      config.filters,
    );
    if (config.remoteListJson) {
      const definitions = await buildEvaluatorDefinitions(filteredEvaluators);
      console.log(JSON.stringify(definitions));
      return;
    }

    if (config.remoteRequest) {
      ok = await runRemoteEvalRequest(runner, config, config.remoteRequest);
      return;
    }

    if (config.list) {
      for (const entry of filteredEvaluators) {
        console.log(entry.evaluator.evalName);
      }
      return;
    }

    if (btEvalMains.length > 0) {
      globalThis._lazy_load = false;
      for (const main of btEvalMains) {
        try {
          await main(context);
        } catch (err) {
          ok = false;
          if (runner.sse) {
            runner.sse.send("error", serializeError(err));
          } else {
            console.error(err);
          }
        }
      }
    } else {
      if (discoveredEvaluators.length === 0) {
        console.error("No evaluators found. Did you call Eval() in the file?");
        process.exit(1);
      }
      ok = await runner.runRegisteredEvals(filteredEvaluators);
    }
  } finally {
    collectRequireCacheDependencies();
    await collectDenoInfoDependencies(normalized);
    runner.finish(ok);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
