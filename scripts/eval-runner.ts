import { createRequire } from "module";
import net from "net";
import path from "path";
import { fileURLToPath, pathToFileURL } from "url";

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

type BraintrustModule = {
  Eval?: EvalFunction;
  login?: LoginFunction;
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

declare global {
  // eslint-disable-next-line no-var
  var _evals: GlobalEvals | undefined;
  // eslint-disable-next-line no-var
  var _lazy_load: boolean | undefined;
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

const runtimeRequire = createRequire(
  path.join(process.cwd(), "__bt-eval-runner__.cjs"),
);
const fsMutable = runtimeRequire("fs") as typeof import("fs");
const moduleMutable = (() => {
  try {
    return runtimeRequire("node:module") as Record<string, unknown>;
  } catch {
    return {};
  }
})();

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
  const sock = process.env.BT_EVAL_SSE_SOCK;
  if (sock) {
    const socket = net.createConnection({ path: sock });
    socket.on("error", (err) => {
      console.error(`Failed to connect to SSE socket: ${err.message}`);
      process.exitCode = 1;
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

  const socket = net.createConnection({ host, port });
  socket.setNoDelay(true);

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

async function loadFiles(files: string[]): Promise<unknown[]> {
  const modules: unknown[] = [];
  for (const file of files) {
    const fileUrl = pathToFileURL(file).href;
    try {
      const mod = await import(fileUrl);
      modules.push(mod);
    } catch (err) {
      if (shouldTryRequire(file, err)) {
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
      throw err;
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

function serializeError(err: unknown) {
  if (err instanceof Error) {
    return { message: err.message, stack: err.stack };
  }
  return { message: String(err) };
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

async function createEvalRunner() {
  const braintrust = await loadBraintrust();
  const Eval = braintrust.Eval;
  if (typeof Eval !== "function") {
    throw new Error("Unable to load Eval() from braintrust package.");
  }
  const login = braintrust.login;

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
    }
    return result;
  };

  const runRegisteredEvals = async (evaluators: EvaluatorEntry[]) => {
    const results = await Promise.all(
      evaluators.map(async (entry) => {
        try {
          const options = entry.reporter
            ? { reporter: entry.reporter }
            : undefined;
          const result = await runEval(
            entry.evaluator.projectName,
            entry.evaluator,
            options,
          );
          const failingResults = result.results.filter(
            (r: { error?: unknown }) => r.error !== undefined,
          );
          return failingResults.length === 0;
        } catch (err) {
          if (sse) {
            sse.send("error", serializeError(err));
          } else {
            console.error(err);
          }
          return false;
        }
      }),
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
  await loadBraintrust();
  initRegistry();
  const modules = await loadFiles(normalized);
  const btEvalMains = collectBtEvalMains(modules);

  const runner = await createEvalRunner();
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
    runRegisteredEvals: () => runner.runRegisteredEvals(getEvaluators()),
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
      const evaluators = getEvaluators();
      if (evaluators.length === 0) {
        console.error("No evaluators found. Did you call Eval() in the file?");
        process.exit(1);
      }
      ok = await runner.runRegisteredEvals(evaluators);
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
