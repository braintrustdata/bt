import { createRequire } from "module";
import net from "net";
import path from "path";
import { pathToFileURL } from "url";

type EvaluatorEntry = {
  evaluator: {
    evalName: string;
    projectName: string;
  } & Record<string, unknown>;
  reporter?: unknown;
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

type BtEvalMain = (context: BtEvalContext) => void | Promise<void>;

type BtEvalContext = {
  Eval: (
    projectName: string,
    evaluator: unknown,
    options?: EvalOptions,
  ) => Promise<unknown>;
  runEval: (
    projectName: string,
    evaluator: Record<string, unknown>,
    options?: EvalOptions,
  ) => Promise<unknown>;
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

function normalizeFiles(files: string[]): string[] {
  return files.map((file) => path.resolve(process.cwd(), file));
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
  (globalThis as any)._evals = {
    functions: [],
    prompts: [],
    parameters: [],
    evaluators: {},
    reporters: {},
  };
  (globalThis as any)._lazy_load = true;
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
  const mod = await import(moduleUrl);
  return (mod as any).default ?? mod;
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
  const evals = (globalThis as any)._evals;
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
  return process.env.BT_EVAL_NO_SEND_LOGS === "1";
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
  const Eval =
    (braintrust as any).Eval ??
    ((braintrust as any).default && (braintrust as any).default.Eval);
  if (typeof Eval !== "function") {
    throw new Error("Unable to load Eval() from braintrust package.");
  }

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
    (globalThis as any)._lazy_load = false;
    const evaluatorName = getEvaluatorName(evaluator, projectName);
    const opts = makeEvalOptions(evaluatorName, options);
    const result = await Eval(projectName, evaluator as any, opts as any);
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
    let ok = true;
    for (const entry of evaluators) {
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
        if (failingResults.length > 0) {
          ok = false;
        }
      } catch (err) {
        ok = false;
        if (sse) {
          sse.send("error", serializeError(err));
        } else {
          console.error(err);
        }
      }
    }
    return ok;
  };

  const finish = (ok: boolean) => {
    if (sse) {
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
    runEval,
    runRegisteredEvals,
    makeEvalOptions,
    finish,
  };
}

async function main() {
  const files = process.argv.slice(2);
  if (files.length === 0) {
    console.error("No eval files provided.");
    process.exit(1);
  }

  const normalized = normalizeFiles(files);
  ensureBraintrustAvailable();
  await loadBraintrust();
  initRegistry();
  const modules = await loadFiles(normalized);
  const btEvalMains = collectBtEvalMains(modules);

  const runner = await createEvalRunner();
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
      (globalThis as any)._lazy_load = false;
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
    runner.finish(ok);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
