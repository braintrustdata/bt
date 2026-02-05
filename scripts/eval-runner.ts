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
  try {
    const require = createRequire(process.cwd() + "/");
    require.resolve("braintrust");
  } catch {
    const message =
      "Unable to resolve the `braintrust` package. " +
      "Please install it in your project (e.g. `pnpm add braintrust` or `npm install braintrust`).";
    throw new Error(message);
  }
}

async function loadBraintrust() {
  const require = createRequire(process.cwd() + "/");
  const resolved = require.resolve("braintrust");
  const moduleUrl = pathToFileURL(resolved).href;
  const mod = await import(moduleUrl);
  return (mod as any).default ?? mod;
}

async function loadFiles(files: string[]) {
  const require = createRequire(import.meta.url);
  for (const file of files) {
    const fileUrl = pathToFileURL(file).href;
    try {
      await import(fileUrl);
    } catch (err) {
      if (shouldTryRequire(file, err)) {
        try {
          require(file);
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

async function runEvals(evaluators: EvaluatorEntry[]) {
  (globalThis as any)._lazy_load = false;

  const braintrust = await loadBraintrust();
  const Eval =
    (braintrust as any).Eval ??
    ((braintrust as any).default && (braintrust as any).default.Eval);
  if (typeof Eval !== "function") {
    throw new Error("Unable to load Eval() from braintrust package.");
  }

  const sse = createSseWriter();
  let ok = true;
  for (const entry of evaluators) {
    try {
      const opts = sse
        ? {
            reporter: {
              name: "bt-silent-reporter",
              reportEval: () => true,
              reportRun: () => true,
            },
            progress: createEvalProgressReporter(sse, entry.evaluator.evalName),
            stream: (data: unknown) => {
              sse.send("progress", data);
            },
            onStart: (metadata: unknown) => {
              sse.send("start", metadata);
            },
          }
        : undefined;

      const result = await Eval(
        entry.evaluator.projectName,
        entry.evaluator as any,
        opts as any,
      );

      const failingResults = result.results.filter(
        (r: { error?: unknown }) => r.error !== undefined,
      );
      if (failingResults.length > 0) {
        ok = false;
        if (sse) {
          sendConsole(
            sse,
            `Evaluator ${entry.evaluator.evalName} failed with ${failingResults.length} error${failingResults.length === 1 ? "" : "s"}.`,
          );
        }
      }

      if (sse) {
        sse.send("summary", result.summary);
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

  if (sse) {
    sse.send("done", "");
    sse.close();
  }

  if (!ok) {
    process.exitCode = 1;
  }
}

async function main() {
  const files = process.argv.slice(2);
  if (files.length === 0) {
    console.error("No eval files provided.");
    process.exit(1);
  }

  const normalized = normalizeFiles(files);
  initRegistry();
  ensureBraintrustAvailable();
  await loadFiles(normalized);
  const evaluators = getEvaluators();

  if (evaluators.length === 0) {
    console.error("No evaluators found. Did you call Eval() in the file?");
    process.exit(1);
  }

  await runEvals(evaluators);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
