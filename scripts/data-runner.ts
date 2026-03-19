import net from "node:net";
import readline from "node:readline";

import {
  callEvaluatorData,
  formatError,
  getBraintrustStateGetter,
  getEvaluators,
  initRegistry,
  loadBraintrust,
  loadFiles,
  normalizeFiles,
  propagateInheritedBraintrustState,
  toAsyncIterable,
} from "./runner-common";

type StartMessage = {
  type: "start";
  name: string;
};

type ClientMessage =
  | StartMessage
  | { type: "next" }
  | { type: "close" };

type ServerMessage =
  | {
      type: "ready";
      evaluator_name: string;
      max_concurrency: number;
      experiment_name: string;
    }
  | { type: "row"; datum: unknown; trial_index: number }
  | { type: "eof" }
  | { type: "error"; message: string };

function writeMessage(socket: net.Socket, message: ServerMessage) {
  socket.write(`${JSON.stringify(message)}\n`);
}

function parseMessage(line: string): ClientMessage {
  const parsed = JSON.parse(line) as { type?: unknown; name?: unknown };
  if (parsed.type === "start") {
    if (typeof parsed.name !== "string" || parsed.name.length === 0) {
      throw new Error("Start request must include a non-empty evaluator name.");
    }
    return { type: "start", name: parsed.name };
  }
  if (parsed.type === "next" || parsed.type === "close") {
    return { type: parsed.type };
  }
  throw new Error(`Unsupported pull command '${String(parsed.type)}'`);
}

async function readMessage(
  lines: AsyncIterator<string>,
): Promise<ClientMessage | null> {
  const next = await lines.next();
  if (next.done) {
    return null;
  }
  return parseMessage(next.value);
}

function applyExtraArgsFromEnv() {
  const extraArgs: string[] = process.env.BT_EVAL_EXTRA_ARGS_JSON
    ? (JSON.parse(process.env.BT_EVAL_EXTRA_ARGS_JSON) as string[])
    : [];
  process.argv = [...process.argv.slice(0, 2), ...extraArgs];
}

function toPositiveInteger(value: unknown, fallback: number): number {
  const parsed = Number(value);
  if (Number.isFinite(parsed) && parsed > 0) {
    return Math.floor(parsed);
  }
  return fallback;
}

async function main() {
  const files = process.argv.slice(2);
  if (files.length === 0) {
    throw new Error("No eval files provided.");
  }
  const socketPath = process.env.BT_EVAL_PULL_SOCK;
  if (!socketPath) {
    throw new Error("Missing BT_EVAL_PULL_SOCK");
  }

  const normalized = normalizeFiles(files);
  const braintrust = await loadBraintrust(normalized);
  propagateInheritedBraintrustState(braintrust);
  initRegistry();
  applyExtraArgsFromEnv();
  await loadFiles(normalized);

  const socket = net.createConnection({ path: socketPath });
  const socketReady = new Promise<void>((resolve, reject) => {
    socket.once("connect", resolve);
    socket.once("error", reject);
  });
  await socketReady;

  const reader = readline.createInterface({
    input: socket,
    crlfDelay: Infinity,
  });
  const lines = reader[Symbol.asyncIterator]();

  try {
    const start = await readMessage(lines);
    if (!start) {
      return;
    }
    if (start.type !== "start") {
      throw new Error("Expected initial start command.");
    }

    const entry = getEvaluators().find(
      (candidate) => candidate.evaluator.evalName === start.name,
    );
    if (!entry) {
      writeMessage(socket, {
        type: "error",
        message: `Evaluator '${start.name}' not found`,
      });
      return;
    }

    const getState = getBraintrustStateGetter(braintrust);
    const state = getState ? getState() : undefined;
    const evaluator = {
      ...entry.evaluator,
      ...(state !== undefined && state !== null ? { state } : {}),
    };
    const { data: rawData } = callEvaluatorData(evaluator.data);
    const dataIterable = toAsyncIterable<unknown>(rawData);
    const iterator = dataIterable[Symbol.asyncIterator]();
    const trialCount = toPositiveInteger(evaluator.trialCount, 1);
    const maxConcurrency = toPositiveInteger(evaluator.maxConcurrency, 10);
    const experimentName =
      typeof evaluator.experimentName === "string" &&
      evaluator.experimentName.length > 0
        ? evaluator.experimentName
        : `${entry.evaluator.evalName}-${Date.now()}`;

    writeMessage(socket, {
      type: "ready",
      evaluator_name: entry.evaluator.evalName,
      max_concurrency: maxConcurrency,
      experiment_name: experimentName,
    });

    let currentDatum: unknown | undefined;
    let trialIndex = 0;
    while (true) {
      const message = await readMessage(lines);
      if (!message || message.type === "close") {
        return;
      }
      if (message.type !== "next") {
        throw new Error(`Unsupported pull command '${message.type}'`);
      }

      if (currentDatum === undefined) {
        const next = await iterator.next();
        if (next.done) {
          writeMessage(socket, { type: "eof" });
          continue;
        }
        currentDatum = next.value;
        trialIndex = 0;
      }

      writeMessage(socket, {
        type: "row",
        datum: currentDatum,
        trial_index: trialIndex,
      });

      trialIndex += 1;
      if (trialIndex >= trialCount) {
        currentDatum = undefined;
      }
    }
  } catch (err) {
    writeMessage(socket, {
      type: "error",
      message: formatError(err),
    });
  } finally {
    reader.close();
    socket.end();
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
