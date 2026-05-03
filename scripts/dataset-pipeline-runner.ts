import { createRequire } from "node:module";
import fs from "node:fs";
import net from "node:net";
import { pathToFileURL } from "node:url";
import path from "node:path";

type PipelineSource = {
  projectName?: string;
  projectId?: string;
  orgName?: string;
  filter?: string;
  scope?: "span" | "trace";
};

type PipelineTarget = {
  projectName?: string;
  projectId?: string;
  orgName?: string;
  datasetName?: string;
  description?: string;
  metadata?: Record<string, unknown>;
};

type DatasetPipelineDefinition = {
  name?: string;
  source?: PipelineSource;
  target?: PipelineTarget;
  transform?: (
    args: DatasetPipelineTransformArgs,
  ) => unknown | Promise<unknown>;
};

type DatasetPipelineTransformArgs = {
  input?: unknown;
  output?: unknown;
  expected?: unknown;
  metadata?: unknown;
  trace: unknown;
};

type BraintrustModule = {
  DatasetPipeline?: (
    definition: DatasetPipelineDefinition,
  ) => DatasetPipelineDefinition;
  getRegisteredDatasetPipelines?: () => DatasetPipelineDefinition[];
  isDatasetPipelineDefinition?: (
    value: unknown,
  ) => value is DatasetPipelineDefinition;
  LocalTrace?: new (options: {
    objectType: "project_logs";
    objectId: string;
    rootSpanId: string;
    state: unknown;
  }) => unknown;
  _internalGetGlobalState?: () => BraintrustState;
  loginToState?: (options: { orgName: string }) => Promise<BraintrustState>;
  default?: BraintrustModule;
};

type BraintrustState = {
  loggedIn?: boolean;
  orgName?: string;
  login: (options: Record<string, unknown>) => Promise<BraintrustState>;
};

type DiscoveryRef = {
  root_span_id?: unknown;
  id?: unknown;
};

type HydratedCandidate = {
  trace: unknown;
  id?: string;
  origin?: {
    object_type: "project_logs";
    object_id: string;
    id: string;
  };
};

type Stage = "inspect" | "transform";

type SseWriter = {
  send: (event: string, payload: unknown) => void;
  close: () => void;
};

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function normalizeBraintrustModule(value: unknown): BraintrustModule {
  if (isObject(value) && "default" in value && isObject(value.default)) {
    return value.default as BraintrustModule;
  }
  if (isObject(value)) {
    return value as BraintrustModule;
  }
  throw new Error("Unable to load braintrust module.");
}

function resolveBraintrustPath(pipelineFile: string): string {
  const file = path.resolve(process.cwd(), pipelineFile);
  try {
    const require = createRequire(pathToFileURL(file).href);
    return require.resolve("braintrust");
  } catch {}

  try {
    const require = createRequire(process.cwd() + "/");
    return require.resolve("braintrust");
  } catch {
    throw new Error(
      "Unable to resolve the `braintrust` package. Please install it in your project.",
    );
  }
}

async function loadBraintrust(pipelineFile: string): Promise<BraintrustModule> {
  const cjsPath = resolveBraintrustPath(pipelineFile);
  const cjsUrl = pathToFileURL(cjsPath).href;

  try {
    return normalizeBraintrustModule(await import(cjsUrl));
  } catch {}

  const esmPath = cjsPath.replace(/\.js$/, ".mjs");
  if (esmPath !== cjsPath && fs.existsSync(esmPath)) {
    try {
      return normalizeBraintrustModule(
        await import(pathToFileURL(esmPath).href),
      );
    } catch {}
  }

  const require = createRequire(cjsUrl);
  return normalizeBraintrustModule(require(cjsPath));
}

async function loadPipelineFile(file: string): Promise<unknown> {
  const absolute = path.resolve(process.cwd(), file);
  const fileUrl = pathToFileURL(absolute).href;
  try {
    return await import(fileUrl);
  } catch (importErr) {
    try {
      const require = createRequire(fileUrl);
      return require(absolute);
    } catch (requireErr) {
      throw new Error(
        `Failed to load ${file}: import failed with ${formatError(importErr)}; require failed with ${formatError(requireErr)}`,
      );
    }
  }
}

function formatError(err: unknown): string {
  return err instanceof Error ? err.message : String(err);
}

function collectPipelines(
  braintrust: BraintrustModule,
  loadedModule: unknown,
): DatasetPipelineDefinition[] {
  const pipelines = new Set<DatasetPipelineDefinition>();
  const isPipeline = (value: unknown): value is DatasetPipelineDefinition =>
    (braintrust.isDatasetPipelineDefinition?.(value) ?? false) ||
    (isObject(value) &&
      isObject(value.source) &&
      isObject(value.target) &&
      typeof value.transform === "function");

  for (const pipeline of braintrust.getRegisteredDatasetPipelines?.() ?? []) {
    pipelines.add(pipeline);
  }

  if (isObject(loadedModule)) {
    for (const value of Object.values(loadedModule)) {
      if (isPipeline(value)) {
        pipelines.add(value);
      }
    }
  }

  if (isPipeline(loadedModule)) {
    pipelines.add(loadedModule);
  }

  return [...pipelines];
}

function selectPipeline(
  pipelines: DatasetPipelineDefinition[],
  name: string | undefined,
): DatasetPipelineDefinition {
  if (name) {
    const matches = pipelines.filter((pipeline) => pipeline.name === name);
    if (matches.length === 0) {
      throw new Error(
        `No dataset pipeline named ${JSON.stringify(name)} found.`,
      );
    }
    if (matches.length > 1) {
      throw new Error(
        `Multiple dataset pipelines named ${JSON.stringify(name)} found.`,
      );
    }
    return matches[0];
  }

  if (pipelines.length === 0) {
    throw new Error(
      "No dataset pipelines found. Did you call DatasetPipeline()?",
    );
  }
  if (pipelines.length > 1) {
    const names = pipelines
      .map((pipeline) => pipeline.name ?? "<unnamed>")
      .join(", ");
    throw new Error(
      `Multiple dataset pipelines found (${names}). Pass --name.`,
    );
  }
  return pipelines[0];
}

function parseStage(): Stage {
  const value = process.env.BT_DATASET_PIPELINE_STAGE;
  if (value === "inspect" || value === "transform") {
    return value;
  }
  throw new Error("BT_DATASET_PIPELINE_STAGE must be inspect or transform.");
}

async function readRequest(): Promise<unknown> {
  const chunks: Buffer[] = [];
  for await (const chunk of process.stdin) {
    chunks.push(Buffer.isBuffer(chunk) ? chunk : Buffer.from(String(chunk)));
  }
  const text = Buffer.concat(chunks).toString("utf8").trim();
  return text.length > 0 ? JSON.parse(text) : {};
}

function writeResponse(value: unknown, sse: SseWriter | null): void {
  if (sse) {
    sse.send("response", value);
    sse.close();
  } else {
    process.stdout.write(`${JSON.stringify(value)}\n`);
  }
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
  const sock = process.env.BT_DATASET_PIPELINE_SSE_SOCK;
  if (!sock) {
    return null;
  }
  const socket = net.createConnection({ path: sock });
  socket.on("error", (err) => {
    console.error(
      `Failed to connect to dataset pipeline socket: ${
        err instanceof Error ? err.message : String(err)
      }`,
    );
  });
  return {
    send: (event: string, payload: unknown) => {
      if (!socket.writable) {
        return;
      }
      const data =
        typeof payload === "string" ? payload : JSON.stringify(payload);
      socket.write(serializeSseEvent({ event, data }));
    },
    close: () => {
      socket.end();
    },
  };
}

function writeProgress(sse: SseWriter | null, rows: number): void {
  if (!sse) {
    return;
  }
  sse.send("progress", {
    type: "dataset_pipeline_progress",
    kind: "candidate",
    rows,
  });
}

function requireArrayField(request: unknown, field: string): unknown[] {
  if (!isObject(request) || !Array.isArray(request[field])) {
    throw new Error(`Request field ${field} must be an array.`);
  }
  return request[field] as unknown[];
}

function requireStringField(request: unknown, field: string): string {
  if (!isObject(request) || typeof request[field] !== "string") {
    throw new Error(`Request field ${field} must be a string.`);
  }
  return request[field] as string;
}

function optionalPositiveIntegerField(
  request: unknown,
  field: string,
): number | undefined {
  if (!isObject(request) || request[field] === undefined) {
    return undefined;
  }
  const value = request[field];
  if (!Number.isInteger(value) || (value as number) <= 0) {
    throw new Error(`Request field ${field} must be a positive integer.`);
  }
  return value as number;
}

function requirePipelineSource(
  pipeline: DatasetPipelineDefinition,
  sourceOverride?: PipelineSource,
): PipelineSource {
  if (!isObject(pipeline.source)) {
    throw new Error("Dataset pipeline source is required.");
  }
  return { ...pipeline.source, ...(sourceOverride ?? {}) };
}

function requireBraintrustRuntime(braintrust: BraintrustModule) {
  if (
    !braintrust.LocalTrace ||
    !braintrust._internalGetGlobalState ||
    !braintrust.loginToState
  ) {
    throw new Error(
      "The installed braintrust package does not include dataset pipeline runtime support.",
    );
  }
}

async function stateForOrg(
  braintrust: BraintrustModule,
  orgName: string | undefined,
): Promise<BraintrustState> {
  if (!braintrust._internalGetGlobalState || !braintrust.loginToState) {
    throw new Error("The installed braintrust package cannot authenticate.");
  }
  const state = braintrust._internalGetGlobalState();
  if (!orgName) {
    await state.login({});
    return state;
  }
  if (!state.loggedIn) {
    await state.login({ orgName });
    return state;
  }
  if (state.orgName === orgName) {
    return state;
  }
  return braintrust.loginToState({ orgName });
}

function refRootSpanId(ref: unknown): string {
  if (!isObject(ref) || typeof ref.root_span_id !== "string") {
    throw new Error("Discovery ref is missing root_span_id.");
  }
  return ref.root_span_id;
}

function refSpanRowId(ref: DiscoveryRef): string | undefined {
  return typeof ref.id === "string" ? ref.id : undefined;
}

async function hydrateDiscoveryRefs(
  braintrust: BraintrustModule,
  pipeline: DatasetPipelineDefinition,
  sourceOverride: PipelineSource | undefined,
  sourceProjectId: string,
  refs: unknown[],
): Promise<HydratedCandidate[]> {
  requireBraintrustRuntime(braintrust);
  const source = requirePipelineSource(pipeline, sourceOverride);
  const state = await stateForOrg(braintrust, source.orgName);
  const tracesByRootSpanId = new Map<string, unknown>();
  return refs.map((ref) => {
    const rootSpanId = refRootSpanId(ref);
    const id = refSpanRowId(ref as DiscoveryRef);
    let trace = tracesByRootSpanId.get(rootSpanId);
    if (!trace) {
      trace = new braintrust.LocalTrace!({
        objectType: "project_logs",
        objectId: sourceProjectId,
        rootSpanId,
        state,
      });
      tracesByRootSpanId.set(rootSpanId, trace);
    }
    return {
      trace,
      ...(id ? { id } : {}),
      ...(id
        ? {
            origin: {
              object_type: "project_logs" as const,
              object_id: sourceProjectId,
              id,
            },
          }
        : {}),
    };
  });
}

function spanAttr(row: unknown, name: string): unknown {
  return isObject(row) ? row[name] : undefined;
}

async function sourceRowForCandidate(
  candidate: HydratedCandidate,
): Promise<unknown | undefined> {
  if (!candidate.id) {
    return undefined;
  }
  const trace = candidate.trace;
  if (!isObject(trace) || typeof trace.getSpans !== "function") {
    throw new Error("Hydrated trace does not support getSpans().");
  }
  const spans = await trace.getSpans({ includeScorers: true });
  if (!Array.isArray(spans)) {
    throw new Error("Hydrated trace getSpans() did not return an array.");
  }
  const row = spans.find(
    (span) =>
      spanAttr(span, "id") === candidate.id ||
      spanAttr(span, "span_id") === candidate.id,
  );
  if (!row) {
    throw new Error(
      `Source span row ${JSON.stringify(candidate.id)} was not found in hydrated trace.`,
    );
  }
  return row;
}

async function transformArgsForCandidate(
  candidate: HydratedCandidate,
): Promise<DatasetPipelineTransformArgs> {
  const row = await sourceRowForCandidate(candidate);
  return {
    input: spanAttr(row, "input"),
    output: spanAttr(row, "output"),
    expected: spanAttr(row, "expected"),
    metadata: spanAttr(row, "metadata"),
    trace: candidate.trace,
  };
}

function normalizeTransformResult(result: unknown): unknown[] {
  if (result == null) {
    return [];
  }
  return Array.isArray(result) ? result : [result];
}

function candidateFallbackId(candidate: HydratedCandidate): string | undefined {
  if (candidate.id) {
    return candidate.id;
  }
  const trace = candidate.trace;
  if (
    isObject(trace) &&
    typeof trace.getConfiguration === "function" &&
    isObject(trace.getConfiguration())
  ) {
    const config = trace.getConfiguration() as Record<string, unknown>;
    return typeof config.root_span_id === "string"
      ? config.root_span_id
      : undefined;
  }
  return undefined;
}

function withPipelineDefaults(
  row: unknown,
  candidate: HydratedCandidate,
  rowIndex: number | undefined,
): unknown {
  if (!isObject(row)) {
    throw new Error("Dataset pipeline transform must return an object row.");
  }
  const fallbackId = candidateFallbackId(candidate);
  return {
    ...row,
    ...(row.id === undefined && fallbackId
      ? {
          id: rowIndex === undefined ? fallbackId : `${fallbackId}:${rowIndex}`,
        }
      : {}),
    ...(row.origin === undefined && candidate.origin
      ? { origin: candidate.origin }
      : {}),
  };
}

async function transformRefs(
  braintrust: BraintrustModule,
  pipeline: DatasetPipelineDefinition,
  sourceOverride: PipelineSource | undefined,
  sourceProjectId: string,
  refs: unknown[],
  maxConcurrency = 16,
  sse: SseWriter | null = null,
): Promise<unknown[]> {
  if (!Number.isInteger(maxConcurrency) || maxConcurrency <= 0) {
    throw new Error("maxConcurrency must be a positive integer.");
  }
  if (typeof pipeline.transform !== "function") {
    throw new Error("Dataset pipeline transform must be a function.");
  }
  const candidates = await hydrateDiscoveryRefs(
    braintrust,
    pipeline,
    sourceOverride,
    sourceProjectId,
    refs,
  );
  const transformedRows: unknown[][] = new Array(candidates.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < candidates.length) {
      const index = nextIndex++;
      const candidate = candidates[index];
      const args = await transformArgsForCandidate(candidate);
      const result = await pipeline.transform!(args);
      const rows = normalizeTransformResult(result);
      transformedRows[index] = rows.map((row, rowIndex) =>
        withPipelineDefaults(
          row,
          candidate,
          rows.length > 1 ? rowIndex : undefined,
        ),
      );
      writeProgress(sse, transformedRows[index].length);
    }
  }

  const workerCount = Math.min(maxConcurrency, Math.max(candidates.length, 1));
  await Promise.all(Array.from({ length: workerCount }, () => worker()));
  return transformedRows.flat();
}

async function main() {
  const pipelineFile = process.argv[2];
  if (!pipelineFile) {
    throw new Error("Pipeline file is required.");
  }

  const [braintrust, loadedModule] = await Promise.all([
    loadBraintrust(pipelineFile),
    loadPipelineFile(pipelineFile),
  ]);
  const pipeline = selectPipeline(
    collectPipelines(braintrust, loadedModule),
    process.env.BT_DATASET_PIPELINE_NAME || undefined,
  );
  const stage = parseStage();
  const sse = createSseWriter();

  if (stage === "inspect") {
    writeResponse(
      {
        name: pipeline.name,
        source: pipeline.source,
        target: pipeline.target,
      },
      sse,
    );
  } else if (stage === "transform") {
    const request = await readRequest();
    const refs = requireArrayField(request, "refs");
    const sourceProjectId = requireStringField(request, "sourceProjectId");
    const sourceOverride =
      isObject(request) && isObject(request.source)
        ? (request.source as PipelineSource)
        : undefined;
    const rows = await transformRefs(
      braintrust,
      pipeline,
      sourceOverride,
      sourceProjectId,
      refs,
      optionalPositiveIntegerField(request, "maxConcurrency"),
      sse,
    );
    writeResponse(
      { candidates: refs.length, rowCount: rows.length, rows },
      sse,
    );
  } else {
    throw new Error(`Unsupported dataset pipeline stage: ${stage}`);
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.stack || err.message : String(err));
  process.exit(1);
});
