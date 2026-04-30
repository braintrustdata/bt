import { createRequire } from "node:module";
import fs from "node:fs";
import { pathToFileURL } from "node:url";
import path from "node:path";

type PipelineSource = {
  projectName?: string;
  projectId?: string;
  orgName?: string;
  filter?: string;
  scope?: "span" | "trace";
  limit?: number;
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
    candidate: HydratedCandidate,
    context: { pipeline: DatasetPipelineDefinition },
  ) => unknown | Promise<unknown>;
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

function writeResponse(value: unknown): void {
  process.stdout.write(`${JSON.stringify(value)}\n`);
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
): PipelineSource {
  if (!isObject(pipeline.source)) {
    throw new Error("Dataset pipeline source is required.");
  }
  return pipeline.source;
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
  sourceProjectId: string,
  refs: unknown[],
): Promise<HydratedCandidate[]> {
  requireBraintrustRuntime(braintrust);
  const source = requirePipelineSource(pipeline);
  const state = await stateForOrg(braintrust, source.orgName);
  return refs.map((ref) => {
    const rootSpanId = refRootSpanId(ref);
    const id = refSpanRowId(ref as DiscoveryRef);
    return {
      trace: new braintrust.LocalTrace!({
        objectType: "project_logs",
        objectId: sourceProjectId,
        rootSpanId,
        state,
      }),
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
  sourceProjectId: string,
  refs: unknown[],
  maxConcurrency = 16,
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
    sourceProjectId,
    refs,
  );
  const transformedRows: unknown[][] = new Array(candidates.length);
  let nextIndex = 0;

  async function worker() {
    while (nextIndex < candidates.length) {
      const index = nextIndex++;
      const candidate = candidates[index];
      const result = await pipeline.transform!(candidate, { pipeline });
      const rows = normalizeTransformResult(result);
      transformedRows[index] = rows.map((row, rowIndex) =>
        withPipelineDefaults(
          row,
          candidate,
          rows.length > 1 ? rowIndex : undefined,
        ),
      );
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

  if (stage === "inspect") {
    writeResponse({
      name: pipeline.name,
      source: pipeline.source,
      target: pipeline.target,
    });
  } else if (stage === "transform") {
    const request = await readRequest();
    const refs = requireArrayField(request, "refs");
    const sourceProjectId = requireStringField(request, "sourceProjectId");
    const rows = await transformRefs(
      braintrust,
      pipeline,
      sourceProjectId,
      refs,
      optionalPositiveIntegerField(request, "maxConcurrency"),
    );
    writeResponse({ candidates: refs.length, rowCount: rows.length, rows });
  } else {
    throw new Error(`Unsupported dataset pipeline stage: ${stage}`);
  }
}

main().catch((err) => {
  console.error(err instanceof Error ? err.stack || err.message : String(err));
  process.exit(1);
});
