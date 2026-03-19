import { createRequire } from "node:module";
import fs from "node:fs";
import path from "node:path";
import { pathToFileURL } from "node:url";

export type JsonPrimitive = string | number | boolean | null;
export type JsonArray = JsonValue[];
export type JsonObject = { [key: string]: JsonValue };
export type JsonValue = JsonPrimitive | JsonArray | JsonObject;

export type ProjectSelector = {
  project_id?: string;
  project_name?: string;
};

export type ProjectRef = {
  id?: string;
  name?: string;
};

export type EvaluatorDefinition = {
  evalName: string;
  projectName: string;
  data?: unknown;
  trialCount?: unknown;
  maxConcurrency?: unknown;
  experimentName?: unknown;
} & Record<string, unknown>;

export type EvaluatorEntry = {
  evaluator: EvaluatorDefinition;
  reporter?: unknown;
};

export type BraintrustModule = {
  Eval?: (...args: unknown[]) => unknown;
  login?: (...args: unknown[]) => Promise<unknown>;
  initDataset?: (...args: unknown[]) => unknown;
  invoke?: (...args: unknown[]) => Promise<unknown>;
  _internalGetGlobalState?: () => unknown;
  default?: BraintrustModule;
};

export type GlobalEvals = {
  functions: unknown[];
  prompts: unknown[];
  parameters: unknown[];
  evaluators: Record<string, EvaluatorEntry>;
  reporters: Record<string, unknown>;
};

export type EvalFilter = {
  path: string[];
  pattern: RegExp;
};

export type SerializedEvalFilter = {
  path: string[];
  pattern: string;
};

declare global {
  // eslint-disable-next-line no-var
  var _evals: GlobalEvals | undefined;
  // eslint-disable-next-line no-var
  var _lazy_load: boolean | undefined;
  // eslint-disable-next-line no-var
  var __inherited_braintrust_state: unknown;
}

export function asProjectSelector(
  project: ProjectRef | undefined,
): ProjectSelector {
  if (!project) {
    return {};
  }

  if (typeof project.id === "string" && project.id.trim().length > 0) {
    return { project_id: project.id };
  }

  if (typeof project.name === "string" && project.name.trim().length > 0) {
    return { project_name: project.name };
  }

  return {};
}

export function selectorToProjectId(selector: ProjectSelector): string {
  if (
    typeof selector.project_id === "string" &&
    selector.project_id.trim().length > 0
  ) {
    return selector.project_id;
  }

  if (
    typeof selector.project_name === "string" &&
    selector.project_name.trim().length > 0
  ) {
    return `name:${selector.project_name}`;
  }

  return "";
}

export function isJsonObject(
  value: JsonValue | undefined,
): value is JsonObject {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function toJsonValue(input: JsonValue): JsonValue {
  if (Array.isArray(input)) {
    return input.map((item) => toJsonValue(item));
  }

  if (input !== null && typeof input === "object") {
    const out: JsonObject = {};
    for (const [key, value] of Object.entries(input)) {
      if (
        value === null ||
        typeof value === "string" ||
        typeof value === "number" ||
        typeof value === "boolean"
      ) {
        out[key] = value;
      } else if (Array.isArray(value)) {
        out[key] = value.map((entry) => toJsonValue(entry));
      } else if (typeof value === "object") {
        out[key] = toJsonValue(value as JsonObject);
      }
    }
    return out;
  }

  return input;
}

export function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

export function normalizeFiles(files: string[]): string[] {
  return files.map((file) => path.resolve(process.cwd(), file));
}

export function envFlag(name: string): boolean {
  const value = process.env[name];
  if (!value) {
    return false;
  }
  const normalized = value.toLowerCase();
  return !["0", "false", "no", "off", ""].includes(normalized);
}

export function serializeJSONWithPlainString(value: unknown): string {
  if (typeof value === "string") {
    return value;
  }
  return JSON.stringify(value);
}

export function parseSerializedFilters(
  serialized: string | undefined,
): EvalFilter[] {
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

export function formatError(err: unknown): string {
  if (err instanceof Error) {
    return err.message;
  }
  return String(err);
}

export function initRegistry() {
  globalThis._evals = {
    functions: [],
    prompts: [],
    parameters: [],
    evaluators: {},
    reporters: {},
  };
  globalThis._lazy_load = true;
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

export function resolveBraintrustPath(files: string[]): string {
  const normalizedFiles = normalizeFiles(files);
  for (const file of normalizedFiles) {
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

export async function loadBraintrust(
  files: string[],
): Promise<BraintrustModule> {
  const cjsPath = resolveBraintrustPath(files);
  const cjsUrl = pathToFileURL(cjsPath).href;

  try {
    const mod: unknown = await import(cjsUrl);
    return normalizeBraintrustModule(mod);
  } catch {}

  const esmPath = cjsPath.replace(/\.js$/, ".mjs");
  if (esmPath !== cjsPath && fs.existsSync(esmPath)) {
    try {
      const mod: unknown = await import(pathToFileURL(esmPath).href);
      return normalizeBraintrustModule(mod);
    } catch {}
  }

  const require = createRequire(cjsUrl);
  const mod: unknown = require(cjsPath);
  return normalizeBraintrustModule(mod);
}

export type ParseParentFunction = (parent: unknown) => string | undefined;

function extractParseParent(mod: unknown): ParseParentFunction | null {
  if (!isObject(mod)) {
    return null;
  }
  const candidate = Reflect.get(mod, "parseParent");
  if (typeof candidate === "function") {
    return candidate as ParseParentFunction;
  }
  const defaultExport = Reflect.get(mod, "default");
  if (isObject(defaultExport)) {
    const fromDefault = Reflect.get(defaultExport, "parseParent");
    if (typeof fromDefault === "function") {
      return fromDefault as ParseParentFunction;
    }
  }
  return null;
}

export function loadBraintrustUtilParseParent(
  files: string[],
): ParseParentFunction | null {
  const braintrustPath = resolveBraintrustPath(files);
  const requireFromBraintrust = createRequire(
    pathToFileURL(braintrustPath).href,
  );
  try {
    const utilMod: unknown = requireFromBraintrust("braintrust/util");
    return extractParseParent(utilMod);
  } catch {
    return null;
  }
}

function extractGlobalStateGetter(mod: unknown): (() => unknown) | null {
  if (!isObject(mod)) {
    return null;
  }
  const candidate = Reflect.get(mod, "_internalGetGlobalState");
  if (typeof candidate === "function") {
    return candidate as () => unknown;
  }
  const defaultExport = Reflect.get(mod, "default");
  if (isObject(defaultExport)) {
    const fromDefault = Reflect.get(defaultExport, "_internalGetGlobalState");
    if (typeof fromDefault === "function") {
      return fromDefault as () => unknown;
    }
  }
  return null;
}

export function getBraintrustStateGetter(
  braintrust: BraintrustModule,
): (() => unknown) | null {
  return extractGlobalStateGetter(braintrust);
}

export function propagateInheritedBraintrustState(braintrust: BraintrustModule) {
  const getter = getBraintrustStateGetter(braintrust);
  if (!getter) {
    return;
  }
  const state = getter();
  if (state !== undefined && state !== null) {
    globalThis.__inherited_braintrust_state = state;
  }
}

export async function loadFiles(files: string[]): Promise<unknown[]> {
  const modules: unknown[] = [];
  const forceEsm = envFlag("BT_EVAL_FORCE_ESM");
  const isViteNode = process.env.BT_EVAL_RUNNER_KIND === "vite-node";
  for (const file of files) {
    const fileUrl = pathToFileURL(file).href;
    const isTypeScript = file.endsWith(".ts") || file.endsWith(".tsx");
    const preferRequire =
      !forceEsm &&
      !(isViteNode && isTypeScript) &&
      (isTypeScript || file.endsWith(".cjs"));

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
  if (envFlag("BT_EVAL_FORCE_ESM")) {
    return false;
  }
  if (process.env.BT_EVAL_RUNNER_KIND === "vite-node") {
    return false;
  }
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

export function getEvaluators(): EvaluatorEntry[] {
  const evals = globalThis._evals;
  if (!evals || !evals.evaluators) {
    return [];
  }
  return Object.values(evals.evaluators) as EvaluatorEntry[];
}

export function getReporters(): Record<string, unknown> {
  const evals = globalThis._evals;
  if (!evals || !evals.reporters) {
    return {};
  }
  return evals.reporters as Record<string, unknown>;
}

export function evaluateFilter(
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

export function filterEvaluators(
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

export function callEvaluatorData(
  data: unknown,
): { data: unknown; baseExperiment: string | undefined } {
  const dataResult = typeof data === "function" ? (data as () => unknown)() : data;
  let baseExperiment: string | undefined = undefined;
  if (
    isObject(dataResult) &&
    Reflect.get(dataResult, "_type") === "BaseExperiment" &&
    typeof Reflect.get(dataResult, "name") === "string"
  ) {
    baseExperiment = Reflect.get(dataResult, "name") as string;
  }
  return { data: dataResult, baseExperiment };
}

export function toAsyncIterable<T>(value: unknown): AsyncIterable<T> {
  if (
    typeof value === "object" &&
    value !== null &&
    Symbol.asyncIterator in value &&
    typeof (value as AsyncIterable<T>)[Symbol.asyncIterator] === "function"
  ) {
    return value as AsyncIterable<T>;
  }
  if (
    typeof value === "object" &&
    value !== null &&
    Symbol.iterator in value &&
    typeof (value as Iterable<T>)[Symbol.iterator] === "function"
  ) {
    const iterable = value as Iterable<T>;
    return (async function* () {
      for (const item of iterable) {
        yield item;
      }
    })();
  }
  throw new Error(
    "Evaluator data must be an array, iterable, or async iterable",
  );
}
