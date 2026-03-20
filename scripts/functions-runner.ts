import path from "node:path";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

import {
  asProjectSelector,
  isJsonObject,
  ProjectRef,
  selectorToProjectId,
  toJsonValue,
  type JsonObject,
  type JsonValue,
} from "./runner-common";

type Resolver = {
  resolve: (project: ProjectRef) => Promise<string>;
};

type CodeRegistryItem = {
  project?: ProjectRef;
  name?: string;
  slug?: string;
  description?: string;
  type?: string;
  functionType?: string;
  ifExists?: string;
  metadata?: JsonValue;
  tags?: unknown;
  parameters?: unknown;
  returns?: unknown;
  preview?: string;
};

type EventRegistryItem = {
  project?: ProjectRef;
  toFunctionDefinition?: (resolver: Resolver) => Promise<JsonObject>;
  name?: string;
  slug?: string;
  description?: string;
  ifExists?: string;
  metadata?: JsonValue;
  prompt?: JsonValue;
  toolFunctions?: LegacyToolFunction[];
};

type LegacyToolFunction = {
  type?: string;
  id?: string;
  name?: string;
  slug?: string;
  project?: ProjectRef;
  project_id?: string;
};

type CodeEntry = {
  kind: "code";
  project_id?: string;
  project_name?: string;
  name: string;
  slug: string;
  description?: string;
  function_type?: string;
  if_exists?: string;
  metadata?: JsonValue;
  tags?: string[];
  function_schema?: JsonValue;
  preview?: string;
  location: JsonValue;
};

type FunctionEventEntry = {
  kind: "function_event";
  project_id?: string;
  project_name?: string;
  event: JsonValue;
};

type ManifestFile = {
  source_file: string;
  entries: Array<CodeEntry | FunctionEventEntry>;
};

type Manifest = {
  runtime_context: {
    runtime: "node";
    version: string;
  };
  files: ManifestFile[];
};

type EvalRegistry = NonNullable<typeof globalThis._evals>;
type ZodToJsonSchemaFn = (schema: unknown) => unknown;
type ZodV4ToJsonSchemaFn = (
  schema: unknown,
  options?: { target?: string },
) => unknown;
type ZodSchemaSerializer = (schema: unknown) => JsonObject | undefined;

let moduleImportNonce = 0;
let zodSchemaSerializer: ZodSchemaSerializer | null | undefined;

const runtimeRequire: NodeRequire | null =
  typeof require === "function" ? require : null;

function safeCreateRequire(modulePath: string): NodeRequire | null {
  try {
    return createRequire(modulePath);
  } catch {
    return null;
  }
}

const localRequire =
  runtimeRequire ?? safeCreateRequire(path.join(process.cwd(), "package.json"));

function requireCandidates(): NodeRequire[] {
  const candidates: NodeRequire[] = [];
  if (localRequire) {
    candidates.push(localRequire);
  }
  const cwdRequire = safeCreateRequire(
    path.join(process.cwd(), "package.json"),
  );
  if (cwdRequire) {
    let exists = false;
    for (const candidate of candidates) {
      if (candidate === cwdRequire) {
        exists = true;
        break;
      }
    }
    if (!exists) {
      candidates.push(cwdRequire);
    }
  }
  return candidates;
}

function resolveWithBraintrustFallback<T>(
  moduleId: string,
  extract: (module: unknown) => T | null,
): T | null {
  const candidates = requireCandidates();
  for (const candidateRequire of candidates) {
    try {
      const converted = extract(candidateRequire(moduleId));
      if (converted) {
        return converted;
      }
    } catch {
      // Try the next location.
    }
  }

  for (const candidateRequire of candidates) {
    try {
      const braintrustPkg = candidateRequire.resolve("braintrust/package.json");
      const braintrustRequire = createRequire(braintrustPkg);
      const converted = extract(braintrustRequire(moduleId));
      if (converted) {
        return converted;
      }
    } catch {
      // Try the next location.
    }
  }

  return null;
}

function freshRegistry(): EvalRegistry {
  return {
    functions: [],
    prompts: [],
    parameters: [],
    evaluators: {},
    reporters: {},
  };
}

function currentRegistry(fallback: EvalRegistry): EvalRegistry {
  const registry = globalThis._evals;
  if (!registry) {
    return fallback;
  }

  return {
    functions: Array.isArray(registry.functions) ? registry.functions : [],
    prompts: Array.isArray(registry.prompts) ? registry.prompts : [],
    parameters: Array.isArray(registry.parameters) ? registry.parameters : [],
    evaluators:
      registry.evaluators !== null && typeof registry.evaluators === "object"
        ? registry.evaluators
        : {},
    reporters:
      registry.reporters !== null && typeof registry.reporters === "object"
        ? registry.reporters
        : {},
  };
}

function buildIsolatedImportUrl(absolutePath: string): string {
  const moduleUrl = pathToFileURL(absolutePath);
  // Force top-level evaluation for each input file, even if imported earlier
  // as a dependency while processing a previous input file.
  moduleUrl.searchParams.set("bt_runner_input_nonce", `${moduleImportNonce}`);
  moduleImportNonce += 1;
  return moduleUrl.href;
}

function isZodV4Schema(schema: unknown): boolean {
  return (
    schema !== null &&
    typeof schema === "object" &&
    "_zod" in schema &&
    (schema as { _zod?: unknown })._zod !== undefined
  );
}

function isLikelyZodSchema(schema: unknown): boolean {
  return (
    isZodV4Schema(schema) ||
    (schema !== null &&
      typeof schema === "object" &&
      "_def" in schema &&
      typeof (schema as { safeParse?: unknown }).safeParse === "function")
  );
}

function extractZodToJsonSchemaV3(module: unknown): ZodToJsonSchemaFn | null {
  if (typeof module === "function") {
    return module as ZodToJsonSchemaFn;
  }
  if (module && typeof module === "object") {
    const direct = (module as { zodToJsonSchema?: unknown }).zodToJsonSchema;
    if (typeof direct === "function") {
      return direct as ZodToJsonSchemaFn;
    }

    const defaultExport = (module as { default?: unknown }).default;
    if (typeof defaultExport === "function") {
      return defaultExport as ZodToJsonSchemaFn;
    }
    if (defaultExport && typeof defaultExport === "object") {
      const fromDefault = (defaultExport as { zodToJsonSchema?: unknown })
        .zodToJsonSchema;
      if (typeof fromDefault === "function") {
        return fromDefault as ZodToJsonSchemaFn;
      }
    }
  }
  return null;
}

function extractZodToJsonSchemaV4(module: unknown): ZodV4ToJsonSchemaFn | null {
  if (module && typeof module === "object") {
    const direct = (module as { toJSONSchema?: unknown }).toJSONSchema;
    if (typeof direct === "function") {
      return direct as ZodV4ToJsonSchemaFn;
    }

    const defaultExport = (module as { default?: unknown }).default;
    if (defaultExport && typeof defaultExport === "object") {
      const fromDefault = (defaultExport as { toJSONSchema?: unknown })
        .toJSONSchema;
      if (typeof fromDefault === "function") {
        return fromDefault as ZodV4ToJsonSchemaFn;
      }
    }
  }
  return null;
}

function loadZodSchemaSerializer(): ZodSchemaSerializer | null {
  if (zodSchemaSerializer !== undefined) {
    return zodSchemaSerializer;
  }

  const zodToJsonSchemaV3 = resolveWithBraintrustFallback(
    "zod-to-json-schema",
    extractZodToJsonSchemaV3,
  );
  const zodToJsonSchemaV4 = resolveWithBraintrustFallback(
    "zod/v4",
    extractZodToJsonSchemaV4,
  );

  if (!zodToJsonSchemaV3 && !zodToJsonSchemaV4) {
    zodSchemaSerializer = null;
    return zodSchemaSerializer;
  }

  zodSchemaSerializer = (schema: unknown): JsonObject | undefined => {
    try {
      const converted =
        isZodV4Schema(schema) && zodToJsonSchemaV4
          ? zodToJsonSchemaV4(schema, { target: "draft-7" })
          : zodToJsonSchemaV3
            ? zodToJsonSchemaV3(schema)
            : zodToJsonSchemaV4
              ? zodToJsonSchemaV4(schema, { target: "draft-7" })
              : undefined;
      const normalized = toJsonValue(converted as JsonValue);
      return isJsonObject(normalized) ? normalized : undefined;
    } catch {
      return undefined;
    }
  };

  return zodSchemaSerializer;
}

function schemaToJsonSchema(schema: unknown): JsonObject | undefined {
  if (schema === undefined || schema === null) {
    return undefined;
  }

  const serializer = loadZodSchemaSerializer();
  const zodSchema = isLikelyZodSchema(schema);
  if (serializer) {
    const converted = serializer(schema);
    if (converted) {
      return converted;
    }
  }

  if (zodSchema) {
    return undefined;
  }

  const normalizedSchema = toJsonValue(schema as JsonValue);
  return isJsonObject(normalizedSchema) ? normalizedSchema : undefined;
}

async function collectFunctionEvents(
  items: EventRegistryItem[],
  includeLegacyPrompts: boolean,
): Promise<FunctionEventEntry[]> {
  const entries: FunctionEventEntry[] = [];

  const resolver: Resolver = {
    resolve: async (project: ProjectRef): Promise<string> => {
      const selector = asProjectSelector(project);
      return selectorToProjectId(selector);
    },
  };

  for (const item of items) {
    if (!item.toFunctionDefinition) {
      if (includeLegacyPrompts) {
        const entry = await collectLegacyPromptEvent(item, resolver);
        if (entry) {
          entries.push(entry);
        }
      }
      continue;
    }

    const event = await item.toFunctionDefinition(resolver);
    const normalizedEvent = toJsonValue(event);
    if (!isJsonObject(normalizedEvent)) {
      continue;
    }

    const selector = asProjectSelector(item.project);
    const projectId =
      typeof selector.project_id === "string" ? selector.project_id : undefined;
    const projectName =
      typeof selector.project_name === "string"
        ? selector.project_name
        : undefined;

    entries.push({
      kind: "function_event",
      project_id: projectId,
      project_name: projectName,
      event: normalizedEvent,
    });
  }

  return entries;
}

async function collectLegacyPromptEvent(
  item: EventRegistryItem,
  resolver: Resolver,
): Promise<FunctionEventEntry | null> {
  if (typeof item.name !== "string" || typeof item.slug !== "string") {
    return null;
  }

  const normalizedPrompt = toJsonValue(item.prompt ?? {});
  if (!isJsonObject(normalizedPrompt)) {
    return null;
  }

  const promptData: JsonObject = { ...normalizedPrompt };
  const toolFunctions = Array.isArray(item.toolFunctions)
    ? item.toolFunctions
    : [];
  if (toolFunctions.length > 0) {
    const resolvedTools: JsonValue[] = [];
    for (const tool of toolFunctions) {
      const resolved = await resolveLegacyToolFunction(tool, resolver);
      if (resolved) {
        resolvedTools.push(resolved);
      }
    }
    if (resolvedTools.length > 0) {
      promptData.tool_functions = resolvedTools;
    }
  }

  const selector = asProjectSelector(item.project);
  const projectId =
    typeof selector.project_id === "string" ? selector.project_id : undefined;
  const projectName =
    typeof selector.project_name === "string"
      ? selector.project_name
      : undefined;

  const event: JsonObject = {
    name: item.name,
    slug: item.slug,
    description: typeof item.description === "string" ? item.description : "",
    function_data: {
      type: "prompt",
    },
    prompt_data: promptData,
  };
  if (typeof item.ifExists === "string") {
    event.if_exists = item.ifExists;
  }
  if (item.metadata !== undefined) {
    event.metadata = item.metadata;
  }

  return {
    kind: "function_event",
    project_id: projectId,
    project_name: projectName,
    event,
  };
}

async function resolveLegacyToolFunction(
  tool: LegacyToolFunction,
  resolver: Resolver,
): Promise<JsonObject | null> {
  if (
    typeof tool.slug === "string" &&
    tool.slug.length > 0 &&
    tool.project !== undefined
  ) {
    const projectId = await resolver.resolve(tool.project);
    if (projectId.length > 0) {
      return {
        type: "slug",
        project_id: projectId,
        slug: tool.slug,
      };
    }
  }

  const direct: JsonObject = {};
  if (typeof tool.type === "string") {
    direct.type = tool.type;
  }
  if (typeof tool.id === "string") {
    direct.id = tool.id;
  }
  if (typeof tool.name === "string") {
    direct.name = tool.name;
  }
  if (typeof tool.project_id === "string") {
    direct.project_id = tool.project_id;
  }
  if (typeof tool.slug === "string") {
    direct.slug = tool.slug;
  }

  return Object.keys(direct).length > 0 ? direct : null;
}

function collectCodeEntries(items: CodeRegistryItem[]): CodeEntry[] {
  const entries: CodeEntry[] = [];
  const emptyParametersSchema: JsonObject = {
    type: "object",
    properties: {},
    additionalProperties: false,
  };

  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];

    if (typeof item.name !== "string" || typeof item.slug !== "string") {
      continue;
    }

    const selector = asProjectSelector(item.project);
    const tags = Array.isArray(item.tags)
      ? item.tags.filter((tag): tag is string => typeof tag === "string")
      : [];
    const parametersSchema =
      item.parameters === undefined || item.parameters === null
        ? emptyParametersSchema
        : schemaToJsonSchema(item.parameters);
    if (!parametersSchema) {
      throw new Error(`Function ${item.name} has invalid parameters schema`);
    }
    const returnsSchema = schemaToJsonSchema(item.returns);
    const functionSchema: JsonObject = {
      parameters: parametersSchema,
    };
    if (returnsSchema) {
      functionSchema.returns = returnsSchema;
    }

    const entry: CodeEntry = {
      kind: "code",
      project_id:
        typeof selector.project_id === "string"
          ? selector.project_id
          : undefined,
      project_name:
        typeof selector.project_name === "string"
          ? selector.project_name
          : undefined,
      name: item.name,
      slug: item.slug,
      description:
        typeof item.description === "string" ? item.description : undefined,
      function_type:
        typeof item.type === "string"
          ? item.type
          : typeof item.functionType === "string"
            ? item.functionType
            : undefined,
      if_exists: typeof item.ifExists === "string" ? item.ifExists : undefined,
      metadata: item.metadata,
      preview: typeof item.preview === "string" ? item.preview : undefined,
      location: {
        type: "function",
        index,
      },
    };

    if (tags.length > 0) {
      entry.tags = tags;
    }
    if (Object.keys(functionSchema).length > 0) {
      entry.function_schema = functionSchema;
    }

    entries.push(entry);
  }

  return entries;
}

async function processFile(filePath: string): Promise<ManifestFile> {
  const absolutePath = path.resolve(process.cwd(), filePath);
  const fallbackRegistry = freshRegistry();
  globalThis._evals = fallbackRegistry;
  globalThis._lazy_load = true;

  await import(buildIsolatedImportUrl(absolutePath));
  const registry = currentRegistry(fallbackRegistry);

  const entries: Array<CodeEntry | FunctionEventEntry> = [
    ...collectCodeEntries(registry.functions as CodeRegistryItem[]),
    ...(await collectFunctionEvents(
      registry.prompts as EventRegistryItem[],
      true,
    )),
    ...(await collectFunctionEvents(
      registry.parameters as EventRegistryItem[],
      false,
    )),
  ];

  return {
    source_file: absolutePath,
    entries,
  };
}

async function main(): Promise<void> {
  const files = process.argv.slice(2);
  if (files.length === 0) {
    throw new Error("functions-runner requires at least one input file");
  }

  const manifest: Manifest = {
    runtime_context: {
      runtime: "node",
      version:
        typeof process.version === "string" && process.version.startsWith("v")
          ? process.version.slice(1)
          : typeof process.version === "string" && process.version.length > 0
            ? process.version
            : "unknown",
    },
    files: [],
  };

  for (const file of files) {
    const result = await processFile(file);
    manifest.files.push(result);
  }

  process.stdout.write(JSON.stringify(manifest));
}

main().catch((error: Error) => {
  const message = error instanceof Error ? error.message : String(error);
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
