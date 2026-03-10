import path from "node:path";
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

  for (let index = 0; index < items.length; index += 1) {
    const item = items[index];

    if (typeof item.name !== "string" || typeof item.slug !== "string") {
      continue;
    }

    const selector = asProjectSelector(item.project);

    entries.push({
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
    });
  }

  return entries;
}

async function processFile(filePath: string): Promise<ManifestFile> {
  const absolutePath = path.resolve(process.cwd(), filePath);
  const fallbackRegistry = freshRegistry();
  globalThis._evals = fallbackRegistry;
  globalThis._lazy_load = true;

  await import(pathToFileURL(absolutePath).href);
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
