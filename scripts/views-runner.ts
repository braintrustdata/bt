import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { createRequire } from "node:module";
import { pathToFileURL } from "node:url";

type EsbuildModule = {
  build: (options: Record<string, unknown>) => Promise<unknown>;
};

type JsonValue =
  | string
  | number
  | boolean
  | null
  | JsonValue[]
  | { [key: string]: JsonValue };

type RegisteredCustomView = {
  kind?: string;
  name?: string;
  slug?: string;
  description?: string;
  project?: string | { id?: string; name?: string };
  dataset?: { id?: string; name?: string };
  tags?: unknown;
  metadata?: unknown;
};

type ManifestEntry = {
  kind: "view";
  view_type: "trace" | "dataset";
  name: string;
  slug: string;
  code: string;
  description?: string;
  project_id?: string;
  project_name?: string;
  dataset_id?: string;
  dataset_name?: string;
  tags?: string[];
  metadata?: JsonValue;
};

type Manifest = {
  runtime_context: {
    runtime: "browser";
    version: "latest";
  };
  files: Array<{
    source_file: string;
    entries: ManifestEntry[];
  }>;
};

const __filename = path.resolve(process.argv[1] || "views-runner.ts");
const __dirname = path.dirname(__filename);

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function normalizeEsbuildModule(value: unknown): EsbuildModule | null {
  if (isObject(value) && typeof value.build === "function") {
    return value as EsbuildModule;
  }
  if (
    isObject(value) &&
    isObject(value.default) &&
    typeof value.default.build === "function"
  ) {
    return value.default as EsbuildModule;
  }
  return null;
}

async function loadEsbuild(sourceFiles: string[]): Promise<EsbuildModule> {
  const candidates = [
    ...sourceFiles.map((file) => path.resolve(file)),
    path.join(process.cwd(), "package.json"),
    path.join(__dirname, "package.json"),
  ];

  for (const candidate of candidates) {
    try {
      const requireFrom = createRequire(pathToFileURL(candidate).href);
      const resolved = requireFrom.resolve("esbuild");
      const loaded = requireFrom(resolved);
      const normalized = normalizeEsbuildModule(loaded);
      if (normalized) {
        return normalized;
      }
    } catch {
      // Try the next search root.
    }
  }

  try {
    const esbuildSpecifier = "esbuild";
    const loaded = await import(esbuildSpecifier);
    const normalized = normalizeEsbuildModule(loaded);
    if (normalized) {
      return normalized;
    }
  } catch {
    // handled below
  }

  throw new Error(
    "failed to load esbuild for custom view bundling; install esbuild in your project",
  );
}

function loadTsconfigPath(): string | undefined {
  const tsNode = process.env.TS_NODE_PROJECT?.trim();
  if (tsNode) {
    return tsNode;
  }
  const tsx = process.env.TSX_TSCONFIG_PATH?.trim();
  if (tsx) {
    return tsx;
  }
  return undefined;
}

function sdkPath(): string {
  return process.env.BT_VIEWS_SDK_PATH || path.join(__dirname, "views-sdk.ts");
}

function reactModuleSource(): string {
  return `
const ReactValue = globalThis.React || React;
export default ReactValue;
export const Children = ReactValue.Children;
export const Component = ReactValue.Component;
export const Fragment = ReactValue.Fragment;
export const Profiler = ReactValue.Profiler;
export const PureComponent = ReactValue.PureComponent;
export const StrictMode = ReactValue.StrictMode;
export const Suspense = ReactValue.Suspense;
export const cloneElement = ReactValue.cloneElement;
export const createContext = ReactValue.createContext;
export const createElement = ReactValue.createElement;
export const createRef = ReactValue.createRef;
export const forwardRef = ReactValue.forwardRef;
export const isValidElement = ReactValue.isValidElement;
export const lazy = ReactValue.lazy;
export const memo = ReactValue.memo;
export const startTransition = ReactValue.startTransition;
export const useCallback = ReactValue.useCallback;
export const useContext = ReactValue.useContext;
export const useDebugValue = ReactValue.useDebugValue;
export const useDeferredValue = ReactValue.useDeferredValue;
export const useEffect = ReactValue.useEffect;
export const useId = ReactValue.useId;
export const useImperativeHandle = ReactValue.useImperativeHandle;
export const useInsertionEffect = ReactValue.useInsertionEffect;
export const useLayoutEffect = ReactValue.useLayoutEffect;
export const useMemo = ReactValue.useMemo;
export const useReducer = ReactValue.useReducer;
export const useRef = ReactValue.useRef;
export const useState = ReactValue.useState;
export const useSyncExternalStore = ReactValue.useSyncExternalStore;
export const useTransition = ReactValue.useTransition;
`;
}

function jsxRuntimeSource(): string {
  return `
const ReactValue = globalThis.React || React;
export const Fragment = ReactValue.Fragment;
export const jsx = ReactValue.createElement;
export const jsxs = ReactValue.createElement;
export const jsxDEV = ReactValue.createElement;
`;
}

function installDiscoveryReactStub(): void {
  const globalWithReact = globalThis as {
    React?: Record<string, unknown>;
  };
  if (globalWithReact.React) {
    return;
  }
  const createElement = (...args: unknown[]) => ({
    type: args[0],
    props: args[1] ?? {},
    children: args.slice(2),
  });
  globalWithReact.React = {
    Children: {},
    Component: class {},
    Fragment: Symbol.for("react.fragment"),
    Profiler: Symbol.for("react.profiler"),
    PureComponent: class {},
    StrictMode: Symbol.for("react.strict_mode"),
    Suspense: Symbol.for("react.suspense"),
    cloneElement: createElement,
    createContext: () => ({}),
    createElement,
    createRef: () => ({ current: null }),
    forwardRef: (component: unknown) => component,
    isValidElement: (value: unknown) =>
      typeof value === "object" && value !== null,
    lazy: (loader: unknown) => loader,
    memo: (component: unknown) => component,
    startTransition: (callback: () => void) => callback(),
    useCallback: (callback: unknown) => callback,
    useContext: () => undefined,
    useDebugValue: () => undefined,
    useDeferredValue: (value: unknown) => value,
    useEffect: () => undefined,
    useId: () => "bt-view-preview-id",
    useImperativeHandle: () => undefined,
    useInsertionEffect: () => undefined,
    useLayoutEffect: () => undefined,
    useMemo: (factory: () => unknown) => factory(),
    useReducer: (_reducer: unknown, initialValue: unknown) => [
      initialValue,
      () => undefined,
    ],
    useRef: (current: unknown) => ({ current }),
    useState: (initialValue: unknown) => [initialValue, () => undefined],
    useSyncExternalStore: () => undefined,
    useTransition: () => [false, () => undefined],
  };
}

function braintrustViewPlugin() {
  const helperPath = sdkPath();
  return {
    name: "braintrust-custom-view-local-sdk",
    setup(build: {
      onResolve: (
        options: { filter: RegExp },
        callback: (args: { path: string }) => unknown,
      ) => void;
      onLoad: (
        options: { filter: RegExp; namespace?: string },
        callback: (args: { path: string }) => unknown,
      ) => void;
    }) {
      build.onResolve({ filter: /^braintrust\/custom-views$/ }, () => ({
        path: helperPath,
      }));
      build.onResolve({ filter: /^@braintrust\/local\/custom-views$/ }, () => ({
        path: helperPath,
      }));
      build.onResolve({ filter: /^react$/ }, () => ({
        path: "react",
        namespace: "bt-view-react",
      }));
      build.onResolve({ filter: /^react\/jsx-runtime$/ }, () => ({
        path: "react/jsx-runtime",
        namespace: "bt-view-react",
      }));
      build.onResolve({ filter: /^react\/jsx-dev-runtime$/ }, () => ({
        path: "react/jsx-dev-runtime",
        namespace: "bt-view-react",
      }));
      build.onLoad(
        { filter: /^react$/, namespace: "bt-view-react" },
        () => ({
          contents: reactModuleSource(),
          loader: "js",
        }),
      );
      build.onLoad(
        { filter: /^react\/jsx-(dev-)?runtime$/, namespace: "bt-view-react" },
        () => ({
          contents: jsxRuntimeSource(),
          loader: "js",
        }),
      );
    },
  };
}

function validateJson(value: unknown, label: string): JsonValue | undefined {
  if (value === undefined) {
    return undefined;
  }
  try {
    return JSON.parse(JSON.stringify(value)) as JsonValue;
  } catch {
    throw new Error(`${label} must be JSON-serializable`);
  }
}

function normalizeTags(value: unknown, slug: string): string[] | undefined {
  if (value === undefined) {
    return undefined;
  }
  if (!Array.isArray(value) || !value.every((tag) => typeof tag === "string")) {
    throw new Error(`custom view '${slug}' tags must be an array of strings`);
  }
  return value;
}

function projectFields(
  project: RegisteredCustomView["project"],
): Pick<ManifestEntry, "project_id" | "project_name"> {
  if (project === undefined) {
    return {};
  }
  if (typeof project === "string") {
    return { project_name: project };
  }
  if (typeof project.id === "string" && project.id.trim()) {
    return { project_id: project.id };
  }
  if (typeof project.name === "string" && project.name.trim()) {
    return { project_name: project.name };
  }
  throw new Error("custom view project must be a string, { id }, or { name }");
}

function datasetFields(
  view: RegisteredCustomView,
): Pick<ManifestEntry, "dataset_id" | "dataset_name"> {
  if (view.kind !== "dataset") {
    return {};
  }
  const dataset = view.dataset;
  if (!dataset) {
    throw new Error(`dataset custom view '${view.slug}' requires dataset`);
  }
  if (typeof dataset.id === "string" && dataset.id.trim()) {
    return { dataset_id: dataset.id };
  }
  if (typeof dataset.name === "string" && dataset.name.trim()) {
    return { dataset_name: dataset.name };
  }
  throw new Error(
    `dataset custom view '${view.slug}' dataset must be { id } or { name }`,
  );
}

function validateRegisteredView(view: RegisteredCustomView): void {
  if (view.kind !== "trace" && view.kind !== "dataset") {
    throw new Error("custom view kind must be 'trace' or 'dataset'");
  }
  if (typeof view.name !== "string" || !view.name.trim()) {
    throw new Error("custom view name is required");
  }
  if (typeof view.slug !== "string" || !view.slug.trim()) {
    throw new Error(`custom view '${view.name}' slug is required`);
  }
}

async function bundleForDiscovery(
  esbuild: EsbuildModule,
  sourceFile: string,
  outputFile: string,
  tsconfig: string | undefined,
): Promise<void> {
  await esbuild.build({
    entryPoints: [sourceFile],
    outfile: outputFile,
    bundle: true,
    platform: "node",
    target: "node18",
    format: "cjs",
    tsconfig,
    write: true,
    plugins: [braintrustViewPlugin()],
  });
}

async function collectViews(
  esbuild: EsbuildModule,
  sourceFile: string,
  tempDir: string,
  tsconfig: string | undefined,
): Promise<RegisteredCustomView[]> {
  (globalThis as { __braintrust_custom_views?: RegisteredCustomView[] }).__braintrust_custom_views = [];
  installDiscoveryReactStub();
  const outputFile = path.join(
    tempDir,
    `${path.basename(sourceFile).replace(/[^\w.-]/g, "_")}.collector.cjs`,
  );
  await bundleForDiscovery(esbuild, sourceFile, outputFile, tsconfig);
  await import(`${pathToFileURL(outputFile).href}?bt_view_nonce=${Date.now()}`);
  const views =
    (globalThis as { __braintrust_custom_views?: RegisteredCustomView[] })
      .__braintrust_custom_views ?? [];
  const seen = new Set<string>();
  for (const view of views) {
    validateRegisteredView(view);
    const key = `${view.kind}:${view.slug}`;
    if (seen.has(key)) {
      throw new Error(`duplicate custom view slug '${view.slug}' in ${sourceFile}`);
    }
    seen.add(key);
  }
  return [...views];
}

function virtualEntrySource(sourceFile: string, slug: string): string {
  return `
import ${JSON.stringify(sourceFile)};
import { __getCustomViews } from "braintrust/custom-views";
const view = [...__getCustomViews()].reverse().find((candidate) => candidate.slug === ${JSON.stringify(slug)});
if (!view) {
  throw new Error("Custom view not registered: ${slug.replaceAll('"', '\\"')}");
}
export default view.component;
`;
}

async function bundleBrowserCode(
  esbuild: EsbuildModule,
  sourceFile: string,
  view: RegisteredCustomView,
  tempDir: string,
  tsconfig: string | undefined,
): Promise<string> {
  const entryFile = path.join(
    tempDir,
    `${view.slug?.replace(/[^\w.-]/g, "_") || "view"}.entry.ts`,
  );
  const outputFile = path.join(
    tempDir,
    `${view.slug?.replace(/[^\w.-]/g, "_") || "view"}.browser.js`,
  );
  fs.writeFileSync(entryFile, virtualEntrySource(sourceFile, view.slug!), "utf8");

  await esbuild.build({
    entryPoints: [entryFile],
    outfile: outputFile,
    bundle: true,
    platform: "browser",
    target: "es2019",
    format: "iife",
    globalName: "__BraintrustCustomView",
    tsconfig,
    write: true,
    treeShaking: true,
    jsxFactory: "React.createElement",
    jsxFragment: "React.Fragment",
    footer: {
      js: "module.exports = __BraintrustCustomView;",
    },
    plugins: [braintrustViewPlugin()],
  });

  return fs.readFileSync(outputFile, "utf8");
}

async function buildManifest(files: string[]): Promise<Manifest> {
  const sourceFiles = files.map((file) => path.resolve(file));
  const esbuild = await loadEsbuild(sourceFiles);
  const tsconfig = loadTsconfigPath();
  const tempDir = fs.mkdtempSync(path.join(os.tmpdir(), "bt-views-"));

  try {
    const manifest: Manifest = {
      runtime_context: {
        runtime: "browser",
        version: "latest",
      },
      files: [],
    };

    for (const sourceFile of sourceFiles) {
      if (!fs.existsSync(sourceFile)) {
        throw new Error(`custom view file not found: ${sourceFile}`);
      }
      const views = await collectViews(esbuild, sourceFile, tempDir, tsconfig);
      const entries: ManifestEntry[] = [];
      for (const view of views) {
        const code = await bundleBrowserCode(
          esbuild,
          sourceFile,
          view,
          tempDir,
          tsconfig,
        );
        entries.push({
          kind: "view",
          view_type: view.kind as "trace" | "dataset",
          name: view.name!,
          slug: view.slug!,
          description: view.description,
          code,
          ...projectFields(view.project),
          ...datasetFields(view),
          tags: normalizeTags(view.tags, view.slug!),
          metadata: validateJson(view.metadata, `custom view '${view.slug}' metadata`),
        });
      }
      manifest.files.push({ source_file: sourceFile, entries });
    }

    return manifest;
  } finally {
    fs.rmSync(tempDir, { recursive: true, force: true });
  }
}

async function main(): Promise<void> {
  const files = process.argv.slice(2);
  if (files.length === 0) {
    throw new Error("views-runner requires at least one view file");
  }

  const manifest = await buildManifest(files);
  process.stdout.write(`${JSON.stringify(manifest)}\n`);
}

main().catch((error: unknown) => {
  const message =
    error instanceof Error
      ? error.message
      : `failed to build custom view manifest: ${String(error)}`;
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
