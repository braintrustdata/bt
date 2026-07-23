import fs from "node:fs";
import { pathToFileURL } from "node:url";

function isObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function installDiscoveryReactStub() {
  if (globalThis.React) {
    return;
  }

  const createElement = (...args) => ({
    type: args[0],
    props: args[1] ?? {},
    children: args.slice(2),
  });
  globalThis.React = {
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
    forwardRef: (component) => component,
    isValidElement: (value) => typeof value === "object" && value !== null,
    lazy: (loader) => loader,
    memo: (component) => component,
    startTransition: (callback) => callback(),
    useCallback: (callback) => callback,
    useContext: () => undefined,
    useDebugValue: () => undefined,
    useDeferredValue: (value) => value,
    useEffect: () => undefined,
    useId: () => "bt-view-id",
    useImperativeHandle: () => undefined,
    useInsertionEffect: () => undefined,
    useLayoutEffect: () => undefined,
    useMemo: (factory) => factory(),
    useReducer: (_reducer, initialValue) => [initialValue, () => undefined],
    useRef: (current) => ({ current }),
    useState: (initialValue) => [initialValue, () => undefined],
    useSyncExternalStore: () => undefined,
    useTransition: () => [false, () => undefined],
  };
}

function defaultExportFromModule(loaded) {
  if (
    !isObject(loaded) ||
    !Object.prototype.hasOwnProperty.call(loaded, "default")
  ) {
    return undefined;
  }
  const value = loaded.default;
  if (
    isObject(value) &&
    Object.prototype.hasOwnProperty.call(value, "default")
  ) {
    return value.default;
  }
  return value;
}

function validateCustomView(value, sourceFile) {
  if (!isObject(value)) {
    throw new Error(
      `${sourceFile} must default-export customTraceView(...) or customDatasetView(...)`,
    );
  }
  if (value.kind !== "trace" && value.kind !== "dataset") {
    throw new Error(
      `${sourceFile} default export must be a trace or dataset custom view`,
    );
  }
  if (typeof value.name !== "string" || !value.name.trim()) {
    throw new Error(`${sourceFile} custom view name is required`);
  }
  if (typeof value.slug !== "string" || !value.slug.trim()) {
    throw new Error(`${sourceFile} custom view '${value.name}' slug is required`);
  }
  if (typeof value.component !== "function") {
    throw new Error(
      `${sourceFile} custom view '${value.slug}' component must be a function`,
    );
  }
}

function projectFields(project) {
  if (typeof project === "string" && project.trim()) {
    return { project_name: project };
  }
  if (!isObject(project)) {
    return {};
  }
  return {
    ...(typeof project.id === "string" && project.id.trim()
      ? { project_id: project.id }
      : {}),
    ...(typeof project.name === "string" && project.name.trim()
      ? { project_name: project.name }
      : {}),
  };
}

function datasetFields(view) {
  if (view.kind !== "dataset" || !isObject(view.dataset)) {
    return {};
  }
  return {
    ...(typeof view.dataset.id === "string" && view.dataset.id.trim()
      ? { dataset_id: view.dataset.id }
      : {}),
    ...(typeof view.dataset.name === "string" && view.dataset.name.trim()
      ? { dataset_name: view.dataset.name }
      : {}),
  };
}

async function buildManifest(inputPath) {
  const input = JSON.parse(fs.readFileSync(inputPath, "utf8"));
  if (!Array.isArray(input.files) || input.files.length === 0) {
    throw new Error("views metadata runner requires at least one bundled view");
  }

  installDiscoveryReactStub();
  const manifest = {
    runtime_context: {
      runtime: "browser",
      version: "latest",
    },
    files: [],
  };

  for (const file of input.files) {
    if (!isObject(file)) {
      throw new Error("views metadata input file entries must be objects");
    }
    const { source_file: sourceFile, bundle_file: bundleFile } = file;
    if (typeof sourceFile !== "string" || typeof bundleFile !== "string") {
      throw new Error("views metadata input entries require source_file and bundle_file");
    }
    const loaded = await import(
      `${pathToFileURL(bundleFile).href}?bt_view_nonce=${Date.now()}`
    );
    const view = defaultExportFromModule(loaded);
    validateCustomView(view, sourceFile);
    manifest.files.push({
      source_file: sourceFile,
      dependencies: [],
      entries: [
        {
          kind: "view",
          view_type: view.kind,
          name: view.name,
          slug: view.slug,
          code: "",
          ...projectFields(view.project),
          ...datasetFields(view),
        },
      ],
    });
  }

  return manifest;
}

async function main() {
  const inputPath = process.argv[2];
  if (!inputPath) {
    throw new Error("views metadata runner requires an input path argument");
  }
  const manifest = await buildManifest(inputPath);
  process.stdout.write(`${JSON.stringify(manifest)}\n`);
}

main().catch((error) => {
  const message =
    error instanceof Error
      ? error.message
      : `failed to build custom view metadata: ${String(error)}`;
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
