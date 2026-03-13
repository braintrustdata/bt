import fs from "node:fs";
import path from "node:path";

type EsbuildBuild = (options: Record<string, unknown>) => Promise<unknown>;
type EsbuildModule = {
  build: EsbuildBuild;
};

function isObject(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function isEsbuildModule(value: unknown): value is EsbuildModule {
  return isObject(value) && typeof value.build === "function";
}

function parseExternalPackages(raw: string | undefined): string[] {
  if (!raw) {
    return [];
  }
  return raw
    .split(",")
    .map((value) => value.trim())
    .filter((value) => value.length > 0);
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

function createMarkKnownPackagesExternalPlugin(additionalPackages: string[]) {
  return {
    name: "make-known-packages-external",
    setup(build: {
      onResolve: (
        opts: { filter: RegExp },
        cb: (args: { path: string }) => { path: string; external: boolean },
      ) => void;
    }) {
      const knownPackages = [
        "braintrust",
        "autoevals",
        "@braintrust/",
        "config",
        "lightningcss",
        "@mapbox/node-pre-gyp",
        "fsevents",
        "chokidar",
        ...additionalPackages,
      ];
      const escapedPackages = knownPackages.map((pkg) => {
        const escaped = pkg.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
        if (pkg.endsWith("/")) {
          return `${escaped}.*`;
        }
        return `${escaped}(?:\\/.*)?`;
      });
      const knownPackagesFilter = new RegExp(
        `^(${escapedPackages.join("|")})$`,
      );
      build.onResolve({ filter: knownPackagesFilter }, (args) => ({
        path: args.path,
        external: true,
      }));
    },
  };
}

async function loadEsbuild(): Promise<EsbuildModule> {
  if (typeof require === "function") {
    try {
      const loaded = require("esbuild") as unknown;
      if (isEsbuildModule(loaded)) {
        return loaded;
      }
      if (isObject(loaded) && isEsbuildModule(loaded.default)) {
        return loaded.default;
      }
    } catch {
      // Fall through to dynamic import.
    }
  }

  try {
    // Keep module name dynamic so TypeScript doesn't require local esbuild types at compile time.
    const specifier = "esbuild";
    const loaded = (await import(specifier)) as unknown;
    if (isEsbuildModule(loaded)) {
      return loaded;
    }
    if (isObject(loaded) && isEsbuildModule(loaded.default)) {
      return loaded.default;
    }
  } catch {
    // handled below
  }

  throw new Error(
    "failed to load esbuild for JS bundling; install esbuild in your project or use a runner that provides it",
  );
}

async function main(): Promise<void> {
  const [sourceFile, outputFile] = process.argv.slice(2);
  if (!sourceFile || !outputFile) {
    throw new Error("functions-bundler requires <SOURCE_FILE> <OUTPUT_FILE>");
  }

  const esbuild = await loadEsbuild();
  const externalPackages = parseExternalPackages(
    process.env.BT_FUNCTIONS_PUSH_EXTERNAL_PACKAGES,
  );
  const selfContained = process.env.BT_FUNCTIONS_PUSH_SELF_CONTAINED === "1";
  const tsconfig = loadTsconfigPath();

  const outputDir = path.dirname(outputFile);
  fs.mkdirSync(outputDir, { recursive: true });

  const targetVersion =
    typeof process.version === "string" && process.version.startsWith("v")
      ? process.version.slice(1)
      : process.versions.node || "18";

  await esbuild.build({
    entryPoints: [sourceFile],
    bundle: true,
    treeShaking: true,
    platform: "node",
    target: `node${targetVersion}`,
    write: true,
    outfile: outputFile,
    tsconfig,
    external: selfContained
      ? ["fsevents", "chokidar"]
      : ["node_modules/*", "fsevents"],
    plugins: selfContained
      ? []
      : [createMarkKnownPackagesExternalPlugin(externalPackages)],
  });
}

main().catch((error: unknown) => {
  const message =
    error instanceof Error
      ? error.message
      : `failed to bundle JS source: ${String(error)}`;
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
