import { spawnSync } from "node:child_process";
import fs from "node:fs";
import { createRequire } from "node:module";
import path from "node:path";
import { pathToFileURL } from "node:url";

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

function buildExternalPackagePatterns(additionalPackages: string[]): string[] {
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
  const patterns = new Set<string>(["node_modules/*"]);
  for (const pkg of knownPackages) {
    const trimmed = pkg.trim();
    if (!trimmed) {
      continue;
    }
    if (trimmed.endsWith("/")) {
      patterns.add(`${trimmed}*`);
      continue;
    }
    patterns.add(trimmed);
    patterns.add(`${trimmed}/*`);
  }
  return [...patterns];
}

function findNodeModulesBinary(
  binary: string,
  startPath: string,
): string | null {
  let current = path.resolve(startPath);
  if (!fs.existsSync(current)) {
    current = path.dirname(current);
  } else if (!fs.statSync(current).isDirectory()) {
    current = path.dirname(current);
  }

  const binaryCandidates =
    process.platform === "win32" ? [`${binary}.cmd`, binary] : [binary];

  while (true) {
    for (const candidateName of binaryCandidates) {
      const candidate = path.join(
        current,
        "node_modules",
        ".bin",
        candidateName,
      );
      if (fs.existsSync(candidate)) {
        return candidate;
      }
    }

    const parent = path.dirname(current);
    if (parent === current) {
      return null;
    }
    current = parent;
  }
}

function resolveEsbuildBinary(sourceFile: string): string | null {
  const searchRoots = [path.resolve(sourceFile), process.cwd()];
  const seen = new Set<string>();
  for (const root of searchRoots) {
    const normalized = path.resolve(root);
    if (seen.has(normalized)) {
      continue;
    }
    seen.add(normalized);
    const candidate = findNodeModulesBinary("esbuild", normalized);
    if (candidate) {
      return candidate;
    }
  }
  return null;
}

function resolveEsbuildModulePath(sourceFile: string): string | null {
  const filePath = path.resolve(sourceFile);
  try {
    const requireFromFile = createRequire(pathToFileURL(filePath).href);
    return requireFromFile.resolve("esbuild");
  } catch {
    // Fall through to process cwd.
  }

  try {
    const requireFromCwd = createRequire(path.join(process.cwd(), "noop.js"));
    return requireFromCwd.resolve("esbuild");
  } catch {
    return null;
  }
}

function normalizeEsbuildModule(loaded: unknown): EsbuildModule | null {
  if (isEsbuildModule(loaded)) {
    return loaded;
  }
  if (isObject(loaded) && isEsbuildModule(loaded.default)) {
    return loaded.default;
  }
  return null;
}

async function loadEsbuild(sourceFile: string): Promise<EsbuildModule | null> {
  const resolvedPath = resolveEsbuildModulePath(sourceFile);
  if (resolvedPath) {
    if (typeof require === "function") {
      try {
        const loaded = require(resolvedPath) as unknown;
        const normalized = normalizeEsbuildModule(loaded);
        if (normalized) {
          return normalized;
        }
      } catch {
        // Fall through to dynamic import.
      }
    }

    try {
      const loaded = (await import(
        pathToFileURL(resolvedPath).href
      )) as unknown;
      const normalized = normalizeEsbuildModule(loaded);
      if (normalized) {
        return normalized;
      }
    } catch {
      // Fall through to direct require/import.
    }
  }

  if (typeof require === "function") {
    try {
      const loaded = require("esbuild") as unknown;
      const normalized = normalizeEsbuildModule(loaded);
      if (normalized) {
        return normalized;
      }
    } catch {
      // Fall through to dynamic import.
    }
  }

  try {
    // Keep module name dynamic so TypeScript doesn't require local esbuild types at compile time.
    const specifier = "esbuild";
    const loaded = (await import(specifier)) as unknown;
    const normalized = normalizeEsbuildModule(loaded);
    if (normalized) {
      return normalized;
    }
  } catch {
    // handled below
  }

  return null;
}

function computeNodeTargetVersion(): string {
  return typeof process.version === "string" && process.version.startsWith("v")
    ? process.version.slice(1)
    : process.versions.node || "18";
}

async function bundleWithEsbuildModule(
  esbuild: EsbuildModule,
  sourceFile: string,
  outputFile: string,
  tsconfig: string | undefined,
  external: string[],
): Promise<void> {
  await esbuild.build({
    entryPoints: [sourceFile],
    bundle: true,
    treeShaking: true,
    platform: "node",
    target: `node${computeNodeTargetVersion()}`,
    write: true,
    outfile: outputFile,
    tsconfig,
    external,
  });
}

function bundleWithEsbuildBinary(
  esbuildBinary: string,
  sourceFile: string,
  outputFile: string,
  tsconfig: string | undefined,
  external: string[],
): void {
  const args: string[] = [
    sourceFile,
    "--bundle",
    "--tree-shaking=true",
    "--platform=node",
    `--target=node${computeNodeTargetVersion()}`,
    `--outfile=${outputFile}`,
  ];

  if (tsconfig) {
    args.push(`--tsconfig=${tsconfig}`);
  }
  for (const pattern of external) {
    args.push(`--external:${pattern}`);
  }

  const result = spawnSync(esbuildBinary, args, { encoding: "utf8" });
  if (result.error) {
    throw new Error(
      `failed to invoke esbuild CLI at ${esbuildBinary}: ${result.error.message}`,
    );
  }
  if (result.status !== 0) {
    const stderr = (result.stderr ?? "").trim();
    const stdout = (result.stdout ?? "").trim();
    const details = stderr || stdout || "unknown error";
    throw new Error(
      `esbuild CLI exited with status ${String(result.status)}: ${details}`,
    );
  }
}

async function main(): Promise<void> {
  const [sourceFile, outputFile] = process.argv.slice(2);
  if (!sourceFile || !outputFile) {
    throw new Error("functions-bundler requires <SOURCE_FILE> <OUTPUT_FILE>");
  }

  const externalPackages = parseExternalPackages(
    process.env.BT_FUNCTIONS_PUSH_EXTERNAL_PACKAGES,
  );
  const selfContained = process.env.BT_FUNCTIONS_PUSH_SELF_CONTAINED === "1";
  const external = selfContained
    ? ["fsevents", "chokidar"]
    : buildExternalPackagePatterns(externalPackages);
  const tsconfig = loadTsconfigPath();

  const outputDir = path.dirname(outputFile);
  fs.mkdirSync(outputDir, { recursive: true });

  const esbuild = await loadEsbuild(sourceFile);
  if (esbuild) {
    await bundleWithEsbuildModule(
      esbuild,
      sourceFile,
      outputFile,
      tsconfig,
      external,
    );
    return;
  }

  const esbuildBinary = resolveEsbuildBinary(sourceFile);
  if (esbuildBinary) {
    bundleWithEsbuildBinary(
      esbuildBinary,
      sourceFile,
      outputFile,
      tsconfig,
      external,
    );
    return;
  }

  throw new Error(
    "failed to load esbuild for JS bundling; install esbuild in your project or use a runner that provides it",
  );
}

main().catch((error: unknown) => {
  const message =
    error instanceof Error
      ? error.message
      : `failed to bundle JS source: ${String(error)}`;
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
