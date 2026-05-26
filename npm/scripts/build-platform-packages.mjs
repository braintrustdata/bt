#!/usr/bin/env node
// Builds the per-platform npm packages from cargo-dist release archives.
//
//   --version <semver>      version to stamp into every package.json (required)
//   --archives-dir <path>   directory containing cargo-dist archives
//                           (bt-<target>.tar.gz / bt-<target>.zip), required
//   --out-dir <path>        directory to write packages into (default: npm/dist)
//
// Emits <out-dir>/cli/ (wrapper) and <out-dir>/cli-<pkg>/ (one per target),
// each ready to `npm publish`.

import { execFileSync } from "node:child_process";
import {
  chmodSync,
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const NPM_DIR = resolve(__dirname, "..");

function parseArgs(argv) {
  const args = {};
  for (let i = 0; i < argv.length; i++) {
    const k = argv[i];
    if (k.startsWith("--")) args[k.slice(2)] = argv[++i];
  }
  return args;
}

const args = parseArgs(process.argv.slice(2));
const version = args.version;
const archivesDir = args["archives-dir"] && resolve(args["archives-dir"]);
const outDir = resolve(args["out-dir"] ?? join(NPM_DIR, "dist"));

if (!version) throw new Error("--version is required");
if (!archivesDir) throw new Error("--archives-dir is required");
if (!existsSync(archivesDir))
  throw new Error(`archives-dir not found: ${archivesDir}`);

const targets = JSON.parse(readFileSync(join(NPM_DIR, "targets.json"), "utf8"));

if (existsSync(outDir)) rmSync(outDir, { recursive: true, force: true });
mkdirSync(outDir, { recursive: true });

function extract(archive, dest) {
  mkdirSync(dest, { recursive: true });
  if (archive.endsWith(".tar.gz")) {
    execFileSync("tar", ["-xzf", archive, "-C", dest], { stdio: "inherit" });
  } else if (archive.endsWith(".zip")) {
    execFileSync("unzip", ["-o", "-q", archive, "-d", dest], {
      stdio: "inherit",
    });
  } else {
    throw new Error(`Unsupported archive: ${archive}`);
  }
}

function findBinary(rootDir, binName) {
  const stack = [rootDir];
  while (stack.length) {
    const cur = stack.pop();
    for (const entry of readdirSync(cur)) {
      const full = join(cur, entry);
      const s = statSync(full);
      if (s.isDirectory()) stack.push(full);
      else if (entry === binName) return full;
    }
  }
  throw new Error(`Binary ${binName} not found under ${rootDir}`);
}

// --- Wrapper package ---
const wrapperSrc = join(NPM_DIR, "cli");
const wrapperOut = join(outDir, "cli");
cpSync(wrapperSrc, wrapperOut, { recursive: true });
const wrapperPkgPath = join(wrapperOut, "package.json");
const wrapperPkg = JSON.parse(readFileSync(wrapperPkgPath, "utf8"));
wrapperPkg.version = version;
for (const key of Object.keys(wrapperPkg.optionalDependencies ?? {})) {
  wrapperPkg.optionalDependencies[key] = version;
}
writeFileSync(wrapperPkgPath, JSON.stringify(wrapperPkg, null, 2) + "\n");
console.log(`Built wrapper -> ${wrapperOut}`);

// --- Per-platform packages ---
for (const [target, spec] of Object.entries(targets)) {
  const archiveName = `bt-${target}.${spec.archiveExt}`;
  const archive = join(archivesDir, archiveName);
  if (!existsSync(archive)) {
    console.warn(`SKIP ${target}: archive not found (${archive})`);
    continue;
  }

  const stagingDir = join(outDir, ".staging", target);
  extract(archive, stagingDir);
  const binPath = findBinary(stagingDir, spec.bin);

  const pkgName = `@braintrust/cli-${spec.pkg}`;
  const pkgOut = join(outDir, `cli-${spec.pkg}`);
  const pkgBin = join(pkgOut, "bin");
  mkdirSync(pkgBin, { recursive: true });
  cpSync(binPath, join(pkgBin, spec.bin));
  if (spec.os !== "win32") chmodSync(join(pkgBin, spec.bin), 0o755);

  const platformPkg = {
    name: pkgName,
    version,
    description: `Prebuilt bt binary for ${spec.os}-${spec.cpu}${spec.libc ? `-${spec.libc}` : ""}`,
    homepage: "https://github.com/braintrustdata/bt",
    repository: {
      type: "git",
      url: "git+https://github.com/braintrustdata/bt.git",
    },
    license: "Apache-2.0",
    author: "Braintrust engineering <eng@braintrust.dev>",
    files: ["bin/"],
    os: [spec.os],
    cpu: [spec.cpu],
    publishConfig: {
      access: "public",
      provenance: true,
    },
    preferUnplugged: true,
  };
  if (spec.libc) platformPkg.libc = [spec.libc];

  writeFileSync(
    join(pkgOut, "package.json"),
    JSON.stringify(platformPkg, null, 2) + "\n",
  );
  writeFileSync(
    join(pkgOut, "README.md"),
    `# ${pkgName}\n\nPrebuilt \`bt\` binary for ${spec.os}-${spec.cpu}${spec.libc ? ` (${spec.libc})` : ""}.\n\nInstalled automatically as an optional dependency of [\`@braintrust/cli\`](https://www.npmjs.com/package/@braintrust/cli). Install that package instead.\n`,
  );

  console.log(`Built ${pkgName} -> ${pkgOut}`);
}

rmSync(join(outDir, ".staging"), { recursive: true, force: true });
console.log(`\nAll packages written to ${outDir}`);
