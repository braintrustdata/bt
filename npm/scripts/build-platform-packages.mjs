#!/usr/bin/env node
// Builds the per-platform npm packages from cargo-dist release archives.
//
//   --version <semver>      version to stamp into every package.json (required)
//   --archives-dir <path>   directory containing cargo-dist archives
//                           (bt-<target>.tar.gz / bt-<target>.zip), required
//   --out-dir <path>        directory to write packages into (default: npm/dist)
//
// Emits <out-dir>/bt-<pkg>/ (one per target), each ready to `npm publish`.
// The `bt` command is exposed via the `braintrust` SDK, which lists these
// packages as optionalDependencies and ships a launcher that resolves the
// matching binary.

import { execFileSync } from "node:child_process";
import {
  chmodSync,
  cpSync,
  existsSync,
  mkdirSync,
  readFileSync,
  rmSync,
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

// --- Per-platform packages ---
for (const [target, spec] of Object.entries(targets)) {
  const archiveName = `bt-${target}.${spec.archiveExt}`;
  const archive = join(archivesDir, archiveName);
  if (!existsSync(archive)) {
    // Fail hard, don't skip: the SDK pins each package at an exact version, so a
    // missing platform would break installs for that platform at runtime.
    throw new Error(`Archive not found for ${target}: ${archive}`);
  }

  const stagingDir = join(outDir, ".staging", target);
  mkdirSync(stagingDir, { recursive: true });
  if (archive.endsWith(".tar.gz")) {
    execFileSync("tar", ["-xzf", archive, "-C", stagingDir], {
      stdio: "inherit",
    });
  } else if (archive.endsWith(".zip")) {
    execFileSync("unzip", ["-o", "-q", archive, "-d", stagingDir], {
      stdio: "inherit",
    });
  } else {
    throw new Error(`Unsupported archive: ${archive}`);
  }

  const binPath = join(stagingDir, `bt-${target}`, spec.bin);
  if (!existsSync(binPath)) {
    throw new Error(`Binary ${spec.bin} not found at ${binPath}`);
  }

  const pkgName = `@braintrust/bt-${spec.pkg}`;
  const pkgOut = join(outDir, `bt-${spec.pkg}`);
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
    preferUnplugged: true,
  };
  if (spec.libc) platformPkg.libc = [spec.libc];

  writeFileSync(
    join(pkgOut, "package.json"),
    JSON.stringify(platformPkg, null, 2) + "\n",
  );
  writeFileSync(
    join(pkgOut, "README.md"),
    `# ${pkgName}\n\nPrebuilt \`bt\` binary for ${spec.os}-${spec.cpu}${spec.libc ? ` (${spec.libc})` : ""}.\n\nInstalled automatically as an optional dependency of [\`braintrust\`](https://www.npmjs.com/package/braintrust), which exposes the \`bt\` command. Install that package instead.\n`,
  );

  console.log(`Built ${pkgName} -> ${pkgOut}`);
}

rmSync(join(outDir, ".staging"), { recursive: true, force: true });

const expected = Object.keys(targets).length;
console.log(`\nAll ${expected} packages written to ${outDir}`);
