#!/usr/bin/env node
// Prepares checked-in npm packages using cargo-dist release archives.
//
//   --version <semver>      version to stamp into every package.json (required)
//   --archives-dir <path>   directory containing cargo-dist archives
//                           (bt-<target>.tar.gz / bt-<target>.zip), required
//   --out-dir <path>        directory to write packages into (default: npm/dist)
//
// Emits <out-dir>/bt-<pkg>/ (one per target) and <out-dir>/bt/, each ready
// to `npm publish`. The @braintrust/bt package exposes the `bt` command.

import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
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
const checksums = {};

if (existsSync(outDir)) rmSync(outDir, { recursive: true, force: true });
mkdirSync(outDir, { recursive: true });

// --- Per-platform packages ---
for (const [target, platform] of Object.entries(targets)) {
  const packageDir = join(NPM_DIR, "platforms", `bt-${platform}`);
  const platformPkg = JSON.parse(
    readFileSync(join(packageDir, "package.json"), "utf8"),
  );
  const isWindows = platformPkg.os.includes("win32");
  const binaryName = isWindows ? "bt.exe" : "bt";
  const archiveName = `bt-${target}.${isWindows ? "zip" : "tar.gz"}`;
  const archive = join(archivesDir, archiveName);
  if (!existsSync(archive)) {
    // Fail hard, don't skip: the wrapper pins each package at an exact version, so a
    // missing platform would break installs for that platform at runtime.
    throw new Error(`Archive not found for ${target}: ${archive}`);
  }

  const stagingDir = join(outDir, ".staging", target);
  mkdirSync(stagingDir, { recursive: true });
  if (archive.endsWith(".tar.gz")) {
    // cargo-dist tarballs nest contents under a `bt-<target>/` directory; strip
    // it so the binary lands at the staging root, matching the flat zips.
    execFileSync(
      "tar",
      ["-xzf", archive, "-C", stagingDir, "--strip-components=1"],
      {
        stdio: "inherit",
      },
    );
  } else if (archive.endsWith(".zip")) {
    // cargo-dist zips are already flat (binary at the archive root).
    execFileSync("unzip", ["-o", "-q", archive, "-d", stagingDir], {
      stdio: "inherit",
    });
  } else {
    throw new Error(`Unsupported archive: ${archive}`);
  }

  const binPath = join(stagingDir, binaryName);
  if (!existsSync(binPath)) {
    throw new Error(`Binary ${binaryName} not found at ${binPath}`);
  }

  checksums[platformPkg.name] = createHash("sha256")
    .update(readFileSync(binPath))
    .digest("hex");
  const pkgOut = join(outDir, `bt-${platform}`);
  const pkgBin = join(pkgOut, "bin");
  mkdirSync(pkgBin, { recursive: true });
  cpSync(binPath, join(pkgBin, binaryName));
  if (!isWindows) chmodSync(join(pkgBin, binaryName), 0o755);

  platformPkg.version = version;
  writeFileSync(
    join(pkgOut, "package.json"),
    JSON.stringify(platformPkg, null, 2) + "\n",
  );
  cpSync(join(NPM_DIR, "platforms", "README.md"), join(pkgOut, "README.md"));

  console.log(`Prepared ${platformPkg.name} -> ${pkgOut}`);
}

rmSync(join(outDir, ".staging"), { recursive: true, force: true });

// --- Standalone CLI package ---
const wrapperOut = join(outDir, "bt");
cpSync(join(NPM_DIR, "bt"), wrapperOut, { recursive: true });
cpSync(join(NPM_DIR, "..", "LICENSE"), join(wrapperOut, "LICENSE"));
writeFileSync(
  join(wrapperOut, "checksums.json"),
  JSON.stringify(checksums, null, 2) + "\n",
);
chmodSync(join(wrapperOut, "bin", "bt"), 0o755);
const wrapperPkg = JSON.parse(
  readFileSync(join(wrapperOut, "package.json"), "utf8"),
);
wrapperPkg.version = version;
delete wrapperPkg.private;
for (const dependency of Object.keys(wrapperPkg.optionalDependencies)) {
  wrapperPkg.optionalDependencies[dependency] = version;
}
writeFileSync(
  join(wrapperOut, "package.json"),
  JSON.stringify(wrapperPkg, null, 2) + "\n",
);
console.log(`Built @braintrust/bt -> ${wrapperOut}`);

const expected = Object.keys(targets).length + 1;
console.log(`\nAll ${expected} packages written to ${outDir}`);
