"use strict";

// Shared helpers for locating the `bt` binary. Used by both `bin/bt`
// (the runtime launcher) and `scripts/install.js` (the postinstall
// fallback downloader).

const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");

const BINARY_DISTRIBUTIONS = [
  { packageName: "@braintrust/bt-darwin-arm64", subpath: "bin/bt" },
  { packageName: "@braintrust/bt-darwin-x64", subpath: "bin/bt" },
  { packageName: "@braintrust/bt-linux-arm64", subpath: "bin/bt" },
  { packageName: "@braintrust/bt-linux-x64", subpath: "bin/bt" },
  { packageName: "@braintrust/bt-linux-x64-musl", subpath: "bin/bt" },
  { packageName: "@braintrust/bt-win32-arm64", subpath: "bin/bt.exe" },
  { packageName: "@braintrust/bt-win32-x64", subpath: "bin/bt.exe" },
];

function detectLibc() {
  if (process.platform !== "linux") return null;
  try {
    const report = process.report && process.report.getReport();
    if (report && report.header && report.header.glibcVersionRuntime) {
      return "glibc";
    }
    return "musl";
  } catch {
    return "glibc";
  }
}

function binaryName() {
  return process.platform === "win32" ? "bt.exe" : "bt";
}

function getDistributionForThisPlatform() {
  const arch = os.arch();
  const platform = os.platform();
  const subpath = `bin/${binaryName()}`;

  let packageName;
  if (platform === "darwin") {
    if (arch === "arm64") packageName = "@braintrust/bt-darwin-arm64";
    else if (arch === "x64") packageName = "@braintrust/bt-darwin-x64";
  } else if (platform === "linux") {
    const libc = detectLibc();
    if (arch === "arm64" && libc === "glibc") {
      packageName = "@braintrust/bt-linux-arm64";
    } else if (arch === "x64") {
      packageName =
        libc === "musl"
          ? "@braintrust/bt-linux-x64-musl"
          : "@braintrust/bt-linux-x64";
    }
  } else if (platform === "win32") {
    if (arch === "arm64") packageName = "@braintrust/bt-win32-arm64";
    else if (arch === "x64") packageName = "@braintrust/bt-win32-x64";
  }

  return { packageName, subpath };
}

function throwUnsupportedPlatformError() {
  throw new Error(
    `Unsupported operating system or architecture! The bt CLI does not work on ${process.platform}-${process.arch}.

bt supports:
- macOS (darwin) on arm64 and x64
- Linux glibc on arm64 and x64; Linux musl on x64
- Windows on arm64 and x64`,
  );
}

// Constructed indirectly so bundlers (e.g. @vercel/nft) don't statically
// detect the fallback binary path as an asset to trace.
function getFallbackBinaryPath() {
  const parts = [__dirname, binaryName()];
  return path.resolve(...parts);
}

function resolvePackageBinary(packageName, subpath) {
  const binaryPath = require.resolve(`${packageName}/${subpath}`);
  // Read the manifest beside this binary, which may belong to a hoisted
  // dependency rather than the version pinned by this wrapper.
  const installed = require(
    path.resolve(path.dirname(binaryPath), "..", "package.json"),
  );
  const expectedVersion =
    require("../package.json").optionalDependencies[packageName];
  if (installed.version !== expectedVersion) {
    throw new Error(
      `Expected ${packageName}@${expectedVersion}, but found ${installed.version}. Reinstall @braintrust/bt with optional dependencies enabled.`,
    );
  }
  return binaryPath;
}

function getBinaryPath() {
  // npm launcher compatibility: this override selects the executable before
  // clap can run, so it cannot be configured by the Rust CLI.
  if (process.env.BT_BINARY_PATH) {
    return process.env.BT_BINARY_PATH;
  }

  const { packageName, subpath } = getDistributionForThisPlatform();

  if (packageName === undefined) {
    throwUnsupportedPlatformError();
  }

  // Prefer the optional dep so a stale fallback from a prior
  // `--omit=optional` install can't shadow a newer optional dep on upgrade.
  try {
    return resolvePackageBinary(packageName, subpath);
  } catch (err) {
    const fallbackBinaryPath = getFallbackBinaryPath();
    if (fs.existsSync(fallbackBinaryPath)) {
      return fallbackBinaryPath;
    }
    if (err.code !== "MODULE_NOT_FOUND") throw err;

    const otherInstalled = BINARY_DISTRIBUTIONS.find((dist) => {
      try {
        require.resolve(`${dist.packageName}/${dist.subpath}`);
        return true;
      } catch {
        return false;
      }
    });

    // Error messages inspired by esbuild:
    // https://github.com/evanw/esbuild/blob/f3d535262e3998d845d0f102b944ecd5a9efda57/lib/npm/node-platform.ts#L150
    if (otherInstalled) {
      throw new Error(
        `bt binary for this platform/architecture not found!

The "${otherInstalled.packageName}" package is installed, but for the current platform you should have the "${packageName}" package installed instead. This usually happens if "@braintrust/bt" is installed on one platform (for example macOS or Windows) and the "node_modules" folder is then reused on another (for example Linux in Docker).

To fix this, avoid copying the "node_modules" folder, and instead freshly install your dependencies on the target system. You can also configure your package manager to install the right package. For example, yarn has the "supportedArchitectures" feature: https://yarnpkg.com/configuration/yarnrc/#supportedArchitecture.`,
      );
    }

    throw new Error(
      `bt binary for this platform/architecture not found!

It seems like none of the "@braintrust/bt" package's optional dependencies got installed. Please make sure your package manager is configured to install optional dependencies. If you are using npm, don't set the "--no-optional", "--ignore-optional", or "--omit=optional" flags. The "@braintrust/bt" package needs the "optionalDependencies" feature in order to install the bt binary.`,
    );
  }
}

module.exports = {
  BINARY_DISTRIBUTIONS,
  binaryName,
  getBinaryPath,
  getDistributionForThisPlatform,
  getFallbackBinaryPath,
  resolvePackageBinary,
  throwUnsupportedPlatformError,
};
