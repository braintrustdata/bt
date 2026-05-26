#!/usr/bin/env node
"use strict";

const { spawnSync } = require("node:child_process");
const {
  PLATFORM_PACKAGES,
  currentPlatformPackage,
  binaryName,
} = require("../lib/platform");

function resolveBinary() {
  const pkg = currentPlatformPackage();
  if (!pkg) {
    throw new Error(
      `No prebuilt bt binary for ${process.platform}-${process.arch}. ` +
        `Supported: ${Object.keys(PLATFORM_PACKAGES).join(", ")}. ` +
        `See https://github.com/braintrustdata/bt for other install methods.`,
    );
  }
  try {
    return require.resolve(`${pkg}/bin/${binaryName()}`);
  } catch (err) {
    throw new Error(
      `Failed to locate ${pkg}. This package is shipped via optionalDependencies; ` +
        `if you installed with --ignore-optional / --no-optional, reinstall without ` +
        `that flag, or use the shell installer at https://github.com/braintrustdata/bt. ` +
        `(${err.message})`,
    );
  }
}

try {
  const binary = resolveBinary();
  const result = spawnSync(binary, process.argv.slice(2), {
    stdio: "inherit",
    windowsHide: true,
  });
  if (result.error) throw result.error;
  process.exit(result.status ?? 1);
} catch (err) {
  console.error(`bt: ${err.message}`);
  process.exit(1);
}
