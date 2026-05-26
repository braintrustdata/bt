"use strict";

const PLATFORM_PACKAGES = {
  "darwin-arm64": "@braintrust/bt-darwin-arm64",
  "darwin-x64": "@braintrust/bt-darwin-x64",
  "linux-arm64": "@braintrust/bt-linux-arm64",
  "linux-x64-glibc": "@braintrust/bt-linux-x64",
  "linux-x64-musl": "@braintrust/bt-linux-x64-musl",
  "win32-arm64": "@braintrust/bt-win32-arm64",
  "win32-x64": "@braintrust/bt-win32-x64",
};

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

function platformKey() {
  const { platform, arch } = process;
  if (platform === "linux" && arch === "x64") {
    return `linux-x64-${detectLibc()}`;
  }
  return `${platform}-${arch}`;
}

function currentPlatformPackage() {
  return PLATFORM_PACKAGES[platformKey()] || null;
}

function binaryName() {
  return process.platform === "win32" ? "bt.exe" : "bt";
}

module.exports = {
  PLATFORM_PACKAGES,
  currentPlatformPackage,
  binaryName,
};
