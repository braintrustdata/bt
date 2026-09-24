const assert = require("node:assert/strict");
const { spawn } = require("node:child_process");
const { EventEmitter } = require("node:events");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const { test } = require("node:test");
const vm = require("node:vm");
const { gzipSync } = require("node:zlib");
const { Worker } = require("node:worker_threads");

const packageDir = path.resolve(__dirname, "../bt");
const helperSource = fs.readFileSync(
  path.join(packageDir, "scripts/bt-helper.js"),
  "utf8",
);
const installSource = fs.readFileSync(
  path.join(packageDir, "scripts/install.js"),
  "utf8",
);

function loadHelper({
  platform = "linux",
  arch = "x64",
  libc = "glibc",
  env = {},
  installed = [],
  installedVersion = "1.2.3",
  missingVersion = false,
  fallback = false,
} = {}) {
  const fakeRequire = (name) => {
    if (name === "node:os")
      return { platform: () => platform, arch: () => arch };
    if (name === "node:fs") return { existsSync: () => fallback };
    if (name === "../package.json")
      return {
        optionalDependencies: missingVersion
          ? {}
          : { "@braintrust/bt-linux-x64": "1.2.3" },
      };
    if (
      installed.some(
        (binary) =>
          name === path.resolve("/resolved", binary, "../..", "package.json"),
      )
    )
      return { version: installedVersion };
    return require(name);
  };
  fakeRequire.resolve = (name) => {
    if (installed.includes(name)) return `/resolved/${name}`;
    throw Object.assign(new Error(`Cannot find module '${name}'`), {
      code: "MODULE_NOT_FOUND",
    });
  };
  const context = {
    require: fakeRequire,
    module: { exports: {} },
    __dirname: path.join(packageDir, "scripts"),
    process: {
      platform,
      arch,
      env,
      report: {
        getReport: () => ({
          header: libc === "glibc" ? { glibcVersionRuntime: "2.31" } : {},
        }),
      },
    },
  };
  vm.runInNewContext(helperSource, context);
  return context.module.exports;
}

test("resolves all seven published platforms", () => {
  const targets = require("../targets.json");
  for (const platform of Object.values(targets)) {
    const pkg = require(`../platforms/bt-${platform}/package.json`);
    const helper = loadHelper({
      platform: pkg.os[0],
      arch: pkg.cpu[0],
      libc: pkg.libc?.[0],
    });
    const dist = helper.getDistributionForThisPlatform();
    assert.equal(dist.packageName, pkg.name);
    assert.equal(dist.subpath, pkg.os[0] === "win32" ? "bin/bt.exe" : "bin/bt");
  }
});

test("binary override, optional dependency, then fallback precedence", () => {
  const installed = ["@braintrust/bt-linux-x64/bin/bt"];
  assert.equal(
    loadHelper({
      env: { BT_BINARY_PATH: "/custom/bt" },
      installed,
      fallback: true,
    }).getBinaryPath(),
    "/custom/bt",
  );
  assert.equal(
    loadHelper({ installed, fallback: true }).getBinaryPath(),
    "/resolved/@braintrust/bt-linux-x64/bin/bt",
  );
  assert.equal(
    loadHelper({ fallback: true }).getBinaryPath(),
    path.join(packageDir, "scripts/bt"),
  );
});

test("rejects a hoisted binary with a different version and prefers the fallback", () => {
  const options = {
    installed: ["@braintrust/bt-linux-x64/bin/bt"],
    installedVersion: "1.2.2",
  };
  assert.throws(
    () => loadHelper(options).getBinaryPath(),
    /Expected @braintrust\/bt-linux-x64@1\.2\.3, but found 1\.2\.2.*Reinstall/,
  );
  assert.equal(
    loadHelper({ ...options, fallback: true }).getBinaryPath(),
    path.join(packageDir, "scripts/bt"),
  );
});

test("ARM64 musl is unsupported even when a glibc binary is installed", () => {
  const helper = loadHelper({
    arch: "arm64",
    libc: "musl",
    installed: ["@braintrust/bt-linux-arm64/bin/bt"],
  });
  assert.equal(helper.getDistributionForThisPlatform().packageName, undefined);
  assert.throws(
    () => helper.getBinaryPath(),
    /Unsupported operating system[\s\S]*Linux glibc on arm64 and x64; Linux musl on x64/,
  );
});

test("missing and wrong-platform dependencies give actionable errors", () => {
  assert.throws(
    () => loadHelper().getBinaryPath(),
    /@braintrust\/bt.*optional dependencies/,
  );
  assert.throws(
    () =>
      loadHelper({
        installed: ["@braintrust/bt-darwin-arm64/bin/bt"],
      }).getBinaryPath(),
    /freshly install your dependencies on the target system/,
  );
  assert.throws(
    () => loadHelper({ platform: "freebsd" }).getBinaryPath(),
    /Unsupported operating system/,
  );
});

// Exercise the unmodified lifecycle entry point with in-memory registry and
// filesystem stubs, without network access or user configuration changes.
async function installerWorker({
  env = {},
  installed = false,
  installedVersion = "1.2.3",
  arch = "x64",
  libc = "glibc",
  unsupported = false,
  status = 200,
  missingVersion = false,
  invalidTar = false,
  malformedTar = false,
  truncatedTar = false,
  networkError = false,
  badChecksum = false,
  missingChecksum = false,
  writeError = false,
} = {}) {
  const writes = [];
  const messages = [];
  const exits = [];
  const requests = [];
  const renames = [];
  const removals = [];
  const binary = Buffer.from("synthetic bt binary");
  const tar = Buffer.alloc(1536);
  tar.write("package/bin/bt");
  tar.write(`${binary.length.toString(8).padStart(11, "0")}\0`, 124);
  binary.copy(tar, 512);
  if (malformedTar) tar.write("not-octal!!!\0", 124);
  const { parentPort, workerData } = require("node:worker_threads");
  const finish = () =>
    parentPort.postMessage({
      writes,
      messages: messages.join("\n"),
      exits,
      requests,
      renames,
      removals,
      binary,
    });
  const fakeRequire = (name) => {
    if (name === "./bt-helper")
      return {
        ...loadHelper({
          platform: unsupported ? "freebsd" : "linux",
          arch,
          libc,
          installed: installed ? ["@braintrust/bt-linux-x64/bin/bt"] : [],
          installedVersion,
          missingVersion,
        }),
        getFallbackBinaryPath: () => "/fallback/bt",
      };
    if (name === "../package.json")
      return {
        optionalDependencies: missingVersion
          ? {}
          : { "@braintrust/bt-linux-x64": "1.2.3" },
      };
    if (name === "../checksums.json")
      return missingChecksum
        ? {}
        : {
            "@braintrust/bt-linux-x64": require("node:crypto")
              .createHash("sha256")
              .update(badChecksum ? "different binary" : binary)
              .digest("hex"),
          };
    if (name === "node:fs")
      return {
        mkdirSync: () => {},
        writeFileSync: (file, content, options) => {
          if (writeError) throw new Error("disk full");
          writes.push({ file, content, mode: options.mode });
        },
        renameSync: (from, to) => renames.push({ from, to }),
        rmSync: (file) => removals.push(file),
      };
    if (name === "node:https")
      return {
        get: (url, callback) => {
          requests.push(url);
          const request = new EventEmitter();
          queueMicrotask(() => {
            if (networkError)
              return request.emit("error", new Error("offline"));
            const response = new EventEmitter();
            response.statusCode = status;
            response.headers = {};
            response.resume = () => {};
            callback(response);
            response.emit(
              "data",
              gzipSync(
                invalidTar
                  ? Buffer.alloc(512)
                  : truncatedTar
                    ? tar.subarray(0, 512)
                    : tar,
              ),
            );
            response.emit("end");
          });
          return request;
        },
      };
    return require(name);
  };
  vm.runInNewContext(workerData.source, {
    require: fakeRequire,
    Buffer,
    console: {
      log: (message) => messages.push(message),
      error: (message) => messages.push(message),
    },
    process: {
      env,
      platform: "linux",
      arch,
      pid: 123,
      exit: (code) => {
        exits.push(code);
        finish();
        process.exit(code);
      },
    },
  });
  await new Promise((resolve) => setImmediate(resolve));
  finish();
}

function runInstaller(options = {}) {
  const worker = new Worker(
    `
    const { EventEmitter } = require("node:events");
    const vm = require("node:vm");
    const path = require("node:path");
    const { gzipSync } = require("node:zlib");
    const { workerData } = require("node:worker_threads");
    const packageDir = workerData.packageDir;
    const helperSource = workerData.helperSource;
    const loadHelper = ${loadHelper.toString()};
    (${installerWorker.toString()})(workerData.options);
  `,
    {
      eval: true,
      workerData: { source: installSource, helperSource, packageDir, options },
    },
  );
  return new Promise((resolve, reject) => {
    let result;
    worker.on("message", (message) => {
      result = message;
    });
    worker.on("error", reject);
    worker.on("exit", (code) => {
      if (!result) reject(new Error(`Installer worker exited with ${code}`));
      else resolve({ ...result, exitCode: code });
    });
  });
}

test("postinstall skips downloads for installed dependencies or explicit overrides", async () => {
  for (const options of [
    { installed: true },
    { env: { BT_SKIP_DOWNLOAD: "1" } },
    { env: { BT_SKIP_DOWNLOAD: "1" }, unsupported: true },
    { env: { BT_BINARY_PATH: "/custom/bt" } },
    { env: { BT_BINARY_PATH: "/custom/bt" }, unsupported: true },
  ]) {
    const result = await runInstaller(options);
    assert.equal(result.requests.length, 0);
    assert.deepEqual(result.exits, [0]);
    assert.equal(result.exitCode, 0);
  }
});

test("postinstall downloads the exact pinned binary and makes it executable", async () => {
  const result = await runInstaller();
  assert.equal(result.exitCode, 0);
  assert.deepEqual(result.requests, [
    "https://registry.npmjs.org/@braintrust/bt-linux-x64/-/bt-linux-x64-1.2.3.tgz",
  ]);
  assert.deepEqual(result.writes, [
    { file: "/fallback/bt.123.tmp", content: result.binary, mode: 0o755 },
  ]);
  assert.deepEqual(result.renames, [
    { from: "/fallback/bt.123.tmp", to: "/fallback/bt" },
  ]);
  assert.deepEqual(result.removals, ["/fallback/bt.123.tmp"]);
});

test("postinstall downloads the pinned version when an older hoisted binary exists", async () => {
  const result = await runInstaller({
    installed: true,
    installedVersion: "1.2.2",
  });
  assert.equal(result.exitCode, 0);
  assert.deepEqual(result.requests, [
    "https://registry.npmjs.org/@braintrust/bt-linux-x64/-/bt-linux-x64-1.2.3.tgz",
  ]);
  assert.equal(result.writes.length, 1);
  assert.equal(result.renames.length, 1);
  assert.match(result.messages, /Expected .*@1\.2\.3, but found 1\.2\.2/);
});

test("postinstall fails without downloading a glibc binary on ARM64 musl", async () => {
  const result = await runInstaller({ arch: "arm64", libc: "musl" });
  assert.deepEqual(result.requests, []);
  assert.deepEqual(result.writes, []);
  assert.deepEqual(result.exits, [1]);
  assert.equal(result.exitCode, 1);
  assert.match(result.messages, /no prebuilt binary available for linux-arm64/);
});

test("postinstall fails for unsupported platforms and download failures", async () => {
  for (const [options, message] of [
    [{ unsupported: true }, /no prebuilt binary/],
    [{ missingVersion: true }, /cannot determine which version/],
    [{ status: 503 }, /status code 503/],
    [{ networkError: true }, /offline/],
    [{ invalidTar: true }, /could not find/],
    [{ malformedTar: true }, /Invalid or truncated tar entry/],
    [{ truncatedTar: true }, /Invalid or truncated tar entry/],
    [{ badChecksum: true }, /Checksum validation failed/],
    [{ missingChecksum: true }, /No release checksum found/],
    [{ writeError: true }, /disk full/],
  ]) {
    const result = await runInstaller(options);
    assert.deepEqual(result.exits, [1]);
    assert.equal(result.exitCode, 1);
    assert.equal(result.writes.length, 0);
    assert.equal(result.renames.length, 0);
    if (options.writeError)
      assert.deepEqual(result.removals, ["/fallback/bt.123.tmp"]);
    assert.match(result.messages, message);
    assert.doesNotMatch(result.messages, /SDK is unaffected/);
  }
});

function launch(args, options = {}) {
  return spawn(process.execPath, [path.join(packageDir, "bin/bt"), ...args], {
    env: { ...process.env, BT_BINARY_PATH: process.execPath },
    ...options,
  });
}

test("launcher forwards arguments, stdin, stdout, stderr, and exit codes", async () => {
  const child = launch([
    "-e",
    'process.stdin.on("data", data => { process.stdout.write(JSON.stringify(process.argv.slice(1))); process.stdout.write(data); process.stderr.write("diagnostic"); process.exitCode = 7; });',
    "--",
    "argument with spaces",
    "--flag",
  ]);
  let stdout = "";
  let stderr = "";
  child.stdout.on("data", (chunk) => {
    stdout += chunk;
  });
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  const completed = new Promise((resolve, reject) => {
    child.on("error", reject);
    child.on("close", resolve);
  });
  child.stdin.end("input");
  assert.equal(await completed, 7);
  assert.equal(stdout, '["argument with spaces","--flag"]input');
  assert.equal(stderr, "diagnostic");
});

test("launcher reports spawn failures", async () => {
  const child = launch([], {
    env: {
      ...process.env,
      BT_BINARY_PATH: path.join(os.tmpdir(), "bt-test-nonexistent", "bt"),
    },
  });
  let stderr = "";
  child.stderr.on("data", (chunk) => {
    stderr += chunk;
  });
  assert.equal(await new Promise((resolve) => child.on("close", resolve)), 1);
  assert.match(stderr, /ENOENT/);
});

for (const signal of ["SIGINT", "SIGTERM"]) {
  test(
    `launcher forwards and re-raises ${signal}`,
    { skip: process.platform === "win32", timeout: 10000 },
    async (t) => {
      const child = launch([
        "-e",
        `process.on("${signal}", () => { process.stderr.write("forwarded"); process.removeAllListeners("${signal}"); process.kill(process.pid, "${signal}"); }); process.stdout.write("ready"); setInterval(() => {}, 1000);`,
      ]);
      t.after(() => child.kill("SIGKILL"));
      let stderr = "";
      child.stderr.on("data", (chunk) => {
        stderr += chunk;
      });
      const completed = new Promise((resolve) =>
        child.on("close", (code, receivedSignal) =>
          resolve({ code, signal: receivedSignal }),
        ),
      );
      await new Promise((resolve) => child.stdout.once("data", resolve));
      child.kill(signal);
      assert.deepEqual(await completed, { code: null, signal });
      assert.equal(stderr, "forwarded");
    },
  );
}
