import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { createRequire } from "node:module";
import {
  cpSync,
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";

const npmDir = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const targets = Object.fromEntries(
  Object.entries(
    JSON.parse(readFileSync(join(npmDir, "targets.json"), "utf8")),
  ).map(([target, platform]) => {
    const pkg = JSON.parse(
      readFileSync(
        join(npmDir, "platforms", `bt-${platform}`, "package.json"),
        "utf8",
      ),
    );
    return [
      target,
      {
        pkg: platform,
        os: pkg.os[0],
        cpu: pkg.cpu[0],
        libc: pkg.libc?.[0],
        bin: pkg.os[0] === "win32" ? "bt.exe" : "bt",
        archiveExt: pkg.os[0] === "win32" ? "zip" : "tar.gz",
      },
    ];
  }),
);
const version = "1.2.3";

test(
  "builds and installs the standalone package from release archives",
  { skip: process.platform !== "linux" },
  async (t) => {
    const root = mkdtempSync(join(tmpdir(), "bt-npm-test-"));
    t.after(() => rmSync(root, { recursive: true, force: true }));
    const archives = join(root, "archives");
    const out = join(root, "dist");
    mkdirSync(archives);
    const stub = `#!/bin/sh\nprintf 'bt ${version}\\n'\n`;
    for (const [target, spec] of Object.entries(targets)) {
      const staging = join(root, `bt-${target}`);
      mkdirSync(staging);
      writeFileSync(join(staging, spec.bin), stub);
      const archive = join(archives, `bt-${target}.${spec.archiveExt}`);
      if (spec.archiveExt === "zip") {
        execFileSync("python3", ["-m", "zipfile", "-c", archive, spec.bin], {
          cwd: staging,
        });
      } else {
        execFileSync("tar", ["-czf", archive, "-C", root, `bt-${target}`]);
      }
    }
    const registry = join(root, "registry.mjs");
    writeFileSync(
      registry,
      "globalThis.fetch = async () => new Response(null, { status: 404 });\n",
    );
    const args = [
      "--import",
      registry,
      join(npmDir, "scripts/build-platform-packages.mjs"),
      "--version",
      version,
      "--archives-dir",
      archives,
      "--out-dir",
      out,
    ];
    execFileSync(process.execPath, args);
    const wrapper = JSON.parse(
      readFileSync(join(out, "bt/package.json"), "utf8"),
    );
    const artifacts = join(out, "artifacts");
    const manifest = JSON.parse(
      readFileSync(join(artifacts, "release-manifest.json"), "utf8"),
    );
    const env = { ...process.env, npm_config_cache: join(root, "cache") };
    delete env.BT_BINARY_PATH;
    delete env.BT_SKIP_DOWNLOAD;

    await t.test(
      "release manifest orders packed platforms before the wrapper with complete npm SBOMs",
      () => {
        const platformNames = Object.keys(wrapper.optionalDependencies);
        assert.deepEqual(
          manifest.packages.map((pkg) => pkg.name),
          [...platformNames, wrapper.name],
        );
        for (const pkg of manifest.packages) {
          assert.equal(pkg.version, version);
          const tarball = join(artifacts, pkg.tarball_asset);
          const packed = JSON.parse(
            execFileSync("tar", ["-xOf", tarball, "package/package.json"], {
              encoding: "utf8",
            }),
          );
          assert.equal(packed.name, pkg.name);
          assert.equal(packed.version, version);
          const files = execFileSync("tar", ["-tf", tarball], {
            encoding: "utf8",
          });
          assert.doesNotMatch(files, /node_modules/);
          const sbom = JSON.parse(
            readFileSync(join(artifacts, pkg.sbom_asset), "utf8"),
          );
          assert.equal(sbom.bomFormat, "CycloneDX");
          assert.equal(
            sbom.metadata.component["bom-ref"],
            `${pkg.name}@${version}`,
          );
          assert.equal(sbom.metadata.component.version, version);
          const expectedDeps = pkg.name === wrapper.name ? platformNames : [];
          assert.deepEqual(
            sbom.components.map((component) => component["bom-ref"]).sort(),
            expectedDeps.map((name) => `${name}@${version}`).sort(),
          );
          for (const component of sbom.components) {
            assert.equal(component.version, version);
          }
          assert.deepEqual(
            sbom.dependencies
              .find(
                (dependency) =>
                  dependency.ref === sbom.metadata.component["bom-ref"],
              )
              .dependsOn.sort(),
            expectedDeps.map((name) => `${name}@${version}`).sort(),
          );
        }
        assert.equal(existsSync(join(out, "bt/node_modules")), false);
      },
    );

    await t.test(
      "wrapper pins every platform and includes only the runtime files",
      () => {
        assert.equal(wrapper.name, "@braintrust/bt");
        assert.equal(wrapper.version, version);
        assert.equal(wrapper.private, undefined);
        assert.deepEqual(wrapper.bin, { bt: "./bin/bt" });
        assert.equal(wrapper.scripts.postinstall, "node ./scripts/install.js");
        assert.deepEqual(
          wrapper.optionalDependencies,
          Object.fromEntries(
            Object.values(targets).map((spec) => [
              `@braintrust/bt-${spec.pkg}`,
              version,
            ]),
          ),
        );
        assert.equal(statSync(join(out, "bt/bin/bt")).mode & 0o777, 0o755);
        assert.equal(existsSync(join(out, ".staging")), false);
        const checksums = JSON.parse(
          readFileSync(join(out, "bt/checksums.json"), "utf8"),
        );
        assert.equal(
          Object.keys(checksums).length,
          Object.keys(targets).length,
        );
        for (const spec of Object.values(targets)) {
          const dir = join(out, `bt-${spec.pkg}`);
          const pkg = JSON.parse(
            readFileSync(join(dir, "package.json"), "utf8"),
          );
          assert.equal(pkg.version, version);
          assert.deepEqual(pkg.os, [spec.os]);
          assert.deepEqual(pkg.cpu, [spec.cpu]);
          assert.deepEqual(pkg.libc, spec.libc ? [spec.libc] : undefined);
          assert.equal(readFileSync(join(dir, "bin", spec.bin), "utf8"), stub);
          assert.equal(
            checksums[pkg.name],
            createHash("sha256").update(stub).digest("hex"),
          );
          if (spec.os !== "win32")
            assert.equal(
              statSync(join(dir, "bin", spec.bin)).mode & 0o777,
              0o755,
            );
        }
        const [pack] = JSON.parse(
          execFileSync(
            "npm",
            ["pack", "--dry-run", "--json", "--ignore-scripts"],
            { cwd: join(out, "bt"), env, encoding: "utf8" },
          ),
        );
        assert.deepEqual(
          pack.files.map(({ path }) => path).sort(),
          [
            "LICENSE",
            "README.md",
            "bin/bt",
            "checksums.json",
            "package.json",
            "scripts/bt-helper.js",
            "scripts/install.js",
          ].sort(),
        );
      },
    );

    const helper = createRequire(import.meta.url)(
      join(npmDir, "bt/scripts/bt-helper.js"),
    );
    const hostName = helper
      .getDistributionForThisPlatform()
      .packageName.split("/")[1];
    const tarballs = ["@braintrust/bt", `@braintrust/${hostName}`].map((name) =>
      join(
        artifacts,
        manifest.packages.find((pkg) => pkg.name === name).tarball_asset,
      ),
    );

    await t.test(
      "local and global packed installs work with lifecycle scripts disabled",
      () => {
        const project = join(root, "project");
        mkdirSync(project);
        writeFileSync(
          join(project, "package.json"),
          JSON.stringify({ name: "test-project", private: true }),
        );
        const installArgs = [
          "install",
          "--ignore-scripts",
          "--offline",
          "--no-audit",
          "--no-fund",
          ...tarballs,
        ];
        execFileSync("npm", installArgs, { cwd: project, env, stdio: "pipe" });
        const output = execFileSync(
          "npm",
          ["exec", "--offline", "--", "bt", "--version"],
          { cwd: project, env, encoding: "utf8" },
        );
        assert.equal(output.trim(), `bt ${version}`);
        const prefix = join(root, "global");
        execFileSync("npm", [...installArgs, "--global", "--prefix", prefix], {
          cwd: root,
          env,
          stdio: "pipe",
        });
        assert.equal(
          execFileSync(join(prefix, "bin/bt"), ["--version"], {
            env,
            encoding: "utf8",
          }).trim(),
          `bt ${version}`,
        );
      },
    );

    await t.test(
      "disabling both optional dependencies and scripts gives an actionable launch error",
      () => {
        const project = join(root, "without-binary");
        mkdirSync(project);
        writeFileSync(
          join(project, "package.json"),
          JSON.stringify({ name: "test-project", private: true }),
        );
        execFileSync(
          "npm",
          [
            "install",
            "--omit=optional",
            "--ignore-scripts",
            "--offline",
            "--no-audit",
            "--no-fund",
            tarballs[0],
          ],
          { cwd: project, env, stdio: "pipe" },
        );
        const result = spawnSync(
          join(project, "node_modules/.bin/bt"),
          ["--version"],
          { cwd: project, env, encoding: "utf8" },
        );
        assert.equal(result.status, 1);
        assert.equal(result.stdout, "");
        assert.match(
          result.stderr,
          /configured to install optional dependencies/,
        );
        assert.equal(
          existsSync(join(project, "node_modules/@braintrust/bt/scripts/bt")),
          false,
        );
      },
    );

    await t.test(
      "fallback installs and verifies the executable from a packed npm tarball",
      () => {
        const project = join(root, "fallback");
        cpSync(join(out, "bt"), project, { recursive: true });
        const preload = join(root, "registry.cjs");
        // Serve the real packed platform package without accessing the network.
        writeFileSync(
          preload,
          `
          const assert = require("node:assert/strict");
          const { EventEmitter } = require("node:events");
          const { Readable } = require("node:stream");
          const tarball = require("node:fs").readFileSync(${JSON.stringify(tarballs[1])});
          require("node:https").get = (url, callback) => {
            assert.equal(url, ${JSON.stringify(`https://registry.npmjs.org/@braintrust/${hostName}/-/${hostName}-${version}.tgz`)});
            const response = Readable.from([tarball]);
            response.statusCode = 200;
            response.headers = {};
            process.nextTick(() => callback(response));
            return new EventEmitter();
          };
        `,
        );
        execFileSync(
          process.execPath,
          ["--require", preload, join(project, "scripts/install.js")],
          { env, stdio: "pipe" },
        );
        const binary = join(project, "scripts/bt");
        assert.equal(readFileSync(binary, "utf8"), stub);
        assert.equal(statSync(binary).mode & 0o777, 0o755);
        assert.equal(
          execFileSync(join(project, "bin/bt"), ["--version"], {
            env,
            encoding: "utf8",
          }).trim(),
          `bt ${version}`,
        );
      },
    );

    await t.test(
      "a partial release retry hashes the published Windows binary instead of its rebuild",
      () => {
        const [target, spec] = Object.entries(targets).find(
          ([, spec]) => spec.pkg === "win32-x64",
        );
        const name = `@braintrust/bt-${spec.pkg}`;
        const publishedTarball = join(
          artifacts,
          manifest.packages.find((pkg) => pkg.name === name).tarball_asset,
        );
        const rebuilt = `${stub}# changed signing timestamp\n`;
        const staging = join(root, `bt-${target}`);
        writeFileSync(join(staging, spec.bin), rebuilt);
        execFileSync(
          "python3",
          [
            "-m",
            "zipfile",
            "-c",
            join(archives, `bt-${target}.zip`),
            spec.bin,
          ],
          { cwd: staging },
        );
        writeFileSync(
          registry,
          `
          import { readFileSync } from "node:fs";
          globalThis.fetch = async (url) => {
            if (url === ${JSON.stringify(`https://registry.npmjs.org/${name}/-/bt-${spec.pkg}-${version}.tgz`)}) {
              return new Response(readFileSync(${JSON.stringify(publishedTarball)}));
            }
            return new Response(null, { status: 404 });
          };
        `,
        );
        const retryOut = join(root, "retry");
        execFileSync(process.execPath, [...args, "--out-dir", retryOut]);
        const checksums = JSON.parse(
          readFileSync(join(retryOut, "bt/checksums.json"), "utf8"),
        );
        assert.equal(
          readFileSync(join(retryOut, `bt-${spec.pkg}/bin/bt.exe`), "utf8"),
          rebuilt,
        );
        assert.equal(
          checksums[name],
          createHash("sha256").update(stub).digest("hex"),
        );
        assert.notEqual(
          checksums[name],
          createHash("sha256").update(rebuilt).digest("hex"),
        );
        const wrapperTarball = manifest.packages.find(
          (pkg) => pkg.name === "@braintrust/bt",
        ).tarball_asset;
        assert.deepEqual(
          JSON.parse(
            execFileSync(
              "tar",
              [
                "-xOf",
                join(retryOut, "artifacts", wrapperTarball),
                "package/checksums.json",
              ],
              { encoding: "utf8" },
            ),
          ),
          checksums,
        );
      },
    );

    await t.test(
      "registry and published archive failures stop before packing the wrapper",
      () => {
        for (const [response, message] of [
          ["new Response(null, { status: 503 })", /HTTP 503/],
          ['Promise.reject(new Error("offline"))', /offline/],
          ['new Response("invalid tarball")', /Command failed: tar/],
        ]) {
          writeFileSync(
            registry,
            `globalThis.fetch = async () => ${response};\n`,
          );
          const failedOut = join(root, "failed");
          const result = spawnSync(
            process.execPath,
            [...args, "--out-dir", failedOut],
            { encoding: "utf8" },
          );
          assert.notEqual(result.status, 0);
          assert.match(result.stderr, message);
          assert.equal(existsSync(join(failedOut, "bt/package.json")), false);
        }
      },
    );

    await t.test(
      "missing archives fail before emitting a publishable wrapper",
      () => {
        const [target, spec] = Object.entries(targets)[0];
        rmSync(join(archives, `bt-${target}.${spec.archiveExt}`));
        const result = spawnSync(process.execPath, args, { encoding: "utf8" });
        assert.notEqual(result.status, 0);
        assert.match(result.stderr, /Archive not found/);
        assert.equal(existsSync(join(out, "bt/package.json")), false);
      },
    );
  },
);
