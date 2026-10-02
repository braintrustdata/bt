import fs from "node:fs";
import { Console } from "node:console";
import { pathToFileURL } from "node:url";

function isObject(value) {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function defaultExportFromModule(loaded) {
  if (
    !isObject(loaded) ||
    !Object.prototype.hasOwnProperty.call(loaded, "default")
  ) {
    return undefined;
  }
  const value = loaded.default;
  if (
    isObject(value) &&
    Object.prototype.hasOwnProperty.call(value, "default")
  ) {
    return value.default;
  }
  return value;
}

function validatePreprocessor(value, sourceFile) {
  if (!isObject(value)) {
    throw new Error(
      `${sourceFile} must default-export a preprocessor definition: { name, slug, handler }`,
    );
  }
  if (typeof value.name !== "string" || !value.name.trim()) {
    throw new Error(`${sourceFile} preprocessor name is required`);
  }
  if (typeof value.slug !== "string" || !value.slug.trim()) {
    throw new Error(
      `${sourceFile} preprocessor '${value.name}' slug is required`,
    );
  }
  if (typeof value.handler !== "function") {
    throw new Error(
      `${sourceFile} preprocessor '${value.slug}' handler must be a function`,
    );
  }
  if (
    value.description !== undefined &&
    value.description !== null &&
    typeof value.description !== "string"
  ) {
    throw new Error(
      `${sourceFile} preprocessor '${value.slug}' description must be a string`,
    );
  }
}

function projectFields(project) {
  if (typeof project === "string" && project.trim()) {
    return { project_name: project };
  }
  if (!isObject(project)) {
    return {};
  }
  const fields = {};
  if (typeof project.id === "string" && project.id.trim()) {
    fields.project_id = project.id;
  }
  if (typeof project.name === "string" && project.name.trim()) {
    fields.project_name = project.name;
  }
  return fields;
}

async function buildManifest(inputPath) {
  const input = JSON.parse(fs.readFileSync(inputPath, "utf8"));
  if (!Array.isArray(input.files) || input.files.length === 0) {
    throw new Error(
      "preprocessors metadata runner requires at least one bundled preprocessor",
    );
  }

  // User modules may log while being imported. Keep the manifest on stdout.
  globalThis.console = new Console(process.stderr, process.stderr);
  const manifest = {
    runtime_context: {
      runtime: "quickjs",
      version: "latest",
    },
    files: [],
  };

  for (const file of input.files) {
    if (!isObject(file)) {
      throw new Error(
        "preprocessors metadata input file entries must be objects",
      );
    }
    const { source_file: sourceFile, bundle_file: bundleFile } = file;
    if (typeof sourceFile !== "string" || typeof bundleFile !== "string") {
      throw new Error(
        "preprocessors metadata input entries require source_file and bundle_file",
      );
    }
    const loaded = await import(
      `${pathToFileURL(bundleFile).href}?bt_preprocessor_nonce=${Date.now()}`
    );
    const preprocessor = defaultExportFromModule(loaded);
    validatePreprocessor(preprocessor, sourceFile);
    manifest.files.push({
      source_file: sourceFile,
      entries: [
        {
          name: preprocessor.name,
          slug: preprocessor.slug,
          ...(typeof preprocessor.description === "string" &&
          preprocessor.description.trim()
            ? { description: preprocessor.description }
            : {}),
          code: "",
          ...projectFields(preprocessor.project),
        },
      ],
    });
  }

  return manifest;
}

async function main() {
  const inputPath = process.argv[2];
  if (!inputPath) {
    throw new Error(
      "preprocessors metadata runner requires an input path argument",
    );
  }
  const manifest = await buildManifest(inputPath);
  process.stdout.write(`${JSON.stringify(manifest)}\n`);
}

main().catch((error) => {
  const message =
    error instanceof Error
      ? error.message
      : `failed to build preprocessor metadata: ${String(error)}`;
  process.stderr.write(`${message}\n`);
  process.exitCode = 1;
});
