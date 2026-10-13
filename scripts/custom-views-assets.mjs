import { buildSync } from "esbuild";

// Embed the released SDK entry point, including any internal chunks it imports.
for (const [entryPoint, outfile] of [
  ["braintrust/custom-views", process.argv[2]],
  ["braintrust/preprocessors", process.argv[3]],
]) {
  buildSync({
    entryPoints: [entryPoint],
    outfile,
    bundle: true,
    format: "esm",
    platform: "browser",
  });
}
