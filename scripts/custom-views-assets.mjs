import { buildSync } from "esbuild";

// Embed the released SDK entry point, including any internal chunks it imports.
buildSync({
  entryPoints: ["braintrust/custom-views"],
  outfile: process.argv[2],
  bundle: true,
  format: "esm",
  platform: "browser",
});
