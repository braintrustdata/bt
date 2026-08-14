if (process.env.BT_TEST_AUTO_INSTRUMENTATION_FAILURE) {
  throw new Error("synthetic auto-instrumentation failure");
}

const { register } = require("node:module");
const { pathToFileURL } = require("node:url");

register("./esm-auto-instrumentation-hook.mjs", {
  parentURL: pathToFileURL(__filename).href,
});
globalThis.__btAutoInstrumentationApplied = true;
