import { Eval } from "braintrust";
import { autoInstrumented } from "esm-instrumentation-target";

if (!autoInstrumented) {
  throw new Error(
    "Braintrust auto-instrumentation did not transform an ESM import",
  );
}

Eval("auto-instrumentation-esm", {
  data: [{ input: "instrumented", expected: "instrumented" }],
  task: async (input) => input,
  scores: [
    ({ output, expected }) => ({
      name: "exact_match",
      score: output === expected ? 1 : 0,
    }),
  ],
});
