import { Eval } from "braintrust";
import { isMatch } from "micromatch";

const exactMatch = ({
  output,
  expected,
}: {
  output: string;
  expected?: string;
}) => ({
  name: "exact_match",
  score: output === expected ? 1 : 0,
});

Eval("test-cli-eval-vite-node-cjs", {
  experimentName: "Vite Node CJS Test",
  data: () => [
    { input: "src/foo.ts", expected: "matched" },
    { input: "src/bar.js", expected: "unmatched" },
  ],
  task: async (input: string) =>
    isMatch(input, "**/*.ts") ? "matched" : "unmatched",
  scores: [exactMatch],
});
