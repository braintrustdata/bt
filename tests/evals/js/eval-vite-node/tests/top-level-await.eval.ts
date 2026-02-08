import { Eval } from "braintrust";

// ESM-only feature: verifies the runner can load top-level await.
const config = await Promise.resolve({ prefix: "Result: " });

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

Eval("test-cli-eval-vite-node", {
  experimentName: "Top-Level Await Test",
  data: () => [{ input: "test", expected: "Result: test" }],
  task: async (input: string) => config.prefix + input,
  scores: [exactMatch],
});
