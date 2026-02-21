import { Eval } from "braintrust";
import { hello } from "esm-only-pkg";

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

Eval("test-cli-eval-ts-esm-only-dep", {
  experimentName: "ESM Only Dep Test",
  data: () => [{ input: "test", expected: hello() }],
  task: async (input: string) => input,
  scores: [exactMatch],
});
