import { Eval } from "braintrust";
import { greetAll } from "@repo/lib";

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

Eval("test-cli-eval-ts-esm-monorepo", {
  experimentName: "ESM Monorepo Test",
  data: () => [
    { input: "Alice", expected: "Hello Alice" },
    { input: "Bob", expected: "Hello Bob" },
  ],
  task: async (input: string) => greetAll([input])[0],
  scores: [exactMatch],
});
