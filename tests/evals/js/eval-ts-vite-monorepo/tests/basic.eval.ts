import { Eval } from "braintrust";
import { greetAll } from "@repo/lib";

const exactMatch = ({ output, expected }) => ({
  name: "exact_match",
  score: output === expected ? 1 : 0,
});

Eval("test-cli-eval-ts-vite-monorepo", {
  experimentName: "Vite Monorepo Test",
  data: () => [
    { input: "Alice", expected: "Hello Alice" },
    { input: "Bob", expected: "Hello Bob" },
  ],
  task: async (input) => greetAll([input])[0],
  scores: [exactMatch],
});
