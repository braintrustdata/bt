import { Eval } from "braintrust";
import { parseArgs } from "node:util";
import { greetAll } from "@repo/lib";

// Verify that process.argv does not contain eval file paths.
// This would throw ERR_PARSE_ARGS_UNEXPECTED_POSITIONAL if the runner
// leaks file paths into argv (the bug reported by a customer whose eval
// file used parseArgs for --shard / --experiment-name flags).
parseArgs({ args: process.argv.slice(2), strict: true, options: {} });

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
