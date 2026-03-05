import { Eval } from "braintrust";
import { parseArgs } from "node:util";

// Verify that process.argv does not contain eval file paths or any other
// positional arguments. The runner passes multiple files on its argv
// (e.g. `tsx eval-runner.ts basic.eval.mjs top-level-await.eval.mjs
// parse-args.eval.mjs`), so this also checks that multiple positional
// entries are all stripped, not just the last one.
//
// This would throw ERR_PARSE_ARGS_UNEXPECTED_POSITIONAL if the runner
// leaks file paths into argv (the bug reported by a customer whose eval
// file used parseArgs for --shard / --experiment-name flags).
const remaining = process.argv.slice(2);
if (remaining.length !== 0) {
  throw new Error(
    `Expected process.argv to be stripped of all positional arguments, but got: ${JSON.stringify(remaining)}`,
  );
}
parseArgs({ args: remaining, strict: true, options: {} });

const exactMatch = ({ output, expected }) => ({
  name: "exact_match",
  score: output === expected ? 1 : 0,
});

Eval("test-cli-eval-esm", {
  experimentName: "Parse Args Test",
  data: () => [{ input: "test", expected: "test" }],
  task: async (input) => input,
  scores: [exactMatch],
});
