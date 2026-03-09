import { Eval } from "braintrust";

// Verify that args passed after `--` on the bt eval command line are forwarded
// to user code via process.argv.  For example:
//
//   bt eval ./retrieval.eval.ts -- --description "Production Eval" --shard=1/4
//
// should result in process.argv.slice(2) === ["--description", "Production Eval", "--shard=1/4"]
const args = process.argv.slice(2);
const expected = ["--description", "test-desc", "--shard=1/4"];

if (JSON.stringify(args) !== JSON.stringify(expected)) {
  throw new Error(
    `Expected process.argv.slice(2) to be ${JSON.stringify(expected)}, but got ${JSON.stringify(args)}`,
  );
}

const exactMatch = ({ output, expected }) => ({
  name: "exact_match",
  score: output === expected ? 1 : 0,
});

Eval("test-extra-args", {
  data: () => [{ input: "test", expected: "test" }],
  task: async (input) => input,
  scores: [exactMatch],
});
