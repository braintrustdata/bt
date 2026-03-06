import { Eval } from "braintrust";

const exactMatch = ({ output, expected }) => ({
  name: "exact_match",
  score: output === expected ? 1 : 0,
});

Eval("test-glob-b", {
  data: () => [{ input: "b", expected: "b" }],
  task: async (input) => input,
  scores: [exactMatch],
});
