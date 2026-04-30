import { Eval } from "braintrust";

Eval("matrix-multi-alpha", {
  data: () => [{ input: "a", expected: "a" }],
  task: (input) => input,
  scores: [],
});

Eval("matrix-multi-beta", {
  data: () => [{ input: "b", expected: "b" }],
  task: (input) => input,
  scores: [],
});
