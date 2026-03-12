import { Eval } from "braintrust";

const exactMatch = ({
  output,
  expected,
}: {
  output: number;
  expected?: number;
}) => ({
  name: "exact_match",
  score: output === expected ? 1 : 0,
});

Eval("cli-mixed-basic", {
  experimentName: "Mixed Basic Test",
  data: () => [
    { input: 1, expected: 1 },
    { input: 2, expected: 2 },
  ],
  task: async (input: number) => input,
  scores: [exactMatch],
});
