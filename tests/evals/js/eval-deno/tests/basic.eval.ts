import { Eval } from "braintrust";
import { cases } from "./helper.ts";

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

Eval("test-cli-eval-deno", {
  experimentName: "Deno basic eval",
  data: cases,
  task: async (input: string) => `Hello ${input}`,
  scores: [exactMatch],
});
