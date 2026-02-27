import { Eval } from "braintrust";
import { z } from "zod";

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

Eval("test-cli-remote-list-params", {
  experimentName: "Remote List Params",
  data: () => [{ input: "test", expected: "test" }],
  task: async (input: string) => input,
  scores: [exactMatch],
  parameters: {
    optional_no_default: z
      .string()
      .optional()
      .describe("Optional prompt override"),
    with_default: z.string().default("default text").describe("Prompt default"),
  },
});
