import { Eval } from "braintrust";
import { z } from "zod";

Eval("test-params-json-obj", {
  parameters: {
    model: z.string().default("gpt-3.5-turbo"),
    count: z.number().default(1),
  },
  data: () => [{ input: "test", expected: "test" }],
  task: (input, hooks) => {
    const { model, count } = hooks.parameters;
    if (model !== "gpt-4o") {
      throw new Error(`Expected model "gpt-4o", got ${JSON.stringify(model)}`);
    }
    if (count !== 5) {
      throw new Error(`Expected count 5, got ${JSON.stringify(count)}`);
    }
    return input;
  },
  scores: [],
});
