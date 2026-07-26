import { Eval } from "braintrust";
import { z } from "zod";

Eval("test-params-kv", {
  parameters: {
    model: z.string().default("gpt-3.5-turbo"),
    count: z.number().default(1),
    enabled: z.boolean().default(false),
  },
  data: () => [{ input: "test", expected: "test" }],
  task: (input, hooks) => {
    const { model, count, enabled } = hooks.parameters;
    if (model !== "gpt-4o") {
      throw new Error(`Expected model "gpt-4o", got ${JSON.stringify(model)}`);
    }
    if (count !== 5) {
      throw new Error(`Expected count 5, got ${JSON.stringify(count)}`);
    }
    if (enabled !== true) {
      throw new Error(`Expected enabled true, got ${JSON.stringify(enabled)}`);
    }
    return input;
  },
  scores: [],
});
