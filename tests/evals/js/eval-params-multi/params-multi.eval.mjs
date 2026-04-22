import { Eval } from "braintrust";
import { z } from "zod";

Eval("test-params-multi-a", {
  parameters: {
    model: z.string().default("gpt-3.5-turbo"),
    count: z.number().default(1),
  },
  data: () => [{ input: "a", expected: "a" }],
  task: (input, hooks) => {
    const params = hooks.parameters;
    if (params.model !== "gpt-4o") {
      throw new Error(
        `A: expected model "gpt-4o", got ${JSON.stringify(params.model)}`,
      );
    }
    if (params.count !== 5) {
      throw new Error(
        `A: expected count 5, got ${JSON.stringify(params.count)}`,
      );
    }
    if ("enabled" in params) {
      throw new Error(
        `A: unexpected param 'enabled' leaked in: ${JSON.stringify(params)}`,
      );
    }
    return input;
  },
  scores: [],
});

Eval("test-params-multi-b", {
  parameters: {
    enabled: z.boolean().default(false),
  },
  data: () => [{ input: "b", expected: "b" }],
  task: (input, hooks) => {
    const params = hooks.parameters;
    if (params.enabled !== true) {
      throw new Error(
        `B: expected enabled true, got ${JSON.stringify(params.enabled)}`,
      );
    }
    if ("model" in params || "count" in params) {
      throw new Error(
        `B: unexpected params leaked in: ${JSON.stringify(params)}`,
      );
    }
    return input;
  },
  scores: [],
});
