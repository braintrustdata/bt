import { appendFileSync } from "node:fs";
import { Eval } from "braintrust";
import { z } from "zod";

const outPath = process.env.BT_MATRIX_TEST_OUT;

Eval("matrix-single", {
  parameters: {
    model: z.string().default("default-model"),
    enableBashTool: z.boolean().default(false),
  },
  data: () => [{ input: "x", expected: "x" }],
  task: (input, hooks) => {
    const { model, enableBashTool } = hooks.parameters;
    if (outPath) {
      appendFileSync(outPath, `${model}|${enableBashTool}\n`);
    }
    return input;
  },
  scores: [],
});
