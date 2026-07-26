import { appendFileSync } from "node:fs";
import { Eval } from "braintrust";
import { z } from "zod";

const outPath = process.env.BT_MATRIX_TEST_OUT;

Eval("matrix-terminate", {
  parameters: {
    model: z.string().default("default-model"),
  },
  data: () => [{ input: "x", expected: "x" }],
  task: (input, hooks) => {
    const { model } = hooks.parameters;
    if (outPath) {
      appendFileSync(outPath, `${model}\n`);
    }
    if (model === "fail") {
      throw new Error("intentional failure for terminate test");
    }
    return input;
  },
  scores: [],
});
