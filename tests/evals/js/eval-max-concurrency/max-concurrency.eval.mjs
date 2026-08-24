import { appendFileSync } from "node:fs";
import { Eval } from "braintrust";

const outputPath = process.env.BT_MAX_CONCURRENCY_TEST_OUT;

function registerEvaluator(name) {
  Eval(`test-max-concurrency-${name}`, {
    data: () => [{ input: name }],
    task: async (input) => {
      if (outputPath) {
        appendFileSync(outputPath, `start:${name}\n`);
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
      if (outputPath) {
        appendFileSync(outputPath, `end:${name}\n`);
      }
      return input;
    },
    scores: [],
  });
}

registerEvaluator("alpha");
registerEvaluator("beta");
registerEvaluator("gamma");
