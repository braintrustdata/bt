import { appendFileSync } from "node:fs";
import { Eval } from "braintrust";

const outputPath = process.env.BT_MAX_CONCURRENCY_TEST_OUT;

function registerEvaluator(name) {
  Eval(`test-max-concurrency-sampling-${name}`, {
    data: async () => {
      if (outputPath) {
        appendFileSync(outputPath, `start:${name}\n`);
      }
      await new Promise((resolve) => setTimeout(resolve, 250));
      if (outputPath) {
        appendFileSync(outputPath, `end:${name}\n`);
      }
      return [{ input: name }];
    },
    task: (input) => input,
    scores: [],
  });
}

registerEvaluator("alpha");
registerEvaluator("beta");
registerEvaluator("gamma");
