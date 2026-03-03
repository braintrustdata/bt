// Minimal braintrust SDK mock — just enough for the eval-runner to work.

export async function Eval(name, evaluator) {
  if (globalThis._lazy_load) {
    if (globalThis._evals) {
      globalThis._evals.evaluators[name] = {
        evaluator: { evalName: name, projectName: name, ...evaluator },
      };
    }
    return;
  }

  const data =
    typeof evaluator.data === "function"
      ? await evaluator.data()
      : evaluator.data;
  const results = [];
  for (const item of data) {
    try {
      const output = await evaluator.task(item.input);
      const scores = [];
      for (const scorer of evaluator.scores || []) {
        scores.push(
          await scorer({ output, expected: item.expected, input: item.input }),
        );
      }
      results.push({ output, scores });
    } catch (error) {
      results.push({ error });
    }
  }
  return {
    results,
    summary: { projectName: name, experimentName: evaluator.experimentName },
  };
}

export async function login() {}
export function initDataset() {}
export function invoke() {}
export function defaultErrorScoreHandler() {
  return { name: "error", score: 0 };
}
