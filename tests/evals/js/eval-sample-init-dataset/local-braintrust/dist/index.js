"use strict";

function makeRecords(count, prefix) {
  return Array.from({ length: count }, (_, index) => ({
    input: `${prefix}-${index}`,
    expected: `${prefix}-${index}`,
  }));
}

function datasetArgs(projectOrOptions, options) {
  if (typeof projectOrOptions === "string") {
    return {
      project: projectOrOptions,
      ...(options || {}),
    };
  }
  return projectOrOptions || {};
}

function internalBtqlWithDefaultSample(value) {
  const sample = globalThis.__bt_eval_sample_rate;
  if (sample === undefined) {
    return value;
  }
  if (value === undefined) {
    return { sample };
  }
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return value;
  }
  if (Object.prototype.hasOwnProperty.call(value, "sample")) {
    return value;
  }
  return { ...value, sample };
}

async function Eval(projectName, evaluator) {
  const evalName = evaluator.evalName || projectName;
  if (globalThis._lazy_load) {
    globalThis._evals.evaluators[evalName] = {
      evaluator: { evalName, projectName, ...evaluator },
    };
    return;
  }

  const data =
    typeof evaluator.data === "function"
      ? await evaluator.data.call(evaluator)
      : await evaluator.data;
  const results = [];
  for (const item of data) {
    try {
      const output = await evaluator.task(item.input);
      const scores = [];
      for (const scorer of evaluator.scores || []) {
        scores.push(
          await scorer({
            input: item.input,
            output,
            expected: item.expected,
          }),
        );
      }
      results.push({ output, scores });
    } catch (error) {
      results.push({ error });
    }
  }
  return {
    results,
    summary: { projectName, experimentName: evaluator.experimentName },
  };
}

async function login() {}

function initDataset(projectOrOptions, options) {
  const args = datasetArgs(projectOrOptions, options);
  const btql = internalBtqlWithDefaultSample(args._internal_btql);
  if (args.dataset === "auto-object" || args.dataset === "auto-string") {
    if (!btql || btql.sample !== 5) {
      throw new Error(
        `expected automatic sample 5 for ${args.dataset}, received ${JSON.stringify(btql)}`,
      );
    }
    return makeRecords(5, args.dataset);
  }

  if (args.dataset === "explicit-sample") {
    if (!btql || btql.sample !== 2) {
      throw new Error(
        `expected explicit sample 2 to be preserved, received ${JSON.stringify(btql)}`,
      );
    }
    if (btql.filter !== "metadata.kind = 'synthetic'") {
      throw new Error(
        `expected explicit filter to be preserved, received ${JSON.stringify(btql)}`,
      );
    }
    return makeRecords(2, args.dataset);
  }

  throw new Error(`unexpected dataset ${args.dataset}`);
}

function invoke() {}

const api = {
  Eval,
  login,
  initDataset,
  invoke,
};

exports.default = api;
exports.Eval = Eval;
exports.login = login;
exports.initDataset = initDataset;
exports.invoke = invoke;
