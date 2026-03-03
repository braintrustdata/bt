// Minimal braintrust SDK mock — CJS bundle matching the real SDK's tsup output.
//
// The __export + exports.X = X pattern is what triggers the vite-node
// compatibility issue: vite-node's module proxy intercepts the exports.X
// assignments and fails when the internal namespace has getter-only properties.
"use strict";
Object.defineProperty(exports, "__esModule", { value: true });

var __defProp = Object.defineProperty;
var __export = function (target, all) {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};

var exports_exports = {};
__export(exports_exports, {
  Eval: function () {
    return Eval;
  },
  login: function () {
    return login;
  },
  initDataset: function () {
    return initDataset;
  },
  invoke: function () {
    return invoke;
  },
  defaultErrorScoreHandler: function () {
    return defaultErrorScoreHandler;
  },
});

async function Eval(name, evaluator) {
  if (globalThis._lazy_load) {
    if (globalThis._evals) {
      globalThis._evals.evaluators[name] = {
        evaluator: { evalName: name, projectName: name, ...evaluator },
      };
    }
    return;
  }

  var data =
    typeof evaluator.data === "function"
      ? await evaluator.data()
      : evaluator.data;
  var results = [];
  for (var i = 0; i < data.length; i++) {
    var item = data[i];
    try {
      var output = await evaluator.task(item.input);
      var scores = [];
      var scorers = evaluator.scores || [];
      for (var j = 0; j < scorers.length; j++) {
        scores.push(
          await scorers[j]({
            output: output,
            expected: item.expected,
            input: item.input,
          }),
        );
      }
      results.push({ output: output, scores: scores });
    } catch (error) {
      results.push({ error: error });
    }
  }
  return {
    results: results,
    summary: { projectName: name, experimentName: evaluator.experimentName },
  };
}

async function login() {}
function initDataset() {}
function invoke() {}
function defaultErrorScoreHandler() {
  return { name: "error", score: 0 };
}

exports.default = exports_exports;
exports.Eval = Eval;
exports.login = login;
exports.initDataset = initDataset;
exports.invoke = invoke;
exports.defaultErrorScoreHandler = defaultErrorScoreHandler;
