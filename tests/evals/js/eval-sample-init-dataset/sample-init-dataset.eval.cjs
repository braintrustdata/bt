const { Eval, initDataset } = require("braintrust");

function exactMatch({ output, expected }) {
  return { name: "exact_match", score: output === expected ? 1 : 0 };
}

Eval("auto-sample-init-dataset-object", {
  data: initDataset({
    project: "test-project",
    dataset: "auto-object",
  }),
  task: async (input) => input,
  scores: [exactMatch],
});

Eval("auto-sample-init-dataset-string", {
  data: initDataset("test-project", {
    dataset: "auto-string",
  }),
  task: async (input) => input,
  scores: [exactMatch],
});

Eval("preserve-explicit-init-dataset-sample", {
  data: initDataset({
    project: "test-project",
    dataset: "explicit-sample",
    _internal_btql: {
      filter: "metadata.kind = 'synthetic'",
      sample: 2,
    },
  }),
  task: async (input) => input,
  scores: [exactMatch],
});
