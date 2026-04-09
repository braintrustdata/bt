type EvalContext = {
  runEval: (
    projectName: string,
    evaluator: Record<string, unknown>,
  ) => Promise<unknown>;
};

type ScoreArgs = {
  output: string;
  expected: string;
};

function exactMatch({ output, expected }: ScoreArgs) {
  return output === expected ? 1 : 0;
}

type EvalInput = {
  text: string;
  shouldFail: boolean;
};

export async function btEvalMain(ctx: EvalContext) {
  const evaluator = {
    evalName: "direct-basic",
    records: [
      { input: { text: "sample-0", shouldFail: true }, expected: "sample-0" },
      { input: { text: "sample-1", shouldFail: true }, expected: "sample-1" },
      { input: { text: "sample-2", shouldFail: true }, expected: "sample-2" },
      { input: { text: "sample-3", shouldFail: true }, expected: "sample-3" },
      { input: { text: "sample-4", shouldFail: true }, expected: "sample-4" },
      { input: { text: "sample-5", shouldFail: true }, expected: "sample-5" },
      { input: { text: "sample-6", shouldFail: true }, expected: "sample-6" },
      { input: { text: "sample-7", shouldFail: true }, expected: "sample-7" },
      { input: { text: "sample-8", shouldFail: true }, expected: "sample-8" },
      { input: { text: "sample-9", shouldFail: false }, expected: "sample-9" },
    ],
    data() {
      return this.records;
    },
    task: (input: EvalInput) => {
      if (input.shouldFail) {
        throw new Error("intentional fixture failure");
      }
      return input.text;
    },
    scores: [exactMatch],
  };

  await ctx.runEval("BT CLI Tests", evaluator);
}
