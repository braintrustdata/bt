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

export async function btEvalMain(ctx: EvalContext) {
  await ctx.runEval("BT CLI Tests", {
    evalName: "direct-basic",
    data: () => [
      { input: "Cara", expected: "Hello Cara" },
      { input: "Dan", expected: "Hello Dan" },
    ],
    task: (input: string) => `Hello ${input}`,
    scores: [exactMatch],
  });
}
