export async function btEvalMain(ctx) {
  // The runner should error before reaching this code when --matrix-param is set.
  await ctx.runEval("BT CLI Tests", {
    evalName: "bt-eval-main-matrix-error",
    data: () => [{ input: "x", expected: "x" }],
    task: (input) => input,
    scores: [],
  });
}
