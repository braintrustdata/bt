import { Eval } from "braintrust";

type Input = {
  message: string;
  shouldError: boolean;
};

Eval("reporter-example", {
  evalName: "github-actions-reporter-example",
  data: (): Array<{ input: Input; expected: string }> => [
    {
      input: { message: "successful case", shouldError: false },
      expected: "successful case",
    },
    {
      input: { message: "annotated case", shouldError: true },
      expected: "annotated case",
    },
  ],
  task: ({ message, shouldError }: Input): string => {
    if (shouldError) {
      throw new Error("intentional error from the GitHub Actions reporter example");
    }
    return message;
  },
  scores: [
    ({ output, expected }: { output: string; expected: string }): number =>
      output === expected ? 1 : 0,
  ],
});
