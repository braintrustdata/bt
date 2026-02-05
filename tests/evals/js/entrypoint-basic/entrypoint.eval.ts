import { Eval } from "braintrust";

type ScoreArgs = {
  output: string;
  expected: string;
};

function exactMatch({ output, expected }: ScoreArgs) {
  return output === expected ? 1 : 0;
}

Eval("BT CLI Tests", {
  evalName: "entrypoint-basic",
  data: () => [
    { input: "Alice", expected: "Hello Alice" },
    { input: "Bob", expected: "Hello Bob" },
  ],
  task: (input: string) => `Hello ${input}`,
  scores: [exactMatch],
});
