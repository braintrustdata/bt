import { Eval } from "braintrust";

const exactMatch = ({
  output,
  expected,
}: {
  output: string;
  expected?: string;
}) => ({
  name: "exact_match",
  score: output === expected ? 1 : 0,
});

const testFilePath = "./test-data.txt";
await Bun.write(testFilePath, "Hello from Bun!");

Eval("test-cli-eval-bun", {
  experimentName: "Bun File API Test",
  data: async () => {
    const file = Bun.file(testFilePath);
    const content = await file.text();

    return [
      { input: "file_content", expected: content },
      { input: "file_size", expected: String(file.size) },
    ];
  },
  task: async (input: string) => {
    const file = Bun.file(testFilePath);

    if (input === "file_content") {
      return await file.text();
    }
    if (input === "file_size") {
      return String(file.size);
    }
    return "";
  },
  scores: [exactMatch],
});
