export function cases() {
  return [
    { input: "Alice", expected: "Hello Alice" },
    { input: "Bob", expected: "Hello Bob" },
  ];
}

export const exactMatch = ({
  output,
  expected,
}: {
  output: string;
  expected?: string;
}) => ({
  name: "exact_match",
  score: output === expected ? 1 : 0,
});
