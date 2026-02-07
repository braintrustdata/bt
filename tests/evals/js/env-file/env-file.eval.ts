import { Eval } from "braintrust";

const EXPECTED = "from-custom-env-file";

Eval("BT CLI Env File", {
  evalName: "env-file",
  data: () => [{ input: null, expected: EXPECTED }],
  task: () => {
    const value = process.env.BT_FIXTURE_ENV_FILE;
    if (value !== EXPECTED) {
      throw new Error(
        `BT_FIXTURE_ENV_FILE expected ${EXPECTED}, got ${value ?? "<missing>"}`,
      );
    }
    return value;
  },
  scores: [
    ({ output, expected }: { output: string; expected: string }) =>
      output === expected ? 1 : 0,
  ],
});
