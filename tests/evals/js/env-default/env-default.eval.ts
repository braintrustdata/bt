import { Eval } from "braintrust";

const EXPECTED = "from-dotenv-development-local";

Eval("BT CLI Env Default", {
  evalName: "env-default",
  data: () => [{ input: null, expected: EXPECTED }],
  task: () => {
    const value = process.env.BT_FIXTURE_ENV_ORDER;
    if (value !== EXPECTED) {
      throw new Error(
        `BT_FIXTURE_ENV_ORDER expected ${EXPECTED}, got ${value ?? "<missing>"}`,
      );
    }
    return value;
  },
  scores: [
    ({ output, expected }: { output: string; expected: string }) =>
      output === expected ? 1 : 0,
  ],
});
