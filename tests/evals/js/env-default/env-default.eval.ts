import { Eval } from "braintrust";

Eval("BT CLI Env Default", {
  evalName: "env-default",
  data: () => [{ input: null }],
  task: () => {
    const value = process.env.BT_FIXTURE_ENV_ORDER;
    if (value !== undefined) {
      throw new Error(`BT_FIXTURE_ENV_ORDER expected <missing>, got ${value}`);
    }
    return "<missing>";
  },
  scores: [
    ({ output }: { output: string }) => (output === "<missing>" ? 1 : 0),
  ],
});
