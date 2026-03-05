import "./instrument";

import { Eval } from "braintrust";
import { cases, exactMatch } from "./helper";

Eval("test-cli-eval-vite", {
  experimentName: "Vite Test",
  data: cases,
  task: async (input: string) => `Hello ${input}`,
  scores: [exactMatch],
});
