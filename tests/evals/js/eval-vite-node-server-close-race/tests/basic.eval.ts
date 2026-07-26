import { Eval } from "braintrust";
// Local TS import that needs vite's transform pipeline (via vite-node's
// fetchModule). When the eval-runner fires main() without awaiting it,
// vite-node closes its dev server before this import is resolved and the
// transform throws ERR_CLOSED_SERVER.
import { exactMatch } from "../utils/scorer";

Eval("test-cli-eval-vite-node-server-close-race", {
  experimentName: "vite-node server-close race",
  data: () => [{ input: "test", expected: "test" }],
  task: async (input: string) => input,
  scores: [exactMatch],
});
