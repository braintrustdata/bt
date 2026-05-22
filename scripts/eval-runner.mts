// vite-node-only entry. vite-node's CLI awaits the entry module's body via
// runner.executeFile and then immediately closes its dev server. The body
// completes when vite-node's async wrapper around it resolves: immediately if
// the entry fires main() without awaiting it. After server.close() runs, any
// in-flight dynamic imports inside main() throw ERR_CLOSED_SERVER against the
// closed dev environment. Awaiting main() here keeps the wrapper pending
// until the work is actually done.
//
// vite-node v5.3.0 source:
//   https://github.com/antfu-collective/vite-node/blob/v5.3.0/src/cli.ts#L146-L152
//   https://github.com/antfu-collective/vite-node/blob/v5.3.0/src/client.ts#L578-L599
//
// This is the same shape as https://github.com/vitejs/vite/issues/13786 (top-
// level fire-and-forget under vite-node throws ERR_CLOSED_SERVER while pure
// Node runs it fine). That issue was closed by https://github.com/vitejs/vite/
// pull/13787 which silences the error for SSR by default (recoverable=false),
// so the race stays hidden unless the user's Vite config opts back in (e.g.
// via vite-plus).
//
// Every other runner uses the generated default `eval-runner.ts` (impl with a
// fire-and-forget tail). .mts is needed here because top-level await requires
// ESM at the loader for tsx/Bun/Deno; vite-node is format-agnostic but uses
// the same source.
import { main } from "./eval-runner-impl.ts";

try {
  await main();
} catch (err) {
  console.error(err);
  process.exit(1);
}
