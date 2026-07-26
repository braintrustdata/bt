import { setTimeout as sleep } from "node:timers/promises";
import type { Plugin } from "vite";

// Slow down transforms slightly to exaggerate the race between vite-node
// closing its dev server and the eval-runner's main() still resolving its
// dynamic imports.
const slowTransform = (): Plugin => ({
  name: "bt-eval-runner-slow-transform",
  enforce: "pre",
  async transform(_code, id) {
    if (id.endsWith(".eval.ts") || id.endsWith("/scorer.ts")) {
      await sleep(100);
    }
    return null;
  },
});

// Enable `dev.recoverable` so the throw-on-closed-server check actually
// fires for our SSR environment. Without it, vite silently lets late
// transforms slip through and the bug doesn't manifest in this fixture.
// Vite wrappers (e.g. vite-plus) enable this in their default setup, which
// is why the bug surfaces in real-world projects but not the vanilla
// vite-node CLI defaults.
export default {
  plugins: [slowTransform()],
  environments: {
    ssr: {
      dev: {
        recoverable: true,
      },
    },
  },
};
