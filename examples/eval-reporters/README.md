# Eval reporter examples

## GitHub Actions

Install this example's dependencies, build `bt`, and run:

```bash
pnpm install --dir examples/eval-reporters
cargo build --bin bt
cd examples/eval-reporters
../../target/debug/bt eval --no-send-logs \
  --reporter=github-actions \
  github-actions.eval.ts
```

The second case intentionally throws. In GitHub Actions, the reporter emits an `::error` workflow command so the case appears as an annotation. The command exits non-zero because the eval contains an errored case; this is expected for this example.
