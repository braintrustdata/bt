# @braintrust/bt

The [Braintrust](https://www.braintrust.dev) command-line interface (`bt`),
distributed as an npm package.

## Install

```bash
npm install -g @braintrust/bt
# or
pnpm add -g @braintrust/bt
```

Then:

```bash
bt --help
```

## How it works

This package is a thin Node.js launcher. The actual `bt` binary is shipped via
platform-specific packages declared as `optionalDependencies`:

- `@braintrust/bt-darwin-arm64`
- `@braintrust/bt-darwin-x64`
- `@braintrust/bt-linux-arm64`
- `@braintrust/bt-linux-x64`
- `@braintrust/bt-linux-x64-musl`
- `@braintrust/bt-win32-arm64`
- `@braintrust/bt-win32-x64`

`npm/pnpm/yarn` install only the one matching your OS + CPU (+ libc on Linux).

If your platform isn't supported, see
<https://github.com/braintrustdata/bt> for alternative installation methods
(shell installer, building with `cargo`).

## License

Apache-2.0
