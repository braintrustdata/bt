# @braintrust/cli

The [Braintrust](https://www.braintrust.dev) command-line interface (`bt`),
distributed as an npm package.

## Install

```bash
npm install -g @braintrust/cli
# or
pnpm add -g @braintrust/cli
```

Then:

```bash
bt --help
```

## How it works

This package is a thin Node.js launcher. The actual `bt` binary is shipped via
platform-specific packages declared as `optionalDependencies`:

- `@braintrust/cli-darwin-arm64`
- `@braintrust/cli-darwin-x64`
- `@braintrust/cli-linux-arm64`
- `@braintrust/cli-linux-x64`
- `@braintrust/cli-linux-x64-musl`
- `@braintrust/cli-win32-arm64`
- `@braintrust/cli-win32-x64`

`npm/pnpm/yarn` install only the one matching your OS + CPU (+ libc on Linux).

If your platform isn't supported, see
<https://github.com/braintrustdata/bt> for alternative installation methods
(shell installer, building with `cargo`).

## License

Apache-2.0
