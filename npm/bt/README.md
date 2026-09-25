# Braintrust CLI (`bt`)

Install the CLI globally:

```bash
npm install -g @braintrust/bt
bt --version
```

Or add it to a project:

```bash
npm install --save-dev @braintrust/bt
npx bt --help
```

Run without adding a project dependency:

```bash
npx @braintrust/bt --help
```

Use `@braintrust/bt@<version>` to select a specific stable CLI release.
The CLI is distributed separately from the `braintrust` JavaScript SDK.
See the [CLI documentation](https://github.com/braintrustdata/bt#readme) for commands and configuration.

## Installation behavior

The package installs the matching `@braintrust/bt-*` binary as an optional dependency.
Prebuilt binaries are available for macOS arm64/x64, Linux glibc arm64/x64, Linux musl x64, and Windows arm64/x64.
`bt eval` currently supports Linux and macOS only.

If optional dependencies are omitted, the postinstall script attempts to download the matching binary from the public npm registry.
The fallback binary is verified against a SHA-256 checksum shipped with this package before it is installed.
Download, checksum, and file-write failures fail installation with a nonzero exit code.
Unsupported platforms and missing release metadata also fail installation.
The launcher reports an error if no binary is available when invoked.
When install scripts are disabled, optional dependencies must be installed for the CLI to work.

| Optional dependencies | Install scripts | Result                                                              |
| --------------------- | --------------- | ------------------------------------------------------------------- |
| Enabled               | Enabled         | Use the platform package; no fallback download.                     |
| Enabled               | Disabled        | Use the platform package.                                           |
| Disabled              | Enabled         | Download and verify the fallback binary.                            |
| Disabled              | Disabled        | Installation succeeds, but running `bt` reports the missing binary. |

If both features were disabled, reinstall with `npm install --include=optional @braintrust/bt` (add `--global` for a global installation).
When moving a project between operating systems or architectures, reinstall dependencies on the destination instead of copying `node_modules`.

The launcher and installer retain two compatibility overrides from the JavaScript SDK:

- `BT_BINARY_PATH` selects an existing executable instead of the packaged binary and skips the postinstall download.
- `BT_SKIP_DOWNLOAD=1` disables the postinstall fallback download.

These apply before the native CLI runs; native CLI configuration remains available through its flags and environment variables.

## Update and uninstall

Update a global installation:

```bash
npm install -g @braintrust/bt@latest
```

For a project installation, use `npm install --save-dev @braintrust/bt@latest`.
Use npm to update npm installations; `bt update` is for official installer installations.

Uninstall globally with `npm uninstall -g @braintrust/bt`, or from a project with `npm uninstall @braintrust/bt`.
