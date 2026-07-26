# Ruby SDK Install

Reference guide for installing the Braintrust Ruby SDK.

- SDK repo: https://github.com/braintrustdata/braintrust-sdk-ruby
- RubyGems: https://rubygems.org/gems/braintrust
- Requires Ruby 3.1+

## Install the SDK

Install the latest published version of the `braintrust` gem. Do not hard-pin the version unless the user asks -- let Bundler record whatever it normally records.

The SDK has three setup approaches. Choose the one that fits the project best.

### Option A: Setup script (recommended for most apps)

Add to the Gemfile with the `require` option. This auto-instruments all supported libraries at load time -- no additional code needed.

```ruby
gem "braintrust", require: "braintrust/setup"
```

Then run:

```bash
bundle install
```

Configure the project name **in code** by calling `Braintrust.init` during app boot. In Rails, add `config/initializers/braintrust.rb`:

```ruby
Braintrust.init(default_project: "my-project")
```

For non-Rails apps, call `Braintrust.init(default_project: "my-project")` early in the boot sequence (e.g. `config.ru`, `boot.rb`, or the main entrypoint before any LLM clients are created).

Do **not** set the project name via the `BRAINTRUST_DEFAULT_PROJECT` environment variable -- the project name must live in code.

**Important**: The application must call `Bundler.require` for the auto-instrumentation to kick in (Rails does this by default). If not, add `require "braintrust/setup"` to an initializer file.

### Option B: CLI command (no source code changes)

Install the gem:

```bash
gem install braintrust
```

Or, preferably, add it to the Gemfile so it is checked in:

```ruby
gem "braintrust"
```

Then wrap the application's start command:

```bash
braintrust exec -- ruby app.rb
braintrust exec -- bundle exec rails server
```

To limit which providers are instrumented:

```bash
braintrust exec --only openai -- ruby app.rb
```

**Requirement: persist `braintrust exec` into the normal run path.** A one-off `braintrust exec -- ...` during verification is **not enough** if the next developer, CI job, or deploy will go back to a plain `ruby` / `bundle exec` / `rails server` command. Update whichever launch path the project actually uses:

- **`Procfile` / `foreman`**: change `web: bundle exec rails server` to `web: braintrust exec -- bundle exec rails server`.
- **`Dockerfile` / container entrypoint**: update the `CMD` / `ENTRYPOINT` or checked-in start script.
- **Process managers / deploy config**: update systemd units, Kubernetes manifests, ECS task definitions, etc.
- **`Makefile` / scripts**: update `make run` / `bin/start` / etc.
- **Bootstrap / CI / devcontainer setup**: if the repo already has a checked-in way to install required tooling, make sure `braintrust` is installed there too so future users and CI do not hit `braintrust: command not found`.

A shell-local one-off `braintrust exec -- ...` does **not** satisfy this requirement by itself. If you use Option B but do not modify any persisted launch path, treat the installation as incomplete.

### Option C: Braintrust.init (explicit control)

Add to the Gemfile:

```ruby
gem "braintrust"
```

Then call `Braintrust.init` in your code:

```ruby
require "braintrust"

Braintrust.init(default_project: "my-project")
```

Options for `Braintrust.init`:

| Option            | Default                             | Description                                                                 |
| ----------------- | ----------------------------------- | --------------------------------------------------------------------------- |
| `default_project` | `ENV['BRAINTRUST_DEFAULT_PROJECT']` | Default project for spans                                                   |
| `auto_instrument` | `true`                              | `true`, `false`, or Hash with `:only`/`:except` keys to filter integrations |
| `api_key`         | `ENV['BRAINTRUST_API_KEY']`         | API key                                                                     |

## Instrument the application

**You must read https://www.braintrust.dev/docs/instrument/trace-llm-calls before instrumenting anything.** That page is the source of truth for supported providers and setup, and may have changed since this guide was written.

### Prefer automatic instrumentation

**Automatic instrumentation is the recommended path and should be used whenever possible.** All three setup approaches above (`braintrust/setup`, `braintrust exec`, `Braintrust.init`) auto-instrument every supported library that is installed -- no wrapping code needed.

Manual span / wrapper code should only be used as a **last resort**, e.g. for custom business-logic spans or to cover a library that auto-instrumentation doesn't yet support. Don't reach for manual tracing before confirming auto-instrumentation can do the job.

### Supported providers (auto-instrumented)

For the current list of auto-instrumented gems and their integration names, see https://www.braintrust.dev/docs/instrument/trace-llm-calls.

### Selectively enabling integrations

```ruby
Braintrust.init(auto_instrument: { only: [:openai] })
```

Or via environment variables:

```bash
export BRAINTRUST_INSTRUMENT_ONLY=openai,anthropic
```

## Run the application

Prefer the project's existing run entrypoint, and -- if you picked Option B -- make sure that entrypoint now goes through `braintrust exec`.

Try to figure out how the project is normally run from the project structure:

- **Procfile / foreman**: prefer `foreman start` or whatever the repo already uses, and update the `Procfile` entries (not an ad-hoc shell command)
- **Rails**: `bundle exec rails server` or `bin/rails server` -- if using Option B, update the persisted start script to wrap it in `braintrust exec --`
- **Rack/Sinatra**: `bundle exec rackup` or `ruby app.rb` -- update the persisted launch command, not a one-off invocation
- **Script**: `bundle exec ruby main.rb` or `ruby main.rb`
- **Docker / container**: update the `Dockerfile`'s `CMD` / `ENTRYPOINT` or checked-in start script

If you can't determine how the app is supposed to be run in normal use, ask the user before proceeding.

## Generate a permalink (required)

Follow the permalink generation steps in the agent task (Step 5). Use the project name you configured in code above.
