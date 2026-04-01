# Ruby SDK Install

Reference guide for installing the Braintrust Ruby SDK.

- SDK repo: https://github.com/braintrustdata/braintrust-sdk-ruby
- RubyGems: https://rubygems.org/gems/braintrust
- Requires Ruby 3.1+

## Find the latest version of the SDK

Look up the latest version from RubyGems **without installing anything**. Do not guess -- use a read-only query so the environment stays unchanged.

```bash
gem search braintrust --remote --versions
```

## Install the SDK

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

Configure the project name in code (preferred over env vars):

```ruby
require "braintrust"

Braintrust.init(default_project: "my-project")
```

**Important**: The application must call `Bundler.require` for this to work (Rails does this by default). If not, add `require "braintrust/setup"` to an initializer file.

### Option B: CLI command (no source code changes)

Install the gem system-wide:

```bash
gem install braintrust
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

## Install instrumentation

By default, all three setup approaches auto-instrument every supported library that is installed. No wrapping code is needed.

### Supported providers (auto-instrumented)

| Provider  | Gem           | Integration name |
| --------- | ------------- | ---------------- |
| OpenAI    | `openai`      | `:openai`        |
|           | `ruby-openai` | `:ruby_openai`   |
| Anthropic | `anthropic`   | `:anthropic`     |
| Multiple  | `ruby_llm`    | `:ruby_llm`      |

### Selectively enabling integrations

```ruby
Braintrust.init(auto_instrument: { only: [:openai] })
```

Or via environment variables:

```bash
export BRAINTRUST_INSTRUMENT_ONLY=openai,anthropic
```

## Run the application

Try to figure out how to run the application from the project structure:

- **Rails**: `bundle exec rails server` or `bin/rails server`
- **Rack/Sinatra**: `bundle exec rackup` or `ruby app.rb`
- **Script**: `bundle exec ruby main.rb` or `ruby main.rb`
- **CLI wrap**: `braintrust exec -- <start command>`

If you can't determine how to run the app, ask the user.

## Generate a permalink (required)

Follow the permalink generation steps in the agent task (Step 5). Use the `default_project` argument passed to `Braintrust.init` as the project name.
