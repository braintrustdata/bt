# bt CLI Codebase Patterns

Established patterns in the bt CLI that new code must follow for consistency.

## Module Structure

### Resource modules follow a standard layout

Every resource (projects, prompts) uses the same file structure:

```
src/<resource>/
├── mod.rs      # Args, subcommand enum, run() dispatcher
├── api.rs      # HTTP API calls, request/response types
├── list.rs     # List subcommand
├── view.rs     # View subcommand
├── delete.rs   # Delete subcommand
└── ...         # Additional subcommands
```

### mod.rs pattern

Each resource module's `mod.rs` follows this exact pattern:

```rust
// 1. Args struct with Optional subcommand (None defaults to List)
#[derive(Debug, Clone, Args)]
pub struct ResourceArgs {
    #[command(subcommand)]
    command: Option<ResourceCommands>,
}

// 2. Subcommand enum with doc comments as help text
#[derive(Debug, Clone, Subcommand)]
enum ResourceCommands {
    /// List all resources
    List,
    /// View a resource
    View(ViewArgs),
    /// Delete a resource
    Delete(DeleteArgs),
}

// 3. run() function: login → client → match dispatch
pub async fn run(base: BaseArgs, args: ResourceArgs) -> Result<()> {
    let ctx = login(&base).await?;
    let client = ApiClient::new(&ctx)?;
    // ... resolve project if needed ...
    match args.command {
        None | Some(ResourceCommands::List) => list::run(...).await,
        Some(ResourceCommands::View(a)) => view::run(...).await,
        Some(ResourceCommands::Delete(a)) => delete::run(...).await,
    }
}
```

**Key rule**: `None` always maps to `List` — running `bt <resource>` with no subcommand shows the list.

### api.rs pattern

```rust
// 1. Main model struct: Serialize + Deserialize, pub fields
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Resource {
    pub id: String,
    pub name: String,
    #[serde(default)]
    pub description: Option<String>,
}

// 2. Private ListResponse wrapper
#[derive(Debug, Deserialize)]
struct ListResponse {
    objects: Vec<Resource>,
}

// 3. Functions take &ApiClient, return Result<T>
pub async fn list_resources(client: &ApiClient, ...) -> Result<Vec<Resource>> { ... }
pub async fn get_resource_by_name(client: &ApiClient, ...) -> Result<Option<Resource>> { ... }
pub async fn delete_resource(client: &ApiClient, id: &str) -> Result<()> { ... }
```

**Key rules**:

- URL-encode all query params with `urlencoding::encode()`
- `get_by_name` returns `Option<T>` (not an error for missing)
- `list_` returns `Vec<T>` via `ListResponse.objects`

## UX Patterns

### Interactive fallback pattern

When a required identifier (name, slug) isn't provided:

```rust
let resource = match identifier {
    Some(s) => fetch_by_identifier(client, s).await?,
    None => {
        if !std::io::stdin().is_terminal() {
            bail!("<identifier> required. Use: bt <resource> <cmd> <identifier>");
        }
        select_resource_interactive(client).await?
    }
};
```

**Key rules**:

- Always check `std::io::stdin().is_terminal()` before interactive prompts
- Bail message must include the exact command syntax to use non-interactively
- Use `select_<resource>_interactive()` for fuzzy selection

### Interactive selection pattern

```rust
pub async fn select_resource_interactive(client: &ApiClient, ...) -> Result<Resource> {
    let mut items = with_spinner("Loading ...", api::list_resources(client, ...)).await?;
    if items.is_empty() {
        bail!("no <resources> found");
    }
    items.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = items.iter().map(|i| i.name.as_str()).collect();
    let selection = ui::fuzzy_select("Select <resource>", &names)?;
    Ok(items[selection].clone())
}
```

**Key rules**:

- Sort alphabetically before display
- Use `ui::fuzzy_select()` (wraps dialoguer FuzzySelect with TTY guard)
- Bail early if list is empty
- Wrap API call in `with_spinner`

### Delete confirmation pattern

```rust
if std::io::stdin().is_terminal() {
    let confirm = Confirm::new()
        .with_prompt(format!("Delete <resource> '{}'?", name))
        .default(false)
        .interact()?;
    if !confirm {
        return Ok(());
    }
}
```

**Key rules**:

- Default to `false` (don't delete)
- Only prompt when stdin is terminal
- Silent (no confirmation) when non-interactive — relies on explicit identifier arg
- Return `Ok(())` on decline, not an error

### Success/error status pattern

Mutating operations (create, delete) use `print_command_status`:

```rust
match with_spinner("Deleting...", api::delete(client, &id)).await {
    Ok(_) => {
        print_command_status(CommandStatus::Success, &format!("Deleted '{name}'"));
        Ok(())
    }
    Err(e) => {
        print_command_status(CommandStatus::Error, &format!("Failed to delete '{name}'"));
        Err(e)
    }
}
```

**Key rules**:

- `✓` green for success, `✗` red for error (via `CommandStatus` enum)
- Message format: past tense verb + quoted resource name
- Print status THEN return the error (so user sees both)

### "Open in browser" pattern

```rust
let url = format!(
    "{}/app/{}/p/{}",
    app_url.trim_end_matches('/'),
    encode(org_name),
    encode(&name)
);
open::that(&url)?;
print_command_status(CommandStatus::Success, &format!("Opened {url} in browser"));
```

### List output pattern

```rust
if json {
    println!("{}", serde_json::to_string(&items)?);
} else {
    let mut output = String::new();
    // Summary line: count + context
    writeln!(output, "{} found in {}\n", count, context)?;
    // Table
    let mut table = styled_table();
    table.set_header(vec![header("Col1"), header("Col2")]);
    apply_column_padding(&mut table, (0, 6));
    for item in &items {
        table.add_row(vec![...]);
    }
    write!(output, "{table}")?;
    print_with_pager(&output)?;
}
```

**Key rules**:

- JSON check first, early return
- Build output into a `String`, then pipe through `print_with_pager`
- Summary line before table: "{count} {resource}s found in {context}"
- Table uses `styled_table()` (NOTHING preset, dynamic width)
- Headers use `header()` (bold + dim)
- Column padding `(0, 6)` — no left padding, 6 right padding
- Truncate descriptions to 60 chars with `truncate(s, 60)`
- Missing descriptions display as `"-"`

## Code Patterns

### All subcommand functions are `pub async fn run(...) -> Result<()>`

Entry point for every subcommand. Parameters are borrowed references, not owned.

### BaseArgs are global and flattened via CLIArgs<T>

```rust
// In main.rs:
Commands::Resource(CLIArgs { base, args }) => resource::run(base, args).await?

// BaseArgs contains: json, project, api_key, api_url, app_url, env_file
```

`--json` and `--project` are always available on any resource subcommand.

### Error handling

- Use `anyhow::Result` everywhere
- Use `.context("human-readable message")` on fallible operations
- Use `bail!("message")` for expected user errors
- Use `anyhow!("message")` for constructing error values
- HTTP errors: `"request failed ({status}): {body}"` format

### Spinner usage

- `with_spinner("Loading...", future)` — shows after 300ms, clears on completion
- `with_spinner_visible("Creating...", future, Duration)` — always shows, enforces minimum display time
- Spinner message: present participle + "..." (e.g., "Loading prompts...", "Deleting project...")
- Only shows when stderr is terminal

### Positional + flag dual args pattern

When a subcommand takes a primary identifier:

```rust
#[derive(Debug, Clone, Args)]
pub struct ViewArgs {
    /// Resource identifier (positional)
    #[arg(value_name = "IDENTIFIER")]
    identifier_positional: Option<String>,

    /// Resource identifier (flag)
    #[arg(long = "identifier", short = 'x')]
    identifier_flag: Option<String>,
}

impl ViewArgs {
    fn identifier(&self) -> Option<&str> {
        self.identifier_positional
            .as_deref()
            .or(self.identifier_flag.as_deref())
    }
}
```

**Key rule**: Positional takes precedence over flag. Both are optional (falls back to interactive).

### Project resolution pattern

Commands that operate within a project scope:

```rust
let project = match base.project {
    Some(p) => p,
    None if std::io::stdin().is_terminal() => select_project_interactive(&client).await?,
    None => anyhow::bail!("--project required (or set BRAINTRUST_DEFAULT_PROJECT)"),
};
```

### Color/styling conventions

- `console::style(name).bold()` — resource names, identifiers
- `console::style(text).dim()` — secondary info, separators, headers
- `console::style(text).green()` — success, user role
- `console::style(text).blue()` — assistant role
- `console::style(text).red()` — errors (via CommandStatus)
- `console::style(text).yellow()` — tool names, function calls
- `console::style(text).cyan()` — template variables, spinners
- `console::style(text).magenta()` — section labels (e.g., "tools")

### Import conventions

```rust
// std imports first
use std::fmt::Write as _;
use std::io::IsTerminal;

// External crates
use anyhow::{bail, Result};
use dialoguer::console;

// Internal crate imports
use crate::http::ApiClient;
use crate::ui::{...};

// Sibling module
use super::api;
```

### View output pattern (rich display)

For detailed single-resource views:

```rust
let mut output = String::new();
writeln!(output, "Viewing {}", console::style(&name).bold())?;
// ... render fields ...
print_with_pager(&output)?;
```

Build into String, then page. Use box-drawing characters (`┃`, `│`) for structure.
