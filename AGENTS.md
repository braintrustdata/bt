# Agent Rules

## Clap argument sourcing

- If a value is exposed as a Clap argument (including `env = ...` on that argument), do not read it with `std::env::var`.
- Always read it from the parsed Clap args struct instead.
