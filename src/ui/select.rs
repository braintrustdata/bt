use std::io::IsTerminal;

use anyhow::{bail, Result};
use dialoguer::{theme::ColorfulTheme, FuzzySelect};

/// Fuzzy select from a list of items. Requires TTY.
pub fn fuzzy_select<T: ToString>(prompt: &str, items: &[T]) -> Result<usize> {
    if !std::io::stdin().is_terminal() {
        bail!("interactive mode requires TTY");
    }

    if items.is_empty() {
        bail!("no items to select from");
    }

    let labels: Vec<String> = items.iter().map(|i| i.to_string()).collect();

    let selection = FuzzySelect::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(&labels)
        .default(0)
        .interact()?;

    Ok(selection)
}
