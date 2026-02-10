use std::io::IsTerminal;

use anyhow::{bail, Result};
use dialoguer::{theme::ColorfulTheme, FuzzySelect};

use crate::{http::ApiClient, projects::api, ui::with_spinner};

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

/// Interactive selector for project data
pub async fn select_project_interactive(client: &ApiClient) -> Result<String> {
    let mut projects = with_spinner("Loading projects...", api::list_projects(client)).await?;

    if projects.is_empty() {
        bail!("no projects found in org '{}'", &client.org_name());
    }

    projects.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = projects.iter().map(|p| p.name.as_str()).collect();

    let selection = fuzzy_select("Select project", &names)?;
    Ok(projects[selection].name.clone())
}
