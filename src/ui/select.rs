use anyhow::{bail, Result};
use dialoguer::{theme::ColorfulTheme, FuzzySelect};

use crate::{http::ApiClient, projects::api, ui::with_spinner};

/// Fuzzy select from a list of items. Requires TTY.
pub fn fuzzy_select<T: ToString>(prompt: &str, items: &[T], default: usize) -> Result<usize> {
    if !super::is_interactive() {
        bail!("interactive mode requires TTY");
    }

    if items.is_empty() {
        bail!("no items to select from");
    }

    let labels: Vec<String> = items.iter().map(|i| i.to_string()).collect();

    let selection = FuzzySelect::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(&labels)
        .default(default)
        .max_length(12)
        .interact()?;

    Ok(selection)
}

/// Interactive selector for project data
pub async fn select_project_interactive(
    client: &ApiClient,
    select_label: Option<&str>,
    current: Option<&str>,
) -> Result<String> {
    let mut projects = with_spinner("Loading projects...", api::list_projects(client)).await?;

    if projects.is_empty() {
        bail!("no projects found in org '{}'", &client.org_name());
    }

    projects.sort_by(|a, b| a.name.cmp(&b.name));
    let names: Vec<&str> = projects.iter().map(|p| p.name.as_str()).collect();
    let default = current
        .and_then(|c| names.iter().position(|n| *n == c))
        .unwrap_or(0);

    let label = select_label.unwrap_or("Select project");
    let selection = fuzzy_select(label, &names, default)?;
    Ok(projects[selection].name.clone())
}
