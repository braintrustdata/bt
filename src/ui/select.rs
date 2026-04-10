use std::io::IsTerminal;

use anyhow::{anyhow, bail, Result};
use dialoguer::console::Term;
use dialoguer::{theme::ColorfulTheme, FuzzySelect, Input};

use crate::{http::ApiClient, projects::api, ui::with_spinner};

/// Open a Term for interactive prompts.
///
/// Prefers stderr (already a TTY in the common case). Falls back to `/dev/tty`
/// so that prompts still work when stdin/stderr are redirected — e.g. when bt
/// is invoked from a shell script: `echo "bt setup" | sh`.
///
/// Returns `None` when no interactive terminal is available at all (headless CI).
fn tty_term() -> Option<Term> {
    if std::io::stderr().is_terminal() {
        return Some(Term::stderr());
    }
    #[cfg(unix)]
    {
        use std::fs::OpenOptions;
        if let Ok(tty) = OpenOptions::new().read(true).write(true).open("/dev/tty") {
            if let Ok(tty2) = tty.try_clone() {
                return Some(Term::read_write_pair(tty, tty2));
            }
        }
    }
    None
}

/// Fuzzy select from a list of items. Requires an interactive terminal.
/// Works even when stdin is piped (e.g. `echo "bt setup" | sh`) because
/// it falls back to /dev/tty for both display and keyboard input.
pub fn fuzzy_select<T: ToString>(prompt: &str, items: &[T], default: usize) -> Result<usize> {
    let Some(term) = tty_term() else {
        bail!("interactive mode requires TTY");
    };

    if items.is_empty() {
        bail!("no items to select from");
    }

    let labels: Vec<String> = items.iter().map(|i| i.to_string()).collect();

    let selection = FuzzySelect::with_theme(&ColorfulTheme::default())
        .with_prompt(prompt)
        .items(&labels)
        .default(default)
        .max_length(12)
        .interact_on(&term)?;

    Ok(selection)
}

pub async fn select_project(
    client: &ApiClient,
    project_name: Option<&str>,
    label: Option<&str>,
) -> Result<api::Project> {
    if let Some(name) = project_name {
        return with_spinner("Loading project...", api::get_project_by_name(client, name))
            .await?
            .ok_or_else(|| anyhow!("project '{}' not found", name));
    }

    let mut projects = with_spinner("Loading projects...", api::list_projects(client)).await?;
    projects.sort_by(|a, b| a.name.cmp(&b.name));

    const CREATE_OPTION: &str = "+ Create new project";
    let mut labels: Vec<&str> = vec![CREATE_OPTION];
    labels.extend(projects.iter().map(|p| p.name.as_str()));
    let default = if projects.is_empty() { 0 } else { 1 };
    let selection = fuzzy_select(label.unwrap_or("Select project"), &labels, default)?;

    if selection == 0 {
        let name: String = Input::with_theme(&ColorfulTheme::default())
            .with_prompt("New project name")
            .interact_text()?;
        return with_spinner(
            &format!("Creating project '{name}'..."),
            api::create_project(client, &name),
        )
        .await;
    }

    Ok(projects[selection - 1].clone())
}
